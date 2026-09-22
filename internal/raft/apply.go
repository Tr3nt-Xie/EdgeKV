package raft

import (
	"context"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

// Propose replicates a command and blocks until it has been committed and
// applied, returning the state machine's result.
//
// An error does not mean the command was not executed. If leadership is lost
// or ctx expires after the entry was appended, a later leader may still commit
// it. Callers retry with the same request ID and rely on deduplication.
func (r *Raft) Propose(ctx context.Context, data []byte) ([]byte, error) {
	p := &proposal{data: data, done: make(chan proposalResult, 1)}
	select {
	case r.proposeCh <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, ErrStopped
	}
	select {
	case res := <-p.done:
		return res.result, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, ErrStopped
	}
}

// runProposer turns the stream of proposals into batches. Everything that
// queued up while the previous batch was being flushed goes to disk with one
// fsync and to the followers in one AppendEntries: this is group commit, and
// it is why throughput rises with concurrency even though each flush is slow.
func (r *Raft) runProposer() {
	defer r.wg.Done()
	batch := make([]*proposal, 0, r.cfg.MaxProposalBatch)
	for {
		batch = batch[:0]
		select {
		case <-r.stopCh:
			return
		case p := <-r.proposeCh:
			batch = append(batch, p)
		}
	drain:
		for len(batch) < r.cfg.MaxProposalBatch {
			select {
			case p := <-r.proposeCh:
				batch = append(batch, p)
			default:
				break drain
			}
		}
		r.appendProposals(batch)
	}
}

func (r *Raft) appendProposals(batch []*proposal) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader || r.transferTarget != "" || r.stopped {
		err := error(&NotLeaderError{Leader: r.leaderHintLocked()})
		if r.stopped {
			err = ErrStopped
		}
		for _, p := range batch {
			p.done <- proposalResult{err: err}
		}
		return
	}

	entries := make([]*pb.Entry, len(batch))
	for i, p := range batch {
		p.term = r.term
		entries[i] = &pb.Entry{
			Index: r.rlog.lastIndex() + 1 + uint64(i),
			Term:  r.term,
			Type:  pb.EntryType_ENTRY_NORMAL,
			Data:  p.data,
		}
	}
	r.rlog.append(entries...)
	// The leader's own copy counts towards the majority, so it has to be
	// durable before it is counted — i.e. before advanceCommitLocked can see it.
	r.persistLocked(false, entries)
	for i, p := range batch {
		r.waiters[entries[i].Index] = p
	}
	r.triggerReplicationLocked()
	r.advanceCommitLocked()
}

func (r *Raft) leaderHintLocked() string {
	if r.state == Leader {
		return "" // we are leader but refusing (transfer in progress)
	}
	return r.leaderID
}

// ---------------------------------------------------------------------------
// Linearizable reads
// ---------------------------------------------------------------------------

// ReadIndex returns once it is safe to serve a linearizable read from the
// local state machine.
//
// A leader cannot simply answer from its own state: it may have been deposed
// without knowing, and would then serve stale data. ReadIndex (Raft thesis
// §6.4) closes that hole without writing to the log:
//
//  1. remember the current commitIndex as readIndex
//  2. exchange one round of heartbeats with a majority — this proves no newer
//     leader existed at the time the read arrived
//  3. wait until the state machine has applied up to readIndex
//
// Any write acknowledged before the read began is at or below readIndex, so
// the read observes it.
func (r *Raft) ReadIndex(ctx context.Context) error {
	req := &readRequest{done: make(chan readResult, 1)}
	select {
	case r.readCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return ErrStopped
	}
	select {
	case res := <-req.done:
		if res.err != nil {
			return res.err
		}
		return r.waitApplied(ctx, res.index)
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return ErrStopped
	}
}

// runReader batches concurrent reads so that they share one heartbeat round.
func (r *Raft) runReader() {
	defer r.wg.Done()
	var batch []*readRequest
	for {
		batch = batch[:0]
		select {
		case <-r.stopCh:
			return
		case req := <-r.readCh:
			batch = append(batch, req)
		}
	drain:
		for {
			select {
			case req := <-r.readCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		index, err := r.confirmLeadership()
		for _, req := range batch {
			req.done <- readResult{index: index, err: err}
		}
	}
}

func (r *Raft) confirmLeadership() (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		err := &NotLeaderError{Leader: r.leaderID}
		r.mu.Unlock()
		return 0, err
	}
	// Until the no-op of this term commits, commitIndex may lag behind writes
	// that the previous leader acknowledged.
	if r.commitIndex < r.noopIndex {
		r.mu.Unlock()
		return 0, ErrNotReady
	}
	term, readIndex, start := r.term, r.commitIndex, time.Now()
	r.triggerReplicationLocked()
	r.mu.Unlock()

	deadline := time.NewTimer(r.cfg.ElectionTimeoutMax)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		if r.state != Leader || r.term != term {
			err := &NotLeaderError{Leader: r.leaderID}
			r.mu.Unlock()
			return 0, err
		}
		// Only acknowledgements of RPCs sent after the read arrived count. An
		// older ack proves leadership at some earlier moment, which is useless.
		if r.hasQuorumAckSinceLocked(start) {
			r.mu.Unlock()
			return readIndex, nil
		}
		notify := r.ackNotify
		r.mu.Unlock()

		select {
		case <-notify:
		case <-deadline.C:
			return 0, &NotLeaderError{}
		case <-r.stopCh:
			return 0, ErrStopped
		}
	}
}

func (r *Raft) waitApplied(ctx context.Context, index uint64) error {
	for {
		r.mu.Lock()
		if r.lastApplied >= index {
			r.mu.Unlock()
			return nil
		}
		notify := r.appliedNotify
		r.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return ErrStopped
		}
	}
}

// ---------------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------------

// runApplier is the only goroutine that touches the state machine. It applies
// committed entries in order, installs snapshots received from the leader, and
// takes local snapshots — so none of the three can ever interleave.
func (r *Raft) runApplier() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.stopped && r.pendingSnapshot == nil && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}

		if snap := r.pendingSnapshot; snap != nil {
			r.pendingSnapshot = nil
			r.mu.Unlock()
			if err := r.cfg.StateMachine.Restore(snap.Data); err != nil {
				panic("raft: cannot restore snapshot: " + err.Error())
			}
			r.mu.Lock()
			r.lastApplied = max(r.lastApplied, snap.LastIncludedIndex)
			broadcast(&r.appliedNotify)
			r.mu.Unlock()
			continue
		}

		entries := r.rlog.slice(r.lastApplied+1, r.commitIndex)
		r.mu.Unlock()

		// Apply outside the lock: the state machine may be slow, and Raft must
		// keep answering heartbeats meanwhile.
		results := make([][]byte, len(entries))
		for i, e := range entries {
			if e.Type == pb.EntryType_ENTRY_NORMAL {
				results[i] = r.cfg.StateMachine.Apply(e.Index, e.Data)
			}
		}

		r.mu.Lock()
		for i, e := range entries {
			p, ok := r.waiters[e.Index]
			if !ok {
				continue
			}
			delete(r.waiters, e.Index)
			if p.term == e.Term {
				p.done <- proposalResult{result: results[i]}
			} else {
				// A different leader's entry ended up at this index; ours was
				// overwritten.
				p.done <- proposalResult{err: &NotLeaderError{Leader: r.leaderID}}
			}
		}
		last := entries[len(entries)-1].Index
		r.lastApplied = max(r.lastApplied, last)
		broadcast(&r.appliedNotify)
		needSnapshot := r.cfg.SnapshotThreshold > 0 && r.lastApplied > r.rlog.snapIndex &&
			r.lastApplied-r.rlog.snapIndex >= r.cfg.SnapshotThreshold
		r.mu.Unlock()

		if needSnapshot {
			r.takeSnapshot(last)
		}
	}
}

// takeSnapshot runs on the applier goroutine, between two applies, so the
// state machine is exactly at `index` while it is serialised.
func (r *Raft) takeSnapshot(index uint64) {
	start := time.Now()
	data, err := r.cfg.StateMachine.Snapshot()
	if err != nil {
		r.log.Error("snapshot failed", "err", err)
		return
	}

	r.mu.Lock()
	term, ok := r.rlog.term(index)
	r.mu.Unlock()
	if !ok {
		return // a snapshot from the leader overtook us
	}

	// The slow part — writing the file — happens without the Raft lock.
	meta := &pb.SnapshotMeta{LastIncludedIndex: index, LastIncludedTerm: term}
	if err := r.cfg.Storage.SaveSnapshot(meta, data, false); err != nil {
		r.log.Error("cannot save snapshot", "err", err)
		return
	}

	r.mu.Lock()
	r.rlog.compactTo(index, term)
	r.mu.Unlock()
	r.log.Info("took snapshot", "index", index, "bytes", len(data), "elapsed", time.Since(start))
}

// ---------------------------------------------------------------------------
// Leadership transfer
// ---------------------------------------------------------------------------

// TransferLeadership hands leadership to target (Raft thesis §3.10): stop
// accepting writes, bring the target fully up to date, then tell it to start an
// election at once. Its log is as complete as ours, so it wins.
func (r *Raft) TransferLeadership(ctx context.Context, target string) error {
	r.mu.Lock()
	if r.state != Leader {
		err := &NotLeaderError{Leader: r.leaderID}
		r.mu.Unlock()
		return err
	}
	if _, ok := r.matchIndex[target]; !ok || r.transferTarget != "" {
		r.mu.Unlock()
		return ErrTransferFailed
	}
	// Writes are refused for as long as a transfer is in flight, so do not even
	// begin one towards a node that has gone quiet.
	if time.Since(r.ackedSendTime[target]) > r.cfg.ElectionTimeoutMin {
		r.mu.Unlock()
		return ErrTransferFailed
	}
	term := r.term
	r.transferTarget = target
	r.triggerReplicationLocked()
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		if r.state == Leader && r.term == term {
			r.transferTarget = "" // transfer failed; resume accepting writes
		}
		r.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(ctx, r.cfg.ElectionTimeoutMax)
	defer cancel()

	// Wait for the target to hold every entry we have.
	for {
		r.mu.Lock()
		if r.state != Leader || r.term != term {
			r.mu.Unlock()
			return nil // someone else took over already
		}
		caughtUp := r.matchIndex[target] == r.rlog.lastIndex()
		notify := r.ackNotify
		r.mu.Unlock()
		if caughtUp {
			break
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return ErrTransferFailed
		}
	}

	req := &pb.TimeoutNowRequest{Group: r.cfg.Group, Term: term, LeaderId: r.id}
	if _, err := r.cfg.Transport.TimeoutNow(ctx, target, req); err != nil {
		return ErrTransferFailed
	}

	// Success is observed as us losing leadership.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		done := r.state != Leader || r.term != term
		r.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ErrTransferFailed
		}
	}
}
