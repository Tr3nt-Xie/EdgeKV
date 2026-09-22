package raft

import (
	"context"
	"slices"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// runReplicator owns replication to one follower for the duration of one
// leadership term. It sends whenever it is triggered (new entries, a pending
// read) and otherwise every heartbeat interval.
func (r *Raft) runReplicator(ctx context.Context, peer string, term uint64) {
	defer r.wg.Done()

	r.mu.Lock()
	trigger := r.triggers[peer]
	r.mu.Unlock()

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		// Keep sending while the follower is behind; fall back to the
		// heartbeat pace when it is caught up or unreachable.
		for r.replicateOnce(ctx, peer, term) {
		}
		timer.Reset(r.cfg.HeartbeatInterval)
		select {
		case <-ctx.Done():
			return
		case <-trigger:
		case <-timer.C:
		}
	}
}

func (r *Raft) triggerReplicationLocked() {
	for _, ch := range r.triggers {
		select {
		case ch <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
}

// replicateOnce sends one AppendEntries (or one snapshot) to peer. It returns
// true if there is more to send right away.
func (r *Raft) replicateOnce(ctx context.Context, peer string, term uint64) bool {
	r.mu.Lock()
	if ctx.Err() != nil || r.state != Leader || r.term != term {
		r.mu.Unlock()
		return false
	}
	next := r.nextIndex[peer]
	if next <= r.rlog.snapIndex {
		// The entries this follower needs were compacted away.
		r.mu.Unlock()
		return r.sendSnapshot(ctx, peer, term)
	}

	prevIndex := next - 1
	prevTerm, _ := r.rlog.term(prevIndex)
	last := min(r.rlog.lastIndex(), prevIndex+uint64(r.cfg.MaxAppendEntries))
	req := &pb.AppendEntriesRequest{
		Group:        r.cfg.Group,
		Term:         term,
		LeaderId:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      r.rlog.slice(next, last),
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	sentAt := time.Now()
	rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCTimeout)
	resp, err := r.cfg.Transport.AppendEntries(rpcCtx, peer, req)
	cancel()
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if resp.Term > r.term {
		r.becomeFollowerLocked(resp.Term, "")
		return false
	}
	// A reply to a request from an earlier term, or one that arrives after we
	// stepped down, says nothing about the present.
	if r.state != Leader || r.term != term {
		return false
	}
	r.recordAckLocked(peer, sentAt)

	if resp.Success {
		match := req.PrevLogIndex + uint64(len(req.Entries))
		// Replies can arrive out of order; matchIndex only moves forward.
		if match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
			r.nextIndex[peer] = match + 1
			r.advanceCommitLocked()
		}
		return r.nextIndex[peer] <= r.rlog.lastIndex()
	}

	// Log mismatch: jump back using the follower's hint.
	next = resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		// If we also have entries of the conflicting term, the logs may agree
		// up to our last entry of that term. Otherwise skip the term entirely.
		if idx := r.rlog.lastIndexOfTerm(resp.ConflictTerm); idx != 0 {
			next = idx + 1
		}
	}
	r.nextIndex[peer] = max(1, min(next, r.rlog.lastIndex()+1))
	return true
}

func (r *Raft) recordAckLocked(peer string, sentAt time.Time) {
	if sentAt.After(r.ackedSendTime[peer]) {
		r.ackedSendTime[peer] = sentAt
		broadcast(&r.ackNotify)
	}
}

// advanceCommitLocked moves commitIndex to the highest index stored on a
// majority — but only if that entry is from the current term.
//
// The restriction is Raft §5.4.2 (Figure 8). An entry from an older term can
// sit on a majority and still be overwritten by a future leader, so counting
// replicas is not enough to call it committed. Committing an entry of the
// current term is safe, and by the Log Matching property it commits everything
// before it too.
func (r *Raft) advanceCommitLocked() {
	matches := make([]uint64, 0, len(r.others)+1)
	matches = append(matches, r.rlog.lastIndex())
	for _, p := range r.others {
		matches = append(matches, r.matchIndex[p])
	}
	slices.Sort(matches)
	// With n sorted values, the one at position n-quorum is held by at least
	// quorum nodes.
	candidate := matches[len(matches)-r.quorum]

	if candidate <= r.commitIndex {
		return
	}
	if t, _ := r.rlog.term(candidate); t != r.term {
		return
	}
	r.commitIndex = candidate
	r.applyCond.Broadcast()
}

// HandleAppendEntries implements the receiver side of AppendEntries (Raft §5.3).
func (r *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.term {
		return &pb.AppendEntriesResponse{Term: r.term}
	}
	// Equal term and we are a candidate: someone else won this election.
	if req.Term > r.term || r.state != Follower || r.leaderID != req.LeaderId {
		r.becomeFollowerLocked(req.Term, req.LeaderId)
	}
	r.lastLeaderContact = time.Now()
	r.resetElectionTimerLocked()

	prevIndex, entries := req.PrevLogIndex, req.Entries

	// Part of what the leader sent may already be inside our snapshot. Those
	// entries are committed, hence identical; skip them.
	if prevIndex < r.rlog.snapIndex {
		skip := r.rlog.snapIndex - prevIndex
		if skip >= uint64(len(entries)) {
			return &pb.AppendEntriesResponse{Term: r.term, Success: true}
		}
		entries = entries[skip:]
		prevIndex = r.rlog.snapIndex
	}

	// Consistency check: our log must contain the entry that precedes the new ones.
	if prevIndex > r.rlog.lastIndex() {
		return &pb.AppendEntriesResponse{Term: r.term, ConflictIndex: r.rlog.lastIndex() + 1}
	}
	if prevIndex == req.PrevLogIndex {
		if t, _ := r.rlog.term(prevIndex); t != req.PrevLogTerm {
			return &pb.AppendEntriesResponse{
				Term:          r.term,
				ConflictTerm:  t,
				ConflictIndex: r.rlog.firstIndexOfTerm(t, prevIndex),
			}
		}
	}

	// Append, truncating only on a real conflict. An entry we already hold
	// with the same term is left alone: a delayed, shorter request must never
	// chop off entries that a newer request already delivered.
	var fresh []*pb.Entry
	for i, e := range entries {
		t, ok := r.rlog.term(e.Index)
		if ok && t == e.Term {
			continue
		}
		if ok {
			r.rlog.truncateFrom(e.Index)
		}
		fresh = entries[i:]
		break
	}
	if len(fresh) > 0 {
		r.rlog.append(fresh...)
		r.persistLocked(false, fresh) // durable before we acknowledge
	}

	// Only entries we have verified against the leader may be committed. Our
	// log can extend past them with leftovers from an older leader.
	lastVerified := req.PrevLogIndex + uint64(len(req.Entries))
	if newCommit := min(req.LeaderCommit, lastVerified); newCommit > r.commitIndex {
		r.commitIndex = newCommit
		r.applyCond.Broadcast()
	}
	return &pb.AppendEntriesResponse{Term: r.term, Success: true}
}

// sendSnapshot ships the latest snapshot to a follower that has fallen behind
// the compacted part of the log.
func (r *Raft) sendSnapshot(ctx context.Context, peer string, term uint64) bool {
	meta, data, err := r.cfg.Storage.Snapshot()
	if err != nil || meta == nil {
		r.log.Error("cannot load snapshot to send", "peer", peer, "err", err)
		return false
	}
	req := &pb.InstallSnapshotRequest{
		Group: r.cfg.Group, Term: term, LeaderId: r.id,
		LastIncludedIndex: meta.LastIncludedIndex, LastIncludedTerm: meta.LastIncludedTerm,
		Data: data,
	}
	sentAt := time.Now()
	// Snapshots are large; give them far more time than a heartbeat.
	rpcCtx, cancel := context.WithTimeout(ctx, 20*r.cfg.RPCTimeout+time.Minute)
	resp, err := r.cfg.Transport.InstallSnapshot(rpcCtx, peer, req)
	cancel()
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if resp.Term > r.term {
		r.becomeFollowerLocked(resp.Term, "")
		return false
	}
	if r.state != Leader || r.term != term {
		return false
	}
	r.recordAckLocked(peer, sentAt)
	if meta.LastIncludedIndex > r.matchIndex[peer] {
		r.matchIndex[peer] = meta.LastIncludedIndex
		r.advanceCommitLocked()
	}
	r.nextIndex[peer] = r.matchIndex[peer] + 1
	r.log.Info("installed snapshot on follower", "peer", peer, "index", meta.LastIncludedIndex)
	return r.nextIndex[peer] <= r.rlog.lastIndex()
}

// HandleInstallSnapshot implements the receiver side of InstallSnapshot (Raft §7).
func (r *Raft) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.term {
		return &pb.InstallSnapshotResponse{Term: r.term}
	}
	if req.Term > r.term || r.state != Follower || r.leaderID != req.LeaderId {
		r.becomeFollowerLocked(req.Term, req.LeaderId)
	}
	r.lastLeaderContact = time.Now()
	defer r.resetElectionTimerLocked() // writing the snapshot below can be slow

	// Nothing to gain from a snapshot of what we have already committed.
	if req.LastIncludedIndex <= r.commitIndex {
		return &pb.InstallSnapshotResponse{Term: r.term}
	}

	// If our log agrees with the snapshot at its last index, the entries after
	// it are still good and must be kept: we may have acknowledged them, and
	// the leader may have counted us towards their commitment.
	t, ok := r.rlog.term(req.LastIncludedIndex)
	keepSuffix := ok && t == req.LastIncludedTerm

	meta := &pb.SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	if err := r.cfg.Storage.SaveSnapshot(meta, req.Data, !keepSuffix); err != nil {
		panic("raft: cannot persist snapshot: " + err.Error())
	}
	if keepSuffix {
		r.rlog.compactTo(req.LastIncludedIndex, req.LastIncludedTerm)
	} else {
		r.rlog.reset(req.LastIncludedIndex, req.LastIncludedTerm)
	}
	r.commitIndex = req.LastIncludedIndex
	r.pendingSnapshot = req
	r.applyCond.Broadcast()
	return &pb.InstallSnapshotResponse{Term: r.term}
}
