package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// Raft is one replica of one Raft group.
type Raft struct {
	cfg    Config
	id     string
	others []string // every peer except this node
	quorum int
	log    *slog.Logger

	mu sync.Mutex

	// Persistent state (Raft Figure 2). Must be durable before any RPC reply
	// that depends on it leaves this node.
	term     uint64
	votedFor string
	rlog     *raftLog

	// Volatile state on all servers.
	state       State
	leaderID    string
	commitIndex uint64
	lastApplied uint64

	electionDeadline  time.Time
	lastLeaderContact time.Time
	campaigning       bool

	// Volatile state on the leader, re-initialised on every election win.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	// ackedSendTime[p] is the send time of the most recent RPC that p answered
	// in the current term. ReadIndex and check-quorum both build on it.
	ackedSendTime  map[string]time.Time
	noopIndex      uint64 // index of the no-op this leader appended on election
	transferTarget string
	leaderCancel   context.CancelFunc
	triggers       map[string]chan struct{}

	// waiters maps a log index to the proposal waiting for it to be applied.
	waiters map[uint64]*proposal

	// pendingSnapshot is handed from HandleInstallSnapshot to the applier, so
	// that the state machine only ever has one writer.
	pendingSnapshot *pb.InstallSnapshotRequest

	applyCond *sync.Cond
	// appliedNotify and ackNotify are closed and replaced to wake everyone
	// waiting for lastApplied or ackedSendTime to move.
	appliedNotify chan struct{}
	ackNotify     chan struct{}

	proposeCh chan *proposal
	readCh    chan *readRequest

	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

type proposal struct {
	data []byte
	term uint64
	done chan proposalResult
}

type proposalResult struct {
	result []byte
	err    error
}

type readRequest struct {
	done chan readResult
}

type readResult struct {
	index uint64
	err   error
}

// New creates a node and restores its state from storage. Call Start to make
// it participate in the group.
func New(cfg Config) (*Raft, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	r := &Raft{
		cfg:           cfg,
		id:            cfg.ID,
		quorum:        len(cfg.Peers)/2 + 1,
		log:           cfg.Logger.With("node", cfg.ID, "group", cfg.Group),
		rlog:          &raftLog{},
		state:         Follower,
		waiters:       make(map[uint64]*proposal),
		appliedNotify: make(chan struct{}),
		ackNotify:     make(chan struct{}),
		proposeCh:     make(chan *proposal, 4096),
		readCh:        make(chan *readRequest, 4096),
		stopCh:        make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			r.others = append(r.others, p)
		}
	}

	loaded, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	if loaded.HardState != nil {
		r.term, r.votedFor = loaded.HardState.Term, loaded.HardState.VotedFor
	}
	if meta := loaded.SnapshotMeta; meta != nil {
		if err := cfg.StateMachine.Restore(loaded.SnapshotData); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
		r.rlog.reset(meta.LastIncludedIndex, meta.LastIncludedTerm)
		// Everything inside a snapshot was committed and applied. Beyond it we
		// know nothing yet: commitIndex is deliberately not persisted and is
		// re-learned from the leader, after which the applier replays the log.
		r.commitIndex, r.lastApplied = meta.LastIncludedIndex, meta.LastIncludedIndex
	}
	r.rlog.append(loaded.Entries...)
	return r, nil
}

// Start launches the node's goroutines.
func (r *Raft) Start() {
	r.mu.Lock()
	r.resetElectionTimerLocked()
	r.mu.Unlock()

	r.wg.Add(4)
	go r.runTicker()
	go r.runProposer()
	go r.runReader()
	go r.runApplier()
}

// Stop shuts the node down and waits for its goroutines to exit. It does not
// close the storage.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stopCh)
	if r.leaderCancel != nil {
		r.leaderCancel()
	}
	r.failWaitersLocked(ErrStopped)
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

// Status returns a snapshot of the node's state.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statusLocked()
}

func (r *Raft) statusLocked() Status {
	st := Status{
		ID: r.id, Group: r.cfg.Group, State: r.state, Term: r.term, Leader: r.leaderID,
		CommitIndex: r.commitIndex, LastApplied: r.lastApplied,
		LastLogIndex: r.rlog.lastIndex(), SnapshotIndex: r.rlog.snapIndex,
	}
	if r.state == Leader {
		st.MatchIndex = make(map[string]uint64, len(r.matchIndex))
		for p, m := range r.matchIndex {
			st.MatchIndex[p] = m
		}
	}
	return st
}

// IsLeader reports whether this node currently believes it is the leader.
// A true answer can already be stale; only ReadIndex proves leadership.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

// Leader returns the ID of the leader this node currently knows of.
func (r *Raft) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

func (r *Raft) runTicker() {
	defer r.wg.Done()
	tick := min(r.cfg.HeartbeatInterval/2, 10*time.Millisecond)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case now := <-ticker.C:
			r.tick(now)
		}
	}
}

func (r *Raft) tick(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return
	}
	if r.state == Leader {
		// Check-quorum: a leader that cannot reach a majority steps down on its
		// own, so clients stop waiting on a node that can never commit.
		if !r.hasQuorumAckSinceLocked(now.Add(-r.cfg.ElectionTimeoutMax)) {
			r.log.Info("lost contact with a majority, stepping down", "term", r.term)
			r.becomeFollowerLocked(r.term, "")
		}
		return
	}
	if now.After(r.electionDeadline) && !r.campaigning {
		r.campaigning = true
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.campaign(false)
		}()
	}
}

// resetElectionTimerLocked picks a fresh random timeout. Randomisation is what
// makes split votes rare: nodes time out at different moments, so usually one
// candidate wins before the others even start (Raft §5.2).
func (r *Raft) resetElectionTimerLocked() {
	spread := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	timeout := r.cfg.ElectionTimeoutMin + time.Duration(rand.Int64N(int64(spread)+1))
	r.electionDeadline = time.Now().Add(timeout)
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

// becomeFollowerLocked moves to follower in the given term. If the term is new
// the vote is cleared and the hard state is persisted before returning.
func (r *Raft) becomeFollowerLocked(term uint64, leader string) {
	wasLeader := r.state == Leader
	changed := r.state != Follower || r.leaderID != leader

	if term > r.term {
		r.term, r.votedFor = term, ""
		r.persistLocked(true, nil)
		changed = true
	}
	r.state = Follower
	r.leaderID = leader

	if wasLeader {
		r.leaderCancel()
		r.leaderCancel = nil
		r.transferTarget = ""
		// The pending entries may still commit under the next leader. We cannot
		// know, so callers are told to retry; request IDs make that safe.
		r.failWaitersLocked(&NotLeaderError{Leader: leader})
		r.resetElectionTimerLocked()
	}
	if changed {
		r.notifyStateChangeLocked()
	}
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	r.transferTarget = ""
	r.nextIndex = make(map[string]uint64, len(r.others))
	r.matchIndex = make(map[string]uint64, len(r.others))
	r.ackedSendTime = make(map[string]time.Time, len(r.others))
	r.triggers = make(map[string]chan struct{}, len(r.others))
	now := time.Now()
	for _, p := range r.others {
		r.nextIndex[p] = r.rlog.lastIndex() + 1
		r.matchIndex[p] = 0
		r.ackedSendTime[p] = now // grace period for check-quorum
		r.triggers[p] = make(chan struct{}, 1)
	}

	// A leader may only count replicas for entries of its own term (§5.4.2),
	// so until it has such an entry it can neither commit what it inherited
	// nor serve linearizable reads. A no-op closes that gap immediately.
	noop := &pb.Entry{Index: r.rlog.lastIndex() + 1, Term: r.term, Type: pb.EntryType_ENTRY_NOOP}
	r.rlog.append(noop)
	r.persistLocked(false, []*pb.Entry{noop})
	r.noopIndex = noop.Index

	ctx, cancel := context.WithCancel(context.Background())
	r.leaderCancel = cancel
	for _, p := range r.others {
		r.wg.Add(1)
		go r.runReplicator(ctx, p, r.term)
	}
	r.log.Info("became leader", "term", r.term, "lastIndex", r.rlog.lastIndex())
	r.advanceCommitLocked() // a single-node group commits right away
	r.notifyStateChangeLocked()
}

func (r *Raft) notifyStateChangeLocked() {
	if r.cfg.OnStateChange == nil {
		return
	}
	st := r.statusLocked()
	go r.cfg.OnStateChange(st)
}

// persistLocked writes to stable storage and only returns once the data is
// durable. A node that cannot persist must not keep running: answering an RPC
// on the strength of state that may be lost is exactly how two leaders end up
// in the same term.
func (r *Raft) persistLocked(hardState bool, entries []*pb.Entry) {
	var hs *pb.HardState
	if hardState {
		hs = &pb.HardState{Term: r.term, VotedFor: r.votedFor}
	}
	if err := r.cfg.Storage.Save(hs, entries); err != nil {
		panic(fmt.Sprintf("raft: node %s cannot persist: %v", r.id, err))
	}
}

func (r *Raft) failWaitersLocked(err error) {
	for index, p := range r.waiters {
		p.done <- proposalResult{err: err}
		delete(r.waiters, index)
	}
}

// hasQuorumAckSinceLocked reports whether a majority (counting this node) has
// answered an RPC that was sent at or after t, in the current term.
func (r *Raft) hasQuorumAckSinceLocked(t time.Time) bool {
	acks := 1
	for _, p := range r.others {
		if !r.ackedSendTime[p].Before(t) {
			acks++
		}
	}
	return acks >= r.quorum
}

func broadcast(ch *chan struct{}) {
	close(*ch)
	*ch = make(chan struct{})
}
