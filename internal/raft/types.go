// Package raft implements the Raft consensus algorithm: leader election with
// pre-vote, log replication with fast conflict backtracking, durable state,
// snapshots, linearizable reads via ReadIndex, and leadership transfer.
//
// Concurrency model: one mutex (Raft.mu) guards all Raft state. Long-running
// work happens in a fixed set of goroutines — a ticker, a proposer, a reader,
// an applier, and one replicator per peer while leader — and none of them
// holds the mutex across a network call.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// StateMachine is what committed entries are applied to. Apply is called from
// a single goroutine, in log order.
type StateMachine interface {
	Apply(index uint64, data []byte) []byte
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Transport sends Raft RPCs to a peer identified by its node ID.
type Transport interface {
	RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, to string, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error)
}

// Handler is the receiving side of Transport. *Raft implements it.
type Handler interface {
	HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse
	HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse
	HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse
	HandleTimeoutNow(req *pb.TimeoutNowRequest) *pb.TimeoutNowResponse
}

// Storage makes Raft's persistent state durable. Every method must only return
// once the data would survive a crash.
type Storage interface {
	// Load returns everything that was saved before the last shutdown or crash.
	Load() (*Loaded, error)
	// Save persists the hard state (if non-nil) and appends entries (if any)
	// with a single flush. An entry whose index already exists replaces that
	// entry and everything after it.
	Save(hs *pb.HardState, entries []*pb.Entry) error
	// SaveSnapshot persists a snapshot and lets the storage drop log entries
	// up to meta.LastIncludedIndex. If discardLog is true the whole log is
	// dropped, atomically with the snapshot becoming visible.
	SaveSnapshot(meta *pb.SnapshotMeta, data []byte, discardLog bool) error
	// Snapshot returns the most recent snapshot, or nil meta if there is none.
	Snapshot() (*pb.SnapshotMeta, []byte, error)
	Close() error
}

// Loaded is the result of Storage.Load.
type Loaded struct {
	HardState    *pb.HardState
	SnapshotMeta *pb.SnapshotMeta // nil if no snapshot
	SnapshotData []byte
	Entries      []*pb.Entry // entries after the snapshot, contiguous
}

// Config configures one Raft node (one replica of one group).
type Config struct {
	ID    string
	Group uint32
	// Peers lists every member of the group, including this node.
	Peers []string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration

	// SnapshotThreshold is how many applied entries accumulate before the log
	// is compacted into a snapshot. 0 disables snapshots.
	SnapshotThreshold uint64
	// MaxAppendEntries caps the entries carried by one AppendEntries RPC.
	MaxAppendEntries int
	// MaxProposalBatch caps how many proposals share one disk flush.
	MaxProposalBatch int

	Storage      Storage
	Transport    Transport
	StateMachine StateMachine
	Logger       *slog.Logger

	// OnStateChange, if set, is called (without the Raft lock held) whenever
	// this node's role or known leader changes.
	OnStateChange func(Status)
}

func (c *Config) setDefaults() error {
	if c.ID == "" || len(c.Peers) == 0 {
		return errors.New("raft: ID and Peers are required")
	}
	found := false
	for _, p := range c.Peers {
		found = found || p == c.ID
	}
	if !found {
		return fmt.Errorf("raft: node %q is not in Peers %v", c.ID, c.Peers)
	}
	if c.Storage == nil || c.Transport == nil || c.StateMachine == nil {
		return errors.New("raft: Storage, Transport and StateMachine are required")
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeoutMin
	}
	if c.MaxAppendEntries == 0 {
		c.MaxAppendEntries = 512
	}
	if c.MaxProposalBatch == 0 {
		c.MaxProposalBatch = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// Status is a point-in-time view of a node, for metrics and debugging.
type Status struct {
	ID            string
	Group         uint32
	State         State
	Term          uint64
	Leader        string
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	SnapshotIndex uint64
	// MatchIndex is only populated on the leader: how far each follower's log
	// is known to match. LastLogIndex - MatchIndex[p] is p's replication lag.
	MatchIndex map[string]uint64
}

// NotLeaderError is returned when a request reaches a node that is not the
// leader, or that stopped being the leader before the request completed.
//
// In the second case the command may or may not have been committed. The
// caller must retry with the same request ID and let the state machine's
// deduplication make the retry safe.
type NotLeaderError struct {
	Leader string // best known leader, may be empty
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not leader (leader unknown)"
	}
	return "raft: not leader, try " + e.Leader
}

var (
	// ErrStopped is returned after Stop has been called.
	ErrStopped = errors.New("raft: stopped")
	// ErrNotReady is returned by ReadIndex when a new leader has not yet
	// committed an entry of its own term. It clears within one round trip.
	ErrNotReady = errors.New("raft: leader has not committed an entry in its term yet")
	// ErrTransferFailed is returned when a leadership transfer did not complete.
	ErrTransferFailed = errors.New("raft: leadership transfer failed")
)
