// Package node hosts one replica of every shard on a single process
// (Multi-Raft) and exposes key-value operations on top of them.
//
// Each shard is an independent Raft group with its own leader, term, log, WAL
// directory and state machine. Writes to keys in different shards are ordered
// by different leaders and hit different logs, which is what lets write
// throughput scale past a single Raft group.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raftstore"
	"github.com/Tr3nt-Xie/EdgeKV/internal/shard"
	"github.com/Tr3nt-Xie/EdgeKV/internal/store"
)

// Config configures a Node.
type Config struct {
	ID        string
	Peers     []string // every node ID in the cluster, including this one
	NumShards int
	DataDir   string
	Transport raft.Transport

	// Sync controls fsync on the Raft log. Must be true outside benchmarks.
	Sync              bool
	SnapshotThreshold uint64
	WALSegmentSize    int64

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// RebalanceInterval is how often leaders are moved back to their preferred
	// node. 0 disables rebalancing.
	RebalanceInterval time.Duration

	Logger *slog.Logger
	// OnLeaderChange is called whenever a shard on this node sees a new leader.
	OnLeaderChange func(shardID uint32, st raft.Status)
}

// Shard is this node's replica of one Raft group.
type Shard struct {
	ID      uint32
	Raft    *raft.Raft
	Store   *store.Store
	storage *raftstore.Store
}

// Node is a process-level container for shard replicas.
type Node struct {
	cfg    Config
	peers  []string // sorted, so every node computes the same preferred leaders
	shards []*Shard
	log    *slog.Logger

	stopCh chan struct{}
	done   chan struct{}
}

// New opens the storage of every shard and restores their state. Nothing runs
// until Start is called.
func New(cfg Config) (*Node, error) {
	if cfg.NumShards <= 0 {
		return nil, errors.New("node: NumShards must be positive")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	n := &Node{
		cfg: cfg, log: cfg.Logger,
		peers:  append([]string(nil), cfg.Peers...),
		stopCh: make(chan struct{}), done: make(chan struct{}),
	}
	sort.Strings(n.peers)

	for i := 0; i < cfg.NumShards; i++ {
		id := uint32(i)
		storage, err := raftstore.Open(
			filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", i)),
			raftstore.Options{Sync: cfg.Sync, SegmentSize: cfg.WALSegmentSize},
		)
		if err != nil {
			n.closeStorage()
			return nil, fmt.Errorf("node: open shard %d: %w", i, err)
		}
		sm := store.New()
		r, err := raft.New(raft.Config{
			ID: cfg.ID, Group: id, Peers: cfg.Peers,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			SnapshotThreshold:  cfg.SnapshotThreshold,
			Storage:            storage,
			Transport:          cfg.Transport,
			StateMachine:       sm,
			Logger:             cfg.Logger,
			OnStateChange: func(st raft.Status) {
				if cfg.OnLeaderChange != nil {
					cfg.OnLeaderChange(id, st)
				}
			},
		})
		if err != nil {
			storage.Close()
			n.closeStorage()
			return nil, fmt.Errorf("node: start shard %d: %w", i, err)
		}
		n.shards = append(n.shards, &Shard{ID: id, Raft: r, Store: sm, storage: storage})
	}
	return n, nil
}

// Start makes every shard replica join its Raft group.
func (n *Node) Start() {
	for _, s := range n.shards {
		s.Raft.Start()
	}
	go n.runRebalancer()
}

// Stop shuts all shards down and closes their storage.
func (n *Node) Stop() {
	close(n.stopCh)
	<-n.done
	for _, s := range n.shards {
		s.Raft.Stop()
	}
	n.closeStorage()
}

func (n *Node) closeStorage() {
	for _, s := range n.shards {
		s.storage.Close()
	}
}

// Handler returns the Raft RPC handler of a group, or nil if unknown. It is
// what the gRPC server uses to route an incoming RPC to the right group.
func (n *Node) Handler(group uint32) raft.Handler {
	if int(group) >= len(n.shards) {
		return nil
	}
	return n.shards[group].Raft
}

// NumShards returns the number of shards.
func (n *Node) NumShards() int { return len(n.shards) }

// Shards returns every shard replica on this node.
func (n *Node) Shards() []*Shard { return n.shards }

// ShardFor returns the shard replica that owns key.
func (n *Node) ShardFor(key string) *Shard {
	return n.shards[shard.ForKey(key, len(n.shards))]
}

// Write replicates a command through the shard that owns its key and returns
// the state machine's verdict. It fails with *raft.NotLeaderError if this node
// does not lead that shard.
func (n *Node) Write(ctx context.Context, cmd *pb.Command) (*pb.Result, error) {
	// The leader decides the timestamp once; replicas copy it from the log.
	// Reading the clock inside Apply would give every replica a different value.
	cmd.TimestampUnixMs = time.Now().UnixMilli()
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	out, err := n.ShardFor(cmd.Key).Raft.Propose(ctx, data)
	if err != nil {
		return nil, err
	}
	res := &pb.Result{}
	if err := proto.Unmarshal(out, res); err != nil {
		return nil, err
	}
	return res, nil
}

// Get performs a linearizable read.
func (n *Node) Get(ctx context.Context, key string) (store.Entry, error) {
	s := n.ShardFor(key)
	// A new leader needs one round trip to commit its no-op before it can
	// serve reads. Wait that out here instead of bothering the client.
	for {
		err := s.Raft.ReadIndex(ctx)
		if err == nil {
			break
		}
		if !errors.Is(err, raft.ErrNotReady) {
			return store.Entry{}, err
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return store.Entry{}, ctx.Err()
		}
	}
	return s.Store.Get(key)
}

// PreferredLeader returns the node that should lead a shard when everything is
// healthy. Spreading leaders is what turns extra shards into extra throughput:
// with all leaders on one node, that node's disk and CPU stay the bottleneck.
func (n *Node) PreferredLeader(shardID uint32) string {
	return n.peers[int(shardID)%len(n.peers)]
}

// WALSize returns the on-disk size of a shard's log.
func (s *Shard) WALSize() int64 { return s.storage.WALSize() }

// runRebalancer hands leadership back to the preferred node. After a failover
// one node can end up leading every shard; this undoes that once the failed
// node is healthy again.
func (n *Node) runRebalancer() {
	defer close(n.done)
	if n.cfg.RebalanceInterval <= 0 {
		<-n.stopCh
		return
	}
	ticker := time.NewTicker(n.cfg.RebalanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}
		for _, s := range n.shards {
			preferred := n.PreferredLeader(s.ID)
			st := s.Raft.Status()
			if st.State != raft.Leader || preferred == n.cfg.ID {
				continue
			}
			// Only hand over to a node that is alive and nearly caught up;
			// otherwise the transfer would stall writes for nothing.
			match, ok := st.MatchIndex[preferred]
			if !ok || st.LastLogIndex-match > 64 {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := s.Raft.TransferLeadership(ctx, preferred)
			cancel()
			n.log.Info("rebalance: leadership transfer", "shard", s.ID, "to", preferred, "err", err)
		}
	}
}
