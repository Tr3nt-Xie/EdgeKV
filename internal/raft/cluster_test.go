package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// testSM is a state machine that just remembers every command it applied.
type testSM struct {
	mu      sync.Mutex
	applied []string
}

func (s *testSM) Apply(index uint64, data []byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, string(data))
	return []byte(fmt.Sprintf("%d", index))
}

func (s *testSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.applied)
}

func (s *testSM) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = nil
	return json.Unmarshal(data, &s.applied)
}

func (s *testSM) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.applied)
}

type cluster struct {
	t        *testing.T
	net      *MemNetwork
	ids      []string
	nodes    map[string]*Raft
	sms      map[string]*testSM
	storages map[string]*MemStorage
	snapshot uint64
}

func newCluster(t *testing.T, n int, snapshotThreshold uint64) *cluster {
	c := &cluster{
		t: t, net: NewMemNetwork(), snapshot: snapshotThreshold,
		nodes: make(map[string]*Raft), sms: make(map[string]*testSM), storages: make(map[string]*MemStorage),
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.storages[id] = NewMemStorage()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, r := range c.nodes {
			r.Stop()
		}
	})
	return c
}

// start boots a node from whatever its storage holds — i.e. also a restart.
func (c *cluster) start(id string) {
	c.sms[id] = &testSM{}
	r, err := New(Config{
		ID: id, Peers: c.ids,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
		SnapshotThreshold:  c.snapshot,
		MaxAppendEntries:   64,
		Storage:            c.storages[id],
		Transport:          c.net.Transport(id),
		StateMachine:       c.sms[id],
	})
	if err != nil {
		c.t.Fatalf("New(%s): %v", id, err)
	}
	c.nodes[id] = r
	c.net.Register(id, r)
	c.net.SetDown(id, false)
	r.Start()
}

// crash stops a node and throws away everything that was only in memory.
func (c *cluster) crash(id string) {
	c.net.SetDown(id, true)
	c.nodes[id].Stop()
}

func (c *cluster) leaders() []*Raft {
	var out []*Raft
	for _, r := range c.nodes {
		if st := r.Status(); st.State == Leader {
			out = append(out, r)
		}
	}
	return out
}

// waitLeader waits until exactly one of the given nodes is leader and returns it.
func (c *cluster) waitLeader(among ...string) *Raft {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var found []*Raft
		for _, id := range among {
			if c.nodes[id].IsLeader() {
				found = append(found, c.nodes[id])
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v", among)
	return nil
}

// propose submits a command, following leader changes until it is applied.
func (c *cluster) propose(cmd string, among ...string) {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range among {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := c.nodes[id].Propose(ctx, []byte(cmd))
			cancel()
			if err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("could not commit %q", cmd)
}

// waitApplied waits until each given node has applied at least n commands.
func (c *cluster) waitApplied(n int, ids ...string) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.ids
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			ok = ok && len(c.sms[id].log()) >= n
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s applied %d, status %+v", id, len(c.sms[id].log()), c.nodes[id].Status())
	}
	c.t.Fatalf("nodes %v did not apply %d commands in time", ids, n)
}

// checkConsistent asserts State Machine Safety: no two nodes applied different
// commands at the same position.
func (c *cluster) checkConsistent() {
	c.t.Helper()
	var ref []string
	var refID string
	for _, id := range c.ids {
		got := c.sms[id].log()
		n := min(len(ref), len(got))
		if !slices.Equal(ref[:n], got[:n]) {
			c.t.Fatalf("state machines diverged:\n%s: %v\n%s: %v", refID, ref, id, got)
		}
		if len(got) > len(ref) {
			ref, refID = got, id
		}
	}
}

func others(all []string, exclude ...string) []string {
	var out []string
	for _, id := range all {
		if !slices.Contains(exclude, id) {
			out = append(out, id)
		}
	}
	return out
}

func isNotLeader(err error) bool {
	var nl *NotLeaderError
	return errors.As(err, &nl)
}
