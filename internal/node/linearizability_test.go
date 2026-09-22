package node

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
	"github.com/Tr3nt-Xie/EdgeKV/internal/store"
)

// ---------------------------------------------------------------------------
// Test cluster: real Node objects, real on-disk storage, in-memory network.
// ---------------------------------------------------------------------------

type testCluster struct {
	t     *testing.T
	net   *raft.MemNetwork
	ids   []string
	dirs  map[string]string
	mu    sync.RWMutex
	nodes map[string]*Node
}

func newTestCluster(t *testing.T, size, shards int) *testCluster {
	c := &testCluster{t: t, net: raft.NewMemNetwork(), dirs: map[string]string{}, nodes: map[string]*Node{}}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.dirs[id] = t.TempDir()
	}
	for _, id := range c.ids {
		c.start(id, shards)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id string, shards int) {
	n, err := New(Config{
		ID: id, Peers: c.ids, NumShards: shards, DataDir: c.dirs[id],
		Transport:          c.net.Transport(id),
		SnapshotThreshold:  40,
		WALSegmentSize:     8 << 10,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	c.net.Register(id, n.Dispatcher())
	c.net.SetDown(id, false)
	n.Start()
}

func (c *testCluster) crash(id string) {
	c.net.SetDown(id, true)
	c.mu.RLock()
	n := c.nodes[id]
	c.mu.RUnlock()
	n.Stop()
}

func (c *testCluster) node(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

// leaderOf returns the ID of the node that leads the shard owning key.
func (c *testCluster) leaderOf(key string) string {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if c.node(id).ShardFor(key).Raft.IsLeader() {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader for %q", key)
	return ""
}

// ---------------------------------------------------------------------------
// A client that behaves like the real one: follow leader hints, retry with the
// same request ID, give up after a deadline.
// ---------------------------------------------------------------------------

type kvInput struct {
	op       string // "get", "put", "cas", "delete"
	key      string
	value    string
	expected uint64
}

type kvOutput struct {
	status  pb.Status
	value   string
	version uint64
	unknown bool // the client gave up; the operation may or may not have happened
}

func (c *testCluster) do(in kvInput, requestID string, budget time.Duration) kvOutput {
	deadline := time.Now().Add(budget)
	target := c.ids[rand.IntN(len(c.ids))]
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		out, err := c.attempt(ctx, target, in, requestID)
		cancel()
		if err == nil {
			return out
		}
		var nl *raft.NotLeaderError
		if errors.As(err, &nl) && nl.Leader != "" && nl.Leader != target {
			target = nl.Leader
		} else {
			target = c.ids[rand.IntN(len(c.ids))]
			time.Sleep(10 * time.Millisecond)
		}
	}
	return kvOutput{unknown: true}
}

func (c *testCluster) attempt(ctx context.Context, target string, in kvInput, requestID string) (kvOutput, error) {
	n := c.node(target)
	if in.op == "get" {
		e, err := n.Get(ctx, in.key)
		switch {
		case errors.Is(err, store.ErrNotFound):
			return kvOutput{status: pb.Status_STATUS_NOT_FOUND}, nil
		case err != nil:
			return kvOutput{}, err
		}
		return kvOutput{value: string(e.Value), version: e.Version}, nil
	}
	cmd := &pb.Command{Key: in.key, Value: []byte(in.value), RequestId: requestID}
	switch in.op {
	case "cas":
		cmd.HasExpectedVersion, cmd.ExpectedVersion = true, in.expected
	case "delete":
		cmd.Op = pb.Op_OP_DELETE
	}
	res, err := n.Write(ctx, cmd)
	if err != nil {
		return kvOutput{}, err
	}
	return kvOutput{status: res.Status, version: res.Version}, nil
}

// ---------------------------------------------------------------------------
// The sequential specification of one key. Porcupine searches for an order of
// the concurrent operations that (a) respects real time and (b) is legal
// according to this model. If none exists, the history is not linearizable.
// ---------------------------------------------------------------------------

type keyState struct {
	exists  bool
	value   string
	version uint64
}

var kvModel = porcupine.Model{
	// Keys are independent, so each can be checked on its own. This matters:
	// the search is exponential in the worst case.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return keyState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st, in, out := state.(keyState), input.(kvInput), output.(kvOutput)
		visible := uint64(0)
		if st.exists {
			visible = st.version
		}
		switch in.op {
		case "get":
			if !st.exists {
				return out.status == pb.Status_STATUS_NOT_FOUND, st
			}
			return out.status == pb.Status_STATUS_OK && out.value == st.value && out.version == st.version, st

		case "put", "cas":
			if in.op == "cas" && in.expected != visible {
				ok := out.unknown || (out.status == pb.Status_STATUS_VERSION_MISMATCH && out.version == visible)
				return ok, st
			}
			next := keyState{exists: true, value: in.value, version: st.version + 1}
			ok := out.unknown || (out.status == pb.Status_STATUS_OK && out.version == next.version)
			return ok, next

		case "delete":
			if !st.exists {
				return out.unknown || out.status == pb.Status_STATUS_NOT_FOUND, st
			}
			next := keyState{version: st.version + 1}
			return out.unknown || (out.status == pb.Status_STATUS_OK && out.version == next.version), next
		}
		return false, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in, out := input.(kvInput), output.(kvOutput)
		return fmt.Sprintf("%s(%s,%q,exp=%d) -> %+v", in.op, in.key, in.value, in.expected, out)
	},
}

type history struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (h *history) record(client int, in kvInput, call int64, out kvOutput) {
	ret := time.Now().UnixNano()
	if out.unknown {
		if in.op == "get" {
			return // a read that never returned constrains nothing
		}
		// A write with an unknown outcome might take effect at any later time,
		// so it stays "in flight" until the end of the history.
		ret = 1 << 62
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
}

func (h *history) check(t *testing.T) porcupine.CheckResult {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	res, _ := porcupine.CheckOperationsVerbose(kvModel, h.ops, 60*time.Second)
	return res
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const shards = 2
	c := newTestCluster(t, 3, shards)
	h := &history{}
	keys := []string{"task:1:owner", "task:2:owner", "agent:9:lease", "rate:user-7"}

	var clients sync.WaitGroup
	// Run for a fixed time rather than a fixed number of operations: the
	// workload has to overlap with several rounds of every kind of fault.
	const numClients, duration = 6, 9 * time.Second
	end := time.Now().Add(duration)
	for cl := 0; cl < numClients; cl++ {
		clients.Add(1)
		go func() {
			defer clients.Done()
			lastSeen := map[string]uint64{}
			for i := 0; time.Now().Before(end); i++ {
				time.Sleep(time.Duration(rand.IntN(4)) * time.Millisecond)
				key := keys[rand.IntN(len(keys))]
				in := kvInput{key: key, value: fmt.Sprintf("c%d-%d", cl, i)}
				switch r := rand.IntN(10); {
				case r < 4:
					in.op = "get"
				case r < 6:
					in.op = "put"
				case r < 9:
					in.op, in.expected = "cas", lastSeen[key]
				default:
					in.op = "delete"
				}
				call := time.Now().UnixNano()
				out := c.do(in, fmt.Sprintf("c%d-op%d", cl, i), 4*time.Second)
				h.record(cl, in, call, out)
				if !out.unknown && out.status != pb.Status_STATUS_NOT_FOUND {
					lastSeen[key] = out.version
				}
			}
		}()
	}

	stop := make(chan struct{})
	var chaos sync.WaitGroup
	chaos.Add(1)
	rounds := 0
	go func() {
		defer chaos.Done()
		for round := 0; ; round++ {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
			}
			rounds = round + 1
			victim := c.leaderOf(keys[round%len(keys)])
			switch round % 3 {
			case 0: // cut the leader off from everyone else
				var rest []string
				for _, id := range c.ids {
					if id != victim {
						rest = append(rest, id)
					}
				}
				c.net.Partition([]string{victim}, rest)
				time.Sleep(500 * time.Millisecond)
				c.net.Heal()
			case 1: // kill the leader process and bring it back from disk
				c.crash(victim)
				time.Sleep(400 * time.Millisecond)
				c.start(victim, shards)
			case 2: // lossy, slow network
				c.net.SetUnreliable(0.15, 0, 20*time.Millisecond)
				time.Sleep(500 * time.Millisecond)
				c.net.SetUnreliable(0, 0, 0)
			}
		}
	}()

	clients.Wait()
	close(stop)
	chaos.Wait()

	unknown := 0
	for _, op := range h.ops {
		if op.Output.(kvOutput).unknown {
			unknown++
		}
	}
	t.Logf("history: %d operations, %d with unknown outcome, %d fault rounds", len(h.ops), unknown, rounds)
	if rounds < 6 {
		t.Fatalf("only %d fault rounds ran; the workload did not overlap with enough faults", rounds)
	}
	if res := h.check(t); res != porcupine.Ok {
		t.Fatalf("history is not linearizable (result: %v)", res)
	}
}

// Negative control. If reads skip ReadIndex and trust a node that merely
// believes it is the leader, a partitioned old leader serves stale data. The
// checker must catch it — otherwise a green result above would prove nothing.
func TestCheckerDetectsStaleRead(t *testing.T) {
	c := newTestCluster(t, 3, 1)
	h := &history{}
	const key = "task:1:status"

	write := func(v string) {
		in := kvInput{op: "put", key: key, value: v}
		call := time.Now().UnixNano()
		out := c.do(in, "w-"+v, 5*time.Second)
		if out.unknown {
			t.Fatalf("write %q did not complete", v)
		}
		h.record(0, in, call, out)
	}

	write("pending")
	old := c.leaderOf(key)
	var rest []string
	for _, id := range c.ids {
		if id != old {
			rest = append(rest, id)
		}
	}

	// Wait until the old leader has applied "pending", then isolate it.
	for {
		if e, err := c.node(old).ShardFor(key).Store.Get(key); err == nil && string(e.Value) == "pending" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.net.Partition([]string{old}, rest)

	// The majority elects a new leader and commits a newer value.
	for {
		in := kvInput{op: "put", key: key, value: "running"}
		call := time.Now().UnixNano()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		out, err := c.attempt(ctx, rest[rand.IntN(len(rest))], in, "w-running")
		cancel()
		if err == nil {
			h.record(0, in, call, out)
			break
		}
	}

	// The unsafe read: straight from the old leader's local state.
	in := kvInput{op: "get", key: key}
	call := time.Now().UnixNano()
	e, err := c.node(old).ShardFor(key).Store.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	h.record(1, in, call, kvOutput{value: string(e.Value), version: e.Version})
	if string(e.Value) != "pending" {
		t.Fatalf("expected the isolated node to still hold the old value, got %q", e.Value)
	}
	if res := h.check(t); res != porcupine.Illegal {
		t.Fatalf("checker accepted a stale read (result: %v)", res)
	}

	// The safe read on the same node refuses instead of lying.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.node(old).Get(ctx, key); err == nil {
		t.Fatal("linearizable Get succeeded on a partitioned former leader")
	}
}
