package raft

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// MemNetwork is an in-process network with fault injection. Tests use it to
// partition nodes, drop and delay messages, and take nodes offline, all
// deterministically and without real sockets.
//
// Every message is cloned on the way through, exactly as a real network would
// serialise it, so nodes can never share memory by accident.
type MemNetwork struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	down     map[string]bool    // node is unreachable in both directions
	blocked  map[[2]string]bool // directed link from -> to is cut
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
}

var errUnreachable = errors.New("memnet: unreachable")

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{
		handlers: make(map[string]Handler),
		down:     make(map[string]bool),
		blocked:  make(map[[2]string]bool),
	}
}

// Register attaches a node's handler. Registering again replaces the previous
// handler, which is how a restarted node rejoins.
func (n *MemNetwork) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Transport returns the Transport that node `from` should use.
func (n *MemNetwork) Transport(from string) Transport {
	return &memTransport{net: n, from: from}
}

// SetDown makes a node unreachable (true) or reachable again (false).
func (n *MemNetwork) SetDown(id string, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// Partition cuts every link between the two groups, in both directions.
func (n *MemNetwork) Partition(a, b []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, x := range a {
		for _, y := range b {
			n.blocked[[2]string{x, y}] = true
			n.blocked[[2]string{y, x}] = true
		}
	}
}

// Heal removes every partition.
func (n *MemNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[[2]string]bool)
}

// SetUnreliable makes the network drop a fraction of messages and delay the rest.
func (n *MemNetwork) SetUnreliable(dropRate float64, minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.minDelay, n.maxDelay = dropRate, minDelay, maxDelay
}

// deliver models one direction of a message: it may be dropped or delayed.
func (n *MemNetwork) deliver(ctx context.Context, from, to string) (Handler, error) {
	n.mu.RLock()
	h := n.handlers[to]
	cut := n.down[from] || n.down[to] || n.blocked[[2]string{from, to}]
	dropRate, minDelay, maxDelay := n.dropRate, n.minDelay, n.maxDelay
	n.mu.RUnlock()

	if h == nil || cut || (dropRate > 0 && rand.Float64() < dropRate) {
		// A lost message looks like silence: the caller waits for its timeout.
		<-ctx.Done()
		return nil, errUnreachable
	}
	if maxDelay > 0 {
		delay := minDelay + time.Duration(rand.Int64N(int64(maxDelay-minDelay)+1))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h, nil
}

// replyPath models the response travelling back; it can be lost as well, which
// is the interesting case: the receiver acted, but the sender never finds out.
func (n *MemNetwork) replyPath(ctx context.Context, from, to string) error {
	_, err := n.deliver(ctx, to, from)
	return err
}

type memTransport struct {
	net  *MemNetwork
	from string
}

func call[Req, Resp proto.Message](ctx context.Context, t *memTransport, to string, req Req, fn func(Handler, Req) Resp) (Resp, error) {
	var zero Resp
	h, err := t.net.deliver(ctx, t.from, to)
	if err != nil {
		return zero, err
	}
	resp := fn(h, proto.Clone(req).(Req))
	if err := t.net.replyPath(ctx, t.from, to); err != nil {
		return zero, err
	}
	return proto.Clone(resp).(Resp), nil
}

func (t *memTransport) RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return call(ctx, t, to, req, Handler.HandleRequestVote)
}

func (t *memTransport) AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return call(ctx, t, to, req, Handler.HandleAppendEntries)
}

func (t *memTransport) InstallSnapshot(ctx context.Context, to string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	return call(ctx, t, to, req, Handler.HandleInstallSnapshot)
}

func (t *memTransport) TimeoutNow(ctx context.Context, to string, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	return call(ctx, t, to, req, Handler.HandleTimeoutNow)
}
