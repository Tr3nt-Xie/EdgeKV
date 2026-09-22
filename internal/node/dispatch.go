package node

import (
	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
)

// Dispatcher is a raft.Handler for the whole node: it looks at the group number
// inside each request and forwards it to that shard's Raft instance. The
// in-memory test network uses it; the gRPC server does the same job through
// transport.Server.Resolve.
type Dispatcher struct{ n *Node }

// Dispatcher returns a handler that routes by group.
func (n *Node) Dispatcher() raft.Handler { return &Dispatcher{n: n} }

func (d *Dispatcher) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	if h := d.n.Handler(req.Group); h != nil {
		return h.HandleRequestVote(req)
	}
	return &pb.RequestVoteResponse{}
}

func (d *Dispatcher) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	if h := d.n.Handler(req.Group); h != nil {
		return h.HandleAppendEntries(req)
	}
	return &pb.AppendEntriesResponse{}
}

func (d *Dispatcher) HandleInstallSnapshot(req *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	if h := d.n.Handler(req.Group); h != nil {
		return h.HandleInstallSnapshot(req)
	}
	return &pb.InstallSnapshotResponse{}
}

func (d *Dispatcher) HandleTimeoutNow(req *pb.TimeoutNowRequest) *pb.TimeoutNowResponse {
	if h := d.n.Handler(req.Group); h != nil {
		return h.HandleTimeoutNow(req)
	}
	return &pb.TimeoutNowResponse{}
}
