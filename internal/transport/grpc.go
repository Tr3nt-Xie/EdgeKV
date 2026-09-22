// Package transport carries Raft RPCs between nodes over gRPC.
package transport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
)

// snapshotChunkSize keeps each message far below gRPC's 4 MiB default limit.
const snapshotChunkSize = 1 << 20

// Client implements raft.Transport. One Client is shared by every Raft group
// on a node, so all groups multiplex over a single HTTP/2 connection per peer.
type Client struct {
	addrs map[string]string // node ID -> host:port

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

var _ raft.Transport = (*Client)(nil)

// NewClient returns a transport that reaches peers at the given addresses.
func NewClient(addrs map[string]string) *Client {
	return &Client{addrs: addrs, conns: make(map[string]*grpc.ClientConn)}
}

func (c *Client) peer(to string) (pb.RaftClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[to]; ok {
		return pb.NewRaftClient(conn), nil
	}
	addr, ok := c.addrs[to]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %q", to)
	}
	// NewClient does not connect; gRPC dials lazily and reconnects with backoff
	// on its own, so a peer that is down now is simply retried later.
	//
	// Traffic is plaintext. Raft ports are only reachable inside the VPC
	// security group; mutual TLS would be the next step for a real deployment.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[to] = conn
	return pb.NewRaftClient(conn), nil
}

func (c *Client) RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	p, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	return p.RequestVote(ctx, req)
}

func (c *Client) AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	p, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	return p.AppendEntries(ctx, req)
}

func (c *Client) TimeoutNow(ctx context.Context, to string, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	p, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	return p.TimeoutNow(ctx, req)
}

// InstallSnapshot streams the snapshot in chunks. Raft itself sees one logical
// request; chunking is purely a transport concern.
func (c *Client) InstallSnapshot(ctx context.Context, to string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	p, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	stream, err := p.InstallSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	header := &pb.InstallSnapshotRequest{
		Group: req.Group, Term: req.Term, LeaderId: req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm,
	}
	if err := stream.Send(&pb.InstallSnapshotChunk{Header: header}); err != nil {
		return nil, err
	}
	for data := req.Data; len(data) > 0; {
		n := min(len(data), snapshotChunkSize)
		if err := stream.Send(&pb.InstallSnapshotChunk{Data: data[:n]}); err != nil {
			return nil, err
		}
		data = data[n:]
	}
	return stream.CloseAndRecv()
}

// Close tears down all connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conn := range c.conns {
		conn.Close()
		delete(c.conns, id)
	}
}

// Server receives Raft RPCs and hands each to the Raft group it names.
type Server struct {
	pb.UnimplementedRaftServer
	// Resolve returns the handler for a group, or nil if this node does not
	// host it.
	Resolve func(group uint32) raft.Handler
}

func (s *Server) handler(group uint32) (raft.Handler, error) {
	if h := s.Resolve(group); h != nil {
		return h, nil
	}
	return nil, status.Errorf(codes.NotFound, "raft group %d is not hosted here", group)
}

func (s *Server) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	h, err := s.handler(req.Group)
	if err != nil {
		return nil, err
	}
	return h.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	h, err := s.handler(req.Group)
	if err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(req), nil
}

func (s *Server) TimeoutNow(_ context.Context, req *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	h, err := s.handler(req.Group)
	if err != nil {
		return nil, err
	}
	return h.HandleTimeoutNow(req), nil
}

func (s *Server) InstallSnapshot(stream grpc.ClientStreamingServer[pb.InstallSnapshotChunk, pb.InstallSnapshotResponse]) error {
	var req *pb.InstallSnapshotRequest
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunk.Header != nil {
			req = chunk.Header
		}
		if req == nil {
			return status.Error(codes.InvalidArgument, "snapshot stream did not start with a header")
		}
		req.Data = append(req.Data, chunk.Data...)
	}
	if req == nil {
		return status.Error(codes.InvalidArgument, "empty snapshot stream")
	}
	h, err := s.handler(req.Group)
	if err != nil {
		return err
	}
	return stream.SendAndClose(h.HandleInstallSnapshot(req))
}
