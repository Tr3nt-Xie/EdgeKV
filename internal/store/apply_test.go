package store

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

func apply(t *testing.T, s *Store, index uint64, cmd *pb.Command) *pb.Result {
	t.Helper()
	data, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var res pb.Result
	if err := proto.Unmarshal(s.Apply(index, data), &res); err != nil {
		t.Fatal(err)
	}
	return &res
}

func TestApplyPutDelete(t *testing.T) {
	s := New()
	res := apply(t, s, 1, &pb.Command{Op: pb.Op_OP_PUT, Key: "k", Value: []byte("v"), TimestampUnixMs: 42})
	if res.Status != pb.Status_STATUS_OK || res.Version != 1 {
		t.Fatalf("put: got %v", res)
	}
	e := mustGet(t, s, "k")
	if e.UpdatedUnixMs != 42 {
		t.Fatalf("timestamp must come from the command, got %d", e.UpdatedUnixMs)
	}
	res = apply(t, s, 2, &pb.Command{Op: pb.Op_OP_DELETE, Key: "k"})
	if res.Status != pb.Status_STATUS_OK || res.Version != 2 {
		t.Fatalf("delete: got %v", res)
	}
	res = apply(t, s, 3, &pb.Command{Op: pb.Op_OP_DELETE, Key: "k"})
	if res.Status != pb.Status_STATUS_NOT_FOUND {
		t.Fatalf("second delete: got %v, want NOT_FOUND", res)
	}
}

// A retry carries the same request ID. It must not execute twice, and it must
// get the original answer even though the state has moved on since.
func TestApplyDeduplicatesRetries(t *testing.T) {
	s := New()
	cas := &pb.Command{
		Op: pb.Op_OP_PUT, Key: "lease", Value: []byte("worker-1"),
		HasExpectedVersion: true, ExpectedVersion: 0, RequestId: "req-1",
	}
	first := apply(t, s, 1, cas)
	if first.Status != pb.Status_STATUS_OK || first.Version != 1 {
		t.Fatalf("first attempt: got %v", first)
	}

	// Without dedup this retry would fail with VERSION_MISMATCH, and the client
	// would wrongly conclude that it does not hold the lease.
	retry := apply(t, s, 2, cas)
	if retry.Status != pb.Status_STATUS_OK || retry.Version != 1 {
		t.Fatalf("retry: got %v, want the original OK v1", retry)
	}
	if e := mustGet(t, s, "lease"); e.Version != 1 {
		t.Fatalf("retry executed again: version is %d, want 1", e.Version)
	}
}

func TestDedupEvictionIsIndexBased(t *testing.T) {
	s := New()
	s.SetDedupWindow(10)
	put := &pb.Command{Op: pb.Op_OP_PUT, Key: "k", Value: []byte("v"), RequestId: "old"}
	apply(t, s, 1, put)

	// Still inside the window: duplicate.
	apply(t, s, 11, put)
	if e := mustGet(t, s, "k"); e.Version != 1 {
		t.Fatalf("inside window: version %d, want 1", e.Version)
	}
	// Past the window the ID has been forgotten, so it executes again. The
	// window therefore bounds how late a retry may arrive.
	apply(t, s, 12, put)
	if e := mustGet(t, s, "k"); e.Version != 2 {
		t.Fatalf("outside window: version %d, want 2", e.Version)
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := New()
	apply(t, s, 1, &pb.Command{Op: pb.Op_OP_PUT, Key: "a", Value: []byte("1"), Cacheable: true, TtlSeconds: 30, RequestId: "r1"})
	apply(t, s, 2, &pb.Command{Op: pb.Op_OP_PUT, Key: "b", Value: []byte("2"), RequestId: "r2"})
	apply(t, s, 3, &pb.Command{Op: pb.Op_OP_DELETE, Key: "b", RequestId: "r3"})

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := New()
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}

	e := mustGet(t, r, "a")
	if string(e.Value) != "1" || e.Version != 1 || !e.Cacheable || e.TTLSeconds != 30 {
		t.Fatalf("restored a = %+v", e)
	}
	// The tombstone must survive, otherwise b would restart at version 1.
	if v := r.Put("b", []byte("again")); v != 3 {
		t.Fatalf("Put on restored tombstone: version %d, want 3", v)
	}
	// The dedup table must survive, otherwise a retry after a restart would
	// execute a second time.
	res := apply(t, r, 4, &pb.Command{Op: pb.Op_OP_PUT, Key: "a", Value: []byte("dup"), RequestId: "r1"})
	if res.Version != 1 {
		t.Fatalf("retry after restore executed again: %v", res)
	}
	if e := mustGet(t, r, "a"); string(e.Value) != "1" {
		t.Fatalf("retry after restore changed the value to %q", e.Value)
	}
}

// Two replicas fed the same log must end in the same state.
func TestDeterminism(t *testing.T) {
	a, b := New(), New()
	cmds := []*pb.Command{
		{Op: pb.Op_OP_PUT, Key: "x", Value: []byte("1"), RequestId: "1"},
		{Op: pb.Op_OP_PUT, Key: "x", Value: []byte("2"), HasExpectedVersion: true, ExpectedVersion: 1, RequestId: "2"},
		{Op: pb.Op_OP_PUT, Key: "x", Value: []byte("3"), HasExpectedVersion: true, ExpectedVersion: 1, RequestId: "3"},
		{Op: pb.Op_OP_DELETE, Key: "x", RequestId: "4"},
		{Op: pb.Op_OP_PUT, Key: "x", Value: []byte("5"), RequestId: "2"}, // duplicate ID
	}
	for i, c := range cmds {
		ra := apply(t, a, uint64(i+1), c)
		rb := apply(t, b, uint64(i+1), c)
		if !proto.Equal(ra, rb) {
			t.Fatalf("cmd %d: replicas disagree: %v vs %v", i, ra, rb)
		}
	}
	if _, err := a.Get("x"); err != ErrNotFound {
		t.Fatalf("x should be deleted, got err %v", err)
	}
}
