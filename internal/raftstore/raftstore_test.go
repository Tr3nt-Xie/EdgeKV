package raftstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
)

func entries(term uint64, from, to uint64) []*pb.Entry {
	var out []*pb.Entry
	for i := from; i <= to; i++ {
		out = append(out, &pb.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("t%d-i%d", term, i))})
	}
	return out
}

func reopen(t *testing.T, s *Store, dir string, opts Options) (*Store, *raft.Loaded) {
	t.Helper()
	if s != nil {
		s.Close()
	}
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, loaded
}

func TestSaveAndLoad(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: true}
	s, loaded := reopen(t, nil, dir, opts)
	if loaded.HardState != nil || len(loaded.Entries) != 0 {
		t.Fatalf("fresh store is not empty: %+v", loaded)
	}
	s.Save(&pb.HardState{Term: 3, VotedFor: "n2"}, entries(3, 1, 5))
	s.Save(&pb.HardState{Term: 4, VotedFor: "n1"}, nil)

	_, loaded = reopen(t, s, dir, opts)
	if loaded.HardState.Term != 4 || loaded.HardState.VotedFor != "n1" {
		t.Fatalf("hard state = %+v, want the last one written", loaded.HardState)
	}
	if len(loaded.Entries) != 5 || loaded.Entries[4].Index != 5 {
		t.Fatalf("got %d entries", len(loaded.Entries))
	}
}

// A follower that overwrites a conflicting suffix only ever appends to the
// WAL. Replay must still end up with the truncated log.
func TestOverwriteSuffix(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: true}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(nil, entries(1, 1, 10))
	s.Save(nil, entries(2, 6, 7)) // conflict at 6: entries 6..10 of term 1 are gone

	_, loaded := reopen(t, s, dir, opts)
	if len(loaded.Entries) != 7 {
		t.Fatalf("got %d entries, want 7", len(loaded.Entries))
	}
	for _, e := range loaded.Entries {
		want := uint64(1)
		if e.Index >= 6 {
			want = 2
		}
		if e.Term != want {
			t.Fatalf("index %d has term %d, want %d", e.Index, e.Term, want)
		}
	}
}

func TestSnapshotCompactsWAL(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: true, SegmentSize: 512}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(&pb.HardState{Term: 1, VotedFor: "n1"}, nil)
	for i := uint64(1); i <= 100; i++ {
		s.Save(nil, entries(1, i, i))
	}
	before := s.WALSize()
	segsBefore := len(s.wal.Segments())
	if segsBefore < 5 {
		t.Fatalf("expected many segments with a 512 byte limit, got %d", segsBefore)
	}

	if err := s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: 90, LastIncludedTerm: 1}, []byte("state@90"), false); err != nil {
		t.Fatal(err)
	}
	if after := s.WALSize(); after >= before/2 {
		t.Fatalf("WAL did not shrink: %d -> %d bytes", before, after)
	}

	_, loaded := reopen(t, s, dir, opts)
	if loaded.SnapshotMeta.LastIncludedIndex != 90 || string(loaded.SnapshotData) != "state@90" {
		t.Fatalf("snapshot = %+v %q", loaded.SnapshotMeta, loaded.SnapshotData)
	}
	if len(loaded.Entries) != 10 || loaded.Entries[0].Index != 91 {
		t.Fatalf("got %d entries starting at %d, want 10 starting at 91", len(loaded.Entries), loaded.Entries[0].Index)
	}
	// The vote was written into the very first segment, which is gone now. It
	// must have been carried forward on every rotation.
	if loaded.HardState == nil || loaded.HardState.VotedFor != "n1" {
		t.Fatalf("hard state lost in compaction: %+v", loaded.HardState)
	}
}

func TestSnapshotDiscardLog(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: true}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(&pb.HardState{Term: 2}, entries(1, 1, 10)) // a stale, divergent log

	if err := s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: 50, LastIncludedTerm: 2}, []byte("state@50"), true); err != nil {
		t.Fatal(err)
	}
	s.Save(nil, entries(2, 51, 52))

	_, loaded := reopen(t, s, dir, opts)
	if len(loaded.Entries) != 2 || loaded.Entries[0].Index != 51 {
		t.Fatalf("entries after discard = %v", loaded.Entries)
	}
	if loaded.HardState == nil || loaded.HardState.Term != 2 {
		t.Fatalf("hard state = %+v", loaded.HardState)
	}
}

func TestStaleSnapshotIsIgnored(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: true}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(nil, entries(1, 1, 20))
	s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: 15, LastIncludedTerm: 1}, []byte("new"), false)
	s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 1}, []byte("old"), false)

	_, loaded := reopen(t, s, dir, opts)
	if string(loaded.SnapshotData) != "new" {
		t.Fatalf("snapshot = %q, want \"new\"", loaded.SnapshotData)
	}
}

// ---------------------------------------------------------------------------
// End to end: a Raft group on real files, killed and restarted.
// ---------------------------------------------------------------------------

type listSM struct {
	mu      sync.Mutex
	applied []string
}

func (s *listSM) Apply(_ uint64, data []byte) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, string(data))
	return nil
}
func (s *listSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.applied)
}
func (s *listSM) Restore(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = nil
	return json.Unmarshal(b, &s.applied)
}
func (s *listSM) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.applied)
}

func TestRaftOnDiskSurvivesRestarts(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	net := raft.NewMemNetwork()
	dirs := map[string]string{}
	for _, id := range ids {
		dirs[id] = t.TempDir()
	}
	nodes := map[string]*raft.Raft{}
	stores := map[string]*Store{}
	sms := map[string]*listSM{}

	start := func(id string) {
		st, err := Open(dirs[id], Options{Sync: false, SegmentSize: 2048})
		if err != nil {
			t.Fatal(err)
		}
		sms[id] = &listSM{}
		r, err := raft.New(raft.Config{
			ID: id, Peers: ids,
			HeartbeatInterval: 20 * time.Millisecond, ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond, RPCTimeout: 100 * time.Millisecond,
			SnapshotThreshold: 25,
			Storage:           st, Transport: net.Transport(id), StateMachine: sms[id],
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id], stores[id] = r, st
		net.Register(id, r)
		net.SetDown(id, false)
		r.Start()
	}
	stop := func(id string) {
		net.SetDown(id, true)
		nodes[id].Stop()
		stores[id].Close()
	}
	propose := func(cmd string) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			for _, id := range ids {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := nodes[id].Propose(ctx, []byte(cmd))
				cancel()
				if err == nil {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("could not commit %q", cmd)
	}
	waitApplied := func(n int) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			ok := true
			for _, id := range ids {
				ok = ok && len(sms[id].log()) >= n
			}
			if ok {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("not all nodes applied %d commands", n)
	}

	for _, id := range ids {
		start(id)
	}
	defer func() {
		for _, id := range ids {
			stop(id)
		}
	}()

	total := 0
	for round := 0; round < 3; round++ {
		for i := 0; i < 40; i++ {
			propose(fmt.Sprintf("r%d-%d", round, i))
			total++
		}
		waitApplied(total)
		// Kill one node this round, all of them the next: both a follower
		// catching up and a cold start of the whole group get exercised.
		victims := []string{ids[round%3]}
		if round == 1 {
			victims = ids
		}
		for _, id := range victims {
			stop(id)
		}
		for _, id := range victims {
			start(id)
		}
	}
	propose("final")
	total++
	waitApplied(total)

	ref := sms["n1"].log()
	for _, id := range ids {
		if got := sms[id].log(); !slices.Equal(got, ref) {
			t.Fatalf("%s diverged from n1", id)
		}
	}
}

// Two snapshots in flight at once (the applier's own and one installed from
// the leader) must never leave the store in a state where the log has been
// compacted but no snapshot file survives. Regression test for a bug found by
// the linearizability chaos test.
func TestConcurrentSnapshotsNeverLoseTheSnapshot(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: false, SegmentSize: 1024}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(&pb.HardState{Term: 1}, entries(1, 1, 200))

	var wg sync.WaitGroup
	for round := 0; round < 20; round++ {
		base := uint64(10 + round*8)
		for _, idx := range []uint64{base, base + 3, base + 5} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: idx, LastIncludedTerm: 1}, []byte(fmt.Sprintf("state@%d", idx)), false)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Save(nil, entries(1, 201+uint64(round), 201+uint64(round)))
		}()
		wg.Wait()

		// Whatever interleaving happened, a reload must find a snapshot whose
		// index is at least the highest one saved, followed by a contiguous log.
		_, loaded := reopen(t, s, dir, opts)
		if loaded.SnapshotMeta == nil {
			t.Fatalf("round %d: no snapshot on disk after concurrent SaveSnapshot", round)
		}
		if loaded.SnapshotMeta.LastIncludedIndex < base+5 {
			t.Fatalf("round %d: snapshot index %d, want >= %d", round, loaded.SnapshotMeta.LastIncludedIndex, base+5)
		}
		if n := len(loaded.Entries); n > 0 && loaded.Entries[0].Index != loaded.SnapshotMeta.LastIncludedIndex+1 {
			t.Fatalf("round %d: gap between snapshot %d and first entry %d", round, loaded.SnapshotMeta.LastIncludedIndex, loaded.Entries[0].Index)
		}
		s, _ = reopen(t, nil, dir, opts)
	}
}

// A Save that rotates the WAL (and therefore runs GC) while a SaveSnapshot is
// between renaming its file and publishing it in s.meta must not delete that
// newer snapshot. Regression test: found by the linearizability chaos test as
// "gap in log: have 0 entries after snapshot 0".
func TestRotationGCDoesNotDeleteNewerSnapshot(t *testing.T) {
	dir, opts := t.TempDir(), Options{Sync: false, SegmentSize: 512}
	s, _ := reopen(t, nil, dir, opts)
	s.Save(&pb.HardState{Term: 1}, entries(1, 1, 50))
	if err := s.SaveSnapshot(&pb.SnapshotMeta{LastIncludedIndex: 20, LastIncludedTerm: 1}, []byte("old"), false); err != nil {
		t.Fatal(err)
	}

	// Simulate the window inside SaveSnapshot(40): the file is already on disk
	// under its final name, but s.meta still says 20.
	newer := &pb.SnapshotMeta{LastIncludedIndex: 40, LastIncludedTerm: 1}
	if err := s.writeSnapshotFile(newer, []byte("new")); err != nil {
		t.Fatal(err)
	}
	// Now a Save large enough to rotate, which runs gcLocked with meta=20.
	big := make([]*pb.Entry, 0, 20)
	for i := uint64(51); i <= 70; i++ {
		big = append(big, &pb.Entry{Index: i, Term: 1, Data: make([]byte, 100)})
	}
	if err := s.Save(nil, big); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.snapshotPath(newer)); err != nil {
		t.Fatalf("rotation GC deleted the newer snapshot file: %v", err)
	}
	// Finish the SaveSnapshot(40): it must publish and be what a reload finds.
	if err := s.SaveSnapshot(newer, []byte("new"), false); err != nil {
		t.Fatal(err)
	}
	_, loaded := reopen(t, s, dir, opts)
	if loaded.SnapshotMeta == nil || loaded.SnapshotMeta.LastIncludedIndex != 40 || string(loaded.SnapshotData) != "new" {
		t.Fatalf("reload: snapshot = %v %q, want index 40 \"new\"", loaded.SnapshotMeta, loaded.SnapshotData)
	}
}
