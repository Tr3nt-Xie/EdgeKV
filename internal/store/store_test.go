package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func mustGet(t *testing.T, s *Store, key string) Entry {
	t.Helper()
	e, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): unexpected error: %v", key, err)
	}
	return e
}

func TestGetMissing(t *testing.T) {
	s := New()
	if _, err := s.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on missing key: got err %v, want ErrNotFound", err)
	}
}

func TestPutThenGet(t *testing.T) {
	s := New()
	if v := s.Put("task:1:status", []byte("pending")); v != 1 {
		t.Fatalf("first Put: got version %d, want 1", v)
	}
	e := mustGet(t, s, "task:1:status")
	if string(e.Value) != "pending" || e.Version != 1 {
		t.Fatalf("got %q v%d, want \"pending\" v1", e.Value, e.Version)
	}
}

func TestVersionIncrementsPerKey(t *testing.T) {
	s := New()
	s.Put("a", []byte("1"))
	s.Put("a", []byte("2"))
	if v := s.Put("a", []byte("3")); v != 3 {
		t.Fatalf("third Put on a: got version %d, want 3", v)
	}
	if v := s.Put("b", []byte("x")); v != 1 {
		t.Fatalf("first Put on b: got version %d, want 1 (versions are per key)", v)
	}
}

func TestPutCopiesValue(t *testing.T) {
	s := New()
	buf := []byte("hello")
	s.Put("k", buf)
	buf[0] = 'J' // caller mutates its own slice after Put
	if e := mustGet(t, s, "k"); string(e.Value) != "hello" {
		t.Fatalf("store aliased the caller's slice: got %q, want \"hello\"", e.Value)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := New()
	s.Put("k", []byte("hello"))
	e := mustGet(t, s, "k")
	e.Value[0] = 'J'
	if e2 := mustGet(t, s, "k"); string(e2.Value) != "hello" {
		t.Fatalf("Get leaked internal slice: got %q, want \"hello\"", e2.Value)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Put("k", []byte("v"))
	v, err := s.Delete("k")
	if err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if v != 2 {
		t.Fatalf("Delete: got tombstone version %d, want 2", v)
	}
	if _, err := s.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete: got err %v, want ErrNotFound", err)
	}
}

func TestDeleteMissing(t *testing.T) {
	s := New()
	if _, err := s.Delete("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete on missing key: got err %v, want ErrNotFound", err)
	}
	s.Put("k", []byte("v"))
	s.Delete("k")
	if _, err := s.Delete("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: got err %v, want ErrNotFound", err)
	}
}

// A re-created key must continue from the tombstone's version. If versions
// restarted at 1, a client holding a stale "version 1" could win a CAS against
// a completely different incarnation of the key (the ABA problem).
func TestVersionSurvivesDelete(t *testing.T) {
	s := New()
	s.Put("k", []byte("old")) // v1
	s.Delete("k")             // v2
	if v := s.Put("k", []byte("new")); v != 3 {
		t.Fatalf("Put after Delete: got version %d, want 3", v)
	}
	if _, err := s.CAS("k", []byte("stale"), 1); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("CAS with pre-delete version: got err %v, want ErrVersionMismatch", err)
	}
}

func TestCAS(t *testing.T) {
	s := New()
	s.Put("k", []byte("a")) // v1

	v, err := s.CAS("k", []byte("b"), 1)
	if err != nil || v != 2 {
		t.Fatalf("CAS with correct version: got v%d err %v, want v2 nil", v, err)
	}

	if _, err := s.CAS("k", []byte("c"), 1); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("CAS with stale version: got err %v, want ErrVersionMismatch", err)
	}
	if e := mustGet(t, s, "k"); string(e.Value) != "b" || e.Version != 2 {
		t.Fatalf("failed CAS must not change state: got %q v%d, want \"b\" v2", e.Value, e.Version)
	}
}

func TestCASCreateIfAbsent(t *testing.T) {
	s := New()

	v, err := s.CAS("lease", []byte("worker-1"), 0)
	if err != nil || v != 1 {
		t.Fatalf("CAS(0) on missing key: got v%d err %v, want v1 nil", v, err)
	}
	if _, err := s.CAS("lease", []byte("worker-2"), 0); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("CAS(0) on existing key: got err %v, want ErrVersionMismatch", err)
	}

	s.Delete("lease") // v2
	if _, err := s.CAS("lease", []byte("worker-2"), 2); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("CAS(tombstone version) on deleted key: got err %v, want ErrVersionMismatch", err)
	}
	v, err = s.CAS("lease", []byte("worker-2"), 0)
	if err != nil || v != 3 {
		t.Fatalf("CAS(0) on deleted key: got v%d err %v, want v3 nil", v, err)
	}
}

// 50 workers race to grab the same lease. Exactly one may win.
// Run with: go test -race ./...
func TestConcurrentCASSingleWinner(t *testing.T) {
	s := New()
	const workers = 50
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.CAS("lease", []byte("mine"), 0); err == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("got %d CAS winners, want exactly 1", got)
	}
}

func TestConcurrentPutNoLostUpdates(t *testing.T) {
	s := New()
	const workers, perWorker = 8, 1000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				s.Put("counter", []byte("x"))
			}
		}()
	}
	wg.Wait()
	if e := mustGet(t, s, "counter"); e.Version != workers*perWorker {
		t.Fatalf("got version %d, want %d (lost updates?)", e.Version, workers*perWorker)
	}
}
