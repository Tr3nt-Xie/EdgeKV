// Package store implements the in-memory key-value state machine.
//
// The store knows nothing about disks, networks or Raft. It is a deterministic
// state machine: the same sequence of commands always yields the same state.
// Durability and replication are layered on top of it by the Raft log.
//
// Determinism rules that every change to this package must respect:
//   - never read the clock (timestamps arrive inside the command, stamped by the leader)
//   - never use randomness
//   - never let behaviour depend on map iteration order
package store

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

var (
	// ErrNotFound is returned when the key does not exist or has been deleted.
	ErrNotFound = errors.New("store: key not found")
	// ErrVersionMismatch is returned by CAS when the expected version is stale.
	ErrVersionMismatch = errors.New("store: version mismatch")
)

// DefaultDedupWindow is how many log entries a request ID is remembered for.
const DefaultDedupWindow = 100_000

// Entry is what a reader sees for a key.
type Entry struct {
	Value         []byte
	Version       uint64
	Cacheable     bool
	TTLSeconds    uint32
	UpdatedUnixMs int64
}

// record is the internal representation of a key. A deleted key keeps its
// record as a tombstone so that its version never goes backwards.
type record struct {
	value      []byte
	version    uint64
	deleted    bool
	cacheable  bool
	ttlSeconds uint32
	updatedMs  int64
}

// dedupEntry remembers the outcome of a command so that a retry with the same
// request ID returns the original result instead of executing again.
type dedupEntry struct {
	index  uint64 // log index at which the command was first applied
	result *pb.Result
}

type dedupKey struct {
	index uint64
	id    string
}

// Store is a concurrency-safe versioned map.
type Store struct {
	mu   sync.RWMutex
	data map[string]*record

	dedup       map[string]dedupEntry
	dedupOrder  []dedupKey // FIFO by log index, used for eviction
	dedupWindow uint64
}

// New returns an empty store.
func New() *Store {
	return &Store{
		data:        make(map[string]*record),
		dedup:       make(map[string]dedupEntry),
		dedupWindow: DefaultDedupWindow,
	}
}

// SetDedupWindow changes how many log entries a request ID is remembered for.
// Every replica must use the same value, otherwise they would disagree on
// whether a late retry is a duplicate.
func (s *Store) SetDedupWindow(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dedupWindow = n
}

// Get returns the current value and version of key.
func (s *Store) Get(key string) (Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.data[key]
	if !ok || rec.deleted {
		return Entry{}, ErrNotFound
	}
	// A []byte aliases its backing array: returning rec.value directly would
	// let the caller modify the store's data without holding the lock.
	return Entry{
		Value:         cloneBytes(rec.value),
		Version:       rec.version,
		Cacheable:     rec.cacheable,
		TTLSeconds:    rec.ttlSeconds,
		UpdatedUnixMs: rec.updatedMs,
	}, nil
}

// Len returns the number of live (non-deleted) keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, rec := range s.data {
		if !rec.deleted {
			n++
		}
	}
	return n
}

// Put unconditionally sets key to value and returns the new version.
// Versions are per key, start at 1 and increase by 1 on every Put or Delete,
// including across a delete (a re-created key continues from the tombstone's
// version).
func (s *Store) Put(key string, value []byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := s.applyLocked(&pb.Command{Op: pb.Op_OP_PUT, Key: key, Value: value})
	return res.Version
}

// CAS (compare-and-set) sets key to value only if the key's current version
// equals expectedVersion. expectedVersion == 0 means "the key must not exist"
// (never written, or deleted). On a mismatch it returns ErrVersionMismatch and
// changes nothing.
func (s *Store) CAS(key string, value []byte, expectedVersion uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := s.applyLocked(&pb.Command{
		Op: pb.Op_OP_PUT, Key: key, Value: value,
		HasExpectedVersion: true, ExpectedVersion: expectedVersion,
	})
	return res.Version, statusErr(res.Status)
}

// Delete marks key as deleted and returns the tombstone's version.
// Deleting a key that does not exist returns ErrNotFound.
func (s *Store) Delete(key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := s.applyLocked(&pb.Command{Op: pb.Op_OP_DELETE, Key: key})
	return res.Version, statusErr(res.Status)
}

// Apply executes one committed log entry. index is the entry's Raft log index
// and data is a marshalled pb.Command; the return value is a marshalled
// pb.Result. It is called by exactly one goroutine (Raft's applier), in log
// order, on every replica.
func (s *Store) Apply(index uint64, data []byte) []byte {
	var cmd pb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		// Every replica sees the same bytes, so every replica panics at the
		// same index. Skipping the entry instead would silently diverge.
		panic(fmt.Sprintf("store: corrupt command at index %d: %v", index, err))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.evictDedupLocked(index)

	if cmd.RequestId != "" {
		if prev, ok := s.dedup[cmd.RequestId]; ok {
			return mustMarshal(prev.result)
		}
	}

	res := s.applyLocked(&cmd)

	if cmd.RequestId != "" {
		s.dedup[cmd.RequestId] = dedupEntry{index: index, result: res}
		s.dedupOrder = append(s.dedupOrder, dedupKey{index: index, id: cmd.RequestId})
	}
	return mustMarshal(res)
}

// applyLocked is the single place where state changes. The compare and the
// write happen under the same lock acquisition, which is what makes CAS
// atomic. Callers must hold s.mu for writing.
func (s *Store) applyLocked(cmd *pb.Command) *pb.Result {
	rec, exists := s.data[cmd.Key]

	// The version a client can observe: a missing or deleted key is version 0.
	var visible uint64
	if exists && !rec.deleted {
		visible = rec.version
	}

	if cmd.HasExpectedVersion && cmd.ExpectedVersion != visible {
		return &pb.Result{Status: pb.Status_STATUS_VERSION_MISMATCH, Version: visible}
	}

	switch cmd.Op {
	case pb.Op_OP_PUT:
		if !exists {
			rec = &record{}
			s.data[cmd.Key] = rec
		}
		rec.value = cloneBytes(cmd.Value)
		rec.version++
		rec.deleted = false
		rec.cacheable = cmd.Cacheable
		rec.ttlSeconds = cmd.TtlSeconds
		rec.updatedMs = cmd.TimestampUnixMs
		return &pb.Result{Status: pb.Status_STATUS_OK, Version: rec.version}

	case pb.Op_OP_DELETE:
		if visible == 0 {
			return &pb.Result{Status: pb.Status_STATUS_NOT_FOUND}
		}
		// Keep the record as a tombstone: removing it would restart the
		// version at 1 and re-open the ABA problem for CAS.
		rec.value = nil
		rec.version++
		rec.deleted = true
		rec.updatedMs = cmd.TimestampUnixMs
		return &pb.Result{Status: pb.Status_STATUS_OK, Version: rec.version}

	default:
		panic(fmt.Sprintf("store: unknown op %v", cmd.Op))
	}
}

// evictDedupLocked forgets request IDs older than the window. Eviction is
// driven by the log index, never by wall-clock time, so that every replica
// evicts the same entries at the same point in the log.
func (s *Store) evictDedupLocked(index uint64) {
	for len(s.dedupOrder) > 0 {
		oldest := s.dedupOrder[0]
		if index-oldest.index <= s.dedupWindow {
			break
		}
		delete(s.dedup, oldest.id)
		s.dedupOrder = s.dedupOrder[1:]
	}
}

// Snapshot serialises the whole state machine, including the dedup table.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := &pb.StoreSnapshot{
		Records: make([]*pb.SnapshotRecord, 0, len(s.data)),
		Dedup:   make([]*pb.SnapshotDedup, 0, len(s.dedupOrder)),
	}
	for key, rec := range s.data {
		snap.Records = append(snap.Records, &pb.SnapshotRecord{
			Key: key, Value: rec.value, Version: rec.version, Deleted: rec.deleted,
			Cacheable: rec.cacheable, TtlSeconds: rec.ttlSeconds, UpdatedUnixMs: rec.updatedMs,
		})
	}
	// dedupOrder is already in log order, so the restored FIFO is identical.
	for _, k := range s.dedupOrder {
		snap.Dedup = append(snap.Dedup, &pb.SnapshotDedup{
			RequestId: k.id, Index: k.index, Result: s.dedup[k.id].result,
		})
	}
	return proto.Marshal(snap)
}

// Restore replaces the whole state machine with a snapshot.
func (s *Store) Restore(data []byte) error {
	var snap pb.StoreSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("store: corrupt snapshot: %w", err)
	}

	newData := make(map[string]*record, len(snap.Records))
	for _, r := range snap.Records {
		newData[r.Key] = &record{
			value: r.Value, version: r.Version, deleted: r.Deleted,
			cacheable: r.Cacheable, ttlSeconds: r.TtlSeconds, updatedMs: r.UpdatedUnixMs,
		}
	}
	newDedup := make(map[string]dedupEntry, len(snap.Dedup))
	newOrder := make([]dedupKey, 0, len(snap.Dedup))
	for _, d := range snap.Dedup {
		newDedup[d.RequestId] = dedupEntry{index: d.Index, result: d.Result}
		newOrder = append(newOrder, dedupKey{index: d.Index, id: d.RequestId})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.dedup, s.dedupOrder = newData, newDedup, newOrder
	return nil
}

func statusErr(st pb.Status) error {
	switch st {
	case pb.Status_STATUS_OK:
		return nil
	case pb.Status_STATUS_NOT_FOUND:
		return ErrNotFound
	case pb.Status_STATUS_VERSION_MISMATCH:
		return ErrVersionMismatch
	default:
		return fmt.Errorf("store: unknown status %v", st)
	}
}

func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("store: marshal: %v", err))
	}
	return b
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
