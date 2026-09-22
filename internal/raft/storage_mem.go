package raft

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// MemStorage is a Storage that keeps everything in memory. It is used by
// tests: "crashing" a node means discarding the Raft object and creating a new
// one on the same MemStorage, which then plays the role of the disk.
type MemStorage struct {
	mu       sync.Mutex
	hs       *pb.HardState
	meta     *pb.SnapshotMeta
	snapshot []byte
	entries  []*pb.Entry
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Load() (*Loaded, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &Loaded{
		HardState:    m.hs,
		SnapshotMeta: m.meta,
		SnapshotData: m.snapshot,
		Entries:      append([]*pb.Entry(nil), m.entries...),
	}, nil
}

func (m *MemStorage) Save(hs *pb.HardState, entries []*pb.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hs != nil {
		m.hs = proto.Clone(hs).(*pb.HardState)
	}
	for _, e := range entries {
		m.appendLocked(e)
	}
	return nil
}

// appendLocked gives Save its "an existing index replaces the tail" meaning.
func (m *MemStorage) appendLocked(e *pb.Entry) {
	first := uint64(1)
	if m.meta != nil {
		first = m.meta.LastIncludedIndex + 1
	}
	if e.Index < first {
		return
	}
	m.entries = append(m.entries[:e.Index-first], e)
}

func (m *MemStorage) SaveSnapshot(meta *pb.SnapshotMeta, data []byte, discardLog bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.meta != nil && meta.LastIncludedIndex <= m.meta.LastIncludedIndex {
		return nil
	}
	first := uint64(1)
	if m.meta != nil {
		first = m.meta.LastIncludedIndex + 1
	}
	drop := meta.LastIncludedIndex + 1 - first
	if discardLog || drop >= uint64(len(m.entries)) {
		m.entries = nil
	} else {
		m.entries = append([]*pb.Entry(nil), m.entries[drop:]...)
	}
	m.meta = proto.Clone(meta).(*pb.SnapshotMeta)
	m.snapshot = data
	return nil
}

func (m *MemStorage) Snapshot() (*pb.SnapshotMeta, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta, m.snapshot, nil
}

func (m *MemStorage) Close() error { return nil }
