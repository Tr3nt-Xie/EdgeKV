// Package raftstore is the durable implementation of raft.Storage. It keeps
// the Raft log and hard state in a segmented WAL and snapshots in separate
// files.
//
// Directory layout:
//
//	<dir>/wal/<seq>-<firstIndex>.wal
//	<dir>/snap-<index>-<term>.snap
//
// Recovery is: load the newest snapshot, then replay the WAL on top of it.
package raftstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
	"github.com/Tr3nt-Xie/EdgeKV/internal/raft"
	"github.com/Tr3nt-Xie/EdgeKV/internal/wal"
)

const (
	recEntry     byte = 1
	recHardState byte = 2

	snapMagic = "EKVSNAP1"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options configures a Store.
type Options struct {
	// Sync controls whether every Save is fsynced. Raft is only correct with
	// Sync on; turning it off exists to measure what durability costs.
	Sync        bool
	SegmentSize int64
}

// Store implements raft.Storage on the local filesystem.
type Store struct {
	dir string
	wal *wal.WAL

	mu        sync.Mutex
	hs        *pb.HardState
	meta      *pb.SnapshotMeta
	lastIndex uint64

	// snapMu serialises whole SaveSnapshot calls. Two snapshots can be in
	// flight at once (the applier's own and one installed from the leader);
	// letting them interleave once allowed GC for the newer one to delete the
	// older one's file between its rename and its meta update.
	snapMu sync.Mutex
}

var _ raft.Storage = (*Store)(nil)

// Open opens or creates the storage rooted at dir.
func Open(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(dir, "wal"), wal.Options{Sync: opts.Sync, SegmentSize: opts.SegmentSize})
	if err != nil {
		return nil, err
	}
	return &Store{dir: dir, wal: w}, nil
}

// Load restores the newest snapshot and replays the WAL on top of it.
func (s *Store) Load() (*raft.Loaded, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, data, err := s.loadNewestSnapshot()
	if err != nil {
		return nil, err
	}
	s.meta = meta

	var snapIndex, minSeq uint64
	if meta != nil {
		snapIndex, minSeq = meta.LastIncludedIndex, meta.MinWalSegment
	}

	var entries []*pb.Entry
	err = s.wal.ReadAll(minSeq, func(rec wal.Record) error {
		switch rec.Type {
		case recHardState:
			hs := &pb.HardState{}
			if err := proto.Unmarshal(rec.Data, hs); err != nil {
				return err
			}
			s.hs = hs
		case recEntry:
			e := &pb.Entry{}
			if err := proto.Unmarshal(rec.Data, e); err != nil {
				return err
			}
			if e.Index <= snapIndex {
				return nil // already covered by the snapshot
			}
			// The WAL is append-only, so a follower that had to overwrite a
			// conflicting suffix simply wrote the new entries after the old
			// ones. Replaying "an existing index replaces the tail" reproduces
			// the truncation without ever rewriting a file.
			pos := e.Index - snapIndex - 1
			if pos > uint64(len(entries)) {
				return fmt.Errorf("raftstore: gap in log: have %d entries after snapshot %d, next is index %d",
					len(entries), snapIndex, e.Index)
			}
			entries = append(entries[:pos], e)
		default:
			return fmt.Errorf("raftstore: unknown record type %d", rec.Type)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.lastIndex = snapIndex + uint64(len(entries))
	if err := s.gcLocked(); err != nil {
		return nil, err
	}
	return &raft.Loaded{HardState: s.hs, SnapshotMeta: meta, SnapshotData: data, Entries: entries}, nil
}

// Save writes the hard state and the entries as one batch with one fsync.
func (s *Store) Save(hs *pb.HardState, entries []*pb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	recs := make([]wal.Record, 0, len(entries)+1)
	if hs != nil {
		rec, err := hardStateRecord(hs)
		if err != nil {
			return err
		}
		recs = append(recs, rec)
	}
	for _, e := range entries {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		recs = append(recs, wal.Record{Type: recEntry, Data: b})
	}
	if err := s.wal.Append(recs...); err != nil {
		return err
	}

	if hs != nil {
		s.hs = proto.Clone(hs).(*pb.HardState)
	}
	if n := len(entries); n > 0 {
		s.lastIndex = entries[n-1].Index
	}
	if s.wal.ShouldRotate() {
		if err := s.rotateLocked(); err != nil {
			return err
		}
		return s.gcLocked()
	}
	return nil
}

// rotateLocked starts a new segment that opens with the current hard state, so
// that older segments can later be deleted without losing the term or vote.
func (s *Store) rotateLocked() error {
	var head []wal.Record
	if s.hs != nil {
		rec, err := hardStateRecord(s.hs)
		if err != nil {
			return err
		}
		head = append(head, rec)
	}
	_, err := s.wal.Rotate(s.lastIndex+1, head...)
	return err
}

func hardStateRecord(hs *pb.HardState) (wal.Record, error) {
	b, err := proto.Marshal(hs)
	return wal.Record{Type: recHardState, Data: b}, err
}

// SaveSnapshot persists a snapshot and garbage-collects the log it covers.
func (s *Store) SaveSnapshot(meta *pb.SnapshotMeta, data []byte, discardLog bool) error {
	meta = proto.Clone(meta).(*pb.SnapshotMeta)

	s.snapMu.Lock()
	defer s.snapMu.Unlock()

	s.mu.Lock()
	if s.meta != nil && meta.LastIncludedIndex <= s.meta.LastIncludedIndex {
		s.mu.Unlock()
		return nil // stale: a newer snapshot is already in place
	}
	if s.meta != nil {
		meta.MinWalSegment = s.meta.MinWalSegment
	}
	if discardLog {
		// Everything in the current segments is about to become invalid. Start
		// a new segment and record, inside the snapshot itself, that recovery
		// must ignore the older ones. The snapshot's rename is then the single
		// atomic step that switches over both the state and the log:
		//   crash before the rename -> old snapshot, old log, as if nothing happened
		//   crash after the rename  -> new snapshot, old segments ignored
		s.lastIndex = meta.LastIncludedIndex
		if err := s.rotateLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
		segs := s.wal.Segments()
		meta.MinWalSegment = segs[len(segs)-1].Seq
	}
	s.mu.Unlock()

	// Writing a large file is slow; do it without holding the lock so that
	// Save (on Raft's critical path) is not blocked behind it.
	if err := s.writeSnapshotFile(meta, data); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meta == nil || meta.LastIncludedIndex > s.meta.LastIncludedIndex {
		s.meta = meta
	}
	return s.gcLocked()
}

// gcLocked deletes WAL segments and snapshot files that recovery no longer needs.
func (s *Store) gcLocked() error {
	if s.meta == nil {
		return nil
	}
	segs := s.wal.Segments()
	removeBefore := s.meta.MinWalSegment
	// Segment i holds only indexes below segment i+1's first index. If that
	// first index is within the snapshot, all of segment i is too.
	for i := 0; i+1 < len(segs); i++ {
		if segs[i+1].Tag > s.meta.LastIncludedIndex+1 {
			break
		}
		removeBefore = max(removeBefore, segs[i+1].Seq)
	}
	if err := s.wal.RemoveBefore(removeBefore); err != nil {
		return err
	}

	// Older snapshots cannot serve as a fallback once the log they would need
	// has been deleted, so remove everything below the current one. Never
	// touch a file with a HIGHER index: SaveSnapshot renames its file before
	// re-taking the lock to publish it in s.meta, and a Save-triggered rotation
	// can run GC in that window. Deleting the newer file there would leave the
	// store believing in a snapshot that no longer exists on disk.
	files, err := s.snapshotFiles()
	if err != nil {
		return err
	}
	current := s.snapshotPath(s.meta)
	for _, f := range files {
		if f == current {
			continue
		}
		idx, _, ok := parseSnapshotPath(f)
		if ok && idx > s.meta.LastIncludedIndex {
			continue
		}
		os.Remove(f)
	}
	return nil
}

// parseSnapshotPath extracts (index, term) from snap-<index>-<term>.snap.
func parseSnapshotPath(path string) (index, term uint64, ok bool) {
	var i, t uint64
	if _, err := fmt.Sscanf(filepath.Base(path), "snap-%016x-%016x.snap", &i, &t); err != nil {
		return 0, 0, false
	}
	return i, t, true
}

// Snapshot returns the newest snapshot.
func (s *Store) Snapshot() (*pb.SnapshotMeta, []byte, error) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	s.mu.Lock()
	meta := s.meta
	s.mu.Unlock()
	if meta == nil {
		return nil, nil, nil
	}
	return readSnapshotFile(s.snapshotPath(meta))
}

// WALSize returns the bytes the log currently occupies on disk.
func (s *Store) WALSize() int64 { return s.wal.SizeOnDisk() }

func (s *Store) Close() error { return s.wal.Close() }

// ---------------------------------------------------------------------------
// Snapshot files
//
//	magic(8) | metaLen u32 | meta | dataLen u64 | data | crc32c u32
// ---------------------------------------------------------------------------

func (s *Store) snapshotPath(meta *pb.SnapshotMeta) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%016x-%016x.snap", meta.LastIncludedIndex, meta.LastIncludedTerm))
}

func (s *Store) snapshotFiles() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, "snap-*.snap"))
	sort.Strings(files) // fixed-width hex: lexical order is index order
	return files, err
}

// writeSnapshotFile uses the write-temp, fsync, rename, fsync-directory
// sequence. A reader therefore sees either the complete new file or no new
// file at all, never a half-written one.
func (s *Store) writeSnapshotFile(meta *pb.SnapshotMeta, data []byte) error {
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(snapMagic)+4+len(metaBytes)+8+len(data)+4)
	buf = append(buf, snapMagic...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(metaBytes)))
	buf = append(buf, metaBytes...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(data)))
	buf = append(buf, data...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	final := s.snapshotPath(meta)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(s.dir)
}

var errBadSnapshot = errors.New("raftstore: snapshot file is corrupt")

func readSnapshotFile(path string) (*pb.SnapshotMeta, []byte, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	if len(buf) < len(snapMagic)+4+8+4 || string(buf[:len(snapMagic)]) != snapMagic {
		return nil, nil, errBadSnapshot
	}
	body, sum := buf[:len(buf)-4], binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, nil, errBadSnapshot
	}
	p := body[len(snapMagic):]
	metaLen := binary.LittleEndian.Uint32(p)
	p = p[4:]
	if uint64(len(p)) < uint64(metaLen)+8 {
		return nil, nil, errBadSnapshot
	}
	meta := &pb.SnapshotMeta{}
	if err := proto.Unmarshal(p[:metaLen], meta); err != nil {
		return nil, nil, errBadSnapshot
	}
	p = p[metaLen:]
	dataLen := binary.LittleEndian.Uint64(p)
	p = p[8:]
	if uint64(len(p)) != dataLen {
		return nil, nil, errBadSnapshot
	}
	return meta, p, nil
}

func (s *Store) loadNewestSnapshot() (*pb.SnapshotMeta, []byte, error) {
	// Leftovers of a snapshot that was being written when we crashed.
	if tmps, _ := filepath.Glob(filepath.Join(s.dir, "snap-*.snap.tmp")); len(tmps) > 0 {
		for _, t := range tmps {
			os.Remove(t)
		}
	}
	files, err := s.snapshotFiles()
	if err != nil || len(files) == 0 {
		return nil, nil, err
	}
	// Do not fall back to an older snapshot if the newest is damaged: the log
	// between the two may already be gone, and starting from a state with a
	// hole in it is worse than not starting.
	newest := files[len(files)-1]
	meta, data, err := readSnapshotFile(newest)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %s", err, newest)
	}
	return meta, data, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
