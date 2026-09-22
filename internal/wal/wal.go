// Package wal implements a segmented, checksummed write-ahead log.
//
// The WAL is generic: it stores typed byte records and knows nothing about
// Raft. The meaning of each record type lives in package raftstore.
//
// On-disk layout
//
//	<dir>/<seq>-<tag>.wal        one file per segment, both numbers in hex
//
// Record framing inside a segment (little endian):
//
//	+----------+-----------+--------+----------------+
//	| len u32  | crc32c u32| type u8| data (len bytes)|
//	+----------+-----------+--------+----------------+
//
// The CRC covers type and data. A record is only considered written once
// Append has returned, which happens after fsync when Options.Sync is set.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	headerSize = 9 // len(4) + crc(4) + type(1)

	// DefaultSegmentSize is the size at which ShouldRotate starts reporting true.
	DefaultSegmentSize = 16 << 20

	// maxRecordSize guards against reading a garbage length from a torn header
	// and trying to allocate gigabytes.
	maxRecordSize = 256 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means a record failed its checksum somewhere other than the tail
// of the last segment. That cannot be explained by a crash during a write, so
// the WAL refuses to guess and the node must not start.
var ErrCorrupt = errors.New("wal: corrupt record in the middle of the log")

// Record is one entry in the log.
type Record struct {
	Type byte
	Data []byte
}

// Options configures a WAL.
type Options struct {
	// SegmentSize is the soft limit of one segment file.
	SegmentSize int64
	// Sync makes every Append call fsync before returning. Turning it off
	// trades durability for throughput and exists for benchmarking only.
	Sync bool
}

// Segment describes one segment file.
type Segment struct {
	Seq  uint64 // monotonically increasing sequence number
	Tag  uint64 // caller-defined; raftstore stores "first log index that may appear here"
	Path string
}

// WAL is an append-only log split into segment files.
type WAL struct {
	dir  string
	opts Options

	mu       sync.Mutex
	segments []Segment // sorted by Seq; the last one is active
	active   *os.File
	size     int64
	buf      []byte // reused encode buffer
}

// Open opens the WAL in dir, creating the directory and a first segment if
// needed. It does not read any records; call ReadAll for that.
func Open(dir string, opts Options) (*WAL, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, opts: opts, segments: segs}
	if len(segs) == 0 {
		if err := w.createSegment(1, 0); err != nil {
			return nil, err
		}
		return w, nil
	}
	last := segs[len(segs)-1]
	f, err := os.OpenFile(last.Path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	w.active, w.size = f, size
	return w, nil
}

func listSegments(dir string) ([]Segment, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return nil, err
	}
	segs := make([]Segment, 0, len(names))
	for _, path := range names {
		base := strings.TrimSuffix(filepath.Base(path), ".wal")
		var seq, tag uint64
		if _, err := fmt.Sscanf(base, "%016x-%016x", &seq, &tag); err != nil {
			return nil, fmt.Errorf("wal: unexpected file %q", path)
		}
		segs = append(segs, Segment{Seq: seq, Tag: tag, Path: path})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].Seq < segs[j].Seq })
	return segs, nil
}

func (w *WAL) createSegment(seq, tag uint64) error {
	path := filepath.Join(w.dir, fmt.Sprintf("%016x-%016x.wal", seq, tag))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	// A new file only survives a crash once its directory entry is durable.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.segments = append(w.segments, Segment{Seq: seq, Tag: tag, Path: path})
	w.active, w.size = f, 0
	return nil
}

// ReadAll replays every record of every segment with Seq >= minSeq, in order.
//
// A torn or corrupt record at the tail of the last segment is the expected
// result of crashing in the middle of an Append. Such a record was never
// acknowledged, so ReadAll truncates the file at that point and carries on. The
// same damage anywhere else returns ErrCorrupt.
func (w *WAL) ReadAll(minSeq uint64, fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, seg := range w.segments {
		if seg.Seq < minSeq {
			continue
		}
		isLast := i == len(w.segments)-1
		goodOffset, err := readSegment(seg.Path, fn)
		if err == nil {
			continue
		}
		if !errors.Is(err, errTorn) {
			return err
		}
		if !isLast {
			return fmt.Errorf("%w: segment %s offset %d", ErrCorrupt, seg.Path, goodOffset)
		}
		if err := w.active.Truncate(goodOffset); err != nil {
			return err
		}
		if err := w.active.Sync(); err != nil {
			return err
		}
		if _, err := w.active.Seek(goodOffset, io.SeekStart); err != nil {
			return err
		}
		w.size = goodOffset
	}
	return nil
}

var errTorn = errors.New("wal: torn record")

// readSegment calls fn for each valid record and returns the offset just past
// the last valid one. It returns errTorn if the file ends in an incomplete or
// checksum-failing record.
func readSegment(path string, fn func(Record) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var offset int64
	var header [headerSize]byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				return offset, nil // clean end of segment
			}
			if err == io.ErrUnexpectedEOF {
				return offset, errTorn
			}
			return offset, err
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		sum := binary.LittleEndian.Uint32(header[4:8])
		typ := header[8]
		if length > maxRecordSize {
			return offset, errTorn
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return offset, errTorn
			}
			return offset, err
		}
		if checksum(typ, data) != sum {
			return offset, errTorn
		}
		if err := fn(Record{Type: typ, Data: data}); err != nil {
			return offset, err
		}
		offset += headerSize + int64(length)
	}
}

func checksum(typ byte, data []byte) uint32 {
	sum := crc32.Update(0, castagnoli, []byte{typ})
	return crc32.Update(sum, castagnoli, data)
}

// Append writes the records to the active segment with a single write call
// and, if Options.Sync is set, a single fsync. Batching many records into one
// Append is what makes group commit cheap: the cost of durability is paid once
// per batch, not once per record.
func (w *WAL) Append(recs ...Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(recs)
}

func (w *WAL) appendLocked(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	w.buf = w.buf[:0]
	for _, rec := range recs {
		var header [headerSize]byte
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(rec.Data)))
		binary.LittleEndian.PutUint32(header[4:8], checksum(rec.Type, rec.Data))
		header[8] = rec.Type
		w.buf = append(w.buf, header[:]...)
		w.buf = append(w.buf, rec.Data...)
	}
	n, err := w.active.Write(w.buf)
	w.size += int64(n)
	if err != nil {
		return err
	}
	if w.opts.Sync {
		// On Linux this is fsync(2). On macOS Go issues F_FULLFSYNC, which also
		// flushes the drive's own cache and is therefore much slower.
		return w.active.Sync()
	}
	return nil
}

// ShouldRotate reports whether the active segment has reached its size limit.
func (w *WAL) ShouldRotate() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size >= w.opts.SegmentSize
}

// Rotate seals the active segment and starts a new one whose first records are
// head. The caller uses head to re-state anything that must not be lost when
// older segments are deleted (for Raft: the current term and vote).
func (w *WAL) Rotate(tag uint64, head ...Record) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.active.Sync(); err != nil {
		return 0, err
	}
	if err := w.active.Close(); err != nil {
		return 0, err
	}
	seq := w.segments[len(w.segments)-1].Seq + 1
	if err := w.createSegment(seq, tag); err != nil {
		return 0, err
	}
	if err := w.appendLocked(head); err != nil {
		return 0, err
	}
	// The head records must be durable even when Options.Sync is off, because
	// the caller is about to delete the segments that held the previous copy.
	if err := w.active.Sync(); err != nil {
		return 0, err
	}
	return seq, nil
}

// RemoveBefore deletes every segment with Seq < seq. The active segment is
// never deleted.
func (w *WAL) RemoveBefore(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keep := w.segments[:0:0]
	removed := false
	for i, seg := range w.segments {
		if seg.Seq < seq && i != len(w.segments)-1 {
			if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
				return err
			}
			removed = true
			continue
		}
		keep = append(keep, seg)
	}
	w.segments = keep
	if removed {
		return syncDir(w.dir)
	}
	return nil
}

// Segments returns a copy of the current segment list, oldest first.
func (w *WAL) Segments() []Segment {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Segment(nil), w.segments...)
}

// SizeOnDisk returns the total size of all segment files in bytes.
func (w *WAL) SizeOnDisk() int64 {
	var total int64
	for _, seg := range w.Segments() {
		if st, err := os.Stat(seg.Path); err == nil {
			total += st.Size()
		}
	}
	return total
}

// Close syncs and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return nil
	}
	err := w.active.Sync()
	if cerr := w.active.Close(); err == nil {
		err = cerr
	}
	w.active = nil
	return err
}

// syncDir fsyncs a directory so that file creations, renames and deletions
// inside it survive a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
