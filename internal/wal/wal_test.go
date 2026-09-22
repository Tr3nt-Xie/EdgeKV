package wal

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func readAll(t *testing.T, w *WAL) []Record {
	t.Helper()
	var out []Record
	if err := w.ReadAll(0, func(r Record) error { out = append(out, r); return nil }); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return out
}

func mustOpen(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := Open(dir, Options{Sync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	for i := 0; i < 10; i++ {
		if err := w.Append(Record{Type: 1, Data: []byte(fmt.Sprintf("rec-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w = mustOpen(t, dir)
	defer w.Close()
	recs := readAll(t, w)
	if len(recs) != 10 {
		t.Fatalf("replayed %d records, want 10", len(recs))
	}
	for i, r := range recs {
		if want := fmt.Sprintf("rec-%d", i); string(r.Data) != want || r.Type != 1 {
			t.Fatalf("record %d = %q type %d, want %q type 1", i, r.Data, r.Type, want)
		}
	}
}

// Crash in the middle of a write: the file ends with half a record. The record
// was never acknowledged, so recovery drops it and the log stays usable.
func TestTornTailIsTruncated(t *testing.T) {
	for _, cut := range []int64{1, 5, headerSize, headerSize + 3} {
		t.Run(fmt.Sprintf("cut=%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			w := mustOpen(t, dir)
			w.Append(Record{Type: 1, Data: []byte("first")})
			w.Append(Record{Type: 1, Data: []byte("second-record")})
			path := w.Segments()[0].Path
			w.Close()

			st, _ := os.Stat(path)
			secondStart := int64(headerSize + len("first"))
			if err := os.Truncate(path, secondStart+cut); err != nil {
				t.Fatal(err)
			}
			if secondStart+cut >= st.Size() {
				t.Fatalf("test bug: cut %d does not shorten the file", cut)
			}

			w = mustOpen(t, dir)
			defer w.Close()
			recs := readAll(t, w)
			if len(recs) != 1 || string(recs[0].Data) != "first" {
				t.Fatalf("got %d records, want only \"first\"", len(recs))
			}
			// The garbage must really be gone: a new append has to land right
			// after "first", not after the torn bytes.
			if err := w.Append(Record{Type: 1, Data: []byte("third")}); err != nil {
				t.Fatal(err)
			}
			recs = readAll(t, w)
			if len(recs) != 2 || string(recs[1].Data) != "third" {
				t.Fatalf("after repair + append got %d records", len(recs))
			}
		})
	}
}

func TestBitFlipAtTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(Record{Type: 1, Data: []byte("first")})
	w.Append(Record{Type: 1, Data: []byte("second")})
	path := w.Segments()[0].Path
	w.Close()

	flipLastByte(t, path)

	w = mustOpen(t, dir)
	defer w.Close()
	if recs := readAll(t, w); len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
}

// Damage in a sealed segment cannot be a torn write. Guessing would risk
// silently dropping acknowledged data, so the WAL must refuse.
func TestCorruptionInSealedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(Record{Type: 1, Data: []byte("first")})
	sealed := w.Segments()[0].Path
	if _, err := w.Rotate(2); err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Type: 1, Data: []byte("second")})
	w.Close()

	flipLastByte(t, sealed)

	w = mustOpen(t, dir)
	defer w.Close()
	err := w.ReadAll(0, func(Record) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got err %v, want ErrCorrupt", err)
	}
}

func TestRotateAndRemove(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	defer w.Close()

	w.Append(Record{Type: 1, Data: []byte("a")})
	seq2, err := w.Rotate(100, Record{Type: 2, Data: []byte("head")})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Type: 1, Data: []byte("b")})

	segs := w.Segments()
	if len(segs) != 2 || segs[1].Seq != seq2 || segs[1].Tag != 100 {
		t.Fatalf("segments = %+v", segs)
	}

	if err := w.RemoveBefore(seq2); err != nil {
		t.Fatal(err)
	}
	if segs := w.Segments(); len(segs) != 1 {
		t.Fatalf("after RemoveBefore: %d segments, want 1", len(segs))
	}
	recs := readAll(t, w)
	if len(recs) != 2 || string(recs[0].Data) != "head" || string(recs[1].Data) != "b" {
		t.Fatalf("records after compaction = %v", recs)
	}

	// RemoveBefore must never delete the active segment.
	if err := w.RemoveBefore(seq2 + 10); err != nil {
		t.Fatal(err)
	}
	if segs := w.Segments(); len(segs) != 1 {
		t.Fatal("active segment was deleted")
	}
}

func TestReadAllHonoursMinSeq(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	defer w.Close()
	w.Append(Record{Type: 1, Data: []byte("old")})
	seq2, _ := w.Rotate(0)
	w.Append(Record{Type: 1, Data: []byte("new")})

	var got []string
	w.ReadAll(seq2, func(r Record) error { got = append(got, string(r.Data)); return nil })
	if len(got) != 1 || got[0] != "new" {
		t.Fatalf("got %v, want [new]", got)
	}
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
