package raft

import (
	"fmt"

	"github.com/Tr3nt-Xie/EdgeKV/internal/pb"
)

// raftLog is the in-memory log. Entries up to and including snapIndex have
// been folded into a snapshot and are gone; entries[i] holds log index
// snapIndex+1+i. Entries are treated as immutable once appended, so slices of
// pointers can be handed to other goroutines without copying.
//
// raftLog is not safe for concurrent use; Raft.mu protects it.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []*pb.Entry
}

func (l *raftLog) lastIndex() uint64 {
	return l.snapIndex + uint64(len(l.entries))
}

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index. ok is false if the index has
// been compacted away or lies beyond the end of the log.
func (l *raftLog) term(index uint64) (term uint64, ok bool) {
	switch {
	case index == l.snapIndex:
		return l.snapTerm, true
	case index < l.snapIndex || index > l.lastIndex():
		return 0, false
	}
	return l.entries[index-l.snapIndex-1].Term, true
}

// slice returns entries with lo <= index <= hi.
func (l *raftLog) slice(lo, hi uint64) []*pb.Entry {
	if lo <= l.snapIndex || hi > l.lastIndex() || lo > hi {
		if lo > hi {
			return nil
		}
		panic(fmt.Sprintf("raft: slice [%d,%d] out of range (snap %d, last %d)", lo, hi, l.snapIndex, l.lastIndex()))
	}
	out := make([]*pb.Entry, hi-lo+1)
	copy(out, l.entries[lo-l.snapIndex-1:hi-l.snapIndex])
	return out
}

func (l *raftLog) append(entries ...*pb.Entry) {
	for _, e := range entries {
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raft: appending index %d after %d", e.Index, l.lastIndex()))
		}
		l.entries = append(l.entries, e)
	}
}

// truncateFrom drops the entry at index and everything after it.
func (l *raftLog) truncateFrom(index uint64) {
	if index <= l.snapIndex {
		panic(fmt.Sprintf("raft: truncating at %d, inside snapshot %d", index, l.snapIndex))
	}
	if index > l.lastIndex() {
		return
	}
	l.entries = l.entries[:index-l.snapIndex-1]
}

// firstIndexOfTerm returns the lowest index in the log whose entry has the
// given term, scanning backwards from `from`.
func (l *raftLog) firstIndexOfTerm(term, from uint64) uint64 {
	index := from
	for index > l.snapIndex+1 {
		if t, _ := l.term(index - 1); t != term {
			break
		}
		index--
	}
	return index
}

// lastIndexOfTerm returns the highest index whose entry has the given term, or
// 0 if the log holds no entry of that term.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == term {
			return l.entries[i].Index
		}
		if l.entries[i].Term < term {
			break // terms never decrease along the log
		}
	}
	return 0
}

// compactTo discards entries up to and including index.
func (l *raftLog) compactTo(index, term uint64) {
	if index <= l.snapIndex {
		return
	}
	if index >= l.lastIndex() {
		l.entries = nil
	} else {
		// Copy so the old backing array, and the entries it pins, can be freed.
		l.entries = append([]*pb.Entry(nil), l.entries[index-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm = index, term
}

// reset replaces the whole log with an empty one that starts after a snapshot.
func (l *raftLog) reset(index, term uint64) {
	l.entries = nil
	l.snapIndex, l.snapTerm = index, term
}
