package shard

import (
	"fmt"
	"testing"
)

func TestForKeyIsStableAndInRange(t *testing.T) {
	for _, n := range []int{1, 3, 8} {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("task:%d:status", i)
			s := ForKey(key, n)
			if s < 0 || s >= n {
				t.Fatalf("ForKey(%q, %d) = %d, out of range", key, n, s)
			}
			if s != ForKey(key, n) {
				t.Fatalf("ForKey(%q, %d) is not deterministic", key, n)
			}
		}
	}
	// Pinned value: if this changes, every deployed cluster would look for its
	// keys in the wrong shard.
	if got := ForKey("task:4832:status", 3); got != 1 {
		t.Fatalf("ForKey(task:4832:status, 3) = %d; the hash function must never change", got)
	}
}

func TestForKeySpreadsEvenly(t *testing.T) {
	const n, keys = 4, 100_000
	counts := make([]int, n)
	for i := 0; i < keys; i++ {
		counts[ForKey(fmt.Sprintf("key-%d", i), n)]++
	}
	for s, c := range counts {
		if share := float64(c) / keys; share < 0.23 || share > 0.27 {
			t.Fatalf("shard %d holds %.1f%% of keys, want about 25%%", s, share*100)
		}
	}
}
