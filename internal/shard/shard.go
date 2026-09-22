// Package shard maps keys to shards.
package shard

import "hash/fnv"

// ForKey returns the shard that owns key, in [0, numShards).
//
// The mapping is hash(key) mod numShards with a fixed numShards. That is safe
// here because shards are logical: what moves when nodes are added is the
// placement of a shard's replicas, never the key-to-shard mapping. It is the
// same idea as Redis Cluster's 16384 slots or Kafka partitions. Changing
// numShards itself would remap almost every key, and is out of scope.
//
// FNV-1a is used because it is in the standard library, stable across
// processes and Go versions (unlike the built-in map hash, which is seeded per
// process), and spreads short string keys well enough for this purpose.
func ForKey(key string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}
