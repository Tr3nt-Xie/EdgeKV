# EdgeKV Architecture

## Consistency contract

| Operation | Path | Guarantee |
|---|---|---|
| Write | `PUT /v1/kv/{key}` | Returns only after the entry is durable (fsync) on a majority of replicas and applied. |
| Conditional write | `PUT /v1/kv/{key}?expectedVersion=n` | Compare-and-set on the per-key version; `n = 0` means "must not exist". `412` on mismatch. |
| Delete | `DELETE /v1/kv/{key}` | Replicated tombstone; the version keeps increasing across delete/re-create. |
| Strong read | `GET /v1/kv/{key}` | Linearizable via ReadIndex. Always `Cache-Control: no-store`. |
| Cached read | `GET /v1/cache/{key}` | Bounded staleness: at most the key's TTL. The origin itself still reads linearizably; staleness is introduced only by the cache layer. |
| Versioned cached read | `GET /v1/cache/{key}?v=n` | Immutable; never stale. |

Every write carries a `request_id`. Retrying with the same ID after a timeout is safe: the state machine executes it at most once and returns the original result.

Ordering is per shard. There are no cross-shard transactions.

## Write path

```
client ─▶ any node ─▶ (forward or 421) ─▶ shard leader
   leader: append to log ─▶ WAL fsync ─▶ AppendEntries to followers
   follower: consistency check ─▶ append ─▶ WAL fsync ─▶ ack
   leader: majority + current-term rule ─▶ commitIndex ─▶ applier ─▶ Store.Apply ─▶ reply
   after reply: enqueue CDN invalidation (best effort)
```

Concurrent proposals are batched: one fsync and one AppendEntries per batch (group commit).

## Read paths

**Strong:** `ReadIndex` — record commitIndex, confirm leadership with one heartbeat round to a majority (only acks to RPCs sent after the read began count), wait for the applier to reach that index, read the local state machine. A deposed leader that hasn't noticed refuses instead of answering.

**Cached:** the origin performs the same strong read, then sets `Cache-Control: public, max-age=TTL`, `ETag: "v<version>"` and `Last-Modified`. Conditional requests get `304`. Errors are never cacheable.

## Sharding

`shard = FNV-1a(key) mod N`, with N fixed per cluster. Each shard is an independent Raft group with its own term, log, WAL directory and state machine. All groups on a node share one gRPC connection per peer; each RPC names its group.

Preferred leader of shard *i* is `sortedPeers[i mod len(peers)]`. A node leading a shard it should not lead transfers leadership back once the preferred node is healthy and caught up.

## Durability

- **WAL** (`internal/wal`): append-only segments; each record is `len | crc32c | type | data`. Recovery truncates a torn record at the tail of the last segment (never acknowledged) and refuses to start on damage anywhere else.
- **Raft storage** (`internal/raftstore`): term/vote and log entries are WAL records. A follower overwriting a conflicting suffix appends the new entries; replay treats "same index" as "replaces the rest". Every rotation re-states term/vote in the new segment so old segments can be deleted.
- **Snapshots**: written to a temp file, fsynced, renamed, directory fsynced. Discarding the whole log (after an InstallSnapshot that conflicts with the local log) is made atomic by rotating the WAL first and recording the new segment number inside the snapshot.
- `commitIndex` is not persisted; it is re-learned from the leader after restart.

## Edge caching

Only keys under an allow-listed namespace *and* written with `cache_policy.cacheable = true` are ever served from `/v1/cache`. Writes to cacheable keys enqueue an invalidation after the commit; the queue batches, retries with backoff, and on final failure logs and gives up — the TTL then bounds staleness. CloudFront caches only the `/v1/cache/*` behaviour, with the query string in the cache key so versioned URLs work.

## Failure model

Crash-stop nodes, delayed or lost messages, temporary partitions. A three-replica shard tolerates one failed node. Not handled: Byzantine nodes, permanent loss of a whole region, disks that lie about fsync.

## Verification

- Unit tests with the race detector for every package.
- Raft cluster tests on an in-memory network with fault injection (partition, node down, drop rate, delay), one test per safety property from the paper, plus a chaos test.
- A Porcupine linearizability check of ~4000 GET/PUT/CAS/DELETE operations from 6 clients on real nodes with real on-disk storage, while the leader is partitioned, killed and restarted, and the network drops 15% of messages. A negative test confirms the checker rejects a history containing a stale read from a partitioned former leader.
