# EdgeKV

A fault-tolerant, sharded key-value store with edge caching, written from scratch in Go.

EdgeKV targets the small, critical state an AI-agent platform depends on — task status, worker leases, idempotency records, feature flags, model configuration. Some of that data must always be read fresh; some of it is read constantly and changes rarely. EdgeKV serves both, with the consistency contract made explicit in the API.

```
                  clients
                     │
            CloudFront / edge cache          caches only  GET /v1/cache/*
                     │
        ┌────────────┼────────────┐
     node n1      node n2      node n3       HTTP API + shard router + gRPC Raft
   ┌──┬──┬──┐   ┌──┬──┬──┐   ┌──┬──┬──┐
   │s0│s1│s2│   │s0│s1│s2│   │s0│s1│s2│      one Raft group per shard, leaders spread
   └──┴──┴──┘   └──┴──┴──┘   └──┴──┴──┘
      WAL + snapshots on local disk / EBS
```

## What's inside

| Layer | Package | Highlights |
|---|---|---|
| State machine | `internal/store` | Per-key versions, compare-and-set, tombstones, request-ID deduplication (index-window evicted), protobuf snapshots |
| Write-ahead log | `internal/wal` | Segmented, CRC32C per record, fsync'd group commit, torn-tail repair, refuses to start on mid-log corruption |
| Consensus | `internal/raft` | Election with pre-vote, log replication with fast conflict backtracking, current-term commit rule, ReadIndex linearizable reads, snapshots / InstallSnapshot, leadership transfer, check-quorum |
| Durable Raft storage | `internal/raftstore` | Raft log + hard state on the WAL, atomic snapshot files, log compaction |
| Multi-Raft node | `internal/node` | One Raft group per shard, FNV key hashing, automatic leader spreading |
| HTTP gateway | `internal/server` | `/v1/kv` (linearizable, `no-store`), `/v1/cache` (bounded staleness: TTL, ETag, versioned URLs), forward-to-leader or `421` for leader-aware clients, Prometheus `/metrics` |
| Edge | `internal/cdn`, `cmd/edgekv-edge` | Async, batched, retried invalidation (CloudFront or a local CloudFront-like simulator with fault injection) |
| Client | `pkg/client` | Leader cache, retries with the same request ID |
| Verification | `internal/node/linearizability_test.go` | Porcupine linearizability check under partitions, crashes and packet loss, plus a negative test that proves the checker catches stale reads |

## Quick start

```bash
# three nodes + edge cache as local processes
scripts/cluster.sh start && sleep 2 && scripts/cluster.sh status

# write, read, CAS
curl -X PUT localhost:8081/v1/kv/task:1:status -d '{"value":"running"}'
curl localhost:8082/v1/kv/task:1:status
curl -X PUT 'localhost:8083/v1/kv/task:1:status?expectedVersion=1' -d '{"value":"done"}'

# cacheable key, read through the edge
curl -X PUT localhost:8081/v1/kv/config:model -d '{"value":"v12","cache_policy":{"cacheable":true,"ttl_seconds":30}}'
curl -i localhost:8000/v1/cache/config:model | grep -iE 'x-cache|cache-control'

# crash a leader, keep writing, bring it back
scripts/cluster.sh kill n2 && scripts/cluster.sh restart n2
```

Or with Docker: `docker compose up -d --build`, then the same `curl`s against `localhost:8081-8083` and `localhost:8000`.

## Tests

```bash
go test -race ./...            # ~40 s; includes chaos and linearizability tests
go test -race -short ./...     # skips the slow ones
```

## Benchmarks

`bench/run-local.sh` runs the experiment matrix from the spec (topology, fsync cost, concurrency, key distribution, cache TTL vs staleness, invalidation failure) and `bench/plot.py` renders `docs/charts/`. Results and analysis, local and AWS: [docs/experiments.md](docs/experiments.md). Headlines: on AWS, six shards give 1.9× the throughput of one; with CloudFront invalidation taking 3–5 s to propagate, maximum observed staleness stayed under the TTL.

## Deploy

[deploy/README.md](deploy/README.md): Terraform for a VPC, three EC2 nodes with EBS gp3 volumes, a locked-down security group, and a CloudFront distribution that caches only `/v1/cache/*`.

## Docs

- [docs/architecture.md](docs/architecture.md) — design overview and consistency contract
- [docs/experiments.md](docs/experiments.md) — benchmark method, results and analysis

## Not in scope

Cross-shard transactions, online membership change and rebalancing, multi-region consensus, Byzantine fault tolerance, tombstone garbage collection, mTLS between nodes.

## License

MIT
