#!/usr/bin/env bash
# Run the local experiment matrix and append JSON results to bench/results/local.jsonl.
#
#   bench/run-local.sh            full matrix (~10 minutes)
#   QUICK=1 bench/run-local.sh    shorter runs
#
# Every run restarts the cluster with a clean data directory so results do not
# depend on the previous run's log size or leader placement.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=bench/results/local.jsonl
mkdir -p bench/results
DUR=${DUR:-10s}; WARM=${WARM:-2s}; KEYS=${KEYS:-20000}
[[ ${QUICK:-0} == 1 ]] && { DUR=4s; WARM=1s; KEYS=5000; }
BENCH="go run ./cmd/edgekv-bench -duration $DUR -warmup $WARM -keys $KEYS -out $OUT"

cluster() { # cluster NODES SHARDS FSYNC [EDGE]
  scripts/cluster.sh clean >/dev/null 2>&1 || true
  NODES=$1 SHARDS=$2 FSYNC=$3 EDGE=${4:-0} scripts/cluster.sh start >/dev/null
  sleep 2.5
}
nodes_flag() { local s=""; for i in $(seq 1 "$1"); do s+="http://127.0.0.1:$((8080+i)),"; done; echo "${s%,}"; }

echo "== 1. topology: single node vs 3-node Raft vs 3-node × 3 shards (fsync on, 50 clients)"
cluster 1 1 true;  $BENCH -name topo-1node-1shard  -nodes "$(nodes_flag 1)" -clients 50
cluster 3 1 true;  $BENCH -name topo-3node-1shard  -nodes "$(nodes_flag 3)" -clients 50
cluster 3 3 true;  $BENCH -name topo-3node-3shard  -nodes "$(nodes_flag 3)" -clients 50
cluster 3 6 true;  $BENCH -name topo-3node-6shard  -nodes "$(nodes_flag 3)" -clients 50

echo "== 2. durability: fsync on vs off (3 nodes, 3 shards, write-only, 50 clients)"
cluster 3 3 true;  $BENCH -name fsync-on  -mix put=100 -clients 50
cluster 3 3 false; $BENCH -name fsync-off -mix put=100 -clients 50

echo "== 3. concurrency: latency vs clients (3 nodes, 3 shards, fsync on)"
cluster 3 3 true
for c in 1 10 50 100 500; do
  $BENCH -name "clients-$c" -clients "$c" -preload=false
done

echo "== 4. key distribution: uniform vs zipf (3 nodes, 3 shards, 50 clients)"
$BENCH -name dist-uniform -dist uniform -clients 50 -preload=false
$BENCH -name dist-zipf    -dist zipf    -clients 50 -preload=false

echo "== 5. cache: origin reads vs edge reads, and TTL vs staleness"
cluster 3 3 true 1
$BENCH -name cache-origin -mix cget=100 -clients 50 -preload=true
for ttl in 5 30 60; do
  $BENCH -name "cache-edge-ttl$ttl" -mix cget=90,put=10 -clients 50 -ttl "$ttl" -edge http://127.0.0.1:8000 -preload=true
  curl -s -X POST localhost:8000/_edge/flush
done

echo "== 6. invalidation failure: stale reads must stay under the TTL"
scripts/cluster.sh clean >/dev/null 2>&1 || true
NODES=3 SHARDS=3 FSYNC=true EDGE=1 EDGE_FLAGS="-fail-invalidations" scripts/cluster.sh start >/dev/null; sleep 2.5
$BENCH -name cache-invalidation-failed-ttl5 -mix cget=90,put=10 -clients 50 -ttl 5 -edge http://127.0.0.1:8000 -preload=true
curl -s localhost:8000/_edge/stats | tee -a bench/results/edge-stats.jsonl; echo

scripts/cluster.sh stop >/dev/null
echo "results in $OUT"
