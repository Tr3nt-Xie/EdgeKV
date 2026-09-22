#!/usr/bin/env bash
# Run a local EdgeKV cluster as plain processes (no Docker).
#
#   scripts/cluster.sh start            start n1 n2 n3 (and the edge cache if EDGE=1)
#   scripts/cluster.sh stop             stop everything
#   scripts/cluster.sh kill n2          kill -9 one node (simulates a crash)
#   scripts/cluster.sh restart n2       start it again from its data directory
#   scripts/cluster.sh status           show the leader of every shard
#   scripts/cluster.sh clean            stop and delete all data
#
# Environment: NODES (default 3), SHARDS (3), FSYNC (true), EDGE (0),
#              RUN_DIR (./run), EXTRA (extra flags for every node)
set -euo pipefail

cd "$(dirname "$0")/.."
NODES=${NODES:-3}
SHARDS=${SHARDS:-3}
FSYNC=${FSYNC:-true}
EDGE=${EDGE:-0}
RUN_DIR=${RUN_DIR:-./run}
EXTRA=${EXTRA:-}
BIN=$RUN_DIR/bin

http_port() { echo $((8080 + ${1#n})); }   # n1 -> 8081
raft_port() { echo $((9090 + ${1#n})); }   # n1 -> 9091

cluster_spec() {
  local spec=""
  for i in $(seq 1 "$NODES"); do
    spec+="n$i=127.0.0.1:$(raft_port n$i)|http://127.0.0.1:$(http_port n$i),"
  done
  echo "${spec%,}"
}

build() {
  mkdir -p "$BIN"
  go build -o "$BIN/" ./cmd/...
}

start_node() {
  local id=$1 invalidator="none"
  [[ $EDGE == 1 ]] && invalidator="edge:http://127.0.0.1:8000"
  mkdir -p "$RUN_DIR/$id"
  # shellcheck disable=SC2086
  nohup "$BIN/edgekv" -id "$id" -cluster "$(cluster_spec)" \
    -http "127.0.0.1:$(http_port "$id")" -raft "127.0.0.1:$(raft_port "$id")" \
    -data "$RUN_DIR/$id/data" -shards "$SHARDS" -fsync="$FSYNC" \
    -invalidator "$invalidator" $EXTRA \
    >>"$RUN_DIR/$id/log.jsonl" 2>&1 &
  echo $! >"$RUN_DIR/$id/pid"
  echo "started $id (pid $!, http :$(http_port "$id"))"
}

stop_pidfile() {
  local pidfile=$1 sig=${2:-TERM}
  [[ -f $pidfile ]] || return 0
  kill "-$sig" "$(cat "$pidfile")" 2>/dev/null || true
  rm -f "$pidfile"
}

case ${1:-} in
  start)
    build
    for i in $(seq 1 "$NODES"); do start_node "n$i"; done
    if [[ $EDGE == 1 ]]; then
      origins=""
      for i in $(seq 1 "$NODES"); do origins+="http://127.0.0.1:$(http_port n$i),"; done
      mkdir -p "$RUN_DIR/edge"
      # shellcheck disable=SC2086
      nohup "$BIN/edgekv-edge" -listen 127.0.0.1:8000 -origins "${origins%,}" ${EDGE_FLAGS:-} \
        >>"$RUN_DIR/edge/log.jsonl" 2>&1 &
      echo $! >"$RUN_DIR/edge/pid"
      echo "started edge cache (pid $!, http :8000)"
    fi
    ;;
  stop)
    for d in "$RUN_DIR"/*/; do stop_pidfile "$d/pid"; done
    ;;
  kill)
    stop_pidfile "$RUN_DIR/$2/pid" KILL
    echo "killed $2"
    ;;
  restart)
    build
    start_node "$2"
    ;;
  status)
    for i in $(seq 1 "$NODES"); do
      if out=$(curl -fsS -m 1 "http://127.0.0.1:$(http_port n$i)/v1/status" 2>/dev/null); then
        echo "$out" | python3 -c '
import json, sys
st = json.load(sys.stdin)
parts = []
for s in st["shards"]:
    parts.append("shard%d=%s(leader=%s,term=%d,commit=%d)" % (
        s["shard"], s["state"], s["leader"] or "?", s["term"], s["commit_index"]))
print(st["node"] + ": " + "  ".join(parts))'
      else
        echo "n$i: down"
      fi
    done
    ;;
  clean)
    "$0" stop
    rm -rf "$RUN_DIR"
    ;;
  *)
    sed -n '2,14p' "$0"
    exit 1
    ;;
esac
