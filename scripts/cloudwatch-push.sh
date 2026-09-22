#!/usr/bin/env bash
# Scrape /metrics from every node and push a few key series to CloudWatch.
# Usage: scripts/cloudwatch-push.sh http://ip1:8080 http://ip2:8080 ...
# Requires the aws CLI configured with cloudwatch:PutMetricData.
set -euo pipefail
NAMESPACE=${NAMESPACE:-EdgeKV}

while true; do
  for base in "$@"; do
    node=$(curl -sf -m 2 "$base/v1/status" | python3 -c 'import json,sys;print(json.load(sys.stdin)["node"])' || continue)
    curl -sf -m 2 "$base/metrics" | python3 - "$node" "$NAMESPACE" <<'EOF' | xargs -0 -I{} sh -c 'aws cloudwatch put-metric-data --cli-input-json "{}" >/dev/null'
import json, re, sys
node, ns = sys.argv[1], sys.argv[2]
want = {"edgekv_raft_is_leader", "edgekv_raft_commit_index", "edgekv_raft_replication_lag_entries",
        "edgekv_wal_size_bytes", "edgekv_raft_leader_changes_total", "edgekv_http_requests_total"}
data = []
for line in sys.stdin:
    if line.startswith("#"): continue
    m = re.match(r'(\w+)(\{[^}]*\})? (\S+)', line)
    if not m or m.group(1) not in want: continue
    dims = [{"Name": "node", "Value": node}]
    for k, v in re.findall(r'(\w+)="([^"]*)"', m.group(2) or ""):
        dims.append({"Name": k, "Value": v})
    data.append({"MetricName": m.group(1), "Dimensions": dims, "Value": float(m.group(3))})
for i in range(0, len(data), 20):   # API limit: 20 datums per call
    sys.stdout.write(json.dumps({"Namespace": ns, "MetricData": data[i:i+20]}) + "\0")
EOF
  done
  sleep 30
done
