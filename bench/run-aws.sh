#!/usr/bin/env bash
# Run the experiment matrix against the AWS cluster created by deploy/terraform.
# The load generator runs inside the VPC (on n1) so that public-internet RTT
# does not dominate the closed-loop latency. Cache experiments run from the
# laptop through CloudFront, because that is the path real clients take.
#
#   bench/run-aws.sh            (needs: terraform outputs, ~/.ssh/edgekv.pem)
set -euo pipefail
cd "$(dirname "$0")/.."
KEY=${KEY:-~/.ssh/edgekv.pem}
OUT=bench/results/aws.jsonl
mkdir -p bench/results
TF=deploy/terraform

eval "$(cd $TF && tofu output -json nodes | python3 -c '
import json,sys
n=json.load(sys.stdin)
for k,v in n.items(): print("%s_PUB=%s; %s_PRIV=%s" % (k.upper(), v["public_ip"], k.upper(), v["private_ip"]))')"
CF=$(cd $TF && tofu output -raw cloudfront_url)
DIST=$(cd $TF && tofu output -raw cloudfront_distribution_id)
SPEC="n1=$N1_PRIV:9090|http://$N1_PRIV:8080,n2=$N2_PRIV:9090|http://$N2_PRIV:8080,n3=$N3_PRIV:9090|http://$N3_PRIV:8080"
PRIV_NODES="http://$N1_PRIV:8080,http://$N2_PRIV:8080,http://$N3_PRIV:8080"
PUB_NODES="http://$N1_PUB:8080,http://$N2_PUB:8080,http://$N3_PUB:8080"

ssh_() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o LogLevel=ERROR "ec2-user@$1" "${@:2}"; }

# restart_cluster SHARDS FSYNC — wipes data so every run starts identical
restart_cluster() {
  for i in 1 2 3; do
    ip=$(eval echo \$N${i}_PUB)
    ssh_ "$ip" "sudo docker rm -f edgekv >/dev/null 2>&1; sudo rm -rf /data/shard-*; sudo docker run -d --name edgekv --restart unless-stopped --network host -v /data:/data -e EDGEKV_ID=n$i -e 'EDGEKV_CLUSTER=$SPEC' -e EDGEKV_SHARDS=$1 -e EDGEKV_FSYNC=$2 -e EDGEKV_INVALIDATOR=cloudfront:$DIST -e AWS_REGION=us-west-2 ghcr.io/tr3nt-xie/edgekv:latest >/dev/null" &
  done
  wait
  sleep 6
}

# bench_in_vpc NAME ARGS... — runs edgekv-bench on n1, appends the JSON line to $OUT
bench_in_vpc() {
  local name=$1; shift
  ssh_ "$N1_PUB" "sudo docker run --rm --network host --entrypoint edgekv-bench ghcr.io/tr3nt-xie/edgekv:latest -nodes $PRIV_NODES -name $name -duration 10s -warmup 2s -keys 20000 $*" 2>/dev/null | tail -1 | tee -a "$OUT" | python3 -c '
import json,sys; r=json.loads(sys.stdin.read()); l=r["latency_ms"]["all"]
print("%s: %d clients, %.0f ops/s, p50 %.2fms p95 %.2fms p99 %.2fms, %d errors" % (r["name"], r["clients"], r["ops_per_s"], l["p50"], l["p95"], l["p99"], r["errors"]))'
}

echo "== 1. topology (fsync on, 50 clients, 80/15/5)"
restart_cluster 1 true;  bench_in_vpc aws-topo-3node-1shard -clients 50
restart_cluster 3 true;  bench_in_vpc aws-topo-3node-3shard -clients 50
restart_cluster 6 true;  bench_in_vpc aws-topo-3node-6shard -clients 50

echo "== 2. durability (3 shards, write-only, 50 clients)"
bench_in_vpc aws-fsync-on -mix put=100 -clients 50
restart_cluster 3 false; bench_in_vpc aws-fsync-off -mix put=100 -clients 50

echo "== 3. concurrency (3 shards, fsync on)"
restart_cluster 3 true
for c in 1 10 50 100 500; do bench_in_vpc "aws-clients-$c" -clients "$c" -preload=false; done

echo "== 4. cache via CloudFront, from the laptop (90% cached GET / 10% PUT, 20 clients)"
go run ./cmd/edgekv-bench -nodes "$PUB_NODES" -name aws-cache-origin -mix cget=100 -clients 20 -keys 2000 -duration 10s -warmup 2s -out $OUT 2>&1 | tail -1
for ttl in 5 30; do
  go run ./cmd/edgekv-bench -nodes "$PUB_NODES" -edge "$CF" -name "aws-cache-cf-ttl$ttl" -mix cget=90,put=10 -clients 20 -keys 2000 -ttl $ttl -duration 20s -warmup 2s -out $OUT 2>&1 | tail -1
done
echo "results in $OUT"
