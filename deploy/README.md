# Deploying EdgeKV to AWS

Everything here is optional and costs money. Local Docker Compose is the
development target; AWS is for the final demo and the CloudFront experiments.

## What gets created

| Resource | Purpose |
|---|---|
| VPC + 3 public subnets (one per AZ) | Node failure and AZ failure are separate experiments |
| Security group | 9090 (Raft) only between nodes; 8080 (HTTP) only from CloudFront's origin-facing ranges and your IP |
| 3 × EC2 t3.small | One `edgekv` container each, host networking |
| 3 × EBS gp3 10 GB | WAL and snapshots, separate from the root volume |
| IAM role | `cloudfront:CreateInvalidation` on this distribution + CloudWatch metrics/logs |
| CloudFront distribution | `/v1/cache/*` cached (TTL from origin, capped at 300 s, query string in cache key); everything else `CachingDisabled` |

Not included, on purpose: ALB (CloudFront's origin group does failover between
nodes), NAT Gateway (nodes have public IPs; ~$32/month saved), TLS to origin
(HTTP inside the security group boundary).

## Steps

```bash
# 1. publish the image (once)
docker build -t ghcr.io/tr3nt-xie/edgekv:latest .
docker push ghcr.io/tr3nt-xie/edgekv:latest

# 2. create everything
cd deploy/terraform
terraform init
terraform apply -var "my_ip=$(curl -s https://checkip.amazonaws.com)/32" -var "ssh_key_name=<your-key>"

# 3. check
N1=$(terraform output -json nodes | jq -r .n1.http)
curl $N1/v1/status
CF=$(terraform output -raw cloudfront_url)
curl -i $CF/v1/cache/config:model:v12

# 4. run the experiments (see docs/experiments.md), then ALWAYS
terraform destroy
```

## Budget guard

Set a $20 budget alarm in AWS Budgets before `apply`. With the resources above
a full day costs about $2; the risk is forgetting to destroy.

## CloudWatch

Nodes expose Prometheus metrics at `:8080/metrics`. To ship them to CloudWatch
install the CloudWatch agent on each instance with a Prometheus scrape config,
or run `scripts/cloudwatch-push.sh` from your laptop, which scrapes `/metrics`
and calls `put-metric-data` every 30 s.
