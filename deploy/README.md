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

## Prerequisites (one-time)

1. **AWS credentials** for Terraform. Either install the CLI and run
   `aws configure`, or export them:
   ```bash
   export AWS_ACCESS_KEY_ID=...
   export AWS_SECRET_ACCESS_KEY=...
   export AWS_REGION=us-west-2
   ```
   The IAM user needs EC2, VPC, EBS, IAM (role/profile creation) and CloudFront
   permissions. `AdministratorAccess` on a throwaway user is the pragmatic choice
   for a personal project.

2. **An EC2 key pair in the region.** Create it in the console
   (EC2 → Key Pairs → Create, type ED25519 or RSA, format `.pem`) and save the
   file, e.g. `~/.ssh/edgekv.pem`, then `chmod 600 ~/.ssh/edgekv.pem`.
   Terraform uses it to SSH in and start the containers.

3. **A published container image.** The instances pull it, so it must be
   reachable without credentials. With GitHub Container Registry:
   ```bash
   echo $GITHUB_TOKEN | docker login ghcr.io -u Tr3nt-Xie --password-stdin   # token needs write:packages
   docker build --platform linux/amd64 -t ghcr.io/tr3nt-xie/edgekv:latest .
   docker push ghcr.io/tr3nt-xie/edgekv:latest
   ```
   Then on GitHub: your profile → Packages → edgekv → Package settings →
   **Change visibility → Public**. (`--platform linux/amd64` matters: the
   instances are x86, your Mac is ARM.)

4. **Terraform or OpenTofu.** `brew install opentofu` and use `tofu` wherever
   the steps below say `terraform`; they are command-compatible.

5. **A budget alarm.** AWS Budgets → create a $20 budget with an email alert.
   The resources cost about $2/day; the risk is forgetting to destroy them.

## Steps

```bash
cd deploy/terraform
terraform init
terraform apply \
  -var "my_ip=$(curl -s https://checkip.amazonaws.com)/32" \
  -var "ssh_key_name=edgekv" \
  -var "ssh_private_key=~/.ssh/edgekv.pem"
```

`apply` takes 5–10 minutes; CloudFront distribution creation is the slow part.
The last phase SSHes into each instance, installs Docker, formats and mounts
the EBS volume and starts the container.

Check:

```bash
terraform output                                  # node IPs and the CloudFront URL
N1=$(terraform output -json nodes | jq -r .n1.http)
curl $N1/v1/status
curl -X PUT $N1/v1/kv/config:model -d '{"value":"v12","cache_policy":{"cacheable":true,"ttl_seconds":30}}'
CF=$(terraform output -raw cloudfront_url)
curl -i $CF/v1/cache/config:model | grep -iE 'x-cache|age|cache-control'   # second call: "Hit from cloudfront"
```

Run the experiments from your laptop against the cluster (see
`docs/experiments.md`, "To measure on AWS"):

```bash
NODES=$(terraform output -json nodes | jq -r '[.[].http] | join(",")')
go run ./cmd/edgekv-bench -nodes "$NODES" -name aws-3node-3shard -clients 50 -out bench/results/aws.jsonl
go run ./cmd/edgekv-bench -nodes "$NODES" -edge "$CF" -mix cget=90,put=10 -name aws-cache-ttl30 -clients 50 -out bench/results/aws.jsonl
```

Fault demo: stop an instance in the console (or `aws ec2 stop-instances`),
keep writing, start it again; `curl $N2/v1/status` shows it catching up.

When done — **always**:

```bash
terraform destroy
```

Then verify in the console that no EC2 instances, EBS volumes or CloudFront
distributions remain.

## Troubleshooting

- **`apply` hangs at "Still creating... null_resource.bootstrap"**: SSH is not
  reachable. Check that `my_ip` is your current public IP (it changes on
  different networks) and that the key pair name matches the region.
- **Container keeps restarting**: `ssh -i ~/.ssh/edgekv.pem ec2-user@<ip> sudo docker logs edgekv`.
  A `permission denied` on `/data` means the volume mount step failed; an image
  pull error means the package is still private.
- **CloudFront returns 502**: the origin security group must allow the
  CloudFront prefix list on 8080 (it does by default) and the nodes must be up.
- **Changed the image?** `terraform apply` again does not restart containers.
  SSH in and `sudo docker pull ... && sudo docker restart edgekv`, or
  `terraform taint null_resource.bootstrap[0]` etc. before `apply`.

## CloudWatch

Nodes expose Prometheus metrics at `:8080/metrics`. To ship them to CloudWatch,
run `scripts/cloudwatch-push.sh $N1 $N2 $N3` from your laptop (needs the AWS
CLI and `cloudwatch:PutMetricData`); it scrapes every 30 s. Then build a
dashboard on the `EdgeKV` namespace: `edgekv_raft_is_leader` per shard,
`edgekv_raft_replication_lag_entries`, `edgekv_raft_leader_changes_total`.
