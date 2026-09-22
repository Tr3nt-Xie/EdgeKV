# EdgeKV on AWS: three EC2 nodes with EBS gp3 data volumes inside one VPC,
# a security group that exposes the HTTP API only to the CDN / operator and
# keeps Raft traffic inside the group, and a CloudFront distribution whose
# only cacheable behaviour is /v1/cache/*.
#
#   terraform init
#   terraform apply -var "my_ip=$(curl -s https://checkip.amazonaws.com)/32"
#   terraform output
#   terraform destroy      # always, when done: budget guard
#
# Cost: 3 × t3.small + 3 × 10 GB gp3 ≈ $0.08/hour. CloudFront is pay per
# request and effectively free at demo volumes.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" { default = "us-west-2" }
variable "instance_type" { default = "t3.small" }
variable "my_ip" { description = "CIDR allowed to reach the HTTP API and SSH directly, e.g. 1.2.3.4/32" }
variable "ssh_key_name" { default = null }
variable "image" { default = "ghcr.io/tr3nt-xie/edgekv:latest" }
variable "shards" { default = 3 }

locals {
  nodes = ["n1", "n2", "n3"]
  # id=raftAddr|httpURL for every node, using private IPs (stable inside the VPC)
  cluster_spec = join(",", [for i, n in local.nodes :
  "${n}=${aws_instance.node[i].private_ip}:9090|http://${aws_instance.node[i].private_ip}:8080"])
}

# --- network -----------------------------------------------------------------

data "aws_availability_zones" "available" {}

resource "aws_vpc" "edgekv" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "edgekv" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.edgekv.id
}

# One public subnet per AZ: a node failure and an AZ failure are then
# different experiments.
resource "aws_subnet" "public" {
  count                   = 3
  vpc_id                  = aws_vpc.edgekv.id
  cidr_block              = "10.42.${count.index}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "edgekv-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.edgekv.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "public" {
  count          = 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "node" {
  name   = "edgekv-node"
  vpc_id = aws_vpc.edgekv.id

  # Raft gRPC: only from other nodes.
  ingress {
    from_port = 9090
    to_port   = 9090
    protocol  = "tcp"
    self      = true
  }
  # HTTP API: from CloudFront's origin-facing ranges and from the operator.
  ingress {
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.cloudfront.id]
    cidr_blocks     = [var.my_ip]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

# --- instances ---------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_iam_role" "node" {
  name = "edgekv-node"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

# The node needs exactly two permissions: invalidate its own distribution and
# push metrics/logs to CloudWatch.
resource "aws_iam_role_policy" "node" {
  role = aws_iam_role.node.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["cloudfront:CreateInvalidation"], Resource = aws_cloudfront_distribution.edge.arn },
      { Effect = "Allow", Action = ["cloudwatch:PutMetricData", "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], Resource = "*" },
    ]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "edgekv-node"
  role = aws_iam_role.node.name
}

resource "aws_instance" "node" {
  count                  = 3
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public[count.index].id
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name
  key_name               = var.ssh_key_name
  tags                   = { Name = "edgekv-${local.nodes[count.index]}" }
  lifecycle { ignore_changes = [ami] }
}

# Separate EBS volume for WAL + snapshots so that the root volume and the data
# live on different devices, and so the data survives an instance replacement.
resource "aws_ebs_volume" "data" {
  count             = 3
  availability_zone = aws_subnet.public[count.index].availability_zone
  size              = 10
  type              = "gp3"
  iops              = 3000
  throughput        = 125
  tags              = { Name = "edgekv-${local.nodes[count.index]}-data" }
}

resource "aws_volume_attachment" "data" {
  count       = 3
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data[count.index].id
  instance_id = aws_instance.node[count.index].id
}

# Cluster membership needs every private IP, which only exists after the
# instances are created; so the container is started by a second-phase
# provisioner rather than user_data.
resource "null_resource" "bootstrap" {
  count = 3
  triggers = {
    instance = aws_instance.node[count.index].id
    spec     = local.cluster_spec
  }
  connection {
    type        = "ssh"
    host        = aws_instance.node[count.index].public_ip
    user        = "ec2-user"
    private_key = var.ssh_key_name != null ? file("~/.ssh/${var.ssh_key_name}.pem") : null
  }
  provisioner "remote-exec" {
    inline = [
      "sudo dnf install -y docker && sudo systemctl enable --now docker",
      # Format the data volume on first boot only, then mount it.
      "if ! sudo blkid /dev/xvdf; then sudo mkfs.ext4 /dev/xvdf; fi",
      "sudo mkdir -p /data && sudo mount /dev/xvdf /data && sudo chown 10001:10001 /data",
      "echo '/dev/xvdf /data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab",
      "sudo docker rm -f edgekv || true",
      join(" ", [
        "sudo docker run -d --name edgekv --restart unless-stopped --network host -v /data:/data",
        "-e EDGEKV_ID=${local.nodes[count.index]}",
        "-e 'EDGEKV_CLUSTER=${local.cluster_spec}'",
        "-e EDGEKV_SHARDS=${var.shards}",
        "-e EDGEKV_INVALIDATOR=cloudfront:${aws_cloudfront_distribution.edge.id}",
        "-e AWS_REGION=${var.region}",
        var.image,
      ]),
    ]
  }
  depends_on = [aws_volume_attachment.data]
}

# --- CloudFront --------------------------------------------------------------

resource "aws_cloudfront_cache_policy" "cache_path" {
  name        = "edgekv-cache-path"
  min_ttl     = 0
  default_ttl = 30
  max_ttl     = 300 # the origin's max-age wins when lower; this caps it
  parameters_in_cache_key_and_forwarded_to_origin {
    # ?v=N must be part of the cache key for versioned URLs to work.
    query_strings_config { query_string_behavior = "all" }
    headers_config { header_behavior = "none" }
    cookies_config { cookie_behavior = "none" }
    enable_accept_encoding_gzip = true
  }
}

resource "aws_cloudfront_origin_request_policy" "all_viewer" {
  name = "edgekv-forward-all"
  query_strings_config { query_string_behavior = "all" }
  headers_config { header_behavior = "allViewer" }
  cookies_config { cookie_behavior = "none" }
}

resource "aws_cloudfront_distribution" "edge" {
  enabled         = true
  comment         = "EdgeKV"
  price_class     = "PriceClass_100"
  http_version    = "http2and3"
  is_ipv6_enabled = true

  # All three nodes are origins; any of them proxies to the right leader.
  dynamic "origin" {
    for_each = aws_instance.node
    content {
      origin_id   = "edgekv-${local.nodes[origin.key]}"
      domain_name = origin.value.public_dns
      custom_origin_config {
        http_port              = 8080
        https_port             = 443
        origin_protocol_policy = "http-only"
        origin_ssl_protocols   = ["TLSv1.2"]
      }
    }
  }
  origin_group {
    origin_id = "edgekv-nodes"
    failover_criteria { status_codes = [502, 503, 504] }
    member { origin_id = "edgekv-n1" }
    member { origin_id = "edgekv-n2" }
  }

  # Default: never cache. /v1/kv, /v1/status and everything else pass through.
  default_cache_behavior {
    target_origin_id         = "edgekv-nodes"
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad" # AWS managed CachingDisabled
    origin_request_policy_id = aws_cloudfront_origin_request_policy.all_viewer.id
  }

  # The one cacheable path.
  ordered_cache_behavior {
    path_pattern             = "/v1/cache/*"
    target_origin_id         = "edgekv-nodes"
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["GET", "HEAD"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = aws_cloudfront_cache_policy.cache_path.id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.all_viewer.id
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }
  viewer_certificate { cloudfront_default_certificate = true }
}

# --- outputs -----------------------------------------------------------------

output "nodes" {
  value = { for i, n in local.nodes : n => {
    public_ip  = aws_instance.node[i].public_ip
    private_ip = aws_instance.node[i].private_ip
    http       = "http://${aws_instance.node[i].public_ip}:8080"
  } }
}

output "cloudfront_url" {
  value = "https://${aws_cloudfront_distribution.edge.domain_name}"
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.edge.id
}
