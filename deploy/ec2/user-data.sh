#!/bin/bash
# EC2 user data for chainscan (Amazon Linux 2023). Placeholders __REGION__ __BUCKET__ __KEY__ __PARAM__
# are filled in at launch. Secrets are NOT here: deploy.sh reads them from SSM Parameter Store.
set -euxo pipefail
dnf install -y docker
systemctl enable --now docker
ARCH=$(uname -m)
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-${ARCH}" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
mkdir -p /opt/chainscan/{data,caddy}
cat > /opt/chainscan/deploy.conf <<CONF
REGION=__REGION__
BUCKET=__BUCKET__
KEY=__KEY__
PARAM=__PARAM__
CONF
aws s3 cp "s3://__BUCKET__/__KEY__" /tmp/bundle.tgz --region __REGION__
mkdir -p /tmp/bundle && tar -xf /tmp/bundle.tgz -C /tmp/bundle
install -m 755 /tmp/bundle/deploy/ec2/deploy.sh /opt/chainscan/deploy.sh
/opt/chainscan/deploy.sh
