#!/bin/bash
# EC2 user data for chainscan (Amazon Linux 2023). Placeholders __REGION__ __PARAM__ __REPO__ __BRANCH__
# are filled in at launch. No secrets here: the GitHub read token and app keys live in SSM Parameter Store.
set -euxo pipefail
dnf install -y docker git
systemctl enable --now docker
ARCH=$(uname -m)
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-${ARCH}" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
mkdir -p /opt/chainscan/{data,caddy}
cat > /opt/chainscan/deploy.conf <<CONF
REGION=__REGION__
PARAM=__PARAM__
REPO=__REPO__
BRANCH=__BRANCH__
CONF
GH=$(aws ssm get-parameter --name "__PARAM__/deploy/GITHUB_TOKEN" --with-decryption --region __REGION__ --query Parameter.Value --output text)
git clone --depth 1 -b __BRANCH__ "https://x-access-token:${GH}@github.com/__REPO__.git" /tmp/chainscan-src
install -m 755 /tmp/chainscan-src/deploy/ec2/deploy.sh /opt/chainscan/deploy.sh
rm -rf /tmp/chainscan-src
/opt/chainscan/deploy.sh
