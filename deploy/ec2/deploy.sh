#!/bin/bash
# (Re)deploy chainscan on this instance: pull the latest code from GitHub, load settings and secrets
# from SSM Parameter Store, rebuild and restart. Running scans are interrupted; press Resume after.
#   run remotely:  aws ssm send-command --document-name AWS-RunShellScript \
#                    --targets Key=tag:Name,Values=chainscan --parameters commands=/opt/chainscan/deploy.sh
set -euxo pipefail
source /opt/chainscan/deploy.conf      # REGION, PARAM, REPO, BRANCH
cd /opt/chainscan
GH=$(aws ssm get-parameter --name "${PARAM}/deploy/GITHUB_TOKEN" --with-decryption --region "$REGION" \
     --query Parameter.Value --output text)
rm -rf src.new
git clone --depth 1 -b "$BRANCH" "https://x-access-token:${GH}@github.com/${REPO}.git" src.new
git -C src.new remote set-url origin "https://github.com/${REPO}.git"   # don't leave the token on disk
rm -rf src && mv src.new src
cp src/deploy/ec2/docker-compose.yml src/deploy/ec2/Caddyfile /opt/chainscan/
install -m 755 src/deploy/ec2/deploy.sh /opt/chainscan/deploy.sh
# app settings and secrets -> app.env (top-level parameters only; deploy/* stays out of the app)
aws ssm get-parameters-by-path --path "$PARAM" --with-decryption --region "$REGION" \
  --query 'Parameters[].[Name,Value]' --output text | while IFS=$'\t' read -r name value; do
    echo "${name##*/}=${value}"; done > app.env.new
chmod 600 app.env.new && mv app.env.new app.env
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
SITE=$(grep '^SITE_HOST=' app.env | cut -d= -f2- || true)   # set at launch from the Elastic IP
echo "SITE_HOST=${SITE:-${IP//./-}.sslip.io}" > .env
docker compose up -d --build
docker image prune -f
