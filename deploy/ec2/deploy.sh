#!/bin/bash
# (Re)deploy chainscan on this instance: fetch the code bundle from S3, load secrets from
# SSM Parameter Store, rebuild and restart. Running jobs are interrupted; they resume from checkpoints.
set -euxo pipefail
source /opt/chainscan/deploy.conf      # REGION, BUCKET, KEY, PARAM
cd /opt/chainscan
aws s3 cp "s3://${BUCKET}/${KEY}" /opt/chainscan/bundle.tgz --region "$REGION"
rm -rf src.new && mkdir src.new && tar -xzf bundle.tgz -C src.new
rm -rf src && mv src.new src
cp src/deploy/ec2/docker-compose.yml src/deploy/ec2/Caddyfile /opt/chainscan/
# secrets -> app.env (never stored in user data or the bundle)
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
