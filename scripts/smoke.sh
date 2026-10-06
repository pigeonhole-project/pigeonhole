#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:8333}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3gram}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

TMP="$(mktemp)"
OUT="$(mktemp)"
trap 'rm -f "$TMP" "$OUT"' EXIT

echo "hello s3gram $(date)" >"$TMP"

aws --endpoint-url "$ENDPOINT" s3 mb "s3://demo" || true
aws --endpoint-url "$ENDPOINT" s3 cp "$TMP" "s3://demo/smoke.txt"
aws --endpoint-url "$ENDPOINT" s3 ls "s3://demo/"
aws --endpoint-url "$ENDPOINT" s3 cp "s3://demo/smoke.txt" "$OUT"
cmp -s "$TMP" "$OUT"
aws --endpoint-url "$ENDPOINT" s3 rm "s3://demo/smoke.txt"
echo "smoke ok"
