#!/usr/bin/env bash
# Quick AWS CLI smoke against a running s3gram instance.
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:8333}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3gram}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
fi

need() { command -v "$1" >/dev/null || { echo "missing dependency: $1" >&2; exit 1; }; }
need aws
need dd
need cmp

TMP="$(mktemp)"
OUT="$(mktemp)"
BIG="$(mktemp)"
trap 'rm -f "$TMP" "$OUT" "$BIG"' EXIT

echo "hello s3gram $(date)" >"$TMP"
dd if=/dev/urandom of="$BIG" bs=1m count=12 status=none

echo "== basic put/get/list/rm =="
aws --endpoint-url "$ENDPOINT" s3 mb "s3://demo" 2>/dev/null || true
aws --endpoint-url "$ENDPOINT" s3 cp "$TMP" "s3://demo/smoke.txt"
aws --endpoint-url "$ENDPOINT" s3 ls "s3://demo/" >/dev/null
aws --endpoint-url "$ENDPOINT" s3 cp "s3://demo/smoke.txt" "$OUT"
cmp -s "$TMP" "$OUT"

echo "== multipart (default aws cli threshold) =="
aws --endpoint-url "$ENDPOINT" s3 cp "$BIG" "s3://demo/multipart.bin"
aws --endpoint-url "$ENDPOINT" s3 cp "s3://demo/multipart.bin" "$OUT"
cmp -s "$BIG" "$OUT"

echo "== copy + metadata =="
aws --endpoint-url "$ENDPOINT" s3api put-object \
  --bucket demo --key meta.txt --body "$TMP" \
  --metadata "author=s3gram,env=smoke" >/dev/null
aws --endpoint-url "$ENDPOINT" s3 cp "s3://demo/meta.txt" "s3://demo/meta-copy.txt"
meta="$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket demo --key meta-copy.txt)"
echo "$meta" | grep -q '"author": "s3gram"'
echo "$meta" | grep -q '"env": "smoke"'

echo "== cleanup =="
aws --endpoint-url "$ENDPOINT" s3 rm "s3://demo/smoke.txt" >/dev/null
aws --endpoint-url "$ENDPOINT" s3 rm "s3://demo/multipart.bin" >/dev/null
aws --endpoint-url "$ENDPOINT" s3 rm "s3://demo/meta.txt" >/dev/null
aws --endpoint-url "$ENDPOINT" s3 rm "s3://demo/meta-copy.txt" >/dev/null

echo "smoke ok"
