#!/usr/bin/env bash
# Endless mixed S3 traffic for watching metrics: PUTs small / compressible /
# multipart, GETs whole and ranged, HEADs (also of missing keys), LIST, COPY,
# DELETE. Ctrl-C to stop.
#
#   ENDPOINT=http://127.0.0.1:8333 ./scripts/metrics-load.sh

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:8333}"
BUCKET="${BUCKET:-demo}"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-pigeonhole}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-pigeonholesecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true
command -v aws >/dev/null || { echo "missing dependency: aws (brew install awscli)" >&2; exit 1; }

s3() { aws --endpoint-url "$ENDPOINT" --no-cli-pager s3 "$@" >/dev/null 2>&1; }
api() { aws --endpoint-url "$ENDPOINT" --no-cli-pager s3api "$@" >/dev/null 2>&1; }

D="$(mktemp -d)"
trap 'rm -rf "$D"' EXIT
dd if=/dev/urandom of="$D/small" bs=1k count=4 status=none
dd if=/dev/urandom of="$D/mid" bs=1m count=2 status=none
dd if=/dev/urandom of="$D/big" bs=1m count=24 status=none     # multipart, several chunks
yes "pigeonhole compressible line" | head -c 8388608 >"$D/text"   # zstd-friendly

api create-bucket --bucket "$BUCKET"
i=0
while true; do
  i=$((i + 1))
  s3 cp "$D/small" "s3://$BUCKET/small-$((i % 20))"
  s3 cp "$D/text" "s3://$BUCKET/text-$((i % 5))"
  (( i % 3 == 0 )) && s3 cp "$D/mid" "s3://$BUCKET/mid-$((i % 10))"
  (( i % 10 == 1 )) && s3 cp "$D/big" "s3://$BUCKET/big-$((i % 3))"
  s3 cp "s3://$BUCKET/small-$((RANDOM % 20))" "$D/out"
  s3 cp "s3://$BUCKET/text-$((RANDOM % 5))" "$D/out"
  api get-object --bucket "$BUCKET" --key "text-$((RANDOM % 5))" --range bytes=1000000-1999999 "$D/out"
  (( i % 4 == 0 )) && s3 cp "s3://$BUCKET/big-$((RANDOM % 3))" "$D/out"
  (( i % 5 == 0 )) && api copy-object --bucket "$BUCKET" --key "copy-$((i % 4))" --copy-source "$BUCKET/small-$((RANDOM % 20))"
  s3 ls "s3://$BUCKET/"
  api head-object --bucket "$BUCKET" --key "missing-$i"
  (( i % 7 == 0 )) && s3 rm "s3://$BUCKET/small-$((RANDOM % 20))"
  echo "iteration $i"
done
