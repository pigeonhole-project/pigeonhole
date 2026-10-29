#!/usr/bin/env bash
# Endless mixed S3 traffic for watching metrics: PUTs small / compressible /
# multipart, GETs whole and ranged, HEADs (also of missing keys), LIST, COPY,
# DELETE. Ctrl-C to stop.
#
#   ENDPOINT=http://127.0.0.1:8333 ./scripts/metrics-load.sh
#   LOAD_PAUSE=10 …    # seconds between a client's iterations (a chat store's rate limits)
#   LOAD_CLIENTS=4 …   # clients side by side (default 4)

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:8333}"
BUCKET="${BUCKET:-demo}"
PAUSE="${LOAD_PAUSE:-0}"
CLIENTS="${LOAD_CLIENTS:-4}"
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

# A chat store's log commits in seconds, more when rate limited: wait long.
AWS=(aws --endpoint-url "$ENDPOINT" --no-cli-pager --cli-read-timeout 300)
s3() { "${AWS[@]}" s3 "$@" >/dev/null 2>&1; }
api() { "${AWS[@]}" s3api "$@" >/dev/null 2>&1; }

D="$(mktemp -d)"
dd if=/dev/urandom of="$D/small" bs=1k count=4 status=none
dd if=/dev/urandom of="$D/mid" bs=1m count=2 status=none
dd if=/dev/urandom of="$D/big" bs=1m count=24 status=none     # multipart, several chunks
yes "pigeonhole compressible line" | head -c 8388608 >"$D/text"   # zstd-friendly

until api head-bucket --bucket "$BUCKET" || api create-bucket --bucket "$BUCKET"; do
  echo "creating bucket $BUCKET failed; retrying" >&2
  sleep 5
done
# One client: its own keys, one request at a time.
client() {
  local c="$1" i=0 out="$D/out-$1" p="c$1"
  while true; do
    i=$((i + 1))
    s3 cp "$D/small" "s3://$BUCKET/$p/small-$((i % 20))"
    s3 cp "$D/text" "s3://$BUCKET/$p/text-$((i % 5))"
    (( i % 3 == 0 )) && s3 cp "$D/mid" "s3://$BUCKET/$p/mid-$((i % 10))"
    (( i % 10 == 1 )) && s3 cp "$D/big" "s3://$BUCKET/$p/big-$((i % 3))"
    s3 cp "s3://$BUCKET/$p/small-$((RANDOM % 20))" "$out"
    s3 cp "s3://$BUCKET/$p/text-$((RANDOM % 5))" "$out"
    api get-object --bucket "$BUCKET" --key "$p/text-$((RANDOM % 5))" --range bytes=1000000-1999999 "$out"
    (( i % 4 == 0 )) && s3 cp "s3://$BUCKET/$p/big-$((RANDOM % 3))" "$out"
    (( i % 5 == 0 )) && api copy-object --bucket "$BUCKET" --key "$p/copy-$((i % 4))" --copy-source "$BUCKET/$p/small-$((RANDOM % 20))"
    s3 ls "s3://$BUCKET/$p/"
    api head-object --bucket "$BUCKET" --key "$p/missing-$i"
    (( i % 7 == 0 )) && s3 rm "s3://$BUCKET/$p/small-$((RANDOM % 20))"
    echo "client $c iteration $i"
    sleep "$PAUSE"
  done
}

# Clients run side by side, so their metadata proposals meet in the
# tablet's batcher and share log entries.
trap 'exit 0' INT TERM
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$D"' EXIT
for c in $(seq 1 "$CLIENTS"); do
  client "$c" &
done
wait
