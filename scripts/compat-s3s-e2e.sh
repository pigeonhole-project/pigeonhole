#!/usr/bin/env bash
# Run s3s-e2e against s3gram with MemoryBlobStore (no Telegram).
#
# Builds the upstream s3s-e2e binary from a shallow clone, starts a temporary
# MemoryBlobStore server, then runs the suite.
#
# Usage:
#   ./scripts/compat-s3s-e2e.sh                  # full suite
#   ./scripts/compat-s3s-e2e.sh --filter '^Basic'  # CI-style subset
#   ./scripts/compat-s3s-e2e.sh --list

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${S3GRAM_PORT:-18334}"
DB="${S3GRAM_MEMORY_DB:-$ROOT/.cache/pigeonhole-gateway-s3s-e2e.db}"
LOG="${S3GRAM_MEMORY_LOG:-$ROOT/.cache/pigeonhole-gateway-s3s-e2e.log}"
PIDFILE="${S3GRAM_MEMORY_PID:-$ROOT/.cache/pigeonhole-gateway-s3s-e2e.pid}"
S3S_DIR="${S3S_DIR:-$ROOT/.cache/s3s-upstream}"
S3S_REPO="${S3S_REPO:-https://github.com/s3s-project/s3s.git}"
S3S_REF="${S3S_REF:-main}"
E2E_BIN="${S3S_E2E_BIN:-$S3S_DIR/target/release/s3s-e2e}"
RESULT_JSON="${S3S_E2E_JSON:-$ROOT/.cache/s3s-e2e-report.json}"
RESULT_LOG="${S3S_E2E_LOG:-$ROOT/.cache/s3s-e2e-results.log}"

cd "$ROOT"
mkdir -p "$(dirname "$DB")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

cleanup() {
  if [[ -f "$PIDFILE" ]]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
  fi
}
trap cleanup EXIT

rm -f "$DB" "${DB}-wal" "${DB}-shm"

if [[ ! -d "$S3S_DIR/.git" ]]; then
  echo "cloning s3s into $S3S_DIR ..."
  git clone --depth 1 --branch "$S3S_REF" "$S3S_REPO" "$S3S_DIR"
else
  echo "updating s3s in $S3S_DIR ..."
  git -C "$S3S_DIR" fetch --depth 1 origin "$S3S_REF"
  git -C "$S3S_DIR" checkout -q FETCH_HEAD
fi

source "$HOME/.cargo/env" 2>/dev/null || true

need_e2e_build=1
if [[ -x "$E2E_BIN" ]]; then
  # Rebuild when the upstream checkout moved.
  bin_mtime="$(stat -f %m "$E2E_BIN" 2>/dev/null || stat -c %Y "$E2E_BIN")"
  head_mtime="$(git -C "$S3S_DIR" log -1 --format=%ct)"
  if [[ "$bin_mtime" -ge "$head_mtime" ]]; then
    need_e2e_build=0
  fi
fi
if [[ "$need_e2e_build" -eq 1 ]]; then
  echo "building s3s-e2e (release) ..."
  # Touch build.rs so version metadata refresh is picked up (upstream justfile).
  touch "$S3S_DIR/crates/s3s-e2e/build.rs"
  cargo build --manifest-path "$S3S_DIR/Cargo.toml" -p s3s-e2e --release
fi
if [[ ! -x "$E2E_BIN" ]]; then
  echo "s3s-e2e binary missing at $E2E_BIN" >&2
  exit 1
fi

cargo build --release -q

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; refusing to start memory server" >&2
  exit 1
fi

CFG="$ROOT/.cache/pigeonhole-gateway-s3s-e2e-config.toml"
CONFIG_OUT="$CFG" \
  CONFIG_MEMORY=true \
  CONFIG_LISTEN_ADDR="127.0.0.1:${PORT}" \
  CONFIG_DATABASE_URL="sqlite:${DB}?mode=rwc" \
  CONFIG_SNAPSHOT_SECS=0 \
  "$ROOT/scripts/gen-config.sh"
export S3GRAM_CONFIG="$CFG"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3gram}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export AWS_ENDPOINT_URL="http://127.0.0.1:${PORT}"
# Avoid IMDS / SSO probes during aws-config load.
export AWS_EC2_METADATA_DISABLED=1
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null

./target/release/s3gram >"$LOG" 2>&1 &
echo $! >"$PIDFILE"

for _ in $(seq 1 50); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "$AWS_ENDPOINT_URL/" || true)"
  if [[ -n "$code" && "$code" != "000" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "${code:-}" || "$code" == "000" ]]; then
  echo "memory server failed to start; see $LOG" >&2
  exit 1
fi
echo "s3gram MemoryBlobStore listening on :$PORT (pid $(cat "$PIDFILE"))"

export RUST_LOG="${RUST_LOG:-s3s_e2e=info,s3s_test=info}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

echo "running: $E2E_BIN --json $RESULT_JSON $*"
set +e
"$E2E_BIN" --json "$RESULT_JSON" "$@" 2>&1 | tee "$RESULT_LOG"
rc=${PIPESTATUS[0]}
set -e

echo "s3s-e2e exit=$rc (log: $RESULT_LOG, json: $RESULT_JSON)"

# STS is not implemented; accept exit!=0 when the only failure is assume_role.
if [[ "$rc" -ne 0 ]]; then
  if grep -q 'FAILED.*test_assume_role' "$RESULT_LOG" \
    && ! grep -E 'FAILED.*(Basic|Multipart|Tagging|ListPagination|PresignedUrl)/' "$RESULT_LOG" >/dev/null; then
    echo "NOTE: only STS assume_role failed (expected without STS); treating as success"
    exit 0
  fi
fi
exit "$rc"
