#!/usr/bin/env bash
# Run s3s upstream tests/boto3 against s3gram MemoryBlobStore (no Telegram).
#
# Usage:
#   ./scripts/compat-s3s-boto3.sh
#   S3GRAM_PORT=18333 ./scripts/compat-s3s-boto3.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${S3GRAM_PORT:-18333}"
DB="${S3GRAM_MEMORY_DB:-$ROOT/.cache/s3gram-s3s-boto3.db}"
LOG="${S3GRAM_MEMORY_LOG:-$ROOT/.cache/s3gram-s3s-boto3.log}"
PIDFILE="${S3GRAM_MEMORY_PID:-$ROOT/.cache/s3gram-s3s-boto3.pid}"
S3S_DIR="${S3S_DIR:-$ROOT/.cache/s3s-upstream}"
S3S_REPO="${S3S_REPO:-https://github.com/s3s-project/s3s.git}"
S3S_REF="${S3S_REF:-main}"
VENV="${S3S_BOTO3_VENV:-$ROOT/.cache/s3s-boto3-venv}"
RESULT_LOG="${S3S_BOTO3_LOG:-$ROOT/.cache/s3s-boto3-results.txt}"

cd "$ROOT"
mkdir -p "$(dirname "$DB")" "$VENV"

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

if [[ ! -x "$VENV/bin/python" ]]; then
  if command -v uv >/dev/null 2>&1; then
    uv venv "$VENV"
    uv pip install --python "$VENV/bin/python" boto3 requests
  else
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q boto3 requests
  fi
fi
PY="$VENV/bin/python"
"$PY" -c 'import boto3, requests'

source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -q

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; refusing to start memory server" >&2
  exit 1
fi

CFG="$ROOT/.cache/s3gram-s3s-boto3-config.toml"
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
export AWS_ENDPOINT_URL="http://127.0.0.1:${PORT}"

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

: >"$RESULT_LOG"
failed=0
shopt -s nullglob
scripts=("$S3S_DIR"/tests/boto3/*.py)
if [[ ${#scripts[@]} -eq 0 ]]; then
  echo "no boto3 scripts under $S3S_DIR/tests/boto3" >&2
  exit 1
fi

for script in "${scripts[@]}"; do
  name="$(basename "$script")"
  echo "======== $name ========" | tee -a "$RESULT_LOG"
  set +e
  if [[ "$name" == "put_object_no_content_length.py" ]]; then
    "$PY" "$script" "$AWS_ENDPOINT_URL" 2>&1 | tee -a "$RESULT_LOG"
  else
    "$PY" "$script" 2>&1 | tee -a "$RESULT_LOG"
  fi
  rc=${PIPESTATUS[0]}
  set -e
  echo "exit=$rc" | tee -a "$RESULT_LOG"
  if [[ "$rc" -ne 0 ]]; then
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo "s3s boto3 tests FAILED (see $RESULT_LOG)" >&2
  exit 1
fi
echo "s3s boto3 tests OK (see $RESULT_LOG)"
