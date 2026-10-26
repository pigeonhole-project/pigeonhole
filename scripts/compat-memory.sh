#!/usr/bin/env bash
# Run s3-tests against pigeonhole with MemoryBlobStore (no Telegram).
#
# Usage:
#   ./scripts/compat-memory.sh           # curated known-good
#   ./scripts/compat-memory.sh --all     # full functional suite
#   ./scripts/compat-memory.sh -- <pytest args...>

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${PIGEONHOLE_PORT:-8333}"
DB="${PIGEONHOLE_MEMORY_DB:-$ROOT/.cache/pigeonhole-memory.db}"
LOG="${PIGEONHOLE_MEMORY_LOG:-$ROOT/.cache/pigeonhole-memory.log}"
PIDFILE="${PIGEONHOLE_MEMORY_PID:-$ROOT/.cache/pigeonhole-memory.pid}"

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

# Fresh index + chunk-store (+ CAS) DBs for a clean suite run.
# MemoryBlobStore keys restart at 1 each process; leftover *-blob.db
# sort_keys would collide under UNIQUE (instance_id, sort_key).
# shellcheck source=rm-suite-dbs.sh
source "$ROOT/scripts/rm-suite-dbs.sh"
rm_suite_dbs "$DB"

source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -q

# Free the port if a previous server is lingering.
if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; refusing to start memory server" >&2
  exit 1
fi

CFG="$ROOT/.cache/pigeonhole-memory-config.toml"
CONFIG_OUT="$CFG" \
  CONFIG_INSTANCE=memory \
  CONFIG_LISTEN_ADDR="127.0.0.1:${PORT}" \
  CONFIG_DATABASE_URL="sqlite:${DB}?mode=rwc" \
  CONFIG_SNAPSHOT_SECS=0 \
  "$ROOT/scripts/gen-config.sh"
export PIGEONHOLE_CONFIG="$CFG"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-pigeonhole}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-pigeonholesecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

./target/release/pigeonhole >"$LOG" 2>&1 &
echo $! >"$PIDFILE"

for _ in $(seq 1 50); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/" || true)"
  if [[ -n "$code" && "$code" != "000" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "${code:-}" || "$code" == "000" ]]; then
  echo "memory server failed to start; see $LOG" >&2
  exit 1
fi
echo "pigeonhole MemoryBlobStore listening on :$PORT (pid $(cat "$PIDFILE"))"

export PIGEONHOLE_HOST=127.0.0.1
export PIGEONHOLE_PORT="$PORT"
./scripts/compat.sh "$@"
