#!/usr/bin/env bash
# Purge the real Discord backend + run the full client suite against it once.
#
# Requires DISCORD_BOT_TOKEN + DISCORD_CHANNEL_ID in .env.
# Build with: cargo build --release --features discord
#
# Usage:
#   ./scripts/compat-discord.sh
#   ./scripts/compat-discord.sh --all

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="${PIGEONHOLE_DC_LOG:-$ROOT/.cache/pigeonhole-storage-discord.log}"
PIDFILE="${PIGEONHOLE_DC_PID:-$ROOT/.cache/pigeonhole-storage-discord.pid}"
REPORT="${PIGEONHOLE_DC_REPORT:-$ROOT/.cache/discord-suite-report.txt}"

COMPAT_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) COMPAT_ARGS+=(--all); shift ;;
    --) shift; COMPAT_ARGS+=("$@"); break ;;
    *) COMPAT_ARGS+=("$1"); shift ;;
  esac
done

compat_sh() {
  if [[ ${#COMPAT_ARGS[@]} -eq 0 ]]; then
    ./scripts/compat.sh
  else
    ./scripts/compat.sh "${COMPAT_ARGS[@]}"
  fi
}

cd "$ROOT"
mkdir -p "$(dirname "$LOG")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
: "${DISCORD_BOT_TOKEN:?DISCORD_BOT_TOKEN required in .env}"

: "${DISCORD_CHANNEL_ID:?DISCORD_CHANNEL_ID required in .env}"

LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:8333}"
PORT="${LISTEN_ADDR##*:}"
HOST_PORT="127.0.0.1:${PORT}"
ENDPOINT_URL="http://${HOST_PORT}"

cleanup() {
  if [[ -f "$PIDFILE" ]]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null || true
    rm -f "$PIDFILE"
  fi
}
trap cleanup EXIT

source "$HOME/.cargo/env" 2>/dev/null || true

pass=0
fail=0
: >"$REPORT"
log_step() { echo "==> $*" | tee -a "$REPORT"; }
record() {
  local name="$1" rc="$2"
  if [[ "$rc" -eq 0 ]]; then
    echo "PASS  $name" | tee -a "$REPORT"
    pass=$((pass + 1))
  else
    echo "FAIL  $name (exit=$rc)" | tee -a "$REPORT"
    fail=$((fail + 1))
  fi
}

log_step "cargo test --workspace (discord crate uses wiremock only)"
set +e
cargo test -p pigeonhole-storage-discord 2>&1 | tee -a "$REPORT"
record "cargo test -p pigeonhole-storage-discord" "${PIPESTATUS[0]}"
set -e

log_step "build release (features discord)"
cargo build --release --features discord -q

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; stop the existing server before compat-discord" >&2
  exit 1
fi

CFG="$ROOT/.cache/pigeonhole-storage-discord-config.toml"
CONFIG_OUT="$CFG" \
  CONFIG_INSTANCE=discord \
  CONFIG_SCOPE_ID="$DISCORD_CHANNEL_ID" \
  CONFIG_LISTEN_ADDR="$LISTEN_ADDR" \
  CONFIG_META_LOG="$ROOT/.cache/pigeonhole-discord.db.log" \
  "$ROOT/scripts/gen-config.sh"
export PIGEONHOLE_CONFIG="$CFG"
export DISCORD_BOT_TOKEN

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-pigeonhole}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-pigeonholesecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export AWS_ENDPOINT_URL="$ENDPOINT_URL"
export AWS_EC2_METADATA_DISABLED=1
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
export PIGEONHOLE_HOST=127.0.0.1
export PIGEONHOLE_PORT="$PORT"
export ENDPOINT="$ENDPOINT_URL"

./target/release/pigeonhole >"$LOG" 2>&1 &
echo $! >"$PIDFILE"

for _ in $(seq 1 80); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT_URL/" || true)"
  if [[ -n "$code" && "$code" != "000" ]]; then
    break
  fi
  sleep 0.15
done
if [[ -z "${code:-}" || "$code" == "000" ]]; then
  echo "discord server failed to start; see $LOG" >&2
  exit 1
fi
log_step "pigeonhole DiscordBlobStore listening on $ENDPOINT_URL (pid $(cat "$PIDFILE"))"

log_step "smoke"
set +e
./scripts/smoke.sh 2>&1 | tee -a "$REPORT"
record "smoke" "${PIPESTATUS[0]}"
set -e

log_step "compat.sh"
set +e
compat_sh 2>&1 | tee -a "$REPORT"
record "compat/s3-tests" "${PIPESTATUS[0]}"
set -e

echo | tee -a "$REPORT"
echo "======== summary ========" | tee -a "$REPORT"
echo "pass=$pass fail=$fail" | tee -a "$REPORT"
echo "log: $LOG" | tee -a "$REPORT"
echo "report: $REPORT" | tee -a "$REPORT"

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
echo "discord suite OK"
