#!/usr/bin/env bash
# Purge the real Telegram backend + run the full client suite against it once.
#
# Unlike compat-memory / compat-s3s-* / compat-rclone, this uses TelegramBlobStore
# (requires BOT_TOKEN + CHAT_ID in .env). Snapshots are disabled for the run.
#
# Steps:
#   1. cargo test
#   2. s3gram purge  (delete tracked TG messages, wipe SQLite)
#   3. start s3gram on LISTEN_ADDR
#   4. smoke + ceph/s3-tests + s3s boto3 + s3s-e2e + rclone
#
# Usage:
#   ./scripts/compat-telegram.sh           # curated s3-tests + other suites
#   ./scripts/compat-telegram.sh --all     # full s3-tests functional (slow)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="${S3GRAM_TG_LOG:-$ROOT/.cache/s3gram-telegram.log}"
PIDFILE="${S3GRAM_TG_PID:-$ROOT/.cache/s3gram-telegram.pid}"
REPORT="${S3GRAM_TG_REPORT:-$ROOT/.cache/telegram-suite-report.txt}"
S3S_DIR="${S3S_DIR:-$ROOT/.cache/s3s-upstream}"
S3S_REPO="${S3S_REPO:-https://github.com/s3s-project/s3s.git}"
S3S_REF="${S3S_REF:-main}"
VENV="${S3S_BOTO3_VENV:-$ROOT/.cache/s3s-boto3-venv}"
E2E_BIN="${S3S_E2E_BIN:-$S3S_DIR/target/release/s3s-e2e}"
RCLONE_CACHE="${RCLONE_CACHE:-$ROOT/.cache/rclone}"

COMPAT_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) COMPAT_ARGS+=(--all); shift ;;
    --) shift; COMPAT_ARGS+=("$@"); break ;;
    *) COMPAT_ARGS+=("$1"); shift ;;
  esac
done
# Bash + set -u treats "${arr[@]}" as unbound when arr is empty.
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
: "${BOT_TOKEN:?BOT_TOKEN required in .env}"

# chat_id from existing s3gram.toml, or CHAT_ID leftover in .env for migration.
if [[ -z "${CHAT_ID:-}" && -f s3gram.toml ]]; then
  CHAT_ID="$(sed -n 's/^chat_id[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' s3gram.toml | head -1)"
fi
: "${CHAT_ID:?chat_id required in s3gram.toml (or CHAT_ID in .env for migration)}"

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
skip=0
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

log_step "cargo test"
set +e
cargo test 2>&1 | tee -a "$REPORT"
record "cargo test" "${PIPESTATUS[0]}"
set -e

log_step "build release"
cargo build --release -q

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; stop the existing server before compat-telegram" >&2
  exit 1
fi

CFG="$ROOT/.cache/s3gram-telegram-config.toml"
# Prefer database_url from local s3gram.toml if present.
DB_URL="sqlite:s3gram.db"
if [[ -f s3gram.toml ]]; then
  parsed="$(sed -n 's/^database_url[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' s3gram.toml | head -1 || true)"
  [[ -n "$parsed" ]] && DB_URL="$parsed"
fi
CONFIG_OUT="$CFG" \
  CONFIG_MEMORY=false \
  CONFIG_CHAT_ID="$CHAT_ID" \
  CONFIG_LISTEN_ADDR="$LISTEN_ADDR" \
  CONFIG_DATABASE_URL="$DB_URL" \
  CONFIG_SNAPSHOT_SECS=0 \
  "$ROOT/scripts/gen-config.sh"
export S3GRAM_CONFIG="$CFG"

log_step "purge Telegram + wipe index"
./target/release/s3gram purge 2>&1 | tee -a "$REPORT"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3gram}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export AWS_ENDPOINT_URL="$ENDPOINT_URL"
export AWS_EC2_METADATA_DISABLED=1
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
export S3GRAM_HOST=127.0.0.1
export S3GRAM_PORT="$PORT"
export ENDPOINT="$ENDPOINT_URL"

./target/release/s3gram >"$LOG" 2>&1 &
echo $! >"$PIDFILE"

for _ in $(seq 1 80); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT_URL/" || true)"
  if [[ -n "$code" && "$code" != "000" ]]; then
    break
  fi
  sleep 0.15
done
if [[ -z "${code:-}" || "$code" == "000" ]]; then
  echo "telegram server failed to start; see $LOG" >&2
  exit 1
fi
log_step "s3gram TelegramBlobStore listening on $ENDPOINT_URL (pid $(cat "$PIDFILE"))"

log_step "smoke"
set +e
./scripts/smoke.sh 2>&1 | tee -a "$REPORT"
record "smoke" "${PIPESTATUS[0]}"
set -e

if [[ ${#COMPAT_ARGS[@]} -eq 0 ]]; then
  log_step "ceph/s3-tests (compat.sh known-good)"
else
  log_step "ceph/s3-tests (compat.sh ${COMPAT_ARGS[*]})"
fi
set +e
compat_sh 2>&1 | tee -a "$REPORT"
record "compat/s3-tests" "${PIPESTATUS[0]}"
set -e

# --- s3s boto3 ---
if [[ ! -d "$S3S_DIR/.git" ]]; then
  log_step "cloning s3s into $S3S_DIR"
  git clone --depth 1 --branch "$S3S_REF" "$S3S_REPO" "$S3S_DIR"
else
  log_step "updating s3s in $S3S_DIR"
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

log_step "s3s boto3 scripts"
boto_failed=0
shopt -s nullglob
for script in "$S3S_DIR"/tests/boto3/*.py; do
  name="$(basename "$script")"
  echo "-------- $name --------" | tee -a "$REPORT"
  set +e
  if [[ "$name" == "put_object_no_content_length.py" ]]; then
    "$PY" "$script" "$AWS_ENDPOINT_URL" 2>&1 | tee -a "$REPORT"
  else
    "$PY" "$script" 2>&1 | tee -a "$REPORT"
  fi
  rc=${PIPESTATUS[0]}
  set -e
  record "boto3/$name" "$rc"
  if [[ "$rc" -ne 0 ]]; then
    boto_failed=1
  fi
done
if [[ "$boto_failed" -eq 0 ]]; then
  echo "(boto3 suite ok)" | tee -a "$REPORT"
fi

# --- s3s-e2e ---
need_e2e_build=1
if [[ -x "$E2E_BIN" ]]; then
  bin_mtime="$(stat -f %m "$E2E_BIN" 2>/dev/null || stat -c %Y "$E2E_BIN")"
  head_mtime="$(git -C "$S3S_DIR" log -1 --format=%ct)"
  if [[ "$bin_mtime" -ge "$head_mtime" ]]; then
    need_e2e_build=0
  fi
fi
if [[ "$need_e2e_build" -eq 1 ]]; then
  log_step "building s3s-e2e"
  touch "$S3S_DIR/crates/s3s-e2e/build.rs"
  cargo build --manifest-path "$S3S_DIR/Cargo.toml" -p s3s-e2e --release
fi

log_step "s3s-e2e (STS assume_role expected to fail without STS)"
set +e
"$E2E_BIN" --json "$ROOT/.cache/s3s-e2e-telegram-report.json" 2>&1 | tee -a "$REPORT"
e2e_rc=${PIPESTATUS[0]}
set -e
# Accept exit!=0 if the only failure is STS.
if [[ "$e2e_rc" -ne 0 ]]; then
  if grep -q 'FAILED.*test_assume_role' "$REPORT" \
    && ! grep -E 'FAILED.*(Basic|Multipart|Tagging|ListPagination|PresignedUrl)' "$REPORT" >/dev/null; then
    echo "NOTE  s3s-e2e: only STS failed (expected)" | tee -a "$REPORT"
    record "s3s-e2e (S3 cases)" 0
    skip=$((skip + 1))
  else
    record "s3s-e2e" "$e2e_rc"
  fi
else
  record "s3s-e2e" 0
fi

# --- rclone (reuse pinned binary from compat-rclone) ---
# shellcheck disable=SC1091
source "$S3S_DIR/scripts/rclone.env"
RCLONE_VERSION="${RCLONE_VERSION_LINE#rclone v}"
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os" in darwin) os="osx" ;; esac
case "$arch" in x86_64|amd64) arch="amd64" ;; arm64|aarch64) arch="arm64" ;; esac
zip_stem="rclone-v${RCLONE_VERSION}-${os}-${arch}"
RCLONE_BIN="${RCLONE_BIN:-$RCLONE_CACHE/${zip_stem}/rclone}"
if [[ ! -x "$RCLONE_BIN" ]]; then
  log_step "downloading rclone $RCLONE_VERSION"
  mkdir -p "$RCLONE_CACHE"
  curl -fsSL "https://downloads.rclone.org/v${RCLONE_VERSION}/${zip_stem}.zip" \
    -o "$RCLONE_CACHE/${zip_stem}.zip"
  unzip -qo "$RCLONE_CACHE/${zip_stem}.zip" -d "$RCLONE_CACHE"
  chmod +x "$RCLONE_BIN"
fi
export RCLONE_BIN
export RCLONE_EXPECTED_VERSION="$RCLONE_VERSION_LINE"
export RCLONE_S3_PROVIDER="${RCLONE_S3_PROVIDER:-Other}"
export RCLONE_S3_FORCE_PATH_STYLE="${RCLONE_S3_FORCE_PATH_STYLE:-true}"

log_step "rclone e2e"
set +e
/bin/sh "$S3S_DIR/tests/rclone/e2e.sh" 2>&1 | tee -a "$REPORT"
record "rclone e2e" "${PIPESTATUS[0]}"
set -e

echo | tee -a "$REPORT"
echo "======== summary ========" | tee -a "$REPORT"
echo "pass=$pass fail=$fail (sts_skipped_note=$skip)" | tee -a "$REPORT"
echo "log: $LOG" | tee -a "$REPORT"
echo "report: $REPORT" | tee -a "$REPORT"

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
echo "telegram suite OK"
