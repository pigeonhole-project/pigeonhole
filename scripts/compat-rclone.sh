#!/usr/bin/env bash
# Run s3s upstream rclone S3 e2e against s3gram MemoryBlobStore (no Telegram).
#
# Downloads a pinned rclone binary into .cache/ (no Docker required), starts a
# temporary MemoryBlobStore server, then runs tests/rclone/e2e.sh from the
# s3s upstream checkout.
#
# Usage:
#   ./scripts/compat-rclone.sh
#   S3GRAM_PORT=18335 ./scripts/compat-rclone.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${S3GRAM_PORT:-18335}"
DB="${S3GRAM_MEMORY_DB:-$ROOT/.cache/s3gram-rclone.db}"
LOG="${S3GRAM_MEMORY_LOG:-$ROOT/.cache/s3gram-rclone.log}"
PIDFILE="${S3GRAM_MEMORY_PID:-$ROOT/.cache/s3gram-rclone.pid}"
S3S_DIR="${S3S_DIR:-$ROOT/.cache/s3s-upstream}"
S3S_REPO="${S3S_REPO:-https://github.com/s3s-project/s3s.git}"
S3S_REF="${S3S_REF:-main}"
RCLONE_CACHE="${RCLONE_CACHE:-$ROOT/.cache/rclone}"
RESULT_LOG="${RCLONE_E2E_LOG:-$ROOT/.cache/rclone-e2e-results.log}"
WORK_ROOT="${RCLONE_WORK_ROOT:-}"

cd "$ROOT"
mkdir -p "$(dirname "$DB")" "$RCLONE_CACHE"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
unset S3GRAM_INSECURE || true

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

# shellcheck disable=SC1091
source "$S3S_DIR/scripts/rclone.env"
: "${RCLONE_VERSION_LINE:?RCLONE_VERSION_LINE missing from rclone.env}"
# e.g. "rclone v1.75.0" -> "1.75.0"
RCLONE_VERSION="${RCLONE_VERSION_LINE#rclone v}"

detect_rclone_zip() {
  local os arch
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$os" in
    darwin) os="osx" ;;
    linux) ;;
    *)
      echo "unsupported OS for rclone download: $os" >&2
      exit 1
      ;;
  esac
  case "$arch" in
    x86_64 | amd64) arch="amd64" ;;
    arm64 | aarch64) arch="arm64" ;;
    *)
      echo "unsupported arch for rclone download: $arch" >&2
      exit 1
      ;;
  esac
  echo "rclone-v${RCLONE_VERSION}-${os}-${arch}"
}

ensure_rclone() {
  if [[ -n "${RCLONE_BIN:-}" && -x "$RCLONE_BIN" ]]; then
    return
  fi

  local zip_stem zip_url zip_path extract_dir bin
  zip_stem="$(detect_rclone_zip)"
  bin="$RCLONE_CACHE/${zip_stem}/rclone"
  if [[ -x "$bin" ]]; then
    RCLONE_BIN="$bin"
    return
  fi

  zip_url="https://downloads.rclone.org/v${RCLONE_VERSION}/${zip_stem}.zip"
  zip_path="$RCLONE_CACHE/${zip_stem}.zip"
  extract_dir="$RCLONE_CACHE/${zip_stem}"
  echo "downloading rclone $RCLONE_VERSION ($zip_stem) ..."
  curl -fsSL "$zip_url" -o "$zip_path"
  rm -rf "$extract_dir"
  mkdir -p "$extract_dir"
  unzip -q "$zip_path" -d "$RCLONE_CACHE"
  # Zip contains a top-level directory named like the stem.
  if [[ ! -x "$bin" ]]; then
    echo "rclone binary missing after extract: $bin" >&2
    exit 1
  fi
  chmod +x "$bin"
  RCLONE_BIN="$bin"
}

ensure_rclone
export RCLONE_BIN
export RCLONE_EXPECTED_VERSION="$RCLONE_VERSION_LINE"

actual_version="$("$RCLONE_BIN" version | sed -n '1p')"
if [[ "$actual_version" != "$RCLONE_EXPECTED_VERSION" ]]; then
  echo "unexpected rclone version: expected '$RCLONE_EXPECTED_VERSION', got '$actual_version'" >&2
  exit 1
fi
echo "using $actual_version ($RCLONE_BIN)"

source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -q

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT busy; refusing to start memory server" >&2
  exit 1
fi

export S3GRAM_MEMORY=1
export DATABASE_URL="sqlite:${DB}?mode=rwc"
export SNAPSHOT_INTERVAL_SECS=0
export LISTEN_ADDR="127.0.0.1:${PORT}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3gram}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3gramsecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export AWS_ENDPOINT_URL="http://127.0.0.1:${PORT}"
export RCLONE_S3_PROVIDER="${RCLONE_S3_PROVIDER:-Other}"
export RCLONE_S3_FORCE_PATH_STYLE="${RCLONE_S3_FORCE_PATH_STYLE:-true}"
export RCLONE_LOW_LEVEL_RETRIES="${RCLONE_LOW_LEVEL_RETRIES:-3}"
# Avoid IMDS / SSO probes.
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

E2E_SH="$S3S_DIR/tests/rclone/e2e.sh"
if [[ ! -f "$E2E_SH" ]]; then
  echo "missing rclone e2e script: $E2E_SH" >&2
  exit 1
fi

echo "running: $E2E_SH"
set +e
if [[ -n "$WORK_ROOT" ]]; then
  mkdir -p "$WORK_ROOT"
  /bin/sh "$E2E_SH" "$WORK_ROOT" 2>&1 | tee "$RESULT_LOG"
else
  /bin/sh "$E2E_SH" 2>&1 | tee "$RESULT_LOG"
fi
rc=${PIPESTATUS[0]}
set -e

echo "rclone e2e exit=$rc (log: $RESULT_LOG)"
exit "$rc"
