# Shared by every demo: a pigeonhole to point things at, S3 calls by curl,
# pinned third-party binaries. Sourced, not run.
#
#   DEMO_BACKEND=disk   (default) local disk, no secrets
#   DEMO_BACKEND=chats  metadata log + snapshots in the Discord test channels
#                       (DISCORD_{LOG,SNAP}_CHANNEL_ID), objects in the Telegram
#                       one (TG_BLOBS_CHAT_ID); secrets from the repo .env
#
# State lives in .cache/demos/<demo>; binaries in .cache/demos/bin.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEMO_BACKEND="${DEMO_BACKEND:-disk}"
BIN="$ROOT/.cache/demos/bin"
S3_ADDR="${DEMO_S3_ADDR:-127.0.0.1:9333}"
S3_URL="http://$S3_ADDR"
# The demo's own S3 keys (the gateway takes whatever it is started with).
export AWS_ACCESS_KEY_ID=demo AWS_SECRET_ACCESS_KEY=demosecret AWS_REGION=us-east-1
mkdir -p "$BIN"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
  export AWS_ACCESS_KEY_ID=demo AWS_SECRET_ACCESS_KEY=demosecret
fi

say() { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }

# ── pinned tools ──────────────────────────────────────────────────────────────

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) OS=darwin ARCH=arm64 ;;
  Darwin-x86_64) OS=darwin ARCH=amd64 ;;
  Linux-x86_64) OS=linux ARCH=amd64 ;;
  Linux-aarch64) OS=linux ARCH=arm64 ;;
  *) echo "unsupported platform $(uname -sm)" >&2; exit 1 ;;
esac

sha256() { if command -v shasum >/dev/null; then shasum -a 256 "$@"; else sha256sum "$@"; fi; }

# Download `url` to `$BIN/name` once, checking it against the sha256 line for
# `asset` in the release's checksum file `sums_url`.
fetch() {
  local name="$1" url="$2" sums_url="$3" asset="$4"
  [[ -e "$BIN/$name" ]] && return
  local tmp; tmp="$(mktemp -d)"
  curl -fsSL -o "$tmp/$asset" "$url"
  curl -fsSL -o "$tmp/sums" "$sums_url"
  (cd "$tmp" && grep " $asset\$" sums | sha256 -c - >/dev/null) || { echo "$asset: checksum mismatch" >&2; exit 1; }
  case "$asset" in
    *.tar.gz) tar xzf "$tmp/$asset" -C "$tmp" ;;
    *.zip) (cd "$tmp" && unzip -q "$asset") ;;
  esac
  local found; found="$(find "$tmp" -type f \( -name "$name" -o -name "$asset" \) -perm -u+x 2>/dev/null | head -1)"
  [[ -n "$found" ]] || found="$tmp/$asset"
  install -m 0755 "$found" "$BIN/$name"
  rm -rf "$tmp"
}

T4_VERSION=v1.1.16
ETCD_VERSION=v3.7.3
LITESTREAM_VERSION=0.5.17

need_t4() {
  fetch t4 "https://github.com/t4db/t4/releases/download/$T4_VERSION/t4-$OS-$ARCH" \
    "https://github.com/t4db/t4/releases/download/$T4_VERSION/checksums.txt" "t4-$OS-$ARCH"
}

need_etcdctl() {
  local ext=tar.gz; [[ $OS == darwin ]] && ext=zip
  local asset="etcd-$ETCD_VERSION-$OS-$ARCH.$ext"
  fetch etcdctl "https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/$asset" \
    "https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/SHA256SUMS" "$asset"
}

need_litestream() {
  local a=$ARCH; [[ $OS == darwin && $ARCH == amd64 ]] && a=x86_64
  local asset="litestream-$LITESTREAM_VERSION-$OS-$a.tar.gz"
  fetch litestream "https://github.com/benbjohnson/litestream/releases/download/v$LITESTREAM_VERSION/$asset" \
    "https://github.com/benbjohnson/litestream/releases/download/v$LITESTREAM_VERSION/checksums.txt" "$asset"
}

# ── S3 by curl ────────────────────────────────────────────────────────────────

s3() { curl -sS --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"; }

# HTTP status of an S3 request (body discarded).
s3_status() { s3 -o /dev/null -w '%{http_code}' "$@"; }

make_bucket() { s3 -o /dev/null -X PUT -H 'Content-Length: 0' "$S3_URL/$1"; }

# Keys and sizes under `bucket` (ListObjectsV2).
list_bucket() {
  s3 "$S3_URL/$1?list-type=2" | python3 -c '
import re, sys
for k, s in re.findall(r"<Key>([^<]+)</Key>.*?<Size>(\d+)</Size>", sys.stdin.read()):
    print(f"  {k}  ({s} B)")'
}

# ── pigeonhole ────────────────────────────────────────────────────────────────

# Start a fresh pigeonhole for demo `name` (state in .cache/demos/<name>).
pigeonhole_up() {
  local name="$1"
  STATE="$ROOT/.cache/demos/$name"
  pigeonhole_down
  rm -rf "$STATE" && mkdir -p "$STATE"
  (cd "$ROOT" && cargo build -q --release -p pigeonhole)
  case "$DEMO_BACKEND" in
    disk) write_disk_config ;;
    chats) chats_before; write_chats_config ;;
    *) echo "DEMO_BACKEND: disk | chats" >&2; exit 2 ;;
  esac
  PIGEONHOLE_CONFIG="$STATE/pigeonhole.toml" "$ROOT/target/release/pigeonhole" >"$STATE/pigeonhole.log" 2>&1 &
  echo $! >"$STATE/pigeonhole.pid"; disown
  for _ in $(seq 1 120); do
    [[ "$(curl -s -o /dev/null -w '%{http_code}' "$S3_URL/" || true)" =~ ^[1-5] ]] && grep -q 'listening' "$STATE/pigeonhole.log" && break
    sleep 0.5
  done
  grep -q 'listening' "$STATE/pigeonhole.log" || { tail -20 "$STATE/pigeonhole.log"; echo "pigeonhole did not start" >&2; exit 1; }
  echo "pigeonhole on $S3_URL ($DEMO_BACKEND), log: $STATE/pigeonhole.log"
}

pigeonhole_down() {
  [[ -n "${STATE:-}" && -f "$STATE/pigeonhole.pid" ]] && kill "$(cat "$STATE/pigeonhole.pid")" 2>/dev/null || true
  sleep 0.5
}

write_disk_config() {
  cat >"$STATE/pigeonhole.toml" <<EOF
listen_addr = "$S3_ADDR"

[[store]]
id = "meta"
kind = "local_disk"
path = "$STATE/meta"

[[store]]
id = "blobs"
kind = "local_disk"
path = "$STATE/blobs"

[meta]
log = "meta"
snapshot = "meta"

[chunk]
store = "blobs"
EOF
}

write_chats_config() {
  : "${DISCORD_LOG_CHANNEL_ID:?set in .env}" "${DISCORD_SNAP_CHANNEL_ID:?set in .env}" "${TG_BLOBS_CHAT_ID:?set in .env}"
  cat >"$STATE/pigeonhole.toml" <<EOF
listen_addr = "$S3_ADDR"

[[store]]
id = "dc-log"
kind = "discord"
channel_id = $DISCORD_LOG_CHANNEL_ID

[[store]]
id = "dc-snap"
kind = "discord"
channel_id = $DISCORD_SNAP_CHANNEL_ID

[[store]]
id = "tg-blobs"
kind = "telegram"
chat_id = $TG_BLOBS_CHAT_ID
bot_token_env = "TG_BOT_TOKEN"
session = "$ROOT/.cache/demos/tg.session"

[meta]
log = "dc-log"
snapshot = "dc-snap"

[chunk]
store = "tg-blobs"
EOF
}

# The chat test channels are shared: mark Telegram before a run, and after it
# delete only what lies between the marks; the Discord channels are emptied
# whole (they are dedicated to tests).
chats_before() {
  say "marking the Telegram test channels"
  (cd "$ROOT" && TG_BENCH=mark cargo test -q -p pigeonhole-storage-telegram --test live bench_window -- --ignored --nocapture) \
    | grep -E '^TG_' || true
}

chats_after() {
  [[ "$DEMO_BACKEND" == chats ]] || return 0
  say "cleaning up the chat channels"
  (cd "$ROOT" && TG_BENCH=clean cargo test -q -p pigeonhole-storage-telegram --test live bench_window -- --ignored --nocapture) \
    | grep -E 'deleted' || true
  (cd "$ROOT" && cargo test -q -p pigeonhole-storage-discord --test live empty_test_channels -- --ignored --nocapture --test-threads 1) \
    | grep -E 'emptied' || true
}
