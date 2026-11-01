#!/usr/bin/env bash
# Three pigeonhole processes as one Raft cluster over gRPC on localhost
# (docs/raft-over-chat.md § Transports): each with a metadata log of its own,
# one shared local_disk blob store. Writes go to the leader, found by
# GET /_pigeonhole/leader; the leader is killed with -9, a new one serves
# what the old one wrote, and the killed node rejoins as a follower.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/pigeonhole}"
export AWS_ACCESS_KEY_ID=pigeonhole AWS_SECRET_ACCESS_KEY=pigeonholesecret AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export PH_CLUSTER_KEY="${PH_CLUSTER_KEY:-$(openssl rand -hex 32)}"
command -v aws >/dev/null || { echo "missing dependency: aws" >&2; exit 1; }
[[ -x "$BIN" ]] || { echo "build first: cargo build -p pigeonhole ($BIN)" >&2; exit 1; }

DIR="$(mktemp -d)"
echo "dir: $DIR"
PID=()
cleanup() {
  for n in 1 2 3; do [[ -n "${PID[$n]:-}" ]] && kill -9 "${PID[$n]}" 2>/dev/null || true; done
  [[ "${KEEP:-}" == 1 ]] && echo "kept $DIR" || rm -rf "$DIR"
}
trap cleanup EXIT

s3_port() { echo $((18330 + $1)); }
for n in 1 2 3; do
  cat > "$DIR/node$n.toml" <<TOML
listen_addr = "127.0.0.1:$(s3_port "$n")"

[meta]
log = "meta"

[chunk]
store = "data"

[[store]]
id = "meta"
kind = "local_disk"
path = "$DIR/meta$n"

[[store]]
id = "data"
kind = "local_disk"
path = "$DIR/data"

[cluster]
node = $n
id = 1
key_env = "PH_CLUSTER_KEY"

[[node]]
id = 1
grpc = "127.0.0.1:17001"

[[node]]
id = 2
grpc = "127.0.0.1:17002"

[[node]]
id = 3
grpc = "127.0.0.1:17003"
TOML
done

start() {
  PIGEONHOLE_CONFIG="$DIR/node$1.toml" RUST_LOG=info "$BIN" >"$DIR/node$1.log" 2>&1 &
  PID[$1]=$!
}

# The node that answers 200 on /_pigeonhole/leader, among $@.
leader_of() {
  local deadline=$((SECONDS + 30))
  while ((SECONDS < deadline)); do
    for n in "$@"; do
      if curl -sf "http://127.0.0.1:$(s3_port "$n")/_pigeonhole/leader" >/dev/null 2>&1; then echo "$n"; return; fi
    done
    sleep 0.2
  done
  echo "no leader among $*" >&2
  for n in "$@"; do tail -20 "$DIR/node$n.log" >&2; done
  exit 1
}

s3() { aws --endpoint-url "http://127.0.0.1:$(s3_port "$1")" s3 "${@:2}"; }

for n in 1 2 3; do start "$n"; done
L=$(leader_of 1 2 3)
echo "leader: node $L"
F=$(( L % 3 + 1 ))
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$(s3_port "$F")/demo")
[[ "$code" == 503 ]] || { echo "a follower answered $code, not 503" >&2; exit 1; }

head -c 3000000 /dev/urandom >"$DIR/one.bin"
s3 "$L" mb s3://demo >/dev/null
s3 "$L" cp "$DIR/one.bin" s3://demo/one.bin >/dev/null
echo "wrote one.bin through node $L"

kill -9 "${PID[$L]}"; unset "PID[$L]"
REST=(); for n in 1 2 3; do [[ $n != "$L" ]] && REST+=("$n"); done
N=$(leader_of "${REST[@]}")
echo "node $L killed; new leader: node $N"
s3 "$N" cp s3://demo/one.bin "$DIR/back.bin" >/dev/null
cmp "$DIR/one.bin" "$DIR/back.bin"
s3 "$N" cp "$DIR/one.bin" s3://demo/two.bin >/dev/null
echo "read one.bin back and wrote two.bin through node $N"

start "$L"
sleep 3
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$(s3_port "$L")/_pigeonhole/leader")
[[ "$code" == 503 ]] || { echo "the restarted node answered $code: it should follow" >&2; exit 1; }
s3 "$N" ls s3://demo/ | grep -q two.bin
echo "node $L rejoined as a follower; cluster smoke ok"
