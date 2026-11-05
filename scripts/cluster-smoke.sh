#!/usr/bin/env bash
# Three pigeonhole processes as one Raft cluster over gRPC on localhost
# (docs/cluster.md § Transports): each with a metadata log of its own,
# one shared local_disk blob store. Writes go to the leader, found by
# GET /_pigeonhole/leader; the leader is killed with -9, a new one serves
# what the old one wrote, and the killed node rejoins as a follower.
#
# CHAT=telegram: every node has a Telegram outbox, and the nodes in
# CHAT_ONLY (default: 3) have no gRPC, so the others reach them over the
# chat; CHAT_ONLY="1 2 3" runs the whole cluster over Telegram. Live:
# channels TG_RAFT_OUTBOX_1..3 from .env, each node's own bot
# RAFT_BOT_TOKEN_<n>, else TG_BOT_TOKEN[_<n>] (TG_BOT_TOKEN for node 1),
# admin of all three; no two nodes may share one. Telegram paces the whole
# cluster: elections take a minute or two.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -f "$ROOT/.env" ]]; then set -a; source "$ROOT/.env"; set +a; fi
CHAT="${CHAT:-}"
CHAT_ONLY="${CHAT_ONLY:-3}"
[[ "$CHAT" == telegram ]] && : "${LEADER_WAIT:=300}"
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
  if [[ "${KEEP:-}" == 1 ]]; then echo "kept $DIR"; else rm -rf "$DIR"; fi
}
trap cleanup EXIT

s3_port() { echo $((18330 + $1)); }
# A bot's id from its token (an id is no secret: the config names bots by
# it, a node checks its tokens against it): a Telegram token's part before
# ':', a Discord token's first part, in base64.
tg_bot_id() { echo "${1%%:*}"; }
dc_bot_id() { local p="${1%%.*}"; while (( ${#p} % 4 )); do p+="="; done; printf '%s' "$p" | base64 --decode; }
bot_of() { # node $1's Raft bot: RAFT_BOT_TOKEN_<n>, else test bot TG_BOT_TOKEN[_n]; each node its own
  local var="RAFT_BOT_TOKEN_$1" fallback=""
  case $1 in 1) fallback="${TG_BOT_TOKEN:-}" ;; *) local tg="TG_BOT_TOKEN_$1"; fallback="${!tg:-}" ;; esac
  local token="${!var:-$fallback}"
  [[ -n "$token" ]] || { echo "set $var in .env: node $1 needs a Raft bot of its own" >&2; exit 1; }
  echo "$token"
}
node_conf() { # $1: node id
  local grpc outbox
  for m in 1 2 3; do
    grpc="grpc = \"127.0.0.1:1700$m\""
    outbox=""
    if [[ "$CHAT" == telegram ]]; then
      local var="TG_RAFT_OUTBOX_$m"
      outbox="outbox = ${!var:?set $var in .env: the Raft outbox channel of node $m}
bot_ids = [$(tg_bot_id "$(bot_of "$m")")]"
      [[ " $CHAT_ONLY " == *" $m "* ]] && grpc=""
    fi
    printf '\n[[node]]\nid = %s\n%s\n%s\n' "$m" "$grpc" "$outbox"
  done
}
for n in 1 2 3; do
  mkdir -p "$DIR/run$n"
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
bot_token_envs = ["RAFT_BOT_TOKEN"]
$(node_conf "$n")
TOML
done

start() {
  # Its own directory: the bots' session files.
  (cd "$DIR/run$1" && PIGEONHOLE_CONFIG="$DIR/node$1.toml" RAFT_BOT_TOKEN="$( [[ "$CHAT" == telegram ]] && bot_of "$1" )" RUST_LOG=info \
    exec "$BIN" >"$DIR/node$1.log" 2>&1) &
  PID[$1]=$!
}

# The node that answers 200 on /_pigeonhole/leader, among $@.
leader_of() {
  local deadline=$((SECONDS + ${LEADER_WAIT:-30}))
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

# Node $1's HTTP code for path $2, once it listens (it may still be booting).
code_of() {
  local deadline=$((SECONDS + ${LEADER_WAIT:-30})) code
  while ((SECONDS < deadline)); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$(s3_port "$1")$2" || true)
    [[ "$code" != 000 ]] && { echo "$code"; return; }
    sleep 0.2
  done
  echo 000
}

s3() { aws --endpoint-url "http://127.0.0.1:$(s3_port "$1")" s3 "${@:2}"; }

for n in 1 2 3; do start "$n"; done
T=$SECONDS
L=$(leader_of 1 2 3)
echo "leader: node $L ($((SECONDS - T)) s after the start)"
F=$(( L % 3 + 1 ))
code=$(code_of "$F" /demo)
[[ "$code" == 503 ]] || { echo "a follower answered $code, not 503" >&2; exit 1; }

head -c 3000000 /dev/urandom >"$DIR/one.bin"
T=$SECONDS
s3 "$L" mb s3://demo >/dev/null
s3 "$L" cp "$DIR/one.bin" s3://demo/one.bin >/dev/null
echo "wrote one.bin through node $L ($((SECONDS - T)) s)"

kill -9 "${PID[$L]}"; unset "PID[$L]"
REST=(); for n in 1 2 3; do [[ $n != "$L" ]] && REST+=("$n"); done
T=$SECONDS
N=$(leader_of "${REST[@]}")
echo "node $L killed; new leader: node $N ($((SECONDS - T)) s)"
s3 "$N" cp s3://demo/one.bin "$DIR/back.bin" >/dev/null
cmp "$DIR/one.bin" "$DIR/back.bin"
T=$SECONDS
s3 "$N" cp "$DIR/one.bin" s3://demo/two.bin >/dev/null
echo "read one.bin back and wrote two.bin through node $N ($((SECONDS - T)) s for the write)"

start "$L"
sleep 3
code=$(code_of "$L" /_pigeonhole/leader)
[[ "$code" == 503 ]] || { echo "the restarted node answered $code: it should follow" >&2; exit 1; }
s3 "$N" ls s3://demo/ | grep -q two.bin
echo "node $L rejoined as a follower; cluster smoke ok"
