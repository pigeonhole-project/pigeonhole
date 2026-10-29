#!/usr/bin/env bash
# etcd on pigeonhole: T4 (an etcd v3 server whose state lives in S3) on top of
# pigeonhole's S3 gateway. Two T4 nodes elect a leader through a lock object
# written with If-None-Match / If-Match; the leader is killed and the other
# takes over. etcdctl is the client, as kube-apiserver (via kine) would be.
#
#   ./demos/etcd-on-telegram/run.sh                       # on local disk
#   DEMO_BACKEND=chats ./demos/etcd-on-telegram/run.sh    # in Discord + Telegram

source "$(dirname "$0")/../lib/common.sh"
need_t4
need_etcdctl

t4_up() { # node-id client-port peer-port metrics-port
  local id="$1"
  mkdir -p "$STATE/$id"
  T4_S3_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" T4_S3_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    "$BIN/t4" run --node-id "$id" --data-dir "$STATE/$id" \
      --listen "127.0.0.1:$2" --peer-listen "127.0.0.1:$3" --metrics-addr "127.0.0.1:$4" \
      --s3-endpoint "$S3_URL" --s3-region us-east-1 --s3-bucket etcd --s3-prefix t4 \
      >"$STATE/$id.log" 2>&1 &
  echo $! >"$STATE/$id.pid"; disown
}

t4_down() { for p in "$STATE"/*.pid; do [[ "$p" == */pigeonhole.pid ]] || kill -9 "$(cat "$p")" 2>/dev/null || true; done; }

# Wait until `log` has a line matching `pattern`.
wait_for() {
  for _ in $(seq 1 120); do grep -q "$2" "$1" && return; sleep 0.5; done
  echo "timed out waiting for '$2' in $1" >&2; tail -5 "$1" >&2; exit 1
}

events() { grep -hE 'elected leader|following leader|takeover' "$@" | sed -E 's/.*msg="([^"]*)".*/  \1/'; }

etcd() { local ep="$1"; shift; "$BIN/etcdctl" --endpoints="$ep" --command-timeout=60s "$@"; }

trap 't4_down; pigeonhole_down; chats_after' EXIT

pigeonhole_up etcd-on-telegram
make_bucket etcd

say "two T4 nodes on pigeonhole's S3"
t4_up n1 23791 23801 23811
wait_for "$STATE/n1.log" 'elected leader'
t4_up n2 23792 23802 23812
wait_for "$STATE/n2.log" 'following leader'
events "$STATE/n1.log" "$STATE/n2.log"

say "etcdctl against the leader"
etcd 127.0.0.1:23791 put /registry/pods/default/nginx '{"phase":"Running"}'
etcd 127.0.0.1:23791 put /registry/configmaps/default/greeting 'hello from a chat'
etcd 127.0.0.1:23791 get --prefix /registry --keys-only | grep -v '^$'
echo "a transaction (compare, then put):"
printf 'value("/registry/configmaps/default/greeting") = "hello from a chat"\n\nput /registry/configmaps/default/greeting "updated in a txn"\n\n\n' \
  | etcd 127.0.0.1:23791 txn
etcd 127.0.0.1:23791 get /registry/configmaps/default/greeting --print-value-only
lease="$(etcd 127.0.0.1:23791 lease grant 60 | awk '{print $2}')"
etcd 127.0.0.1:23791 put --lease="$lease" /registry/leases/kube-scheduler n1 >/dev/null
echo "lease $lease holds /registry/leases/kube-scheduler"

say "what T4 keeps in the bucket"
list_bucket etcd

say "kill -9 the leader"
kill -9 "$(cat "$STATE/n1.pid")"; rm "$STATE/n1.pid"
t0=$(date +%s)
wait_for "$STATE/n2.log" 'elected leader'
echo "n2 took over after $(( $(date +%s) - t0 ))s:"
events "$STATE/n2.log"
echo "the lock now: $(s3 "$S3_URL/etcd/t4/leader-lock" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["node_id"], "term", d["term"])')"

say "writes go on through the new leader"
etcd 127.0.0.1:23792 put /registry/pods/default/nginx '{"phase":"Running","node":"n2"}'
etcd 127.0.0.1:23792 get --prefix /registry

say "the bucket now (WAL segments upload in the background, every 10 s)"
sleep 12
list_bucket etcd
