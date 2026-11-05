#!/usr/bin/env bash
# SlateDB on pigeonhole: an LSM key-value store written for object storage
# keeps its WAL, SSTs and manifest in pigeonhole's S3 gateway. Its writers
# are fenced by conditional puts, which pigeonhole decides in its own log.
#
#   ./demos/slatedb-on-telegram/run.sh                       # on local disk
#   DEMO_BACKEND=fake ./demos/slatedb-on-telegram/run.sh     # on fake Discord + Telegram channels

source "$(dirname "$0")/../lib/common.sh"

trap 'pigeonhole_down; chats_after' EXIT

# The demo's program (./slate), a crate of its own outside the workspace.
say "building the demo's SlateDB program"
(cd "$ROOT/demos/slatedb-on-telegram/slate" && CARGO_TARGET_DIR="$ROOT/.cache/demos/slate-target" cargo build -q --release)
SLATE="$ROOT/.cache/demos/slate-target/release/slate"

pigeonhole_up slatedb-on-telegram
make_bucket slate
export SLATE_S3_URL="$S3_URL" SLATE_BUCKET=slate SLATE_PATH=pigeons

# Objects under the database's prefixes: how many, and how many are small
# enough to live inline in pigeonhole's log (<= 1 KiB, docs/inline-objects.md).
prefixes() {
  s3 "$S3_URL/slate?list-type=2&max-keys=1000" | python3 -c '
import collections, re, sys
rows = re.findall(r"<Key>([^<]+)</Key>.*?<Size>(\d+)</Size>", sys.stdin.read())
by = collections.defaultdict(list)
for k, s in rows:
    by["/".join(k.split("/")[:2]) + "/"].append(int(s))
for p, sizes in sorted(by.items()):
    print(f"  {p:<22} {len(sizes):>3} objects, {sum(sizes):>7} B, {sum(s <= 1024 for s in sizes):>3} inline")'
}

say "SlateDB opens a database in the bucket and writes 40 pigeons"
"$SLATE" put 0 40
"$SLATE" scan pigeon/

say "what SlateDB keeps in the bucket"
prefixes

case "$DEMO_BACKEND" in disk | memory) run_for=3 ;; *) run_for=20 ;; esac
say "a writer that ticks every 200 ms, killed with -9 after ${run_for} s"
"$SLATE" writer >"$STATE/writer.log" 2>&1 &
writer=$!
sleep "$run_for"
kill -9 "$writer"
wait "$writer" 2>/dev/null || true
tail -1 "$STATE/writer.log"
echo "  every tick it called durable is in the bucket:"
"$SLATE" scan tick/ | tail -2

say "two writers on one database: the newer fences the older"
"$SLATE" fence

say "memtables flushed to L0 SSTs, then the compactor merges them"
"$SLATE" flush 6
"$SLATE" status

say "a checkpoint, more pigeons, then a clone of the checkpoint"
checkpoint="$("$SLATE" checkpoint)"
echo "  checkpoint $checkpoint"
"$SLATE" put 40 20 | tail -1
"$SLATE" clone pigeons-then "$checkpoint"
echo "  the database now:"
"$SLATE" scan pigeon/ | tail -1
echo "  the clone, as of the checkpoint:"
SLATE_PATH=pigeons-then "$SLATE" scan pigeon/ | tail -1

say "the bucket at the end"
prefixes
