#!/usr/bin/env bash
# S3 conditional writes on pigeonhole, by curl: If-None-Match / If-Match,
# and ten writers racing to create one key (exactly one wins).
#
#   ./demos/conditional-writes/run.sh
#   DEMO_BACKEND=chats ./demos/conditional-writes/run.sh   # in Discord + Telegram

source "$(dirname "$0")/../lib/common.sh"
trap 'pigeonhole_down; chats_after' EXIT

pigeonhole_up conditional-writes
make_bucket demo
K="$S3_URL/demo/lock"

say "create only if absent (If-None-Match: *)"
echo "first:  $(s3_status -X PUT -H 'If-None-Match: *' --data-binary 'owner=alice' "$K")"
echo "second: $(s3_status -X PUT -H 'If-None-Match: *' --data-binary 'owner=bob' "$K")   ← 412: alice holds it"

say "compare-and-swap on the ETag (If-Match)"
etag="$(s3 -sI "$K" | tr -d '\r' | awk -F': ' 'tolower($1)=="etag"{print $2}')"
echo "current ETag $etag"
echo "with a stale ETag:   $(s3_status -X PUT -H 'If-Match: "00000000000000000000000000000000"' --data-binary 'owner=mallory' "$K")"
echo "with the current one: $(s3_status -X PUT -H "If-Match: $etag" --data-binary 'owner=alice;renewed' "$K")"
echo "If-Match on a key that is not there: $(s3_status -X PUT -H "If-Match: $etag" --data-binary x "$S3_URL/demo/nope")"

say "ten writers race for one key"
for i in $(seq 1 10); do
  s3_status -X PUT -H 'If-None-Match: *' --data-binary "writer $i" "$S3_URL/demo/race" >"$STATE/race.$i" &
done
wait
for i in $(seq 1 10); do printf 'writer %2s: %s\n' "$i" "$(cat "$STATE/race.$i")"; done
won="$(cat "$STATE"/race.* | grep -c 200 || true)"
echo "winners: $won (the log orders the writes; the first to commit wins)"
echo "the key holds: $(s3 "$S3_URL/demo/race")"
[[ "$won" == 1 ]]
