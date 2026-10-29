#!/usr/bin/env bash
# SQLite on pigeonhole: Litestream streams a database's WAL into pigeonhole's
# S3 gateway; the local file is deleted and restored from the bucket, whole
# and as of a moment in the past.
#
#   ./demos/sqlite-on-telegram/run.sh                       # on local disk
#   DEMO_BACKEND=chats ./demos/sqlite-on-telegram/run.sh    # in Discord + Telegram

source "$(dirname "$0")/../lib/common.sh"
need_litestream

sql() { python3 - "$@" <<'EOF'
import sqlite3, sys
db, query = sys.argv[1], sys.argv[2]
c = sqlite3.connect(db)
c.execute("PRAGMA busy_timeout = 5000")
rows = c.execute(query).fetchall()
c.commit()
for r in rows:
    print("  " + " | ".join(str(x) for x in r))
EOF
}

ls_down() { [[ -f "$STATE/litestream.pid" ]] && kill "$(cat "$STATE/litestream.pid")" 2>/dev/null || true; }
trap 'ls_down; pigeonhole_down; chats_after' EXIT

pigeonhole_up sqlite-on-telegram
make_bucket sqlite
DB="$STATE/app.db"
cat >"$STATE/litestream.yml" <<EOF
dbs:
  - path: $DB
    replica:
      type: s3
      bucket: sqlite
      path: app
      endpoint: $S3_URL
      region: us-east-1
      force-path-style: true
      sync-interval: 1s
EOF

say "a SQLite database, replicated by Litestream into pigeonhole"
sql "$DB" "PRAGMA journal_mode = WAL" >/dev/null
sql "$DB" "CREATE TABLE IF NOT EXISTS pigeons (id INTEGER PRIMARY KEY, name TEXT, at TEXT DEFAULT CURRENT_TIMESTAMP)"
"$BIN/litestream" replicate -config "$STATE/litestream.yml" >"$STATE/litestream.log" 2>&1 &
echo $! >"$STATE/litestream.pid"; disown
sleep 3

for name in Gerald Margaret Pip; do sql "$DB" "INSERT INTO pigeons (name) VALUES ('$name')"; done
sleep 3
before="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "three pigeons in, at $before"
sleep 2
for name in Dmitri Wanda; do sql "$DB" "INSERT INTO pigeons (name) VALUES ('$name')"; done
sql "$DB" "SELECT id, name FROM pigeons"
sleep 4

say "what Litestream keeps in the bucket"
list_bucket sqlite

say "the local database is gone"
ls_down; sleep 1
rm -f "$DB" "$DB-wal" "$DB-shm"
ls "$STATE"/app.db* 2>/dev/null || echo "  (no app.db)"

say "restore it from pigeonhole"
"$BIN/litestream" restore -config "$STATE/litestream.yml" -o "$STATE/restored.db" "$DB"
sql "$STATE/restored.db" "SELECT id, name FROM pigeons"

say "and as it was at $before"
"$BIN/litestream" restore -config "$STATE/litestream.yml" -timestamp "$before" -o "$STATE/then.db" "$DB"
sql "$STATE/then.db" "SELECT id, name FROM pigeons"
