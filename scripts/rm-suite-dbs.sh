#!/usr/bin/env bash
# Remove an index sqlite DB and its companion chunk-store / CAS files.
# Usage: rm_suite_dbs /path/to/index.db
#
# Companion paths mirror crates/bin/pigeonhole blob_db_url_from_index /
# cas_db_url_from_index: stem.db → stem-blob.db, stem-cas.db.

rm_suite_dbs() {
  local db="$1"
  rm -f "$db" "${db}-wal" "${db}-shm"
  local stem blob cas
  if [[ "$db" == *.db ]]; then
    stem="${db%.db}"
  else
    stem="$db"
  fi
  blob="${stem}-blob.db"
  cas="${stem}-cas.db"
  rm -f "$blob" "${blob}-wal" "${blob}-shm" \
        "$cas" "${cas}-wal" "${cas}-shm"
}
