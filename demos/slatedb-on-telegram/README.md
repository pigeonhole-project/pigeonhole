# SlateDB on Telegram

```
./demos/slatedb-on-telegram/run.sh                        # local disk
DEMO_BACKEND=memory ./demos/slatedb-on-telegram/run.sh    # memory
DEMO_BACKEND=fake ./demos/slatedb-on-telegram/run.sh      # fake Discord + Telegram channels
```

[SlateDB](https://slatedb.io) is an embedded key-value store (an LSM tree)
that keeps everything in object storage: a WAL of small objects, SSTs, and a
manifest that names the live SSTs. It has no local disk and no coordinator.
One writer at a time is enforced by conditional puts alone: each writer
claims a `writer_epoch` in a new manifest written with `If-None-Match: *`,
then puts a fence object at the next WAL id. An older writer's next WAL put
then finds that id taken and stops. Pointed at pigeonhole, each of those
conditional puts is decided in pigeonhole's log. With `fake`, that log is a
Discord channel and the objects are messages in a Telegram channel.

`slate/` is the demo's program: SlateDB 0.18 over `object_store`'s S3
client, one subcommand per step. It is a crate of its own, outside the
workspace, so SlateDB stays out of pigeonhole's lock file and checks.

The demo:

1. opens a database in the bucket `slate` and writes 40 keys; a `DbReader`
   (it reads, holds no epoch, and fences no one) scans them back;
2. lists what SlateDB keeps: `manifest/`, `wal/`, `compacted/` (the SSTs, L0
   and sorted runs alike), `compactions/` (the compactor's state);
3. runs a writer that puts a tick every 200 ms and prints each once it is
   durable, then `kill -9`s it: every tick it printed is there, and maybe one
   more that it had no time to print;
4. opens a second writer on the same database. The first one's next write
   is refused (`detected newer DB client`);
5. flushes six memtables to L0 SSTs. The compactor, running in the same
   process, merges them into sorted runs, and the manifest shows the result;
6. takes a checkpoint, writes 20 more keys, and clones the database from
   the checkpoint. The clone has the 40 keys of the checkpoint and shares the
   parent's SSTs; only the WAL is copied.

## What to expect

| | disk, memory | fake (latency 40–120 ms) |
|---|---|---|
| open a database | ~10 ms | 5–7 s |
| a write durable (one WAL put) | ~0.1 s | ~1 s |
| L0 compacted after the sixth flush | ~6 s | ~7 s |
| the whole demo | ~30 s | ~2.5 min |

On fake channels every S3 write is an entry in the Discord log, about a
second each. An open is a handful of them in a row (manifest, WAL fence,
compactor epoch). The objects of 1 KiB or less (every manifest and
`compactions/` object, most WAL objects) stay inline in that log
(docs/inline-objects.md). Only SSTs and larger WAL objects become messages in
the Telegram channel, which takes about 20 a minute. The last listing counts
the inline objects per prefix.

## What pigeonhole had to offer

All of it was already there; the demo needed no change to the gateway:

- `PutObject` with `If-None-Match: *`, decided in the log
  (docs/conditional-writes.md). WAL objects, manifests and the compactor's
  state are all written that way, each under the next id; nothing is
  overwritten in place;
- user metadata: SlateDB tags each conditional put with an id, so that after
  a timeout it can tell its own write from someone else's;
- `ListObjectsV2` with `start-after` (WAL replay lists from the last
  compacted id) and ranged `GetObject` (SST blocks).
