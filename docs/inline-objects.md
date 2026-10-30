# Inline objects

Small S3 objects stored in pigeonhole's metadata instead of as chunks, and
a cooler that moves them out once they go cold.

Status: implemented (2026-10-30), on by default (`max_object_size = "1 KiB"`).

- [In short](#in-short)
- [Why](#why)
- [How it behaves](#how-it-behaves)
- [Configuration](#configuration)
- [Lifecycle of an object](#lifecycle-of-an-object)
- [S3 operations](#s3-operations)
- [The cooler](#the-cooler)
- [Durability and recovery](#durability-and-recovery)
- [Cost and sizing](#cost-and-sizing)
- [Operating it](#operating-it)
- [Design](#design)
- [Alternatives considered](#alternatives-considered)
- [Measurements](#measurements)
- [Tests](#tests)
- [Limits and future work](#limits-and-future-work)

## In short

- An object of at most `max_object_size` bytes is **inline**: its bytes
  are part of its metadata record. Writing it is one entry in the metadata
  log, with no chunk and no upload to a blob store. Reading it is a lookup
  in memory.
- Larger objects are stored as before: chunks in the blob stores, extents
  in the record.
- A background **cooler** moves inline objects out to chunks:
  - when they have not been rewritten for `cool_after`;
  - or, oldest first, when all inline bytes together exceed `budget`.
  Hot small objects (locks, manifests, pointers) stay inline. The rest drift
  out.
- Clients see no difference: same ETags, same ranges, same conditional
  writes, whether an object is inline or not, and while it moves.
- Every knob is in `[inline]`. `max_object_size = "off"` stops inlining, and
  the cooler then moves out whatever is still inline.

## Why

Every S3 object used to be a chunk in a blob store plus a metadata record
(key → extents, ETag). For a 200-byte object the chunk is pure overhead. In a
Telegram channel a chunk is a post, and a channel takes about 20 posts a
minute (`[telegram] send_rate_per_sec = 0.3`).

Software that coordinates through S3 writes exactly such objects, and often:

- [T4](https://github.com/t4db/t4) (etcd on S3) renews its ~250-byte
  `leader-lock` every 2 s with `If-Match`;
- Litestream renews its `lock.json`;
- table formats (Delta, Iceberg) move small pointer files.

Behind a burst of large uploads, such a write waits its turn for the
channel. Then:

1. it times out in its client;
2. it lands anyway;
3. the client's next conditional write carries a stale ETag and gets 412;
4. the client concludes that it lost its lock.

That is what made T4 on a Telegram channel step down after every takeover
(docs/conditional-writes.md § Live; measured below).

The metadata record is written anyway, and a conditional write is decided
there. If a small object's bytes travel in that same entry, writing it costs
one log append and nothing else.

## How it behaves

| | inline | chunked |
|---|---|---|
| where the bytes are | the object's record: the log entry that wrote it, then snapshots; in RAM | chunks in the blob stores of `[chunk] store` |
| a write | one metadata log entry | upload the chunk(s), then one log entry |
| a read | memory | the blob stores (and the block cache) |
| replication | with the metadata (log + snapshots; with Raft, every node) | `[[replicas]]` / `[[shards]]` placement |
| sweeper, repair | not involved (no chunks) | as always |
| ETag | MD5 of the body | MD5 (single part), or MD5-of-MD5s-N (multipart) |

## Configuration

```toml
[inline]
max_object_size = "1 KiB"     # "off" = never inline (and drain what is inline)
budget = "64 MiB"             # all inline bytes; above it the oldest go out
low_watermark = 0.9           # ... until under this share of the budget
cool_after = "10m"            # out once not rewritten this long ("off" = only by budget)
min_inline = "30s"            # never out while written less than this long ago
cool_every = "1m"             # pause between cooler passes
cool_batch = 64               # most objects one pass moves out
```

Sizes are bytes or strings with a binary unit (`"1 KiB"`, `"64 MiB"`);
decimal units are refused. Durations are strings (`"30s"`, `"10m"`).
`"off"` switches off the two knobs that can be off; `0` / `"0s"` there is
refused as ambiguous, as everywhere in the config.

### `max_object_size`

Objects up to this many bytes are stored inline on `PutObject` and
`CopyObject`. Multipart uploads are never inline.

- `"off"` turns inlining off for new writes. The cooler keeps running and moves
  out every object that is still inline, `cool_batch` per `cool_every`
  (see [Turning it off](#turning-it-off)).
- Lowering it does not touch objects already inline above the new size.
  They leave by age or budget like any other inline object.
- Raising it costs RAM per object. The budget caps the total, so a higher
  threshold means fewer objects fit.

Rule of thumb: `1 KiB` covers locks, manifests and pointers. `4–16 KiB`
fits workloads full of small files, if the budget allows.

### `budget` and `low_watermark`

`budget` caps the sum of inline bodies (keys and other metadata are not
counted). It is soft:

- a write never fails because of it;
- when the total is over `budget`, the cooler moves out the oldest inline
  objects (by last write) until the total is at most
  `low_watermark × budget`, never one younger than `min_inline`.

The gap between `budget` and `low_watermark × budget` keeps the cooler from
moving one object per pass at the edge.

### `cool_after`

An inline object not rewritten for this long is moved out on the next pass.

- `"off"`: never by age, only by budget. Inline objects then stay until the
  budget pushes them out.
- Coordination objects are rewritten every few seconds and never reach
  `cool_after`. Objects written once (a WAL segment, a backup index) leave
  after `cool_after`.

### `min_inline`

No object written less than this long ago is moved out, by either rule:
a lock that was just written is not moved out because the tablet is over
budget (nor by `cool_after`, if that is set shorter). Keep it above the
renewal interval of whatever writes locks (T4: 2 s; the default 30 s leaves
room).

### `cool_every` and `cool_batch`

A pass every `cool_every` moves at most `cool_batch` objects, one after the
other. Each move is an upload to the blob store, so in a rate-limited
channel the cooler adds up to `cool_batch` uploads per `cool_every` (64 per
minute is about 1/s; Telegram takes about 0.3/s). Keep `cool_batch` small on
Telegram (e.g. 8), or let the age rule do the work slowly.

## Lifecycle of an object

```
PUT ≤ max_object_size ──► inline ──(not rewritten for cool_after,
        │                   ▲        or oldest while over budget)──► chunked
        │                   │                                          │
        │                   └──────── PUT ≤ max_object_size ◄──────────┘
        │
PUT > max_object_size ──► chunked ──── DELETE (either state) ──► gone
```

- A rewrite decides the new state from its own size: a small rewrite of a
  chunked object makes it inline again and releases its chunks.
- Cooling never changes the bytes, the ETag, `mtime`, tags or metadata. It
  changes only where the bytes are.

## S3 operations

**PutObject**
- With `Content-Length ≤ max_object_size`, the body is read into memory,
  Content-MD5 and `x-amz-checksum-*` are checked as usual, and the object is
  committed inline.
- Without a length (aws-chunked uploads), the gateway reads up to
  `max_object_size + 1` bytes. If the body ends within the limit, it is
  inline; otherwise the bytes already read are put back in front of the
  stream and the body is ingested as chunks. A `Content-Length` that turns
  out wrong behaves the same way.
- `PutObject` with tags, metadata and conditional headers works as for any
  object. The tags travel in the same command.

**GetObject, HeadObject.** The body and any range come from the record. No
store call, no chunk pin, no block cache.

**CopyObject**
- An inline source is copied as bytes. The copy is inline if its size is
  within the current `max_object_size`, else it is stored as new chunks
  (after inlining was turned off or the threshold lowered).
- A chunked source shares its extents as before, whatever its size.

**UploadPartCopy** from an inline source: the requested range of its bytes
is uploaded as the part's chunk.

**Multipart uploads** are never inline: a completed upload consists of its
parts' chunks.

**DeleteObject, overwrite.** Return the replaced chunks for release, as
before. An inline body has none.

**Conditional writes** (`If-Match`, `If-None-Match`) are checked against
the record, whatever its body (docs/conditional-writes.md).

**ListObjects, tagging, metadata, checksums, versions listing**: unchanged.

## The cooler

A background task like the sweeper and the repairer, started by `serve`.
Each pass:

1. **Pick candidates** from the state as of now:
   - every inline object whose `mtime` is older than `cool_after`;
   - if the inline total is over `budget`, also the oldest others until the
     projected total is at most `low_watermark × budget`;
   - never one younger than `min_inline`;
   - at most `cool_batch`, oldest first.
2. **For each candidate:**
   - upload its bytes through the ordinary ingest, so placement, replication
     and codec apply as for any write;
   - commit the new chunk together with a `Cool { bucket, key, etag, extents }`
     command, in one log entry;
   - if `Cool` is rejected because the object was rewritten or deleted since
     it was picked, release the chunk. The newer state stays.

The pass walks every object once to find candidates (a read of the
in-memory state, like the stats gauges do every 5 s).

## Durability and recovery

An inline object is exactly as durable as the metadata:

- it is in the log entry that wrote it, and in every snapshot taken while
  it is inline;
- every object's existence already depended on that metadata, so inline
  adds no new way to lose data;
- with Raft between nodes, it replicates with the log;
- under a `memory` log it disappears with the process, like all metadata.

**Boot and replay.** States are rebuilt from the newest snapshot plus the
log, inline bytes included. `inline_bytes` is recounted on load.

**Snapshots.** Inline bodies are part of the `s3` state, so a snapshot
grows by the inline bytes (bounded by `budget`).

**`restore --lossy`.** Starts a new log from the newest snapshot. Inline
objects come back as of that snapshot, the same as every other object's
metadata.

**Log retention.** Old log entries carrying inline bytes are collected with
the log they belong to (docs/log-retention.md).

## Cost and sizing

**RAM.** About the inline total (at most `budget`, plus whatever arrives
between cooler passes), plus per-object allocation overhead. Keys and the
other metadata of each object were in RAM already.

**Snapshot size.** Grows by the inline total.

**Log volume.** Every write of an inline object carries its bytes. A
coordination object rewritten every `t` seconds adds `size / t` bytes per
second until the next snapshot rebases the log. For example, a 250-byte lock
renewed every 2 s adds about 10 MiB a day, and with
`snapshot_every_entries = 10000` that log is collected long before.

**Memory per request.** An inline candidate's body is buffered in the
gateway, at most `max_object_size + 1` bytes per request. It is not counted
against `[ingest] memory_budget`. At 1 KiB that is 256 KiB at the default
256 concurrent requests.

**Blob store traffic.** Inline writes post nothing. The cooler adds one
upload per object it moves out.

## Operating it

### Metrics

| Metric | Kind | Meaning |
|---|---|---|
| `pigeonhole_inline_objects` | gauge | objects stored inline |
| `pigeonhole_inline_bytes` | gauge | their bytes (what `budget` caps) |
| `pigeonhole_cooled_total{reason}` | counter | objects moved out, by `age` or `budget` |
| `pigeonhole_cool_rejected_total` | counter | picked, but rewritten or deleted before the move |
| `pigeonhole_background_pass_seconds{task="cool"}` | histogram | one cooler pass |

Log lines:
- `PutObject ok … inline=true` for an inline write;
- `cooled inline objects by_age=… by_budget=… rejected=…` after a pass that
  did something;
- `cooler pass failed` with the error.

### What to watch

- **Over budget for long:** `pigeonhole_inline_bytes` stays above `budget`.
  The cooler cannot keep up (writes outpace `cool_batch / cool_every`), or
  everything is younger than `min_inline`. Raise `cool_batch`, lower
  `min_inline`, or lower `max_object_size`.
- **Many rejections:** a high rate of `pigeonhole_cool_rejected_total`
  means the cooler picks objects that are still being rewritten. That is
  harmless (each rejection costs one uploaded chunk, released at once) but
  wasteful. Raise `cool_after` or `min_inline`.
- **Slow passes** in a rate-limited channel: lower `cool_batch`.

### Recipes

- **Coordination on S3** (T4, Litestream, table formats): the defaults.
  Locks stay inline, so their writes never queue behind uploads.
- **Many small files** (backup indexes, thumbnails): raise `budget` to what
  the node can hold in RAM, consider `max_object_size = "4 KiB"`, and keep
  `cool_after` long enough for the hot set.
- **Disk-only deployments:** inlining still saves a record in a segment
  and its fsync per small object. The cost is RAM. Set `max_object_size = "off"` if memory is
  tight.

### Turning it off

Set `max_object_size = "off"` and restart. New writes are chunked. The cooler
keeps running and moves every inline object out, `cool_batch` per
`cool_every`, ignoring `cool_after` and `min_inline`. Draining N objects
takes `N / cool_batch × cool_every` (100 000 objects at 64 per minute take
about 26 h; raise `cool_batch` to go faster). When `pigeonhole_inline_objects`
reaches 0, nothing in the metadata holds object bytes any more.

### Upgrading

`S3State::VERSION` is 4. A binary with inline objects refuses logs and
snapshots of earlier versions, and older binaries refuse these (no
compatibility, as everywhere in this project).

## Design

### Data model

The object record has a body instead of bare extents:

```rust
pub enum Body {
    Chunks(Vec<Extent>),
    Inline(Vec<u8>),
}
```

- `S3Command::PutObject` carries a `body` (and its tags; see below).
- A new command moves an inline object out:

  ```rust
  /// Move an inline object out to chunks; its bytes, ETag and the rest stay.
  Cool { bucket, key, etag, extents }
  ```

- `S3State` keeps `inline_bytes`, the sum of inline bodies. It is
  maintained by `apply` (`write_object`, `remove_object`, `Cool`) and
  recounted in `rebuild()` after a snapshot loads (`#[serde(skip)]`).

### Determinism

Everything `apply` decides comes from the command and the state:

- **Inline or not** is the proposer's choice, carried in `body`. Replay
  does not look at the configuration.
- **Which objects to cool** is the cooler's choice, carried in `Cool`.
  `apply(Cool)` only checks that the object is still the inline version
  with that ETag. Otherwise it rejects (`PreconditionFailed`, or `NoSuchKey`
  after a delete) and leaves the state untouched. So any rewrite or delete
  wins over a concurrent cooling, and cooling never changes what a reader
  sees.
- **"Cold"** is measured by `mtime`, which is in the record. The decision
  would stay safe if it also used reads (kept in memory on the cooling
  node), since replay only checks the command.

### Tags in the put

Since conditional writes (docs/conditional-writes.md), the tags of a PUT
travel inside `PutObject`. Before, they were a second command,
`PutObjectTags`, in the same entry. Commands apply independently, so a
refused conditional `PutObject` would have let `PutObjectTags` tag the object
that was already there. One command decides the whole put. Inline bodies
keep it that way.

### One entry per write

Inline or not, a write is one log entry (docs/one-entry-per-operation.md):

- a chunked write: the chunk commit and the object;
- an inline write: the object alone;
- a cooling: the chunk commit and the `Cool`.

A rejected command still consumes its entry. For a chunked put or a cooling,
the chunks committed in that entry are then released by the caller.

## Alternatives considered

- **A priority lane in the channel's limiter.** Small or urgent writes would
  take the next send slot ahead of queued uploads. Rejected after measuring:
  a lone T4 leader renews its lock every 2 s (0.5 writes/s) and the channel
  takes 0.3 posts/s, so the lock alone outruns the channel; a lane would
  only reorder a queue that grows anyway.
- **Small objects to a faster store** (e.g. Discord or disk for objects
  under a size). It helps, but it is configuration rather than design, and
  it still costs a store call per write. With inlining the write costs
  nothing beyond the log entry that is written anyway.
- **Inline forever, no cooler.** It is simpler, but RAM and snapshots grow
  with every small object ever written. The cooler keeps the tablet holding
  only what is hot.

## Measurements

The T4 demo (`demos/etcd-on-telegram`) with objects in a Telegram channel
and metadata in Discord (`DEMO_BACKEND=chats`), 2026-10-30.

**Before (inlining off):**
- the objects channel took 11 PUTs, 24.1 s of store time, of which 21.8 s
  was waiting for our own send limiter
  (`pigeonhole_limiter_wait_seconds{op="send"}`);
- right after the takeover a lock renewal spent 4.8 s in the store, was
  abandoned by T4 at 5 s, and still committed. The next renewal carried the
  old ETag and got 412, and T4 stepped down;
- etcd answered `no leader` after the takeover.

**After (inlining on, defaults):**
- the takeover holds and writes go on through the new leader;
- lock PUTs take 0.84 s (p50), 2 s at most: the Discord log's commit, with
  no post;
- the Telegram channel got 2 posts in the whole run (the two objects over
  1 KiB, a checkpoint's OPTIONS and an SST) against about 13 before.

## Tests

- **State machine** (`gateway-s3/src/commands.rs`):
  - inline put, overwrite in both directions (inline ↔ chunks), delete;
  - `Cool` accepted, and rejected after a rewrite or a delete;
  - `inline_bytes` after each of these, and after encode, decode and
    rebuild.
- **Cooler policy** (`gateway-s3/src/cool.rs`):
  - by age, with hot objects staying;
  - by budget down to the watermark, oldest first;
  - `min_inline` and `cool_batch` respected.
- **Gateway** (`bin/tests/s3_api.rs`):
  - sizes at and around the threshold, with and without Content-Length;
  - ranges, ETag, empty object;
  - Content-MD5 on an inline body (a mismatch commits nothing);
  - copy and UploadPartCopy from an inline source; a copy follows the
    current threshold (inline source, inlining off: chunked copy);
  - an inline rewrite releases the chunks it replaces;
  - a cooler pass moves objects out with reads unchanged;
  - the cooler racing rewrites and deletes: no lost write, no leaked chunk.
- **Config** (`bin/src/config.rs`): units, defaults, `"off"`; `"0s"` and
  decimal units refused; with inlining off the cooler drains everything
  (`bin/src/background.rs`).
- **Live:** the T4 demo with objects in Telegram keeps its leader after a
  takeover (above).

## Limits and future work

- **Spare budget.** The cooler does not yet wait for the blob store's spare
  budget (`wait_idle`, as log collection does). A pass in a busy channel
  queues with client writes. Until then, keep `cool_batch` small there.
- **Reads.** The cooler goes by last write. An object that is read often but
  never rewritten still leaves after `cool_after`.
- **Raft.** With several nodes the cooler must run on the leader only. There
  is one node today.
- **Per bucket or prefix.** The settings are global (e.g. inlining only
  under `locks/` is not possible yet).
- **Warming.** A small chunked object becomes inline again only when it is
  rewritten, not when it is read often.
- **bytestream gateway** (CAS): not inlined.
- **`sweep_stress`** runs with inlining off. Inline objects under load are
  covered by the cooler race test only.
