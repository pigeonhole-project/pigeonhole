# Small objects inline in the metadata, cooled out when cold

Status: implemented (2026-10-30).

## Why

Every S3 object is today a chunk in a blob store plus a metadata entry
(key → extents, ETag). For a 200-byte object the chunk is all cost: in a
Telegram channel it is a post, and a channel takes ~20 posts a minute
(`[telegram] send_rate_per_sec = 0.3`). Software that coordinates through S3
writes exactly such objects, often: T4 renews its 223-byte `leader-lock`
every 2 s with `If-Match`, Litestream its `lock.json`, table formats their
pointer files. Behind a burst of large uploads such a write waits its turn
for the channel, times out in its client, lands anyway, and the client's
next conditional write carries a stale ETag (the T4 step-down in
docs/conditional-writes.md § Live).

The metadata entry is written anyway, and the condition is decided there.
If a small object's bytes travel in that same entry, its write is one log
append and no post at all; its read is a lookup in memory.

## What

An object whose body is at most `max_object_bytes` is stored **inline**:
its bytes are part of its record in the `s3` namespace (in the log entry
that writes it, then in snapshots), and it has no chunks.

A background **cooler** moves inline objects out to chunks, as an ordinary
object, when they are cold (not rewritten for `cool_after_secs`) or when the
inline bytes of the tablet exceed `budget_bytes`. A cooled object is like
any other until it is rewritten; a rewrite of a small object is inline
again. So hot small objects (locks, manifests) live in the tablet and the
rest of the small objects drift out of it.

Every knob is configurable; `max_object_bytes = 0` turns inlining off and
the gateway behaves as today.

```toml
[inline]
# Objects up to this size are stored in the metadata, not as chunks
# (0 = never). Applies to PutObject and CopyObject; multipart uploads never
# inline.
max_object_bytes = 1024
# All inline bytes of the tablet. Above it the cooler moves the coldest
# inline objects out until they fit under low_watermark * budget_bytes.
# A write never fails on the budget (it is enforced after the fact).
budget_bytes = 67108864            # 64 MiB
low_watermark = 0.9
# An inline object not rewritten for this long is moved out (0 = only by
# budget). Locks and manifests rewritten every few seconds never get there.
cool_after_secs = 600
# Never move out an object written less than this long ago, budget or not
# (a lock in use stays inline).
min_inline_secs = 30
# How often the cooler looks; how many objects one pass moves at most.
cool_every_secs = 60
cool_batch = 64
```

## Data model

`ObjectRec` gets a body instead of bare extents:

```rust
enum Body {
    Chunks(Vec<Extent>),
    Inline(Vec<u8>),
}
```

`S3Command::PutObject { extents, .. }` becomes `PutObject { body, .. }`.
New command:

```rust
/// Move an inline object out to chunks; its bytes are unchanged.
Cool { bucket, key, etag, extents }
```

`apply(Cool)` checks that the object is still inline with that ETag
(`Expect::ETag`, docs/conditional-writes.md) and replaces its body with the
extents; otherwise it rejects (`PreconditionFailed` / `NoSuchKey`) and the
cooler releases the chunks it committed. A concurrent rewrite therefore
always wins over cooling, and cooling never changes what a reader sees.

`S3State` keeps `inline_bytes` (sum of inline bodies), derived in
`rebuild()` and maintained by `apply`. `S3State::VERSION` bumps.

`mtime` is already in the record; it is what "cold" is measured by. Reads
are not in the log; a cooler that preferred objects not read lately (an
in-memory record on the node that cools) would still be safe, since the
choice travels in the `Cool` command and replay only checks it. Not built:
the cooler goes by `mtime` alone.

## Paths

- **PutObject.** With `Content-Length ≤ max_object_bytes` the gateway reads
  the body into memory, checks Content-MD5 and checksums as today, and
  proposes `PutObject { body: Inline }`, without ingest or a chunk commit.
  With no length (aws-chunked), it reads up to `max_object_bytes + 1`
  bytes: if the body ends there it is inline, else those bytes are prepended
  to the stream and it is ingested as today. The ETag is the MD5, as for
  any single-part object.
- **GetObject / HeadObject.** Inline: the body (and ranges) from the
  record, no store call; the read holds no chunk pin.
- **CopyObject.** An inline source copies its bytes (inline if the
  destination's size qualifies, which it does: same size). A chunked source
  copies extents as today, whatever its size.
- **UploadPartCopy** from an inline source: its bytes are ingested as the
  part.
- **Multipart** never inlines: a completed upload is made of parts' chunks.
- **Overwrite / delete.** Released chunk ids as today; an inline body
  releases nothing.
- **Conditional writes** are unchanged: the condition is on the record,
  whatever its body.
- **ListObjects, tags, metadata, checksums**: unchanged.

## The cooler

A background task like the sweeper and the repairer (only on the node that
holds the tablet; with Raft, the leader). Each pass:

1. Candidates: inline objects with `mtime` older than `cool_after_secs`;
   if `inline_bytes > budget_bytes`, also the oldest by `mtime` until the
   projected total is under `low_watermark × budget_bytes`. Never one
   written within `min_inline_secs`. At most `cool_batch`.
2. For each: ingest its bytes as a chunk (the ordinary write path, so the
   placement, replication and codec settings apply), then commit the chunk
   along with `Cool { etag }` in one entry. A rejected `Cool` releases the
   chunk.

A pass moves at most `cool_batch` objects, one after the other, every
`cool_every_secs`. It does not yet wait for the stores' spare budget (as the
log's collection does with `wait_idle`), so a pass in a busy channel queues
with client writes: keep `cool_batch` small there.

## Durability and cost

An inline object is as durable as the metadata log and its snapshots: the
place every object's existence already depends on. With Raft between nodes
it replicates with the log. Under a `memory` log it is gone with the
process, as all metadata is.

The cost is RAM and snapshot size, bounded by `budget_bytes` (plus what
arrives between cooler passes), and log volume: every rewrite of an inline
object carries its bytes. A 223-byte lock rewritten every 2 s adds ~10 MiB
of log a day until the next snapshot rebases it away.

## Observability

Gauges `pigeonhole_inline_bytes`, `pigeonhole_inline_objects`; counters
`pigeonhole_cooled_total{reason="age"|"budget"}`,
`pigeonhole_cool_rejected_total`; a cooler pass in
`pigeonhole_background_pass_seconds{task="cool"}`. The `PutObject ok` log
line says `inline=true` for an inline write.

## Measured (step 0, 2026-10-30)

The T4 demo with objects in Telegram (`DEMO_BACKEND=chats`,
`DEMO_LOG_LEVEL=…,pigeonhole::request_log=debug,pigeonhole_blob::metrics=debug`):

- The objects channel took 11 PUTs in the run, 24.1 s of store time, of
  which 21.8 s was waiting for our own send limiter
  (`pigeonhole_limiter_wait_seconds{op="send"}`); the rest is the post.
- After the takeover a lock renewal spent 4.8 s in the store, was abandoned
  by T4 at 5 s, and still committed; the next renewal (3.2 s in the store)
  carried the old ETag and got 412: the step-down.
- A lone T4 leader is in fast mode: it renews every 2 s and flushes a WAL
  segment per write, more than the channel's 0.3 posts/s on its own. So a
  priority lane in the limiter would not do: the lock alone outruns the
  channel. Inline, neither the lock (223 B) nor the WAL segments (~150 B)
  post at all.

## Result (2026-10-30)

The same demo with inlining on (the default 1 KiB): the takeover holds and
writes go on through the new leader; no lock renewal times out. Lock PUTs
take 0.84 s (p50), 2 s at most: the Discord log's commit, no post. The
Telegram channel got 2 posts in the whole run (the two objects over 1 KiB,
a checkpoint's OPTIONS and an SST) against ~13 before.

## Steps

0. ~~Measure first.~~ Done, above.
1. `Body`, `PutObject` with an inline body, `Cool`, `inline_bytes`;
   `[inline]` config; the inline write and read paths; copy.
2. The cooler, with its metrics.
3. Checks:
   - unit: `apply` for inline put / overwrite (inline ↔ chunks) / delete /
     `Cool` (accepted, rejected after a rewrite); `inline_bytes` after
     replay and after a snapshot equals a recount;
   - gateway: sizes around the threshold, with and without
     Content-Length; ranges; copy both ways; Content-MD5 on an inline body;
   - sim: the cooler racing rewrites and deletes (no lost write, no leaked
     or double-released chunk); `sweep_stress` with inlining on;
   - live: the T4 demo with objects in Telegram (`DEMO_BACKEND=chats`)
     keeps its leader after a takeover.

## Not now

- Per-bucket or per-prefix settings (e.g. inline only under `locks/`).
- Inlining in the bytestream (CAS) gateway.
- Warming: moving a small chunked object back inline when it gets hot. A
  rewrite does that already; reads alone do not.
