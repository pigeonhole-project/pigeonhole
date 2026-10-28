# Sweep invariants

The sweeper deletes two kinds of backend blobs: parts of **released chunks**
(reclaim) and **orphans** — blobs no chunk owns (failed or abandoned uploads, a
dead writer's leftovers). Both run only while the tablet holds its generation.

## Orphans: the upload horizon

Every upload is pinned **before** it sends anything (`UploadPins::pin`, in
`PartPacker`); the pin records the highest key the instance had returned at
that moment and is dropped only after the commit that references the part
(`CommitChunk` / `AddReplica`) is applied, or when the upload fails. Backend
keys grow with time (Telegram / Discord message ids, the memory store's
counter), so whatever an in-flight upload gets lies above every pin's floor.

`UploadPins::horizon(instance)` = the lowest floor of a live pin, or the
highest returned key when nothing is pinned. At or below it every blob of this
process is either committed — owned by a chunk in the tablet's state — or
garbage. The sweeper lists candidates up to the horizon and, per batch, right
before deleting:

1. drops every key some chunk in the state owns (`owned_part_keys_among`,
   live or released: released parts are reclaim's to delete);
2. confirms the tablet still holds its generation (`MetaStore::confirm_active`,
   a round trip through the log); a fenced incarnation deletes nothing.

No grace period and no per-batch in-flight check are needed: a key enters the
candidate range only after its pin is gone, i.e. after its commit is visible in
the state. After a restart there are no pins and nothing returned yet;
`ChunkStore::open` seeds the horizon with the highest committed key, so a dead
writer's uploads below it are swept right away and the rest after the first new
upload.

What the listing yields per backend:

- **Telegram** cannot list: every message id up to the horizon is a candidate,
  ours or not. A chat that holds blobs must hold nothing else that matters —
  in particular **not the Telegram LogStore** (roadmap step 6 uses its own chat).
- **Discord** lists the channel and keeps only messages whose attachment is
  named with one of our blob ids (`ph-<32 hex>.bin`, see `BlobId`).
- **Memory** lists its own messages.

Residual window: a newer generation can claim the log between the confirmation
and the delete of one batch. The deleted keys are still garbage for this
incarnation's state and below its horizon; the newer generation's own uploads
have higher keys than anything this process returned before them.

## Reclaim and reads

```
live (refs ≥ 1)
  │ Release → refs = 0
  ▼
live (refs = 0)          ← still visible to Retain until BeginReclaim
  │ BeginReclaim: refs = 0 and not reclaiming → reclaiming
  ▼
reclaiming               ← Retain → ChunkGone; a new read → ChunkGone
  │ no read pins, in the base snapshot → delete backend parts, then DropChunk
  ▼
gone
```

- `Retain` only succeeds if every listed chunk is live with `refs > 0` (all-or-nothing).
- `Release` rejects a double release (`refs` would go below 0); it does not clamp.
- `BeginReclaim` is committed to the log before any part is deleted
  (`propose` returns only after the append), so a restart or failover never
  brings back a reference to deleted parts.
- **Snapshots that outlive the log** (`[meta.snapshot]` on another path or in
  a chat next to a local log): parts are deleted only once
  `MetaStore::restorable_position` (the base) reaches a position at or after
  the chunk's `BeginReclaim` (`BlobDb::reclaim_in_snapshot`; the gate is
  process memory, set when this process first commits or sees `BeginReclaim`).
  A lossy restore from the newest snapshot then has the chunk `reclaiming` or
  gone, never live under an object. Reclaim lags snapshots by up to
  `every_secs`.
- **Read pins.** `ChunkStore::read` pins the chunks it reads
  (`BlobDb::pin_reads`, process memory) and then checks they are not
  reclaiming. Reclaim commits `BeginReclaim`, then checks the pins and leaves a
  pinned chunk `reclaiming` for the next pass. One of the two always sees the
  other: parts are never deleted under a read, and a read that starts after
  `BeginReclaim` fails with `ChunkGone`. The S3 GET then looks the key up again
  (the object was overwritten or deleted) — up to `GET_ATTEMPTS` times.
- Chunks left `reclaiming` after a crash or a deferral are finished on the next pass.

## Metrics

- `pigeonhole_chunk_gone_total` — failed `retain` against reclaiming/missing chunks
- `pigeonhole_double_release_total` — `release` with `refs` already 0
