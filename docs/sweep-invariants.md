# Sweep invariants

The sweeper deletes two kinds of backend blobs: parts of **released chunks**
(reclaim) and **orphans** — blobs no chunk owns (failed or abandoned uploads, a
dead writer's leftovers). Both run only on the leader: the background loops
skip their passes unless `MetaStore::is_leader` (a single node always
leads), and every delete batch is preceded by `MetaStore::confirm_active`
(below).

## Orphans: the upload horizon

Every upload is pinned **before** it sends anything (`UploadPins::pin`, in
`PartPacker`); the pin records the highest key the instance had returned at
that moment and is dropped only after the commit that references the part
(`CommitChunk` / `AddReplica`) is applied, or when the upload fails. An
ingest's chunks are committed with the operation that points at them (one
entry, see one-entry-per-operation.md), so their pins live across the whole
S3 / CAS write; a write that fails before (a checksum mismatch) commits
nothing and leaves the parts as orphans. The pins go **with the proposal**
(`PendingChunks::propose_in` and `BlobDb::commit_instance_replicas` hand them
to `Proposal::hold`): the tablet's batcher drops them once the entry is
applied, or once it is certain the entry will not be (refused before the
batcher, not the leader, the tablet stopped, the caller gone before the
write began). A caller dropped while its entry is being written (a client
gone, a timeout, an admin repair cancelled) no longer takes the pins with it,
so the horizon cannot rise past parts an entry is about to point at. Backend
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
   a round trip through the log; on a lease log also inside the lease's
   local deadline, docs/discord-lease.md); a fenced incarnation deletes nothing;
3. drops the owned keys once more: `confirm_active` returns once everything
   committed is applied, so an entry already in Raft when step 1 looked is
   in the state now. With the pins in the proposal this never finds anything;
   it is the second line should a pin go early.

No grace period and no per-batch in-flight check are needed: a key enters the
candidate range only after its pin is gone, i.e. after its commit is visible in
the state. After a restart or a change of leader there are no pins and
nothing returned yet; `ChunkStore::take_over`
(`crates/blob/pigeonhole-chunk-store/src/layer.rs`, run on boot when leading
and on every gain of leadership) seeds the horizon with the highest committed
key, so a dead writer's uploads below it are swept right away and the rest
after the first new upload. Pins live in the leader's process only: its
sweeper is the only one that deletes.

Only this deployment's blobs are candidates: every `BlobId` carries the
deployment's owner in its top 32 bits, and `BlobBackend::candidates(owner,
…)` returns only blobs named with that owner's ids, so a store shared by
mistake with another deployment never loses the other's blobs to this sweep
([store-owner.md](store-owner.md)).

What the listing yields per backend:

- **Telegram** (MTProto) reads the channel's id range up to the horizon, 100
  ids per `channels.getMessages`, and keeps only documents named with one of
  our blob ids (`ph-<32 hex>.bin`); it never deletes what is not ours.
- **Discord** lists the channel and keeps only messages whose attachment is
  named with one of our blob ids (`ph-<32 hex>.bin`, see `BlobId`).
- **Memory** lists its own messages.

Residual window: a newer generation can claim the log between the confirmation
and the delete of one batch. The deleted keys are still garbage for this
incarnation's state and below its horizon; the newer generation's own uploads
have higher keys than anything this process returned before them.

An answer lost after the put landed (the store failed after applying it, or
a dropped request went out late) leaves a blob whose key was never
returned, so it may lie above the horizon. It stays until a newer upload
returns a higher key; on an idle store it waits for the next write. The
fault scenarios check "no garbage left" only after one more upload.

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
- **Snapshots that outlive the log** (`[meta] snapshot` on another path or in
  a chat next to a local log): parts are deleted only once
  `MetaStore::restorable_position` (the base) reaches a position at or after
  the chunk's `BeginReclaim` (`BlobDb::reclaim_in_snapshot`; the gate is
  process memory, set when this process first commits or sees `BeginReclaim`).
  A lossy restore from the newest snapshot then has the chunk `reclaiming` or
  gone, never live under an object. Reclaim lags snapshots by up to
  `snapshot_every`.
- **Read pins.** `ChunkStore::read` pins the chunks it reads
  (`BlobDb::pin_reads`, process memory) and then checks they are not
  reclaiming. Reclaim commits `BeginReclaim`, then checks the pins and leaves a
  pinned chunk `reclaiming` for the next pass. One of the two always sees the
  other: parts are never deleted under a read, and a read that starts after
  `BeginReclaim` fails with `ChunkGone`. The S3 GET then looks the key up again
  (the object was overwritten or deleted) — up to `GET_ATTEMPTS` times.
- Chunks left `reclaiming` after a crash or a deferral are finished on the next pass.
- **Retired instances.** A reclaimed chunk with parts on an instance no
  longer in the placement (taken out of the config) keeps its metadata,
  `reclaiming`: the parts it can reach go, the others stay named in it.
  Dropping it would leave those blobs to nobody; the orphan sweep never
  lists a store that is not configured. Each pass counts such chunks
  (`SweepStats::retired_left`, gauge `pigeonhole_sweep_retired_left`, a
  warning); put the store back in the config and the next pass finishes
  them.

## Metrics

- `pigeonhole_sweep_retired_left` — reclaimed chunks kept for parts on a store no longer configured
- `pigeonhole_chunk_gone_total` — failed `retain` against reclaiming/missing chunks
- `pigeonhole_double_release_total` — `release` with `refs` already 0
