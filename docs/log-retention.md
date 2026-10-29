# Log retention

Status: **proposal**. Read [chain-log.md](chain-log.md) (the log over a
`Chain`), [meta-backends.md](meta-backends.md) (`LogStore`, snapshots, lossy
restore) and [sweep-invariants.md](sweep-invariants.md) (reclaim) first.

## Why

Today a rebase collects at once: the log drops everything behind the new base
(`Chain::prune`, now in a background task), and the tablet removes the
snapshot the log started from before. Neither is needed:

- **Correctness and boot time** do not depend on it. A boot reads the tail,
  loads the snapshot the base names and reads back to the base; nothing
  behind it is touched.
- **Space** matters on a local disk and in memory, eventually. In a chat it
  is hygiene: Telegram and Discord keep messages without a quota.

What collecting at once costs:

1. **Rate limits.** A delete spends the same budget as an append. On Discord
   collection held the log writer for 112 s after a snapshot. In the
   background it still competes with writes.
2. **The way back.** If the newest snapshot cannot be read (a bug in its
   encoding, a damaged copy, a lost message), the previous snapshot plus the
   log after it would rebuild the same state. We delete both right away, so a
   damaged snapshot means `restore --lossy` and lost changes.
3. **Risk.** Deletion is the one dangerous operation. Doing it rarely, in one
   place and below a watermark that only rises leaves few races to think
   about.

So: keep the recent past by a policy, collect lazily in one place, and use
what is kept: a fallback boot now, a point-in-time restore later.

## What is kept

```toml
[meta.retain]
snapshots = 2     # the newest N bases the log was rebased on (≥ 1)
```

A **retained base** is one of the `snapshots` newest bases; there is no
time-based policy (no clock in the decision, no time in the log). The
**watermark** is the oldest retained base. Kept:

- every retained base's snapshot;
- the log from the watermark up to the head.

Everything older is garbage: log blobs below the watermark and the snapshots
of bases older than the watermark. A snapshot that no base ever named (a
newer one replaced it before the log moved onto it) is garbage at once, as
today.

The chain is its own catalog: every rebase record names its snapshot, so
walking back from the tail lists the bases (`at`, snapshot ref) without a
separate index.

Proposed defaults: memory and `local_disk` `snapshots = 2`; chats
`snapshots = 3`. With `snapshots = 1` the behaviour is today's, minus the
immediate delete.

## The collector

One background task per tablet, next to the snapshot ticker, replaces the
collection in `rebase` and the removals in the write loop:

1. After each rebase (and once at boot) it walks back from the tail to the
   watermark, listing bases. The walk reads what a boot reads anyway, plus
   the retained log.
2. It computes the watermark from the policy. The watermark never moves
   back.
3. It deletes the snapshots of bases below the watermark, then prunes the
   chain below the watermark's rebase record (`Chain::prune`). Snapshots go
   first: a crash in between leaves log blobs no base needs, never a base
   without its snapshot.
4. It deletes in batches where the store has them (Telegram deletes up to 100
   messages per call; Discord bulk-deletes up to 100 messages younger than 14
   days), paced as below.

**Pacing.** The collector has a rate of its own (a cap, e.g. one batch a
second) and the lowest priority in the store's shared budget (the bot's and
the chat's buckets in `rate_limit.rs`): it takes a token only when nobody
else is waiting and the bucket has spare tokens, otherwise it sleeps and
looks again. While the store is busy it makes no progress at all, and that
is fine: nothing waits for it. When the store is idle it collects at its own
rate. A 429 on a delete backs it off like any caller. The limiter needs one
call for this, a non-blocking `try_acquire_idle(class)` that fails instead of
queueing.

**Safety across incarnations.** A newer generation sees every rebase record
an older one saw, plus its own, so its watermark is at or above the older
one's. An old incarnation still collecting therefore never deletes what the
newer one keeps. Shrinking `snapshots` in the config of a newer incarnation is the one way to
lose retained history, and that is the point of the change.

## Fallback boot

A boot that cannot use the base it found (the snapshot is missing, fails its
checksum or does not decode) walks back to the previous retained base, loads
its snapshot and replays the log from there to the head. The result is the
same state: everything at or below every base is committed, so the replayed
entries are the ones the newer snapshot was built from. The fallback is
therefore safe on any error, transient ones included; it is only slower.

Needs:

- a checksum on the stored snapshot envelope (`StoredSnapshot`, a format
  bump), so a damaged snapshot is told apart from a valid one;
- `LogStore` lists the retained bases (`discover` returns them, newest
  first), and `read` serves positions behind the current base down to the
  watermark (today they are `Lost`).

The tablet logs the fallback and counts it (`snapshot_fallbacks_total`), and
stores a fresh snapshot soon after, so the damaged one stops being the base.

## Point-in-time restore (later)

`pigeonhole restore --to <position>`: the state at any position at or above the watermark, from
the base below it plus the log up to the position. The restored tablet starts
a new log (a new `log_id`, as a lossy restore does); the old log's tail after
the position is abandoned.

What makes it consistent is reclaim. A part released and deleted after the
position would be referenced by the restored state. Reclaim already waits for
`restorable_position` when snapshots outlive the log
([sweep-invariants.md](sweep-invariants.md) § Reclaim): parts are deleted
only once the base passes the chunk's `BeginReclaim`. With retention the gate
becomes the **watermark**: a part is deleted only when no retained base or
retained log can bring its chunk back. The cost is that released data stays
for the retention window too. Blobs uploaded after the position are orphans
of the restored state; the sweeper's horizon (seeded from the highest
committed key) collects them after the first new upload.

The fallback boot does not need this: it rebuilds the current state, where
those chunks are reclaiming or gone already.

## What each store pays

| Store | Retained log | Retained snapshots | Collector |
|---|---|---|---|
| memory | RAM: the log since the oldest retained base | N copies of the state | free |
| `local_disk` | segments below the watermark punched later | files | free; bounds disk use |
| Telegram | messages | documents | batched deletes, paced |
| Discord | messages and threads | attachments | bulk deletes while younger than 14 days, single deletes after |

The boot cost does not change: a boot reads back to the newest base only,
unless it falls back.

## Simulation scenarios

In `pigeonhole-sim`, under the simulator with many seeds:

1. **A damaged newest snapshot.** The tablet writes, snapshots and rebases a
   few times; the snapshot store damages its newest snapshot (a `Faulty`
   plan that corrupts a chosen put); a reboot falls back and holds every
   acknowledged command. Without retention (`snapshots = 1`) the same seed
   fails to boot: proof that the scenario tests the fallback.
2. **Collector across a takeover.** An old incarnation collects while a newer
   one writes and rebases; then the newer one's newest snapshot is damaged;
   a reboot still finds a retained base.
3. **Bounded space.** After `quiesce()` and a few rebases the stores hold at
   most the retained snapshots and the log from the watermark: nothing older,
   nothing retained missing.
4. Later, **point-in-time restore**: an S3 workload records the object list
   at some positions; a restore to a random retained position reads back
   exactly that list, every object whole.

## Steps

1. Checksum in the snapshot envelope; `discover` lists the retained bases;
   `read` behind the current base down to the watermark. The conformance
   case `rebase_covers_and_collects` becomes "readable down to the watermark,
   `Lost` below".
2. The collector and `[meta.retain]`: the watermark, snapshot removal moved
   out of the write loop, collection out of `rebase`, batching; idle-only
   tokens in the limiter.
   Scenario 3.
3. Fallback boot, its metric; scenarios 1 and 2.
4. Docs: meta-backends.md (what a boot does, retention), README § Durability.
5. Later: the reclaim gate on the watermark, `restore --to`, scenario 4.

## Open questions

- **Defaults for chats.** Three snapshots and the log between them is cheap
  for a small tablet. Is a size cap (`max_bytes`) needed as well, collecting
  early when the retained log grows past it?
- **One watermark or two.** Log and snapshots retained together keep the
  model simple. Old snapshots without their log (`restore --lossy` targets)
  would be cheaper than log, but only useful for a lossy restore.
- **A store that is never idle.** Under a steady full load the collector
  never gets a token and garbage grows. Accept it (load ends; disk space is
  the only real limit, and the disk store has no rate limit), or let it take
  a token after waiting long enough?
