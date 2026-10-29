# Log retention

Status: **implemented** (retention, idle-only collection, fallback boot);
point-in-time restore is later. See Implementation at the end. Read [chain-log.md](chain-log.md) (the log over a
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
[meta]
keep_snapshots = 2   # the newest N bases the log was rebased on (≥ 1)
```

A **kept base** is one of the `keep_snapshots` newest bases; there is no
time-based policy (no clock in the decision, no time in the log). The
**watermark** is the oldest kept base. Kept:

- every kept base's snapshot;
- the log from the watermark up to the head.

Everything older is garbage: log blobs below the watermark and the snapshots
of bases that fell out. A snapshot that no base ever named (a newer one
replaced it before the log moved onto it) is garbage at once, as before.

The log is its own catalog: its registers (carried by every log blob, see
chain-log.md) hold the base and the older bases kept, newest first, so the
tail alone lists them (`Discovered::older`), with no walk and no index.
`keep_snapshots = 1` is the old behaviour, minus the immediate delete.

## Collection

The tablet decides, the log store deletes:

1. `LogStore::rebase(gen, base, keep)` writes the new base with the older
   bases kept (at most `keep - 1`) and returns the ones that fell out.
2. The tablet removes the snapshots of those, after answering the purge.
3. The log store collects below the watermark in the background (one
   collection at a time): it walks back to the oldest blob a reader from the
   watermark needs and prunes the chain below it (`Chain::prune`). Order
   does not matter: neither a dropped base's snapshot nor the log below the
   watermark is needed by anything kept.
4. Prunes delete in batches (Telegram up to 100 messages per call), each
   batch on spare budget only, as below.

**Pacing.** The collector has a rate of its own (a cap, e.g. one batch a
second) and the lowest priority in the store's shared budget: before each
batch it waits until the store is idle, then spends tokens as usual. While
the store is busy it makes no progress at all, and that is fine: nothing
waits for it. When the store is idle it collects at its own rate. A 429 on a
delete backs it off like any caller.

What idle means (built, `rate_limit.rs`):

- **Telegram, Discord**: `ChatLimiter::wait_idle(budget)` returns once
  nobody waits for the chat's bucket or the bot's, and both are at least
  half full.
- **`local_disk`, memory**: a `StoreLimiter` counts operations in flight;
  `wait_idle` returns when none is (the disk's budget is its I/O: deletes and
  hole punching compete with appends and their syncs). An optional rate
  (`with_rate`) adds a token bucket with the same rule; none by default.

`Chain::prune` is background work by contract: memory, disk and Telegram
prune a batch per idle moment already; so does Discord.

**Safety across incarnations.** A newer generation's registers start from
the older one's tail and only add bases, so its watermark is at or above the
older one's. An old incarnation still collecting therefore never deletes
what the newer one keeps. Lowering `keep_snapshots` in the config of a newer
incarnation is the one way to lose kept history, and that is the point of
the change.

## Fallback boot

A boot that cannot load the newest base's snapshot (missing, fails its
checksum, does not decode) tries the older bases kept, newest first, loads
the first that works and replays the log from there to the head. The result
is the same state: everything at or below every base is committed, so the
replayed entries are the ones the newer snapshot was built from. The fallback
is therefore safe on any error, transient ones included; it is only slower.

The stored snapshot already carries a crc (`StoredSnapshot::encode`), and its
ref the blob's crc, so a damaged copy is told apart from a valid one. The
log serves positions behind the current base down to the watermark;
further back is `Lost`. The node tells Raft that its log starts at the base
it actually loaded (`Shared::base`), not at the log's newest.

The tablet logs the fallback, counts it
(`pigeonhole_meta_snapshot_fallbacks_total`) and asks for a snapshot right
after the boot, so the damaged one stops being the newest base.

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
while its chunk is retained too. Blobs uploaded after the position are orphans
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

`crates/testing/pigeonhole-sim/tests/retention.rs`, under the simulator with
many seeds:

1. **A damaged newest snapshot after a takeover.** Writer A is taken over by
   B at a random moment (A's collection may still run), B writes until the
   log keeps `keep` bases (2 or 3, drawn), its newest snapshot is then
   damaged (`Faulty::damage`), and a third boot holds every command A and B
   had acknowledged. Afterwards the snapshot store holds at most `keep + 1`
   snapshots. With the fallback disabled the scenario fails at once.
2. **One base kept**: the same damage stops the boot (proof that scenario 1
   tests the fallback).

Later, **point-in-time restore**: an S3 workload records the object list at
some positions; a restore to a random kept position reads back exactly that
list, every object whole.

## Implementation

- `LogStore::rebase(gen, base, keep) -> dropped bases`; `Discovered::older`;
  `read` down to the watermark. Chain log registers carry `older`
  (`LOG_FORMAT` 4).
- `[meta] keep_snapshots` (default 2) → `MetaOptions::keep_snapshots`; the
  write loop removes dropped bases' snapshots.
- Fallback boot in `MetaStore::open` (`load_some_base`).
- Idle-only pacing: `ChatLimiter::wait_idle`, `StoreLimiter` for disk and
  memory; `Chain::prune` on every store waits for idle per batch. The
  collector's own rate cap is not built: idle-only was enough.
- Tests: the conformance case `older_bases_are_kept_and_readable`, the
  tablet test `a_boot_falls_back_to_an_older_base_when_the_newest_snapshot_is_unreadable`,
  and the scenarios above.
- Not done: the reclaim gate on the watermark, `restore --to`.

## Open questions

- **Snapshots no base names.** An incarnation that stores a snapshot and is
  fenced (or crashes) before it rebases on it leaves the snapshot behind;
  nothing ever removes it (as before retention). The simulation found
  three such copies after one takeover. A sweep of the snapshot store at boot
  (this log's snapshots at or below the newest base that no kept base names)
  would collect them; it needs listing by label on every snapshot store.

- **Defaults for chats.** Three snapshots and the log between them is cheap
  for a small tablet. Is a size cap (`max_bytes`) needed as well, collecting
  early when the retained log grows past it?
- **One watermark or two.** Log and snapshots retained together keep the
  model simple. Old snapshots without their log (`restore --lossy` targets)
  would be cheaper than log, but only useful for a lossy restore.
- **A store that is never idle.** Under a steady full load the collector
  never gets a token and garbage grows. Accept it (load ends), or, on a disk
  running out of space, let it take a token after waiting long enough?
- **Limits for disk and memory.** Operations per second only, or bytes per
  second as well (a disk is bound by bandwidth, a chat by calls)?
