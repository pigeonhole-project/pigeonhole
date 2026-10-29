# The log on top of blob storage: `Chain`

Status: **steps 1–4 implemented** (see Implementation notes). Read [meta-backends.md](meta-backends.md) (the `LogStore`
and `SnapshotStore` contracts), [telegram-mtproto.md](telegram-mtproto.md) (the
order-fenced channel log, implemented), [discord-log.md](discord-log.md) (the
thread-fenced log, proposed) and [sweep-invariants.md](sweep-invariants.md)
first.

## Why

Blob storage and the metadata log are independent today. Every place that can
hold a log has its own implementation: `LocalLogStore` (segment files),
`MemoryLogStore`, `ChannelLogStore` over a `ChannelApi` (Telegram), a planned
`LinkableChat` log (Discord), a planned slot log (OpenDAL); and the same for
snapshots (`LocalSnapshotStore`, `MemorySnapshotStore`,
`ChannelSnapshotStore`). `ChannelApi` (`send_document`, `get_messages`,
`download`, `delete`) is `BlobBackend` again under another name.

The goal: **blob storage at the bottom, the log on top**. One `LogStore`
implementation and one `SnapshotStore` implementation, generic over a store;
a store's specifics live only in how it implements a general trait.

## The model

Every store already is a *channel*: immutable blobs, keys chosen by the store,
growing with time (Telegram ids, Discord snowflakes, the memory counter, disk
keys) — the sweeper's horizon rests on it. A log needs exactly one thing more:

> **First successor wins.** Every blob has at most one successor; of several
> blobs put after the same `prev`, exactly one wins, and every reader agrees
> which.

Each store has an atomic operation for it:

| Store | The operation |
|---|---|
| Telegram (MTProto) | the order of ids in a channel: the first message naming `prev` wins |
| Discord | a thread from a message: started once (`160004` after) |
| OpenDAL (s3, gcs, fs) | a write with `if_not_exists` |
| local disk | an append under `flock` after checking the tail |
| memory | a mutex |

## Traits

`BlobBackend` / `Sweepable` stay as they are (moved, see Layering). New:

```rust
/// A store whose blobs can form chains: the log's capability.
#[async_trait]
pub trait Chain: BlobBackend {
    /// Put `data` as the successor of `prev` (`None`: the start of a chain).
    /// Of all `put_after` calls with the same `prev`, exactly one gets
    /// `Won(id)`; the others get `Lost { winner }` (their blob is garbage, the
    /// store may delete it). The outcome never changes once returned.
    async fn put_after(&self, prev: Option<&Self::Id>, meta: &ChainMeta, data: Bytes)
        -> Result<After<Self::Id>>;

    /// The winning successor of `id`, if any yet.
    async fn successor(&self, id: &Self::Id) -> Result<Option<Self::Id>>;

    /// The last blob of the chain that contains `from`, found the store's
    /// cheapest way (top-down from the newest blob, forward listing, the last
    /// record of a segment).
    async fn tail(&self, from: &Self::Id) -> Result<Self::Id>;

    /// Up to `limit` chain members after `from`, in order, with their
    /// metadata; data only when `with_data` (replay reads in pages, never one
    /// blob per call).
    async fn chain_from(&self, from: &Self::Id, limit: usize, with_data: bool)
        -> Result<Vec<ChainItem<Self::Id>>>;
}

pub enum After<Id> { Won(Id), Lost { winner: Id } }

/// Small metadata every store keeps readable without downloading the blob
/// (a Telegram / Discord file name, an OpenDAL key or object metadata, a
/// record header on disk): enough to decide links and fences.
pub struct ChainMeta { pub kind: ChainKind, pub gen: u64 }
pub enum ChainKind { Claim, Batch, Snapshot }

pub struct ChainItem<Id> { pub id: Id, pub prev: Option<Id>, pub meta: ChainMeta, pub data: Option<Bytes> }
```

What `Chain` promises and what it does not:

- **Linearizable links.** `Won` is final; `successor` and `tail` never
  contradict a returned `Won`. How the store gets there (reading a range,
  re-reading ids not visible yet, a thread, a conditional write) is its own
  business.
- **No timing assumptions** in the protocol above it. A store may need some
  internally (Telegram re-reads an empty id a few times before taking it as
  deleted); they stay inside its implementation.
- A store that cannot implement `Chain` cannot hold a log; the type says so.

## `ChainLogStore<C: Chain>`: the one `LogStore`

Blobs of a log chain: `Claim { gen }` and `Batch { gen, first, entries, base,
vote, head_term, collected }` (the registers as in today's channel log), both
postcard, versioned. `ChainMeta` carries kind and generation.

**Writer invariant**: a writer calls `put_after(prev)` only with a `prev` it
got `Won` for, or found as the tail. So every blob that won its link is in the
chain, and the tail is the newest winner.

- **claim(g)**: `tail(base)` → `T`; `put_after(T, Claim(g))`. `Lost { winner }`
  → read the winner's meta: a claim or batch of a generation ≥ `g` →
  `Fenced { current }`; else retry from the new tail. `Won` → every older
  writer's next `put_after` names a `prev` that already has a successor, and
  loses. The head is the last batch before the claim.
- **append(g, entries)**: `put_after(head, Batch)`; `Won` → committed, new
  head; `Lost { winner }` → `Fenced { current: winner's gen }`.
- **truncate(g, from)** and **save_vote(g, vote)**: a batch with `first = from`
  and no entries / with the new vote; same as append.
- **confirm(g)**: `successor(head)` is `None` → still ours (no claim linked
  after us), else `Fenced`. No write in the protocol; a store may write to
  answer (Telegram learns its top by posting).
- **discover**: start at the newest `Batch` that carries a base (the rebase
  blob, found by `tail` from any chain member, or the store's oldest blob),
  `tail`, read the registers of the last batch.
- **read(from, head)**: `chain_from` in pages with data, from the rebase blob.
- **rebase(g, base)**: a batch carrying the new base; once `Won`, delete the
  chain's blobs below it (`BlobBackend::delete`), except what a store's own
  link check still reads (each store says what, e.g. Telegram's range below
  the rebase blob).

The validity rules of the Telegram order fence (claims of higher generations
in `(since, batch)`) become the Telegram `Chain` implementation's way to decide
"first successor"; generations are only needed for `Fenced { current }`.

## `BlobSnapshotStore<B: BlobBackend>`: the one `SnapshotStore`

`put` = `BlobBackend::put`, `SnapshotRef` = the blob's id (+ crc), `get`,
`remove` = delete. `newest()` (lossy restore only) = the newest blob whose
meta says `Snapshot`: on a `Chain` store, snapshots form a chain of their own
and the newest is its tail; on others, a listing.

## Implementations

| Store | `put_after(prev)` | `successor(id)` | `tail(from)` | `chain_from` |
|---|---|---|---|---|
| Telegram | send with `prev` in the name; `Won` iff no message naming `prev` lies in `(prev, b)` — fast path `b == prev + 1` | the first id after `id` whose name names it | top-down from our newest message: the newest message that won its link | `getMessages` pages of 100 |
| Discord | message, then a thread from `prev` named after it; `160004` → `Lost` | `prev`'s `thread` field | the newest 100 messages with `thread` | listing pages of 100 |
| OpenDAL | `PUT after/<prev>` with `if_not_exists` (one request) | stat `after/<id>` | `list` from `after/<from>` (keys grow) | `list` + reads |
| memory | under the mutex | the table | the table | the table |
| local disk | under `flock`: the tail is `prev`, append a record, one fsync | the next record | the last record | sequential segment reads |

Telegram keeps its implementation notes (empty ids taken as deleted after
re-reads, probes to learn the top, cached locations); Discord its spike
findings (archive links, delete the message before its thread, 50 links per
300 s).

## `DiskBlobStore` on segments

For the disk to cost what `LocalLogStore` costs today (one fsync per append),
the store itself changes, for data blobs too:

- Blobs are records in append-only segment files (`seg-<seq>`), Haystack /
  Bitcask style: `MAGIC | len | crc | ChainMeta | prev | data`.
- **Key = byte position**: `(seq << 40) | offset`. It grows (the sweeper's
  horizon), and a blob is read with one `pread`, no index.
- `put` / `put_after`: under `flock`, check the tail (the owner caches it;
  a `fstat` of the active segment tells whether anyone else appended), append,
  fsync. A segment rolls over at a size limit.
- A torn record at the end of the last segment was never acknowledged; open
  truncates it (as `LocalLogStore::claim` does now).
- Delete: a segment whose records are all dead is removed; inside a live
  segment the space is freed by punching a hole (`FALLOC_FL_PUNCH_HOLE` on
  Linux, `F_PUNCHHOLE` on APFS), keys never move. A log store mostly drops
  whole old segments; a data store punches holes.

## Performance: no backend loses

Conditions (each is a property of the traits, not a special case):

1. `chain_from` reads pages; replay never does one call per blob.
2. `tail` is the store's own; Telegram and Discord find it top-down in a page
   or two, as their bespoke logs do.
3. `ChainMeta` and `prev` are readable without a download.
4. Disk keys are byte positions.

Per operation, disk: append and rebase as today; claim and save_vote cheaper
(a record instead of tmp + fsync + rename). Telegram, Discord, OpenDAL: the
same calls as their bespoke designs.

Before a bespoke store is deleted, the same checks run on both:
the `LogStore` conformance suite, a micro-benchmark (append p50 / p99 with
fsync, claim, replay of 1M entries, rebase), and `sweep_stress` with metadata
on disk.

## Layering

`pigeonhole-meta` depends on crates.io only. The storage traits move to a new
crate `core/pigeonhole-storage-api` (crates.io only): `BlobBackend`,
`Sweepable`, `Chain`, `OrderedKey`, `BlobId`, `BackendLimits` and the types
they need. `pigeonhole-blob`, `pigeonhole-meta` and every storage crate depend
on it; `scripts/check_deps.py` learns the new layer. A log or snapshot store
is configured as any `[[store]]`; one store serves one purpose (blobs, a log,
or snapshots), as today.

## Implementation notes (steps 1–4)

Where the code differs from the design above:

- **One chain per store.** `tail()` takes no start: a store holding a log (or
  snapshots) holds nothing else that chains. The trait is
  `put_after(prev, label, data)`, `tail()`, `successor(id)` and
  `prune(below)` ("delete every blob below `below`, members and losers
  alike"), plus a provided `put_after_tail(label, make)`: the blob is built
  from the tail (id and data) and put after it. A claim uses it. The
  default loops (read the tail, put after it, again on `Lost`, so `make`
  must be pure); a store that can decide it atomically overrides it (disk:
  under one `flock`; without it a claimant lost 600 times in a row to a
  writer appending every few ms); `ChainMeta` is a free-form `label` the log parses
  (`log-claim-<gen>`, `log-batch-<gen>`, `snapshot-<log>-<at>.snap`).
- **No `chain_from`.** Replay walks back along the `prev` links in the blobs
  (as the channel log did); the Telegram store reads the whole page below a
  location it misses, so a walk down reads each page once.
- **Every blob carries the registers** (log id, base, the older bases kept
  ([log-retention.md](log-retention.md)), vote, head term, head position), a claim too (an empty batch at head + 1): `discover` reads the
  tail only. Generations only grow along the chain, so the tail's generation
  is the newest claim and a loser's `Fenced { current }` is the winner's.
- **`confirm`** = `successor(our tip)` is none.
- **Telegram**: a member is a document named `<label>~<prev>.bin` (`root`
  for the first). `put_after` posts with a fresh `random_id` per call (two
  writers putting the same name must get two messages), wins iff nothing in
  `(prev, b)` names `prev` (no read when `b == prev + 1`). `tail` and
  `successor` post a probe to learn the top (probes stay: a hole in a checked
  range costs re-reads); `tail` walks down to the newest message that won its
  link. `prune` deletes our members and probes below a point, walking down
  until a page holds none of ours. Empty ids in a checked range are re-read,
  then taken as deleted (`ChainOptions`).
- **Telegram text messages**: blobs up to ~7.5 KiB (log batches, claims,
  probes) are text messages whose bytes come with `getMessages`
  (telegram-mtproto.md § Text messages); a replay of small batches
  downloads nothing.
- **Memory** decides links under a mutex; `MemoryLogStore` and
  `MemorySnapshotStore` are gone.
- **Disk** (step 4): records `magic | flags | label_len | data_len | prev |
  crc | label | data` in `seg-<seq>` files; key = `(seq << 40) | offset`.
  Every write holds the directory's `flock` and first indexes what another
  process appended (an `fstat` of the active segment; the directory is
  listed only when it grew or is full). The chain (`next`, `tail`) is an
  in-memory index rebuilt by a scan on open; the last record's crc is
  checked and a torn one truncated. A delete sets `DEAD` in the header and
  punches a hole over the data (`F_PUNCHHOLE` / `FALLOC_FL_PUNCH_HOLE`); a
  segment with no live record is removed. A `local_disk` `[meta]` store
  keeps the log in `<path>/log` and the snapshots in `<path>/snapshots`.
  `LocalLogStore` / `LocalSnapshotStore` are gone.
- **Benchmark** (`log::chain::tests::disk::bench_disk_logs`, release, macOS,
  before the old log was deleted): append p50 ~4.1 ms for both (one
  `F_FULLFSYNC`); 1M entries in batches of 1000 in ~4.5 s for both; open +
  claim 9–20 ms (old: 210–245 ms); replay of 1M entries 140 ms (290 ms);
  rebase 120 ms (220 ms). `sweep_stress` 38–51 s (was ~48 s).
- Tests: the `LogStore` and `SnapshotStore` conformance suites run on
  `ChainLogStore` / `BlobSnapshotStore` over memory (in `pigeonhole-meta`) and
  over the Telegram store on a fake channel
  (`storage-telegram/tests/chain_log.rs`, with the channel log's scenarios:
  no reads per append, a gap, a deposed writer, an unconfirmed batch, racing
  claims, a claim visible late, a deleted id, a changed blob, pruning).

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. ✅ **Move the traits** into `pigeonhole-storage-api`; no behaviour change.
2. ✅ **`Chain` for memory**, `ChainLogStore`, `BlobSnapshotStore`; the
   `LogStore` conformance suite on `ChainLogStore<MemoryBlobStore>`; the
   tablet's tests on it. Delete `MemoryLogStore` / `MemorySnapshotStore`.
3. ✅ **Telegram `impl Chain`** from `ChannelLogStore`'s logic; the fake channel
   becomes a fake `TelegramClient` under `TelegramBlobStore`; live run on the
   test channels. Delete `ChannelApi`, `ChannelLogStore`,
   `ChannelSnapshotStore`.
4. ✅ **Segmented `DiskBlobStore`** with `impl Chain`; the benchmark against
   `LocalLogStore`; then delete `LocalLogStore` / `LocalSnapshotStore`.
   Repeated `sweep_stress`.
5. **Discord `impl Chain`** (discord-log.md).
6. **OpenDAL `impl Chain`** with the OpenDAL blob backend.

## Open questions

- `confirm` on Telegram writes (a probe or an empty batch) to learn the top:
  keep it, or let `successor(head)` read a page past the head and accept
  "empty" there as "nothing yet" (empties above the top and deleted ids look
  alike)?
- Segment size for data stores (chunks up to 256 MiB) vs log stores (small
  records): one setting per store?
- Hole punching on filesystems without it: fall back to rewriting a segment
  whose live share drops below a threshold (keys would move — not allowed), or
  just keep the space until the whole segment dies?
