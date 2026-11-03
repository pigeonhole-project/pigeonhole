# Metadata as a tablet

Status (2026-11-02): implemented.
- Typed commands and in-memory state machines.
- A tablet over a `ChainLogStore` on a `local_disk`, `memory`, `telegram` or
  `discord` store ([chain-log.md](chain-log.md)).
- Snapshots in a `SnapshotStore` of their own; lossy restore; log retention
  ([log-retention.md](log-retention.md)).
- The tablet over openraft, as a cluster of one or of several nodes
  ([cluster.md](cluster.md)).
- Not built: an `s3` LogStore (with OpenDAL, roadmap step 12).

No backward compatibility: configs, on-disk and in-chat formats are
replaced, and existing data is not migrated. Formats are versioned, so a
binary refuses old data instead of misreading it; a converter can be written
when needed.

The model follows YDB tablets: a state machine over a log kept in a storage
that can **fence** old writers. Each node of a cluster has a log of its own
and openraft keeps the nodes' logs equal. The fence keeps two processes
from acting as one node.

## Why

The old design made metadata durable from inside the chunk store: a periodic
checkpoint of the chunk tables, a superblock pinned in every read-write chat
and a journal of index roots; the S3 and CAS indexes were snapshotted
separately. Commits between checkpoints were lost with the disk, the layers
could disagree after a restore, and publishing the superblock was
check-then-write, so two writers could both win. All of it is gone (step 3).

## Goals

1. Every metadata change is a typed command; state = commands applied in order.
2. The command log lives in a pluggable **`LogStore`** that fences old
   generations; snapshots in a pluggable **`SnapshotStore`**, configured
   separately. State lives in memory and is rebuilt from snapshot + log on
   every boot; there is no local database.
3. A tablet boots anywhere its log is reachable: claim a generation,
   discover, replay, write a zero entry.
4. Side effects outside the state machine (deleting blobs) never run ahead of
   the log and never come from a deposed writer.
5. Implementations are chosen in config: a local directory, Telegram or
   Discord for either store (later S3 with conditional writes for the log).

Non-goals for now: several tablets / sharding metadata; follower reads.

## Model

```
 ┌──────────────────────────── tablet (one per node) ──────────────────────────────┐
 │ propose::<S>(cmd) ─► batch ─► LogStore.append(gen, batch) ─► S::apply in memory │
 │ reads: MetaStore::read::<S> (short read lock on the namespace's state)          │
 │ every N entries: snapshot ─► SnapshotStore.put ─► LogStore.rebase(at, ref)      │
 │ side effects (sweeper, repair) only on the leader, while it holds its generation│
 └──────────────────────────────────────────────────────────────────────────────────┘
          │                                   │                    ▲
          ▼                                   ▼                    │ base snapshot + tail on boot
 LogStore: claim / append / discover /   SnapshotStore: put / get / newest / remove
           read / rebase
```

### Namespaces

Each layer (`chunk`, `s3`, `cas`) is a `StateMachine`:

```rust
trait StateMachine: Default + Serialize + DeserializeOwned {
    const NAME: &'static str;
    const VERSION: u32;          // encoding of Command and of the state
    type Command: Serialize + DeserializeOwned;   // what goes into the log
    type Reply;                  // handed back in-process, never serialized
    type Reject;                 // deterministic failure, state untouched
    fn apply(&mut self, cmd: Self::Command) -> Result<Result<Self::Reply, Self::Reject>>;
    fn check(&self, cmd: &Self::Command) -> Option<Self::Reject> { None }
    fn rebuild(&mut self) {}     // recompute #[serde(skip)] indexes after loading
}
```

**`apply` never fails on what a proposal carries.** Its `Err` stops the
tablet, and not on one node: every node applies the entry and fails alike,
and since the entry stays in the log, every boot replays it into the same
`Err`. One bad proposal would be a permanent outage. So whatever a command
holds that the state does not allow is a `Reject` (`Ok(Err(_))`), with the
state untouched: a chunk id committed already, an unknown instance, a key
another chunk owns, an upload id taken. `Err` is left for what no proposal
can cause: a command of another `VERSION` or one that does not decode, a
namespace the binary does not serve (a binary that cannot read its log must
stop, not guess).

A proposal's commands land in one entry and are applied in order; a
rejection of one undoes nothing else. Two exceptions apply none of them:

- **Check.** In a proposal of several commands, each first goes through its
  namespace's `check`: a rejection the state alone gives it, whatever else
  the proposal holds. One rejected there, and nothing of the proposal is
  applied (the others' outcome is `NotApplied::Skipped`). A chunk commit is
  checked so: a rejected chunk takes the object that points at it with it.
- **Fence.** A command prepared from the leader's state for its term
  (`Prepared::fenced(term)`, a new chunk's id) makes its proposal apply only
  from an entry of that term: written after the lead changed, even back to
  the same node, nothing of it is applied (`NotApplied::Fenced`, a 503 for
  S3). The entry's term is part of the log, so every replay agrees.

Commands, states, log batches and snapshots are encoded with postcard. That
encoding follows the Rust types, so every format carries a version: each batch
its `LOG_FORMAT`, each entry its namespace's `VERSION`, each snapshot its
`SNAPSHOT_FORMAT` and per-namespace `VERSION`. A mismatch is an explicit error
at boot; golden tests pin each command encoding so a shape change has to bump
`VERSION`. State uses ordered containers only, so equal states encode to equal
bytes.

### Generations

Every tablet incarnation runs under a **generation** `g`, strictly greater than
every generation that ran before. All log writes carry `g`. After a newer
generation has **claimed** the log, every `append` of an older one fails with
`Fenced`; the older incarnation stops and never acknowledges again.

### LogStore contract

```rust
trait LogStore {
    /// Become generation `gen` (> any earlier claim, else `Fenced { current }`).
    /// Fences every older generation and returns the log head they left
    /// behind: every entry whose append was acknowledged to an older
    /// generation is ≤ that head.
    async fn claim(&self, gen: u64) -> Result<Head, LogError>;

    /// Durably append a batch for `gen`. `Ok` = committed (position of the
    /// batch's last entry); `Fenced` = a newer generation exists, nothing
    /// about this batch may be acknowledged.
    async fn append(&self, gen: u64, batch: &[Entry]) -> Result<Position, LogError>;

    /// Drop the entries at `from` and after (Raft: a divergent tail).
    async fn truncate(&self, gen: u64, from: Position) -> Result<(), LogError>;

    /// Durably store the Raft vote (opaque).
    async fn save_vote(&self, gen: u64, vote: &[u8]) -> Result<(), LogError>;

    /// Base + log head + newest generation (bootstrap without local state).
    async fn discover(&self) -> Result<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay); `from` may be
    /// behind the base, down to the oldest base kept.
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Start the log from `base` = { at, snapshot: SnapshotRef } (a snapshot
    /// already stored), keeping the `keep` newest bases and the log from the
    /// oldest; collect the rest in the background. Returns the bases that
    /// fell out. Fenced like append (log-retention.md).
    async fn rebase(&self, gen: u64, base: Base, keep: usize) -> Result<Vec<Base>>;
}
```

`Entry` = `{ term, payload }` (the tablet's payload is a list of proposals,
each a `Group { fence, commands }` of `Command { namespace, version, command
}`), a `Batch` = `{ format, gen, first, entries }`, both
postcard. A batch replaces whatever the log held from `first` on, so a
truncate is a batch with no entries and `first` ≤ the head; every store keeps
the records in write order and the log is the result of applying them. The
log also keeps the Raft registers: the head's term and the vote (`discover`). Every log has a `LogId` fixed by its first claim.
One error type for every call:
`LogError::Fenced { current }` (a claim at or below `current` also gets it, so
the tablet retries with `current + 1`), `Lost` (entries the caller needs are
gone or corrupt — collected, a hole, a file ahead of the log) and `Other` (I/O,
outcome unknown). Positions number entries 1, 2, …; an empty batch (the zero
entry) takes no position. A batch is one log write: commits are grouped
(group commit) because some stores (Telegram) allow a write every few seconds.

### SnapshotStore contract

```rust
trait SnapshotStore {
    /// Durably store { log_id, at, data }; the ref goes into the log's base.
    async fn put(&self, snapshot: &StoredSnapshot) -> Result<SnapshotRef>;
    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot>;
    /// The most recently stored snapshot, of any log: lossy restore only.
    async fn newest(&self) -> Result<Option<StoredSnapshot>>;
    /// Drop one nothing starts from any more (best effort).
    async fn remove(&self, r: &SnapshotRef) -> Result<()>;
}
```

The log names the exact snapshot it starts from, so a boot never guesses: a
deposed writer's late `put` is just an unreferenced copy. A store never drops
snapshots on its own; the tablet removes the previous base once the log is
rebased on a newer one. One implementation, `BlobSnapshotStore`
over any store that implements `Chain` (local disk, memory, telegram): snapshots are a
chain of blobs, the ref is the blob id and its crc, `newest` is the tail
(chain-log.md). The log and the snapshots cannot share a store: a store holds
one chain.

### Tablet lifecycle

1. **Boot** (`claim`): `gen = 1 + discover().gen` (`Fenced { current }` moves
   past a racing claim); `claim(gen)` → head.
2. **Recover**: `discover` → `get` the base snapshot into empty states →
   `read` and apply entries after it up to head. A never-claimed log next to a
   snapshot store that holds snapshots refuses to boot (the log was lost: an
   empty state would make the sweeper take every blob for an orphan).
3. **Zero entry**: append an empty batch for `gen` — proves the claim took effect
   and marks the start of this generation (as YDB's `WriteZeroEntry`).
4. **Active**: `propose` queues commands; the batcher appends what is queued
   (group commit), applies it in order, takes a snapshot if one is due, then
   answers the callers. `Fenced` → stop the tablet (proposals fail, side effects
   stop, `serve` exits). A command that cannot be applied stops it too; the next
   boot rebuilds from the log. Several commands of different namespaces can be
   proposed together (`Prepared<S>` built by the owning layer, collected into a
   `Proposal`, `propose_all`): they go into one entry, never split, each applied
   by its own namespace with its own outcome. An S3 or CAS write is one entry:
   its chunks' `CommitChunk`s plus the index command
   ([one-entry-per-operation.md](one-entry-per-operation.md)).
5. **Snapshot**: every `[meta] snapshot_every_entries` entries, and every
   `snapshot_every` when anything changed (an idle tablet wakes up for it), between
   batches: one `TabletSnapshot` of all states, stored with `put` in the
   background (one at a time); when it is stored, the batcher `rebase`s the log
   on it, keeping `[meta] keep_snapshots` bases (and the log from the oldest),
   and removes the snapshots of the bases that fell out
   ([log-retention.md](log-retention.md)). A boot that cannot load the newest
   base's snapshot starts from an older kept one.
   `MetaStore::base_position` is where the log starts now. A failed put or
   rebase keeps the old base; the next snapshot tries again.

Responses are released only after `append` returned `Ok`, so a client never sees
a result the next generation might not have.

**Restore** (`pigeonhole restore --lossy`) starts an empty log from the newest
stored snapshot: the states as of it, positions from 0 (stored again under the
new log id, then `rebase(0)`). It is for a local log lost while its snapshots
survive (another disk, a chat). Lossy: changes after the snapshot are gone and
chunks committed later become orphans (the sweeper deletes them). When
snapshots outlive the log, reclaim deletes parts only after the base snapshot
shows the chunk reclaiming (sweep-invariants.md), so the newest snapshot never
references deleted parts. Only an off-host LogStore makes disk loss lossless.

## LogStore implementations

Every kind is `ChainLogStore` over a store that implements `Chain`
(chain-log.md); what differs is how the store decides the first successor.

### `local_disk` — segment files

The disk blob store: records in append-only segment files, one fsync per
append, the first successor decided under the directory's `flock` (a claim
reads the tail and appends under the same lock). Fences only processes on
the same host; durability = the disk. Default.

### `telegram` — a channel over MTProto, fenced by message order

The generic `ChainLogStore` over `impl Chain for TelegramBlobStore`
([chain-log.md](chain-log.md)). A channel numbers every message, atomically
and in increasing order, and a bot may read any message by id. That order is
the fence: of two puts after `p`, the first by id wins, and a put landing at
`p + 1` needs no read. A deposed writer's late message lands above the newer
claim and loses for every reader alike. About 20 appends a minute per
channel; the bot needs post and delete rights. History, spike and live
findings: [telegram-mtproto.md](telegram-mtproto.md).

### `discord` — a channel, fenced by threads

The generic `ChainLogStore` over `impl Chain for DiscordBlobStore`
([discord-log.md](discord-log.md)): a message can start one thread, ever, so a
put after `p` wins iff its thread on `p` was created first. Threads are
scarce (50 per 300 s), so with `[meta] lease` (the default) only lease
records take one and batches are plain posts kept if answered inside the
lease ([discord-lease.md](discord-lease.md)): about one append a second.
Snapshots: `BlobSnapshotStore` in another channel.

### `s3` — conditional writes (later, roadmap step 12)

Entries as objects `log/<position>` written with `If-None-Match: *`; a claim is
an object at the next position, so two generations cannot both write it. Fast and
cheap compared to Telegram; needs an S3-compatible store with conditional writes.

## Contracts

### Side effects

- Only the leader runs the sweeper and repair (`MetaStore::is_leader`), and
  only while it holds its generation; on `Fenced` they stop before the next
  backend call. Orphan deletes are preceded by
  `MetaStore::confirm_active` (an empty batch through the log), so a deposed
  incarnation that has not written since the claim still finds out.
- Reclaim: propose `BeginReclaim`, wait for it to be **committed** (appended),
  then delete parts, then propose `DropChunk`. A restore or failover never brings
  back a reference to deleted parts.
- Orphan sweep: uploads are pinned before they start and unpinned after their
  commit is applied; orphans are swept up to the pins' horizon. Parts become
  known only through committed `CommitChunk` / `AddReplica`, and a fenced
  incarnation cannot commit, so its in-flight parts are garbage. Reads pin the
  chunks they read; reclaim waits for them (sweep-invariants.md).
- After recovering from a snapshot older than the log head nothing special is
  needed: replay brings the state to the head before side effects start.
- When snapshots outlive the log, reclaim waits for a base at or after its
  `BeginReclaim` (`MetaStore::restorable_position`).

### Garbage of the log itself

The tablet decides what is garbage in its own log (entries ≤ the oldest
base kept), like
YDB's `CollectGarbage` barrier on `generation:step`; ids of log objects are
chosen by the writer, so collection needs no listing. Old Telegram log
messages (ours only) below the oldest batch a reader needs are deleted in
the background after `rebase`, on spare budget only; the snapshots of bases
no longer kept by the tablet.

## Config

```toml
[meta]

log = "meta"                        # a [[store]] id: local_disk | memory | telegram | discord
snapshot = "meta"                   # default: the log's store if local_disk / memory
snapshot_every_entries = 10000      # "off" = never
snapshot_every = "10m"              # also by time if anything changed (dynamic)
keep_snapshots = 2                  # the newest kept, with the log from the oldest
lease = "45s"                       # a discord log's lease; "off" = a thread per batch

[[store]]
id = "meta"
kind = "local_disk"
path = "pigeonhole-meta"

# [[store]]                         # ship the log off-host: a channel of its own
# id = "tg-log"                     # (fenced by message order); snapshots then go
# kind = "telegram"                 # to another telegram store
# chat_id = -1009876543210
```

| `[meta] log` | `[meta] snapshot` | losing this host's disk |
|---|---|---|
| local_disk | the same store | everything |
| memory | memory | everything (tests) |
| local_disk | another local_disk / telegram | `restore --lossy`: the tail after the newest snapshot |
| telegram / discord | another channel | nothing |
| telegram / discord | local_disk / memory | refused by the config |

## Raft between nodes

`MetaStore` runs over openraft 0.9 (`raft.rs`). A single process is a
cluster of one; several nodes are one cluster: [cluster.md](cluster.md)
(transports, configuration, the leader doing all the work). What the tablet
gives openraft:

- **Log storage**: each node's own `LogStore`. Entries carry their Raft
  term; `truncate` drops a divergent tail (a record linking to the entry
  before it); `purge` behind a snapshot is `rebase`; the vote and the head
  term are registers carried in every record of the chain. A conformance
  suite runs against every store kind.
- **Two processes as one node** are fenced as before: every durable change
  (a vote, an acknowledged entry) is a write under the process's
  generation, so a process that lost its log can neither vote nor
  acknowledge.
- **State machine**: the namespaces (`chunk`, `s3`, `cas`, `config`), the
  last applied `LogId` and the membership. Concurrent proposals are batched
  into one Raft entry (openraft 0.9 writes one entry per `client_write`).
  Positions are Raft log indices.
- **Snapshots**: a `TabletSnapshot` with the Raft meta, in the node's own
  `SnapshotStore`. A follower behind the leader's purged log gets the
  leader's snapshot over the network and stores it in its own.

## Open questions

- Telegram: the log trusts that a message is readable by id once its send
  returned (checked live, also from another session); an id that stays empty
  is taken as deleted. Someone deleting a claim by hand defeats the fence.
- Snapshots are taken between batches under read locks: O(state). Fine at our
  scale; persistent maps (imbl) would make them O(1) if it ever matters.
- Batch size / delay defaults per LogStore kind (`local_disk`: no extra delay).
- Any node may lead, a Telegram one included: a leader need not be in the
  majority that commits an entry (Raft §10.2.1), so its slow store delays
  commits only when no majority forms without it.
- Telegram chain truncate (a fork) leaves messages behind until the next
  `rebase` collects them.
