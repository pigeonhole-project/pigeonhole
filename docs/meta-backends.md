# Metadata as a tablet

Status: **steps 1–6 implemented** — typed commands, in-memory state machines,
a tablet over the `local` or `telegram` LogStore, snapshots in a
`SnapshotStore` of their own, lossy restore. The arbiter and the `s3` LogStore
are a proposal. No backward compatibility: configs, on-disk and in-chat formats
are replaced, existing data is not migrated (formats are versioned, so a
binary refuses old data instead of misreading it; a converter can be written
when needed).

The model follows YDB tablets: a replicated state machine over a **shared log**
kept in a storage that can **fence** old writers; an external **arbiter** only
restarts the tablet somewhere else. Correctness comes from the log store, not
from the arbiter.

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
3. A tablet can move to another node: boot = claim a generation, discover,
   replay, write a zero entry.
4. Side effects outside the state machine (deleting blobs) never run ahead of
   the log and never come from a deposed writer.
5. Implementations are chosen in config: a local directory or Telegram for
   either store, later S3 with conditional writes for the log.

Non-goals for now: several tablets / sharding metadata; follower reads.

## Model

```
                 arbiter (bootstrapper)                 liveness only
                    │ starts / restarts the tablet
                    ▼
 ┌──────────────────────────── tablet (one per cluster) ───────────────────────────┐
 │ propose::<S>(cmd) ─► batch ─► LogStore.append(gen, batch) ─► S::apply in memory │
 │ reads: MetaStore::read::<S> (short read lock on the namespace's state)          │
 │ every N entries: snapshot ─► SnapshotStore.put ─► LogStore.rebase(at, ref)      │
 │ side effects (sweeper, repair) only while the tablet holds its generation       │
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
    fn rebuild(&mut self) {}     // recompute #[serde(skip)] indexes after loading
}
```

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

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Start the log from `base` = { at, snapshot: SnapshotRef } (a snapshot
    /// already stored) and drop entries ≤ `at` (collect). Fenced like append.
    async fn rebase(&self, gen: u64, base: Base) -> Result<()>;
}
```

`Entry` = `{ term, payload }` (the tablet's payload is a `Command { namespace,
version, command }`), a `Batch` = `{ format, gen, first, entries }`, both
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
rebased on a newer one. Implementations: `local` (`snapshot-<log id>-<at>.snap`
files, crc-checked; the directory may be the log's) and `telegram`
(`ChannelSnapshotStore`: one document per snapshot in a channel of its own;
the ref is the message id and the document's crc; `newest` posts a probe to
learn the top and walks down). The log and the snapshots cannot share a
channel: snapshot ids would land in the log's checked ranges.

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
   `snapshot_every_secs` when anything changed (an idle tablet wakes up for it), between
   batches: one `TabletSnapshot` of all states, stored with `put` in the
   background (one at a time); when it is stored, the batcher `rebase`s the log
   on it (dropping the log behind it) and removes the previous base snapshot.
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

### Arbiter

Liveness only, like YDB's Bootstrapper / Hive: watch the tablet's node, start it
elsewhere when it is gone. It may be wrong (two incarnations at once) — the
`LogStore` fence makes the older one fail. First implementation: none (single
node; restart = process restart). Later: a small watcher over the same LogStore
(e.g. the claim message's age).

## LogStore implementations

### `local` — file on disk

Segmented append-only files + a `claimed_gen` file updated with fsync before
use, and a `base` file written by `rebase`; `append` checks the generation
under a lock. Fences only processes on the
same host; durability = the disk. Default, and the reference implementation the
others are tested against.

### `telegram` — a channel over MTProto, fenced by message order

Implemented as `ChannelLogStore` (`pigeonhole-meta/src/log/channel.rs`,
protocol over a `ChannelApi` trait, tested on a fake channel) with an MTProto
adapter in the binary. Design, spike and live findings:
[telegram-mtproto.md](telegram-mtproto.md). In short:

- A channel numbers every message, atomically and in increasing order, and a
  bot may read any message by id (`channels.getMessages`). That order is the
  fence: who won is decided by the **position** of a message, not by time.
- Two kinds of documents: `log-<g>-claim.bin` and `log-<g>-batch.bin` (a
  batch with the log's registers: base, vote, head term; `prev` links the
  chain, `since` is the last message its writer had checked).
- A claim is valid iff no valid claim of a generation ≥ its own lies below
  it; a batch of `g` iff no claim of a generation > `g` lies in
  `(since, batch)`. The head is the highest valid batch.
- `append`: send; if the id is `checked + 1`, committed without a read (the
  common case in a dedicated channel), else read the gap for a higher claim.
- `claim`: send the claim, walk down to the highest valid batch, check the
  claims above it.
- A deposed writer's batch that lands late (a dropped grammers request still
  goes out) is above the newer claim and invalid for every reader alike: no
  straggler window, no pins, no description.
- A batch sent but never confirmed is valid if no higher claim precedes it,
  and becomes the head: entries never acknowledged may survive, as Raft allows.

Costs: an append is one send (~20 a minute per channel); `confirm` posts an
empty batch; `discover` posts a probe (kept). Recovery follows `prev` down
to the base, a page read per 100 ids. The bot needs post and delete rights.

### `s3` — conditional writes (later)

Entries as objects `log/<position>` written with `If-None-Match: *`; a claim is
an object at the next position, so two generations cannot both write it. Fast and
cheap compared to Telegram; needs an S3-compatible store with conditional writes.

## Contracts

### Side effects

- Only the active tablet runs the sweeper and repair; on `Fenced` they stop
  before the next backend call. Orphan deletes are preceded by
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

The tablet decides what is garbage in its own log (entries ≤ the base), like
YDB's `CollectGarbage` barrier on `generation:step`; ids of log objects are
chosen by the writer, so collection needs no listing. Old Telegram log
messages (ours only) below the oldest batch a reader needs are deleted by
`rebase`, the previous base snapshot by the tablet.

## Config

```toml
[meta]

log = "meta"                        # a [[store]] id: local_disk | memory | telegram
snapshot = "meta"                   # default: the log's store if local_disk / memory
snapshot_every_entries = 10000      # 0 = never
snapshot_every_secs = 600           # also by time if anything changed

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
| telegram | telegram (another channel) | nothing |
| telegram | local_disk / memory | refused by the config |

## Raft between nodes (proposal, step 8)

Not implemented yet; this section is the design to agree on before code.

### Why

The tablet above has one writer and a shared log in an external store that
fences. Its availability is that store's: a Telegram log is down when
Telegram is, a local log when the host is. Making the shared log itself
survive (a quorum of chats, BookKeeper style) means quorum fencing, recovery
and repair of divergent copies over chats. Raft does exactly that between our
own processes, so the per-node storage can stay simple.

### Model

```
              Raft (terms, votes, replication of commands)
   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │ node a           │◄─►│ node b           │◄─►│ node c           │
   │ log: local_disk  │   │ log: telegram    │   │ log: local_disk  │
   │ snap: local_disk │   │ snap: telegram   │   │ snap: memory?    │
   └──────────────────┘   └──────────────────┘   └──────────────────┘
          blobs: shared [[store]] / [[replicas]] / [[shards]] (unchanged)
```

- A **node** is a process of this binary with a node id, an address, and a
  log store and a snapshot store of its own (`[[node]] log` / `snapshot`, any
  store kind that holds metadata). A node is defined by its stores, not by its
  machine: a node whose log is a Telegram chat can be started anywhere.
- Nodes replicate commands with Raft ([openraft](https://docs.rs/openraft)
  0.9). Terms and quorums fence; a node's store has exactly one writer, the
  node, so it needs local durability only.
- The **leader is the tablet**: it serves writes (`MetaStore::propose` →
  `client_write`), runs the sweeper and repair. Followers forward.
- Single node = a cluster of one; the old tablet code goes away, one code path.

### Node storage (`RaftLogStorage`)

What openraft needs from a node's log, and how each store kind provides it:

| need | local_disk | memory | telegram (channel) |
|---|---|---|---|
| `append(entries)`, durable at callback | segment record + fsync | push | one send per batch (+ a gap read if something lies between) |
| `truncate(from)` (drop a divergent tail) | cut the segment | pop | a new message linking to the entry before `from`: the chain forks, the old tail is unreachable garbage |
| `purge(upto)` (behind a snapshot) | `rebase` (exists) | drop | `rebase` (exists) |
| `save_vote` / `read_vote`, durable | a `vote` file (tmp + rename) | field | carried in every message; a vote change alone sends an empty message |
| one process per node | `flock` + `claimed_gen` (exists) | — | claims ordered by message id (exists) |

Entries carry their Raft `LogId` (term, index) instead of our generation;
`claim` stays, now meaning "this process is node X" so two processes started
as the same node cannot both vote. A conformance suite (append / truncate /
purge / vote / reopen) runs against every kind, as the LogStore suite does now.

Appends are batched: openraft hands `append` several entries at once, and
while one append is in flight new commands queue for the next. A store's
speed limits appends, not commands: a Telegram channel takes about 20
messages a minute, one message carries any number of entries (up to 20 MB).
Costs per append: local_disk ~ms (fsync), telegram ~1 s (one send).

### Two processes as one node

Raft is safe only if every node keeps its promises (a vote, an
acknowledged entry). Two processes started as the same node would break that,
so the node's store fences them, the way the tablet's log fences writers now:

- **Every durable change is fenced.** Granting a vote (`save_vote`),
  acknowledging entries (`append` / `truncate`) and campaigning (a vote for
  itself) first write to the store; the write succeeds only while the process
  holds the store's generation (`flock` + `claimed_gen`, or in a channel no
  newer claim below its message: the order fence of the `telegram` log).
  A process that lost the store can neither vote nor acknowledge.
- **Answers without a write are leased.** A heartbeat answered from memory
  ("I match up to i") writes nothing. A process confirms its fence at least
  every half lease (in a channel: an empty batch) and stops once it is not
  its own; a claiming process waits a lease before taking part. Their
  lifetimes do not overlap if clocks run at about the same rate (the channel
  order gives no time bound of its own).
- **The fence names the node**: a process started as node `c` on node `b`'s
  channel refuses to claim it; one config cannot give two nodes the same store.
- Peers talk to `[[node]] addr`, so a second process elsewhere gets no RPCs;
  it could only campaign, which needs `save_vote`, which is fenced.
- local_disk: `flock` (not on NFS); memory: inside the process.

### State machine and snapshots (`RaftStateMachine`)

- State = the namespaces as now (`chunk`, `s3`, `cas`) + last applied
  `LogId` + membership. `apply` = `StateMachine::apply` per entry.
- Snapshot = `TabletSnapshot` + Raft meta (last log id, membership), stored in
  the node's `SnapshotStore` (`put` → ref). A lagging follower gets the
  leader's snapshot over the network and `put`s it into its own store.
- Positions: our `Position` becomes the Raft log index. Blank and membership
  entries take indices too; nothing relies on positions being dense (the
  reclaim gate only needs them to grow).
- Entry payload: one command per Raft entry (`Entry { namespace, version,
  command }`); openraft batches appends itself, our batcher goes away.

### Network

A tonic gRPC service on `[[node]] addr` (tonic is already a dependency for
REAPI): `AppendEntries`, `Vote`, `InstallSnapshot` (streamed), and `Forward`
(a follower's `propose` to the leader). Payloads are postcard bytes inside a
`bytes` field, so the proto never mirrors our types. No auth in the first
version: a shared secret header later.

### Reads, writes, side effects

- **Writes** (S3 PUT, DELETE, …): the leader serves them; a follower's S3
  front forwards the whole request at first (simplest: upload pins and the
  sweeper horizon are per process, so uploads must happen on the leader).
- **Reads**: first version on the leader too; later followers serve reads with
  read-index (`ensure_linearizable` on the leader) or bounded staleness.
- **Sweeper / repair** run only while leader, and start only after
  `ensure_linearizable` on the new term: by then the leader has applied every
  entry an older leader got into the log, including commits of uploads it did
  not know about, so its horizon (seeded from committed parts) is safe.
  `confirm_active` before deletes becomes `ensure_linearizable`.
- Losing leadership pauses side effects instead of stopping the process.

### Config

```toml
[[node]]
id = "a"
addr = "10.0.0.1:9100"
log = "a-disk"          # [[store]] ids, one purpose each as now
snapshot = "a-disk"

[[node]]
id = "b"
addr = "10.0.0.2:9100"
log = "b-chat"
snapshot = "b-snap-chat"

[meta]
node = "a"              # or PIGEONHOLE_NODE / --node: which node this process is
```

`pigeonhole init-cluster` initializes the membership from `[[node]]` once;
later membership changes go through `change_membership`.

### Restore

If every node lost its log, `restore --lossy` seeds a one-node cluster from
the newest snapshot found in the nodes' snapshot stores; the others join as
learners. The reclaim gate applies when some node's snapshots outlive its log.

### Steps

1. Node storage contract over our stores (Raft `LogId`, truncate, vote) with
   a conformance suite for local_disk, memory, telegram (fake channel).
2. `MetaStore` over openraft in one process: a cluster of one, every existing
   test green, the old tablet batcher removed.
3. Network + in-process multi-node tests (memory stores, a fake transport):
   leader election, failover, a node on a fake Telegram channel, partitions;
   leader-only side effects and forwarding.
4. `[[node]]` config, `init-cluster`, live runs (a node whose log is a real
   Telegram channel).
5. Later: reads on followers, auth between nodes, membership changes in ops.

### Open questions

- openraft 0.9 (stable) is the choice; 0.10 changes the storage traits.
- Any node may lead, a Telegram one included: a leader need not be in the
  majority that commits an entry (Raft §10.2.1), so its slow store delays
  commits only when no majority forms without it. Check openraft's quorum
  accounting of the leader's own append in a test; keep the recent log tail in
  memory so catching followers up does not download old messages. Measure
  commit and append latency per node.
- Telegram chain truncate (a fork) leaves messages behind until the next
  `rebase` collects them.

## Plan

Each step is a separate commit with tests green.

1. ~~Commands, in-process.~~ Done.
2. ~~Tablet + `LogStore` (local).~~ Done: claim → replay → zero entry, group
   commit, fencing, `LogId`.
3. ~~Snapshots and removal of the old durability.~~ Done: states in memory
   (SQLite and proto gone from metadata), typed `StateMachine` with versioned
   postcard encodings, tablet snapshots + collect, lossy restore;
   `durability.rs`, superblock pins, index snapshots and roots deleted.
4. ~~Sweeper contract.~~ Done: `BeginReclaim` is committed before deletes;
   `confirm_active` before each orphan delete batch; loops end on stop.
5. ~~In-flight pin registry.~~ Done: upload pins + horizon, own `BlobId`s,
   read pins; watermarks, sweep grace and `InflightParts` removed.
6. ~~`telegram` LogStore.~~ Done: first over the Bot API (pins, a description
   fence, a straggler window), then replaced by `ChannelLogStore` over MTProto
   (fenced by message order); the LogStore conformance suite and fault
   injection on a fake channel; snapshots in a `SnapshotStore` of their own.
7. **Arbiter** and a second node; `s3` LogStore.

## Open questions

- Telegram: the log trusts that a message is readable by id once its send
  returned (checked live, also from another session); an id that stays empty
  is taken as deleted. Someone deleting a claim by hand defeats the fence.
- Snapshots are taken between batches under read locks: O(state). Fine at our
  scale; persistent maps (imbl) would make them O(1) if it ever matters.
- Batch size / delay defaults per LogStore kind (`local`: no extra delay).
