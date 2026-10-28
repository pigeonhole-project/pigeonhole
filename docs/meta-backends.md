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
(`ChatSnapshotStore`: one document per snapshot in a dedicated chat, the
newest pinned so `newest` needs no history). The log and the snapshots cannot
share a chat: both use the pinned message.

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
   boot rebuilds from the log.
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

### `telegram` — one chat, Bot API only

Implemented as `ChatLogStore` (`pigeonhole-meta/src/log/chat.rs`, protocol
over a `ChatApi` trait, tested on a fake chat) with a Telegram adapter in the
binary. Two registers, both read with one `getChat`
([Bot API](https://core.telegram.org/bots/api)):

- **head** — `pinned_message`: the newest pinned message **by sending date**;
- **fence** — the chat `description`: `pigeonhole-log id=<log id> gen=<g>`.
  Channels, supergroups and groups (private chats are not supported).

**Log layout — a chain.** Every log message is a document (postcard `LogDoc`):
log id, generation, the log's current base, a crc-checked link to the previous
log message (with the position of its last entry) and a batch. Recovery needs
no history access: `getChat` → head → base snapshot from the SnapshotStore →
follow links with `getFile` back to the base → apply forward. `rebase` sends
an empty batch carrying the new base and deletes the messages it covers.

`append(g, batch)`:
1. send the batch (link = this writer's tip, `gen = g`);
2. pin it;
3. `getChat`: `Ok` iff the description still says `gen=g`, else `Fenced`. If
   something newer tops the pin (a straggler), send and pin a newer empty batch
   linked to the tip — never unpin.

`claim(g)`:
1. refuse a chat whose description or pin is not ours (the log needs a
   dedicated chat; the config refuses a log chat that is also a blob chat);
2. `setChatDescription(gen=g)` — from here on every older `append` fails step 3;
3. **wait the straggler window** (`straggler_window_secs`, default 90 s, longer
   than the 30 s deadline the adapter puts on every Bot API call);
4. `getChat`: the description must still say `gen=g` (else `Fenced`); the
   pinned message is the base.

Why it is safe: an older generation acknowledged a batch only after pinning it
and then seeing its own generation in the description — i.e. before the
claimant's step 2. A deposed writer has at most one batch in flight; it lands
within the call deadline, so before step 4, and it links to that writer's own
tip. The head read at step 4 is therefore the newest-sent message of the
newest older generation, and the chain behind it holds every acknowledged
batch. A straggler is never acknowledged but may become part of the history
(its proposer got an error: outcome unknown, as in Raft).

**Two claimants** may race on step 2 (the description is overwritten
blindly): whoever wrote last wins; the other sees a different generation at
step 4 or on its first append and stops. Neither had acknowledged anything.

**Live-bot findings** (2026-10-28, a channel, Bot API):

- `pinned_message` is the newest pinned message **by sending date**, not the
  last one pinned: pinning C, then A, then B returned C. Pinning a newer
  message shows up in the next `getChat` at once.
- After unpinning the head, `getChat` returned **no** `pinned_message` (other
  messages still pinned), also after re-pinning an older one; about a minute
  later it showed the newest remaining pin again. Hence: never unpin, and
  retry a missing head (`head_retries` × `head_retry_delay`, 2 minutes) before
  taking a claimed log for empty.
- `setChatDescription` → `getChat` reads the new value at once, also right
  after a pin. Setting the same value fails with "chat description is not
  modified" (a claimant with the same generation is fenced). Rate: six changes
  in a row went through (one took 3 s), the seventh got "Too Many Requests:
  retry after 43" — fine for claims, which are rare.

Costs and caveats:
- an append is one send, one pin, one `getChat`; appends are batched — a single
  chat should stay under about one message per second
  ([Bot FAQ](https://core.telegram.org/bots/faq)), groups under 20 per minute;
  set `[meta] batch_max_delay_ms` (e.g. 1000) for a Telegram log;
- a claim (boot, failover) takes the straggler window;
- recovery walks the chain back to the base (one `getFile` per batch), so
  snapshots bound recovery time; a snapshot in a chat is one document, so it
  must fit the bot download limit (20 MB);
- every pin leaves a "pinned message" service message in the chat, and nothing
  deletes them yet;
- the bot needs `can_change_info`, `can_pin_messages` (channels: edit
  messages) and `can_delete_messages`;
- chat admins can delete or unpin messages; crc links detect a changed
  message, nothing prevents it;
- the project disclaimer applies even more: this uses a chat as a database log.

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
chosen by the writer, so collection needs no listing. Old Telegram messages are
deleted by `rebase` (`deleteMessage`), the previous base snapshot by the tablet.

## Config

```toml
[meta]
batch_max_delay_ms = 0              # extra group commit window (local: 0;
                                    # batches still form while an append runs)
batch_max_entries = 1024

log = "meta"                        # a [[store]] id: local_disk | memory | telegram
snapshot = "meta"                   # default: the log's store if local_disk / memory
snapshot_every_entries = 10000      # 0 = never
snapshot_every_secs = 600           # also by time if anything changed
# log_straggler_window_secs = 90    # telegram log: claim waits for a deposed writer

[[store]]
id = "meta"
kind = "local_disk"
path = "pigeonhole-meta"

# [[store]]                         # ship the log off-host: a channel of its own
# id = "tg-log"                     # (fence = description); snapshots then go to
# kind = "telegram"                 # another telegram store
# chat_id = -1009876543210
```

| `[meta] log` | `[meta] snapshot` | losing this host's disk |
|---|---|---|
| local_disk | the same store | everything |
| memory | memory | everything (tests) |
| local_disk | another local_disk / telegram | `restore --lossy`: the tail after the newest snapshot |
| telegram | telegram (another chat) | nothing |
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

| need | local_disk | memory | telegram (chain) |
|---|---|---|---|
| `append(entries)`, durable at callback | segment record + fsync | push | send + pin, one message per batch |
| `truncate(from)` (drop a divergent tail) | cut the segment | pop | a new message linking to the entry before `from`: the chain forks, the old tail is unreachable garbage |
| `purge(upto)` (behind a snapshot) | `rebase` (exists) | drop | `rebase` (exists) |
| `save_vote` / `read_vote`, durable | a `vote` file (tmp + rename) | field | carried in every message; a vote change alone sends an empty message |
| one process per node | `flock` + `claimed_gen` (exists) | — | description fence + straggler window (exists) |

Entries carry their Raft `LogId` (term, index) instead of our generation;
`claim` stays, now meaning "this process is node X" so two processes started
as the same node cannot both vote. A conformance suite (append / truncate /
purge / vote / reopen) runs against every kind, as the LogStore suite does now.

Appends are batched: openraft hands `append` several entries at once, and
while one append is in flight new commands queue for the next. A store's
speed limits appends, not commands: a Telegram channel takes about 20
messages a minute, one message carries any number of entries (up to 20 MB).
Costs per append: local_disk ~ms (fsync), telegram 1–3 s (send, pin, read
the fence).

### Two processes as one node

Raft is safe only if every node keeps its promises (a vote, an
acknowledged entry). Two processes started as the same node would break that,
so the node's store fences them, the way the tablet's log fences writers now:

- **Every durable change is fenced.** Granting a vote (`save_vote`),
  acknowledging entries (`append` / `truncate`) and campaigning (a vote for
  itself) first write to the store; the write succeeds only while the process
  holds the store's generation (`flock` + `claimed_gen`, or the chat
  description `pigeonhole-node id=<node> gen=<g>` read back after the pin).
  A process that lost the store can neither vote nor acknowledge.
- **Answers without a write are leased.** A heartbeat answered from memory
  ("I match up to i") writes nothing. A process re-reads its fence at least
  every half straggler window and stops once it is not its own; a claiming
  process waits the window before taking part. Their lifetimes do not overlap,
  on the same timing assumptions as the log fence (a deadline on every call,
  the window longer than it).
- **The fence names the node**: a process started as node `c` on node `b`'s
  chat refuses to claim it; one config cannot give two nodes the same store.
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
   a conformance suite for local_disk, memory, telegram (fake chat).
2. `MetaStore` over openraft in one process: a cluster of one, every existing
   test green, the old tablet batcher removed.
3. Network + in-process multi-node tests (memory stores, a fake transport):
   leader election, failover, a node on a fake Telegram chat, partitions;
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
  `rebase` deletes the chain; also the pin service messages (step 6b).

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
6. ~~`telegram` LogStore.~~ Done: `ChatLogStore` + Telegram adapter, the
   LogStore conformance suite and fault injection on a fake chat; snapshots
   moved into a `SnapshotStore` of their own (`local` | `telegram`).
7. **Arbiter** and a second node; `s3` LogStore.

## Open questions

- Telegram: the description is not a compare-and-swap; the protocol's safety
  rests on reading it right after the pin (read-after-write held on a live
  bot). `setMyDescription` (private chats) is not verified.
- Telegram: a straggler batch pinned between a claim's steps 1 and 2 becomes the
  new chain's base, so a command its writer answered with an error is applied
  anyway (outcome unknown, as in Raft). Document it in the contract, or use a
  new chain per generation (BookKeeper style).
- Snapshots are taken between batches under read locks: O(state). Fine at our
  scale; persistent maps (imbl) would make them O(1) if it ever matters.
- Batch size / delay defaults per LogStore kind (`local`: no extra delay).
