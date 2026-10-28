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

    /// Base + log head + newest generation (bootstrap without local state).
    async fn discover(&self) -> Result<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Start the log from `base` = { at, snapshot: SnapshotRef } (a snapshot
    /// already stored) and drop entries ≤ `at` (collect). Fenced like append.
    async fn rebase(&self, gen: u64, base: Base) -> Result<()>;
}
```

`Entry` = `{ namespace, version, command }`, a `Batch` = `{ format, gen, first,
entries }`, both postcard. Every log has a `LogId` fixed by its first claim.
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
