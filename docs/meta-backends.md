# Metadata as a tablet / Метаданные как таблетка

Status: **steps 1–3 implemented** — typed commands, in-memory state machines,
a tablet over the `local` LogStore, snapshots, export and restore. The rest
is a proposal. No backward compatibility: configs, on-disk and in-chat formats
are replaced, existing data is not migrated (formats are versioned, so a
binary refuses old data instead of misreading it; a converter can be written
when needed).

The model follows YDB tablets: a replicated state machine over a **shared log**
kept in a storage that can **fence** old writers; an external **arbiter** only
restarts the tablet somewhere else. Correctness comes from the log store, not
from the arbiter.

## Why / Зачем

The old design made metadata durable from inside the chunk store: a periodic
checkpoint of the chunk tables, a superblock pinned in every read-write chat
and a journal of index roots; the S3 and CAS indexes were snapshotted
separately. Commits between checkpoints were lost with the disk, the layers
could disagree after a restore, and publishing the superblock was
check-then-write, so two writers could both win. All of it is gone (step 3).

## Goals / Цели

1. Every metadata change is a typed command; state = commands applied in order.
2. The command log lives in a pluggable **`LogStore`** that fences old
   generations. State lives in memory and is rebuilt from snapshot + log on
   every boot; there is no local database.
3. A tablet can move to another node: boot = claim a generation, discover,
   replay, write a zero entry.
4. Side effects outside the state machine (deleting blobs) never run ahead of
   the log and never come from a deposed writer.
5. `LogStore` implementations are chosen in config: local file, Telegram, later
   S3 with conditional writes.

Non-goals for now: several tablets / sharding metadata; follower reads.

## Model / Модель

```
                 arbiter (bootstrapper)                 liveness only
                    │ starts / restarts the tablet
                    ▼
 ┌──────────────────────────── tablet (one per cluster) ───────────────────────────┐
 │ propose::<S>(cmd) ─► batch ─► LogStore.append(gen, batch) ─► S::apply in memory │
 │ reads: MetaStore::read::<S> (short read lock on the namespace's state)          │
 │ every N entries: snapshot all states ─► LogStore.install_snapshot (+ export)    │
 │ side effects (sweeper, repair) only while the tablet holds its generation       │
 └──────────────────────────────────────────────────────────────────────────────────┘
                    │                                             ▲
                    ▼                                             │ snapshot + tail on boot
              LogStore: claim / append / discover / read / install_snapshot
```

### Namespaces / Пространства имён

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

### Generations / Поколения

Every tablet incarnation runs under a **generation** `g`, strictly greater than
every generation that ran before. All log writes carry `g`. After a newer
generation has **claimed** the log, every `append` of an older one fails with
`Fenced`; the older incarnation stops and never acknowledges again.

### LogStore contract / Контракт журнала

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

    /// Newest snapshot + log head (bootstrap without any local state).
    async fn discover(&self) -> Result<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Store a snapshot of the state as of `at` and drop entries ≤ `at` (collect).
    async fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> Result<()>;
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

### Tablet lifecycle / Жизненный цикл

1. **Boot** (`claim`): `gen = 1 + discover().gen` (`Fenced { current }` moves
   past a racing claim); `claim(gen)` → head.
2. **Recover**: `discover` → load the newest snapshot into empty states →
   `read` and apply entries after it up to head.
3. **Zero entry**: append an empty batch for `gen` — proves the claim took effect
   and marks the start of this generation (as YDB's `WriteZeroEntry`).
4. **Active**: `propose` queues commands; the batcher appends what is queued
   (group commit), applies it in order, installs a snapshot if one is due, then
   answers the callers. `Fenced` → stop the tablet (proposals fail, side effects
   stop, `serve` exits). A command that cannot be applied stops it too; the next
   boot rebuilds from the log.
5. **Snapshot**: every `[meta.snapshot] every_entries` entries, between
   batches: one `TabletSnapshot` of all states → `install_snapshot`, which
   drops the log behind it; with `export` set, a copy goes to a directory. A
   failed snapshot keeps the log and is retried after the next batch.

Responses are released only after `append` returned `Ok`, so a client never sees
a result the next generation might not have.

**Restore** (`pigeonhole restore --from-export DIR --lossy`) seeds an empty log
from the newest export: the states as of the export, positions from 0. It is
lossy: changes after the export are gone, chunks committed later become
orphans (the sweeper deletes them) and chunks reclaimed later are referenced
but deleted. Only an off-host LogStore makes disk loss lossless.

### Arbiter / Арбитр

Liveness only, like YDB's Bootstrapper / Hive: watch the tablet's node, start it
elsewhere when it is gone. It may be wrong (two incarnations at once) — the
`LogStore` fence makes the older one fail. First implementation: none (single
node; restart = process restart). Later: a small watcher over the same LogStore
(e.g. the claim message's age).

## LogStore implementations / Реализации

### `local` — file on disk

Segmented append-only files + a `claimed_gen` file updated with fsync before
use; `append` checks the generation under a lock. Fences only processes on the
same host; durability = the disk. Default, and the reference implementation the
others are tested against.

### `telegram` — one chat, Bot API only

Two registers, both read with one `getChat`
([Bot API](https://core.telegram.org/bots/api)):

- **head** — `pinned_message`: the full `Message` of the most recent pinned
  message **by sending date** (pinning adds to a list; the newest-sent pinned
  message wins);
- **fence** — the chat `description` (`setChatDescription`), holding `gen=<g>`.
  Available in channels, supergroups and groups; in a private chat use the bot's
  own description (`setMyDescription` / `getMyDescription`) instead.

**Log layout — a chain.** Every log message is a document: a batch of entries,
its generation, and a link (`file_id` + hash) to the previous log message; the
chain ends at a snapshot document. The head of the chain is pinned. Recovery
needs no history access: `getChat` → head → follow links with `getFile` back to
the snapshot → apply forward.

`append(g, batch)`:
1. send the batch (link = current head, `gen = g`);
2. pin it;
3. `getChat`: `Ok` iff `description` is `gen=g`; otherwise `Fenced`.

`claim(g + 1)`:
1. `setChatDescription("gen=g+1")` — from here on every older `append` fails step 3;
2. `getChat` → `pinned_message` is the old head: the base;
3. send the first batch of the new generation (link = base) and pin it.

Why it is safe: an older generation acknowledged a batch only after pinning it
and then seeing its own generation in the description — i.e. before the
claimant's step 1. The claimant reads the head after step 1, and the head is the
newest-sent pinned message, so it is not older than any acknowledged batch, and
the chain behind it contains all of them.

**Stragglers.** A fenced writer may still pin one batch after the claim (sent
later, so it tops the pin) — never acknowledged. It is recognisable: its `gen` is
below the description. Do **not** unpin it: on a live bot (below) unpinning the
head makes `getChat` report no pinned message for about a minute, which a
reader would take for an empty log. Instead the leader overrides it by sending
and pinning a newer batch (an empty one if idle) that links to its own head, so
the straggler drops out of the chain; recovery skips a head whose `gen` is
below the description and waits for the leader's override (or claims anew).

**Live-bot findings** (2026-10-28, a channel, Bot API):

- `pinned_message` is the newest pinned message **by sending date**, not the
  last one pinned: pinning C, then A, then B returned C. Pinning a newer
  message shows up in the next `getChat` at once.
- After unpinning the head, `getChat` returned **no** `pinned_message` (other
  messages still pinned), also after re-pinning an older one; about a minute
  later it showed the newest remaining pin again. Never unpin in the protocol,
  and never read a missing head as an empty log when the description holds a
  generation.
- `setChatDescription` → `getChat` reads the new value at once, also right
  after a pin. Setting the same value fails with "chat description is not
  modified". Rate: six changes in a row went through (one took 3 s), the
  seventh got "Too Many Requests: retry after 43" — fine for claims, which are
  rare, but tests must not loop on it.

**Two claimants** may race on step 1 (the description is overwritten blindly).
Each verifies after step 3 that the description still holds its generation and
the head is its own batch; the loser stops. This only costs liveness: neither
had acknowledged anything yet.

Costs and caveats:
- an append is one send, one pin, one `getChat`; appends are batched — a single
  chat should stay under about one message per second
  ([Bot FAQ](https://core.telegram.org/bots/faq)), groups under 20 per minute;
  data uploads share the budget if they share the chat;
- recovery walks the chain back to the last snapshot (one `getFile` per batch),
  so snapshots bound recovery time;
- `setChatDescription` needs the `can_change_info` admin right; it is used only
  on claim;
- verified on a live bot (above): newest-by-sending-date; unpin makes the head
  vanish for a while; description is read-after-write and rate-limited;
- chat admins can delete or unpin messages; hash links detect it, nothing
  prevents it;
- the project disclaimer applies even more: this uses a chat as a database log.

### `s3` — conditional writes (later)

Entries as objects `log/<position>` written with `If-None-Match: *`; a claim is
an object at the next position, so two generations cannot both write it. Fast and
cheap compared to Telegram; needs an S3-compatible store with conditional writes.

## Contracts / Контракты

### Side effects / Побочные эффекты

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

### Garbage of the log itself

The tablet decides what is garbage in its own log (entries ≤ the last snapshot),
like YDB's `CollectGarbage` barrier on `generation:step`; ids of log objects are
chosen by the writer, so collection needs no listing. Old Telegram messages are
deleted by the tablet after a snapshot (`deleteMessage`).

## Config / Конфигурация

```toml
[meta]
batch_max_delay_ms = 0              # extra group commit window (local: 0;
                                    # batches still form while an append runs)
batch_max_entries = 1024

[meta.log]
kind = "local"                      # local (default) | telegram | s3
path = "pigeonhole-log"
# kind = "telegram"                 # ship the log off-host
# bot_token_env = "BOT_TOKEN"
# chat_id = -1009876543210          # channel / supergroup (fence = description)

[meta.snapshot]
every_entries = 10000               # 0 = never
# export = "/mnt/backup/pigeonhole" # copy every snapshot here (for restore)
```

## Plan / План

Each step is a separate commit with tests green.

1. ~~Commands, in-process.~~ Done.
2. ~~Tablet + `LogStore` (local).~~ Done: claim → replay → zero entry, group
   commit, fencing, `LogId`.
3. ~~Snapshots and removal of the old durability.~~ Done: states in memory
   (SQLite and proto gone from metadata), typed `StateMachine` with versioned
   postcard encodings, tablet snapshots + collect, export and lossy restore;
   `durability.rs`, superblock pins, index snapshots and roots deleted.
4. ~~Sweeper contract.~~ Done: `BeginReclaim` is committed before deletes;
   `confirm_active` before each orphan delete batch; loops end on stop.
5. ~~In-flight pin registry.~~ Done: upload pins + horizon, own `BlobId`s,
   read pins; watermarks, sweep grace and `InflightParts` removed.
6. **`telegram` LogStore** (chain + description fence above), tested against the
   same suite as `local` with a fake chat (pins, description, sending order),
   plus a fault-injection test with two concurrent incarnations and stragglers.
7. **Arbiter** and a second node; `s3` LogStore.

## Open questions / Открытые вопросы

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
