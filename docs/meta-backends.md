# Metadata as a tablet / Метаданные как таблетка

Status: **step 1 implemented** (commands + one SQLite state machine); the rest is a
proposal. No backward compatibility: configs, on-disk and in-chat formats are
replaced, existing data is not migrated.

The model follows YDB tablets: a replicated state machine over a **shared log**
kept in a storage that can **fence** old writers; an external **arbiter** only
restarts the tablet somewhere else. Correctness comes from the log store, not
from the arbiter.

## Problem / Проблема

Today metadata durability is hard-wired into the chunk store
(`durability.rs`): a checkpoint of chunk metadata plus a superblock pinned in
each read-write chat, and a journal that records only `SetRoot`.

- Chunk commits / retain / release are durable only at the next checkpoint. A
  restore after disk loss can find a root pointing at a chunk missing from the
  checkpoint, and the sweeper then deletes the parts of every chunk committed
  after it.
- The S3 and CAS indexes are snapshotted separately (`s3/index`, `cas/index`), so
  after a restore the layers disagree.
- Publishing the superblock is check-then-write on the pin: two writers can both
  pass `ensure_not_fenced` and the later pin silently wins.

## Goals / Цели

1. Every metadata change is a typed command; state = commands applied in order
   (done in step 1: `pigeonhole-meta`, `chunk` / `s3` / `cas` namespaces, one
   SQLite file).
2. The command log lives in a pluggable **`LogStore`** that fences old
   generations. The node's SQLite file is a cache rebuilt from snapshot + log.
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
 │ propose(Command) ─► batch ─► LogStore.append(gen, batch) ─► apply to SQLite      │
 │ reads ◄──────────────────────────────────────────────────────── SQLite (cache)  │
 │ side effects (sweeper, repair) only while the tablet holds its generation       │
 └──────────────────────────────────────────────────────────────────────────────────┘
                    │                                             ▲
                    ▼                                             │ replay on boot
              LogStore: claim / append / discover / read / snapshot / collect
```

### Generations / Поколения

Every tablet incarnation runs under a **generation** `g`, strictly greater than
every generation that ran before. All log writes carry `g`. After a newer
generation has **claimed** the log, every `append` of an older one fails with
`Fenced`; the older incarnation stops and never acknowledges again.

### LogStore contract / Контракт журнала

```rust
trait LogStore {
    /// Become generation `gen` (> any earlier claim). Fences every older
    /// generation and returns the log head they left behind: every entry
    /// whose append was acknowledged to an older generation is ≤ that head.
    async fn claim(&self, gen: u64) -> Result<Head, ClaimError>;

    /// Durably append a batch for `gen`. `Ok` = committed; `Fenced` = a newer
    /// generation exists, nothing about this batch may be acknowledged.
    async fn append(&self, gen: u64, batch: &[Entry]) -> Result<Position, AppendError>;

    /// Newest snapshot + log head (bootstrap without any local state).
    async fn discover(&self) -> Result<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Store a snapshot of the state as of `at` and drop entries ≤ `at` (collect).
    async fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> Result<()>;
}
```

`Entry` = the existing envelope `{ namespace, command }` (proto), applied by
`MetaStore` exactly as in step 1. A batch is one log write: commits are grouped
(group commit) because some stores (Telegram) allow a write every few seconds.

### Tablet lifecycle / Жизненный цикл

1. **Boot** (`claim`): `gen = 1 + max(known generations)`; `claim(gen)` → head.
2. **Recover**: if the local SQLite file is behind (or absent), `discover` →
   load snapshot → `read` and apply entries up to head.
3. **Zero entry**: append an empty batch for `gen` — proves the claim took effect
   and marks the start of this generation (as YDB's `WriteZeroEntry`).
4. **Active**: `propose` buffers commands; a batch is appended, then applied to
   SQLite, then callers get their responses. `Fenced` → stop the tablet
   (responses fail, side effects stop, process exits or waits for the arbiter).
5. **Snapshot**: periodically `install_snapshot` (SQLite file image) and collect
   the log behind it.

Responses are released only after `append` returned `Ok`, so a client never sees
a result the next generation might not have.

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

### `telegram` — a channel or supergroup, Bot API only

The fence is the **message id sequence**. Facts it relies on
([updates](https://core.telegram.org/api/updates),
[channels](https://core.telegram.org/api/channel),
[Bot API](https://core.telegram.org/bots/api)):

- every channel / supergroup has its **own** monotonically increasing
  `message_id` sequence; every send returns the id it got;
- private chats and basic groups share **one** sequence per account (for a bot:
  all its private chats and basic groups), so anyone who messages the bot would
  insert ids there — the log belongs in a channel or supergroup;
- `getChat().pinned_message` is the full `Message` of the most recent pinned
  message by sending date — pinning a message and calling `getChat` reads it
  back by id (the only read-by-id the Bot API offers).

Layout: every log message is a document holding a batch of entries **and** the
cumulative superblock (last snapshot reference + `file_id`s of the log messages
since it) and the writer's generation. Data blobs may share the chat.

**Generation** = the `message_id` of the writer's **claim** message. Ids grow with
time, so a later claim always has a larger generation.

**Writer rule.** The writer records the id of every message it sends (log
batches and blobs). Before acknowledging a batch that got id `m`, it waits until
all of its sends that could have received an id below `m` have returned; then
every id between its previous log message and `m` must be one of its own. A
foreign id means someone else wrote — possibly a newer claim: the batch is not
acknowledged and the writer stops (`Fenced`).

`claim()`:
1. send a claim message; its id `c` is the new generation;
2. read message `c − 1` (pin → `getChat` → unpin) and walk backwards past
   foreign blobs and log messages of older generations to the newest log message
   or claim; its cumulative superblock is the base;
3. `getChat`-reads and the walk only touch messages sent before `c`.

Why it is safe: to take over, a claimant must send a message, which lands as a
foreign id in the old writer's next range, so the old writer acknowledges only
batches sent before the claim — and those are below `c`, inside the base the
claimant adopts (superblocks are cumulative). An old writer sends at most what it
had in flight after the claim (one log batch, up to `upload_concurrency` blobs)
and then stops.

**Foreign ids seen by the current leader.** When its own range contains foreign
ids, the leader reads them (pin trick): a blob or a log message of an older
generation is a straggler — accept the gap; a claim of a **newer** generation →
stop.

`discover` = `claim`: recovery starts by claiming, then reading backwards from
`c − 1`, skipping stragglers of older generations, to the newest message of the
newest generation.

Costs and caveats:
- an append is **one send**; pins and `getChat` are used only to read by id (claim,
  recovery, rare foreign ids);
- appends are batched (one message per batch); a single chat should stay under
  about one message per second ([Bot FAQ](https://core.telegram.org/bots/faq)),
  groups under 20 per minute — data uploads share that budget when they share
  the chat;
- a log in a private chat is still safe, but any message to the bot in any
  private chat inserts an id and makes the leader stop needlessly — only for
  strictly personal use;
- to verify on a live bot: dense ids in a channel under concurrent sends; the
  pin → `getChat` → unpin read; pinning without admin rights where applicable;
- chat admins can delete messages; hash links detect it, nothing prevents it;
- the project disclaimer applies even more: this uses a chat as a database log.

### `s3` — conditional writes (later)

Entries as objects `log/<position>` written with `If-None-Match: *`; a claim is
an object at the next position, so two generations cannot both write it. Fast and
cheap compared to Telegram; needs an S3-compatible store with conditional writes.

## Contracts / Контракты

### Side effects / Побочные эффекты

- Only the active tablet runs the sweeper and repair; on `Fenced` they stop
  before the next backend call.
- Reclaim: propose `BeginReclaim`, wait for it to be **committed** (appended),
  then delete parts, then propose `DropChunk`. A restore or failover never brings
  back a reference to deleted parts.
- Orphan sweep needs the in-flight registry (own blob ids before upload, a later
  step); parts become known only through committed `CommitChunk` / `AddReplica`,
  and a fenced incarnation cannot commit, so its in-flight parts are garbage.
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
db_path = "pigeonhole-meta.db"      # local SQLite cache of the state
batch_max_delay_ms = 2000           # group commit window

[meta.log]
kind = "local"                      # local (default) | telegram | s3
path = "pigeonhole-log"
# kind = "telegram"                 # ship the log off-host
# bot_token_env = "BOT_TOKEN"
# chat_id = -1009876543210          # channel / supergroup (own id sequence)

[meta.snapshot]
every_entries = 10000
# export = "telegram"               # also push snapshots off-host (like today)
```

## Plan / План

Each step is a separate commit with tests green.

1. ~~Commands, in-process.~~ Done: `pigeonhole-meta`, `chunk` / `s3` / `cas`
   namespaces, one SQLite file.
2. **Tablet + `LogStore` (local).** `LogStore` trait and the `local`
   implementation; `MetaStore::propose` appends a batch before applying; boot =
   claim → discover → replay → zero entry; fenced → stop. Tests: replay rebuilds
   identical SQLite state; a second incarnation fences the first.
3. **Snapshots and removal of the old durability.** `install_snapshot` (SQLite
   file image) + collect; delete `durability.rs` (checkpoint, superblock pin,
   root journal) and the `s3/index` / `cas/index` snapshots; `pigeonhole restore`
   = discover + replay.
4. **Sweeper contract.** `BeginReclaim` committed before deletes; side effects
   only while active.
5. **In-flight pin registry** (own blob ids before upload) replacing put
   watermarks, sweep grace and `InflightParts`; read pins.
6. **`telegram` LogStore** (message-id fence above), tested against the same
   suite as `local` (with a fake chat that assigns ids), plus a fault-injection
   test with two concurrent incarnations and stragglers.
7. **Arbiter** and a second node; `s3` LogStore.

## Open questions / Открытые вопросы

- Snapshot format: SQLite file image (simple) or a logical dump per namespace?
- Telegram: confirm dense per-channel ids under concurrent sends and the
  pin-to-read trick on a live bot.
- Batch size / delay defaults per LogStore kind.
