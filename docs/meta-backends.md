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
below the description. The leader checks the head after its own appends and
unpins such a message; recovery does the same before trusting the head (unpin
while `head.gen < description gen`, re-read). At most one per fenced writer.

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
- to verify on a live bot: `pinned_message` really is newest-by-sending-date
  (not last-pinned); limits of `setChatDescription` / `setMyDescription`;
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
# chat_id = -1009876543210          # channel / supergroup (fence = description)

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
6. **`telegram` LogStore** (chain + description fence above), tested against the
   same suite as `local` with a fake chat (pins, description, sending order),
   plus a fault-injection test with two concurrent incarnations and stragglers.
7. **Arbiter** and a second node; `s3` LogStore.

## Open questions / Открытые вопросы

- Snapshot format: SQLite file image (simple) or a logical dump per namespace?
- Telegram: confirm `pinned_message` is newest-by-sending-date and the limits of
  `setChatDescription` / `setMyDescription` on a live bot.
- Batch size / delay defaults per LogStore kind.
