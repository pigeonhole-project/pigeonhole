# A Turso database replicated into pigeonhole

Status: **proposal**, not spiked. Read [roadmap.md](roadmap.md) § Ground rules,
[meta-backends.md](meta-backends.md) (LogStore, SnapshotStore, generations),
[telegram-mtproto.md](telegram-mtproto.md) and [discord-log.md](discord-log.md)
(the fenced logs this runs on) first.

## Idea

Litestream ships a SQLite database's WAL to object storage and restores it
from a snapshot plus the WAL. pigeonhole's tablet already is "a log of changes
with fenced generations + snapshots in a separate store". So a SQLite-format
database can be replicated over the **same** `LogStore` / `SnapshotStore`
abstractions the tablet uses — a Telegram or Discord channel, OpenDAL slots, a
local directory — and gets what Litestream lacks: **fencing**. Two processes
that both think they are the primary cannot both commit to the log; the
deposed one learns it at its next append.

The engine is [Turso](https://github.com/tursodatabase/turso) (SQLite rewritten
in Rust, beta), embedded in the application's process. Its `conn_raw_api`
feature exposes the WAL frame by frame — the same API Turso's own sync engine
uses — and lets checkpoints be bounded by a frame number.

Not in scope: Turso as pigeonhole's own metadata engine. The tablet's state
stays in memory, rebuilt from log + snapshot only; when it outgrows RAM, the
answer is another tablet.

## Turso API used (`turso_core` 0.9.0-pre.2, feature `conn_raw_api`)

- `Connection::wal_state() -> WalState { checkpoint_seq_no, max_frame }`.
- `Connection::wal_get_frame(frame_no, &mut buf) -> WalFrameInfo { page_no,
  db_size }`; a frame with `db_size > 0` is a commit frame (the end of a
  transaction; `db_size` = database size in pages after it).
- `Connection::wal_insert_begin()`, `wal_insert_frame(frame_no, frame)`,
  `wal_insert_end(force_commit)`: write frames into a WAL (followers).
- `Connection::checkpoint(CheckpointMode::Passive { upper_bound_inclusive })`,
  `Truncate` conditional on `max_frame`, and `PRAGMA wal_autocheckpoint = 0`.

These are pre-release and feature-gated: pin the exact version.

## Design

### Shape

- A library crate `apps/pigeonhole-turso`: `Replica::open(config) -> Replica`,
  `replica.connection()` hands out Turso connections, a background shipper
  task, `replica.wait_shipped(frame)` for synchronous durability, and
  `restore` / `follow` for other processes.
- The log and the snapshot store are built from pigeonhole's config
  (`[[store]]`): `open_log` / `open_snapshots` and the chat adapters
  (`telegram_log.rs`) move from the binary into the `pigeonhole` library crate
  so that other crates can open them.
- A demo binary (`pigeonhole turso demo` or an example) runs a workload
  against a replica.

```toml
[[database]]
id = "app"
path = "app.db"          # the local Turso database (WAL mode)
log = "tg-applog"        # a store that can hold a log (its own channel / dir / bucket)
snapshot = "tg-appsnap"  # a store that can hold snapshots
snapshot_every_frames = 100000
snapshot_every_secs = 3600
durability = "async"     # or "sync": commits wait for the log
```

A database's log is its own: own `LogId`, own channel or prefix; never the
tablet's.

### Log entries

One `Entry` per transaction (or per part of a large one):

```rust
struct WalEntry {
    page_size: u32,
    /// WAL generation (`checkpoint_seq_no`) the frames came from: frame numbers
    /// restart after a truncating checkpoint, positions in our log do not.
    wal_seq: u32,
    first_frame: u64,
    /// Page images in WAL order: (page_no, page bytes).
    pages: Vec<(u32, Vec<u8>)>,
    /// Set on the entry that ends a transaction: database size in pages.
    commit_db_size: Option<u32>,
}
```

postcard + zstd, versioned like every other format. A transaction larger than
the store's message limit is split into several entries; only the last one
carries `commit_db_size`, so replay never applies half a transaction. Entries
are grouped into batches (group commit): a Telegram log takes a batch every
few seconds, a Discord one every ~6 s, a local or OpenDAL one at disk / network
speed.

### Primary

**Open**:

1. `claim(gen)` the database's log: every older primary is fenced from here
   on. `discover` gives the base (a snapshot ref and its position) and the
   head.
2. Local state: a sidecar `app.db-ph` records `{ log_id, position, wal_seq,
   shipped_frame }` as of the last shipped commit, written after each
   successful append.
   - The sidecar's position equals the log head: the local database is the
     log's state, plus possibly commits that were never shipped; ship them
     under the new generation (nobody else wrote in between).
   - Otherwise (no local database, another primary wrote, different
     `log_id`): discard the local files and **restore** (below), then open.
3. `PRAGMA wal_autocheckpoint = 0`: the replica owns checkpoints.

**Ship** (background task, woken by commits and by a timer):

1. `wal_state()`; read frames `(shipped_frame, max_frame]` with
   `wal_get_frame`, cut after the last commit frame (frames past it belong to a
   transaction still in progress).
2. Build entries, `append(gen, batch)`.
3. `Ok` → `shipped_frame` = that commit frame, update the sidecar, wake
   `wait_shipped` waiters.
4. `Fenced` → another primary holds the log. Stop writes (connections return
   an error from now on), fail every waiter, keep nothing: on the next open
   this database restores. Commits made locally and not shipped are lost —
   they were never durable in `async` mode, and were never acknowledged in
   `sync` mode.
5. Other errors: retry; the log decides the outcome (Telegram order fence,
   Discord thread CAS, OpenDAL slot), so a retry never commits twice.

**Durability**: `async` — a commit returns when it is in the local WAL; the
loss window on a crash of the machine is the shipping lag (Litestream's
model). `sync` — the application calls `wait_shipped(commit frame)` (or the
wrapper does it after each commit) and gets `Ok` only once the log has the
transaction; latency is one log append (seconds on a messenger, ms locally).

**Checkpoint**: only frames already shipped may enter the database file:
`checkpoint(Passive { upper_bound_inclusive: Some(shipped_frame) })`. When
everything in the WAL is shipped and checkpointed, `Truncate` (conditional on
`max_frame`) restarts the WAL; `wal_seq` moves on.

**Snapshot** (every `snapshot_every_*`):

1. Checkpoint everything shipped, so the database file is the log's state at a
   known position `p` (the position of the last checkpointed commit).
2. While no checkpoint runs (the replica owns them, so writers continue into
   the WAL undisturbed), copy the database file, zstd, `SnapshotStore::put`
   with `{ log_id, at: p, page_size }`.
3. `rebase(gen, Base { at: p, snapshot })`: the log drops what is ≤ `p`; then
   remove the previous snapshot. A fenced rebase leaves a harmless orphan
   snapshot (it is named by no valid base).

### Restore

1. `discover` the log; `get` the base snapshot into `app.db` (fresh file).
2. `read(base.at, head)`: apply each committed transaction's page images to
   the file directly (write page `page_no` at `(page_no - 1) * page_size`,
   truncate to `commit_db_size` pages); drop a trailing transaction with no
   commit entry.
3. `PRAGMA integrity_check`, write the sidecar at the head's position.

Applying pages to the file needs no Turso at all: it is the SQLite file
format, so the restored file opens in C SQLite too.

### Followers

A read-only copy elsewhere: restore once, then poll `discover` and `read` the
new entries, inserting their frames with `wal_insert_begin` /
`wal_insert_frame` / `wal_insert_end` (or applying pages while no reader is
open). Followers never claim, so they never fence the primary; they lag by the
poll interval plus the log's write delay.

### Encryption

The pages land in a messenger. Turso has encryption at rest: with it on,
frames and snapshots are ciphertext before they leave the process, and nothing
here changes.

## Limits and non-goals

- WAL journal mode only; Turso's MVCC mode keeps a logical log of its own and
  is not supported.
- One primary per database (that is the point of the fence); concurrent
  writers inside it are Turso's business.
- Throughput is the log's: a messenger log suits small, bursty databases; a
  local or OpenDAL log suits real ones.
- Snapshots are whole files; a large database needs a snapshot store that
  takes large documents (MTProto, local, OpenDAL) or split snapshots
  (Discord). Page-level incremental snapshots are a later step.

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. **Spike**: `turso_core` with `conn_raw_api` in a throwaway binary: read
   frames during a write workload, bounded passive checkpoint, truncate,
   `wal_seq` change, insert frames into a fresh database, restore by applying
   pages to a file and open it in Turso and in `sqlite3`.
2. **Library plumbing**: move `open_log` / `open_snapshots` and the chat
   adapters from the binary into the `pigeonhole` library crate.
3. **Crate `pigeonhole-turso`** over `ChainLogStore` / `BlobSnapshotStore` on
   `MemoryBlobStore`:
   ship, checkpoint bound, snapshot + rebase, restore, sidecar. Tests: a
   random SQL workload with crashes at random points (restore equals a
   reference database: same `SELECT` results, `integrity_check` ok); two
   primaries (the second claim fences the first, its unshipped commits are
   dropped); a transaction split over entries; `sync` mode acknowledgements.
4. **Followers**.
5. **Config and demo**: `[[database]]`, the demo binary, runs against a local
   log, then a Telegram (MTProto) and a Discord log with the test channels.

## Open questions

- `sync` durability by an explicit `wait_shipped`, or by wrapping Turso's
  `IO` so the WAL's `sync` after a commit frame completes only once shipped?
  The wrapper makes every commit synchronous with no API change, at the cost
  of depending on Turso's IO layer.
- A C SQLite primary (Litestream-style: tail the `-wal` file, hold a read
  transaction to keep checkpoints back) — worth it, or Turso only?
- Snapshot cadence: frames, bytes, time, or "the log got as long as the
  database"?
