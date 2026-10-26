# Metadata backends / Бэкенды метаданных

Status: **proposal** (not implemented).

## Problem / Проблема

Blob backends (Telegram, Discord) are slow, rate-limited stores for **data**. Today they
also carry **metadata durability**, hard-wired into the chunk store:

- `blob.db` is checkpointed into the blob backend and a superblock is pinned there;
- a journal segment is uploaded and the pin rewritten every 5 s — ~12 sends/min on a
  Telegram chat whose whole budget is ~20/min;
- the journal records only `SetRoot`, so chunk commits / retain / release are durable
  only at the next checkpoint. A restore after disk loss can find a root pointing at a
  chunk that is not in the checkpoint, and the sweeper then deletes the parts of every
  chunk committed after it;
- the S3 index and the CAS index are snapshotted separately (roots `s3/index`,
  `cas/index`) on their own schedule, so after a restore the layers disagree.

None of this is configurable: every deployment pays the send budget and gets the same
(weak) guarantees.

## Goals / Цели

1. Blob backends store data. Writing metadata there is an **option** of a metadata
   backend, never a requirement of the chunk store.
2. Durability of metadata (WAL or not, where it is shipped, snapshot period and
   target) is configured per metadata backend.
3. Each layer keeps its own metadata interface; layers stay independent
   (`check-deps.sh --strict`).
4. Several layers can be configured onto **one** metadata backend, and then they are
   persisted and restored together, as of one point in time.
5. The sweeper's safety does not depend on which strategy is configured.

Non-goals for the first iteration: engines other than SQLite; multiple writer
processes; replicating metadata between nodes for availability.

## Model / Модель

```
 layer interfaces (unchanged ownership)          metadata backend (new)
 ┌──────────────────────────┐                    ┌─────────────────────────────┐
 │ chunk-store: ChunkMeta   │──┐                 │ engine: SQLite file         │
 │  chunks, parts, refs,    │  │  namespace      │ durability:                 │
 │  roots, sweep state      │  ├───────────────► │   wal:      none | ship(..) │
 ├──────────────────────────┤  │                 │   snapshot: none | every(..)│
 │ gateway-s3: S3Index      │──┤                 │             to = path | blob│
 ├──────────────────────────┤  │                 │ marks: current(), durable() │
 │ gateway-bytestream: Cas  │──┘                 └─────────────────────────────┘
 └──────────────────────────┘
```

- A **metadata backend** (`MetaBackend`) = one storage engine + one durability
  strategy. It is named in config and opened once.
- A **layer** opens its store on a backend under a **namespace** (table prefix or an
  attached schema, e.g. `chunk_`, `s3_`, `cas_`; today S3 and CAS both own a `meta`
  table). The layer still owns its schema, queries and migrations.
- Layers that share a backend share one SQLite file and one durability stream, so a
  snapshot or a WAL position covers all of them at once.

### Durability strategies / Стратегии

| Setting | Meaning | Typical use |
|---|---|---|
| `wal = none` | Only the local file. Durable on local disk, lost with it. | dev, caches (CAS) |
| `wal = { ship_to = "<dir or s3 url>" }` | Ship SQLite WAL frames continuously (Litestream-style) to a target that is **not** the blob backend. | production |
| `snapshot = { every_secs, to = "<dir or s3 url>" }` | Full consistent copy on a schedule. | backup, bounded WAL replay |
| `snapshot = { every_secs, to = "blob", via = "<instance>" }` | Today's mechanism: snapshot as chunks + pinned pointer in a blob instance. Rare by design (e.g. hourly). | last-resort bootstrap from the chat alone |

Strategies combine: `wal.ship_to` + an hourly `snapshot.to = "blob"` gives ~1 s RPO from
the WAL target and a fallback that needs nothing but the chat. There is no per-5 s
journal in the blob backend any more.

### Durability marks / Метки надёжности

The backend keeps a monotonically increasing **commit mark** (one counter row bumped in
every write transaction, shared by all namespaces on the backend).

- `current_mark()` — mark of the latest local commit.
- `durable_mark()` — highest mark that would survive losing the local disk:
  - `wal = none`, no snapshot → equals `current_mark()` (the disk is the durability);
  - `wal.ship_to` → mark of the last shipped WAL frame;
  - snapshot only → mark at the start of the last completed snapshot.
- `wait_durable(mark)` — resolves once `durable_mark() ≥ mark` (immediate for `none`;
  for snapshot-only it may wait for the next snapshot — callers use it only where that
  is acceptable).

## Contracts / Контракты

### Sweeper (chunk store)

Deleting backend data must never run ahead of the metadata that makes it garbage.

- `release` that drops a chunk to `refs = 0` stores the commit mark in the chunk row
  (`zero_mark`).
- Reclaim considers only chunks with `zero_mark ≤ durable_mark()` of the chunk store's
  backend. After a restore, a chunk is therefore either still referenced or its
  zero-ref state was durable — its parts are never deleted while a durable reference
  to them can come back.
- Orphan sweep (keys on the blob backend unknown to metadata) runs only when the
  metadata is known to be complete up to the listing: after a restore, the sweeper
  stays in reclaim-only mode until reconciliation (below) has finished.

With `wal = none` this degenerates to today's behaviour (marks are durable at commit).

### Acknowledging writes / Подтверждение записи

Per layer: `ack = "local" | "durable"`.

- `local` (default): a PUT is acknowledged after the local commit. Losing the disk can
  lose up to RPO of acknowledged objects.
- `durable`: the gateway calls `wait_durable(mark)` before acknowledging. Cheap with
  `wal.ship_to`; rejected at config load for snapshot-only backends.

### Shared vs separate backends / Общий или раздельные бэкенды

- **Shared** (chunk store and a gateway on one backend): restore returns all layers as
  of the same mark. Refcounts match gateway references; nothing to reconcile.
- **Separate**: layers restore to different points in time. Supported, with
  consequences handled explicitly:
  - gateway extent → chunk missing in chunk metadata: read fails with `ChunkGone`
    (never aliases, ids are not reused);
  - chunk with `refs > 0` that no gateway references: leaked until **reconciliation** —
    each gateway enumerates its chunk references, the chunk store recomputes `refs`
    from them, and only then is orphan sweep re-enabled.
  The config loader warns when the chunk store's RPO is worse than a gateway's on a
  separate backend (gateway references would point at chunks that can be lost).

## Restore / Восстановление

1. For each metadata backend: pick the newest source among WAL target and snapshots;
   restore the SQLite file (snapshot + WAL replay up to the newest frame).
2. Open layers on their backends (namespaced schemas migrate as usual).
3. If any layer sits on a separate backend: run reconciliation.
4. Start the sweeper (orphan sweep enabled only after step 3).

`pigeonhole restore` keeps its CLI; what it reads comes from the backend config.

## Config / Конфигурация

```toml
[meta.main]
engine = "sqlite"
path = "pigeonhole-meta.db"
wal = { ship_to = "/mnt/backup/pigeonhole-wal" }
snapshot = { every_secs = 3600, to = "blob", via = "tg-main" }

[meta.cache]
engine = "sqlite"
path = "pigeonhole-cas.db"          # no wal, no snapshot: a cache can be rebuilt

[chunk_store]
meta = "main"

[gateway.s3]
meta = "main"                       # same backend → restored together
ack = "local"

[gateway.bytestream]
meta = "cache"
```

Legacy keys (`database_url`, `[snapshot].interval_secs`) map to a single backend `main`
with `snapshot.to = "blob"` so existing configs keep working; the 5 s blob journal is
dropped.

## Migration plan / План переезда

Each step is a separate commit with tests green.

1. **`MetaBackend` + namespaces.** Introduce the backend type (SQLite engine, commit
   mark, `durable_mark`, `wait_durable`); open `BlobDb`, S3 `Index`, `CasIndex` through
   it with prefixes. One shared file by default; `wal = none`, no snapshot.
2. **Snapshot strategy.** Move checkpoint + superblock + pin out of `chunk-store`
   (`durability.rs`) into a `snapshot.to = "blob"` strategy that snapshots the whole
   backend file. Remove the per-5 s journal and the per-layer index snapshots
   (`s3/index`, `cas/index` roots).
3. **Sweeper contract.** `zero_mark`, reclaim gated on `durable_mark()`, reclaim-only
   mode after restore.
4. **WAL shipping** to a directory, then to S3-compatible storage.
5. **Separate backends:** reconciliation + config warnings; `ack = "durable"`.

The unified pin registry for in-flight uploads and reads (replacing put watermarks,
sweep grace, `InflightParts`) is independent of this document and comes after step 3.

## Open questions / Открытые вопросы

- WAL shipping: implement in-process, or run Litestream next to the binary and only
  read its position for `durable_mark()`?
- Snapshot to blob: one SQLite file image (simple, larger) or a logical dump per
  namespace (today's JSON, portable across engines)?
- Should `ack = "durable"` be the default once WAL shipping exists?
