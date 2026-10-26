# Metadata as a replicated state machine / Метаданные как RSM

Status: **proposal** (not implemented). No backward compatibility: configs, on-disk and
in-chat formats are replaced, existing data is not migrated.

## Problem / Проблема

Blob backends (Telegram, Discord) are slow, rate-limited stores for **data**. Today they
also carry **metadata durability**, hard-wired into the chunk store:

- `blob.db` is checkpointed into the blob backend and a superblock is pinned there;
- a journal segment is uploaded and the pin rewritten every 5 s — ~12 sends/min on a
  Telegram chat whose whole budget is ~20/min;
- the journal records only `SetRoot`, so chunk commits / retain / release are durable
  only at the next checkpoint. A restore after disk loss can find a root pointing at a
  chunk missing from the checkpoint, and the sweeper then deletes the parts of every
  chunk committed after it;
- the S3 and CAS indexes are snapshotted separately (roots `s3/index`, `cas/index`) on
  their own schedule, so after a restore the layers disagree.

## Goals / Цели

1. Blob backends store data only. Metadata durability is a property of the metadata
   subsystem, configured independently.
2. Every metadata change is a typed **command**; metadata state is the result of
   applying the command log in order — a replicated state machine (RSM).
3. One node first; adding nodes later is a membership change, not a redesign.
4. Each layer keeps its own commands, schema and queries; layers stay independent
   (`check-deps.sh --strict`). Several layers may share one state machine and are then
   persisted and restored together.
5. Sweeper safety is defined against the RSM commit index, independent of cluster size.

Non-goals for now: storage engines other than SQLite; sharding metadata (multi-raft);
follower reads.

## Model / Модель

```
 gateway / chunk-store code
   │  propose(Command)            read queries (SQL on local SQLite)
   ▼                                   ▲
 ┌───────────────────────── MetaGroup (one raft group) ──────────────────────────┐
 │ OpenRaft                                                                      │
 │  RaftLogStorage   — command log on local disk (proto-encoded entries)         │
 │  RaftStateMachine — apply(entry) → SQL in one transaction per entry           │
 │                     snapshot = consistent copy of the SQLite file             │
 │  RaftNetwork      — gRPC (tonic) between nodes; unused with one node          │
 └───────────────────────────────────────────────────────────────────────────────┘
        │ snapshot export (optional): dir / S3 / blob backend (rare, e.g. hourly)
```

- **Command** — a proto message, one per state transition, one SQLite transaction on
  apply. Commands are **deterministic**: everything that must be identical on every
  replica (chunk ids, timestamps, locators) is chosen by the proposer and carried in the
  command; `apply` never generates ids or reads the clock.
- **Namespaces** — each layer owns its tables under a prefix (`chunk_`, `s3_`, `cas_`;
  today S3 and CAS both have a `meta` table) and its command set in its own proto
  package. The group dispatches an entry to the layer that owns its command.
- **Reads** go to the local SQLite of the node that serves the request. With one node
  this is simply the database; with several, writes and reads go to the leader (see
  Distribution).
- **Durability** is "committed by a quorum of the group". With one node that is the
  local log fsync; snapshot export adds a copy that survives losing the node.

### Commands / Команды (first cut)

```proto
// proto/pigeonhole/meta/v1/chunk.proto
message CommitChunk { int64 chunk_id = 1; int64 logical_size = 2; uint32 crc32 = 3;
                      repeated Block blocks = 4; repeated ReplicaLayout replicas = 5;
                      int64 created_at_ms = 6; }
message Retain      { repeated int64 chunk_ids = 1; }   // all-or-nothing
message Release     { repeated int64 chunk_ids = 1; }
message BeginReclaim{ int64 chunk_id = 1; }             // live, refs=0 → reclaiming
message DropChunk   { int64 chunk_id = 1; }             // after backend deletes
message AddReplica  { int64 chunk_id = 1; ReplicaLayout replica = 2; }  // repair
message SetRoot     { string name = 1; repeated Extent extents = 2; }
message SyncInstances { repeated Instance instances = 1; }

// proto/pigeonhole/meta/v1/s3.proto
message PutObject   { ObjectRow object = 1; }           // returns old chunk ids
message DeleteObject{ string bucket = 1; string key = 2; }
message CopyObject  { ... }  message PutPart { ... }  message CompleteUpload { ... }
message CreateBucket{ ... }  message DeleteBucket { ... }  message PutTags { ... }

// proto/pigeonhole/meta/v1/cas.proto
message PutCasEntry { ... }  message DropCasEntry { ... }  message TouchCasEntry { ... }
```

Commands that can fail on state (`Retain` on a reclaiming chunk, `Release` below zero,
`CreateBucket` that exists) fail **deterministically** in `apply` and return a typed
error in the response; every replica reaches the same result.

Cross-layer atomicity stays as today (e.g. gateway `PutObject` and chunk `Release` of
the old chunks are separate commands). On a shared group a batch command can make them
atomic later.

## Contracts / Контракты

### Sweeper and other side effects / Побочные эффекты

Deleting from the blob backend is a side effect outside the state machine.

- Only the **leader** runs the sweeper and repair, and confirms it is still leader
  (OpenRaft read-index / leader lease) before each delete batch. A deposed leader must
  not delete on stale state.
- Reclaim: propose `BeginReclaim`; only after it is **committed** delete the parts;
  then propose `DropChunk`. A restore or failover therefore never brings back a
  reference to deleted parts: the chunk was already `reclaiming` in the committed log.
- Orphan sweep (keys on the backend unknown to metadata) needs the in-flight upload
  registry (separate design: pins keyed by our own blob ids, assigned before upload).
  Parts become known to metadata only through a committed `CommitChunk` / `AddReplica`;
  a deposed leader cannot commit, so its in-flight parts are garbage after failover.
- After restoring from an exported snapshot the log tail may be missing: sweeper runs
  reclaim-only until reconciliation (below) has finished.

### Acknowledging writes / Подтверждение записи

A PUT is acknowledged after its commands are **committed** in the group (`client_write`
returns). With one node this is a local fsync; with three it is a quorum. There is no
separate `ack` setting: the group size is the knob.

### Shared vs separate groups / Общая или раздельные группы

- **Shared** (chunk store and a gateway in one group): one log, restored as of one
  index. Refcounts match gateway references.
- **Separate** groups restore independently; dangling extents fail with `ChunkGone`
  (ids are never reused), leaked chunks wait for **reconciliation**: gateways enumerate
  their chunk references, chunk refs are recomputed via commands, then orphan sweep is
  re-enabled.

## Distribution / Распределённость

With N nodes each runs the gateways and a replica of the group.

- Writes are forwarded to the leader (or the client is redirected); reads are served by
  the leader. Follower reads are a later step.
- Blob backend rate limits are per **bot**, not per node: uploads go through the leader
  (or the leader hands out send budget).
- Telegram `file_id` is bot-bound: all nodes use the same bot token per instance.
- Data replication across blob instances (`Replicated`, repair) is unchanged; Raft only
  replicates metadata.

## Config / Конфигурация

```toml
[meta]
node_id = 1
data_dir = "/var/lib/pigeonhole/meta"   # raft log + SQLite state machine
listen_addr = "0.0.0.0:7400"            # raft RPC; unused with one node
members = { 1 = "node1:7400" }          # later: { 1 = ..., 2 = ..., 3 = ... }

[meta.snapshot]
every_entries = 50000
export = [
  { to = "dir", path = "/mnt/backup/pigeonhole" },
  { to = "blob", via = "tg-main", every_secs = 3600 },  # last resort: chat only
]

[chunk_store]
group = "main"

[gateway.s3]
group = "main"

[gateway.bytestream]
group = "cache"                         # separate group, no export: a cache
```

## Implementation choice / Выбор реализации

- **OpenRaft** (databendlabs): pluggable `RaftLogStorage` / `RaftStateMachine` /
  network, tokio, deterministic simulation and Jepsen in CI, used by Databend's
  meta-service. Still `0.10.0-alpha.*` — pin an exact version, expect API churn.
  Storage implementations are checked with its `Suite::test_all()`.
- Considered: `tikv/raft-rs` (tick-driven core only, we would write log / state machine
  / transport ourselves); Hiqlite (embeddable raft SQLite on OpenRaft — replicates SQL,
  not typed commands, and would hide the parts we want to build).

## Plan / План

Each step is a separate commit with tests green.

0. **Drop legacy compatibility code**: `migrate.rs` and `pigeonhole migrate`, legacy
   config aliases (`rate_per_sec`, `frame_*`, single-backend `chat_id` form), `serde`
   aliases/defaults for old checkpoint / superblock formats, Stage-B table renames.
1. **Commands, in-process.** Proto packages per layer; every mutation of `BlobDb`,
   S3 `Index`, `CasIndex` becomes `apply(Command)` in one transaction; callers build
   commands (ids chosen by the proposer). Namespaced tables, one SQLite file. No raft
   yet: `propose` applies directly.
2. **Single-node OpenRaft group.** `RaftLogStorage` on local disk, `RaftStateMachine`
   over the SQLite file, `Suite::test_all()` green. `propose` goes through
   `client_write`.
3. **Snapshots and export; remove the old durability.** Snapshot = SQLite file copy;
   export to dir and to blob. Delete `durability.rs` (journal, checkpoint, superblock
   pin), `s3/index` / `cas/index` roots. `pigeonhole restore` reads an export.
4. **Sweeper contract.** `BeginReclaim` → delete → `DropChunk`; reclaim-only mode after
   restore; reconciliation.
5. **In-flight pin registry** (own blob ids before upload) replacing put watermarks,
   sweep grace and `InflightParts`; read pins.
6. **Multi-node:** gRPC raft network, membership changes, leader forwarding, leader
   checks before side effects, shared send budget. Jepsen-style test with node kills.

## Open questions / Открытые вопросы

- Raft log storage: a simple segmented file of our own, or an embedded KV (redb /
  RocksDB)?
- Snapshot transfer to joining nodes: stream the SQLite file as-is, or a logical dump?
- Export to blob: whole SQLite file in chunks via the chunk store (needs a bootstrap
  pointer — the pinned message), or a dedicated format?
