# Roadmap

Working plan for the metadata redesign. Design rationale lives in
[meta-backends.md](meta-backends.md); GC rules in
[sweep-invariants.md](sweep-invariants.md); terms in [glossary.md](glossary.md).
Read those first.

## Ground rules

- Educational project: **no backward compatibility** — change formats, schemas
  and configs outright, no aliases or migrations.
- One logical change per commit; commit only with `cargo test --workspace` and
  `./scripts/check-deps.sh --strict` green. Commit freely, **ask before push**.
- `cargo test --workspace` runs `sweep_stress` once; run it repeatedly
  (`for i in $(seq 1 10); do cargo test -q -p pigeonhole --test sweep_stress || break; done`)
  at the end of a step or before a push, not after every commit (~50 s a run).
- After S3-visible changes also run `make compat-s3s-e2e` (STS failure is
  expected), `make compat-s3s-boto3`, `make compat-memory`.
- Docs, comments and commit messages are in English only.
- Layering is enforced: `meta` and `types`/`codec` depend on crates.io only;
  storage crates must not see chunk-store or gateways.

## Where we are

Done (on `main`), steps 1–6a:

- `pigeonhole-meta` is the tablet. `MetaStore::propose::<S>(cmd)` queues a
  typed command; the batcher appends what is queued to the `LogStore` under the
  tablet's generation, applies it to `S`'s in-memory state, takes a snapshot
  if one is due, then answers with `Result<S::Reply, S::Reject>`. Reads:
  `MetaStore::read::<S>`. Boot = claim `1 + discover().gen` → the base snapshot
  → replay the tail → zero entry. `Fenced` / failed append / unappliable command →
  `Stop`: proposals fail, `serve` exits.
- Namespaces `ChunkState` (chunk-store), `S3State` (gateway-s3), `CasState`
  (bytestream): Rust command enums, serde state, `VERSION` covering both.
  Postcard everywhere, versioned (`LOG_FORMAT`, `VERSION`, `SNAPSHOT_FORMAT`);
  golden tests pin command encodings. No SQLite, no proto in metadata (proto
  only for REAPI).
- `LocalLogStore`: `seg-<first>.log` records `MAGIC|len|crc|Batch`, fsync per
  batch, `claimed_gen` + `log_id` under a `flock`; claim truncates a torn tail;
  `rebase` drops the segments behind the base. `ChatLogStore` keeps the log in
  a Telegram channel.
- Snapshots every `[meta] snapshot_every_entries` (10000) and
  `snapshot_every_secs` (600, when anything changed), stored in a `SnapshotStore` configured apart
  from the log (`local` | `telegram`); the log records which one it starts
  from. `pigeonhole restore --lossy` starts an empty log from the newest one.
- Removed: `durability.rs` (checkpoint, superblock pins, root journal,
  `start_or_restore`), `s3/index` / `cas/index` snapshots, chunk roots.
  `purge` deletes through commands and releases object chunks.
- Node-local progress (sweep cursor, repair queue, read pins) is process
  memory in `BlobDb`, not part of the replicated state.
- Sweeper (sweep-invariants.md): uploads pinned before they start
  (`UploadPins`), orphans swept up to the pins' horizon, our own `BlobId` as
  the uploaded file name (Discord sweeps only ours), read pins hold reclaim,
  `confirm_active` before orphan deletes; no watermarks, no grace.

Known gaps:

- With the `local` log, disk loss loses metadata unless `[meta] snapshot`
  lives elsewhere (and restore from it is lossy); the `telegram` log avoids it.
- Every Telegram pin leaves a "pinned message" service message; nothing
  deletes them (one per log batch, one per stored snapshot).
- fsync per log batch: `sweep_stress` ~50 s on macOS (`F_FULLFSYNC`) vs ~7 s
  without; plain `fsync` (what SQLite does on macOS) is an option.
- Telegram blob chats are swept by id up to the horizon, ours or not: keep
  nothing else in them (the Telegram LogStore needs its own chat).

## Steps

### Step 2 — Tablet over a local `LogStore` ✅ done

`LogStore` contract and `LocalLogStore`; `MetaStore` as a tablet with group
commit; boot = claim → recover → zero entry; `Fenced` stops it; `[meta]` /
`[meta.log]` config. Tests: replay rebuilds identical state, a second
incarnation fences the first without losing acknowledged entries, a crash
between append and apply is applied once on boot.

### Step 3 — Snapshots; remove the old durability ✅ done

State moved from SQLite into typed in-memory `StateMachine`s (after a look at
etcd, ZooKeeper, KRaft, rqlite, hiqlite: small metadata is usually kept in
memory with logical snapshots); commands and log envelope moved from proto to
versioned postcard; tablet snapshots + collect; export and lossy restore; the
old durability, index snapshots and roots deleted.

### Step 4 — Sweeper contract on the log ✅ done

1. ~~Reclaim: propose `BeginReclaim`, await its commit (append), delete parts,
   propose `DropChunk`.~~ Holds already: `propose` returns after the append.
2. ~~Sweeper / repair run only while the tablet is active; stop on `Fenced`.~~
   `MetaStore::confirm_active` (an empty batch through the batcher) before
   every orphan delete batch; the loops end once the tablet stops.
3. ~~Update sweep-invariants.md.~~

### Step 5 — In-flight pin registry (own blob ids) ✅ done

Done as planned, with one change: Telegram orphans are bounded by the
upload horizon (lowest floor of a live pin) rather than by "ids returned
before the earliest in-flight upload started" — the same idea, kept per
instance in `UploadPins`. Found on the way: the orphan sweep also deleted
parts of released (refs = 0) chunks, which read pins cannot protect; it now
leaves every chunk-owned key to reclaim.


1. Assign our own blob id before every upload; pin it in a process-local
   registry until the commit that references it is applied (or the upload
   fails); reads pin the parts they read.
2. Store our id with the locator in chunk parts; put it in the uploaded
   message (file name / caption) for Discord listing and debugging.
3. Orphan sweep: Telegram — message ids `≤ K` where `K` = largest id returned
   before the earliest in-flight upload started; Discord — by our id from the
   listing. Remove put watermarks, sweep grace, `InflightParts`; restore the
   stress test to grace-free settings and make mid-stress GETs
   strict again.

### Step 6 — `telegram` LogStore ✅ done

`ChatLogStore` (protocol over a `ChatApi` trait, in `pigeonhole-meta`) plus a
Telegram adapter in the binary; `[meta.log] kind = "telegram"`. Changed from
the plan after the live-bot check: stragglers are never unpinned (overridden
by a newer pin), and a claim waits a straggler window before reading the head.


Protocol: meta-backends.md § `telegram` (pinned head of a chain of log batches,
`description` as the generation fence, stragglers unpinned).

1. ~~Verify on a live bot.~~ Done 2026-10-28 (meta-backends.md § live-bot
   findings): newest-by-sending-date holds; unpinning the head hides it for
   ~1 min, so stragglers are overridden by a newer pin, never unpinned;
   `setChatDescription` is read-after-write, ~6 changes then a 43 s flood wait.
2. Implement over the Telegram client; a fake chat (pins with send order,
   description register) for tests.
3. Run the `LogStore` test suite from step 2 against it, plus fault injection:
   two concurrent incarnations, a straggler pinned after a claim, racing
   claimants. Settle the open Telegram questions in meta-backends.md first
   (description read after pin; straggler as the new chain's base).
4. The log chat must not be a blob chat (the sweeper deletes every id up to
   its horizon there): refuse such a config.

### Step 6a — Snapshots in a store of their own ✅ done

`SnapshotStore` (`local` | `telegram`, contract tests) next to the
`LogStore`, configured independently (`[meta] snapshot`). The log records its
base (`rebase(gen, { at, snapshot ref })`), so boot loads exactly that snapshot
and a deposed writer's late put cannot confuse it; the tablet removes the
previous base. `every_secs` snapshots an idle tablet. When snapshots outlive
the log, reclaim deletes parts only after the base shows the chunk reclaiming,
so `restore --lossy` loses recent changes but never breaks an object.

### Step 6b — Telegram housekeeping

- Delete the "pinned message" service messages: in a dedicated chat, delete the
  id range behind the base, not only the chain (check on a live bot that a
  range delete is not refused as a whole by one undeletable message).
- Remove the leftover pinning from the Telegram blob store
  (`storage-telegram/src/store.rs`, from the old snapshot manifest) and the pin
  right it still asks for.

### Step 6c — Stores and groups in the config ✅ done

`[[store]]` (local_disk | memory | telegram | discord) says only where bytes live;
`[[replicas]]` combines blob stores; `[meta] log` / `snapshot` and
`[chunk] store` name a store or group. A chat serves one purpose.

### Step 7 — Blob shards ✅ done

`[[shards]]`: each chunk goes to one member (least loaded), a member being a
store or a `[[replicas]]` group; `[chunk] store` may name it. Shards add
throughput (Telegram limits are per chat), replicas inside a shard add
availability. Repair brings a chunk to the members of its own shard, not of
every store. Metadata already records replicas per store. Later: snapshots on
groups (replicas, then shards that split a snapshot).

### Step 7a — Every kind for every purpose ✅ done (except discord metadata)

Blobs on `local_disk` (`pigeonhole-storage-disk`, a file per blob); the log
and snapshots in `memory` (tests, no fsync). Discord as a log / snapshot store
is left for later: its limits (topic edits, at most 50 pins per channel) need
checking on a live bot before the chat protocol is adapted to it.

### Step 8 — Raft between nodes

Design: meta-backends.md § Raft between nodes. Sub-steps:

1. ~~Node storage contract~~ Done: entries carry their term, `truncate`
   (a replacing batch), `save_vote`, `discover` returns the head term and the
   vote; the conformance suite covers them for local_disk, memory, telegram.
2. ~~`MetaStore` over openraft as a cluster of one.~~ Done: `raft.rs` adapts
   the node's LogStore / SnapshotStore to openraft 0.9; `MetaStore` keeps its
   API and batches concurrent proposals into one Raft entry (openraft 0.9
   flushes one entry per `client_write`, see its PR #2163); the log tail stays
   in memory, since openraft reads every entry it applies back from the log
   (sweep_stress 37 s, as before Raft).
3. gRPC network, in-process multi-node tests (failover, partitions, a node on
   a fake chat), leader-only side effects, forwarding.
4. `[[node]]` config, `init-cluster`, live runs with a node on Telegram.
5. Later: follower reads, auth between nodes.

## Open questions

- Default batch window per LogStore kind (`local`: 0, no extra wait).
- Plain `fsync` instead of `F_FULLFSYNC` for the local log on macOS.
- A converter for old logs / snapshots when a `VERSION` changes, if ever needed.
