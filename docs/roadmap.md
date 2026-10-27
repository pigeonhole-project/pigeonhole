# Roadmap / План работ

Working plan for the metadata redesign. Design rationale lives in
[meta-backends.md](meta-backends.md); GC rules in
[sweep-invariants.md](sweep-invariants.md); terms in [glossary.md](glossary.md).
Read those first.

## Ground rules / Правила

- Educational project: **no backward compatibility** — change formats, schemas
  and configs outright, no aliases or migrations.
- One logical change per commit; commit only with `cargo test --workspace` and
  `./scripts/check-deps.sh --strict` green. Commit freely, **ask before push**.
- `cargo test --workspace` runs `sweep_stress` once; run it repeatedly
  (`for i in $(seq 1 10); do cargo test -q -p pigeonhole --test sweep_stress || break; done`)
  at the end of a step or before a push, not after every commit (~50 s a run).
- After S3-visible changes also run `make compat-s3s-e2e` (STS failure is
  expected), `make compat-s3s-boto3`, `make compat-memory`.
- Layering is enforced: `meta` and `types`/`codec` depend on crates.io only;
  storage crates must not see chunk-store or gateways.

## Where we are / Текущее состояние

Done (on `main`), steps 1–3:

- `pigeonhole-meta` is the tablet. `MetaStore::propose::<S>(cmd)` queues a
  typed command; the batcher appends what is queued to the `LogStore` under the
  tablet's generation, applies it to `S`'s in-memory state, installs a snapshot
  if one is due, then answers with `Result<S::Reply, S::Reject>`. Reads:
  `MetaStore::read::<S>`. Boot = claim `1 + discover().gen` → newest snapshot →
  replay the tail → zero entry. `Fenced` / failed append / unappliable command →
  `Stop`: proposals fail, `serve` exits.
- Namespaces `ChunkState` (chunk-store), `S3State` (gateway-s3), `CasState`
  (bytestream): Rust command enums, serde state, `VERSION` covering both.
  Postcard everywhere, versioned (`LOG_FORMAT`, `VERSION`, `SNAPSHOT_FORMAT`);
  golden tests pin command encodings. No SQLite, no proto in metadata (proto
  only for REAPI).
- `LocalLogStore`: `seg-<first>.log` records `MAGIC|len|crc|Batch`, fsync per
  batch, `claimed_gen` + `log_id` under a `flock`; claim truncates a torn tail;
  snapshots drop the segments behind them.
- Snapshots every `[meta.snapshot] every_entries` (10000); `export = DIR` copies
  them; `pigeonhole restore [--from-export DIR] --lossy` seeds an empty log
  from the newest export.
- Removed: `durability.rs` (checkpoint, superblock pins, root journal,
  `start_or_restore`), `s3/index` / `cas/index` snapshots, chunk roots.
  `purge` deletes through commands and releases object chunks.
- Node-local progress (put watermarks, sweep cursor, repair queue) is process
  memory in `BlobDb`, not part of the replicated state.

Known gaps:

- **Disk loss loses metadata** unless a snapshot export lives on another disk,
  and restore from an export is lossy. Fixed by an off-host LogStore (step 6).
- fsync per log batch: `sweep_stress` ~50 s on macOS (`F_FULLFSYNC`) vs ~7 s
  without; plain `fsync` (what SQLite does on macOS) is an option.
- Sweeper protection for fresh parts still relies on put watermarks + sweep
  grace; `sweep_stress` uses `SWEEP_GRACE = 1s` and 400 ops/worker to stay green.
  Mid-read reclaim of a chunk (GET racing overwrite/delete) is still possible.
- `pinned_message` semantics ("most recent by sending date") is from the docs and
  not yet verified on a live bot.

## Steps / Шаги

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

### Step 5 — In-flight pin registry (own blob ids)

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

### Step 6 — `telegram` LogStore

Protocol: meta-backends.md § `telegram` (pinned head of a chain of log batches,
`description` as the generation fence, stragglers unpinned).

1. First verify on a live bot (throwaway channel): `pinned_message` is the
   newest-by-sending-date pinned message; limits of `setChatDescription`.
2. Implement over the Telegram client; a fake chat (pins with send order,
   description register) for tests.
3. Run the `LogStore` test suite from step 2 against it, plus fault injection:
   two concurrent incarnations, a straggler pinned after a claim, racing
   claimants. Settle the open Telegram questions in meta-backends.md first
   (description read after pin; straggler as the new chain's base).

### Step 7 — Arbiter, second node, `s3` LogStore

Bootstrapper-like watcher that restarts the tablet elsewhere; write forwarding
to the active node; `s3` LogStore with conditional writes (`If-None-Match`).

## Open questions / Открытые вопросы

- Default batch window per LogStore kind (`local`: 0, no extra wait).
- Plain `fsync` instead of `F_FULLFSYNC` for the local log on macOS.
- A converter for old logs / snapshots when a `VERSION` changes, if ever needed.
