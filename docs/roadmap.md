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
- Run the stress test several times after touching ingest / sweep / metadata:
  `for i in $(seq 1 10); do cargo test -q -p pigeonhole --test sweep_stress || break; done`.
- After S3-visible changes also run `make compat-s3s-e2e` (STS failure is
  expected), `make compat-s3s-boto3`, `make compat-memory`.
- Layering is enforced: `meta` and `types`/`codec` depend on crates.io only;
  storage crates must not see chunk-store or gateways.

## Where we are / Текущее состояние

Done (on `main`):

- `pigeonhole-meta` (`crates/meta`): `MetaStore::propose(namespace, bytes)`
  applies a layer command via its `Namespace` in one `BEGIN IMMEDIATE`
  transaction (savepoint; `Applied::Reject` rolls back but consumes an index);
  `applied_index` is stored with the state.
- Every metadata change is a proto command:
  `pigeonhole.chunk.v1` (chunk-store, `commands.rs`), `pigeonhole.s3.v1`
  (gateway-s3, `commands.rs`), `pigeonhole.cas.v1` (bytestream, `commands.rs`).
  Proposers (`BlobDb`, `Index`, `CasIndex`) choose ids and timestamps; `apply`
  is deterministic.
- One SQLite file for all layers (`database_url`), tables prefixed `chunk_`,
  `s3_`, `cas_`; the binary opens one `MetaStore` in `runtime.rs`.
- Fixed earlier: sweeper in-flight check order, `BEGIN IMMEDIATE`, copy
  retain-before-publish, ingest leaks, CAS double refs, chunk id reuse
  (`chunk_id_seq`), legacy code removed, config is `[[instances]]`-only.

Still old / known gaps:

- Durability is still `chunk-store/src/durability.rs`: periodic checkpoint of
  chunk tables + superblock pinned via `TypedBootstrapPointer::swap`; its
  journal carries only `SetRoot`. S3 / CAS indexes are snapshotted separately as
  roots `s3/index`, `cas/index` (`gateway-s3/src/snapshot.rs`,
  `bytestream/src/cas_snapshot.rs`). After disk loss chunks committed since the
  last checkpoint are lost and a journaled root may point at a missing chunk.
- Node-local tables written directly (not commands): `chunk_put_watermarks`,
  `chunk_sweep_cursor`, `chunk_repair_queue`; snapshot `s3_meta` / `cas_meta`,
  `import_snapshot`, `wipe_all`, checkpoint import/export.
- Sweeper protection for fresh parts still relies on put watermarks + sweep
  grace; `sweep_stress` uses `SWEEP_GRACE = 1s` and 400 ops/worker to stay green.
  Mid-read reclaim of a chunk (GET racing overwrite/delete) is still possible.
- `pinned_message` semantics ("most recent by sending date") is from the docs and
  not yet verified on a live bot.

## Steps / Шаги

### Step 2 — Tablet over a local `LogStore`

Goal: the command log, not the SQLite file, is the source of truth; boot =
claim → recover → zero entry.

1. In `pigeonhole-meta`: `Entry { namespace, command }` proto envelope; `LogStore`
   trait exactly as in meta-backends.md (`claim`, `append`, `discover`, `read`,
   `install_snapshot`); errors `Fenced`, `Lost`.
2. `LocalLogStore`: segmented append-only files in a directory, fsync per batch,
   `claimed_gen` file; `append` rejects stale generations under a lock.
3. `MetaStore` becomes the tablet: `propose` enqueues; a batcher appends a batch
   (`batch_max_delay_ms`, size cap), then applies entries in order and completes
   callers. Persist `last_applied_position` in the SQLite file.
4. Boot: `claim(gen)` → if SQLite is behind, `discover` + `read` + apply → zero
   entry → active. `Fenced` → stop accepting proposals, report to the binary.
5. Config `[meta]` + `[meta.log]` (`kind = "local"`, `path`), wired in
   `runtime.rs`.

Tests: replay of the log into an empty file yields identical tables (dump and
compare); a second `MetaStore` on the same log fences the first (first gets
`Fenced`, no acknowledged entry is lost); crash between append and apply
re-applies on boot exactly once.

### Step 3 — Snapshots; remove the old durability

1. `install_snapshot`: SQLite file image (consistent copy via `VACUUM INTO`) +
   collect log segments ≤ snapshot position. Periodic, by entry count.
2. Optional snapshot export (`[meta.snapshot] export`) — to a directory first.
3. Delete `durability.rs` (checkpoint, superblock, journal, `commit_root`,
   `start_or_restore`), the `s3/index` / `cas/index` snapshot modules, `s3_meta`
   / `cas_meta` tables, `set_meta`, `import_snapshot`, `wipe_all` rewrite as
   commands if still needed by purge.
4. `pigeonhole restore` = discover + replay from the configured LogStore /
   export. Update `e2e_memory_restore` and `sweep_stress` (restore part).
5. README "Durability and restore" rewritten.

### Step 4 — Sweeper contract on the log

1. Reclaim: propose `BeginReclaim`, await its commit (append), delete parts,
   propose `DropChunk`.
2. Sweeper / repair run only while the tablet is active; stop on `Fenced`.
3. Update sweep-invariants.md.

### Step 5 — In-flight pin registry (own blob ids)

1. Assign our own blob id before every upload; pin it in a process-local
   registry until the commit that references it is applied (or the upload
   fails); reads pin the parts they read.
2. Store our id with the locator in chunk parts; put it in the uploaded
   message (file name / caption) for Discord listing and debugging.
3. Orphan sweep: Telegram — message ids `≤ K` where `K` = largest id returned
   before the earliest in-flight upload started; Discord — by our id from the
   listing. Remove put watermarks, sweep grace, `InflightParts`, the watermark
   table; restore the stress test to grace-free settings and make mid-stress GETs
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
   claimants.

### Step 7 — Arbiter, second node, `s3` LogStore

Bootstrapper-like watcher that restarts the tablet elsewhere; write forwarding
to the active node; `s3` LogStore with conditional writes (`If-None-Match`).

## Open questions / Открытые вопросы

- Snapshot format: SQLite image vs logical dump per namespace.
- Default batch window per LogStore kind.
- Whether node-local tables (watermarks, cursor, repair queue) should move out of
  the replicated SQLite file entirely.
