# Sweep invariants / Инварианты уборки

## What is live / Что считается живым

A backend key (sort key of a blob) must **not** be deleted by the sweeper if any of:

1. **Committed chunk parts** — row in `chunk_parts` for a chunk with `refs > 0` and `state = 'live'`.
2. **Durability system blobs** — checkpoint / journal segment locators and the pin message for the instance (from the current superblock).
3. **In-flight uploads** — keys registered in process-local `InflightParts` (uploaded, not yet durable in `chunk_parts` or the published superblock).

After process restart, `InflightParts` is empty: uncommitted parts of a dead writer are garbage and may be reclaimed.

## Per-batch liveness / Проверка на каждую пачку

The sweeper must **not** snapshot the full live-key set at the start of a pass. Between listing candidates and deleting a batch, another task may `commit_chunk` or publish a superblock. For each batch, immediately before `delete`:

1. consult `InflightParts::contains` (snapshot, **first**);
2. `SELECT sort_key FROM chunk_parts … WHERE sort_key IN (…)` (live chunks only);
3. refresh durability `system_keys`.

Only keys absent from all three sets are deleted.

The order matters. An `InflightGuard` is dropped only **after** its chunk is committed
(or its segment is in the published superblock), so a key that is not in flight at
step 1 but is committed afterwards is visible at steps 2–3. Querying the database
first and `InflightParts` second loses exactly those keys: the commit and the guard
drop both land between the two reads, and a live part is deleted.

`WatermarkBackend`’s per-`put` grace timeout only bounds a single upload; it does **not** replace `InflightParts` for the window from the first part of a chunk until commit.

The guard is registered only after the part's `put` returns, and that return waits for the put watermark to be written to SQLite. Meanwhile a *later* key's watermark (stamped before its own write waited on the lock) can already make this key a sweep candidate. The sweep grace must therefore exceed upload latency plus watermark-write latency; the default 15 minutes does by a wide margin, a few milliseconds does not.

## Chunk refcount lifecycle / Жизненный цикл чанка

```
live (refs ≥ 1)
  │ release → refs = 0
  ▼
live (refs = 0)          ← still visible to retain until CAS
  │ try_begin_reclaim: UPDATE … SET state='reclaiming'
  │   WHERE refs=0 AND state='live'
  ▼
reclaiming               ← retain → ChunkGone (transaction rolls back)
  │ delete backend parts, then DELETE metadata
  ▼
gone
```

- `retain` only succeeds for `state = 'live' AND refs > 0` (all ids in one transaction).
- `release` errors on double-release (`refs` already 0); it does not clamp with `MAX(refs-1, 0)`.
- Chunks left in `reclaiming` after a crash are finished on the next sweep pass.

## Metrics

- `pigeonhole_sweep_inflight_protected_total` — candidates skipped due to `InflightParts`
- `pigeonhole_chunk_gone_total` — failed `retain` against reclaiming/missing chunks
- `pigeonhole_double_release_total` — `release` with `refs` already 0
