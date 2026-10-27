# Sweep invariants / Инварианты уборки

## What is live / Что считается живым

A backend key (sort key of a blob) must **not** be deleted by the sweeper if any of:

1. **Committed chunk parts** — a part of a chunk in the tablet's `ChunkState` with `refs > 0` that is not reclaiming (looked up through its `(instance, sort_key)` index).
2. **In-flight uploads** — keys registered in process-local `InflightParts` (uploaded, not yet committed by `CommitChunk` / `AddReplica`).

After process restart, `InflightParts` is empty: uncommitted parts of a dead writer are garbage and may be reclaimed. Metadata itself lives in the command log, not in backend blobs, so there are no system blobs to protect.

## Per-batch liveness / Проверка на каждую пачку

The sweeper must **not** snapshot the full live-key set at the start of a pass. Between listing candidates and deleting a batch, another task may `commit_chunk`. For each batch, immediately before `delete`:

1. consult `InflightParts::contains` (snapshot, **first**);
2. `BlobDb::live_part_keys_among` (live chunks only).

Only keys absent from both are deleted.

The order matters. An `InflightGuard` is dropped only **after** its chunk is committed,
so a key that is not in flight at step 1 but is committed afterwards is visible at
step 2. Reading committed state first and `InflightParts` second loses exactly those
keys: the commit and the guard drop both land between the two reads, and a live part
is deleted.

`WatermarkBackend`’s per-`put` grace timeout only bounds a single upload; it does **not** replace `InflightParts` for the window from the first part of a chunk until commit.

The guard is registered only after the part's `put` returns, and that return waits for the put watermark to be recorded. Meanwhile a *later* key's watermark (stamped before its own record) can already make this key a sweep candidate. The sweep grace must therefore exceed upload latency plus watermark-record latency; the default 15 minutes does by a wide margin, a few milliseconds does not. (Step 5 of the roadmap replaces watermarks and grace with our own blob ids.)

## Chunk refcount lifecycle / Жизненный цикл чанка

```
live (refs ≥ 1)
  │ Release → refs = 0
  ▼
live (refs = 0)          ← still visible to Retain until BeginReclaim
  │ BeginReclaim: refs = 0 and not reclaiming → reclaiming
  ▼
reclaiming               ← Retain → ChunkGone (command rejected, nothing retained)
  │ delete backend parts, then DropChunk
  ▼
gone
```

- `Retain` only succeeds if every listed chunk is live with `refs > 0` (all-or-nothing).
- `Release` rejects a double release (`refs` would go below 0); it does not clamp.
- Chunks left `reclaiming` after a crash are finished on the next sweep pass.
- `BeginReclaim` is committed to the log before any part is deleted: `propose` returns only after the append, so a restart or failover never brings back a reference to deleted parts (a lossy restore from a snapshot export can).
- Orphans are judged by this incarnation's state, so right before each orphan delete batch the sweeper calls `confirm_active` (a log round trip); a fenced incarnation fails it and deletes nothing. The sweeper and repair loops end once the tablet stops. Residual window: a newer generation can claim between the confirmation and the delete; own blob ids (roadmap step 5) remove the dependence on this check.

## Metrics

- `pigeonhole_chunk_gone_total` — failed `retain` against reclaiming/missing chunks
- `pigeonhole_double_release_total` — `release` with `refs` already 0
