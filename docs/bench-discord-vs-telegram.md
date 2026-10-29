# Discord vs Telegram: one node, measured

One run on 2026-10-29 (code at 26b9e37), the same S3 workload against a
pigeonhole whose blobs, log and snapshots all live in one service's test
channels, and against a local disk for scale. Raw results:
[bench/2026-10-29/](bench/2026-10-29/) (`tables.md` has every number below,
the JSON files every counter).

## Summary

| | disk | Discord | Telegram |
|---|---|---|---|
| Small PUT, one client: latency p50 | 9 ms | 6.7 s | 3.3 s |
| Small PUT, one client: PUTs/s | 104 | 0.15 | 0.31 |
| Small PUT, 8 clients: PUTs/s | 133 | 0.30 | 0.30 |
| Large PUT (1–32 MiB, one at a time) | 101 MiB/s | 1.3 MiB/s | 2.7 MiB/s |
| Cold GET, whole objects | 371 MiB/s | 6.6 MiB/s | 5.6 MiB/s |
| 1 MiB ranged GET p50 | 2 ms | 111 ms | 115 ms |
| HEAD / LIST p50 | 1 ms | 1 ms | 1 ms |

- **Writes are bound by the metadata log, not by bandwidth.** Every PUT is
  one log entry, and a log entry is a chat message: Discord takes ≥ 6.7 s
  per entry, Telegram 3.3 s. With one client Telegram writes twice as fast.
- **Uploading bytes costs the same on both**, about 2.7–2.9 MiB/s per
  upload (16 MiB chunks, one stream).
- **Reads are comparable** (5.6–6.6 MiB/s whole, ~110 ms per 1 MiB range)
  and never touch the log.
- **With 8 clients both stop at 0.30 PUTs/s**, for different reasons:
  Discord on the log (entries are paced; proposals share them), Telegram on
  the blobs channel (every PUT is a message there too).
- Metadata reads (HEAD, LIST) answer from the tablet's memory: ~1 ms
  whatever the backend.

## Setup

- `scripts/bench.sh disk discord telegram` → per backend a fresh server from
  `scripts/metrics-demo.sh` with **no block cache** (`METRICS_DEMO_CACHE=off`)
  and no demo traffic; `scripts/bench-backends.py` drives it over S3 (boto3)
  and reads `/metrics` before and after each phase.
- Each service: blobs, log and snapshots in three channels of its own
  (`[meta] log` / `snapshot` in the service, as `.env`'s test channels);
  chunks of 16 MiB, codec zstd, payloads random (incompressible).
- Limits as shipped: Telegram `[telegram]` defaults (send 0.3/s per chat,
  burst 3); Discord `[discord]` defaults (send 1/s burst 5) and the thread
  pacer (burst 5, then one thread per ~6.7 s, ≤ 50 per 300 s).
- Phases are time-boxed (each until its operation count or time runs out),
  in this order, on one tablet: sequential 4 KiB PUTs (≤ 60, 180 s); PUT
  4×1, 3×8, 2×32 MiB (270 s); GET them back whole (150 s); six 1 MiB ranged
  GETs in a 32 MiB object (60 s); 4 KiB PUTs from 8 clients (180 s); 50
  HEAD + 50 LIST (30 s).
- Client and server on one macOS machine; the services over its internet
  connection (where their servers are was not measured).

## What each phase shows

### Small PUTs, one client

A 4 KiB PUT is one blob message and one log entry.

- **Discord, 6.7 s**: blob upload 0.56 s, then the log entry: append 6.1 s.
  An append is a message plus a thread (the link of the chain); the pacer
  allows one thread per ~6.7 s once its burst of 5 is spent, so the log
  runs at ~0.15 entries/s. Discord itself never answered 429.
- **Telegram, 3.3 s**: blob upload 0.75 s, log append 2.5 s, and 171 s of
  waits in our own limiter over the 180 s phase: each channel may send 0.3 per
  second (`send_rate_per_sec`, under Telegram's ~20 messages a minute per
  channel), and the log channel's bucket sets the pace. One FLOOD_WAIT (3 s).

### Large PUTs

The same 92 MiB in 11 chunks on both. Uploads take ~3 s per chunk on both
services (2.9 s Telegram, 3.1 s Discord): the bandwidth is the same. The
difference is the log entry after each object: 92 ms on Telegram (its bucket
refilled during the upload) against 3.8 s on Discord (the pacer). Hence
2.7 vs 1.3 MiB/s.

### Cold reads

With no cache every 1 MiB block is its own backend read, one after the
other: 92 reads for 92 MiB.

- Whole objects: Discord 6.6 MiB/s (149 ms to the first byte of each block),
  Telegram 5.6 MiB/s with 4 FLOOD_WAITs (6 s in all; without them it would
  be about as fast as Discord).
- 1 MiB ranges: ~110 ms on both, one request each.

`pigeonhole_instance_call_duration_seconds{op="get"}` reads 0 ms for
Telegram: its stream starts lazily, so the call returns before any request.
For Telegram reads look at the client's time or `limiter_wait` / throttles.

### Eight clients

| | Discord | Telegram |
|---|---|---|
| PUTs/s | 0.30 | 0.30 |
| p50 / p95 / max | 13 s / 119 s / 125 s | 23 s / 40 s / 47 s |
| log entries, proposals per entry | 21, 2.67 | 60, 1.02 |
| proposal: queue wait / whole | 15.4 s / 23.9 s | 1 ms / 94 ms |
| blob upload (mean) | 2.5 s | 25 s |

- **Discord** is log-bound and the batcher shows it: proposals queue behind
  the entry being written (15 s on average) and share entries (2.67 each).
  Entries came every ~9 s, slower than the pacer's 6.7 s, and p95 was 119 s:
  the 50-threads-per-300 s window was already mostly spent by the phases
  before, so the pacer held entries until the window moved.
- **Telegram** is blob-bound: eight clients share one blobs channel at 0.3
  sends/s, so an upload waits 25 s in our limiter (398 s of waits in the
  phase) and proposals reach the log one at a time (1.02 per entry). Three
  FLOOD_WAITs (13 s).
- Both land on ~0.3 PUTs/s: Telegram's blobs channel rate, and Discord's
  log rate (0.11 entries/s) times 2.67 proposals per entry.

## Update: the collection off the log's path

The 8-client Discord phase above spent ~5 s of every commit outside the
log append: the snapshot ticker fired in that phase, and the rebase after
it ran its collection inline in the log writer (on Discord: ~150 reads
and ~50 thread deletes at 1/s) for 112 s, every entry queued behind it.
Since `ChainLogStore::rebase` returns once the base is written (the
collection runs in the background) and snapshots on Discord are plain
messages (no paced thread), the same run gives
([raw](bench/2026-10-29-log-collect/discord.json)):

| Discord, 8 clients | before | after |
|---|---|---|
| PUTs in the phase | 56 | 114 |
| client p50 / p95 | 13 s / 119 s | 13 s / 20 s |
| commit / log append mean | 8.9 s / 3.8 s | 6.0 s / 5.8 s |
| proposals per entry | 2.67 | 3.56 |
| snapshot put | 13.4 s | 0.8 s |
| snapshot stored → log rebased | 112 s | 12 s |

The log is still the limit (an entry is a paced thread), but it no longer
stalls.

## Update: the Discord log on a lease

Since the lease (`[meta] lease`, 45 s by default) a Discord log entry is
a plain post; threads are only for the lease records. The same full run on
Discord, against the earlier ones and the first Telegram run (not rerun:
its code did not change for this) ([raw](bench/2026-10-29-lease/discord.json)):

| | Discord first | Discord, log-collect | **Discord, lease** | Telegram first |
|---|---|---|---|---|
| small PUT, 1 client: p50 | 6.7 s | 6.7 s | **0.95 s** | 3.3 s |
| small PUT, 1 client: PUTs/s | 0.15 | 0.15 | **0.93** ¹ | 0.31 |
| log append mean | 6.1 s | 5.7 s | **0.48 s** | 2.5 s |
| large PUT | 1.3 MiB/s | 1.4 MiB/s | **2.7 MiB/s** | 2.7 MiB/s |
| small PUT, 8 clients: PUTs/s | 0.30 | 0.60 | **1.02** | 0.30 |
| small PUT, 8 clients: p50 / p95 | 13 s / 119 s | 13 s / 20 s | **7.8 s / 11 s** | 23 s / 40 s |
| proposals per entry (8 clients) | 2.67 | 3.56 | 1.15 | 1.02 |
| cold GET | 6.6 MiB/s | 8.0 MiB/s | 6.7 MiB/s | 5.6 MiB/s |

¹ The phase stopped at its cap of 60 PUTs (in 65 s).

- **The log no longer limits Discord writes.** An append takes ~0.5 s
  instead of a paced thread's 6 s; one client writes 6× faster and is now
  faster than Telegram.
- **Large PUTs reach upload speed**: 2.7 MiB/s, as Telegram.
- **With 8 clients Discord is now blob-bound, like Telegram**: 190 PUTs in
  187 s is our `[discord]` send budget of the blobs channel (1 message a
  second); entries rarely wait, so batching drops to 1.15 proposals each.
  More blob channels (`[[shards]]`) would lift it, as on Telegram.
- Reads did not change (no log in them).

## Update: reading blocks in parallel does not help

A chunk store that read up to n blocks of a chunk at once (an experiment,
not kept: the reads go one block at a time). The same read phases with
n = 1 and n = 8, back to back ([raw](bench/2026-10-29-read-concurrency/)):

| cold GET, 92 MiB | n = 1 | n = 8 |
|---|---|---|
| Discord | 9.4 MiB/s, 0.10 s to a block's first byte | 6.8 MiB/s, 0.89 s |
| Telegram | 5.9 MiB/s, no FLOOD_WAIT | 4.8 MiB/s, 60 FLOOD_WAITs (88 s) |

- **Discord is bound by the link, not by latency**: with 8 reads at once
  each takes ~9× longer and the sum does not grow; 9.4 MiB/s one at a time
  is the link here. (The first run's 6.6 MiB/s had 149 ms to the first
  byte; this one 100 ms.)
- **Telegram floods parallel `upload.getFile`** well under our own limit
  (15/s): 60 waits in one phase.

So the code went: on these links reading ahead buys nothing, and on
Telegram it costs flood waits. Worth trying again for a store bound by
latency, not by the link, or once reads stream to the client (today a GET
reads its whole range before it answers). What stayed:
`instance_call_duration_seconds{op="get"}` times to the first byte, which
is what showed it.

## What would move the numbers

- **Telegram: more blob channels.** One chat's send budget is the ceiling
  for writes under load; `[[shards]]` over several blob chats multiplies it
  (step 7). The log channel then becomes the limit at 0.3 entries/s, where
  group commit starts to pay as it does on Discord.
- **Discord: fewer threads.** A log entry costs a paced thread; more blob
  channels would not help. Batching already helps under load; an entry
  that links less often than once per message would lift the ceiling
  (a design question for the chain, docs/discord-log.md).
- ~~**Both: reads in parallel.**~~ Tried and measured (below): not faster.
  With the cache on (the default), reads after a write are free (L2
  write-through).
- **Small objects and Telegram's text path**: blobs that fit a text
  message already go as text; they still cost a send each.

## Caveats

- One run, from one place; small samples for the large PUT and GET phases
  (9 objects). The services' latency varies by hour and by route.
- Phases share one tablet and one rate-limit state: Discord's thread window
  spent in early phases slowed the 8-client phase; Telegram's buckets carry
  over the same way. Running a phase alone would give it a fresh budget.
- Telegram ran on `[telegram]` defaults (0.3 sends/s per chat), which sit
  just under the service's ~20 messages a minute per channel; Discord on
  its new defaults and the pacer of 30e19f2.

## Running it again

```sh
brew install uv prometheus grafana
./scripts/bench.sh disk discord telegram   # ~35 min; BENCH_MINUTES scales the phases
cat .cache/bench/report.md
```

It needs the test channels and tokens in `.env`. The run leaves those
channels as it found them: Discord's are emptied whole, Telegram's between
two marker messages it posts (`bench_window` in
`crates/storage/pigeonhole-storage-telegram/tests/live.rs`). While it runs,
the dashboard is up at http://127.0.0.1:3000/d/pigeonhole/pigeonhole.
