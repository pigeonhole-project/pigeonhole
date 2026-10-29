# Deterministic simulation (madsim)

Status: **proposal**. Read [fault-injection.md](fault-injection.md) first: this
design takes its fault model and decorator and makes the rest of the run —
task order, time, randomness, crashes — reproducible from one seed too.

## Why

Fault injection with a seed fixes *which* faults happen, not *when* relative
to everything else. The bugs that cost the most so far were interleavings: a
claim that lost the race to a busy writer 600 times in a row, `confirm`
taking our own in-flight append for a rival once in seven `sweep_stress`
runs. On a multi-threaded tokio runtime such a bug shows up now and then and
does not come back on demand. In a deterministic simulation one seed
determines the whole run: the order tasks run in, every timer, every random
draw, every injected fault, every crash. A failing seed fails every time, can
be stepped through, and becomes a regression test. Time is virtual, so a run
with minute-long timeouts and backoffs takes milliseconds; thousands of seeds
run in a CI job.

The value grows with Raft between nodes (partitions, restarts, elections),
but the single node already has the races above: the tablet's batcher beside
`confirm`, the sweeper beside uploads and reads, a second process taking the
log over.

## madsim in short

[madsim](https://github.com/madsim-rs/madsim) (0.2.34, Apache-2.0; used by
RisingWave) is an async runtime that, built with `RUSTFLAGS="--cfg madsim"`,
runs every task on one thread in an order drawn from a seed, with virtual
time, a simulated network and file system, and nodes that can be killed,
paused and restarted. Built without the flag, its shim crates are the
originals.

- `tokio = { version = "0.2", package = "madsim-tokio" }`: tokio normally, the
  simulator under `cfg(madsim)`. Shims exist for tonic, etcd, rdkafka,
  aws-sdk-s3; `[patch.crates-io]` forks of getrandom, quanta, tokio-stream,
  tokio-retry make their randomness and clocks the simulator's.
- `#[madsim::test]` runs a test under a seed; `MADSIM_TEST_SEED`,
  `MADSIM_TEST_NUM` (how many seeds), `MADSIM_TEST_JOBS`,
  `MADSIM_TEST_CHECK_DETERMINISM` (run each seed twice, compare),
  `MADSIM_TEST_TIME_LIMIT`.
- `runtime::Handle`: `create_node()` (a "process": its own tasks, IP, file
  system), `kill(id)` (drops every task of the node at once — a crash),
  `restart(id)`, `pause` / `resume`.
- `fs`: per-node files (`open`, `create`, `read_at`, `write_all_at`,
  `set_len`, `sync_all`, `metadata`); `FsSim::power_fail(node)` loses what
  was not synced. No directory listing, no `flock`, no hole punching.
- `net`: simulated TCP/UDP with latency, loss, clogged links; `buggify`:
  fault points in the code, enabled per run.
- The README's rule: *all I/O-related interfaces must be mocked during the
  simulation, and all uncertainties should be eliminated.*

## What runs in the simulation

Everything below the S3 / CAS API, in process, with the stores we already
fake; nothing that talks to the outside.

| Part | In the simulation |
|---|---|
| `pigeonhole-meta` (tablet, `ChainLogStore`, snapshots) | as is, on madsim-tokio |
| openraft | through its `AsyncRuntime` type parameter: a `MadsimRuntime` (spawn, sleep, timeout, instant); see the spike |
| `pigeonhole-chunk-store` (ingest, sweeper, repair), `pigeonhole-blob` | as is |
| `pigeonhole-gateway-s3`, `-bytestream` | their service methods called directly, as the tests do now (no HTTP) |
| memory store | as is (a mutex) |
| disk store | over a simulated disk (below), not the host file system |
| Telegram, Discord | their fakes under the real store code (`FakeChannel`, the fake Discord client); grammers and reqwest are not built |
| `Faulty<B>` (fault-injection.md) | draws from the simulator's random source |
| HTTP server, binary, live clients | not built under `cfg(madsim)` |

A **process** is a madsim node. A takeover test starts two nodes on one store
(the store is shared state outside both, as a disk or a channel is); a crash
is `kill(node)`, a power failure `kill` plus dropping unsynced disk writes, a
restart boots a new tablet on the node.

### The simulated disk

`DiskBlobStore` uses what madsim's `fs` lacks (`read_dir`, `flock`,
`remove_file`, hole punching) and calls blocking I/O on `spawn_blocking`. The
store's I/O goes behind a small trait, `SegmentIo` (open / create / list
segments, `read_at`, `write_all_at`, `sync`, `set_len`, `punch`, `remove`,
`lock`), with two implementations:

- `StdIo`: today's code (`std::fs`, `flock`, `F_PUNCHHOLE`), in
  `spawn_blocking`;
- `SimIo`: files in memory shared by every node (a "disk" in a test), each
  write first volatile and durable only after `sync`; `crash()` drops
  volatile bytes, optionally keeping a prefix of the last write (a torn
  record); `lock` is an async mutex held per node, released when the node is
  killed.

The segment logic, the chain and the scan stay one code path; the tests that
crash mid-append no longer need hand-written truncation.

### Time, randomness, ids

Everything nondeterministic must come from the simulator:

- `tokio::time` → madsim-tokio; openraft through `MadsimRuntime`.
- `uuid::Uuid::new_v4()` (`BlobId`, `LogId`), `rand`, and `Utc::now()` in
  commands (`created_at`, `mtime`): madsim's getrandom fork makes `uuid` and
  `rand` deterministic; wall-clock time in commands goes through one
  `pigeonhole-types::clock::now()` (system time normally, madsim's virtual
  time under `cfg(madsim)`). The spike checks what else reads the clock or
  entropy (`MADSIM_TEST_CHECK_DETERMINISM` finds it).
- `HashMap` iteration order depends on a random seed per map: maps whose
  order can reach a decision (not just a lookup) become `BTreeMap`; the
  determinism check finds the rest.
- `tokio::select!` in openraft picks a ready branch at random through tokio's
  own generator: whether madsim-tokio's re-export covers it for a crate that
  depends on plain `tokio` is a spike question (else a `[patch.crates-io]`
  of tokio with madsim's fork, as RisingWave does).

## Scenarios

Each is a `#[madsim::test]`: a fixed set of seeds in `cargo test` under
`cfg(madsim)`, thousands in a nightly job, a failing seed printed and
replayed with `MADSIM_TEST_SEED`.

1. **Log model check** (fault-injection.md § model check) with processes:
   writers of rising generations on nodes, killed and restarted at random,
   readers, over the memory, disk (`SimIo`, power failures) and fake-Telegram
   chains; the same invariants (no acknowledged entry lost, no two
   generations acknowledge one position, a fenced writer never acknowledges
   again).
2. **Takeover under load**: node A writes steadily, node B boots on the same
   log at a random moment, A is killed or keeps running; B holds every
   command A acknowledged, A stops `Fenced` — the race that took 600 tries
   and the `confirm` race, as regression seeds.
3. **sweep_stress in simulation**: the workers, the sweeper and the repair
   loop on one node, faults from `Faulty`, a crash and reboot in the middle;
   afterwards every listed object reads back whole and backend keys equal
   committed live parts.
4. **Snapshots and restore**: crashes while a snapshot is stored, while the
   log is rebased, after the old snapshot is removed; boot always finds the
   snapshot its log names; `restore --lossy` after a lost log.
5. Later, **Raft between nodes** over madsim's network: partitions, a slow
   node's store, leader changes (the step that motivates the most).

## Build and CI

- The workspace's `tokio` becomes `madsim-tokio` (one line in
  `[workspace.dependencies]`, identical to tokio without the flag).
- Crates that cannot build under `cfg(madsim)` (the binary's HTTP serving,
  grammers, reqwest-based clients, the live tests) are gated with
  `#[cfg(not(madsim))]` or left out of the simulation job's package list.
- A new test crate `testing/pigeonhole-sim` holds the scenarios and
  `SimIo`-backed helpers; `cargo test` runs it normally too where a scenario
  makes sense without the simulator (it then simply is a test on tokio).
- CI: a job `RUSTFLAGS="--cfg madsim" cargo test -p pigeonhole-sim` with
  `MADSIM_TEST_NUM=50` and `MADSIM_TEST_CHECK_DETERMINISM=1` on a fixed seed;
  a nightly with thousands of seeds. Locally the same command reproduces a
  seed. Separate `target` directory (`CARGO_TARGET_DIR=target/madsim`): the
  cfg flag rebuilds everything.

## Steps

0. **Spike** (throwaway, a day): build `pigeonhole-meta`, `-blob`,
   `-chunk-store`, `-gateway-s3` with `--cfg madsim` and madsim-tokio; a
   `MadsimRuntime` for openraft; one tablet test under `#[madsim::test]` with
   `MADSIM_TEST_CHECK_DETERMINISM`. Record what breaks (crates on plain tokio,
   `select!`, clocks, entropy, `spawn_blocking`, macOS) and decide.
1. **madsim-tokio** for the workspace, the cfg gates, the CI job compiling
   and running one scenario.
2. **`SegmentIo`**: the disk store over `StdIo` (no behaviour change), then
   `SimIo` with crashes and torn writes.
3. **Determinism**: the clock facade, maps whose order matters, until the
   determinism check passes on every scenario.
4. **Scenarios 1–4**, with the faults of step 11 drawing from the simulator.
5. **Docs**: how to run and replay a seed (README § Tests), what each
   scenario checks.

## Open questions

- **openraft and `select!`**: a `MadsimRuntime` covers spawn and time;
  openraft's own `tokio::select!` and `tokio::sync` are linked from plain
  tokio. Is madsim's tokio fork via `[patch.crates-io]` needed, and does it
  keep the normal build identical?
- **macOS**: does the simulation build and run on macOS as on Linux, or is it
  a Linux-only CI job?
- **Maintenance**: every new dependency that does I/O must have a sim story;
  is a CI job that fails on such a dependency enough?
- **turmoil instead** for the multi-node network later? It simulates hosts
  and links over ordinary tokio (lighter, less deterministic); madsim already
  covers the network, so one simulator is simpler.
- **Overlap with fault-injection.md**: the decorator stays useful outside the
  simulator (fast tests on tokio); under `cfg(madsim)` its draws and delays
  come from the simulator, and process crashes move from hand-made helpers to
  `kill(node)`.
