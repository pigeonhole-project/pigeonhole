# Fault injection

Status: **implemented** (roadmap step 11), together with
[simulation.md](simulation.md); see Implementation for what was built and
what is left. Read [chain-log.md](chain-log.md)
(the `Chain` contract), [meta-backends.md](meta-backends.md) (the `LogStore`
contract) and [sweep-invariants.md](sweep-invariants.md) first.

## Why

Every guarantee pigeonhole makes rests on what a store does when it misbehaves:
a put that fails but lands, a read of a lost or damaged blob, a rate limit in
the middle of a write, a link decided while the caller timed out. Today the
tests cover such cases one at a time, by hand: a fake Telegram channel that
hides an id or loses an answer, a memory store with an `unavailable` flag, a
torn record written by hand. Live runs found the rest (a claim losing 600
times in a row, `confirm` taking our own append for a rival once in seven
`sweep_stress` runs). Fault injection makes these cases systematic: one
decorator any test can put under any store, faults drawn from a seed, and the
invariants checked the same way every time.

Non-goals: chaos in production (no config knob in the binary for now); a
deterministic scheduler for the whole process (see Open questions).

## Fault model

A fault is something a store **may** do under its contract, or something it
must not do but we want to see detected. The two are kept apart: the first
must never break an invariant, the second must be reported (an error, a
`Lost`), never served as data.

### Allowed by the contracts

| Fault | `BlobBackend` | `Chain` |
|---|---|---|
| **Delay** | any op takes up to `d` (a distribution, not a constant) | same |
| **Fail before** | the op errors, nothing happened (a refused connection) | `put_after` errors, nothing put |
| **Fail after** (ambiguous) | `put` errors but the blob is there; `delete` errors but the blobs are gone | `put_after` errors but the blob was linked and won (or lost) |
| **Land late** | `put` errors; the blob appears later, after the caller moved on (a dropped request that still went out) | `put_after` errors; the link is decided later — the deposed-writer case |
| **Rate limit** | errors with `RateLimited { retry_after }`, nothing applied | same |
| **Unavailable** | every op fails for a window | same |
| **Slow visibility** | a fresh blob's `get` fails for a few tries | — (the contract promises linearizable links) |
| **Cancellation** | the caller's future is dropped at a random await | same |

### Contract violations (must be detected)

| Fault | Expected outcome |
|---|---|
| **Lost blob**: a committed part or log blob is gone | read fails over to another replica; repair restores the copy; a lost log blob is `LogError::Lost`, never a shorter history |
| **Corrupted blob**: bytes flipped | the log's crc links catch it (`Lost`); for data, every `StoredBlock` carries the crc32 of its stored bytes: a read that finds a mismatch reads each replica alone, and fails only when every copy is bad; repair never copies a damaged block |
| **Torn disk record** (crash mid-append) | the record is dropped on open; nothing acknowledged is lost |
| **Duplicate put** | the same data twice under two keys: the second is an orphan the sweeper deletes |

## The decorator

A new test-only crate, `testing/pigeonhole-faults` (role `testkit`, never a
dependency of the binary):

```rust
/// `B` with faults injected from `plan`; every injected fault is recorded.
pub struct Faulty<B> { inner: B, plan: Arc<FaultPlan>, log: Arc<FaultLog> }

impl<B: BlobBackend> BlobBackend for Faulty<B> { /* draw, then delegate */ }
impl<B: Chain>       Chain       for Faulty<B> { /* incl. put_after_tail */ }

pub struct FaultPlan {
    seed: u64,
    /// Per operation kind (put, get, delete, list, put_after, tail, …):
    /// the probability of each allowed fault, and its parameters.
    rules: Vec<Rule>,
    /// Windows of unavailability, in operation counts or virtual time.
    outages: Vec<Outage>,
    /// Contract violations, off unless a test asks for them.
    violations: Violations,
}
```

- **Deterministic draws.** Each decision comes from `(seed, store id, op kind,
  per-kind counter)` through a counter-based generator, so the same seed gives
  the same fault to the n-th put of a store regardless of how tasks
  interleave. A failing test prints the seed and the fault log; rerunning with
  `PIGEONHOLE_FAULT_SEED` reproduces the faults (the interleaving may still
  differ, see Open questions).
- **"Fail after" and "land late"** delegate first, then report an error; for
  "land late" the delegate call runs in a spawned task after a delay, while
  the caller already got its error. On a `Chain`, a late `put_after` is
  decided when it runs, exactly like a straggler.
- **Lost / corrupted** act on blobs already put: the decorator remembers keys
  and, at a drawn moment, deletes one underneath or wraps its `get` to flip a
  byte. Only when the test enables violations.
- **Faults stop** at a `quiesce()` call: tests that check eventual properties
  (garbage reclaimed, replicas repaired) turn faults off, then wait.
- `FaultLog` keeps every injected fault with its op, key and time, for the
  failure message and for assertions ("a late put really happened").

Disk faults that cannot be expressed above the trait get helpers on the disk
store's tests: truncate the last segment at a random byte (a crash
mid-append), kill a writer between two operations (drop the handle while a
put is in flight).

## Where it runs

1. **The `LogStore` conformance suite** over `ChainLogStore<Faulty<Memory>>`
   and `<Faulty<Disk>>` with allowed faults: every case still holds, or fails
   with `Other` (outcome unknown), never with a wrong answer.
2. **A model check of the log.** Several handles (writers of rising
   generations and readers) run random operations — claim, append, confirm,
   rebase, discover, read — against one store through `Faulty`, many seeds.
   Recorded: every acknowledged append with its generation and position.
   Checked after each run:
   - every acknowledged entry is in every later history, at its position;
   - no two generations acknowledge different entries at one position;
   - a handle that got `Fenced { current }` never acknowledges again, and
     `current` is a generation that claimed;
   - `confirm` is `Ok` only if no higher claim was linked before it returned;
   - replay after `quiesce()` equals the longest acknowledged history plus at
     most the unacknowledged batches that won their link.
3. **The tablet** (`MetaStore`) over faulty log and snapshot stores: proposals
   either commit or fail with `Stop`; after a `Stop`, a new incarnation boots
   and holds every acknowledged command; snapshots that failed to store never
   become a base.
4. **`sweep_stress` under faults**: the blob store faulty (delays, ambiguous
   puts, late landing, rate limits, outages), optionally the metadata stores
   too. The invariants stay as they are — every listed object reads back whole,
   a GET never returns a torn body — with operation errors now allowed and
   counted. After `quiesce()` and a few sweeps: backend keys = committed live
   parts (the late-landing puts are garbage the sweeper reclaims).
5. **Replicas and repair**: two members, one with lost blobs (violations on);
   reads fail over, scrub enqueues, repair restores; then the lossy member's
   keys equal the healthy one's.

Each runs a fixed set of seeds in `cargo test --workspace` (fast, a few
seconds) and many more under `--ignored` (`PIGEONHOLE_FAULT_RUNS=1000`).

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; bugs the faults find are fixed in
commits of their own, each with the seed that found it as a regression case.

1. **`pigeonhole-faults`**: `Faulty<B>`, `FaultPlan`, `FaultLog`, counter-based
   draws; unit tests that each fault does what it says (late landing really
   lands, a lost blob is gone, the same seed draws the same faults).
2. **Log**: conformance under faults (memory, disk) and the model check; the
   disk crash helpers.
3. **Tablet**: `MetaStore` over faulty stores, incarnations after `Stop`.
4. **`sweep_stress` and repair** under faults.
5. **Docs**: what each store is tested against (meta-backends.md,
   sweep-invariants.md), how to reproduce a seed.

## Implementation

What was built differs from the plan above in a few places:

- The decorator lives in `testing/pigeonhole-sim` (`pigeonhole_sim::faults`),
  next to the scenarios, not in a crate of its own. `FaultPlan` is a set of
  probabilities (`none`, `mild`, `harsh`) rather than per-operation rules;
  draws come from `(seed, operation, its counter)`; `Faulty::faults()` is the
  fault log.
- Implemented faults: delay, fail before, fail after, land late, rate limit,
  and one violation, `corrupt` (a share of blobs, chosen by key, always reads
  back with a byte flipped).
- Determinism of the whole run comes from the simulator
  ([simulation.md](simulation.md)); the same tests also run on plain tokio in
  `cargo test --workspace`.
- Scenarios (`crates/testing/pigeonhole-sim/tests`): the log model check
  (memory and simulated disk, mild and harsh plans), a takeover under load, a
  power failure with a torn write, `sweep_stress` over S3 under faults with a
  restart, and the same with two replicas, one of them damaged.

Left for later: outages, slow visibility, cancellation, lost blobs; the
tablet over faulty log and snapshot stores; snapshots and restore under
crashes; a repair scenario (a lossy member restored to equal the healthy
one).

## Open questions

Settled: interleavings are fixed by the simulator; data blocks carry a crc;
faults stay test-only (no config knob in the binary); counts, not seconds
(virtual time makes delays reproducible anyway).

- **Determinism of interleavings.** Seeds fix the faults, not the schedule of
  a multi-threaded runtime. A current-thread runtime with paused time
  (`tokio::time::pause`) makes most tests deterministic; `sweep_stress` uses
  four workers. Accept "same faults, maybe another interleaving", or move the
  stress test to a simulator (madsim / turmoil) later?
- **Corrupted data blobs.** Chunk reads trust the backend's bytes (the chunk's
  `crc32` covers stored lengths and codecs, not data). Add a crc per block
  (in `StoredBlock`, a `VERSION` bump) so a damaged replica is detected and
  read around? The fault test would make that a requirement.
- **Faults in the binary**: a `[[store]] faults = "seed=…,put.fail_after=0.01"`
  for manual chaos runs against S3 clients, or keep it test-only?
- **Time-based faults** (outages in seconds) vs counts (after the n-th op):
  counts are reproducible, seconds are realistic; support both?
