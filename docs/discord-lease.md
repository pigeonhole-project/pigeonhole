# A lease for the Discord log: appends without a thread each

Status: **implemented** for Discord logs (`[meta] lease_secs`, default 45). See § Implementation notes for where the code differs from the design.
Read [discord-log.md](discord-log.md) (the thread fence this changes),
[chain-log.md](chain-log.md) and
[bench-discord-vs-telegram.md](bench-discord-vs-telegram.md) first.

## Why

The fence exists so that one writer holds the log and learns it before it
acknowledges anything. Without it, two writers (a restarted process while the
old one hangs, two configs on one channel, a request that lands after a
failover) both acknowledge writes and lose them:

- **an acknowledged write is lost**: the old writer answered `200`, but replay
  keeps one history and drops the other;
- **the sweeper deletes live data**: it deletes everything its state does not
  know, so writer A deletes the chunks only writer B's history refers to.

On Telegram the fence is nearly free: an append is one send, and a read only
when another message landed between `p` and `b`. On Discord it is the whole
ceiling: every append is a link, a thread, and threads come at 50 per 300 s
per bot, so ~10 appends a minute. That is the ~9 s per entry the bench
measured with eight clients.

A message send is 5 per ~5 s per channel (spike, 2026-10-28): about 6× more,
at the latency of one POST instead of a place in the thread pacer's queue.

Measured after log-collect (2026-10-29, [bench/](bench/)):

- A Discord append takes 5.8 s on average with eight clients and 6.1 s one
  at a time. A message POST alone (a 4 KiB blob put) takes ~0.56 s, so ~90%
  of an append is waiting for the pacer's next thread (one per ~6.7 s).
- No 429 on message create at our ~1 message/s per channel. Every 429 seen
  was on thread creation.
- Snapshots take no threads any more (`put_loose`). The budget goes to one
  thread per log entry, plus one per snapshot (the rebase batch).
- With eight clients the log is still the limit: 0.6 PUTs/s, 3.56 proposals
  per entry, p50 13 s. The batcher only divides it. One POST per entry
  instead of a paced thread should lift writes about 10×.

## Why not a plain lease

"I hold the log until T; before T I append" breaks on a pause. The writer
checks the lease, stalls for 20 s (GC, a hung connection, a retry), and its
POST lands after a new writer has started. The writer's clock cannot say when
a request reaches the server.

Plain posts validated by scanning do not work either:
[discord-log.md](discord-log.md) § Why the order fence does not carry over.
Snowflakes come from many nodes' clocks, so a lower id can become visible after
a higher one.

## The design: lease records through the thread chain, appends as posts

Two kinds of chain members:

- **Lease records** go through the existing thread compare-and-set: `Claim(g)`
  and `Renew(g, n)`. One holds the lease for `D` (say 45 s) from its own
  server time. A writer renews every `D / 3`, so a few threads a minute
  instead of one per append.
- **Batches** are plain messages: `Batch { gen, prev, … }` names the message
  before it (a batch or the lease record it follows), as today; no thread.

**Server time.** Every message carries its server time: the snowflake's
millisecond timestamp. `ts(m)` below is that; `ε` is a margin for the skew
between Discord's nodes (to measure, § Spike; assume ≤ 1 s).

`expiry(g)` = `ts(newest lease record of g) + D`.

**append(g, batch)**:

1. POST the batch with `prev` = our head and `enforce_nonce` (as today: a
   resend within ~3 min returns the same message, which settles an ambiguous
   send).
2. Acknowledge only if the 201 came back before the writer's local deadline:
   `local time when the newest renewal was sent + D − ε`, on the monotonic
   clock. The send time precedes the server stamp, so this deadline never
   falls after the real expiry.
3. Too late: the batch is not acknowledged. The writer stops appending until a
   renewal goes through (or finds it was fenced).

**renew(g)**: post `Renew(g, n)` and link it by thread after our newest lease
record. `160004` means a newer claim was linked: `Fenced`.

**claim(g)**:

1. Find the newest lease record, from `g' < g`.
2. Wait until server time passes `expiry(g') + ε`: poll with a probe (a
   message's snowflake, deleted after) or the `Date` header. On one host a
   `flock` held by the old process, now free, proves it is dead and skips the
   wait.
3. Find the tail of `g'`: the batch chain from its newest lease record by
   `prev`. The read comes after the expiry, and every acknowledged batch got
   its 201 before it, so every acknowledged batch is visible (assumption A
   below).
4. Post `Claim(g, tail)` and link it by thread after `g'`'s newest lease
   record. `160004`: a renewal of `g'` came first; go back to 1.

**Reading** (discover, replay): follow lease records by threads, as today.
Within each generation, follow batches by `prev` from the tail that the next
claim pinned; for the current generation, from the newest batch of the
current lease. A batch that landed late and was never acknowledged is on no
such path and is ignored, like an unlinked batch today.

## What it assumes (the fence today assumes nothing)

The thread fence has no timing assumptions at all
([discord-log.md](discord-log.md) § Properties). This one has:

- **A. Read after write**: a message whose POST returned 201 is in every later
  listing. Without it, a claimant could miss an acknowledged batch.
- **B. Bounded skew**: Discord's nodes stamp snowflakes within `ε` of each
  other.
- **C. A bounded clock rate on the writer**: the monotonic clock does not run
  slow by more than a little over `D`. This is what every lease needs.

Not assumed: the writer's wall clock, or how long a request takes in flight.
A late request is rejected on its 201, not judged by when it was sent.

## Costs

| | Today | Lease |
|---|---|---|
| Append, Discord | message + thread | 1 POST |
| Threads | one per append (~10/min ceiling) | lease records: ~4/min at `D` = 45 s |
| Sustained appends | ~10/min | ~1/s per channel (message create bucket) |
| A new writer after a crash | at once | after `D + ε` (on one host, at once with `flock`) |
| Reading | follow threads | threads for lease records, `prev` for batches |
| Timing assumptions | none | A, B, C |

The batcher groups S3 operations into an entry, so the gain in operations is
larger than in entries.

## Also covered by the lease

- **The sweeper.** It deletes only under a lease: before each delete batch it
  checks the same local deadline as an append. Open question: a delete
  request that hangs and lands after the expiry. Today's design has the same
  hole for any deposed writer's in-flight delete; see
  [sweep-invariants.md](sweep-invariants.md).
- **Snapshots** do not change: a snapshot is named by a rebase batch, and
  `put_loose` (log-collect) already takes snapshots off the thread budget.

## Telegram

Not changed. Its order fence costs one send per append and assumes nothing.
A lease would save only the rare read when another message lands between `p`
and `b`.

## Spike (before any code)

On a test channel, with two bots or two tokens where noted:

1. **Skew** (B): POST from two clients in parallel, many times. Compare id
   order, `ts` and the local receive time. Measure how often the order
   inverts, and by how much.
2. **Read after write** (A): right after a 201, list the channel from the
   other client. Is the message there every time, over hundreds of tries?
3. **Server time from a probe**: snowflake `ts` vs the `Date` header vs the
   local clock. Which is the better witness for step 2 of claim?
4. **Message create sustained**: 5 per 5 s per channel, or more with
   `X-RateLimit-*` honoured over minutes; does the bucket span channels?
5. **Thread budget with renewals**: a writer renewing every 15 s plus claims.
   What is left for anything else?

## Spike findings (2026-10-29, one bot, the test log channel, API v10)

Two writers posting text messages in parallel, 130 messages in all. After
each 201 the writer listed the newest 50 messages. Everything was deleted
afterwards (`/tmp/dspike/lease_spike.py`).

- **Read after write (A): held, 130 of 130.** Each message was in the
  listing right after its 201.
- **Id order vs happened-before: no inversion.** When one message's 201
  came back before another was sent, the second always had the higher id.
  Concurrent sends swapped order twice, which is expected and harmless.
- **Skew (B): ≤ ~150 ms.** Every snowflake fits `[send, receive]` shifted by
  one clock offset (our clock vs Discord's): the offset lies in
  [629, 780] ms for all 130. The nodes that stamped them agree to within
  that 150 ms window, and the window includes our own timing error.
  `ε` = 1 s is generous.
- **Server time witness: the snowflake.** The `Date` header has whole
  seconds. Every POST's answer gives server time to the millisecond. A
  writer's own appends and renewals carry it for free; a claimant needs one
  probe message.
- **Message create: 5 per 5 s per channel.** `X-RateLimit-Limit: 5`, reset
  about 5 s. The bucket hash is one for all channels, but counts go per
  channel: a burst in the log and the snapshot channels at once spent 5 in
  each. Two writers in one channel sustained 1.03 messages/s. RTT p50
  322 ms, max 1.6 s.
- **Not run: renewals against the thread budget (5).** At `D` = 45 s
  renewals take ~4 threads a minute of the ~10.

So A and B hold with margin. A lease's batch costs one POST (~0.3 s) and
appends sustain ~1/s per channel, against a paced thread every ~6.7 s now.

## Implementation notes (2026-10-29)

- **Where**: `ChainLogStore::with_lease(Lease)` (pigeonhole-meta
  `log/chain.rs`), only for a store that implements `LeaseChain` (a
  `Chain` subtrait in pigeonhole-storage-api), so a lease on any other store
  does not compile:
  - `stamp_ms`: Discord's snowflake time;
  - `put_unlinked`: a member's name, no thread;
  - `list_after`: list newer blobs by label.

  Other stores keep the link-per-batch log.
  `[meta] lease_secs = 0` turns the lease off for Discord too.
- **Renewals run in the background and stay outside the log.** In the first
  live run a renewal waited for the thread pacer (~7 s with the post) in
  front of an append. The next batch then missed the deadline and the handle
  stopped, so thread throttling would have stopped the tablet. Now:
  - once a third of the lease is gone, a write spawns the renewal and goes
    on;
  - only after two thirds does a write wait for it;
  - a renewal names the writer's tip as a hint, but batches keep naming
    batches: a renewal in flight cannot fork the log.

  A reader's tip under a renewal is the newest batch after that hint stamped
  inside the renewal's lease. Under a claim it is the newest batch after the
  claim.
- **No background renewer when idle.** An idle writer's lease runs out. Its
  next write waits for a renewal: the compare-and-set after its last record
  either wins, and it holds again, or loses to a claim, and it is fenced.
- **A claimant's wait** is measured by probes: a loose `log-probe` message,
  its stamp read, deleted right after. Two posts per probe, a few per claim.
- **Collection** never prunes below the newest lease record: it is the
  chain's tail.
- **Metrics**: `pigeonhole_meta_lease_wait_seconds{reason}`, where `renew`
  is a write waiting for a renewal and `claim` is a claimant waiting out the
  old lease.
- **Live** (`tests/live.rs` `lease_log_against_a_live_channel`, 10 s lease,
  the `[discord]` limiter):
  - claim on an empty channel: 3.1 s;
  - 10 appends: 10.9 s (the limiter's 1 send/s; the link-per-batch log
    took ~6 s per append);
  - a second claim waited out the lease (10.6 s) and fenced the first
    writer;
  - a reader saw all 11 entries.
- **Release on a clean shutdown**: on SIGINT or SIGTERM, `serve` calls
  `MetaStore::close`. It stops Raft, then `LogStore::release` links a
  `log-release-<gen>` record after the newest lease record. A release's
  lease ends at its own stamp, so the next claimant waits only for its
  probes and the margin. Live: a claim after a release took 3.0 s, against
  a 10 s wait without one.
- **Not done**: the `flock` shortcut after a crash on the same host, and
  simulation tests for the lease. A crash still costs up to `lease_secs`.

## Steps

1. Spike, write findings here.
2. `Chain` gets a "loose append" (`put_loose`, from log-collect) for batches.
   `ChainLogStore` learns lease records and the local deadline.
   `FakeDiscord` gets a clock and a skew knob.
3. Conformance tests plus simulation: a writer paused past its expiry, a 201
   after the deadline, a late batch from a deposed writer, two claimants.
4. Live run and bench: compare with
   [bench-discord-vs-telegram.md](bench-discord-vs-telegram.md).

## Open questions

- `D`: shorter speeds up failover, longer spends fewer threads. 45 s is a
  guess. Restart-heavy tests and the simulation must not pay `D` for every
  restart. Options: `D` set by config (seconds in tests), the fake's and
  madsim's virtual clock, and the `flock` shortcut on one host.
- Commit latency now rests on assumption A and the deadline check, not on a
  compare-and-set. A violation of A loses an acknowledged batch silently.
  The simulation should inject a late-visible message and show the deadline
  holds.
- Whether the lease belongs in `ChainLogStore`, for every store (the memory
  and disk stores do not need it), or only behind the Discord `Chain`.
- An in-flight delete after the expiry (above).
