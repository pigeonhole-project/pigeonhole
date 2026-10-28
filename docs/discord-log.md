# The metadata log and snapshots in Discord

Status: **proposal**; spike done (see Spike findings). A brief for making a Discord channel a
`LogStore` and a `SnapshotStore`, and for aligning the Discord blob backend
with the MTProto one. Read [roadmap.md](roadmap.md) § Ground rules,
[meta-backends.md](meta-backends.md), [telegram-mtproto.md](telegram-mtproto.md)
(the order fence this is contrasted with) and [glossary.md](glossary.md) first.

Today Discord holds blobs only (`meta_store refuses discord` in the chunk-store
config).

## What the Discord API offers

| | Telegram over MTProto (bot) | Discord (bot) |
|---|---|---|
| Read by id / list | `channels.getMessages`, ≤ 100 ids | `GET /channels/{c}/messages/{m}`; list with `before` / `after` / `around`, ≤ 100 |
| Idempotent send | `random_id` | `nonce` (≤ 25 chars) + `enforce_nonce: true`: same author, "the past few minutes"; works with attachments |
| Message ids | a dense per-channel counter: a sequencer | snowflakes: ms timestamp + worker + counter; sparse, ordered only roughly |
| A register | the channel description, cheap reads | the channel topic: **2 changes per 10 minutes per channel** |
| Create-once | none | **a thread from a message**: the thread id is the message id; a second one fails with `160004` |
| File size | ≥ 300 MB measured | **20 MiB** per file by default since September 2026 (was 10; boosted servers more) |
| File links | `file_reference`, refreshed by `getMessages` | signed CDN URLs (`ex` / `is` / `hm`, ~24 h); a fresh one comes with every message fetch, or `POST /attachments/refresh-urls` (≤ 50 URLs) |
| Send rate | ~20–30 a minute per channel (measured) | higher, dynamic: read `X-RateLimit-*` headers |

Sources: [channels](https://docs.discord.com/developers/resources/channel),
[messages](https://docs.discord.com/developers/resources/message),
[change log](https://docs.discord.com/developers/change-log),
[error codes](https://gist.github.com/Dziurwa14/de2498e5ee28d2089f095aa037957cbb),
[topic rate limit](https://github.com/discord/discord-api-docs/issues/2190),
[refresh-urls](https://docs.discord.food/reference).

## Why the order fence does not carry over

The Telegram log decides validity by a message's position: ids come from one
sequencer per channel and are dense, so `b == checked + 1` proves nothing lies
between, and `messageEmpty` below a returned id means "not visible yet".
Snowflakes give neither. The node that handles a request stamps the id with its
own clock, so a message with a lower id can become visible after one with a
higher id (clock skew between nodes, write latency), and a sparse id space has
no way to tell "not visible yet" from "nothing here". A claimant scanning below
its claim could miss an old generation's batch that lands, with a lower id,
after the scan, though its writer saw it acknowledged. Only a pause would close
that window — the timing assumption the Telegram design got rid of.

## The fence: a thread as compare-and-set

A message can have at most one thread started from it
(`POST /channels/{c}/messages/{m}/threads`; the thread takes the message's id;
a second attempt fails with HTTP 400, code `160004`). That is an atomic
create-if-absent per message — a compare-and-set on its "next" pointer. The log
is a linked list and appending is a lock-free list append.

Messages, all documents named with their kind and generation
(`log-<log_id>-<gen>-claim.bin`, `…-batch.bin`; the document repeats them):

- `Claim { log_id, gen }`
- `Batch { log_id, gen, prev, base, vote, head_term, first, entries }` — as
  `LogDoc` today.

A **link** from `H` to `N` is the thread started from `H`, named
`next <N.id> g<gen>` (thread names hold 100 characters). The **chain** is what
links reach from the start; the **tail** is the chain member with no thread.
A link is only ever made from the linker's own head or from a tail it just
found, so only chain members have threads, and a message nobody links to is
garbage, whatever its id and whenever it landed.

**append(g, batch)**, with `head` = our last linked message:

1. Send the batch (`prev = head`) with `enforce_nonce` (nonce derived from
   `(log_id, gen, first)`: nonces are per bot across channels; a resend
   deduplicates only within ~3 minutes), get its id `b`.
2. Start a thread from `head` named `next b g<g>`.
3. Created: `b` is committed and is the new head, `Ok`. `160004`: someone
   linked `head` first — a newer generation's claim — `Fenced`; `b` stays
   garbage. Any other failure: read `head`'s `thread` field; it names `b` (our
   link went through) or another message (fenced), or there is none (retry).

**claim(g)**:

1. Find the tail (discover).
2. Send `Claim(g)`, start a thread from the tail naming it.
3. `160004`: read the tail's thread, follow the chain to the new tail; if a
   claim of a generation ≥ `g` was linked on the way → `Fenced { current }`;
   otherwise retry the link from the new tail with the same claim message.
4. Linked: `g` holds the log. Every append of an older generation now fails at
   its own head, which is already linked (to our claim, or before it).

**discover**: start at the oldest message that has a thread (after collection,
the rebase batch; list with `after` from the channel's start), follow links to
the tail, collect the newest batch (the head) and claims on the way; replay
follows `prev` from the head down to the base.

**Rebase and collection**: a rebase is a batch carrying the new base. Once it is
linked, the writer deletes the messages below it and the snapshot the old base
named: each **message first, then its thread**. A deleted thread can be started
again from a message that still exists (a reopened compare-and-set), but not
from a deleted message (`10008 Unknown Message`); a thread outlives its
deleted starter.

**Archiving**: an archived thread still answers `160004`. A writer archives the
previous link once the next one is made (`PATCH /channels/{thread}
archived=true`; its rate bucket is per thread), so about one link is active at
a time and the guild's active-thread limit is never near.

**Genesis**: an empty channel has no tail, and two first claims would each
start a chain. `pigeonhole init-log` posts one genesis message, once, by the
operator; a claim with no chain links to the oldest genesis message (a second
one, posted by mistake, is newer and ignored).

**Properties**: no timing assumptions at all — no straggler window, no
visibility bound, no use of id order. A late or duplicate message is never
linked. A batch is committed when linked; a batch sent but never linked is
lost even if its writer died before learning the outcome, which is the
ordinary "unacknowledged" case.

**Throughput**: a link is a thread creation, and those are limited to 50 per
300 s (fixed window, shared by all channels; failed attempts count). So at
most ~10 appends a minute sustained: group commit must aim at one append per
6 s or more, and claims retrying on `160004` spend the same budget. Comparable
to the Telegram log (~20–30 sends a minute), but this is the limit to watch.

**Discover** skips `THREAD_CREATED` system messages (type 18): Discord posts one
when the starter message is 5 or more messages back. An append links the
message right below its batch, so normally none appear.

## Snapshots

A `SnapshotStore` in a channel of its own: plain storage, no order, no fence.

- `SnapshotRef` = `message_id` + crc; `get` fetches the message (fresh CDN
  URLs) and downloads; the crc catches a replaced message.
- Snapshots above 20 MiB are split over several attachments (≤ 10 per message,
  each ≤ 20 MiB: the limit is per file) or messages; the ref names the first,
  which lists the rest.
- `newest()` (lossy restore): list the newest messages, take the first that
  decodes as a snapshot; filter by `log_id` when asked.

## The blob backend, aligned with MTProto

Mostly in place already (listing filtered by `ph-<hex>.bin`, CDN ranges, URL
refresh by fetching the message). Changes:

- `DiscordId` = `message_id` only; the attachment is the message's first one,
  its URL comes from the message (cached, refreshed on 404 / expiry, batched
  through `refresh-urls`).
- Uploads with `enforce_nonce`, nonce derived from the `BlobId` (25 chars:
  base62 of its 128 bits fits in 22): a retried upload never posts twice.
- `BackendLimits::discord` default to 20 MiB (minus margin); keep the
  `max_blob_size` override for boosted servers. The limit is per file, so a
  message may carry several parts.

## Spike findings (2026-10-28, bot on an unboosted test guild, API v10)

Throwaway Python against the test channels (ids in the repo `.env`); all
spike messages and threads were deleted afterwards.

- **Create-once holds**: a second thread from a message → HTTP 400, code
  `160004`. Three concurrent creates on one message: exactly one `201`, two
  `160004`. Archived, and archived + locked, threads still answer `160004`.
- **Re-creation after delete**: deleting a thread clears `message.thread`, and a
  new thread can then be started from the same message. Starting one from a
  deleted message fails (`10008`), also while its old thread still exists (a
  thread survives its starter's deletion). Hence collection order: message,
  then thread.
- **Links are visible in listings**: `GET /channels/{c}/messages` (100 per page)
  returns each message's `thread` with its name and `thread_metadata`; so does
  `GET` of one message. Discover reads 100 links per call.
- **System messages**: starting a thread from a message 5+ messages back posts
  a type 18 (`THREAD_CREATED`) message named like the thread; 1–3 back posts
  nothing.
- **Rate limits** (headers, dynamic):
  - thread creation: 50 per 300 s fixed window, one bucket across channels
    (per bot or per guild: not told apart), plus short bursts limited (a 429
    with `retry_after` ~3–5 s after ~10 quick creates); a 429 creates nothing;
  - message create: 5 per ~5 s per channel (30 sends took 26 s);
  - get one message, list messages: 5 per few s per channel;
  - archive (`PATCH` thread): 10 per 15 s, per thread;
  - bulk delete: 1 per ~2 s per channel; delete thread: effectively free.
- **Nonces**: `enforce_nonce` returns the earlier message for a resend within
  **3–5 minutes** (seen at 3 min, a new message at 5), even with a different
  body or no attachment; it returns the newest message carrying that nonce.
  The scope is the author across channels: the same nonce in another channel
  within the window fails with `404 Unknown Message`. ≤ 25 characters
  (26 → `NONCE_TYPE_TOO_LONG`); digit strings work. Nonces must therefore be
  unique per bot (derive them from log id + generation + position, or the
  `BlobId`), and a send may be retried only within ~3 minutes.
- **Files**: exactly 20 MiB (20 971 520 bytes) per attachment is accepted,
  one byte more → `413`, code `40005`. The limit is per file: one message with
  2 × 15 MiB went through. Upload ~1.8 MB/s (20 MiB in 11 s); CDN download of
  20 MiB in 1.2 s; `Range` answers `206` with `Content-Range`.
- **CDN URLs**: `ex − is` = 24 h; `POST /attachments/refresh-urls` works for a
  bot with bare URLs.
- **Not probed**: the guild's active-thread limit (archiving each link keeps
  us far below it); whether the thread bucket is per bot or per guild (needs a
  second guild).

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. ~~**Spike**~~ Done; results in Spike findings above.
2. **Blob backend**: `DiscordId` = `message_id`, nonce uploads, 20 MiB default
   (exactly 20 971 520 accepted), URL refresh batching. Existing wiremock conformance tests follow.
3. **Linked log** in `pigeonhole-meta`: the protocol over a `LinkableChat`
   trait (`send_document`, `link(from, name) -> Created | Taken(name)`,
   `thread_of(id)`, `list(after, limit)`, `download`, `delete`), a fake with
   racing links, tests: append, fenced append, racing claims, a late
   straggler, an unlinked batch, rebase and collection, genesis.
   `LogDoc` / `prev` / replay are shared with the Telegram chat log; the
   fence is not (`SequencedChat` there, `LinkableChat` here).
4. **Snapshot store** over the same client; split snapshots.
5. **Wiring**: allow discord for `[meta] log` / `snapshot`, `init-log`
   subcommand, permission checks at startup, config example.
6. **Live run**: extend `scripts/compat-discord.sh`; meta-backends.md,
   glossary. Repeated `sweep_stress` and the S3 compat suites at the end.

## Open questions

- Genesis by an explicit `init-log`, or the oldest message after a pause?
- Is the thread-creation bucket per bot or per guild? If per guild, one guild
  per log keeps its ~10 appends a minute to itself.
