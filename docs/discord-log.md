# The metadata log and snapshots in Discord

Status: **proposal**, not spiked. A brief for making a Discord channel a
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
   `(log_id, gen, first)`), get its id `b`.
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
named. Threads of collected messages are left alone (see Spike: re-creation).

**Genesis**: an empty channel has no tail, and two first claims would each
start a chain. `pigeonhole init-log` posts one genesis message, once, by the
operator; a claim with no chain links to the oldest genesis message (a second
one, posted by mistake, is newer and ignored).

**Properties**: no timing assumptions at all — no straggler window, no
visibility bound, no use of id order. A late or duplicate message is never
linked. A batch is committed when linked; a batch sent but never linked is
lost even if its writer died before learning the outcome, which is the
ordinary "unacknowledged" case.

## Snapshots

A `SnapshotStore` in a channel of its own: plain storage, no order, no fence.

- `SnapshotRef` = `message_id` + crc; `get` fetches the message (fresh CDN
  URLs) and downloads; the crc catches a replaced message.
- Snapshots above 20 MiB are split over several attachments (≤ 10 per message)
  or messages; the ref names the first, which lists the rest.
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
  `max_blob_size` override for boosted servers.

## Spike

Against a test guild and channel, with a bot holding `SEND_MESSAGES`,
`READ_MESSAGE_HISTORY`, `CREATE_PUBLIC_THREADS`, `MANAGE_THREADS`,
`MANAGE_MESSAGES`:

1. `160004` on a second thread from one message, also from another process
   and token session; what a racing pair of creates returns.
2. Does `GET /messages` (list) return the `thread` field? Then discover reads
   links 100 at a time; otherwise one fetch per link.
3. **Active thread limit per guild**: threads auto-archive after ≥ 60 min,
   and one per append at ~1 append/s is thousands an hour. Archive each link
   (`PATCH /channels/{thread} archived=true`) right away or a little later;
   check `160004` still holds for an archived thread.
4. **Re-creation after delete**: can a thread be started again from a message
   whose thread was deleted? If so, links must never be deleted, only
   archived (the design above assumes that).
5. Rate limits of thread creation and archiving (headers), and whether
   starting a thread posts anything visible in the channel.
6. `enforce_nonce` window and scope with multipart uploads; nonce format.
7. 20 MiB attachments for a bot on an unboosted server.

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. **Spike** (throwaway binary, not merged); findings into this doc.
2. **Blob backend**: `DiscordId` = `message_id`, nonce uploads, 20 MiB default,
   URL refresh batching. Existing wiremock conformance tests follow.
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

- Archive links right away (one extra call per append) or in batches?
- Genesis by an explicit `init-log`, or the oldest message after a pause?
- One guild per log to keep the active-thread budget to ourselves?
