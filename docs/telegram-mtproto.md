# Telegram over MTProto (bot token)

Status: **implemented** (steps 1–5; see Implementation notes); the Bot API
backend is removed and this one is `kind = "telegram"`. A Telegram backend that talks MTProto instead
of the Bot API, still logged in as a **bot** (no user account). Read [roadmap.md](roadmap.md) § Ground rules,
[meta-backends.md](meta-backends.md), [sweep-invariants.md](sweep-invariants.md)
and [glossary.md](glossary.md) first.

## Why

Two things hurt in the Bot API backend (`storage/pigeonhole-storage-telegram`,
`bin/pigeonhole/src/telegram_log.rs`):

1. **No way to read a message by id or to list a chat.** The sweeper treats
   every message id up to the horizon as a candidate, so a blob chat must hold
   nothing else; nothing can be checked against our `ph-<hex>.bin` names.
2. **The log and the snapshots need pins.** The pin is only a pointer to "our
   newest message", because a bot cannot ask what message N holds. Pinning
   costs a call per append, needs pin rights, posts service messages in groups,
   and a fresh pin can be invisible for about a minute.

A bot logged in over MTProto (`auth.importBotAuthorization`) may call
`channels.getMessages` (up to 100 ids per call). `messages.getHistory` /
`messages.search` are forbidden for bots, but channel message ids are dense,
so reading id ranges in batches of 100 *is* a listing. It also lifts the
Bot API's HTTP limits on file size (20 MiB `getFile`) and gives exact ranged
downloads (`upload.getFile`).

Rejected alternatives:

- **User account over MTProto**: has `getHistory`, but the session is full
  account access, login is interactive (phone, code, 2FA), flood limits are
  undocumented and a ban costs a phone number. Nothing it adds is needed.
- **A second bot reading `getUpdates`** as a history source: updates are kept
  24 h at most, paging past the first 100 confirms (deletes) them, there is one
  consumer per bot, and deletions produce no updates. It is a stream, not
  history; recovery cannot rely on it.
- **`forwardMessage` to a scratch chat** to read a message by id over the Bot
  API: works for finding a head at boot, but is noisy, spends send budget and
  is no use to the sweeper.

## Scope

- New crate `storage/pigeonhole-storage-telegram-mtproto`, new
  `InstanceKind::TelegramMtproto`, `kind = "telegram_mtproto"`. The Bot API
  backend stays as is (comparison, mixed replicas).
- MTProto library: [grammers](https://github.com/Lonami/grammers) (pure Rust,
  tokio, raw `invoke` for any TL function). Pin the exact version; it is pre-1.0.
- A new chat log over MTProto with no pins and no description: the fence is
  the order of messages in the channel (below). It needs range reads, so the
  Bot API `ChatLogStore` cannot use it (see Open questions).
- Only channels / supergroups are accepted: there message ids are per chat and
  dense. In basic groups and private chats ids are a per-account counter, and
  an id-range sweep would hit other chats' ids. Check the peer type at startup.

## Design

### Config and session

```toml
[[store]]
id = "tgm-main"
kind = "telegram_mtproto"
chat_id = -1001234567890
# bot_token_env = "BOT_TOKEN"      # default
# api_id_env = "TG_API_ID"         # my.telegram.org, in .env
# api_hash_env = "TG_API_HASH"
# session = "pigeonhole-tg.session" # MTProto auth key cache; secret, 0600
```

- One MTProto client per bot token per process, shared by blob stores, the log
  and snapshots. The session file is only a cache of the auth key and DC: if it
  is missing, log in again with the token (no interaction). Take an exclusive
  `flock` on it so two processes never share one session.
- Ignore updates (no catch-up, no update loop work).
- Resolve `chat_id` to `InputPeerChannel { channel_id, access_hash }` once at
  startup; check the bot is an admin with post and delete rights.
- Fingerprint `tgm:{bot_id}:{channel_id}`, location `tg:chat:{chat_id}` (same
  location as the Bot API backend: one chat must not get two writers).

### Blob backend

- `Id = TelegramMtprotoId { message_id: i64 }`, `Key = message_id`. Nothing
  else is persisted: `access_hash` / `file_reference` / `dc_id` are fetched.
- **Location cache** (replaces the `file_path` LRU): `message_id ->
  { InputDocumentFileLocation, dc_id, size, file name }`. On a miss or on
  `FILE_REFERENCE_EXPIRED`, `channels.getMessages` (batch misses, ≤ 100 ids)
  and retry once. Check the document's file name matches the expected
  `BlobId` when the caller knows it.
- **put**: `upload.saveBigFilePart` (512 KiB parts, a few in parallel), then
  `messages.sendMedia` with a random `random_id`: the server deduplicates a
  resend with the same `random_id` (also from another session: verified), so
  a send may be retried after any error without a duplicate. Derive
  `random_id` from the `BlobId` (a retry then can never post twice). Return
  the new `message_id`.
- **get**: `upload.getFile` in windows of at most **512 KiB** (grammers 0.11
  asserts on a 1 MiB answer: it exceeds an MTProto container), `offset %
  4096 == 0`, `limit % 4096 == 0`, a window never crossing a 1 MiB boundary.
  A range is rounded out to windows and sliced; windows of one blob can be
  fetched in parallel (~360 ms per 512 KiB window measured, single stream).
  Pass `cdn_supported = false`; follow `FILE_MIGRATE_X`.
- **delete**: `channels.deleteMessages` (≤ 100 ids per call); missing ids are
  success.
- **Sweepable / listing**: candidates are produced by reading id ranges up to
  the horizon with `channels.getMessages` and keeping only documents named
  `ph-<32 hex>.bin`; `can_list = true`. Update sweep-invariants.md: this
  backend, like Discord, never deletes what is not ours.
- `BackendLimits::telegram_mtproto()`: `max_blob_size` = what the chunk store
  allows (`MAX_LOGICAL_CHUNK`, 256 MiB) or less if the spike shows a lower
  bot limit; exact range support.
- Rate limits: `FLOOD_WAIT_X` becomes the shared rate-limited error (see
  below) and a cool-down on the `ChatLimiter` budget of that operation
  (send / read / download / delete). Defaults conservative, tuned in the spike.

### Shared rate-limit error

Move `RateLimited` (now in the Bot API client) and the `retrying` helper (now
in `telegram_log.rs`) somewhere both transports and the log adapters use, e.g.
`pigeonhole-blob`. 429 and `FLOOD_WAIT_X` map to the same type.

### The log: a fence by order in the channel

> **Superseded by [chain-log.md](chain-log.md)** (done): the log is
> `ChainLogStore` over `impl Chain for TelegramBlobStore`. The order fence
> below became the Telegram store's way to decide "first successor wins";
> `ChannelApi` / `ChannelLogStore` / `ChannelSnapshotStore` are gone.

Telegram gives every message in a channel an id, atomically and in increasing
order: one total order of everything every writer posts. With
`channels.getMessages` a reader sees that order, so who won is decided by the
**position** of a message, not by time. No description register, no pins, no
straggler window: a deposed writer's message that lands arbitrarily late (see
Spike findings: a dropped grammers future still sends) is above the newer
claim and invalid for every reader alike.

The log chat holds two kinds of messages, both documents. Kind and generation
are in the file name (`log-<log_id>-<gen>-claim.bin`, `…-batch.bin`) so a
scan decides validity from `getMessages` metadata alone and downloads only the
batches it replays; the document repeats them and a mismatch is `Lost`.

- `Claim { log_id, gen }`
- `Batch { log_id, gen, prev, base, vote, head_term, first, entries }` — as
  `LogDoc` today; `prev` links the previous batch of the chain (for the first
  batch of a generation, its zero entry: the previous head, and it names its
  own claim).

**Validity** depends only on messages with lower ids, which never change once
ids are assigned:

- a claim of `g` is valid iff no valid claim of a generation ≥ `g` is below it
  (two claims of one generation: the lower id wins);
- a batch of `g` is valid iff no valid claim of a generation > `g` lies
  between its `prev` and itself.

**Writer invariant**: a generation sends its first batch only after its claim
is confirmed, and each next batch only after the previous one is confirmed
(one batch in flight; group commit fills the next meanwhile). So the checked
ranges of a generation are contiguous, and a batch's validity is a local fact:
the range `(prev, self)`.

**Reading a range** `(a, b)`: `getMessages` over the ids strictly between, 100
per call. A claim or batch of this log with a generation above ours → fenced.
Batches of lower generations (stragglers) and foreign messages are skipped.
`messageEmpty` means "not visible yet" (nothing above the base is ever deleted,
see collection): re-read after a pause; still empty after a bound → `Other`
(outcome unknown).

**append(g, batch)** with `checked` = the id of our last confirmed batch (or of
our claim):

1. Send the batch, get its id `b`. A send that failed without an answer is
   repeated with the same `random_id` (the server returns the same message).
2. `b == checked + 1`: nothing can lie between, `Ok` at once, without a read —
   the common case in a dedicated chat, one call per append.
3. Otherwise read `(checked, b)`: a higher claim → `Fenced`, nothing is
   acknowledged and the batch is invalid for everyone; else `Ok`.
4. `checked = b`. A failed read is simply repeated: the outcome is already
   fixed by the order in the chat.

**claim(g)** = claim + discover in one pass:

1. Send `Claim(g)`, get its id `c` (also the chat's top: the spike found no
   cheaper way to learn it).
2. Walk down from `c` in pages of 100 to the first batch `B` that is valid by
   its local check (its `(prev, B)` range is read as part of the walk or
   fetched). Higher batches skipped on the way are stragglers whose range holds
   a higher claim. By the writer invariant every valid claim below `B` has a
   generation ≤ `B.gen`, so `B` is the head of its generation's chain. No batch
   at all: walk to the bottom (fresh log).
3. Evaluate the claims in `(B, c)` bottom-up starting from `B.gen`: a claim is
   valid if its generation exceeds every valid one below it. A valid claim ≥
   `g`, or one of `g` with a lower id → `Fenced { current }`, retry above it.
4. Otherwise `g` holds the log. The head is `B` (or nothing); replay follows
   `prev` down to the base. The first batch of `g` (the zero entry) links `B`.

A claim reads `(B.prev, c)`: the claims and stragglers since the last batch,
normally a page or two, not the whole log.

**Rebase and collection**: a rebase is a batch carrying the new base (a
snapshot ref). Only after it is `Ok` does the writer delete the log messages
below the rebase batch's `prev` (the rebase batch and its `prev` stay, so its
local check still reads a range without collected holes) and the snapshot the
old base named. A fenced writer deletes nothing.

**Semantics**: a batch sent but never confirmed (writer died before its check)
is valid if no higher claim precedes it and then becomes the head: entries
never acknowledged may survive, which a Raft log allows. A batch is the commit
point, not an acknowledgement. State it in meta-backends.md; fake-chat tests
cover it, the straggler cases and two racing claims.

`ChatApi` shrinks to what the protocol uses: `send_document(name, data) ->
id`, `get_messages(ids) -> [Empty | Other | Doc { name, download_ref }]`,
`download`, `delete`. `chat/fake.rs` follows (ids grow, a message can be made
visible late).

### Snapshots

In a chat of their own: a `SnapshotStore` is plain storage, no order, no
fence; a snapshot counts only while a valid rebase names it.

- `SnapshotRef` = `message_id` + crc; `get` reads the message by id
  (fresh `file_reference`), the crc catches a replaced message.
- `put` = send; `remove` = delete.
- `newest()` (lossy restore only): learn the top by sending a probe message
  (then delete it) and walk down to the first document that decodes as a
  snapshot. The snapshot carries `(log_id, gen, at)`; `restore --lossy` prints
  it and can filter by `log_id` when a chat holds several logs' snapshots.
- A deposed writer's snapshot is harmless (its rebase is invalid): garbage,
  removed best effort (everything no current base names).

Snapshots in the log chat would work too, but they would take ids in the log's
order and break the `b == checked + 1` fast path; a separate chat also keeps
the snapshots when the log chat is lost.

### With a local log

`[meta] log` on `local_disk`, snapshots in a channel: the local `LogStore`
fences (one process per directory) and the channel only stores snapshots, as
above. Snapshots are taken of applied state, and a local log applies only what
was appended, so every snapshot — even one whose rebase never happened — is a
committed state; `newest()` is safe for lossy restore here. The tablet cannot
move between machines with a local log; that is what the channel log is for.

## Spike findings (2026-10-28, grammers 0.11.0, bot token, test channels)

- **Login**: `auth.importBotAuthorization` (grammers `bot_sign_in`) in 1.8 s;
  the SQLite session then makes later starts authorised at once (~250 ms).
- **No access hash needed**: `channels.getChannels` with `access_hash = 0`
  works for a bot that is admin of the channel and returns the real hash.
- **Listing works**: `channels.getMessages` for 25–70 ids in 60–100 ms.
  Answers: our documents (file name, size, dc), `messageService` for id 1
  ("channel created"), `messageEmpty` for deleted ids **and** for ids past the
  top (indistinguishable).
- **Top id**: not exposed by `channels.getFullChannel` (`read_outbox_max_id`
  is 0 for a bot); the id of a message we send is the top. A claim sends one
  anyway; a reader walks down from the last id it knows.
- **Fence reads are cheap**: `channels.getFullChannel` (the description,
  `about`) 30 times in a row in 3.2 s (~105 ms each), no flood wait.
- **Send rate per channel is the Bot API's**: 40 `sendMedia` back to back
  took 80 s: bursts of ~5, then FLOOD_WAIT 3–5 s, and a 40 s wait near the
  end: ~20–30 messages a minute sustained. The win is one message per log
  append (no pin, no pin service message), not a higher limit.
- **Idempotent sends**: a resend with the same `random_id` returns the same
  message (also from a later session).
- **A dropped future still sends**: `sendMedia` cut by a 20 ms timeout was
  posted anyway. The request is queued in the sender pool and goes out when it
  goes out; "lands within `CALL_DEADLINE` or never" does not hold. Hence the
  order fence above. Why, in grammers-mtsender 0.11: a request
  travels `SenderPoolHandle` → mpsc → `SenderPoolRunner::process_request`
  (which awaits `create_connection` when there is no connection yet: seconds)
  → the connection's mpsc → `Sender::enqueue_body` → `Sender.requests`, and is
  serialized only by `try_fill_write`, i.e. once the previous write (say a
  512 KiB upload part on the same connection) is fully on the socket. The
  answer goes back through a `oneshot`, but nothing on the way checks whether
  its receiver is still there (`tx.is_closed()`), so an abandoned request is
  sent whenever its turn comes. The flood-wait retry loop lives in the
  caller's future, so that part does stop with it.
- **grammers retries flood waits itself** (`AutoSleep`, below a threshold):
  use `NoRetries` for our calls and let the `ChatLimiter` handle
  `FLOOD_WAIT_X` as it handles 429s (one rate-limited error type).
- **Big files**: 60 MB uploaded in 2.3 s, 300 MB in 16 s (~20 MB/s, 4 parallel
  parts); reads at any 4 KiB-aligned offset; `getFile` windows ≤ 512 KiB.
  The bot size ceiling is above our 256 MiB chunk limit (not probed further).
- **File references** (33 bytes) were fresh on every fetch; expiry not
  provoked (re-fetching by `getMessages` on `FILE_REFERENCE_EXPIRED` stays the
  plan).

## Implementation notes (2026-10-28)

Where the code differs from the design above, and why:

- **Claim names carry no `log_id`**: `log-<gen>-claim.bin`,
  `log-<gen>-batch.bin`. A claimant of a fresh log cannot know the id before
  it sends; the first batch fixes it, and of racing fresh claimants only the
  valid one writes a batch. One log per channel.
- **`since` instead of `prev` for validity**: a batch records the last
  message its writer had checked when it sent it (its claim, for the first
  batch of a generation); validity reads `(since, batch)`, `prev` is only the
  chain link (id + crc). A straggler's check reads top-down and stops at the
  first higher claim, so it never reaches collected messages.
- **Empty ids are taken as deleted** after a few re-reads, not `Other`: a live
  check (`tests/live.rs`) found every sent message readable at once, also from
  another session, while any message deleted by hand in a checked range would
  otherwise stop every later claim.
- **`confirm` posts an empty batch** (the top of a channel is only learnt by
  posting); `discover` posts a probe that stays (a deleted one would leave a
  hole); snapshot `newest()` posts a probe and deletes it.
- **Collection** deletes only our messages (claims, batches, probes) below the
  oldest batch a reader needs and below the rebase batch's `since`; foreign
  messages stay.
- **No grammers-client**: only `grammers-mtsender`, `grammers-session` (no
  SQLite) and `grammers-tl-types`; the session is a JSON file of the home DC
  and auth keys, 0600, locked.
- **The grammers patch** is applied to a vendored copy
  (`vendor/grammers-mtsender`, `[patch.crates-io]`): a request whose caller
  is gone is dropped in the pool runner (before and after connecting), when
  the connection task takes it, and in `try_fill_write` before it is
  serialized. A unit test covers the last; a live test
  (`client::abandoned_send_live`) polls a `sendMedia` once and drops it: posted
  without the patch, not with it.
- The log adapter caches document locations and reads the page below a
  missing one, so a replay down its chain reads each page once.

Measured on the test channels: boot 17 s on a fresh log, 24 s with ~50
batches to replay; S3 smoke with every store in a channel 61 s (log appends
are the limit: ~18 sends a minute).

## Text messages (2026-10-28)

Blobs that fit go as **text messages**, the rest as documents; the store
decides (`TelegramBlobStore::with_inline`, on by default), nothing above it
knows (`src/inline.rs`).

Live spike on the test channel:

- The limit is **4096 code points** (`MESSAGE_TOO_LONG` at 4097), not bytes
  or UTF-16 units: 4096 characters outside the BMP (16 KiB of UTF-8) pass.
- Text comes back byte for byte, except whitespace at the ends (stripped)
  and tabs / carriage returns (rewritten); a `\n` inside is kept.
- Reading 10 texts with their payload: 72 ms, one `getMessages`. Reading 10
  small documents: 556 ms (`getMessages` + 10 `getFile`).
- Sending is bound by the channel's message rate either way (~17–20 a
  minute); a text saves the upload calls, not a send.

Encoding: `<name> <len>\n<payload>`, the payload 15 bits a character in
U+20000..U+27FFF (CJK Extension B, all assigned): up to **7595 bytes** per
message with a `BlobId` name. A live test (`tests/live.rs`,
`inline_blobs_against_a_live_channel`) puts 1, 256 and 7595 bytes and reads
them back exactly through a store that cached nothing.

What it changes: log batches, claims, probes and small chunks are texts, so a
replay, a `tail` walk or a sweep reads their bytes with the page that lists
them — a replay of small batches downloads nothing
(`tests/chain_log.rs`). The sweeper and the chain read names from the text's
first line as they read a document's file name.

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. ~~**Spike**~~ Done: a throwaway binary against the test channels (not
   merged); results in Spike findings above.
2. ✅ (without the grammers patch) **Crate** `pigeonhole-storage-telegram-mtproto`. First a patched
   grammers-mtsender (`[patch.crates-io]` to a vendored 0.11.0 copy, and the
   same change offered upstream on codeberg): `try_fill_write` drops requests
   whose result receiver is closed before serializing them, and
   `process_request` / `run_sender` do not enqueue one whose `tx` is already
   closed. Then a request abandoned before its bytes reach the socket is never
   sent; one already written lands within the network's delivery time, as
   over HTTP. Safety does not rest on it (the order fence does); it removes
   useless late sends and makes latency predictable. The log and snapshots get
   a sender pool (connection) of their own, so their sends never queue behind
   blob upload parts. Then the client behind a small trait
   (`getMessages`, `getFile`, `saveBigFilePart` + `sendMedia`,
   `deleteMessages`) with a fake implementation; `BlobBackend` + `Sweepable`;
   the testkit conformance suites and a `typed_backend` test on the fake.
   No wiremock: MTProto is not HTTP.
3. ✅ **Order-fenced log and pinless snapshots** in `pigeonhole-meta`: the
   protocol over the new `ChatApi`, the fake chat (late visibility), tests for
   the fast path, a gap read, stragglers, racing claims, an unconfirmed batch
   becoming the head, rebase and collection. Then the MTProto `ChatApi`
   adapter, and a live check that an id below one returned by a send is never
   `messageEmpty` for long.
4. ✅ **Wiring**: config (`kind = "telegram_mtproto"`, env names, session path),
   `runtime.rs` (`build_member`, `open_log`, `open_snapshots`), startup checks,
   `pigeonhole.toml.example`, `.env.example`.
5. **Live run** (smoke ✅, `compat-telegram-mtproto.sh` written): `scripts/compat-telegram-mtproto.sh` modelled on
   `compat-telegram.sh`; README (backend section, disclaimer still applies),
   meta-backends.md, sweep-invariants.md, glossary. Repeated `sweep_stress`
   and the S3 compat suites from the ground rules at the end.

## Open questions

- The Bot API log cannot read ranges: keep its current pin + description
  protocol beside the new one, or make the Bot API backend blob-only?
- Chunk size is global (`[chunk] size`); with ≫ 20 MiB blobs on one store and
  Discord / Bot API stores beside it, should it become per store?
- Flood limits: measured, the same ~20–30 sends a minute per channel; keep the
  `ChatLimiter` send default (18/min) for both transports.
