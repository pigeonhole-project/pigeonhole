# Telegram over MTProto (bot token)

Status: **proposal**. A brief for implementing a second Telegram backend that
talks MTProto instead of the Bot API, still logged in as a **bot** (no user
account). Read [roadmap.md](roadmap.md) § Ground rules,
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
- The chat log and chat snapshots stop pinning, for **both** transports (the
  Bot API one keeps working through `forwardMessage`, or is dropped from the
  log/snapshot role — decide in step 3, see Open questions).
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
  startup; check the bot is an admin with post, delete, edit-info rights.
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
  `messages.sendMedia` with a random `random_id` (the server deduplicates a
  resend with the same `random_id`, so a send may be retried after a network
  error). Return the new `message_id`.
- **get**: `upload.getFile` in windows: `offset % 4096 == 0`,
  `limit % 4096 == 0`, `1 MiB % limit == 0`, a window never crosses a 1 MiB
  boundary. A range is rounded out to windows and sliced. Pass
  `cdn_supported = false`; follow `FILE_MIGRATE_X` (export/import auth to the
  file's DC; grammers may do this already — check).
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

### Log and snapshots without pins

The chat log protocol (`meta/pigeonhole-meta/src/log/chat.rs`) changes:

- `ChatApi` loses `pin` and `Registers::pinned` / `pinned_other`; it gains
  `top_id()` (the newest message id in the chat) and
  `get_docs(ids) -> Vec<Option<ChatDoc>>` (documents among those ids). The
  description register stays (it is the fence).
- **head** = the newest log message of this log by message id. In a channel
  id order is sending order, which is what "newest pinned by sending date"
  meant.
- **append(g)**: send the batch linked to the current head, read the
  description: `Ok` only if it still holds `g`. No pin.
- **claim(g)**: write the description, wait out the straggler window (as now),
  then find the head: `top_id()`, walk back in batches of 100 with
  `get_docs` to the first message that decodes as a log message of this
  `log_id`; follow `prev` links as now.
- **Stragglers**: unchanged reasoning. A deposed writer's batch landing inside
  the window is the newest by id and links to its own complete chain (as its
  pin did). One landing after the window is overtaken by the next message of
  the new generation, which has a higher id.
- **Semantics change to document**: today a message sent but never pinned
  (writer died between send and pin) cannot become the head; without pins it
  can. Entries never acknowledged may survive, which a Raft log allows. State
  it in meta-backends.md and cover it with a fake-chat test.
- `ChatSnapshotStore`: the newest snapshot is the newest snapshot document by
  id, found the same way; no pin, no "pinned message is not ours" check
  (foreign messages are skipped by decoding).
- `chat/fake.rs` follows: ids grow, `top_id`, `get_docs`, no pins.
- `top_id()` for MTProto: cheapest way to be found in the spike (probing
  `getMessages` beyond the top returns `messageEmpty`; deleted ids look the
  same, so probe forward from a known id, or send a message — a claim writes
  anyway). For the Bot API: send a message and take its id.

## Steps

Each step ends with `cargo test --workspace` and
`./scripts/check-deps.sh --strict` green; one logical change per commit.

1. **Spike** (throwaway binary or `examples/`, not merged as is), against a
   real test channel (ids/token: see the local `.env`): bot login over
   grammers; `channels.getMessages` on a channel; upload / download of a
   ≥ 50 MiB document and the real size ceiling for a bot; ranged `getFile`;
   provoke `FILE_REFERENCE_EXPIRED` (or confirm references survive);
   `FLOOD_WAIT` behaviour at our send rates; **what grammers does with a
   request whose future is dropped** (if it is still sent later, the
   straggler window's premise "a send lands within `CALL_DEADLINE` or never"
   breaks — then the log needs its own connection that is torn down at the
   deadline); cheapest `top_id()`. Write the findings into this doc.
2. **Crate** `pigeonhole-storage-telegram-mtproto`: client behind a small trait
   (`getMessages`, `getFile`, `saveBigFilePart` + `sendMedia`,
   `deleteMessages`) with a fake implementation; `BlobBackend` + `Sweepable`;
   the testkit conformance suites and a `typed_backend` test on the fake.
   No wiremock: MTProto is not HTTP.
3. **Log/snapshots without pins** in `pigeonhole-meta` (new `ChatApi`, fake chat,
   tests incl. the unpinned-head case), then the adapters: MTProto `ChatApi`,
   and either a Bot API `ChatApi` via `forwardMessage` or the Bot API backend
   dropped from the log/snapshot role.
4. **Wiring**: config (`kind = "telegram_mtproto"`, env names, session path),
   `runtime.rs` (`build_member`, `open_log`, `open_snapshots`), startup checks,
   `pigeonhole.toml.example`, `.env.example`.
5. **Live run**: `scripts/compat-telegram-mtproto.sh` modelled on
   `compat-telegram.sh`; README (backend section, disclaimer still applies),
   meta-backends.md, sweep-invariants.md, glossary. Repeated `sweep_stress`
   and the S3 compat suites from the ground rules at the end.

## Open questions

- Does the Bot API backend keep the log/snapshot role (needs `forwardMessage`
  to a scratch chat to read the head) or become blob-only?
- Chunk size is global (`[chunk] size`); with ≫ 20 MiB blobs on one store and
  Discord / Bot API stores beside it, should it become per store?
- Flood limits for a bot over MTProto vs the Bot API: same per-chat ~20
  messages/min for sends? Measure before choosing `ChatLimiter` defaults.
