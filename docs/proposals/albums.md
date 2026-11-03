# Albums: several files in one send

Status: **proposal**, spiked live on 2026-11-02 (not built). Read
[discord-log.md](../discord-log.md) (the Discord store, its limits),
[telegram-mtproto.md](../telegram-mtproto.md), [shared-limiter.md](../shared-limiter.md)
and [bench/discord-vs-telegram.md](../bench/discord-vs-telegram.md) first.

## Idea

A file larger than a blob is cut into several blobs, one message each: an
object into chunks (`chunk.size`), a chunk into parts (`PartPacker`, by the
store's `max_blob_size`), a snapshot into parts (`BlobSnapshotStore`). Both
services can send several files at once — Discord as attachments of one
message, Telegram as an album (`messages.sendMultiMedia`). Would sending a
file's blobs together save messages, the scarcest resource we have, or send
budget?

Short answer, from the spikes below:

- **Discord: yes, 10× fewer messages.** One message takes 10 attachments of
  20 MiB each, 200 MiB in all. Upload speed does not change: it is bound by
  our uplink, not by the message rate.
- **Telegram: no fewer messages** (an album is 10 messages), but an album
  seems to spend about half the send budget of 10 single sends. A possible
  batching of small blobs, not a way to store large files.

## Spikes

Scripts: [scripts/spike-discord-album.py](../../scripts/spike-discord-album.py)
and `album_probe` in `pigeonhole-telegram-mtproto` (inside the crate: it uses
the client's private calls; `cargo test -p pigeonhole-telegram-mtproto --lib
album_probe -- --ignored --nocapture`). Both post in the test blobs channel
and delete what they posted. One macOS machine, home internet.

### Discord

| Case | Result |
|---|---|
| 10 × 1 MiB in one message | 200, 6.9 s; 10 attachments, in the order sent |
| 11 attachments | 400, `50035`, `attachments: BASE_TYPE_MAX_LENGTH` ("10 or fewer") |
| 5 × 20 MiB (100 MiB) | 200, 49 s (2.0 MiB/s) |
| 10 × 20 MiB (200 MiB) | 200, 94 s (2.1 MiB/s) |
| 2 files, one of 20 MiB + 1 byte | 413, `40005`, after 19 s of upload |
| 3 × 2 MiB twice with one nonce (`enforce_nonce`) | the second send returns the first message, all three attachments |
| 4 messages of 20 MiB at once | 28 s, 2.9 MiB/s together |

- The limit is per file (20 MiB, unboosted guild); no per-message or
  per-request cap showed up to 200 MiB.
- Each attachment has its own CDN URL; `Range` answers 206 with the exact
  bytes, the last byte included; `size` is exact. Whole downloads 17–20 MiB/s.
- `GET /channels/{c}/messages/{m}` lists all attachments with fresh URLs:
  one read locates the whole message.
- A send of 10 attachments takes one unit of the channel's send bucket
  (`Limit: 5`, `Remaining: 4` after it), like a send of one.
- Speed: one request runs at ~2 MiB/s whatever its size, four in parallel at
  2.9 MiB/s in all: the uplink (~25 Mbit/s here) is the bound. The channel
  allows ~1 send a second (5 per ~5 s), 20 MiB/s at 20 MiB a message; we
  upload 7–10 times slower than that, so the message rate never bound a large
  upload, and albums cannot speed it up. With an uplink above ~20 MiB/s they
  would lift that ceiling to ~200 MiB/s.

### Telegram (MTProto, bot)

| Case | Result |
|---|---|
| Uploaded files (`InputMediaUploadedDocument`) straight into `sendMultiMedia` | 400 `MEDIA_INVALID` |
| upload + `messages.uploadMedia` per file, then `sendMultiMedia` of 10 `InputMediaDocument` | ok: 10 messages, consecutive ids, one `grouped_id`, each a document with its file name |
| 11 in one album | 400 `MULTI_MEDIA_TOO_LONG` |
| delete one message of an album | the other 9 stay |
| 10 × `uploadMedia` in a row | 1.8 s with the uploads, no flood wait |
| 8 albums of 10 back to back, no limiter | 80 messages in 43 s; a `FLOOD_WAIT` of 3–5 s before every album but the first |
| 16 single `sendMedia` back to back, no limiter | 16 messages in 19 s; bursts of 5, then `FLOOD_WAIT` 5 s |

- An album is 10 ordinary messages to everything we use (`getMessages`,
  `deleteMessages`, the sweep's listing by id); grouping is a client-side
  rendering of `grouped_id`.
- Send budget: singles go 5 per ~5 s, albums 1 per ~5 s. An album costs
  about 5 single sends and posts 10 messages: ~2× the messages per unit of
  budget. The run was short (under a minute per phase); the sustained rate
  (~20 single sends a minute, with 40 s waits now and then) was not measured
  for albums.
- Message count does not change, and large files need no albums here: a
  Telegram message holds up to `MAX_LOGICAL_CHUNK` (256 MiB), so fewer
  messages for a large file is `chunk.size`, not albums.

## Discord: a blob is a message of up to 10 attachments

The blob layer keeps "one blob = one message"; the Discord store cuts a blob
above 20 MiB into attachments of one message itself.

- `pigeonhole_storage_discord::blob_limits`: `max_blob_size = attachments × attachment_size`
  (default 10 × 20 MiB = 200 MiB). A store option
  `attachments_per_message` (1–10, default 10? see Open questions); the
  existing `max_blob_size` becomes the per-attachment size.
- `put(data, name)`: cut `data` into `ceil(len / attachment_size)` pieces at
  fixed offsets (`k × attachment_size`), send them as `files[0..n]` of one
  message with the blob's name as content and the same nonce as today.
  Pieces are plain byte ranges: the store knows nothing of blocks.
- `get(id, range)`: `locate` keeps the list of `(url, size)` of the
  message; a range maps to one attachment or, crossing a boundary, two
  ranged fetches concatenated. A stale URL refreshes the whole list (one
  message fetch, as now).
- `DiscordId` stays `{ message_id }`; the key stays the message id. Blob
  ids, `BlobLocator`, the metadata, the sweep (`candidates`, by message),
  `exists`, `delete`, repair, the upload pins: unchanged. Every piece of a
  blob lives and dies with it, so a message is never partly dead.
- `DcMessage.attachment: Option<_>` becomes `attachments: Vec<_>` (in
  `pigeonhole-storage-discord` and `pigeonhole-discord-http`), `send` takes a
  list of files; the fake Discord takes lists too. Chain messages (log
  batches, claims) keep one attachment.
- What grows on its own: `PartPacker` cuts parts by `max_blob_size`, so a
  chunk of up to 200 MiB is one message; `BlobSnapshotStore` cuts snapshots
  by it too, 10× fewer parts. `chunk.size` stays a global setting: 19 MiB by
  default (one message on any store); a Discord deployment that wants fewer
  messages raises it (≤ 256 MiB logical, so ~190 MiB stored fits one
  message).

What it costs:

- **One request per blob.** A 200 MiB blob is one ~95 s upload here; a
  failure resends all of it, where ten 20 MiB messages would resend one.
  `enforce_nonce` makes a resend within 3–5 minutes return the posted
  message, and the upload itself fits in that window at ≥ ~1 MiB/s; a
  slower link should get a smaller `attachments_per_message`.
- **Parallelism moves up a level.** Today a chunk's parts go out one after
  another anyway (`PartPacker::flush_open` awaits each put); with one
  message per chunk, overlap comes only from several chunks or objects in
  flight. On this uplink it makes no difference (2.1 vs 2.9 MiB/s).
- **Memory.** A chunk reserves `chunk.size + block_size` of
  `ingest.memory_budget` (256 MiB by default) while it uploads: at
  `chunk.size` ~190 MiB one big PUT at a time, unless the budget grows with
  it. The split itself needs no copy: pieces are `Bytes::slice`s.
- **Mixed placement groups.** `chunk.block_size` and chunks are checked
  against the smallest `max_blob_size` of a group: a group of Discord and
  Telegram members gets 200 MiB from Discord and 256 MiB from Telegram;
  memory stays 20 MiB − 1 and would cut parts small for everyone.

## Telegram: albums as send batching (later, maybe)

Not for large files (above). For many small PUTs the blobs channel is the
bound ([bench](../bench/discord-vs-telegram.md), eight clients: 0.3 sends a
second, uploads waiting 25 s in our limiter). Every message of an album is
its own blob, deletable alone, so unrelated blobs may share an album:

- A put queue per channel: puts waiting for the send budget are taken up to
  10 at a time into one `sendMultiMedia`; each still gets its own message
  id, its own `random_id` and its own name.
- Each file costs an `uploadMedia` before the send (no flood wait seen for
  10 in a row; not measured beyond that).
- A failed album fails all its puts; whether a resend with the same
  `random_id`s returns the posted messages, as `sendMedia` does, is not yet
  known.
- Gain: ~2× messages per unit of send budget on a short run. Worth building
  only if a sustained run confirms it.

## Rejected

- **Attachments as blobs** (`DiscordId { message_id, index }`, parts of
  different chunks packed into one message): parts that die at different
  times would pin a message until the last one dies, the sweep and repair
  work by message, and dropping one attachment is a message edit with its
  own rate limit. Packing only one blob's pieces avoids all of it.
- **Albums for large files on Telegram**: the same number of messages; a
  bigger `chunk.size` does it with one.

## Open questions

- Default `attachments_per_message`: 10 (fewest messages) or a smaller
  number with less to resend after a failure? A boosted guild has larger
  attachments; the per-file size stays `max_blob_size`.
- Does a 200 MiB send hold the channel's send bucket for its whole upload
  (other sends of the channel waiting behind it)? Not seen in the spike:
  the bucket answered `Remaining: 4` after each send, but sends were not
  overlapped with a long one.
- Telegram: the sustained send rate of albums over several minutes, and
  idempotent resends of `sendMultiMedia`.
