# pigeonhole

> **⚠️ Educational project.** pigeonhole is a learning exercise in building a
> storage system (chunking, replication, refcounting, garbage collection,
> crash recovery) on top of deliberately unusual backends. It is **not** meant
> for storing real data, and it is **not** an invitation to use Telegram or
> Discord as free file hosting. Using a messenger as a general-purpose storage
> backend may violate the [Telegram Terms of Service](https://telegram.org/tos),
> the [Telegram Bot API Terms](https://telegram.org/tos/bot-developers),
> the [Discord Terms of Service](https://discord.com/terms) and the
> [Discord Developer Policy](https://discord.com/developers/docs/policies-and-agreements/developer-policy).
> If you run it against a real bot, keep it to small, short-lived test data in
> a private chat you own, and make sure your usage complies with those terms.
> For tests and development use a `kind = "memory"` instance — no external service involved.
> The authors take no responsibility for misuse.

Pluggable blob storage with protocol gateways, written in Rust. The main
gateway is S3-compatible HTTP (via [s3s](https://github.com/s3s-project/s3s));
a Bazel/Buck2 remote cache (REAPI v2) gateway is optional. Bytes are stored as
documents/attachments through the Telegram or Discord Bot API, or in memory.

Objects are split into **chunks** (default on-wire ≤ 19 MiB, hard cap
`< 20 MiB` for Telegram `getFile`). Chunk encoding is `raw` | `gzip` | `zstd`
(default `zstd`): compressing policies pack independent 1 MiB **blocks** into
backend messages so each block is compressed once and Range reads only touch
the blocks they need. Chunks are refcounted, so `CopyObject` shares the
source's chunks instead of re-sending bytes (there is no content dedup).

A chunk can have **replicas** on several **instances** (bot + chat/channel,
Telegram and/or Discord). A background repair task re-replicates
under-replicated chunks; a sweeper deletes unreferenced backend messages.
Metadata is a log of commands (a tablet, see
[docs/meta-backends.md](docs/meta-backends.md)); the state is rebuilt from the
log on every boot.

Architecture terms: [docs/glossary.md](docs/glossary.md) (Instance, Blob, Block,
Chunk, Replica, Extent, Tablet, LogStore, Snapshot). GC rules:
[docs/sweep-invariants.md](docs/sweep-invariants.md).

## Layout

Cargo workspace under `crates/` (role dirs + `pigeonhole-*` names):

| Crate | Role |
|---|---|
| `core/pigeonhole-types` | Shared types (`BlobKey`, `Locator`, errors) |
| `core/pigeonhole-codec` | Codecs + `BlockWriter` |
| `meta/pigeonhole-meta` | Metadata tablet: layer commands in a fencing `LogStore`, applied to in-memory `StateMachine`s (`MetaStore`) |
| `blob/pigeonhole-blob` | `BlobBackend`, `Replicated`, rate limits, `BootstrapPointer`, cache knobs |
| `blob/pigeonhole-chunk-store` | `ChunkStore`, the `chunk` namespace, ingest, sweeper, repair, config (no protocol crates) |
| `storage/pigeonhole-storage-telegram` | Telegram Bot API storage |
| `storage/pigeonhole-storage-discord` | Discord Bot API storage |
| `storage/pigeonhole-storage-memory` | In-memory blob storage for tests / `kind = "memory"` stores |
| `storage/pigeonhole-storage-disk` | Blobs as files in a directory (`kind = "local_disk"`) |
| `gateway/pigeonhole-gateway-s3` | S3 (`s3s`) gateway |
| `gateway/pigeonhole-gateway-bytestream` | REAPI v2 remote cache |
| `testing/pigeonhole-testkit` | Storage conformance suites |
| `bin/pigeonhole` | The `pigeonhole` binary (features select storage/gateways) |

Dependency layering is enforced by `./scripts/check-deps.sh --strict` in CI.

## Quick start (no external services)

```bash
cp pigeonhole.toml.example pigeonhole.toml
cp .env.example .env
# in pigeonhole.toml replace the "tg-main" [[store]] with a memory one:
#   [[store]]
#   id = "tg-main"
#   kind = "memory"
cargo run --release
```

Listens on `http://0.0.0.0:8333` by default (`listen_addr`). Point the AWS CLI
at it:

```bash
export AWS_ACCESS_KEY_ID=pigeonhole
export AWS_SECRET_ACCESS_KEY=pigeonholesecret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./file.bin s3://demo/file.bin
aws --endpoint-url http://127.0.0.1:8333 s3 cp s3://demo/file.bin ./out.bin
```

Authentication is AWS SigV4 via s3s `SimpleAuth` (keys from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`; the defaults above are for local use only).

Non-secret settings live in **`pigeonhole.toml`** (path override:
`PIGEONHOLE_CONFIG`). Secrets stay in **`.env` / environment**: `BOT_TOKEN`,
`DISCORD_BOT_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

## Telegram backend

Read the disclaimer at the top first.

1. Create a bot with [@BotFather](https://t.me/BotFather) and get a `BOT_TOKEN`.
2. Create a private channel or group, add the bot as **administrator** with:
   - **Channel:** permission to **edit messages** (needed to pin).
   - **Group / supergroup:** permission to **pin messages**.
   - Pinning in a group also posts a Telegram service message — expected noise.
3. Set the instance `chat_id` in `pigeonhole.toml` (see the example), `BOT_TOKEN` in `.env`.

On startup pigeonhole calls `getMe` + `getChatMember` and exits if the bot is not
an admin with the pin/edit rights above.

Bot API limits that shape the design:

- Upload (`sendDocument`): 50 MiB; download (`getFile`): 20 MiB → `chunk.size`
  must be `< 20 MiB` (default 19 MiB).
- Sends to a chat are slow (~20/min); `getFile` is much faster. pigeonhole uses
  **separate** token budgets for send / getFile / delete; CDN downloads are only
  capped by a connection semaphore. Resolved `file_path` values are kept in an
  in-memory LRU so repeated chunk reads do not call `getFile` again.

## Discord backend

Discord is enabled by default (`--features discord`):

```toml
[[store]]
id = "dc"
kind = "discord"
channel_id = 123456789012345678
# optional: max_blob_size = 10485760

[chunk]
store = "dc"
```

Secret: `DISCORD_BOT_TOKEN` in `.env` (or the env var named by `bot_token_env`). The bot needs permission to view the
channel, send messages, attach files, read history, and pin messages.
Attachment CDN URLs are cached (~50 min) and refreshed on 403/404. Default
`max_blob_size` is ~10 MiB (Discord limit minus margin) — set `[chunk].size`
accordingly.

## Stores and replication

Every place bytes can live is a `[[store]]` (`local_disk`, `memory`,
`telegram`, `discord`; tokens are referenced by env var name, never inline). Stores are
combined into `[[replicas]]` groups, and `[meta] log` / `snapshot` and
`[chunk] store` name a store or a group:

```toml
[[store]]
id = "tg-main"
kind = "telegram"
bot_token_env = "BOT_TOKEN"
chat_id = -1001234567890

[[store]]
id = "dc-backup"
kind = "discord"
bot_token_env = "DISCORD_BOT_TOKEN"
channel_id = 123456789012345678

[[replicas]]
id = "blobs"
members = ["tg-main", "dc-backup"]
write_quorum = 1

[chunk]
store = "blobs"
```

Writes go to every member of the group; reads pick the cheapest available
replica. A chat serves one purpose (log, snapshots or blobs).

`[[shards]]` spreads chunks instead of copying them: each chunk goes to one
member (the least loaded), a store or a `[[replicas]]` group. Telegram limits
are per chat, so more blob chats mean more throughput; a replica group as a
shard keeps that shard readable when one provider is down. Repair keeps each
chunk on every member of its own shard.

```toml
[[shards]]
id = "spread"
members = ["tg-main", "tg-second", "blobs"]   # "blobs" = the replica group above

[chunk]
store = "spread"
``` The `[repair]` task scrubs for under-replicated chunks and backfills
them (re-packing parts when instance blob limits differ).

## Bazel / Buck2 remote cache (REAPI)

Build with the `bytestream` feature, enable `[bytestream]` in `pigeonhole.toml`,
then point Bazel at the gRPC listener (separate port from S3):

```bash
cargo run --release --features bytestream
# pigeonhole.toml: [bytestream] enabled = true, listen_addr = "127.0.0.1:8980"

bazel build --remote_cache=grpc://127.0.0.1:8980 //your/target
```

The server exposes REAPI v2 `Capabilities`, `ContentAddressableStorage`,
`ActionCache`, and `google.bytestream.ByteStream` (SHA256 digests, cache-only).

## Configuration

See [`pigeonhole.toml.example`](pigeonhole.toml.example) for every key with
comments. Sections:

| TOML | Meaning |
|---|---|
| `[[store]]` | Where bytes live: id, kind (`local_disk` + `path` / `memory` / `telegram` + `chat_id` / `discord` + `channel_id`), token env, `max_blob_size`. Every kind holds blobs; the log and snapshots go to `local_disk`, `memory` or `telegram` |
| `[[replicas]]` | A group of blob stores holding a copy each: `members`, `write_quorum` |
| `[[shards]]` | Chunks spread over `members` (stores or replica groups), one member per chunk |
| `listen_addr`, `region` | S3 bind address, SigV4 region |
| `[meta]` | Metadata tablet: `log` / `snapshot` (store ids), group commit (`batch_max_delay_ms` / `batch_max_entries`), `snapshot_every_entries` / `snapshot_every_secs` (`0` = never), `log_straggler_window_secs` |
| `[chunk]` | `store` (a blob store, replica group or shards), `size` (max on-wire), `codec`, `block_size` |
| `[ingest]` | `memory_budget`: cap on ingest buffers across concurrent PUTs |
| `[telegram]` | Separate send / getFile / delete budgets, upload/download concurrency |
| `[cache]` | `file_path` LRU, L1 unpacked blocks, L2 memory/disk blob cache |
| `[sweep]` | Garbage collector: `interval_secs`, `batch_size` |
| `[repair]` | Replica repair/backfill: interval, batch, cost limit, `scrub` |
| `[http]` | Body/header timeouts, concurrency limit, max headers |
| `[metrics]` | Prometheus `/metrics` (feature `metrics-prometheus`, on by default) |
| `[bytestream]` | REAPI gRPC remote cache (feature `bytestream`) |

Unknown keys are rejected, so a stale config fails at startup instead of being
half-applied.

## Durability and restore

Every metadata change is a layer command. `pigeonhole-meta` appends it to the
command log (`[meta] log`, fsynced per batch) and then applies it to the
in-memory state of its layer (`chunk`, `s3`, `cas`); the log is the source of
truth and every boot rebuilds the state by replaying it (see
[docs/meta-backends.md](docs/meta-backends.md)). There is no local database.

Every `[meta] snapshot_every_entries` entries (and every `snapshot_every_secs`
when anything changed) the tablet snapshots all states into the snapshot store and
drops the log behind it, so boot = that snapshot + the log after it. Log and
snapshots are configured separately:

| `[meta] log` | `[meta] snapshot` | losing this host's disk |
|---|---|---|
| a `local_disk` store | the same store (default) | loses the metadata |
| a `local_disk` store | a `local_disk` on another disk, or a `telegram` store | `restore --lossy`: loses the changes after the newest snapshot |
| a `telegram` store | another `telegram` store | loses nothing: a clean machine boots from the chats |

After losing a local log whose snapshots survived:

```bash
# starts an empty [meta] log from the newest snapshot; changes after it are lost
cargo run --release -- restore --lossy
```

Chunks committed after that snapshot become orphans (the sweeper deletes them).
Objects in it read back whole: when snapshots outlive the log, the sweeper
deletes a chunk's parts only after a stored snapshot shows the chunk released.
Starting the server on an empty log next to stored snapshots is refused
instead of booting with empty metadata.

Delete every backend message tracked by the local index (server must be stopped):

```bash
cargo run --release -- purge --yes [--expect-chat CHAT_ID]
```

## Supported S3 ops (subset)

| Operation | Status |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket / HeadBucket | yes |
| GetBucketLocation | yes |
| PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects | yes |
| ListObjects (v1) / ListObjectsV2 | yes |
| ListObjectVersions (non-versioned stub, `VersionId=null`) | yes |
| Multipart (Create / UploadPart / UploadPartCopy / Complete / Abort / ListParts / ListMultipartUploads) | yes |
| GetObject Range (streaming, block-granular) | yes |
| CopyObject (shallow — shares refcounted chunks) | yes |
| Object tagging (Put/Get/Delete + header on Put/CreateMultipart) | yes |
| Content-MD5 (`InvalidDigest` / `BadDigest`) / CRC32 on Put/UploadPart | yes |
| User metadata (`x-amz-meta-*`) | yes |
| Zero-byte objects (no backend upload) | yes |
| Presigned URLs / ACL / bucket versioning | later |

## Tests / CI

GitHub Actions (`.github/workflows/ci.yml`) runs `cargo test --workspace`, the
dependency layering check, and four in-memory client suites: `compat-memory`
(ceph/s3-tests known-good), `compat-s3s-boto3`, `compat-s3s-e2e`, and
`compat-rclone`. None of them touch Telegram or Discord.

```bash
cargo test --workspace         # or: make test
make smoke                     # AWS CLI smoke against a running server

make compat-memory             # ceph/s3-tests, curated (compat/known-good.txt)
./scripts/compat-memory.sh --all
make compat-s3s-boto3          # s3s upstream boto3 tests
make compat-s3s-e2e            # s3s-e2e (Basic + Advanced)
make compat-s3s-e2e ARGS='--filter ^Basic'
make compat-rclone             # rclone S3 e2e (downloads pinned rclone)
```

Manual live suites (not in CI; they purge the chat first — use a throwaway test
chat/channel and see the disclaimer). They read `CHAT_ID` / `DISCORD_CHANNEL_ID`
from `.env`:

```bash
make compat-telegram
make compat-discord
```

## Limitations / ops notes

- **One active process per log.** A process booting on the same `[meta] log`
  claims a newer generation and fences the running one, which exits. The
  `local` log fences only processes on the same host.
- **`file_id` is bot-bound.** Telegram `file_id`s are only valid for the bot
  that uploaded them; rotating `BOT_TOKEN` breaks reads of existing data.
- **Single tenant.** One access key pair, no ACLs.
- HTTP uses tower timeouts, a concurrency limit and a header-count guard
  (`[http]`). Backend/cache counters are logged periodically and, with
  `[metrics] enabled = true`, exported for Prometheus.

## License

See [LICENSE](LICENSE).
