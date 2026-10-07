# pigeonhole

Pluggable blob storage with protocol gateways. The first profile is **s3gram**:
an S3-compatible HTTP gateway in Rust, backed by Telegram (or Discord) Bot API.

The S3 surface uses [s3s](https://github.com/s3s-project/s3s). Objects are split
into configurable chunks (default on-wire ≤19 MiB, hard cap `< 20 MiB` for
Telegram `getFile`) and stored as documents in **one** private chat/channel.
Chunk encoding is `raw` | `gzip` | `zstd` (default `zstd`): compressing policies
pack independent 1 MiB blocks into documents (stored codec `blocks`) so each
block is compressed once; on-wire size stays ≤ `chunk.size` (logical ≤ 256 MiB).
Legacy single-blob `raw`/`gzip`/`zstd` chunks remain readable. Object metadata
lives in a local SQLite index.

Storage I/O goes through `LegacyBlobStore` / `LegacyBlobStoreTg` (`storage-telegram` or
`storage-discord` in production, `storage-memory` in tests) so unit tests never
hit a real Bot API. Roadmap gateways: Kafka, WebDAV.

Layout is a Cargo workspace under `crates/` (role dirs + `pigeonhole-*` names):

| Crate | Role |
|---|---|
| `core/pigeonhole-types` | Shared types (`BlobKey`, `Locator`, errors) |
| `core/pigeonhole-codec` | Codecs + `BlockWriter` |
| `blob/pigeonhole-blob` | `LegacyBlobStore` / `LegacyBlobStoreTg`, rate limits, `BootstrapPointer`, cache |
| `blob/pigeonhole-index` | SQLite index (blob-store layer; merging into blob-store later) |
| `blob/pigeonhole-chunk-store` | Ingest, snapshots, config (no protocol crates) |
| `storage/pigeonhole-storage-telegram` | Telegram Bot API storage |
| `storage/pigeonhole-storage-discord` | Discord Bot API storage |
| `storage/pigeonhole-storage-memory` | In-memory storage for tests / `memory = true` |
| `gateway/pigeonhole-gateway-s3` | S3 (`s3s`) gateway |
| `gateway/pigeonhole-gateway-bytestream` | REAPI v2 remote cache |
| `testing/pigeonhole-testkit` | Storage conformance suites |
| `bin/pigeonhole` | Binaries `pigeonhole` and `s3gram` (same wiring; features select storage/gateways) |

Dependency layering is enforced by `./scripts/check-deps.sh --strict` in CI.

## Bot API limits

- Upload (`sendDocument`): 50 MiB
- Download (`getFile`): 20 MiB → `chunk.size` must be `< 20 MiB` (default 19 MiB)
- Sends to a chat are slow (~20/min); `getFile` is much faster. s3gram uses
  **separate** token budgets for send / getFile / delete; CDN byte downloads are
  only capped by a connection semaphore. Resolved `file_path` values are kept in
  an in-memory LRU so repeated chunk reads do not call `getFile` again.

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and get a `BOT_TOKEN`.
2. Create a private channel or group, add the bot as **administrator** with:
   - **Channel:** permission to **edit messages** (needed to pin).
   - **Group / supergroup:** permission to **pin messages**.
   - Pinning in a group also posts a Telegram service message — expected noise.
3. Copy config + secrets:

```bash
cp s3gram.toml.example s3gram.toml
cp .env.example .env
# edit s3gram.toml (chat_id, listen_addr, chunk, …)
# edit .env (BOT_TOKEN, AWS_* credentials)
```

Non-secret settings live in **`s3gram.toml`** (path override: `S3GRAM_CONFIG`).
Secrets stay in **`.env` / environment**: `BOT_TOKEN`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`.

On startup s3gram calls `getMe` + `getChatMember` and exits if the bot is not
an admin with the pin/edit rights above.

## Run

```bash
cargo run --release
# or: make run
```

Listens on `http://0.0.0.0:8333` by default (`listen_addr` in TOML).

### Discord backend

Discord is enabled by default (`--features discord`). Point the binary at a channel:

```toml
[backend]
kind = "discord"

[discord]
channel_id = "123456789012345678"
# optional: max_blob_size = 10485760
```

```bash
# .env
DISCORD_BOT_TOKEN=...
AWS_ACCESS_KEY_ID=s3gram
AWS_SECRET_ACCESS_KEY=s3gramsecret

cargo run --release
# manual live suite (not CI): make compat-discord
```

The bot needs permission to view the channel, send messages, attach files, read
history, and pin messages. Attachment CDN URLs are cached (~50 min) and refreshed
on 403/404. Default `max_blob_size` is ~10 MiB (Discord limit minus margin) —
set `[chunk].size` accordingly.

### Bazel / Buck2 remote cache (REAPI)

Build with the `bytestream` feature, enable `[bytestream]` in `s3gram.toml`, then
point Bazel at the gRPC listener (separate port from S3):

```bash
cargo run --release --features bytestream
# s3gram.toml: [bytestream] enabled = true, listen_addr = "127.0.0.1:8980"

bazel build --remote_cache=grpc://127.0.0.1:8980 //your/target
```

The server exposes REAPI v2 `Capabilities`, `ContentAddressableStorage`,
`ActionCache`, and `google.bytestream.ByteStream` (SHA256 digests, cache-only).

## Configuration

See [`s3gram.toml.example`](s3gram.toml.example):

| TOML | Meaning |
|---|---|
| `[backend].kind` | `telegram` (default) \| `discord` |
| `chat_id` | Telegram chat/channel (or fallback Discord channel id) |
| `[discord].channel_id` | Discord channel for blobs + pins |
| `listen_addr` | Bind address |
| `database_url` | SQLite URL |
| `region` | SigV4 region string |
| `memory` | `true` → MemoryBlobStore (no Telegram/Discord) |
| `[snapshot].interval_secs` | Auto snapshot period (`0` disables) |
| `[chunk].size` | Max **on-wire** chunk size (`< 20 MiB`) |
| `[chunk].codec` | `raw` \| `gzip` \| `zstd` |
| `[telegram].send_*` | Budget for sendDocument / sendMessage / pin |
| `[telegram].get_file_*` | Budget for getFile |
| `[telegram].delete_*` | Budget for deleteMessage (purge / GC) |
| `[telegram].*_concurrency` | Upload / download connection semaphores |
| `[bytestream].enabled` | REAPI remote cache gRPC (requires `--features bytestream`) |
| `[bytestream].listen_addr` | gRPC bind address (default `127.0.0.1:8980`) |

Legacy `[telegram].rate_per_sec` / `rate_burst` map to `send_*`.

## Tests / CI

GitHub Actions (`.github/workflows/ci.yml`) runs `cargo test --workspace` plus the
four MemoryBlobStore client suites: `compat-memory` (ceph/s3-tests known-good),
`compat-s3s-boto3`, `compat-s3s-e2e`, and `compat-rclone`.

```bash
# unit + in-process S3 API tests (MemoryBlobStore, no Telegram)
cargo test --workspace
# or: make test

# smoke against a running server
make smoke

# ceph/s3-tests against a temporary MemoryBlobStore server (no Telegram)
make compat-memory           # curated known-good (compat/known-good.txt)
./scripts/compat-memory.sh --all

# s3s upstream tests/boto3 (presigned POST, Content-Length edge cases)
make compat-s3s-boto3

# s3s-e2e suite (Basic + Advanced) against MemoryBlobStore
make compat-s3s-e2e
make compat-s3s-e2e ARGS='--filter ^Basic'

# s3s upstream rclone S3 e2e against MemoryBlobStore (downloads pinned rclone)
make compat-rclone

# Real Telegram once: purge tracked messages, wipe index, run all client suites
make compat-telegram
# optional: full ceph/s3-tests functional (slow / many failures expected)
# make compat-telegram ARGS=--all

# Wipe Telegram messages tracked by the local index (server must be stopped)
cargo run --release -- purge --yes
# safer: also confirm chat / planned delete count
# cargo run --release -- purge --yes --expect-chat "$CHAT_ID" --expect-messages N
```

Point the AWS CLI at the gateway:

```bash
export AWS_ACCESS_KEY_ID=s3gram
export AWS_SECRET_ACCESS_KEY=s3gramsecret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./file.bin s3://demo/file.bin
aws --endpoint-url http://127.0.0.1:8333 s3 cp s3://demo/file.bin ./out.bin
```

Authentication is AWS SigV4 via s3s `SimpleAuth` (keys from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`).

## Index snapshot

s3gram periodically exports the SQLite index as gzip Telegram document(s) in
`chat_id`. A small **immutable JSON manifest** is sent and **pinned**; that pin
is the bootstrap pointer (no need to remember `file_id`s after disk loss).

Flow on change: upload parts → send manifest → pin new → unpin/delete old.
Uploads happen only when the index hash changes. Interval:
`[snapshot].interval_secs` (default 300; `0` disables).

Restore on a stopped server / clean machine (`BOT_TOKEN` in `.env` + `chat_id` in TOML):

```bash
# stop s3gram first; refuses a non-empty index unless --force
cargo run --release -- restore
# cargo run --release -- restore --force
# optional legacy: cargo run --release -- restore [--force] <file_id>
```

After a pin restore, meta records the current generation so the next snapshot can
unpin/delete the previous parts. You can also copy the local `s3gram.db` file.

## Supported S3 ops (subset)

| Operation | Status |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket / HeadBucket | yes |
| GetBucketLocation | yes |
| PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects | yes |
| ListObjects (v1) / ListObjectsV2 | yes |
| ListObjectVersions (non-versioned stub, `VersionId=null`) | yes |
| Multipart (Create / UploadPart / UploadPartCopy / Complete / Abort / ListParts / ListMultipartUploads) | yes |
| GetObject Range (streaming) | yes |
| CopyObject (shallow — reuses Telegram file_ids) | yes |
| Object tagging (Put/Get/Delete + header on Put/CreateMultipart) | yes |
| Content-MD5 (`InvalidDigest` / `BadDigest`) / CRC32 on Put/UploadPart | yes |
| User metadata (`x-amz-meta-*`) | yes |
| Zero-byte objects (no Telegram upload) | yes |
| Blob refcount in SQLite | yes |
| `memory = true` (in-memory LegacyBlobStoreTg, no Telegram) | yes |
| Separate send / getFile / delete budgets + upload/download semaphores | yes |
| In-memory LRU cache for Telegram `file_path` | yes |
| Presigned URLs / ACL / bucket versioning | later |

## Limitations / ops notes

- **One writer instance per SQLite file.** Concurrent s3gram processes on the same
  `database_url` are unsupported (index corruption / lock errors).
- **RPO ≈ snapshot interval.** Index durability to Telegram is the pin + snapshot
  parts; between snapshots a crash can lose recent index mutations (blob bytes may
  already be in the chat). Tune `[snapshot].interval_secs`.
- **`file_id` is bot-bound.** Telegram `file_id` values are only valid for the bot
  that uploaded them; rotating `BOT_TOKEN` without a restore from that bot’s pin
  will break reads.
- **ToS.** Storing arbitrary object data in Telegram/Discord must comply with their
  Terms of Service and channel/server policies. This project is a technical gateway,
  not a blessing to ignore those rules.
- **Discord / ByteStream** are optional (`--features discord`, `--features bytestream`).

## Notes

- Telegram `file_id` can become invalid; keep index snapshots.
- Large objects are split automatically; multipart uploads map to sequential chunks.
- HTTP server uses tower `TimeoutLayer`, `ConcurrencyLimitLayer`, and a header-count
  guard (`[http]` in TOML). Backend/cache counters log on an interval via tracing.
