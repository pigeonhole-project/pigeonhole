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
> For tests and development use `memory = true` — no external service involved.
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
Metadata lives in local SQLite and is continuously made durable back into the
backend, so a clean machine can recover everything from a pinned message.

Architecture terms: [docs/glossary.md](docs/glossary.md) (Instance, Blob, Block,
Chunk, Replica, Extent, Root, Superblock). GC rules:
[docs/sweep-invariants.md](docs/sweep-invariants.md).

## Layout

Cargo workspace under `crates/` (role dirs + `pigeonhole-*` names):

| Crate | Role |
|---|---|
| `core/pigeonhole-types` | Shared types (`BlobKey`, `Locator`, errors) |
| `core/pigeonhole-codec` | Codecs + `BlockWriter` |
| `blob/pigeonhole-blob` | `BlobBackend`, `Replicated`, rate limits, `BootstrapPointer`, cache knobs |
| `blob/pigeonhole-chunk-store` | `ChunkStore`, ingest, roots, durability, sweeper, repair, config (no protocol crates) |
| `storage/pigeonhole-storage-telegram` | Telegram Bot API storage |
| `storage/pigeonhole-storage-discord` | Discord Bot API storage |
| `storage/pigeonhole-storage-memory` | In-memory storage for tests / `memory = true` |
| `gateway/pigeonhole-gateway-s3` | S3 (`s3s`) gateway |
| `gateway/pigeonhole-gateway-bytestream` | REAPI v2 remote cache |
| `testing/pigeonhole-testkit` | Storage conformance suites |
| `bin/pigeonhole` | The `pigeonhole` binary (features select storage/gateways) |

Dependency layering is enforced by `./scripts/check-deps.sh --strict` in CI.

## Quick start (no external services)

```bash
cp pigeonhole.toml.example pigeonhole.toml
cp .env.example .env
# in pigeonhole.toml: memory = true
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
3. Set `chat_id` (and `memory = false`) in `pigeonhole.toml`, `BOT_TOKEN` in `.env`.

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
[backend]
kind = "discord"

[discord]
channel_id = "123456789012345678"
# optional: max_blob_size = 10485760
```

Secret: `DISCORD_BOT_TOKEN` in `.env`. The bot needs permission to view the
channel, send messages, attach files, read history, and pin messages.
Attachment CDN URLs are cached (~50 min) and refreshed on 403/404. Default
`max_blob_size` is ~10 MiB (Discord limit minus margin) — set `[chunk].size`
accordingly.

## Multiple instances / replication

Instead of the single-backend `chat_id` / `[backend]` form, declare several
`[[instances]]` (tokens are referenced by env var name, never inline):

```toml
[[instances]]
id = "tg-main"
kind = "telegram"
bot_token_env = "BOT_TOKEN"
chat_id = -1001234567890
role = "read-write"   # read-write | read-only | retired

[[instances]]
id = "dc-backup"
kind = "discord"
bot_token_env = "DISCORD_BOT_TOKEN"
channel_id = 123456789012345678
role = "read-write"
```

Writes go to the read-write instances; reads pick the cheapest available
replica. The `[repair]` task scrubs for under-replicated chunks and backfills
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
| `chat_id`, `[backend]`, `[discord]` | Single-backend form (synthesizes instance `default`) |
| `[[instances]]` | Multi-backend form: id, kind, token env, chat/channel, role |
| `listen_addr`, `region` | S3 bind address, SigV4 region |
| `database_url` | SQLite URL of the S3 index (chunk metadata lives in a sibling `*-blob.db`) |
| `memory` | `true` → in-memory backend (no Telegram/Discord) |
| `[snapshot]` | `interval_secs`: S3 index snapshot / checkpoint period (`0` disables snapshots) |
| `[chunk]` | `size` (max on-wire), `codec`, `block_size` |
| `[ingest]` | `memory_budget`: cap on ingest buffers across concurrent PUTs |
| `[telegram]` | Separate send / getFile / delete budgets, upload/download concurrency |
| `[cache]` | `file_path` LRU, L1 unpacked blocks, L2 memory/disk blob cache |
| `[sweep]` | Garbage collector: `grace_secs`, `interval_secs`, `batch_size` |
| `[repair]` | Replica repair/backfill: interval, batch, cost limit, `scrub` |
| `[http]` | Body/header timeouts, concurrency limit, max headers |
| `[metrics]` | Prometheus `/metrics` (feature `metrics-prometheus`, on by default) |
| `[bytestream]` | REAPI gRPC remote cache (feature `bytestream`) |

Legacy `[telegram].rate_per_sec` / `rate_burst` map to `send_*`.

## Durability and restore

Two pieces of state are made durable in the backend:

- **Chunk metadata** (`blob.db`: chunks, parts, refcounts, roots) — changes are
  appended to a journal flushed every **5 s**, and a full checkpoint is written
  every `[snapshot].interval_secs` (min 60 s; 300 s when snapshots are off).
- **S3 index** (buckets, keys, object metadata → extent lists) — exported as a
  gzip snapshot stored as an ordinary chunk-store object and published as the
  `s3/index` root every `[snapshot].interval_secs` (only when it changed).

Each instance has a pinned **superblock** message (generation, checkpoint and
journal locations, roots); that pin is the bootstrap pointer, so no `file_id`s
need to be remembered after disk loss. Publishing is fenced by generation so a
stale writer cannot overwrite a newer superblock.

**RPO:** chunk metadata ≈ 5 s; the S3 namespace ≈ `[snapshot].interval_secs`.
Objects written after the last index snapshot lose their keys on a crash; their
chunks stay referenced in `blob.db` and are currently leaked (not reclaimed).

Restore on a stopped server / clean machine (tokens in `.env`, instances in TOML):

```bash
# refuses a non-empty index unless --force
cargo run --release -- restore
```

`restore` rebuilds both `blob.db` and the S3 index. Normal startup only
restores `blob.db` automatically (when it is empty and a superblock is pinned);
the S3 index needs an explicit `restore`.

Other commands:

```bash
# delete every backend message tracked by the local index (server must be stopped)
cargo run --release -- purge --yes [--expect-chat CHAT_ID]

# one-off migration of a pre-chunk-store index into blob.db (also done on startup)
cargo run --release -- migrate [--dry-run]
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
chat/channel and see the disclaimer):

```bash
make compat-telegram
make compat-discord
```

## Limitations / ops notes

- **One writer process per database.** Concurrent pigeonhole processes on the
  same `database_url` are unsupported.
- **`file_id` is bot-bound.** Telegram `file_id`s are only valid for the bot
  that uploaded them; rotating `BOT_TOKEN` breaks reads of existing data.
- **Single tenant.** One access key pair, no ACLs.
- HTTP uses tower timeouts, a concurrency limit and a header-count guard
  (`[http]`). Backend/cache counters are logged periodically and, with
  `[metrics] enabled = true`, exported for Prometheus.

## License

See [LICENSE](LICENSE).
