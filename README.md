# s3gram

S3-compatible HTTP gateway in Rust, backed by the Telegram Bot API.

The S3 protocol surface is implemented with
[s3s](https://github.com/s3s-project/s3s). Objects are split into configurable
chunks (default on-wire ≤19 MiB, hard cap `< 20 MiB` for Telegram `getFile`) and
stored as documents in **one** private Telegram chat/channel. Chunk encoding is
`raw` | `gzip` | `zstd` (default `zstd`): compressing policies pack independent
1 MiB frames into Telegram documents (stored codec `frames`) so each block is
compressed once; on-wire size stays ≤ `chunk.size` (logical ≤ 256 MiB). Legacy
single-blob `raw`/`gzip`/`zstd` chunks remain readable. Object metadata lives in
a local SQLite index.

Telegram I/O goes through a `BlobStore` trait (`TelegramBlobStore` in production,
`MemoryBlobStore` in unit tests) so tests never hit the real Bot API.

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

## Configuration

See [`s3gram.toml.example`](s3gram.toml.example):

| TOML | Meaning |
|---|---|
| `chat_id` | Telegram chat/channel for blobs + snapshots |
| `listen_addr` | Bind address |
| `database_url` | SQLite URL |
| `region` | SigV4 region string |
| `memory` | `true` → MemoryBlobStore (no Telegram) |
| `[snapshot].interval_secs` | Auto snapshot period (`0` disables) |
| `[chunk].size` | Max **on-wire** chunk size (`< 20 MiB`) |
| `[chunk].codec` | `raw` \| `gzip` \| `zstd` |
| `[telegram].send_*` | Budget for sendDocument / sendMessage / pin |
| `[telegram].get_file_*` | Budget for getFile |
| `[telegram].delete_*` | Budget for deleteMessage (purge / GC) |
| `[telegram].*_concurrency` | Upload / download connection semaphores |

Legacy `[telegram].rate_per_sec` / `rate_burst` map to `send_*`.

## Tests / CI

GitHub Actions (`.github/workflows/ci.yml`) runs `cargo test` plus the four
MemoryBlobStore client suites: `compat-memory` (ceph/s3-tests known-good),
`compat-s3s-boto3`, `compat-s3s-e2e`, and `compat-rclone`.

```bash
# unit + in-process S3 API tests (MemoryBlobStore, no Telegram)
cargo test
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
| `memory = true` (in-memory BlobStore, no Telegram) | yes |
| Separate send / getFile / delete budgets + upload/download semaphores | yes |
| In-memory LRU cache for Telegram `file_path` | yes |
| Presigned URLs / ACL / bucket versioning | later |

## Notes

- Telegram `file_id` can become invalid; keep index snapshots.
- Large objects are split automatically; multipart uploads map to sequential chunks.
