# s3gram

S3-compatible HTTP gateway in Rust, backed by the Telegram Bot API.

The S3 protocol surface is implemented with
[s3s](https://github.com/s3s-project/s3s). Objects are split into ≤19 MiB chunks
and stored as documents in **one** private Telegram chat/channel (`CHAT_ID`).
Object metadata lives in a local SQLite index.

Telegram I/O goes through a `BlobStore` trait (`TelegramBlobStore` in production,
`MemoryBlobStore` in unit tests) so tests never hit the real Bot API.

## Bot API limits

- Upload (`sendDocument`): 50 MiB
- Download (`getFile`): 20 MiB → s3gram uses 19 MiB chunks

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and get a `BOT_TOKEN`.
2. Create a private channel or group, add the bot as **administrator**.
3. Set `CHAT_ID` (channels are usually `-100...`).
4. Copy env and fill in secrets:

```bash
cp .env.example .env
# edit BOT_TOKEN and CHAT_ID
```

On startup s3gram calls `getMe` + `getChatMember` and exits if the bot is not
an admin in `CHAT_ID`.

## Run

```bash
cargo run --release
# or: make run
```

Listens on `http://0.0.0.0:8333` by default.

## Tests

```bash
# unit + in-process S3 API tests (MemoryBlobStore, no Telegram)
cargo test
# or: make test

# smoke against a running server
make smoke

# ceph/s3-tests against a temporary MemoryBlobStore server (no Telegram)
make compat-memory           # curated known-good
./scripts/compat-memory.sh --all

# s3s upstream tests/boto3 (presigned POST, Content-Length edge cases)
make compat-s3s-boto3

# s3s-e2e suite (Basic + Advanced) against MemoryBlobStore
make compat-s3s-e2e
make compat-s3s-e2e ARGS='--filter ^Basic'

# s3s upstream rclone S3 e2e against MemoryBlobStore (downloads pinned rclone)
make compat-rclone
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

s3gram periodically exports the SQLite index as a gzip Telegram document in
`CHAT_ID`. Oversized snapshots are split into parts plus a small **manifest**;
the restore handle is the single gzip `file_id` or the manifest `file_id`.
Uploads happen only when the index hash changes; previous snapshot messages are
deleted.

Interval (default 300s; `0` disables):

```bash
SNAPSHOT_INTERVAL_SECS=300
```

Restore on a stopped server / clean machine:

```bash
# stop s3gram first
cargo run --release -- restore <file_id>
```

You can also copy the local `s3gram.db` file.

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
| `S3GRAM_MEMORY=1` (in-memory BlobStore, no Telegram) | yes |
| Presigned URLs / ACL / bucket versioning | later |

## License

MIT
