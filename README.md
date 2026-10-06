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
# unit tests (in-memory BlobStore, no Telegram)
cargo test

# smoke against a running server
make smoke
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
restore with `{"file_id": "<manifest_or_single>"}`. Uploads happen only when the
index hash changes; previous snapshot messages are deleted.

Interval (default 300s; `0` disables):

```bash
SNAPSHOT_INTERVAL_SECS=300
```

Periodic export runs in the background; you can also copy the local `s3gram.db`
file. (Manual HTTP export/import endpoints are not wired yet after the s3s
migration.)

## Supported S3 ops (subset)

| Operation | Status |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket | yes |
| PutObject / GetObject / HeadObject / DeleteObject | yes |
| ListObjectsV2 | yes |
| Multipart Upload (Create / UploadPart / Complete / Abort) | yes |
| GetObject Range | yes |
| CopyObject (shallow — reuses Telegram file_ids) | yes |
| User metadata (`x-amz-meta-*`) | yes |
| Blob refcount in SQLite | yes |
| Presigned URLs | later |

## License

MIT
