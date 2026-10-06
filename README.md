# s3gram

S3-compatible HTTP gateway in Rust, backed by the Telegram Bot API.

Objects are split into ≤19 MiB chunks and stored as documents in a private Telegram chat/channel. Object metadata lives in a local SQLite index.

## Bot API limits

- Upload ≤ 50 MiB per file
- Download via `getFile` ≤ 20 MiB → chunk size is **19 MiB**

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and get a `BOT_TOKEN`.
2. Create a private channel (or group) and add the bot as an admin with permission to post messages.
3. Find the `CHAT_ID` (channels are usually `-100...`).
4. Copy env and fill in secrets:

```bash
cp .env.example .env
# edit BOT_TOKEN and CHAT_ID
```

## Run

```bash
cargo run --release
```

Listens on `http://0.0.0.0:8333` by default.

## Smoke test (AWS CLI)

```bash
export AWS_ACCESS_KEY_ID=s3gram
export AWS_SECRET_ACCESS_KEY=s3gramsecret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./README.md s3://demo/readme.md
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/
aws --endpoint-url http://127.0.0.1:8333 s3 cp s3://demo/readme.md ./out.md
aws --endpoint-url http://127.0.0.1:8333 s3 rm s3://demo/readme.md
```

Large files use multipart upload automatically (supported).

For unsigned local debugging: `S3GRAM_INSECURE=1`.

## Index snapshot

Export metadata to Telegram (JSON document in the same chat):

```bash
# with S3GRAM_INSECURE=1, or a valid SigV4 signature
curl -X POST 'http://127.0.0.1:8333/?s3gram-snapshot=export'
```

Restore from a `file_id` or raw JSON body:

```bash
curl -X POST 'http://127.0.0.1:8333/?s3gram-snapshot=import' \
  -H 'Content-Type: application/json' \
  -d '{"file_id":"BQACAg..."}'
```

You can also copy the local `s3gram.db` file.

## Supported API

| Operation | Status |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket | yes |
| PutObject / GetObject / HeadObject / DeleteObject | yes |
| ListObjectsV2 | yes |
| Multipart Upload (Create / UploadPart / Complete / Abort) | yes |
| GetObject Range | yes |
| CopyObject / Presigned URLs | later |

## Notes

- Experimental — not production object storage.
- Telegram `file_id` values can become invalid; keep index snapshots.
- Respect Telegram ToS; do not run a public SaaS on top of this.
