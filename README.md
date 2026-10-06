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
# or: make run
```

Listens on `http://0.0.0.0:8333` by default.

## Tests

With s3gram already running (`make run` in another terminal):

```bash
make smoke        # AWS CLI: put/get, multipart, copy, metadata
make compat       # curated ceph/s3-tests (compat/known-good.txt)
make compat-all   # full s3-tests functional suite (many failures expected)
```

`make compat` clones [ceph/s3-tests](https://github.com/ceph/s3-tests) into `.cache/s3-tests` and runs them via `tox`. Needs `python3`, `tox`, `git`.

```bash
./scripts/compat.sh -- s3tests/functional/test_s3.py -k multipart
```

Credentials come from `.env` (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).

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
| CopyObject (shallow — reuses Telegram file_ids) | yes |
| User metadata (`x-amz-meta-*`) | yes |
| Blob refcount in SQLite | yes |
| Presigned URLs | later |

## Notes

- Experimental — not production object storage.
- Telegram `file_id` values can become invalid; keep index snapshots.
- Respect Telegram ToS; do not run a public SaaS on top of this.
