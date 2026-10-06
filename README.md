# s3gram

S3-compatible HTTP gateway in Rust, backed by the Telegram Bot API.

Each **data bucket** maps to its **own Telegram chat** (object blobs live there).

A reserved **service bucket** (`SERVICE_BUCKET`, default `s3gram`) uses `SERVICE_CHAT_ID` and stores **only**:
- bucket registry (`buckets/{name}.json`)
- index snapshots

Object data is never written to the service chat.

## Bot API limits

- Upload ≤ 50 MiB per file
- Download via `getFile` ≤ 20 MiB → chunk size is **19 MiB**

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather) and get a `BOT_TOKEN`.
2. Create a **service** private channel/group (registry + snapshots), set `SERVICE_CHAT_ID`.
3. Set `ADMIN_CHAT_ID` (chat where you run `/bucket` — often your private chat with the bot).
4. For each data bucket: create a **separate** chat, add the bot, bind with `/bucket` in the admin chat.
5. Copy env and fill in secrets:

```bash
cp .env.example .env
# edit BOT_TOKEN, SERVICE_CHAT_ID, ADMIN_CHAT_ID
```

## Run

```bash
cargo run --release
# or: make run
```

Listens on `http://0.0.0.0:8333` by default. On startup the service bucket is ensured.

## Register a bucket (admin chat)

1. Create a Telegram chat/channel and **add the bot**.
2. In the **admin chat** (`ADMIN_CHAT_ID`) s3gram notifies you.
3. Name the bucket:

```text
/bucket photos
```

or explicitly:

```text
/bucket photos -100XXXXXXXXXX
```

Other commands: `/buckets`, `/unbind <name>`, `/help`.

This writes `s3://s3gram/buckets/photos.json` in the service bucket and stores `chat_id` in SQLite.
Object blobs for that bucket go to the bound Telegram chat.

S3 `CreateBucket` / `aws s3 mb` for a new name requires a prior `/bucket` bind, or
`x-s3gram-chat-id` / `DEFAULT_DATA_CHAT_ID` (tests). Binding to `SERVICE_CHAT_ID` is rejected.

Same-chat CopyObject stays shallow (refcount); cross-chat copy re-uploads into the destination chat.

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

s3gram periodically exports the SQLite index as a Telegram document (`s3gram-index.json`)
in the **service** chat. Uploads happen only when the index hash changes; the previous snapshot
message is deleted.

Interval (default 300s; `0` disables):

```bash
SNAPSHOT_INTERVAL_SECS=300
```

Manual export (same replace-if-changed logic):

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
| CreateBucket / ListBuckets / DeleteBucket | yes (per-chat via `x-s3gram-chat-id`) |
| PutObject / GetObject / HeadObject / DeleteObject | yes |
| ListObjectsV2 | yes |
| Multipart Upload (Create / UploadPart / Complete / Abort) | yes |
| GetObject Range | yes |
| CopyObject (shallow same-chat; deep cross-chat) | yes |
| User metadata (`x-amz-meta-*`) | yes |
| Blob refcount in SQLite | yes |
| Presigned URLs | later |

## Notes

- Experimental — not production object storage.
- Telegram `file_id` values can become invalid; keep index snapshots.
- Respect Telegram ToS; do not run a public SaaS on top of this.
