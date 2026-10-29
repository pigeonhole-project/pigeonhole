# Demos

pigeonhole stores blobs in chat services and keeps its own metadata there
too: a log fenced by the order of messages, snapshots, leases timed by
Discord's clock. On top of it sits an S3 gateway with conditional writes,
decided in that log. That is enough for software that coordinates through
S3 to run on a Telegram channel without knowing it.

Each demo is a folder with a `run.sh` and a README. They build pigeonhole,
start it on a free port, fetch the third-party binaries they need (pinned
versions, checksums verified, into `.cache/demos/bin`), run, and stop
everything on exit.

| Demo | What you see |
|---|---|
| [conditional-writes](conditional-writes/) | `If-None-Match` / `If-Match` by curl; ten writers race for one key, one wins |
| [etcd-on-telegram](etcd-on-telegram/) | an etcd cluster (T4) whose state lives in pigeonhole: election, txn, leases, `kill -9` of the leader and a takeover |
| [sqlite-on-telegram](sqlite-on-telegram/) | a SQLite database streamed into pigeonhole by Litestream, deleted, restored, and restored as of a past moment |

## Where the bytes go

```
./demos/<demo>/run.sh                       # local disk, no secrets
DEMO_BACKEND=chats ./demos/<demo>/run.sh    # Discord + Telegram
```

With `chats`, pigeonhole's metadata log and snapshots go to the Discord test
channels (`DISCORD_LOG_CHANNEL_ID`, `DISCORD_SNAP_CHANNEL_ID`, a lease
instead of a thread per write) and the objects to the Telegram one
(`TG_BLOBS_CHAT_ID`, over MTProto). Secrets and ids come from the repo `.env`
(see `.env.example`). The Telegram channels are marked before a run and only
the messages between the marks are deleted after it; the Discord test
channels are emptied whole, so they must be dedicated to tests.

## Needs

`cargo`, `curl` (with `--aws-sigv4`, 7.75+), `python3`, `unzip`. macOS
(arm64, x86_64) or Linux (x86_64, arm64).
