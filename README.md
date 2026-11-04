# pigeonhole

S3-compatible storage, written in Rust, that keeps its bytes in Telegram and
Discord channels (or on a local disk, or in memory), and its metadata too:
a log fenced by the order of messages, snapshots, Raft between nodes.

**Try it in the browser:**
[pigeonhole-project.github.io/pigeonhole](https://pigeonhole-project.github.io/pigeonhole/).
The real code is built for wasm and runs on fake Telegram and Discord
channels, one node or a cluster of three, and you watch the messages it
posts. **Docs:**
[docs/README.md](docs/README.md). **Roadmap:** [docs/roadmap.md](docs/roadmap.md).

> **⚠️ Educational project.** A learning exercise in building a storage
> system (chunking, replication, refcounting, garbage collection, crash
> recovery, consensus) on deliberately unusual backends. It is **not** for
> real data, and **not** an invitation to use messengers as free file
> hosting: that may violate the [Telegram Terms of Service](https://telegram.org/tos),
> [API Terms](https://core.telegram.org/api/terms) and
> [Bot Developer Terms](https://telegram.org/tos/bot-developers), the
> [Discord Terms of Service](https://discord.com/terms) and
> [Developer Policy](https://discord.com/developers/docs/policies-and-agreements/developer-policy).
> Against a real bot, keep to small, short-lived test data in a private
> channel you own. For development use `kind = "memory"` stores. The
> authors take no responsibility for misuse.

## How it works

- **Gateways.** S3 over HTTP ([s3s](https://github.com/s3s-project/s3s)) and,
  optionally, a Bazel/Buck2 remote cache (REAPI v2, gRPC).
- **Chunks.** Objects are cut into chunks (`chunk.size`, ≤ 19 MiB on the
  wire by default) of independently compressed 1 MiB blocks (`zstd` by
  default), so a range read fetches only its blocks. Chunks are refcounted:
  `CopyObject` shares them.
- **Stores.** A chunk goes to a **store**: a Telegram channel (MTProto,
  logged in as a bot), a Discord channel, a local directory, or memory.
  `[[replicas]]` copy a chunk to several stores and `[[shards]]` spread
  chunks over them. Repair brings back lost copies; a sweeper deletes what
  nothing references.
- **Metadata.** A **tablet**: typed commands in a log, applied to in-memory
  state and rebuilt from the newest snapshot and the log on every boot.
  There is no local database. The log lives on any store and is fenced
  there: on disk by `flock`, in a Telegram channel by the order of message
  ids, in Discord by threads and a lease. One S3 operation is one log entry;
  conditional writes are decided in the log; small objects live inline in it.
- **Cluster.** Several nodes run the tablet as one Raft group (openraft).
  Nodes talk over gRPC, or through Telegram channels for a node that has
  nothing but a channel. The leader does all the work.

Terms: [glossary](docs/glossary.md). The tablet:
[meta-backends.md](docs/meta-backends.md). The cluster:
[cluster.md](docs/cluster.md).

## Quick start (no external services)

```bash
cat > pigeonhole.toml <<'EOF'
[[store]]
id = "mem"
kind = "memory"

[chunk]
store = "mem"
EOF
cargo run --release
```

The metadata goes to `./pigeonhole-meta` (a local disk log), the bytes to
memory. The node listens on `http://0.0.0.0:8333` (`listen_addr`); point any
S3 client at it:

```bash
export AWS_ACCESS_KEY_ID=pigeonhole AWS_SECRET_ACCESS_KEY=pigeonholesecret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./file.bin s3://demo/file.bin
aws --endpoint-url http://127.0.0.1:8333 s3 cp s3://demo/file.bin ./out.bin
```

Settings are in `pigeonhole.toml` (`PIGEONHOLE_CONFIG` overrides the path);
[pigeonhole.toml.example](pigeonhole.toml.example) has every key with
comments. Unknown keys are refused. Secrets never go in the file: it names
env vars (`.env` is read), see [.env.example](.env.example).

## Backends

Every place bytes can live is a `[[store]]`. `[chunk] store` names where
chunks go (a store, a `[[replicas]]` group or `[[shards]]`), and `[meta] log`
and `snapshot` name where the metadata goes. A channel serves one purpose.

- **Telegram** (`kind = "telegram"`). Create a bot with
  [@BotFather](https://t.me/BotFather), get an app's `api_id`/`api_hash` at
  [my.telegram.org](https://my.telegram.org), and make the bot an admin of a
  private channel (post, delete, and pin on a blob channel). pigeonhole logs
  in over MTProto as the bot: files up to the chunk size, a channel read by
  message id, about 20 posts a minute per bot and channel. Several bots per
  store add up their budgets. See
  [telegram-mtproto.md](docs/telegram-mtproto.md) and
  [shared-limiter.md](docs/shared-limiter.md).
- **Discord** (`kind = "discord"`, on by default). The bot views the
  channel, sends messages, attaches files, reads history and manages
  messages; a log or snapshot channel also needs threads. Attachments are up
  to 20 MiB. See [discord-log.md](docs/discord-log.md) and
  [discord-lease.md](docs/discord-lease.md).
- **Local disk** (`kind = "local_disk"`): append-only segment files.
- **Memory** (`kind = "memory"`): tests and throwaway runs.

A store is marked with its deployment's owner. Two configs pointed at one
channel by mistake: the second refuses to start, and neither deletes the
other's blobs ([store-owner.md](docs/store-owner.md)).

## A cluster

```toml
[cluster]
node = 1                              # or PIGEONHOLE_NODE
id = 7
key_env = "PH_CLUSTER_KEY"            # 64 hex digits

[[node]]
id = 1
grpc = "10.0.0.1:7001"
[[node]]
id = 2
grpc = "10.0.0.2:7001"
[[node]]
id = 3
outbox = -1003333333333               # no address: reached through a Telegram channel
```

The same file serves every node; a `[[node]]` may also name its own
metadata `log` and `snapshot` stores, and the blob stores are shared.

- The leader serves S3 and runs the background work. A follower answers 503.
- `GET /_pigeonhole/leader` is 200 on the leader only: point a load
  balancer's health check at it.
- Transports are chosen per pair: gRPC when both nodes have an address,
  Telegram outboxes when both have a channel.
- `make cluster-smoke` runs three processes on localhost and kills the
  leader.

Everything else: [cluster.md](docs/cluster.md).

## Running it

- **Admin API and UI** (`[admin] listen_addr`, off by default, no auth: keep
  it on loopback). It shows the dynamic config and the background tasks.
  With `[admin] ui_dir`, the same UI as the demo runs against the real node.
- **Dynamic config.** The knobs of the background loops (sweep, repair,
  cooler, snapshots, metrics, REAPI GC) change while running and are kept in
  the tablet ([dynamic-config.md](docs/dynamic-config.md)):

  ```bash
  pigeonhole config                          # every dynamic key, every task
  pigeonhole config set repair.interval off
  pigeonhole run repair                      # a pass now
  ```
- **Logs.** stderr, text or JSON, one span per S3 request
  ([observability.md](docs/observability.md)).
- **Metrics.** Prometheus `/metrics`; `make metrics-demo` brings up
  Prometheus and Grafana with a dashboard ([metrics.md](docs/metrics.md)).
- **Durability.** Ship the log and snapshots off the host (Telegram or
  Discord channels) and a clean machine boots from them. With a local log
  and snapshots elsewhere, `pigeonhole restore --lossy` starts from the
  newest snapshot ([log-retention.md](docs/log-retention.md)).
  `pigeonhole purge --yes` deletes every object and lets the sweeper delete
  their blobs.
- **REAPI remote cache.** Build with `--features bytestream`, set
  `[bytestream] enabled = true`, then
  `bazel build --remote_cache=grpc://127.0.0.1:8980 //…`. The CAS
  (ByteStream Read/Write, BatchRead/BatchUpdate, FindMissing) and the
  ActionCache (results kept in the metadata, keyed by the Action's digest,
  up to 1 MiB each) both work. A Write's bytes are hashed on their way in
  and nothing is stored unless they match their digest. Reads and repeat
  writes add no log entry: last-access times are written once per GC pass.
  Tested in process over gRPC (`gateway-bytestream/tests/reapi.rs`: writes,
  reads, ranges, wrong bytes, ActionCache round trip), not yet with a real
  Bazel build.
- **Demos.** etcd and SQLite running on a Telegram channel through S3
  conditional writes: [demos/](demos/README.md).

## S3 support

| Operation | Status |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket / HeadBucket / GetBucketLocation | yes |
| PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects | yes |
| ListObjects (v1) / ListObjectsV2 / ListObjectVersions (non-versioned) | yes |
| Multipart (Create / UploadPart / UploadPartCopy / Complete / Abort / ListParts / ListMultipartUploads) | yes |
| GetObject Range (block-granular) | yes |
| CopyObject (shares refcounted chunks) | yes |
| Conditional writes (`If-Match`, `If-None-Match`) | yes |
| Object tagging, user metadata, Content-MD5 / CRC32 checks | yes |
| Presigned URLs / ACLs / versioning | no |

## Limitations

- **A single tenant**: one access key pair, no ACLs.
- **One process per log.** A process booting on the same `[meta] log`
  claims a newer generation and fences the running one, which exits. A
  `local_disk` log fences only processes on the same host.
- **Blobs are bot-bound.** A Telegram store's fingerprint includes its
  primary bot, so changing that bot is refused at startup.
- **No TLS or auth between nodes** beyond the HMAC on Raft frames, and no
  auth on the admin API: keep both on a private network or loopback.

## Development

A Cargo workspace under `crates/`, by role:

| Crates | Role |
|---|---|
| `core/`: `types`, `codec`, `storage-api`, `limits` | shared types, block codecs, what a store implements (`BlobBackend`, `Chain`), rate limiters |
| `meta/pigeonhole-meta` | the tablet: commands, states, the log, snapshots, openraft |
| `net/`: `openraft-chat`, `openraft-grpc`, `pigeonhole-raft-telegram` | Raft transports: chat channels (standalone crate), gRPC, `Direct` in memory, per-pair `Mixed`; a Telegram channel as an outbox |
| `blob/`: `pigeonhole-blob`, `pigeonhole-chunk-store` | stores behind one face, replication; the chunk store, ingest, sweeper, repair |
| `storage/` | Telegram (MTProto client + store), Discord (HTTP + store), disk, memory |
| `gateway/` | S3 and REAPI |
| `config/pigeonhole-config` | `pigeonhole.toml` into a `Config`, the dynamic config |
| `admin/pigeonhole-admin` | the admin ops, shared by a real node and the demo |
| `node/pigeonhole-node` | the node assembled from a `Config` (tablet, chunk store, S3 handlers, admin, background loops), the same for the binary and the demo |
| `bin/pigeonhole` | the binary: the real channels, HTTP, the Raft runtime, the CLI |
| `web/pigeonhole-web` | the node for wasm on fake channels (the demo) |
| `testing/` | conformance suites, the simulation, fake channels |

Layering is enforced by `make arch`. How agents and
people work in this repository (a worktree per task, checks, merging):
[AGENTS.md](AGENTS.md).

```bash
make lint                # rustfmt --check, clippy -D warnings
make test                # cargo nextest run --workspace
make sim                 # the simulation under madsim (docs/simulation.md)
make web-smoke           # the wasm build, run under Node
make lint-js test-js     # the site's JavaScript
make cluster-smoke       # three processes as one cluster
make compat-memory       # ceph/s3-tests, curated; also compat-s3s-boto3, compat-s3s-e2e, compat-rclone
make compat-telegram     # live, against a throwaway channel (see the disclaimer)
```

CI runs the format, lint (Rust and the site's JavaScript), layering and
complexity checks, the workspace tests, the simulation (50 seeds) and the
in-memory client suites; none of it touches Telegram or Discord. The site is published to GitHub Pages from
`site/` ([site/README.md](site/README.md)).

## License

See [LICENSE](LICENSE).
