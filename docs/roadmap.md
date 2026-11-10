# Roadmap

What is built, what is left, and in which order. Design lives in the docs
the items link to; the index is [README.md](README.md). How sessions work in
this repository (worktrees, checks, merging) is in
[AGENTS.md](../AGENTS.md).

## Ground rules

- Educational project: **no backward compatibility**. Change formats,
  schemas and configs outright, with no aliases or migrations.
- One logical change per commit. Commit freely; **push only when asked**.
- `cargo test --workspace` runs `sweep_stress` once. Run it repeatedly
  (`for i in $(seq 1 10); do cargo test -q -p pigeonhole --test it sweep_stress:: || break; done`)
  at the end of a step or before a push, not after every commit.
- After S3-visible changes also run `make compat-s3s-e2e` (an STS failure is
  expected), `make compat-s3s-boto3` and `make compat-memory`.
- Docs, comments and commit messages are in English only.
- Layering is enforced (`make arch`):
  - `meta`, `types`, `codec` and `storage-api` depend on crates.io only;
  - storage crates do not see chunk-store or the gateways;
  - `openraft-chat` depends on no pigeonhole crate.

## Where we are (2026-11-02)

- **Storage.** S3 (s3s) and REAPI gateways over a chunk store. Objects are
  cut into chunks of compressed blocks, refcounted, and copied to replicas
  or spread over shards of stores: Telegram (MTProto, bots), Discord, local
  disk, memory. Repair, a sweeper with upload and read pins, and a store
  owner marked on every channel.
- **Metadata.** A tablet: typed commands in a log, applied to in-memory
  states, snapshots, retention of the newest bases, fallback boot, lossy
  restore. The log is a `Chain` over any store:
  - the local disk, fenced by `flock`;
  - a Telegram channel, fenced by message order;
  - a Discord channel, fenced by threads, with a lease.

  One S3 operation is one log entry; conditional writes are decided in the
  log; small objects live inline in it.
- **Cluster.** The tablet over openraft on several nodes. Transports are
  chosen per pair: gRPC, or Telegram outboxes for a node with only a
  channel. The leader does all the work; a follower answers 503. A lagging
  follower catches up from a snapshot.
- **Operations.**
  - the dynamic config: the background loops' knobs, changed while running
    and kept in the tablet;
  - an admin API and UI, with a CLI (`pigeonhole config`, `pigeonhole run`);
  - metrics with a Grafana dashboard, logs and request spans.
- **Site.** The home page, and pigeonhole itself running in the browser
  (wasm) on fake Telegram and Discord channels, one node or a cluster of
  three, published on GitHub Pages. A setup wizard; the admin UI, served by
  a real node too.
- **Testing.** madsim simulation, fault injection, client suites
  (ceph/s3-tests, s3s, rclone), live tests against real bots, CI gates
  (rustfmt, clippy `-D warnings`, a complexity cap, layering), demos (etcd
  and SQLite on a Telegram channel).

## Done

| What | Docs |
|---|---|
| The tablet: commands, in-memory states, a fencing log, boot = claim → replay → zero entry | [meta-backends.md](meta-backends.md) |
| Snapshots in a store of their own; lossy restore; the old checkpoint durability removed | [meta-backends.md](meta-backends.md) |
| The sweeper's contract on the log; upload and read pins, our own blob ids | [sweep-invariants.md](sweep-invariants.md) |
| Telegram over MTProto (the Bot API backend removed) | [telegram-mtproto.md](telegram-mtproto.md) |
| `[[store]]`, `[[replicas]]`, `[[shards]]`; every kind for every purpose | [README](../README.md) |
| One log entry per S3 operation | [one-entry-per-operation.md](one-entry-per-operation.md) |
| The log on any `Chain` store; the disk store on segments | [chain-log.md](chain-log.md) |
| Discord: blobs, the log and snapshots; a lease for the log | [discord-log.md](discord-log.md), [discord-lease.md](discord-lease.md) |
| Metrics, a Grafana dashboard; logs and request spans | [metrics.md](metrics.md), [observability.md](observability.md) |
| Fault injection and madsim simulation | [fault-injection.md](fault-injection.md), [simulation.md](simulation.md) |
| Log retention, fallback boot; snapshots in parts over a store's blob size | [log-retention.md](log-retention.md) |
| S3 conditional writes in the log; the etcd, SQLite and SlateDB demos | [conditional-writes.md](conditional-writes.md), [demos](../demos/README.md) |
| Small objects inline in the metadata, a cooler | [inline-objects.md](inline-objects.md) |
| A shared limiter, several bots per store, ingest pipelined with uploads | [shared-limiter.md](shared-limiter.md) |
| A store's owner: owner bits in blob ids, a marker on the channel | [store-owner.md](store-owner.md) |
| The dynamic config, the admin API and CLI | [dynamic-config.md](dynamic-config.md) |
| The site, the wasm demo on fake channels, the wizard, the admin UI | [site/README.md](../site/README.md), [crates/web](../crates/web/README.md) |
| The cluster: openraft over chat and gRPC, per-pair transports, the leader does the work, snapshot catch-up, the Telegram outbox, one config for every node, three nodes in the demo | [cluster.md](cluster.md) |

## Left

### Cluster ([cluster.md](cluster.md) § Status)

1. A live run with a node on real Telegram channels; election time, commit
   latency and failover measured.
2. A Discord outbox (the Message Content intent first).
3. Outbox housekeeping; `seq` restored from the outbox; the poster's bot
   checked against the node.
4. Membership changes and learners through the admin API.
5. A snapshot sent as a reference to the leader's snapshot store; a margin
   of log kept behind a snapshot.

### Single node

- Export of request spans as distributed traces (OpenTelemetry).
- A Discord lease log after a crash: `flock` on the local side.
- Simulation: tablet, snapshot and repair scenarios.
- Point-in-time restore, with reclaim gated on its watermark
  ([log-retention.md](log-retention.md)); a sweep of orphan snapshots.
- The grammers-mtsender patch offered upstream.

### Next

- **OpenDAL.**
  - An OpenDAL blob store (`kind = "opendal"`): keys that grow for the
    sweeper's horizon, listing with `start_after`, batch deletes.
  - `impl Chain` with `if_not_exists` writes (s3, gcs, fs): an S3 log.
  - An OpenDAL `Access` over the S3 index as a gateway: WebDAV,
    `object_store`, FUSE last.
- **Turso replica**: a Turso database whose WAL frames go into a chain log
  ([proposals/turso-replica.md](proposals/turso-replica.md)).
- **Later.**
  - Several tablets when metadata outgrows memory (never a database
    inside the tablet).
  - Encryption per block in the codec.
  - Presigned URLs, ACLs, versioning.

## Order

1. The cluster's live run (1).
2. The Discord outbox (2).
3. The cluster's other items.
4. OpenDAL.
5. Turso.

The single-node items when they block something or come cheap.

## Open questions

- The default batch window per store kind (`local_disk`: no extra wait).
- A converter for old logs and snapshots when a `VERSION` changes, if ever
  needed.
