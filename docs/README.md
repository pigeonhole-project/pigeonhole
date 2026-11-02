# Documentation

Start with the [README](../README.md), then the [glossary](glossary.md).
What is built and what is next: the [roadmap](roadmap.md).

## How it works

- [glossary.md](glossary.md): instance, blob, block, chunk, replica, extent,
  tablet, log, snapshot, owner, leader, outbox.
- [meta-backends.md](meta-backends.md): the metadata tablet. Commands,
  in-memory states, the fencing log and snapshot contracts, boot, the store
  kinds a log runs on.
- [chain-log.md](chain-log.md): one log for every store. The `Chain`
  capability ("first successor wins"), `ChainLogStore`, `BlobSnapshotStore`,
  the disk store on segments.
- [one-entry-per-operation.md](one-entry-per-operation.md): an S3 operation
  is one log entry.
- [sweep-invariants.md](sweep-invariants.md): what the sweeper may delete,
  and when (pins, the horizon, reclaim).
- [store-owner.md](store-owner.md): owner bits in blob ids, the marker on a
  channel; two deployments on one store by mistake.
- [log-retention.md](log-retention.md): how many snapshots and how much log
  are kept, collection on spare budget, a fallback boot.
- [conditional-writes.md](conditional-writes.md): `If-Match` and
  `If-None-Match`, decided in the log; etcd (T4) on pigeonhole.
- [inline-objects.md](inline-objects.md): small objects inside the metadata,
  and the cooler that moves them out.

## Backends

- [telegram-mtproto.md](telegram-mtproto.md): the Telegram store over
  MTProto with a bot token, the spike, the order fence.
- [discord-log.md](discord-log.md): the Discord store; the log fenced by
  threads.
- [discord-lease.md](discord-lease.md): a lease for the Discord log, so
  batches need no thread each.
- [shared-limiter.md](shared-limiter.md): rate limits per bot and per chat,
  pools of bots, flood waits.

## Cluster

- [cluster.md](cluster.md): the tablet on several nodes. The leader does
  the work, transports per pair (gRPC, Telegram outboxes, `Direct`), the
  configuration, clients, status.

## Running it

- [dynamic-config.md](dynamic-config.md): the background loops' knobs while
  running; the admin API and CLI.
- [observability.md](observability.md): logs, levels, request spans.
- [metrics.md](metrics.md): every metric (generated), the Grafana demo.
- [../demos/README.md](../demos/README.md): etcd and SQLite on a Telegram
  channel.
- [../site/README.md](../site/README.md): the site and the in-browser demo;
  [../crates/web/README.md](../crates/web/README.md): pigeonhole built for
  wasm.

## Testing

- [simulation.md](simulation.md): deterministic simulation under madsim.
- [fault-injection.md](fault-injection.md): `Faulty<B>`, the fault model,
  the scenarios.

## Records and proposals

- [bench/discord-vs-telegram.md](bench/discord-vs-telegram.md): the
  2026-10-29 bench, with later updates; raw numbers in [bench/](bench/).
- [proposals/turso-replica.md](proposals/turso-replica.md): a Turso database
  replicated into pigeonhole (not built).
- [proposals/albums.md](proposals/albums.md): several files in one send —
  Discord messages of up to 10 attachments, Telegram albums; live spikes
  (not built).
