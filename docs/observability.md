# Logs and traces

What pigeonhole prints, how to read one request, and how to tune it. Metrics
are in [metrics.md](metrics.md).

Built on [`tracing`](https://docs.rs/tracing): every log line is an event
with structured fields, inside the spans it happened in. Output goes to
stderr (`tracing-subscriber`: text or JSON).

## Configuration

```toml
[log]
level = "info,openraft=warn,s3s=off"   # tracing directives (default shown)
format = "text"                         # or "json"
slow_request = "5s"                     # slower requests are logged at info
```

- `level` takes [`EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
  directives: a default level and per-crate ones, e.g.
  `info,pigeonhole_meta=debug,pigeonhole_storage_telegram=debug`.
- `RUST_LOG`, when set, replaces `level`. Unless it names them, openraft
  stays at `warn` and s3s at `off` (their `info` / `error` lines are noise
  for us: openraft logs every Raft step of a cluster of one, s3s logs a
  refused signature as ERROR). `RUST_LOG=debug` is a fine first try.
- `text` uses colours only when stderr is a terminal; `json` writes one
  object per line with the event's fields and its spans (`span`, `spans`),
  for Loki, Elasticsearch and the like.

## Levels

| Level | What | Examples |
|---|---|---|
| `error` | The process cannot go on as it was | the tablet stopped (failed log write, failed commit) |
| `warn` | Something failed and was handled, or needs a look | a 5xx answer; a store call failed (a replica inside a write that met its quorum shows only here); a sweep or repair pass failed; removing an old snapshot failed; log collection failed; the tablet was fenced by a newer generation |
| `info` | What a person running it wants to see | config loaded, the log claimed (generation, log id, head, base), the tablet ready (position, entries replayed, time), a snapshot stored (position, bytes, time), the log rebased; a slow request; a rate-limit wait of 5 s or more; each S3 write that changed something (`PutObject ok`, `CreateBucket ok`) |
| `debug` | Every step, to follow one request or one store | each request (status, time), each store call, what a store posted / read / deleted (name, bytes, message id or disk key, text or attachment), every chain step (linked, lost the link and to whom, found the tail, pruned), each commit (entries, proposals, position, time) and which position a proposal landed at |

## Spans

| Span | Fields | Where |
|---|---|---|
| `request` | `gateway`, `id`, `method`, `path`, `op` (the S3 operation, once routed) | every S3 request; the id is also sent back as the `x-pigeonhole-request-id` header. The query string is never logged (a presigned URL carries a signature in it). |
| `store` | `instance`, `op`, `bytes` / `keys` | each blob store call through the placement group (below `request` when a request made it) |
| `commit` | `entries`, `proposals`, `position` | the tablet's batcher writing one Raft entry |

A task spawned while serving a request does not inherit its span by itself;
where it matters (releasing an overwritten object's chunks after the answer)
it is passed on explicitly.

## Reading one request

`RUST_LOG=info,pigeonhole_meta=debug,pigeonhole_storage_memory=debug`, one
`PUT /bucket1/k`:

```
DEBUG request{gateway="s3" id=6c19… method=PUT path=/bucket1/k}: pigeonhole_storage_memory: stored store=mem message_id=1 name="ph-916e….bin" bytes=6
DEBUG commit{entries=2 proposals=1 position=5}: pigeonhole_meta: commit ok=true ms=4
DEBUG request{gateway="s3" id=6c19… method=PUT path=/bucket1/k}: pigeonhole_meta: committed position=5 waited_ms=4
 INFO request{gateway="s3" id=6c19… method=PUT path=/bucket1/k}: pigeonhole_gateway_s3::service: PutObject ok bucket=bucket1 key=k size=6 extents=1
```

The blob went to the store first, then one commit took the chunk and the
object together (one entry, step 7b), at position 5. To follow it into the
metadata log, look for the batch holding position 5:

```
DEBUG pigeonhole_meta::log::chain: log batch linked generation=1 first=5 last=5 blob=DiskId { key: … }
DEBUG pigeonhole_storage_disk: linked store=local_disk label="log-batch-1" prev=Some(…) key=… bytes=274
```

The log's writes run in Raft's own task, not in the request's span: the
position is what ties them together.

## Not yet

- Export of the spans as distributed traces (OpenTelemetry / OTLP to
  Jaeger, Tempo): a feature behind a flag, the spans above as they are.
- Log files with rotation (`tracing-appender`); today stderr only, for the
  service manager or a pipe to collect.
