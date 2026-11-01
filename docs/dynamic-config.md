# Dynamic config

The knobs of the background loops change while the process runs. Each loop
can be turned off, given a new period, or asked for a pass now. Everything
else in `pigeonhole.toml` is static: a restart reads it.

## What is dynamic

| key | what |
|---|---|
| `sweep.interval`, `sweep.batch_size` | the sweeper (orphans and zero-ref chunks) |
| `repair.interval`, `repair.batch_size`, `repair.concurrency`, `repair.max_cost_wait`, `repair.scrub` | repair / backfill |
| `inline.cool_every`, `inline.cool_after`, `inline.min_inline`, `inline.cool_batch`, `inline.budget`, `inline.low_watermark` | the cooler of inline objects |
| `meta.snapshot_every` | the periodic snapshot (by time; `snapshot_every_entries` is static) |
| `metrics.every` | how often the tablet's gauges are set |
| `bytestream.gc_every`, `bytestream.gc_ttl`, `bytestream.upload_gc_every` | the REAPI CAS GC and the drop of stale uploads |

A period of `"off"` parks its loop. An asked-for pass still runs. Zero is
refused, as in the file. `inline.max_object_size` is static: the S3 gateway
reads it when it starts.

## Where overrides live

The file gives the base. Overrides live in the tablet, in a namespace of their
own (`config`), one log entry per change. This means:

- they outlive a restart (state comes only from the log and the snapshot);
- every node that replays the log sees the same ones;
- the log inspector shows them like any other command.

The effective knobs are always computed again: the file, the overrides on
top, then the same parsing and checks as for the file (`deny_unknown_fields`,
units, `"off"`, floors such as "at least a second"). They are never edited in
place. So:

- dropping an override brings back the file's value;
- an override the file would refuse is refused;
- a refused change changes nothing.

An override in the tablet that the file no longer allows is left out at boot.
That happens after the binary changes a key's shape, for example. A warning
is logged, `GET /admin/config` shows the override as `refused`, and the
file's value holds. Setting the key again clears it.

## The loops

Every loop waits on a `Schedule` (`pigeonhole_types::schedule`). Its
`Control` holds the knobs. A change takes effect at once:

- a shorter period that is already over starts a pass;
- `None` parks the loop until it is turned back on;
- `run_now` starts one pass even while the loop is off.

The period counts from the end of the previous pass. The snapshot ticker's
control lives in the `MetaStore` (`snapshot_schedule()`). The bytestream
server takes its GC schedules in `Gc`.

## The admin API

It is off by default. Turn it on with `[admin] listen_addr = "127.0.0.1:9100"`.
It has no authentication, so keep it on loopback; any other address logs a
warning.

- `GET /admin/config`: every dynamic key (its value, the file's, the
  override, why one is refused) and every task (its period, when it passes
  next).
- `PUT /admin/config/{key}`: override the key. The body is the value: `5s`,
  `off`, `16`, `true`, or quoted TOML. Answers 204, or 400 with the reason.
- `DELETE /admin/config/{key}`: back to the file's value. Answers 204, or 404
  if the key has no override.
- `POST /admin/run/{task}`: a pass now. Tasks are `sweep`, `repair`, `cool`,
  `snapshot`, `metrics`, `cas_gc`, `upload_gc`. Answers 202.
- `POST /admin/ops`: the site's UI's ops (crates/admin, `pigeonhole-admin`):
  a JSON line (`{"op": "list", "bucket": "photos"}`), then an upload's bytes;
  the answer is `{"reply": …}`. The demo's node runs the same ops in the
  browser, so the UI is one: objects, the log decoded, the background tasks
  with their queues, the dynamic config, verify and repair.
- `/`: that UI, when `[admin] ui_dir` names the built site
  (`python3 site/build.py` → `target/site`). Open
  `http://<admin address>/demo.html`: the page finds `/admin/config` beside
  it and talks to this node instead of starting the demo. It is served from
  the admin address itself, so no other site may call the API from a
  browser (there are no CORS headers).

The CLI talks to it. It reads only `[admin]` of the config, so it needs none
of the stores' secrets:

```
pigeonhole config                          # keys and tasks
pigeonhole config set repair.interval off
pigeonhole config unset repair.interval
pigeonhole run repair
```

The CLI cannot write the overrides into the tablet itself: the running
process holds the log, and fencing would stop either of them.

The site's demo has the same calls as ops (`dynamic`, `dynamic_set`,
`dynamic_unset`, `run_task`).
