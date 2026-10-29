# Conditional writes in the S3 gateway

Status: implemented, checked live with T4 (2026-10-29).

## Why

S3 conditional writes (`If-None-Match: *`, `If-Match: <etag>`) are the one
primitive that lets other systems build on object storage without a
coordinator of their own: a leader lock, a manifest that never moves
backwards, a WAL segment that is never overwritten. With them in our gateway,
systems that already run on S3 run on Telegram, Discord or a disk unchanged:

- **T4** (etcd v3 API over S3; kine speaks to it, so k3s / any
  kube-apiserver can keep its state in it). Its leader lock reads the lock
  object and writes it back with `If-None-Match: *` or `If-Match: <etag>`;
  its manifest moves by `If-Match`. At startup it *probes* the store and
  refuses a multi-node cluster whose store takes the headers and ignores
  them, so "accepted and ignored" is worse than "not offered".
- SlateDB, Delta Lake / Iceberg commits, and anything else that commits by a
  conditional put.

We are unusually well placed for this: every S3 write is already one entry
in a linearizable log, so the condition is checked where the write is
decided, not before it.

## Semantics (as AWS)

| Request | Object absent | Object present, ETag equal | ETag different |
|---|---|---|---|
| `PutObject` / `CompleteMultipartUpload` / `CopyObject` + `If-None-Match: *` | write | 412 `PreconditionFailed` | 412 |
| same + `If-Match: <etag>` | 404 `NoSuchKey` | write | 412 |
| `DeleteObject` + `If-Match: <etag>` | 404 `NoSuchKey` | delete | 412 |

- `If-None-Match` takes only `*`; anything else is 400 `InvalidArgument`
  (AWS: `NotImplemented`).
- ETags compare by value (quotes and `W/` ignored; ours are strong).
- Of two conditional writes racing for one key, the first to commit wins
  and the other gets 412. AWS may also answer 409
  `ConditionalRequestConflict`; we never need to: the log orders them.
- `CopyObject`'s `If-Match` / `If-None-Match` are about the *destination*
  (`x-amz-copy-source-if-*` are about the source and stay unsupported).
- Multipart: the condition is checked at `CompleteMultipartUpload`, against
  the object as it is then; an upload in progress is not an object.

## Where the check lives

In the command, not in the handler. `S3Command::PutObject`,
`CompleteUpload` and `DeleteObject` carry `expect: Option<Expect>`
(`Absent` | `ETag(String)`); `S3State::apply` checks it against the object
as of that log position and rejects with `S3Reject::PreconditionFailed` or
`NoSuchKey`, leaving the state untouched (as every rejection does). The
check is therefore deterministic, replays identically, and is linearizable
with every other write.

The handler checks the condition once before reading the body as well, so a
write that is bound to fail does not upload its chunks first. That early
check is only an optimization; the one in `apply` decides.

A rejected `PutObject` is committed in the same entry as its chunks
(one entry per operation), so the chunks exist with refs = 1 and nothing
points at them: the handler releases them, as it already does when the bucket
disappears under a put. `S3State::VERSION` 3.

## Not in scope

- `If-Match` on `DeleteObjects` (per-object ETags in the batch).
- Versioned reads (`GetObject` with `versionId`): T4 uses them only for a
  point-in-time restore.
- Conditional reads (`If-Match` on `GetObject`/`HeadObject`) are a different
  feature and s3s already routes them; untouched here.

## Checks

- Unit: `apply` truth table above; replay gives the same verdicts.
- Gateway (`tests/s3_api.rs`): each row of the table through s3s; a rejected
  put leaves no chunk referenced.
- Race: N concurrent `If-None-Match: *` puts of one key, exactly one wins.
- T4 against a local pigeonhole: its conditional-write probe passes, and
  `etcdctl put/get/txn` round-trips (live, not in CI).

## Live: T4 on pigeonhole (2026-10-29)

T4 v1.1.16 (`t4 run --s3-endpoint http://127.0.0.1:<port> --peer-listen …`,
multi-node mode) against a local pigeonhole, etcdctl v3.7.3 as the client.

- **Local disk.** The node elects itself through `leader-lock`
  (`If-None-Match` / `If-Match`); put, get, txn and leases work. A second
  node follows; `kill -9` of the leader: the follower catches up from the
  bucket and takes over (term 3) in a few seconds, and writes go on.
- **Chats.** The same with pigeonhole's metadata log on Discord (lease), its
  snapshots in a second Discord channel, and the objects in a Telegram
  channel. Election in ~1 s; an etcd put is 0.4–0.6 s (each one is a
  conditional PUT: a lone leader flushes every batch to the bucket);
  takeover after `kill -9` in ~10 s.
- **Limit found.** T4's lock is valid for 6 s and renewed by `If-Match`. Our
  write latency has tails of several seconds (a Discord lease renewal waits
  for the thread pacer), so one renewal timed out right after the takeover
  and puts then took 0.4–5 s, once 11 s. It held, but T4's lock timings are
  tighter than a chat-backed store's tail; either T4 gets longer lock
  timings or our lease renews earlier.
- **Control.** The same run against main before this change: the gateway
  answered 200 to every conditional put (`If-None-Match: *` twice, a stale
  `If-Match`), so two leaders could both "win". T4 v1.1.16 started anyway:
  its probe of whether a store enforces the headers (t4db/t4#377) is not in
  a release yet.
