# One log entry per S3 operation

Status: **implemented** (steps 1–3; see Implementation notes). Read
[meta-backends.md](meta-backends.md) and
[sweep-invariants.md](sweep-invariants.md) first.

## Why

Every metadata command is its own `MetaStore::propose`, and each layer awaits
its commit before the next step. One S3 PUT is therefore several log entries
in a row:

```
PutObject
 ├─ store.ingest(...)        chunk: CommitChunk per chunk   entries 1..n, each awaited
 ├─ index.put_object(...)    s3:    PutObject               entry n+1 (reply: the old object's chunks)
 ├─ index.put_object_tags()  s3:    PutObjectTags           entry n+2 (if tagged)
 └─ release_ids(old)         chunk: Release                 entry n+3 (if it overwrote an object)
```

On a local disk that is a few fsyncs (~5 ms each on macOS). On a Telegram log
it is the main cost: an entry is a message (plus a pin today), a channel takes
about 20 messages a minute, and the live run (2026-10-28) measured 8–23 s per
small PUT. Concurrent requests already share entries (`MetaStore` batches
whatever is queued), but one request's steps cannot, because each waits for
the previous one.

Goal: **one entry before the client gets its answer**, for every S3 write
whose steps do not depend on each other's outcome.

## What would be a leak

The tempting shortcut is one fat command, e.g. a `PutObject` whose `apply`
commits the chunk and decrements the old object's chunks. That makes one
namespace's state machine change another's. Three things must stay true:

1. The S3 gateway never sees chunk-store commands (`ChunkCommand`).
2. The tablet (`pigeonhole-meta`) knows nothing about layers.
3. A command changes only its own namespace's state.

## Design

### Prepared commands

A layer builds its commands as now, but can hand one back instead of
proposing it:

```rust
/// A command for `S`, built by the layer that owns `S` and opaque to anyone
/// else; proposing it yields `S`'s typed outcome.
pub struct Prepared<S: StateMachine> { command: Command, _s: PhantomData<S> }

impl MetaStore {
    /// Commit several prepared commands as one Raft entry, in order; each is
    /// applied (or rejected) on its own. All or none are durable.
    pub async fn propose_all<T: PreparedTuple>(&self, cmds: T) -> Result<T::Outcomes>;
}
```

- `Prepared<S>` is built only through `S`'s layer (`BlobDb::prepare_commit`,
  `Index::prepare_put`, …); the gateway holds and orders values it cannot
  look into. Rule 1 holds.
- `propose_all` is the existing batch path made explicit: the batcher already
  puts commands of different namespaces into one entry. Rule 2 holds.
- Each command is applied by its own namespace, independently; a rejection
  of one does not undo another. Rule 3 holds.
- Outcomes come back typed (`(Outcome<ChunkState>, Outcome<S3State>)` for a
  pair, `Vec<Outcome<S>>` for a homogeneous list).

The limitation: **a command cannot depend on the outcome of an earlier one in
the same entry.** Operations where it would have to are split (see `copy`).

### The batch stays one entry

`propose_all`'s commands go into one Raft entry together (and with other
queued proposals): never split across entries, so they are durable together.
`MAX_BATCH` counts commands of other proposals only; a prepared batch is
taken whole. An entry larger than a store can take in one write (a Telegram
document over the Bot API: 20 MiB; in practice a few MiB of metadata at most)
is refused with an error rather than split.

### Releases after the answer

Releasing an overwritten object's chunks does not have to precede the
client's answer: what the client relies on is the new object. The gateway
proposes `Release` after answering (in the background, so it shares the next
entry with whatever is queued). The crash risk is unchanged from today, where
a crash between `PutObject` and `Release` already leaves the old chunks with
`refs` one too high: space leaks, data does not. Closing that leak is a
separate job (an offline reconciliation of `refs` against the index), noted
under Open questions.

## Per operation

| operation | entries before the answer, now | with this |
|---|---|---|
| PutObject (n chunks) | n + 1 (+ tags) (+ Release) | **1**: n × CommitChunk + PutObject (+ PutObjectTags); Release after |
| UploadPart (n chunks) | n + 1 | **1**: n × CommitChunk + PutPart; the replaced part's chunks released after |
| CompleteMultipartUpload | 1 (+ Release) | **1**; Release after |
| DeleteObject | 1 (+ Release) | **1**; Release after |
| CopyObject | Retain, then PutObject (+ Release) | **2**: Retain must succeed before the object is written; Release after |
| CreateBucket / DeleteBucket / tags | 1 | 1 (unchanged) |

### PutObject in detail

1. `ingest` uploads blocks and seals chunks as now, but instead of committing
   each chunk it returns them prepared: `Ingested { extents, md5, crc32, size,
   commits: Vec<Prepared<ChunkState>>, pins }`. The upload pins stay held
   inside `Ingested` until the entry is committed (sweep-invariants.md: a
   part is protected from the orphan sweep until its commit is applied).
2. Checksums are verified. **On a mismatch nothing has been committed**: the
   gateway drops `Ingested`, the pins go, the parts become orphans and the
   sweeper deletes them. Today every failure path proposes a `Release`.
3. `propose_all((commits, index.prepare_put(...), tags))`.
4. If `PutObject` was rejected (bucket gone, precondition failed): the chunks
   are committed with `refs = 1` and nothing points at them; the gateway
   proposes their `Release` (as today; a rare path).
5. Answer; then `Release` the old object's chunks.

Chunk ids and `BlobId`s are chosen by the proposer before upload, so a
prepared `CommitChunk` is fully determined when `ingest` returns: nothing in
`PutObject` needs an outcome from it.

### Memory and size

A big object is many chunks; holding their commits until the end means the
metadata of all of them (block lists, replica layouts) sits in memory and in
one entry. A 1 GiB object at 19 MiB chunks is ~55 `CommitChunk`s, a few KiB
each: well within an entry. Objects far larger than that (`MAX_LOGICAL_CHUNK`
× thousands) should commit in groups (`propose_all` every k chunks, the last
group with `PutObject`); the threshold is a constant, not config.

## Tests

- Unit: `propose_all` commits every command in one entry (one more position,
  one append), applies them in order, returns each outcome; a rejected one
  does not undo the others; a fenced tablet fails all of them.
- Gateway: a PUT is one entry (count entries on one tablet (`MetaStore::applied_position`));
  a checksum mismatch commits nothing and leaves only orphans that one sweep
  pass deletes; a rejected `PutObject` releases its chunks.
- `sweep_stress` and the S3 compat suites unchanged in outcome.
- Live: PUT latency on the Telegram log before and after.

## Steps

1. `Prepared<S>` and `propose_all` in `pigeonhole-meta`, with the unit tests.
2. Chunk store: `ingest` returns prepared commits and the pins
   (`BlobDb::prepare_commit`), callers that need a committed chunk right away
   (`put_small`, repair) use a helper that proposes them.
3. S3 index: `prepare_put`, `prepare_put_part`, `prepare_tags`; the gateway's
   PUT, UploadPart, Complete, Delete move to one entry and release after the
   answer. CAS (bytestream) gets the same treatment for `PutEntry`.
4. Docs: meta-backends.md (proposals and entries), sweep-invariants.md (pins
   held across the whole operation).

## Implementation notes (2026-10-28)

- Outcomes are taken by slot (`Proposal::push` → `Slot<S>`,
  `Outcomes::take(slot)`), not by tuple types.
- `ingest` returns `PendingChunks` (commits + upload pins);
  `PendingChunks::propose_in` + `Committing::done` (queue repair, drop the
  pins) or `ChunkStore::commit`; `ingest_and_commit` for callers with
  nothing to commit along.
- `ChunkStore::commit_along(chunks, meta, proposal)`: one entry when the
  other layer shares the tablet (always in the binary); tests that give each
  layer a tablet of its own get two entries, the chunks first.
- No grouping of commits for huge objects yet: every chunk of an object goes
  into its one entry (a few KiB per chunk).
- Releases after the answer are a `tokio::spawn` per operation, not a queue.
- CopyObject is unchanged (Retain, then PutObject); its Release moved after
  the answer.

## Open questions

- Releases after the answer in the background: bounded queue, retried on a
  stopped tablet? A crash still loses them (as today); worth a periodic
  reconciliation of `refs` against the index and CAS?
- `copy` with a `Retain` that cannot fail at apply time if the source is
  live? It can fail (the source chunk was released meanwhile), so it stays
  two entries unless the S3 namespace learns to check chunk liveness, which
  is rule 3.
