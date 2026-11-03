# Glossary

Terms used in code, comments, and docs (Stage B onward).

| Term | Meaning | Size | Mutability | Owner |
|---|---|---|---|---|
| **Store** (instance) | Where bytes live, a `[[store]]`: a dir, or bot + chat/channel (`tg-main`, `dc-backup`) | — | config | `storage-*` + config |
| **Replica group** | `[[replicas]]`: stores that each get a copy of every chunk written to the group | — | config | `Replicated` |
| **Shard** | One member of `[[shards]]` (a store or replica group); a chunk lives in exactly one shard. The chunk records it (`ChunkRec::shard`: the shard's stores at its write, plus those repair copied it to): repair copies a chunk only within the placement shard that holds its recorded stores (a store that joins that shard is backfilled); a re-sharding that puts them in different shards fails the chunk's repair jobs loudly (parked as unrepairable) instead of dropping them | — | config; recorded per chunk | `Replicated` + chunk store |
| **Blob** | One message in one instance; address is `BlobLocator` | ≤ instance `max_blob_size` | immutable | blob storage (stateless) |
| **Block** | Chunk slice; unit of compression and Range | ~1 MiB logical | immutable | codec + chunk-store metadata |
| **Chunk** | Logical immutable storage unit: `ChunkId`, size, CRC, refcount; independent of backend limits | ≤ `chunk.size` stored (19 MiB), ≤ 256 MiB logical | immutable (`refs` / replica set may change) | chunk store |
| **Replica** | Copy of a chunk on one instance = 1..N blobs (**parts**); part boundaries only on block edges | — | created whole or absent | chunk store |
| **Extent** | `(chunk, offset, len)` — logical byte range in a chunk | ≤ chunk | immutable | chunk-store type; held by gateways |
| **Extent list** | Ordered extents = object bytes | any | replaced as a whole | gateways |
| **Object** | Gateway entity: S3 object, `CasEntry`, Kafka segment | any | per protocol | gateways |
| **Tablet** | The metadata state machine: commands → `LogStore` → in-memory `StateMachine`s (`MetaStore`) | — | one active incarnation (in a cluster: a copy per node, only the Raft leader writes) | `pigeonhole-meta` |
| **LogStore** | Fencing command log the tablet appends batches to (`local_disk`, `memory`, `telegram`: a channel fenced by the order of message ids, `discord`: a channel fenced by threads or a lease); one `ChainLogStore` over a **Chain** | — | append-only, collected behind its base | `pigeonhole-meta` |
| **Chain** | A store's capability to link blobs: of several put after one `prev`, exactly one wins ([chain-log.md](chain-log.md)) | — | links final once won | `Chain` impl per store |
| **Label** | What a chain member is called, readable without its data (`log-claim-7`, `snapshot-1-20.snap`); a blob's name follows the same rule: 1–128 of `[A-Za-z0-9._-]` (`check_blob`) | ≤ 128 chars | immutable | `pigeonhole-storage-api` |
| **Decorator** | A store wrapped to add a concern (`Arc<B>`, `Faulty<B>`, `Erased<B>`, `MetricsBackend`); it forwards every method of every store trait its store implements ([chain-log.md](chain-log.md)) | — | — | its crate |
| **Lease** | A Discord log's hold on its chain for `[meta] lease` (default 45 s): lease records are linked by threads, batches between them are plain posts ([discord-lease.md](discord-lease.md)) | — | renewed while held | `pigeonhole-meta` (`log::chain`) |
| **Generation** | Number of a tablet incarnation; a newer claim fences every older one (`Fenced`) | u64 | strictly increasing | `LogStore` |
| **Position** | Index of an entry in the log (1, 2, …) | u64 | — | tablet |
| **Blob id** | Our id of a blob (`BlobId`), chosen before upload and sent as its file name `ph-<hex>.bin`; the top 32 bits are the **owner** | 128 bit | immutable | chunk store |
| **Owner** | A deployment's 32 random bits, chosen once and kept in the tablet; in every blob id it mints and in a marker pinned in each channel store, so two deployments never sweep each other's blobs ([store-owner.md](store-owner.md)) | u32 | fixed | tablet + chunk store |
| **Inline object** | An object of at most `[inline] max_object_size` kept in its metadata record instead of chunks; the cooler moves it out once cold ([inline-objects.md](inline-objects.md)) | ≤ 1 KiB default | until cooled | S3 gateway + tablet |
| **Upload pin** | Process-local mark of an upload not committed yet; with the others it sets the sweep **horizon** | — | until commit | `UploadPins` |
| **Read pin** | Process-local mark of a chunk being read; reclaim waits for it | — | for one read | `BlobDb` |
| **Snapshot** | All namespace states as of a position (`TabletSnapshot`), kept in a **SnapshotStore** (`local_disk`, `memory`, `telegram`, `discord`) | state size | replaced | tablet |
| **Base** | The stored snapshot a log starts from (`at` + `SnapshotRef`), set by `rebase`; entries ≤ `at` are dropped | — | moves forward | `LogStore` |
| **Node** | One process of a Raft cluster, with its own copy of the tablet (its own log and snapshots) and the shared blob stores ([cluster.md](cluster.md)) | — | config | binary |
| **Leader** | The node Raft elected that serves: once it applied its own term's first entry (so everything committed before), the only one that writes, serves S3 and runs background work (`MetaStore::is_leader`; Raft's role alone: `raft_state`) | — | per term | openraft |
| **Outbox** | A node's own channel, where it posts its Raft frames for the nodes that read it over the chat ([cluster.md](cluster.md)) | — | append-only | `openraft-chat` |
| **Zero entry** | Empty batch appended on boot: proves the claim and starts the generation | 0 entries | immutable | tablet |

In code, **blob** means only a physical backend message. REAPI CAS “blobs” in the gateway are `CasEntry`.

See also [sweep invariants](sweep-invariants.md) (`live` / `reclaiming`, in-flight parts, per-batch liveness).

```
chunk (ChunkId, refs)
  ├─ blocks 0..N
  ├─ replica tg-main   = [part 0: blocks …] [part 1: blocks …]
  └─ replica dc-backup = [part 0: blocks …] …
```
