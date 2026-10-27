# Glossary / Словарь

Terms used in code, comments, and docs (Stage B onward).

| Term / Термин | Meaning / Что это | Size / Размер | Mutability / Изменяемость | Owner / Владелец |
|---|---|---|---|---|
| **Instance** | Storage place: bot + chat/channel (`tg-main`, `dc-backup`) / место хранения | — | config | `storage-*` + config |
| **Blob** | One message in one instance; address is `BlobLocator` / одно сообщение в одном инстансе | ≤ instance `max_blob_size` | immutable | blob storage (stateless) |
| **Block** | Chunk slice; unit of compression and Range / часть чанка, единица сжатия и Range | ~1 MiB logical | immutable | codec + chunk-store metadata |
| **Chunk** | Logical immutable storage unit: `ChunkId`, size, CRC, refcount; independent of backend limits / логическая единица хранения | config, e.g. 64 MiB | immutable (`refs` / replica set may change) | chunk store |
| **Replica** | Copy of a chunk on one instance = 1..N blobs (**parts**); part boundaries only on block edges / копия чанка в одном инстансе | — | created whole or absent | chunk store |
| **Extent** | `(chunk, offset, len)` — logical byte range in a chunk / диапазон логических байт чанка | ≤ chunk | immutable | chunk-store type; held by gateways |
| **Extent list** | Ordered extents = object bytes / упорядоченные экстенты = байты объекта | any | replaced as a whole | gateways |
| **Object** | Gateway entity: S3 object, `CasEntry`, Kafka segment / сущность шлюза | any | per protocol | gateways |
| **Tablet** | The metadata state machine: commands → `LogStore` → in-memory `StateMachine`s (`MetaStore`) / таблетка метаданных | — | one active incarnation | `pigeonhole-meta` |
| **LogStore** | Fencing command log the tablet appends batches to (`local`, later `telegram`, `s3`) / журнал команд с отсечением | — | append-only, collected behind snapshots | `pigeonhole-meta` |
| **Generation** | Number of a tablet incarnation; a newer claim fences every older one (`Fenced`) / поколение | u64 | strictly increasing | `LogStore` |
| **Position** | Index of an entry in the log (1, 2, …) / позиция записи | u64 | — | tablet |
| **Blob id** | Our id of a blob (`BlobId`), chosen before upload and sent as its file name `ph-<hex>.bin` / наш id блоба | 128 bit | immutable | chunk store |
| **Upload pin** | Process-local mark of an upload not committed yet; with the others it sets the sweep **horizon** / пин заливки | — | until commit | `UploadPins` |
| **Read pin** | Process-local mark of a chunk being read; reclaim waits for it / пин чтения | — | for one read | `BlobDb` |
| **Snapshot** | All namespace states as of a position (`TabletSnapshot`); the log behind it is dropped / снапшот таблетки | state size | replaced | tablet |
| **Zero entry** | Empty batch appended on boot: proves the claim and starts the generation / нулевая запись | 0 entries | immutable | tablet |

In code, **blob** means only a physical backend message. REAPI CAS “blobs” in the gateway are `CasEntry`.

See also [sweep invariants](sweep-invariants.md) (`live` / `reclaiming`, in-flight parts, per-batch liveness).

```
chunk (ChunkId, refs)
  ├─ blocks 0..N
  ├─ replica tg-main   = [part 0: blocks …] [part 1: blocks …]
  └─ replica dc-backup = [part 0: blocks …] …
```
