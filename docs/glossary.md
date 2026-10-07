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
| **Root** | Named pointer to a gateway index snapshot (`s3/index`) / именованный указатель на снапшот | — | atomic switch | chunk store |
| **Superblock** | Per-instance pin: generation, chunk-store checkpoint/journal, roots / закреп в инстансе | small | rewritten | chunk store |

In code, **blob** means only a physical backend message. REAPI CAS “blobs” in the gateway are `CasEntry`.

```
chunk (ChunkId, refs)
  ├─ blocks 0..N
  ├─ replica tg-main   = [part 0: blocks …] [part 1: blocks …]
  └─ replica dc-backup = [part 0: blocks …] …
```
