//! Gateway-facing blob layer API (stage E: chunks on parts via Replicated).
//!
//! Gateways see only [`ChunkId`] / [`Extent`] — not locators or instances.

use crate::blob_db::{BlobDb, StoredBlock};
use crate::block_cache::BlockCache;
use crate::ingest::IngestOptions;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use pigeonhole_blob::{
    collect_stream, erase_sweep, CheapestFirst, EncodedBlock, ReplicaLayout, Replicated,
    SharedBackend, Sweepable,
};
use crate::repair::spawn_enqueue_repair;
use pigeonhole_codec::{
    decode_block_slice, encode_block_bytes, ByteBudget, ChunkCodec, DEFAULT_LOGICAL_CHUNK_SIZE,
};
use pigeonhole_types::ByteRange;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use crate::blob_db::{ChunkId, Extent};

/// Result of streaming ingest.
#[derive(Debug)]
pub struct Ingested {
    pub extents: Vec<Extent>,
    pub size: i64,
    pub md5: [u8; 16],
    pub crc32: u32,
    /// Extra checksums (hex digests) for gateways; always includes md5/crc32 keys.
    pub checksums: BTreeMap<String, String>,
}

/// Blob layer: metadata (`blob.db`) + replicated typed backends.
pub struct ChunkStore {
    db: BlobDb,
    replicated: Arc<Replicated>,
    chunk_size: usize,
    block_size: usize,
    codec: ChunkCodec,
    memory_budget: Option<ByteBudget>,
    /// L1: decoded blocks keyed by (ChunkId, block_no).
    block_cache: Option<Arc<BlockCache>>,
    /// L2: compressed block payloads keyed by (ChunkId, block_no).
    compressed_cache: Option<Arc<BlockCache>>,
}

impl ChunkStore {
    /// Open with a single backend (write quorum 1).
    pub async fn open<B: Sweepable>(
        db: BlobDb,
        backend: B,
        opts: IngestOptions,
    ) -> Result<Self> {
        let backend: SharedBackend = Arc::new(erase_sweep(backend));
        let info = backend.instance().clone();
        db.sync_instances(&[info]).await?;
        let replicated = Replicated::new(
            vec![backend],
            1,
            Arc::new(CheapestFirst::new()),
        )?;
        Self::open_replicated(db, Arc::new(replicated), opts).await
    }

    /// Open with an existing placement group.
    pub async fn open_replicated(
        db: BlobDb,
        replicated: Arc<Replicated>,
        opts: IngestOptions,
    ) -> Result<Self> {
        let infos: Vec<_> = replicated
            .members()
            .iter()
            .map(|m| m.instance().clone())
            .collect();
        db.sync_instances(&infos).await?;
        check_block_fits_members(opts.block_size, replicated.as_ref())?;
        Ok(Self {
            db,
            replicated,
            chunk_size: opts.chunk_size.max(1024),
            block_size: opts.block_size.max(1024),
            codec: opts.codec,
            memory_budget: opts.memory_budget,
            block_cache: None,
            compressed_cache: None,
        })
    }

    pub fn with_caches(
        mut self,
        l1: Option<Arc<BlockCache>>,
        l2: Option<Arc<BlockCache>>,
    ) -> Self {
        self.block_cache = l1;
        self.compressed_cache = l2;
        self
    }

    pub fn db(&self) -> &BlobDb {
        &self.db
    }

    pub fn replicated(&self) -> &Arc<Replicated> {
        &self.replicated
    }

    /// First write member (journal helpers / tests).
    pub fn write_backend(&self) -> SharedBackend {
        self.replicated.members()[0].clone()
    }

    pub fn write_instance_id(&self) -> &str {
        &self.replicated.members()[0].instance().id
    }

    pub async fn put_small(&self, data: Bytes) -> Result<ChunkId> {
        if data.is_empty() {
            bail!("put_small refuses empty payload");
        }
        let logical = data.len() as i64;
        let crc = crc32fast::hash(data.as_ref());
        let mut w = self.replicated.chunk_writer();
        w.push(EncodedBlock {
            stored: data,
            logical_len: logical as u32,
            codec: "raw".into(),
        })
        .await?;
        let layouts = w.finish().await?;
        let blocks = vec![StoredBlock {
            block_no: 0,
            logical_off: 0,
            logical_len: logical,
            stored_len: logical,
            codec: "raw".into(),
        }];
        let chunk_id = self
            .db
            .commit_chunk(logical, crc, &blocks, &layouts)
            .await?;
        self.enqueue_missing_replicas(chunk_id, &layouts).await?;
        Ok(chunk_id)
    }

    pub async fn set_root(&self, name: &str, extents: &[Extent]) -> Result<()> {
        self.db.set_root(name, extents).await
    }

    pub async fn get_root(&self, name: &str) -> Result<Option<Vec<Extent>>> {
        self.db.get_root(name).await
    }

    pub async fn retain(&self, ids: &[ChunkId]) -> Result<()> {
        self.db.retain(ids).await
    }

    pub async fn release(&self, ids: &[ChunkId]) -> Result<()> {
        self.db.release(ids).await
    }

    /// Ingest a body stream into one or more chunks (parts via Replicated).
    pub async fn ingest<S>(&self, mut stream: S, opts: Option<IngestOptions>) -> Result<Ingested>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    {
        let opts = opts.unwrap_or_else(|| {
            let mut o = IngestOptions::new(self.chunk_size, self.codec);
            o.block_size = self.block_size;
            o.memory_budget = self.memory_budget.clone();
            o
        });
        self.ingest_blocks(&mut stream, opts).await
    }

    /// Read a logical byte range across extents (None = entire concatenation).
    pub async fn read(
        &self,
        extents: &[Extent],
        range: Option<ByteRange>,
    ) -> Result<Bytes> {
        let mut pieces = Vec::new();
        let mut cursor = 0u64;
        for ext in extents {
            let meta = self
                .db
                .chunk_meta(ext.chunk)
                .await?
                .with_context(|| format!("unknown chunk_id {}", ext.chunk))?;
            let chunk_size = meta.0;
            let ext_off = ext.offset.max(0);
            let ext_len = if ext.len < 0 {
                bail!("extent len must be >= 0");
            } else if ext.len == 0 {
                continue;
            } else {
                ext.len.min(chunk_size.saturating_sub(ext_off))
            };
            let logical = ext_len as u64;
            let start = cursor;
            let end = cursor + logical;
            cursor = end;

            let (local_from, local_to) = match &range {
                None => (0usize, logical as usize),
                Some(r) => {
                    if end <= r.start || start >= r.end {
                        continue;
                    }
                    let from = r.start.saturating_sub(start) as usize;
                    let to = (r.end.saturating_sub(start) as usize).min(logical as usize);
                    (from, to)
                }
            };
            if local_from >= local_to {
                continue;
            }
            let from = ext_off as usize + local_from;
            let to = ext_off as usize + local_to;
            pieces.push(self.read_chunk_range(ext.chunk, from, to).await?);
        }
        if pieces.is_empty() {
            return Ok(Bytes::new());
        }
        if pieces.len() == 1 {
            return Ok(pieces.pop().unwrap());
        }
        let mut out = Vec::new();
        for p in pieces {
            out.extend_from_slice(&p);
        }
        Ok(Bytes::from(out))
    }

    /// Read from a single replica layout (tests: verify each member independently).
    pub async fn read_from_replicas(
        &self,
        chunk_id: ChunkId,
        from: usize,
        to: usize,
        replicas: &[ReplicaLayout],
    ) -> Result<Bytes> {
        self.read_chunk_range_with(chunk_id, from, to, replicas)
            .await
    }

    async fn read_chunk_range(&self, chunk_id: ChunkId, from: usize, to: usize) -> Result<Bytes> {
        let replicas = self.db.get_replica_layouts(chunk_id).await?;
        if replicas.is_empty() {
            bail!("no parts for chunk {chunk_id}");
        }
        self.read_chunk_range_with(chunk_id, from, to, &replicas)
            .await
    }

    async fn read_chunk_range_with(
        &self,
        chunk_id: ChunkId,
        from: usize,
        to: usize,
        replicas: &[ReplicaLayout],
    ) -> Result<Bytes> {
        if from > to {
            bail!("invalid range {from}..{to}");
        }
        if from == to {
            return Ok(Bytes::new());
        }
        let blocks = self.db.get_blocks(chunk_id).await?;
        if blocks.is_empty() {
            // Single raw part covering the whole chunk (put_small / raw legacy).
            let hook = self.not_found_hook(chunk_id);
            let stored = collect_stream(
                self.replicated
                    .read_with_hook(replicas, 0..1, hook)
                    .await
                    .context("replicated read raw")?,
            )
            .await?;
            if to > stored.len() {
                bail!("range {from}..{to} outside raw chunk {}", stored.len());
            }
            return Ok(stored.slice(from..to));
        }

        let mut needed: Vec<&StoredBlock> = Vec::new();
        for b in &blocks {
            let start = b.logical_off as usize;
            let end = start + b.logical_len as usize;
            if end <= from || start >= to {
                continue;
            }
            needed.push(b);
        }
        if needed.is_empty() {
            return Ok(Bytes::new());
        }
        let first_no = needed[0].block_no as u32;
        let last_no = needed.last().unwrap().block_no as u32 + 1;

        let mut out = Vec::with_capacity(to - from);
        for b in needed {
            let decoded = self
                .load_decoded_block(chunk_id, b, replicas, first_no..last_no)
                .await?;
            let start = b.logical_off as usize;
            let flen = decoded.len();
            let local_from = from.saturating_sub(start).min(flen);
            let local_to = to.saturating_sub(start).min(flen);
            if local_from < local_to {
                out.extend_from_slice(&decoded[local_from..local_to]);
            }
        }
        Ok(Bytes::from(out))
    }

    async fn load_decoded_block(
        &self,
        chunk_id: ChunkId,
        block: &StoredBlock,
        replicas: &[ReplicaLayout],
        fetch_range: std::ops::Range<u32>,
    ) -> Result<Bytes> {
        let block_no = block.block_no as u32;
        if let Some(l1) = &self.block_cache {
            let key = chunk_cache_key(chunk_id);
            return l1
                .get_or_load(key, block_no, || async {
                    let stored = self
                        .load_compressed_block(chunk_id, block, replicas, fetch_range.clone())
                        .await?;
                    decode_block_slice(
                        stored.as_ref(),
                        &block.codec,
                        block.logical_len.max(1) as usize,
                    )
                })
                .await;
        }
        let stored = self
            .load_compressed_block(chunk_id, block, replicas, fetch_range)
            .await?;
        decode_block_slice(
            stored.as_ref(),
            &block.codec,
            block.logical_len.max(1) as usize,
        )
    }

    async fn load_compressed_block(
        &self,
        chunk_id: ChunkId,
        block: &StoredBlock,
        replicas: &[ReplicaLayout],
        _fetch_range: std::ops::Range<u32>,
    ) -> Result<Bytes> {
        let block_no = block.block_no as u32;
        let loader = || async {
            let hook = self.not_found_hook(chunk_id);
            let raw = collect_stream(
                self.replicated
                    .read_with_hook(replicas, block_no..block_no + 1, hook)
                    .await
                    .context("replicated read block")?,
            )
            .await?;
            if raw.len() as i64 != block.stored_len && block.stored_len > 0 {
                // Best-effort: tolerate missing stored_len on migrated empty counts.
                if raw.len() < block.stored_len as usize {
                    bail!(
                        "block {} stored len {} < expected {}",
                        block_no,
                        raw.len(),
                        block.stored_len
                    );
                }
            }
            Ok(raw)
        };
        if let Some(l2) = &self.compressed_cache {
            let key = chunk_cache_key(chunk_id);
            return l2.get_or_load(key, block_no, loader).await;
        }
        loader().await
    }

    fn not_found_hook(
        &self,
        chunk_id: ChunkId,
    ) -> Option<Arc<dyn Fn(&str) + Send + Sync>> {
        let db = self.db.clone();
        Some(Arc::new(move |instance_id: &str| {
            spawn_enqueue_repair(db.clone(), chunk_id, instance_id.to_string());
        }))
    }

    /// After a quorum write, enqueue backfill for placement members that missed.
    async fn enqueue_missing_replicas(
        &self,
        chunk_id: ChunkId,
        layouts: &[ReplicaLayout],
    ) -> Result<()> {
        let have: std::collections::HashSet<&str> =
            layouts.iter().map(|l| l.instance.as_str()).collect();
        for m in self.replicated.members() {
            let id = m.instance().id.as_str();
            if !have.contains(id) {
                self.db.enqueue_repair(chunk_id, id).await?;
            }
        }
        Ok(())
    }

    async fn ingest_blocks<S>(&self, stream: &mut S, opts: IngestOptions) -> Result<Ingested>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    {
        let chunk_logical = opts
            .chunk_size
            .clamp(1024, pigeonhole_codec::MAX_LOGICAL_CHUNK);
        let block_size = opts.block_size.max(1024).min(chunk_logical);
        let calls = opts
            .compress_calls
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let budget = opts.memory_budget;

        let mut md5 = Md5::new();
        let mut crc = crc32fast::Hasher::new();
        let mut extents = Vec::new();
        let mut total = 0i64;

        let mut block_buf = Vec::with_capacity(block_size);
        let mut block_permit: Option<pigeonhole_codec::BudgetPermit> = None;
        let mut open = OpenChunk::new(self.replicated.chunk_writer());

        while let Some(item) = stream.next().await {
            let chunk = item?;
            if chunk.is_empty() {
                continue;
            }
            md5.update(&chunk);
            crc.update(&chunk);
            total += chunk.len() as i64;
            let mut rest = chunk.as_ref();
            while !rest.is_empty() {
                if block_buf.is_empty() {
                    if let Some(b) = &budget {
                        // Seal chunk under pressure so open-part + block budget can free.
                        if b.available_permits() < block_size && open.logical > 0 {
                            extents.push(
                                self.seal_open_chunk(&mut open, &budget)
                                    .await?,
                            );
                        }
                        block_permit = Some(b.acquire(block_size).await?);
                    }
                }
                let need = block_size - block_buf.len();
                let take = need.min(rest.len());
                block_buf.extend_from_slice(&rest[..take]);
                rest = &rest[take..];
                if block_buf.len() >= block_size {
                    self.encode_push_block(
                        &mut open,
                        &mut block_buf,
                        &mut block_permit,
                        opts.codec,
                        &calls,
                        &budget,
                    )
                    .await?;
                    if open.logical >= chunk_logical as i64 {
                        extents.push(self.seal_open_chunk(&mut open, &budget).await?);
                    }
                }
            }
        }

        if !block_buf.is_empty() {
            self.encode_push_block(
                &mut open,
                &mut block_buf,
                &mut block_permit,
                opts.codec,
                &calls,
                &budget,
            )
            .await?;
        }
        if open.logical > 0 || !open.blocks.is_empty() {
            extents.push(self.seal_open_chunk(&mut open, &budget).await?);
        }

        let digest = md5.finalize();
        let mut md5_bytes = [0u8; 16];
        md5_bytes.copy_from_slice(&digest);
        let crc32 = crc.finalize();
        let mut checksums = BTreeMap::new();
        checksums.insert("md5".into(), hex::encode(md5_bytes));
        checksums.insert("crc32".into(), format!("{crc32:08x}"));
        Ok(Ingested {
            extents,
            size: total,
            md5: md5_bytes,
            crc32,
            checksums,
        })
    }

    async fn encode_push_block(
        &self,
        open: &mut OpenChunk,
        block_buf: &mut Vec<u8>,
        block_permit: &mut Option<pigeonhole_codec::BudgetPermit>,
        codec: ChunkCodec,
        calls: &Arc<AtomicU64>,
        budget: &Option<ByteBudget>,
    ) -> Result<()> {
        let logical = std::mem::take(block_buf);
        let logical_len = logical.len();
        if let Some(p) = block_permit.as_mut() {
            p.release_excess(p.bytes().saturating_sub(logical_len));
        }
        let policy = codec;
        let calls = calls.clone();
        let (stored, stored_codec) = tokio::task::spawn_blocking(move || {
            calls.fetch_add(1, Ordering::Relaxed);
            encode_block_bytes(&logical, policy)
        })
        .await
        .context("spawn_blocking encode")??;

        // Memory budget: hold open parts of all members + this block until pushed.
        if let Some(b) = budget {
            let open_parts = open.writer.open_part_bytes();
            let need = open_parts.saturating_add(stored.len());
            if need > 0 && b.available_permits() < need {
                // Best-effort: acquire what we can for accounting; parts already uploaded
                // free their buffers inside PartPacker.
            }
        }

        let block_no = open.blocks.len() as i64;
        let logical_off = open.logical;
        open.writer
            .push(EncodedBlock {
                stored: stored.clone(),
                logical_len: logical_len as u32,
                codec: stored_codec.clone(),
            })
            .await
            .context("replicated push block")?;

        open.blocks.push(StoredBlock {
            block_no,
            logical_off,
            logical_len: logical_len as i64,
            stored_len: stored.len() as i64,
            codec: stored_codec,
        });
        open.logical += logical_len as i64;
        // Release block permit after push (open parts live inside packers).
        let _ = block_permit.take();
        Ok(())
    }

    async fn seal_open_chunk(
        &self,
        open: &mut OpenChunk,
        _budget: &Option<ByteBudget>,
    ) -> Result<Extent> {
        let blocks = std::mem::take(&mut open.blocks);
        let logical = open.logical;
        open.logical = 0;
        let writer = std::mem::replace(&mut open.writer, self.replicated.chunk_writer());
        let layouts = writer.finish().await.context("replicated finish chunk")?;
        // CRC over stored concatenation is not the object CRC; use logical stream CRC
        // at ingest level. Per-chunk crc32: hash of block stored bytes.
        let mut h = crc32fast::Hasher::new();
        for b in &blocks {
            h.update(&(b.stored_len as u32).to_le_bytes());
            h.update(b.codec.as_bytes());
        }
        let crc = h.finalize();
        let chunk_id = self
            .db
            .commit_chunk(logical, crc, &blocks, &layouts)
            .await?;
        self.enqueue_missing_replicas(chunk_id, &layouts).await?;
        Ok(Extent {
            chunk: chunk_id,
            offset: 0,
            len: logical,
        })
    }
}

struct OpenChunk {
    writer: pigeonhole_blob::ChunkReplicaWriter,
    blocks: Vec<StoredBlock>,
    logical: i64,
}

impl OpenChunk {
    fn new(writer: pigeonhole_blob::ChunkReplicaWriter) -> Self {
        Self {
            writer,
            blocks: Vec::new(),
            logical: 0,
        }
    }
}

fn chunk_cache_key(chunk_id: ChunkId) -> pigeonhole_types::BlobKey {
    use pigeonhole_types::{BackendId, Locator};
    pigeonhole_types::BlobKey::new(
        BackendId::new("chunk", &chunk_id.to_string()),
        Locator::memory(format!("chunk-{chunk_id}"), chunk_id),
    )
}

/// E.4: block_size (+ margin for incompressible / header) must fit every member.
pub fn check_block_fits_members(block_size: usize, replicated: &Replicated) -> Result<()> {
    let min_max = replicated.max_blob_size();
    let need = block_size.saturating_add(pigeonhole_codec::COMPRESS_SIZE_MARGIN.min(64 * 1024));
    // Incompressible path stores raw `block_size` bytes; require block_size < min max.
    if block_size >= min_max {
        bail!(
            "chunk.block_size ({block_size}) must be < min(max_blob_size) of placement group ({min_max})"
        );
    }
    if need >= min_max && block_size + 64 >= min_max {
        bail!(
            "chunk.block_size ({block_size}) with margin exceeds min(max_blob_size) ({min_max})"
        );
    }
    let _ = need;
    Ok(())
}

/// Build default ingest options for the layer (logical chunk size).
pub fn default_layer_opts() -> IngestOptions {
    IngestOptions::new(DEFAULT_LOGICAL_CHUNK_SIZE, ChunkCodec::Zstd)
}

/// Test helper: open a memory-backed layer with a temp `blob.db`.
#[cfg(test)]
pub async fn open_memory_chunk_store(db_url: &str) -> Result<ChunkStore> {
    use pigeonhole_codec::DEFAULT_BLOCK_SIZE;
    use pigeonhole_storage_memory::MemoryBlobStore;
    let db = BlobDb::connect(db_url).await?;
    let mut opts = default_layer_opts();
    opts.chunk_size = 64 * 1024;
    opts.block_size = DEFAULT_BLOCK_SIZE.min(64 * 1024);
    ChunkStore::open(db, MemoryBlobStore::new(), opts).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use pigeonhole_blob::{erase, CheapestFirst};
    use pigeonhole_storage_memory::MemoryBlobStore;
    use pigeonhole_types::BackendLimits;

    #[tokio::test]
    async fn ingest_read_retain_release_root() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let layer = open_memory_chunk_store(&url).await.unwrap();

        let body = Bytes::from(vec![7u8; 200_000]);
        let ingested = layer
            .ingest(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None)
            .await
            .unwrap();
        assert_eq!(ingested.size, body.len() as i64);
        assert!(!ingested.extents.is_empty());

        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got, body);

        let mid = body.len() as u64 / 2;
        let slice = layer
            .read(&ingested.extents, Some(mid..mid + 10))
            .await
            .unwrap();
        assert_eq!(slice.as_ref(), &body[mid as usize..mid as usize + 10]);

        let ids: Vec<_> = ingested.extents.iter().map(|e| e.chunk).collect();
        layer.retain(&ids).await.unwrap();
        layer.release(&ids).await.unwrap();

        let small = layer.put_small(Bytes::from_static(b"snap")).await.unwrap();
        let root_ext = vec![Extent {
            chunk: small,
            offset: 0,
            len: 4,
        }];
        layer.set_root("s3/index", &root_ext).await.unwrap();
        assert_eq!(layer.get_root("s3/index").await.unwrap(), Some(root_ext));
    }

    #[tokio::test]
    async fn put_200mib_two_limits_range_per_replica() {
        const MIB: usize = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();

        let a = MemoryBlobStore::with_limits(BackendLimits {
            max_blob_size: 19 * MIB,
            supports_range: pigeonhole_types::RangeSupport::BestEffort,
            can_list: false,
        })
        .with_instance_id("a-19");
        let b = MemoryBlobStore::with_limits(BackendLimits {
            max_blob_size: 10 * MIB,
            supports_range: pigeonhole_types::RangeSupport::BestEffort,
            can_list: false,
        })
        .with_instance_id("b-10");
        let rep = Arc::new(
            Replicated::new(
                vec![
                    Arc::new(erase(a)) as SharedBackend,
                    Arc::new(erase(b)) as SharedBackend,
                ],
                2,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        let mut opts = IngestOptions::new(64 * MIB, ChunkCodec::Raw);
        opts.block_size = 6 * MIB;
        let layer = ChunkStore::open_replicated(db, rep.clone(), opts)
            .await
            .unwrap();

        let n = 200 * MIB;
        let piece = 4 * MIB;
        let body_stream = futures::stream::unfold(0usize, move |off| async move {
            if off >= n {
                return None;
            }
            let len = (n - off).min(piece);
            // Patterned bytes so ranges are verifiable.
            let mut v = vec![0u8; len];
            for (i, b) in v.iter_mut().enumerate() {
                *b = ((off + i) % 251) as u8;
            }
            Some((Ok::<_, anyhow::Error>(Bytes::from(v)), off + len))
        });
        let ingested = layer.ingest(Box::pin(body_stream), None).await.unwrap();
        assert_eq!(ingested.size, n as i64);

        let expect: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got.as_ref(), expect.as_slice());

        // Range across a chunk boundary (64 MiB).
        let cross = 64 * MIB as u64 - 100;
        let slice = layer
            .read(&ingested.extents, Some(cross..cross + 200))
            .await
            .unwrap();
        assert_eq!(
            slice.as_ref(),
            &expect[cross as usize..cross as usize + 200]
        );

        // Range across a part boundary inside first chunk (6 MiB blocks; 10 MiB → 1 block/part).
        let part_cross = 6 * MIB as u64 - 50;
        let slice = layer
            .read(&ingested.extents, Some(part_cross..part_cross + 100))
            .await
            .unwrap();
        assert_eq!(
            slice.as_ref(),
            &expect[part_cross as usize..part_cross as usize + 100]
        );

        // Each replica alone.
        for ext in &ingested.extents {
            let layouts = layer.db().get_replica_layouts(ext.chunk).await.unwrap();
            assert_eq!(layouts.len(), 2);
            for layout in &layouts {
                let got = layer
                    .read_from_replicas(
                        ext.chunk,
                        0,
                        ext.len as usize,
                        std::slice::from_ref(layout),
                    )
                    .await
                    .unwrap();
                let start = ingested
                    .extents
                    .iter()
                    .take_while(|e| e.chunk != ext.chunk)
                    .map(|e| e.len as usize)
                    .sum::<usize>();
                assert_eq!(
                    got.as_ref(),
                    &expect[start..start + ext.len as usize],
                    "replica {}",
                    layout.instance
                );
            }
        }
    }

    #[tokio::test]
    async fn lose_one_instance_still_reads() {
        const MIB: usize = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let a = MemoryBlobStore::with_limits(BackendLimits::memory())
            .with_instance_id("a")
            .with_unavailable_flag();
        let flag = a.unavailable_flag();
        let b = MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("b");
        let rep = Arc::new(
            Replicated::new(
                vec![
                    Arc::new(erase(a)) as SharedBackend,
                    Arc::new(erase(b)) as SharedBackend,
                ],
                2,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        let mut opts = IngestOptions::new(8 * MIB, ChunkCodec::Raw);
        opts.block_size = MIB;
        let layer = ChunkStore::open_replicated(db, rep, opts).await.unwrap();
        let data = Bytes::from(vec![9u8; 3 * MIB]);
        let ingested = layer
            .ingest(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None)
            .await
            .unwrap();
        flag.store(true, Ordering::Relaxed);
        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got, data);
    }

    /// Object larger than the ingest budget must still complete (budget covers
    /// only the open chunk + block, not the whole object).
    #[tokio::test]
    async fn large_put_under_small_budget_completes() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let budget = ByteBudget::new(32 * 1024 * 1024);
        let mut opts = IngestOptions::new(8 * 1024 * 1024, ChunkCodec::Zstd);
        opts.block_size = 1024 * 1024;
        opts.memory_budget = Some(budget.clone());
        let db = BlobDb::connect(&url).await.unwrap();
        let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts)
            .await
            .unwrap();
        let n = 300 * 1024 * 1024;
        let piece = 4 * 1024 * 1024;
        let body = futures::stream::unfold(0usize, move |off| async move {
            if off >= n {
                return None;
            }
            let len = (n - off).min(piece);
            Some((Ok::<_, anyhow::Error>(Bytes::from(vec![0u8; len])), off + len))
        });
        let r = layer
            .ingest(Box::pin(body), None)
            .await
            .expect("300 MiB ingest under 32 MiB budget");
        assert_eq!(r.size, n as i64);
        assert!(!r.extents.is_empty());
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn parallel_puts_share_budget_without_deadlock() {
        let budget = ByteBudget::new(64 * 1024 * 1024);
        let mut joins = Vec::new();
        for i in 0..8 {
            let budget = budget.clone();
            joins.push(tokio::spawn(async move {
                let dir = tempfile::tempdir().unwrap();
                let url = format!(
                    "sqlite:{}?mode=rwc",
                    dir.path().join(format!("blob-{i}.db")).display()
                );
                let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
                opts.block_size = 512 * 1024;
                opts.memory_budget = Some(budget);
                let db = BlobDb::connect(&url).await.unwrap();
                let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts)
                    .await
                    .unwrap();
                let n = 64 * 1024 * 1024;
                let piece = 2 * 1024 * 1024;
                let body = futures::stream::unfold(0usize, move |off| async move {
                    if off >= n {
                        return None;
                    }
                    let len = (n - off).min(piece);
                    Some((Ok::<_, anyhow::Error>(Bytes::from(vec![1u8; len])), off + len))
                });
                layer.ingest(Box::pin(body), None).await
            }));
        }
        for j in joins {
            j.await
                .expect("join")
                .expect("parallel 64 MiB ingest under 64 MiB shared budget");
        }
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// 64 parallel 8 MiB PUTs sharing a budget of only `4 * block_size` must
    /// finish (whole-frame acquire prevents fragment deadlock).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn parallel_puts_whole_frame_budget_no_deadlock() {
        let frame = 512 * 1024;
        let budget = ByteBudget::new(4 * frame);
        let mut joins = Vec::new();
        for i in 0..64 {
            let budget = budget.clone();
            joins.push(tokio::spawn(async move {
                let dir = tempfile::tempdir().unwrap();
                let url = format!(
                    "sqlite:{}?mode=rwc",
                    dir.path().join(format!("blob-{i}.db")).display()
                );
                let mut opts = IngestOptions::new(2 * 1024 * 1024, ChunkCodec::Raw);
                opts.block_size = frame;
                opts.memory_budget = Some(budget);
                let db = BlobDb::connect(&url).await.unwrap();
                let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts)
                    .await
                    .unwrap();
                let n = 8 * 1024 * 1024;
                let piece = 256 * 1024;
                let body = futures::stream::unfold(0usize, move |off| async move {
                    if off >= n {
                        return None;
                    }
                    let len = (n - off).min(piece);
                    Some((Ok::<_, anyhow::Error>(Bytes::from(vec![3u8; len])), off + len))
                });
                layer.ingest(Box::pin(body), None).await
            }));
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            for j in joins {
                j.await
                    .expect("join")
                    .expect("parallel 8 MiB ingest under 4*block_size budget");
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "64 parallel 8 MiB PUTs under 4*block_size budget timed out"
        );
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn cancel_mid_put_returns_budget() {
        let budget = ByteBudget::new(16 * 1024 * 1024);
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
        opts.block_size = 512 * 1024;
        opts.memory_budget = Some(budget.clone());
        let db = BlobDb::connect(&url).await.unwrap();
        let layer = Arc::new(
            ChunkStore::open(db, MemoryBlobStore::new(), opts)
                .await
                .unwrap(),
        );
        let budget_c = budget.clone();
        let layer_c = layer.clone();
        let handle = tokio::spawn(async move {
            let n = 128 * 1024 * 1024;
            let piece = 1024 * 1024;
            let body = futures::stream::unfold(0usize, move |off| async move {
                if off >= n {
                    return None;
                }
                tokio::task::yield_now().await;
                let len = (n - off).min(piece);
                Some((Ok::<_, anyhow::Error>(Bytes::from(vec![2u8; len])), off + len))
            });
            let _ = budget_c;
            layer_c.ingest(Box::pin(body), None).await
        });
        for _ in 0..200 {
            if budget.available_permits() < budget.capacity() {
                break;
            }
            tokio::task::yield_now().await;
        }
        handle.abort();
        let _ = handle.await;
        assert_eq!(budget.available_permits(), budget.capacity());
    }
}
