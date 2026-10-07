//! Gateway-facing blob layer API (stage 1.6).
//!
//! Gateways see only [`BlobId`] / [`ChunkRef`] — not `DynBlobBackend`, locators, or instances.

use crate::blob_db::BlobDb;
use crate::ingest::IngestOptions;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use pigeonhole_blob::{collect_stream, erase_sweep, SharedBackend, BlobLocator, Sweepable};
use pigeonhole_codec::{
    decode_blocks_range, ByteBudget, ChunkCodec, BlockRecord, BlockWriter, DEFAULT_CHUNK_SIZE,
};
use pigeonhole_types::ByteRange;
use std::sync::Arc;

/// Internal integer blob id (row in `blobs`).
pub type BlobId = i64;

/// Reference to an ingested chunk for ranged reads.
#[derive(Debug, Clone)]
pub struct ChunkRef {
    pub blob_id: BlobId,
}

/// Result of streaming ingest.
#[derive(Debug)]
pub struct Ingested {
    pub blobs: Vec<BlobId>,
    pub size: i64,
    pub md5: [u8; 16],
    pub crc32: u32,
}

/// Blob layer: metadata (`blob.db`) + typed backends behind [`DynBlobBackend`].
pub struct BlobLayer {
    db: BlobDb,
    write: SharedBackend,
    /// Instance id used for new replicas (must exist in `instances`).
    write_instance_id: String,
    chunk_size: usize,
    block_size: usize,
    codec: ChunkCodec,
    memory_budget: Option<ByteBudget>,
}

impl BlobLayer {
    pub async fn open<B: Sweepable>(
        db: BlobDb,
        backend: B,
        opts: IngestOptions,
    ) -> Result<Self> {
        let info = backend.instance().clone();
        db.sync_instances(&[info.clone()]).await?;
        let write_instance_id = info.id.clone();
        Ok(Self {
            db,
            write: Arc::new(erase_sweep(backend)),
            write_instance_id,
            chunk_size: opts.chunk_size.max(1024),
            block_size: opts.block_size.max(1024),
            codec: opts.codec,
            memory_budget: opts.memory_budget,
        })
    }

    pub fn db(&self) -> &BlobDb {
        &self.db
    }

    /// Shared write backend (journal/checkpoint segments, not gateway-facing).
    pub fn write_backend(&self) -> SharedBackend {
        self.write.clone()
    }

    pub fn write_instance_id(&self) -> &str {
        &self.write_instance_id
    }

    pub async fn put_small(&self, data: Bytes) -> Result<BlobId> {
        if data.is_empty() {
            bail!("put_small refuses empty payload");
        }
        let crc = crc32fast::hash(data.as_ref());
        let size = data.len() as i64;
        let stored = self.write.put(data).await.context("backend put_small")?;
        self.register_blob(size, crc, &stored, &[]).await
    }

    pub async fn set_root(&self, name: &str, id: BlobId) -> Result<()> {
        self.db.set_root(name, id).await
    }

    pub async fn get_root(&self, name: &str) -> Result<Option<BlobId>> {
        self.db.get_root(name).await
    }

    pub async fn retain(&self, ids: &[BlobId]) -> Result<()> {
        self.db.retain(ids).await
    }

    pub async fn release(&self, ids: &[BlobId]) -> Result<()> {
        self.db.release(ids).await
    }

    /// Ingest a body stream into one or more blobs (framed when codec compresses).
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

        if opts.codec == ChunkCodec::Raw {
            return self.ingest_raw(&mut stream, opts.chunk_size).await;
        }
        self.ingest_framed(&mut stream, opts).await
    }

    /// Read a logical byte range across chunk refs (None = entire concatenation).
    pub async fn read(
        &self,
        blobs: &[ChunkRef],
        range: Option<ByteRange>,
    ) -> Result<Bytes> {
        let mut pieces = Vec::new();
        let mut cursor = 0u64;
        for ch in blobs {
            let meta = self
                .db
                .blob_meta(ch.blob_id)
                .await?
                .with_context(|| format!("unknown blob_id {}", ch.blob_id))?;
            let logical = meta.0 as u64;
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
            pieces.push(self.read_blob_range(ch.blob_id, local_from, local_to).await?);
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

    async fn read_blob_range(&self, blob_id: BlobId, from: usize, to: usize) -> Result<Bytes> {
        let (_inst, _key, locator) = self
            .db
            .get_any_replica(blob_id)
            .await?
            .with_context(|| format!("no replica for blob {blob_id}"))?;
        let stored = BlobLocator {
            key: _key,
            locator,
        };
        let raw = collect_stream(self.write.get(&stored, None).await?).await?;
        let frames = self.db.get_blocks(blob_id).await?;
        if frames.is_empty() {
            if from > to || to > raw.len() {
                bail!("range {from}..{to} outside raw blob {}", raw.len());
            }
            return Ok(raw.slice(from..to));
        }
        decode_blocks_range(raw.as_ref(), &frames, from, to).context("decode frames range")
    }

    async fn register_blob(
        &self,
        size: i64,
        crc: u32,
        stored: &BlobLocator,
        blocks: &[BlockRecord],
    ) -> Result<BlobId> {
        let blob_id = self.db.insert_blob(size, crc).await?;
        self.db
            .add_replica(
                blob_id,
                &self.write_instance_id,
                &stored.key,
                &stored.locator,
            )
            .await?;
        if !blocks.is_empty() {
            self.db.replace_blocks(blob_id, blocks).await?;
        }
        Ok(blob_id)
    }

    async fn ingest_raw<S>(&self, stream: &mut S, chunk_size: usize) -> Result<Ingested>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    {
        let max = chunk_size.clamp(1024, pigeonhole_codec::MAX_CHUNK_SIZE);
        let mut md5 = Md5::new();
        let mut crc = crc32fast::Hasher::new();
        let mut buf = Vec::new();
        let mut blobs = Vec::new();
        let mut total = 0i64;

        while let Some(item) = stream.next().await {
            let chunk = item?;
            if chunk.is_empty() {
                continue;
            }
            md5.update(&chunk);
            crc.update(&chunk);
            total += chunk.len() as i64;
            buf.extend_from_slice(&chunk);
            while buf.len() >= max {
                let piece: Vec<u8> = buf.drain(..max).collect();
                blobs.push(self.put_raw_piece(piece).await?);
            }
        }
        if !buf.is_empty() {
            blobs.push(self.put_raw_piece(buf).await?);
        }
        let digest = md5.finalize();
        let mut md5_bytes = [0u8; 16];
        md5_bytes.copy_from_slice(&digest);
        Ok(Ingested {
            blobs,
            size: total,
            md5: md5_bytes,
            crc32: crc.finalize(),
        })
    }

    async fn put_raw_piece(&self, piece: Vec<u8>) -> Result<BlobId> {
        let logical = piece.len() as i64;
        let crc = crc32fast::hash(&piece);
        let stored = self
            .write
            .put(Bytes::from(piece))
            .await
            .context("backend put raw")?;
        self.register_blob(logical, crc, &stored, &[]).await
    }

    async fn ingest_framed<S>(&self, stream: &mut S, opts: IngestOptions) -> Result<Ingested>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    {
        let max_stored = opts.chunk_size.clamp(1024, pigeonhole_codec::MAX_CHUNK_SIZE);
        let max_logical = pigeonhole_codec::MAX_LOGICAL_CHUNK;
        let calls = opts
            .compress_calls
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicU64::new(0)));
        let mut writer = BlockWriter::new(
            opts.block_size,
            max_stored,
            max_logical,
            opts.codec,
            opts.memory_budget,
            calls,
        );
        let mut md5 = Md5::new();
        let mut crc = crc32fast::Hasher::new();
        let mut blobs = Vec::new();
        let mut total = 0i64;

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
                let (completed, consumed) = writer.push(rest).await?;
                if consumed == 0 && completed.is_empty() {
                    bail!("ingest made no progress");
                }
                rest = &rest[consumed..];
                for done in completed {
                    blobs.push(self.put_completed(done).await?);
                }
            }
        }
        for done in writer.finish().await? {
            blobs.push(self.put_completed(done).await?);
        }
        let digest = md5.finalize();
        let mut md5_bytes = [0u8; 16];
        md5_bytes.copy_from_slice(&digest);
        Ok(Ingested {
            blobs,
            size: total,
            md5: md5_bytes,
            crc32: crc.finalize(),
        })
    }

    async fn put_completed(&self, mut done: pigeonhole_codec::CompletedChunk) -> Result<BlobId> {
        let logical = done.logical_size;
        let frames = std::mem::take(&mut done.blocks);
        let payload = std::mem::take(&mut done.payload);
        let crc = crc32fast::hash(payload.as_ref());
        let put = self.write.put(payload).await.context("backend put framed");
        // Budget permits on `done` drop after put attempt.
        drop(done);
        let stored = put?;
        self.register_blob(logical, crc, &stored, &frames).await
    }
}

/// Build default ingest options for the layer.
pub fn default_layer_opts() -> IngestOptions {
    IngestOptions::new(DEFAULT_CHUNK_SIZE, ChunkCodec::Zstd)
}

/// Test helper: open a memory-backed layer with a temp `blob.db`.
#[cfg(test)]
pub async fn open_memory_layer(db_url: &str) -> Result<BlobLayer> {
    use pigeonhole_codec::DEFAULT_BLOCK_SIZE;
    use pigeonhole_storage_memory::MemoryBlobStore;
    let db = BlobDb::connect(db_url).await?;
    let mut opts = default_layer_opts();
    opts.chunk_size = 64 * 1024;
    opts.block_size = DEFAULT_BLOCK_SIZE;
    BlobLayer::open(db, MemoryBlobStore::new(), opts).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn ingest_read_retain_release_root() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let layer = open_memory_layer(&url).await.unwrap();

        let body = Bytes::from(vec![7u8; 200_000]);
        let ingested = layer
            .ingest(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None)
            .await
            .unwrap();
        assert_eq!(ingested.size, body.len() as i64);
        assert!(!ingested.blobs.is_empty());

        let refs: Vec<ChunkRef> = ingested
            .blobs
            .iter()
            .map(|&blob_id| ChunkRef { blob_id })
            .collect();
        let got = layer.read(&refs, None).await.unwrap();
        assert_eq!(got, body);

        let mid = body.len() as u64 / 2;
        let slice = layer.read(&refs, Some(mid..mid + 10)).await.unwrap();
        assert_eq!(slice.as_ref(), &body[mid as usize..mid as usize + 10]);

        layer.retain(&ingested.blobs).await.unwrap();
        layer.release(&ingested.blobs).await.unwrap();

        let small = layer.put_small(Bytes::from_static(b"snap")).await.unwrap();
        layer.set_root("s3/index", small).await.unwrap();
        assert_eq!(layer.get_root("s3/index").await.unwrap(), Some(small));
    }
}
