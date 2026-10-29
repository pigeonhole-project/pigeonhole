//! Gateway-facing blob layer API (stage E: chunks on parts via Replicated).
//!
//! Gateways see only [`ChunkId`] / [`Extent`] — not locators or instances.

use crate::blob_db::{BlobDb, StoredBlock};
use crate::commands::ChunkState;
use pigeonhole_meta::{Outcomes, Proposal, Prepared, Slot};
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
    /// The chunks of `extents`, uploaded but not committed: commit them (in
    /// the entry of whatever points at them) before using the extents.
    pub chunks: PendingChunks,
}

/// Chunks uploaded and sealed but not committed. Their parts stay protected
/// from the orphan sweep (upload pins) until the commit is applied; dropped
/// uncommitted, they become orphans the sweeper deletes. Nothing else is
/// ever needed to undo them.
#[must_use = "commit the chunks, or drop them to leave their parts to the sweeper"]
#[derive(Default)]
pub struct PendingChunks {
    chunks: Vec<PendingChunk>,
}

struct PendingChunk {
    chunk_id: ChunkId,
    commit: Prepared<ChunkState>,
    layouts: Vec<pigeonhole_blob::ReplicaLayout>,
    pins: Vec<pigeonhole_blob::UploadPin>,
}

impl std::fmt::Debug for PendingChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.chunks.iter().map(|c| c.chunk_id)).finish()
    }
}

impl PendingChunks {
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Add the commits to `proposal`; once it is committed, finish with
    /// [`Committing::done`].
    pub fn propose_in(self, proposal: &mut Proposal) -> Committing {
        let mut slots = Vec::with_capacity(self.chunks.len());
        let mut rest = Vec::with_capacity(self.chunks.len());
        for c in self.chunks {
            slots.push(proposal.push(c.commit));
            rest.push((c.chunk_id, c.layouts, c.pins));
        }
        Committing { slots, chunks: rest }
    }
}

/// Chunk commits inside a proposal on its way.
#[must_use = "finish with done() once the proposal is committed"]
pub struct Committing {
    slots: Vec<Slot<ChunkState>>,
    chunks: Vec<(ChunkId, Vec<pigeonhole_blob::ReplicaLayout>, Vec<pigeonhole_blob::UploadPin>)>,
}

impl Committing {
    /// Check the commits were applied, queue repair for replicas the write
    /// missed, and release the upload pins.
    pub async fn done(self, store: &ChunkStore, outcomes: &mut Outcomes) -> Result<()> {
        for slot in self.slots {
            if let Err(reason) = outcomes.take(slot)? {
                bail!("chunk commit rejected: {reason:?}");
            }
        }
        for (chunk_id, layouts, pins) in self.chunks {
            store.enqueue_missing_replicas(chunk_id, &layouts).await?;
            drop(pins);
        }
        Ok(())
    }
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
    /// Put the blocks of every chunk written into L2.
    write_through: bool,
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
        // Uncommitted parts at or below the highest committed key are a dead
        // writer's garbage: start the sweep horizon there.
        for info in &infos {
            if let Some(key) = db.max_part_key(&info.id) {
                replicated.pins().seed(&info.id, &key);
            }
        }
        Ok(Self {
            db,
            replicated,
            chunk_size: opts.chunk_size.max(1024),
            block_size: opts.block_size.max(1024),
            codec: opts.codec,
            memory_budget: opts.memory_budget,
            block_cache: None,
            compressed_cache: None,
            write_through: false,
        })
    }

    /// `write_through`: chunks written put their stored blocks into `l2`.
    pub fn with_caches(
        mut self,
        l1: Option<Arc<BlockCache>>,
        l2: Option<Arc<BlockCache>>,
        write_through: bool,
    ) -> Self {
        self.block_cache = l1;
        self.compressed_cache = l2;
        self.write_through = write_through;
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
        let sealed = w.finish().await?;
        let blocks = vec![StoredBlock {
            block_no: 0,
            logical_off: 0,
            logical_len: logical,
            stored_len: logical,
            codec: "raw".into(),
            crc32: crc,
        }];
        let chunk_id = self
            .db
            .commit_chunk(logical, crc, &blocks, &sealed.layouts)
            .await?;
        self.enqueue_missing_replicas(chunk_id, &sealed.layouts)
            .await?;
        drop(sealed.pins);
        Ok(chunk_id)
    }

    pub async fn retain(&self, ids: &[ChunkId]) -> Result<()> {
        self.db.retain(ids).await
    }

    /// Commit `chunks` alone (one entry), for callers with nothing to commit
    /// along with them.
    pub async fn commit(&self, chunks: PendingChunks) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let mut proposal = Proposal::new();
        let committing = chunks.propose_in(&mut proposal);
        let mut outcomes = self.db.meta().propose_all(proposal).await?;
        committing.done(self, &mut outcomes).await
    }

    /// Commit `chunks` along with `proposal` (another layer's commands for
    /// `meta`): one entry when both live in one tablet (always, in the
    /// binary), else the chunks first. The outcomes of `proposal`.
    pub async fn commit_along(
        &self,
        chunks: PendingChunks,
        meta: &Arc<pigeonhole_meta::MetaStore>,
        mut proposal: Proposal,
    ) -> Result<Outcomes> {
        if !Arc::ptr_eq(meta, self.db.meta()) {
            self.commit(chunks).await?;
            return meta.propose_all(proposal).await;
        }
        let committing = chunks.propose_in(&mut proposal);
        let mut outcomes = meta.propose_all(proposal).await?;
        committing.done(self, &mut outcomes).await?;
        Ok(outcomes)
    }

    /// The tablet the chunk state lives in (to propose prepared commands).
    pub fn meta(&self) -> &Arc<pigeonhole_meta::MetaStore> {
        self.db.meta()
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

    /// [`Self::ingest`] and commit the chunks at once (one entry), for
    /// callers with nothing to commit along with them.
    pub async fn ingest_and_commit<S>(&self, stream: S, opts: Option<IngestOptions>) -> Result<Ingested>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin,
    {
        let mut ingested = self.ingest(stream, opts).await?;
        self.commit(std::mem::take(&mut ingested.chunks)).await?;
        Ok(ingested)
    }

    /// Read a logical byte range across extents (None = entire concatenation).
    ///
    /// The chunks stay pinned for the whole read, so the sweeper does not
    /// delete their parts underneath it. A chunk already reclaimed (the object
    /// was overwritten or deleted after its extents were read) fails with
    /// [`ChunkGone`](crate::ChunkGone): re-read the extents and retry.
    pub async fn read(
        &self,
        extents: &[Extent],
        range: Option<ByteRange>,
    ) -> Result<Bytes> {
        let ids: Vec<ChunkId> = extents.iter().map(|e| e.chunk).collect();
        let _pin = self.db.pin_reads(&ids);
        for &chunk_id in &ids {
            if !self.db.chunk_readable(chunk_id) {
                return Err(anyhow::Error::new(crate::ChunkGone { chunk_id }));
            }
        }
        let mut pieces = Vec::new();
        let mut cursor = 0u64;
        for ext in extents {
            let meta = self
                .db
                .chunk_meta(ext.chunk)
                .await?
                .ok_or(crate::ChunkGone { chunk_id: ext.chunk })?;
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
            // Single raw part covering the whole chunk (put_small).
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
            return l1
                .get_or_load(chunk_id, block_no, || async {
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
            let mut raw = collect_stream(
                self.replicated
                    .read_with_hook(replicas, block_no..block_no + 1, hook)
                    .await
                    .context("replicated read block")?,
            )
            .await?;
            if crc32fast::hash(&raw) != block.crc32 {
                raw = self.read_block_around_damage(chunk_id, block, replicas).await?;
            }
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
            return l2.get_or_load(chunk_id, block_no, loader).await;
        }
        loader().await
    }

    /// A block failed its checksum: read it from each replica in turn until
    /// one is intact. The damaged replica stays as it is (repair rewrites
    /// missing copies, not damaged ones).
    async fn read_block_around_damage(
        &self,
        chunk_id: ChunkId,
        block: &StoredBlock,
        replicas: &[ReplicaLayout],
    ) -> Result<Bytes> {
        let block_no = block.block_no as u32;
        for r in replicas {
            let Ok(stream) = self.replicated.read_with_hook(std::slice::from_ref(r), block_no..block_no + 1, None).await
            else {
                continue;
            };
            let Ok(raw) = collect_stream(stream).await else { continue };
            if crc32fast::hash(&raw) == block.crc32 {
                return Ok(raw);
            }
            tracing::error!(chunk_id, block_no, instance = %r.instance, "block fails its checksum on this replica");
        }
        bail!("chunk {chunk_id} block {block_no} fails its checksum on every replica")
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
        let mut md5 = Md5::new();
        let mut crc = crc32fast::Hasher::new();
        let mut total = 0i64;
        let mut feeder = BlockFeeder::new(self, opts);
        while let Some(item) = stream.next().await {
            let bytes = item?;
            md5.update(&bytes);
            crc.update(&bytes);
            total += bytes.len() as i64;
            feeder.feed(&bytes).await?;
        }
        let (extents, pending) = feeder.finish().await?;

        let md5: [u8; 16] = md5.finalize().into();
        let crc32 = crc.finalize();
        let mut checksums = BTreeMap::new();
        checksums.insert("md5".into(), hex::encode(md5));
        checksums.insert("crc32".into(), format!("{crc32:08x}"));
        Ok(Ingested { extents, size: total, md5, crc32, checksums, chunks: pending })
    }

    async fn encode_push_block(
        &self,
        open: &mut OpenChunk,
        block_buf: &mut Vec<u8>,
        block_permit: &mut Option<pigeonhole_codec::BudgetPermit>,
        codec: ChunkCodec,
        calls: &Arc<AtomicU64>,
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
        pigeonhole_blob::record_compression_ratio(stored.len(), logical_len);

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

        if self.write_through && self.compressed_cache.is_some() {
            open.stored.push(stored.clone());
        }
        open.blocks.push(StoredBlock {
            block_no,
            logical_off,
            logical_len: logical_len as i64,
            stored_len: stored.len() as i64,
            codec: stored_codec,
            crc32: crc32fast::hash(&stored),
        });
        open.logical += logical_len as i64;
        // Release block permit after push (open parts live inside packers).
        let _ = block_permit.take();
        Ok(())
    }

    async fn seal_open_chunk(
        &self,
        open: &mut OpenChunk,
        pending: &mut PendingChunks,
    ) -> Result<Extent> {
        let blocks = std::mem::take(&mut open.blocks);
        let stored = std::mem::take(&mut open.stored);
        let logical = open.logical;
        open.logical = 0;
        let writer = std::mem::replace(&mut open.writer, self.replicated.chunk_writer());
        let sealed = writer.finish().await.context("replicated finish chunk")?;
        // CRC over stored concatenation is not the object CRC; use logical stream CRC
        // at ingest level. Per-chunk crc32: hash of block stored bytes.
        let mut h = crc32fast::Hasher::new();
        for b in &blocks {
            h.update(&(b.stored_len as u32).to_le_bytes());
            h.update(b.codec.as_bytes());
        }
        let crc = h.finalize();
        // Committed by the caller, with whatever points at it; the uploads
        // stay pinned until then.
        let (chunk_id, commit) = self.db.prepare_commit(logical, crc, &blocks, &sealed.layouts);
        // Ids are never reused: an entry of a chunk that fails to commit is
        // never asked for.
        if let Some(l2) = &self.compressed_cache {
            for (block_no, data) in stored.into_iter().enumerate() {
                l2.insert(chunk_id, block_no as u32, data).await;
            }
        }
        pending.chunks.push(PendingChunk { chunk_id, commit, layouts: sealed.layouts, pins: sealed.pins });
        Ok(Extent {
            chunk: chunk_id,
            offset: 0,
            len: logical,
        })
    }
}

/// Cuts a stream into blocks and the blocks into chunks as the bytes come,
/// within the memory budget.
struct BlockFeeder<'a> {
    store: &'a ChunkStore,
    chunk_logical: i64,
    block_size: usize,
    codec: ChunkCodec,
    calls: Arc<AtomicU64>,
    budget: Option<ByteBudget>,
    /// The block being filled, and the memory it holds.
    block: Vec<u8>,
    permit: Option<pigeonhole_codec::BudgetPermit>,
    open: OpenChunk,
    extents: Vec<Extent>,
    pending: PendingChunks,
}

impl<'a> BlockFeeder<'a> {
    fn new(store: &'a ChunkStore, opts: IngestOptions) -> Self {
        let chunk_logical = opts.chunk_size.clamp(1024, pigeonhole_codec::MAX_LOGICAL_CHUNK);
        let block_size = opts.block_size.max(1024).min(chunk_logical);
        Self {
            store,
            chunk_logical: chunk_logical as i64,
            block_size,
            codec: opts.codec,
            calls: opts.compress_calls.unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
            budget: opts.memory_budget,
            block: Vec::with_capacity(block_size),
            permit: None,
            open: OpenChunk::new(store.replicated.chunk_writer()),
            extents: Vec::new(),
            pending: PendingChunks::default(),
        }
    }

    async fn feed(&mut self, mut rest: &[u8]) -> Result<()> {
        while !rest.is_empty() {
            if self.block.is_empty() {
                self.reserve_block().await?;
            }
            let take = (self.block_size - self.block.len()).min(rest.len());
            self.block.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.block.len() >= self.block_size {
                self.push_block().await?;
            }
        }
        Ok(())
    }

    /// Memory for the next block. Under pressure the open chunk is sealed
    /// first, so its parts can upload and free theirs.
    async fn reserve_block(&mut self) -> Result<()> {
        let Some(budget) = self.budget.clone() else { return Ok(()) };
        if budget.available_permits() < self.block_size && self.open.logical > 0 {
            self.seal().await?;
        }
        self.permit = Some(budget.acquire(self.block_size).await?);
        Ok(())
    }

    async fn push_block(&mut self) -> Result<()> {
        self.store
            .encode_push_block(&mut self.open, &mut self.block, &mut self.permit, self.codec, &self.calls)
            .await?;
        if self.open.logical >= self.chunk_logical {
            self.seal().await?;
        }
        Ok(())
    }

    async fn seal(&mut self) -> Result<()> {
        let extent = self.store.seal_open_chunk(&mut self.open, &mut self.pending).await?;
        self.extents.push(extent);
        Ok(())
    }

    /// The last block and chunk: every extent, and the chunks to commit.
    async fn finish(mut self) -> Result<(Vec<Extent>, PendingChunks)> {
        if !self.block.is_empty() {
            self.push_block().await?;
        }
        if self.open.logical > 0 || !self.open.blocks.is_empty() {
            self.seal().await?;
        }
        Ok((self.extents, self.pending))
    }
}

struct OpenChunk {
    writer: pigeonhole_blob::ChunkReplicaWriter,
    blocks: Vec<StoredBlock>,
    /// The blocks' stored bytes, kept for L2 when it is written through.
    stored: Vec<Bytes>,
    logical: i64,
}

impl OpenChunk {
    fn new(writer: pigeonhole_blob::ChunkReplicaWriter) -> Self {
        Self {
            writer,
            blocks: Vec::new(),
            stored: Vec::new(),
            logical: 0,
        }
    }
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

/// Test helper: open a memory-backed layer over a local log in `log_dir`.
#[cfg(test)]
pub async fn open_memory_chunk_store(log_dir: impl AsRef<std::path::Path>) -> Result<ChunkStore> {
    use pigeonhole_codec::DEFAULT_BLOCK_SIZE;
    use pigeonhole_storage_memory::MemoryBlobStore;
    let db = BlobDb::open(pigeonhole_testkit::tablet(log_dir, BlobDb::register).await).await?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_ingests_do_not_collide_sort_keys() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let layer = Arc::new(open_memory_chunk_store(&log_dir).await.unwrap());
        let mut joins = Vec::new();
        for i in 0..32u8 {
            let layer = layer.clone();
            joins.push(tokio::spawn(async move {
                let body = Bytes::from(vec![i; 256 * 1024]);
                layer
                    .ingest_and_commit(
                        stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]),
                        None,
                    )
                    .await
                    .map(|ing| (ing, body))
            }));
        }
        for j in joins {
            let (ing, body) = j.await.unwrap().expect("concurrent ingest");
            let got = layer.read(&ing.extents, None).await.unwrap();
            assert_eq!(got, body);
        }
    }

    #[tokio::test]
    async fn an_ingest_commits_its_chunks_in_one_entry_or_not_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let layer = open_memory_chunk_store(dir.path().join("blob.log")).await.unwrap();
        let body = Bytes::from(vec![3u8; 200_000]);
        let before = layer.meta().applied_position();

        // Uploaded, not committed: nothing in the state, nothing readable.
        let ing = layer.ingest(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None).await.unwrap();
        assert!(ing.chunks.len() >= 3, "several chunks: {:?}", ing.chunks);
        assert_eq!(layer.meta().applied_position(), before);
        assert!(layer.read(&ing.extents, None).await.is_err());

        layer.commit(ing.chunks).await.unwrap();
        assert_eq!(layer.meta().applied_position(), before + 1, "every chunk in one entry");
        assert_eq!(layer.read(&ing.extents, None).await.unwrap(), body);

        // Dropped uncommitted: the chunk ids are never known to the state.
        let ing = layer.ingest(stream::iter(vec![Ok::<_, anyhow::Error>(body)]), None).await.unwrap();
        let id = ing.extents[0].chunk;
        drop(ing);
        assert!(layer.db().chunk_meta(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ingest_read_retain_release_root() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let layer = open_memory_chunk_store(&log_dir).await.unwrap();

        let body = Bytes::from(vec![7u8; 200_000]);
        let ingested = layer
            .ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None)
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
    }

    #[tokio::test]
    async fn put_200mib_two_limits_range_per_replica() {
        const MIB: usize = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();

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
        let ingested = layer.ingest_and_commit(Box::pin(body_stream), None).await.unwrap();
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
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
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
            .ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None)
            .await
            .unwrap();
        flag.store(true, Ordering::Relaxed);
        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got, data);
    }

    /// Written through, L2 holds every block: reads need no backend. Without
    /// it, the same read goes to the (now unavailable) backend.
    #[tokio::test]
    async fn written_through_blocks_read_from_l2() {
        const MIB: usize = 1024 * 1024;
        for write_through in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = BlobDb::open(pigeonhole_testkit::tablet(&dir.path().join("blob.log"), BlobDb::register).await).await.unwrap();
            let store = MemoryBlobStore::with_limits(BackendLimits::memory()).with_unavailable_flag();
            let down = store.unavailable_flag();
            let mut opts = IngestOptions::new(8 * MIB, ChunkCodec::Zstd);
            opts.block_size = MIB;
            let l2 = Arc::new(BlockCache::new("l2", 64 * MIB));
            let layer = ChunkStore::open(db, store, opts).await.unwrap().with_caches(None, Some(l2), write_through);
            let data: Bytes = (0..3 * MIB + 5).map(|i| (i / 1000) as u8).collect::<Vec<_>>().into();
            let ingested = layer
                .ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None)
                .await
                .unwrap();
            down.store(true, Ordering::Relaxed);
            let got = layer.read(&ingested.extents, None).await;
            if write_through {
                assert_eq!(got.unwrap(), data);
            } else {
                assert!(got.is_err(), "nothing cached, the backend is down");
            }
        }
    }

    /// Object larger than the ingest budget must still complete (budget covers
    /// only the open chunk + block, not the whole object).
    #[tokio::test]
    async fn large_put_under_small_budget_completes() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let budget = ByteBudget::new(32 * 1024 * 1024);
        let mut opts = IngestOptions::new(8 * 1024 * 1024, ChunkCodec::Zstd);
        opts.block_size = 1024 * 1024;
        opts.memory_budget = Some(budget.clone());
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
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
            .ingest_and_commit(Box::pin(body), None)
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
                let log_dir = dir.path().join(format!("blob-{i}.log"));
                let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
                opts.block_size = 512 * 1024;
                opts.memory_budget = Some(budget);
                let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
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
                layer.ingest_and_commit(Box::pin(body), None).await
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
                let log_dir = dir.path().join(format!("blob-{i}.log"));
                let mut opts = IngestOptions::new(2 * 1024 * 1024, ChunkCodec::Raw);
                opts.block_size = frame;
                opts.memory_budget = Some(budget);
                let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
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
                layer.ingest_and_commit(Box::pin(body), None).await
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
        let log_dir = dir.path().join("blob.log");
        let mut opts = IngestOptions::new(4 * 1024 * 1024, ChunkCodec::Zstd);
        opts.block_size = 512 * 1024;
        opts.memory_budget = Some(budget.clone());
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
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
            layer_c.ingest_and_commit(Box::pin(body), None).await
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
