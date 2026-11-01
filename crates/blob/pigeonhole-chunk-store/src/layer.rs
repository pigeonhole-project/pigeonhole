//! Gateway-facing blob layer API (stage E: chunks on parts via Replicated).
//!
//! Gateways see only [`ChunkId`] / [`Extent`] — not locators or instances.

use crate::blob_db::{BlobDb, StoredBlock};
use crate::block_cache::BlockCache;
use crate::commands::ChunkState;
use crate::ingest::IngestOptions;
use crate::repair::spawn_enqueue_repair;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use pigeonhole_blob::{
    collect_stream, erase, BlobBackend, CheapestFirst, EncodedBlock, ReplicaLayout, Replicated, SharedBackend,
};
use pigeonhole_codec::{decode_block_slice, encode_block_bytes, ByteBudget, ChunkCodec, DEFAULT_CHUNK_SIZE};
use pigeonhole_meta::{Outcomes, Prepared, Proposal, Slot};
use pigeonhole_types::ByteRange;
use std::collections::{BTreeMap, VecDeque};
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

/// Mark every writable member as `owner`'s and refuse one marked for
/// another deployment: two deployments on one store by mistake. (Their
/// blobs would be safe from each other's sweep, but not their budgets.)
async fn check_owner(replicated: &Replicated, owner: pigeonhole_blob::Owner) -> Result<()> {
    for m in replicated.members() {
        let info = m.instance();
        if info.role != pigeonhole_blob::InstanceRole::ReadWrite {
            continue;
        }
        let marked = m.mark_owner(owner).await.with_context(|| format!("mark instance {} as ours", info.id))?;
        if marked != owner {
            bail!(
                "instance {} ({}) belongs to deployment {marked}, this one is {owner}: give each deployment stores of its own",
                info.id,
                info.location
            );
        }
    }
    Ok(())
}

impl ChunkStore {
    /// Open with a single backend (write quorum 1).
    pub async fn open<B: BlobBackend>(db: BlobDb, backend: B, opts: IngestOptions) -> Result<Self> {
        let backend: SharedBackend = Arc::new(erase(backend));
        let info = backend.instance().clone();
        db.sync_instances(&[info]).await?;
        let replicated = Replicated::new(vec![backend], 1, Arc::new(CheapestFirst::new()))?;
        Self::open_replicated(db, Arc::new(replicated), opts).await
    }

    /// Open with an existing placement group.
    pub async fn open_replicated(db: BlobDb, replicated: Arc<Replicated>, opts: IngestOptions) -> Result<Self> {
        let infos: Vec<_> = replicated.members().iter().map(|m| m.instance().clone()).collect();
        db.sync_instances(&infos).await?;
        check_block_fits_members(opts.block_size, replicated.as_ref())?;
        for m in replicated.members() {
            let max = m.limits().max_blob_size;
            if opts.chunk_size > max {
                tracing::warn!(
                    instance = %m.instance().id,
                    chunk_size = opts.chunk_size,
                    max_blob_size = max,
                    "chunk.size is over this store's message size: a chunk takes several messages there"
                );
            }
        }
        let owner = db.owner().await?;
        replicated.pins().set_owner(owner);
        check_owner(replicated.as_ref(), owner).await?;
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
        w.push(EncodedBlock { stored: data, logical_len: logical as u32, codec: "raw".into() }).await?;
        let sealed = w.finish().await?;
        let blocks = vec![StoredBlock {
            block_no: 0,
            logical_off: 0,
            logical_len: logical,
            stored_len: logical,
            codec: "raw".into(),
            crc32: crc,
        }];
        let chunk_id = self.db.commit_chunk(logical, crc, &blocks, &sealed.layouts).await?;
        self.enqueue_missing_replicas(chunk_id, &sealed.layouts).await?;
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
        let opts = opts.unwrap_or_else(|| self.ingest_options());
        self.ingest_blocks(&mut stream, opts).await
    }

    /// The options the store was opened with (`[chunk]`, `[ingest]`), for a
    /// caller to add what it knows of a body, such as its length.
    pub fn ingest_options(&self) -> IngestOptions {
        let mut o = IngestOptions::new(self.chunk_size, self.codec);
        o.block_size = self.block_size;
        o.memory_budget = self.memory_budget.clone();
        o
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
    pub async fn read(&self, extents: &[Extent], range: Option<ByteRange>) -> Result<Bytes> {
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
            let meta = self.db.chunk_meta(ext.chunk).await?.ok_or(crate::ChunkGone { chunk_id: ext.chunk })?;
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
        self.read_chunk_range_with(chunk_id, from, to, replicas).await
    }

    async fn read_chunk_range(&self, chunk_id: ChunkId, from: usize, to: usize) -> Result<Bytes> {
        let replicas = self.db.get_replica_layouts(chunk_id).await?;
        if replicas.is_empty() {
            bail!("no parts for chunk {chunk_id}");
        }
        self.read_chunk_range_with(chunk_id, from, to, &replicas).await
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
                self.replicated.read_with_hook(replicas, 0..1, hook).await.context("replicated read raw")?,
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
            let decoded = self.load_decoded_block(chunk_id, b, replicas, first_no..last_no).await?;
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
                    let stored = self.load_compressed_block(chunk_id, block, replicas, fetch_range.clone()).await?;
                    decode_block_slice(stored.as_ref(), &block.codec, block.logical_len.max(1) as usize)
                })
                .await;
        }
        let stored = self.load_compressed_block(chunk_id, block, replicas, fetch_range).await?;
        decode_block_slice(stored.as_ref(), &block.codec, block.logical_len.max(1) as usize)
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
                    bail!("block {} stored len {} < expected {}", block_no, raw.len(), block.stored_len);
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
            let Ok(stream) =
                self.replicated.read_with_hook(std::slice::from_ref(r), block_no..block_no + 1, None).await
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

    fn not_found_hook(&self, chunk_id: ChunkId) -> Option<pigeonhole_blob::NotFoundHook> {
        let db = self.db.clone();
        Some(Arc::new(move |instance_id: &str| {
            spawn_enqueue_repair(db.clone(), chunk_id, instance_id.to_string());
        }))
    }

    /// After a quorum write, enqueue backfill for placement members that missed.
    async fn enqueue_missing_replicas(&self, chunk_id: ChunkId, layouts: &[ReplicaLayout]) -> Result<()> {
        let have: std::collections::HashSet<&str> = layouts.iter().map(|l| l.instance.as_str()).collect();
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

    /// Compress one block (never larger than it was: a block that does not
    /// shrink is stored raw).
    async fn encode_block(
        &self,
        block_buf: &mut Vec<u8>,
        codec: ChunkCodec,
        calls: &Arc<AtomicU64>,
    ) -> Result<EncodedBlock> {
        let logical = std::mem::take(block_buf);
        let logical_len = logical.len();
        let calls = calls.clone();
        let (stored, codec) = pigeonhole_codec::blocking(move || {
            calls.fetch_add(1, Ordering::Relaxed);
            encode_block_bytes(&logical, codec)
        })
        .await
        .context("encode")??;
        pigeonhole_blob::record_compression_ratio(stored.len(), logical_len);
        Ok(EncodedBlock { stored, logical_len: logical_len as u32, codec })
    }

    async fn push_encoded(&self, open: &mut OpenChunk, block: EncodedBlock) -> Result<()> {
        let EncodedBlock { stored, logical_len, codec } = block;
        let block_no = open.blocks.len() as i64;
        let logical_off = open.logical;
        open.writer
            .push(EncodedBlock { stored: stored.clone(), logical_len, codec: codec.clone() })
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
            codec,
            crc32: crc32fast::hash(&stored),
        });
        open.logical += logical_len as i64;
        open.stored_len += stored.len();
        Ok(())
    }

    /// Seal the open chunk: its upload finishes in the background (the
    /// writer's last part goes out now), and a new chunk opens.
    /// Its reservation shrinks to the stored bytes and is returned when the
    /// upload ends.
    fn start_seal(&self, open: &mut OpenChunk) -> Sealing {
        let writer = std::mem::replace(&mut open.writer, self.replicated.chunk_writer());
        let mut permit = open.permit.take();
        if let Some(p) = permit.as_mut() {
            p.release_excess(p.bytes().saturating_sub(open.stored_len));
        }
        let upload = AbortOnDrop(Some(tokio::spawn(async move {
            let sealed = writer.finish().await;
            drop(permit);
            sealed
        })));
        let sealing = Sealing {
            upload,
            blocks: std::mem::take(&mut open.blocks),
            stored: std::mem::take(&mut open.stored),
            logical: open.logical,
        };
        open.logical = 0;
        open.stored_len = 0;
        sealing
    }

    /// Wait for a sealed chunk's upload; the chunk joins `pending`.
    async fn complete_seal(&self, sealing: Sealing, pending: &mut PendingChunks) -> Result<Extent> {
        let Sealing { upload, blocks, stored, logical } = sealing;
        let sealed = upload.join().await?.context("replicated finish chunk")?;
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
        Ok(Extent { chunk: chunk_id, offset: 0, len: logical })
    }
}

/// Sealed chunks uploading in the background while the body is read on:
/// a server that stopped reading for a whole chunk's upload lost clients
/// whose send timeout is shorter (boto3's is its connect timeout, 10 s in
/// our bench), and uploaded one chunk at a time.
const SEALING_IN_FLIGHT: usize = 2;

/// A sealed chunk whose upload may still run.
struct Sealing {
    upload: AbortOnDrop<Result<pigeonhole_blob::SealedChunk>>,
    blocks: Vec<StoredBlock>,
    stored: Vec<Bytes>,
    logical: i64,
}

/// A task aborted when its handle drops: an ingest given up (an error, a
/// client gone) stops its uploads; what went up is an orphan the sweeper
/// collects (it was never pinned past the upload).
struct AbortOnDrop<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> AbortOnDrop<T> {
    async fn join(mut self) -> Result<T> {
        let handle = self.0.take().expect("joined once");
        handle.await.context("chunk upload task")
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

/// Cuts a stream into blocks and the blocks into chunks as the bytes come,
/// within the memory budget. A chunk is closed by its stored bytes, so a
/// compressible stream fills each message: messages are the scarce resource.
///
/// The budget is taken a chunk at a time: a chunk opens with a reservation
/// for all it can hold (its stored bytes and the block being filled), so it
/// never waits for memory half full and is never cut short for want of it.
/// A feeder waits only holding chunks that are uploading, which return
/// theirs without needing more: no deadlock. Outside the budget: the encoded
/// block that opens a chunk while its reservation is awaited.
struct BlockFeeder<'a> {
    store: &'a ChunkStore,
    /// Most stored (encoded) bytes in a chunk: `chunk.size`.
    chunk_stored: usize,
    /// Most logical bytes in a chunk, however well they compress.
    chunk_logical: i64,
    block_size: usize,
    codec: ChunkCodec,
    calls: Arc<AtomicU64>,
    budget: Option<ByteBudget>,
    size_hint: Option<u64>,
    /// Logical bytes in chunks already sealed.
    sealed_logical: u64,
    /// The block being filled.
    block: Vec<u8>,
    open: OpenChunk,
    /// Sealed, uploading, in the order they were sealed.
    sealing: VecDeque<Sealing>,
    extents: Vec<Extent>,
    pending: PendingChunks,
}

impl<'a> BlockFeeder<'a> {
    fn new(store: &'a ChunkStore, opts: IngestOptions) -> Self {
        let chunk_stored = opts.chunk_size.clamp(1024, pigeonhole_codec::MAX_LOGICAL_CHUNK);
        let block_size = opts.block_size.max(1024).min(chunk_stored);
        Self {
            store,
            chunk_stored,
            chunk_logical: pigeonhole_codec::MAX_LOGICAL_CHUNK as i64,
            block_size,
            codec: opts.codec,
            calls: opts.compress_calls.unwrap_or_else(|| Arc::new(AtomicU64::new(0))),
            budget: opts.memory_budget,
            size_hint: opts.size_hint,
            sealed_logical: 0,
            block: Vec::with_capacity(block_size),
            open: OpenChunk::new(store.replicated.chunk_writer()),
            sealing: VecDeque::new(),
            extents: Vec::new(),
            pending: PendingChunks::default(),
        }
    }

    async fn feed(&mut self, mut rest: &[u8]) -> Result<()> {
        while !rest.is_empty() {
            self.reserve_chunk().await?;
            let take = (self.block_size - self.block.len()).min(rest.len());
            self.block.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.block.len() >= self.block_size {
                self.push_block().await?;
            }
        }
        Ok(())
    }

    /// The open chunk's reservation, unless it has one: all the chunk can
    /// hold, or what is left of a body of known length.
    async fn reserve_chunk(&mut self) -> Result<()> {
        let Some(budget) = &self.budget else { return Ok(()) };
        if self.open.permit.is_some() {
            return Ok(());
        }
        let full = (self.chunk_stored + self.block_size) as u64;
        let left = self.size_hint.map_or(full, |n| n.saturating_sub(self.sealed_logical));
        let n = full.min(left).min(budget.capacity() as u64) as usize;
        self.open.permit = Some(budget.acquire(n).await?);
        Ok(())
    }

    async fn push_block(&mut self) -> Result<()> {
        let block = self.store.encode_block(&mut self.block, self.codec, &self.calls).await?;
        if !self.open.blocks.is_empty() && self.open.stored_len + block.stored.len() > self.chunk_stored {
            self.seal().await?;
            self.reserve_chunk().await?;
        }
        self.store.push_encoded(&mut self.open, block).await?;
        if self.open.stored_len >= self.chunk_stored || self.open.logical >= self.chunk_logical {
            self.seal().await?;
        }
        Ok(())
    }

    /// Seal the open chunk, its upload in the background; with too many
    /// uploading, wait for the oldest first.
    async fn seal(&mut self) -> Result<()> {
        self.sealed_logical += self.open.logical as u64;
        let sealing = self.store.start_seal(&mut self.open);
        self.sealing.push_back(sealing);
        while self.sealing.len() > SEALING_IN_FLIGHT {
            self.complete_oldest().await?;
        }
        Ok(())
    }

    /// The oldest sealed chunk uploaded: its extent, in order.
    async fn complete_oldest(&mut self) -> Result<()> {
        let sealing = self.sealing.pop_front().expect("a chunk sealing");
        let extent = self.store.complete_seal(sealing, &mut self.pending).await?;
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
        while !self.sealing.is_empty() {
            self.complete_oldest().await?;
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
    stored_len: usize,
    /// Its share of the ingest budget (see [`BlockFeeder`]).
    permit: Option<pigeonhole_codec::BudgetPermit>,
}

impl OpenChunk {
    fn new(writer: pigeonhole_blob::ChunkReplicaWriter) -> Self {
        Self { writer, blocks: Vec::new(), stored: Vec::new(), logical: 0, stored_len: 0, permit: None }
    }
}

/// E.4: block_size (+ margin for incompressible / header) must fit every member.
pub fn check_block_fits_members(block_size: usize, replicated: &Replicated) -> Result<()> {
    let min_max = replicated.max_blob_size();
    let need = block_size.saturating_add(pigeonhole_codec::COMPRESS_SIZE_MARGIN.min(64 * 1024));
    // Incompressible path stores raw `block_size` bytes; require block_size < min max.
    if block_size >= min_max {
        bail!("chunk.block_size ({block_size}) must be < min(max_blob_size) of placement group ({min_max})");
    }
    if need >= min_max && block_size + 64 >= min_max {
        bail!("chunk.block_size ({block_size}) with margin exceeds min(max_blob_size) ({min_max})");
    }
    let _ = need;
    Ok(())
}

/// Build default ingest options for the layer.
pub fn default_layer_opts() -> IngestOptions {
    IngestOptions::new(DEFAULT_CHUNK_SIZE, ChunkCodec::Zstd)
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

    /// Bytes zstd cannot shrink (xorshift).
    fn noise(n: usize) -> Bytes {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        };
        Bytes::from((0..n).map(|_| next()).collect::<Vec<u8>>())
    }

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
                    .ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None)
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
        // Noise: a body that compresses would fit one chunk.
        let body = noise(200_000);
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
        let ingested =
            layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(body.clone())]), None).await.unwrap();
        assert_eq!(ingested.size, body.len() as i64);
        assert!(!ingested.extents.is_empty());

        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got, body);

        let mid = body.len() as u64 / 2;
        let slice = layer.read(&ingested.extents, Some(mid..mid + 10)).await.unwrap();
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
                vec![Arc::new(erase(a)) as SharedBackend, Arc::new(erase(b)) as SharedBackend],
                2,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        let mut opts = IngestOptions::new(64 * MIB, ChunkCodec::Raw);
        opts.block_size = 6 * MIB;
        let layer = ChunkStore::open_replicated(db, rep.clone(), opts).await.unwrap();

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
        let slice = layer.read(&ingested.extents, Some(cross..cross + 200)).await.unwrap();
        assert_eq!(slice.as_ref(), &expect[cross as usize..cross as usize + 200]);

        // Range across a part boundary inside first chunk (6 MiB blocks; 10 MiB → 1 block/part).
        let part_cross = 6 * MIB as u64 - 50;
        let slice = layer.read(&ingested.extents, Some(part_cross..part_cross + 100)).await.unwrap();
        assert_eq!(slice.as_ref(), &expect[part_cross as usize..part_cross as usize + 100]);

        // Each replica alone.
        for ext in &ingested.extents {
            let layouts = layer.db().get_replica_layouts(ext.chunk).await.unwrap();
            assert_eq!(layouts.len(), 2);
            for layout in &layouts {
                let got = layer
                    .read_from_replicas(ext.chunk, 0, ext.len as usize, std::slice::from_ref(layout))
                    .await
                    .unwrap();
                let start =
                    ingested.extents.iter().take_while(|e| e.chunk != ext.chunk).map(|e| e.len as usize).sum::<usize>();
                assert_eq!(got.as_ref(), &expect[start..start + ext.len as usize], "replica {}", layout.instance);
            }
        }
    }

    #[tokio::test]
    async fn lose_one_instance_still_reads() {
        const MIB: usize = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("blob.log");
        let db = BlobDb::open(pigeonhole_testkit::tablet(&log_dir, BlobDb::register).await).await.unwrap();
        let a = MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("a").with_unavailable_flag();
        let flag = a.unavailable_flag();
        let b = MemoryBlobStore::with_limits(BackendLimits::memory()).with_instance_id("b");
        let rep = Arc::new(
            Replicated::new(
                vec![Arc::new(erase(a)) as SharedBackend, Arc::new(erase(b)) as SharedBackend],
                2,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        let mut opts = IngestOptions::new(8 * MIB, ChunkCodec::Raw);
        opts.block_size = MIB;
        let layer = ChunkStore::open_replicated(db, rep, opts).await.unwrap();
        let data = Bytes::from(vec![9u8; 3 * MIB]);
        let ingested =
            layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None).await.unwrap();
        flag.store(true, Ordering::Relaxed);
        let got = layer.read(&ingested.extents, None).await.unwrap();
        assert_eq!(got, data);
    }

    /// `chunk.size` caps a chunk's stored bytes, not its logical ones: a
    /// body that compresses goes up in as few messages as its stored size
    /// needs, one that does not is cut at `chunk.size` on the wire.
    #[tokio::test]
    async fn chunks_are_cut_by_stored_bytes() {
        const MIB: usize = 1024 * 1024;
        async fn parts_per_chunk(layer: &ChunkStore, data: &Bytes) -> Vec<(usize, i64)> {
            let ingested =
                layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None).await.unwrap();
            assert_eq!(layer.read(&ingested.extents, None).await.unwrap(), *data);
            let mut out = Vec::new();
            for ext in &ingested.extents {
                let layouts = layer.db().get_replica_layouts(ext.chunk).await.unwrap();
                let stored: u64 = layouts[0].parts.iter().flat_map(|p| &p.block_stored_lens).map(|&l| l as u64).sum();
                assert!(stored <= MIB as u64, "chunk stored {stored} over chunk.size");
                out.push((layouts[0].parts.len(), ext.len));
            }
            out
        }
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let mut opts = IngestOptions::new(MIB, ChunkCodec::Zstd);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap();

        // 8 MiB that compress far below 1 MiB: one chunk, one message.
        let text = Bytes::from((0..8 * MIB).map(|i| b"pigeonhole "[i % 11]).collect::<Vec<u8>>());
        assert_eq!(parts_per_chunk(&layer, &text).await, vec![(1, 8 * MIB as i64)]);

        // 3 MiB of noise stays raw: three full messages, each its own chunk.
        let noise = noise(3 * MIB);
        let chunks = parts_per_chunk(&layer, &noise).await;
        assert!(chunks.iter().all(|&(parts, _)| parts == 1), "{chunks:?}");
        assert_eq!(chunks.len(), 3, "{chunks:?}");
    }

    /// Written through, L2 holds every block: reads need no backend. Without
    /// it, the same read goes to the (now unavailable) backend.
    #[tokio::test]
    async fn written_through_blocks_read_from_l2() {
        const MIB: usize = 1024 * 1024;
        for write_through in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = BlobDb::open(pigeonhole_testkit::tablet(&dir.path().join("blob.log"), BlobDb::register).await)
                .await
                .unwrap();
            let store = MemoryBlobStore::with_limits(BackendLimits::memory()).with_unavailable_flag();
            let down = store.unavailable_flag();
            let mut opts = IngestOptions::new(8 * MIB, ChunkCodec::Zstd);
            opts.block_size = MIB;
            let l2 = Arc::new(BlockCache::new("l2", 64 * MIB));
            let layer = ChunkStore::open(db, store, opts).await.unwrap().with_caches(None, Some(l2), write_through);
            let data: Bytes = (0..3 * MIB + 5).map(|i| (i / 1000) as u8).collect::<Vec<_>>().into();
            let ingested =
                layer.ingest_and_commit(stream::iter(vec![Ok::<_, anyhow::Error>(data.clone())]), None).await.unwrap();
            down.store(true, Ordering::Relaxed);
            let got = layer.read(&ingested.extents, None).await;
            if write_through {
                assert_eq!(got.unwrap(), data);
            } else {
                assert!(got.is_err(), "nothing cached, the backend is down");
            }
        }
    }

    /// A blob store whose puts wait for the test to open them.
    struct Gated {
        inner: MemoryBlobStore,
        gate: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl BlobBackend for Gated {
        type Id = <MemoryBlobStore as BlobBackend>::Id;
        type Key = <MemoryBlobStore as BlobBackend>::Key;
        fn instance(&self) -> &pigeonhole_blob::InstanceInfo {
            self.inner.instance()
        }
        fn limits(&self) -> &BackendLimits {
            self.inner.limits()
        }
        fn key(id: &Self::Id) -> Self::Key {
            MemoryBlobStore::key(id)
        }
        fn cost(&self, op: pigeonhole_blob::OpKind, id: Option<&Self::Id>) -> pigeonhole_blob::CostHint {
            self.inner.cost(op, id)
        }
        async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
            self.gate.acquire().await?.forget();
            self.inner.put(data, name).await
        }
        async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<pigeonhole_blob::BoxByteStream> {
            self.inner.get(id, range).await
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            self.inner.delete(keys).await
        }
        async fn candidates(
            &self,
            owner: pigeonhole_blob::Owner,
            after: Option<Self::Key>,
            upto: Self::Key,
            limit: usize,
        ) -> Result<Vec<Self::Key>> {
            self.inner.candidates(owner, after, upto, limit).await
        }
    }

    /// A blob store marked as another deployment's (a channel's pinned marker).
    struct Marked(MemoryBlobStore, pigeonhole_blob::Owner);

    #[async_trait::async_trait]
    impl BlobBackend for Marked {
        type Id = <MemoryBlobStore as BlobBackend>::Id;
        type Key = <MemoryBlobStore as BlobBackend>::Key;
        fn instance(&self) -> &pigeonhole_blob::InstanceInfo {
            self.0.instance()
        }
        fn limits(&self) -> &BackendLimits {
            self.0.limits()
        }
        fn key(id: &Self::Id) -> Self::Key {
            MemoryBlobStore::key(id)
        }
        fn cost(&self, op: pigeonhole_blob::OpKind, id: Option<&Self::Id>) -> pigeonhole_blob::CostHint {
            self.0.cost(op, id)
        }
        async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
            self.0.put(data, name).await
        }
        async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<pigeonhole_blob::BoxByteStream> {
            self.0.get(id, range).await
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            self.0.delete(keys).await
        }
        async fn candidates(
            &self,
            owner: pigeonhole_blob::Owner,
            after: Option<Self::Key>,
            upto: Self::Key,
            limit: usize,
        ) -> Result<Vec<Self::Key>> {
            self.0.candidates(owner, after, upto, limit).await
        }
        async fn mark_owner(&self, _owner: pigeonhole_blob::Owner) -> Result<pigeonhole_blob::Owner> {
            Ok(self.1)
        }
    }

    /// A store another deployment marked is refused at open; the tablet's
    /// own owner is kept across a reopen.
    #[tokio::test]
    async fn a_store_marked_for_another_deployment_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let ours = db.owner().await.unwrap();
        assert_eq!(db.owner().await.unwrap(), ours, "chosen once");
        let opts = || IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        assert!(ChunkStore::open(db.clone(), Marked(MemoryBlobStore::new(), ours), opts()).await.is_ok());
        let theirs = pigeonhole_blob::Owner(ours.0 ^ 1);
        let err = ChunkStore::open(db, Marked(MemoryBlobStore::new(), theirs), opts()).await.err().unwrap();
        assert!(format!("{err:#}").contains(&format!("belongs to deployment {theirs}")), "{err:#}");
    }

    /// The body is read on while sealed chunks upload, up to
    /// SEALING_IN_FLIGHT of them; the extents keep the body's order.
    #[tokio::test]
    async fn the_body_is_read_on_while_chunks_upload() {
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer =
            ChunkStore::open(db, Gated { inner: MemoryBlobStore::new(), gate: gate.clone() }, opts).await.unwrap();
        let chunks = 6usize;
        let pieces: Vec<Bytes> = (0..chunks).map(|i| Bytes::from(vec![i as u8; 64 * 1024])).collect();
        let read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = {
            let (pieces, read) = (pieces.clone(), read.clone());
            stream::iter(pieces).map(move |p| {
                read.fetch_add(1, Ordering::SeqCst);
                Ok::<_, anyhow::Error>(p)
            })
        };
        let layer = Arc::new(layer);
        let ingest = {
            let layer = layer.clone();
            tokio::spawn(async move { layer.ingest_and_commit(body, None).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        // With no upload done: SEALING_IN_FLIGHT chunks sealed and uploading,
        // one more sealed waits for the oldest. Before, reading stopped at
        // the first chunk's upload.
        assert_eq!(read.load(Ordering::SeqCst), SEALING_IN_FLIGHT + 1, "read on past the first chunk");
        assert!(!ingest.is_finished());
        gate.add_permits(1000);
        let ingested = ingest.await.unwrap().unwrap();
        assert_eq!(read.load(Ordering::SeqCst), chunks);
        let all: Vec<u8> = pieces.iter().flat_map(|p| p.iter().copied()).collect();
        assert_eq!(layer.read(&ingested.extents, None).await.unwrap(), Bytes::from(all), "in the body's order");
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
        let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap();
        let n = 300 * 1024 * 1024;
        let piece = 4 * 1024 * 1024;
        let body = futures::stream::unfold(0usize, move |off| async move {
            if off >= n {
                return None;
            }
            let len = (n - off).min(piece);
            Some((Ok::<_, anyhow::Error>(Bytes::from(vec![0u8; len])), off + len))
        });
        let r = layer.ingest_and_commit(Box::pin(body), None).await.expect("300 MiB ingest under 32 MiB budget");
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
                let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap();
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
            j.await.expect("join").expect("parallel 64 MiB ingest under 64 MiB shared budget");
        }
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// 64 parallel 8 MiB PUTs sharing a budget smaller than one chunk's
    /// reservation must finish: a chunk takes it whole, so none waits holding a part.
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
                let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap();
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
                j.await.expect("join").expect("parallel 8 MiB ingest under 4*block_size budget");
            }
        })
        .await;
        assert!(result.is_ok(), "64 parallel 8 MiB PUTs under 4*block_size budget timed out");
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// A tight shared budget makes PUTs wait for it, not cut their chunks
    /// short: every chunk but a body's last fills `chunk.size`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_tight_budget_does_not_underfill_chunks() {
        const KIB: usize = 1024;
        // One chunk's reservation: before, the PUTs' blocks used it up and
        // each cut its chunk short to "free" memory it did not hold.
        let budget = ByteBudget::new(1024 * KIB + 256 * KIB);
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let mut opts = IngestOptions::new(1024 * KIB, ChunkCodec::Zstd);
        opts.block_size = 256 * KIB;
        opts.memory_budget = Some(budget.clone());
        let layer = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
        let mut joins = Vec::new();
        for _ in 0..16 {
            let layer = layer.clone();
            let body = noise(2 * 1024 * KIB + 100 * KIB);
            joins.push(tokio::spawn(async move {
                let pieces: Vec<_> = body.chunks(64 * KIB).map(Bytes::copy_from_slice).collect();
                // A client's body comes in pieces with waits between them.
                let body = stream::iter(pieces).then(|p| async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    Ok::<_, anyhow::Error>(p)
                });
                layer.ingest_and_commit(Box::pin(body), None).await.unwrap()
            }));
        }
        for j in joins {
            let ingested = j.await.unwrap();
            let (last, full) = ingested.extents.split_last().unwrap();
            for ext in full {
                let layouts = layer.db().get_replica_layouts(ext.chunk).await.unwrap();
                let stored: u32 = layouts[0].parts.iter().flat_map(|p| &p.block_stored_lens).sum();
                assert_eq!(stored as usize, 1024 * KIB, "chunk {} cut short", ext.chunk);
            }
            assert_eq!(last.len as usize, 100 * KIB);
        }
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// A body of known length reserves what it has, not a whole chunk.
    #[tokio::test]
    async fn a_size_hint_bounds_the_reservation() {
        const KIB: usize = 1024;
        let budget = ByteBudget::new(4 * 1024 * KIB);
        let dir = tempfile::tempdir().unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(dir.path().join("blob.log"), BlobDb::register).await)
            .await
            .unwrap();
        let mut opts = IngestOptions::new(1024 * KIB, ChunkCodec::Zstd);
        opts.block_size = 64 * KIB;
        opts.memory_budget = Some(budget.clone());
        let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts.clone()).await.unwrap();
        for (hint, held) in [(None, 1024 * KIB + 64 * KIB), (Some(200 * KIB as u64), 200 * KIB)] {
            let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let data = noise(200 * KIB);
            let body = {
                let (budget, seen) = (budget.clone(), seen.clone());
                stream::iter(data.chunks(50 * KIB).map(Bytes::copy_from_slice)).map(move |p| {
                    seen.fetch_max(budget.capacity() - budget.available_permits(), Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(p)
                })
            };
            let mut o = opts.clone();
            o.size_hint = hint;
            layer.ingest_and_commit(body, Some(o)).await.unwrap();
            assert_eq!(seen.load(Ordering::SeqCst), held, "hint {hint:?}");
            assert_eq!(budget.available_permits(), budget.capacity());
        }
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
        let layer = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
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
