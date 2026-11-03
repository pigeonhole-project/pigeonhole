//! Reads: an extent list's bytes as a stream.
//!
//! A read is planned when it opens: the chunks are pinned
//! ([`BlobDb::pin_reads`]) and checked not reclaiming, and the blocks it
//! needs are cut into **windows** — consecutive blocks of one chunk, at most
//! [`READ_WINDOW`] stored bytes. A window's blocks missing from both caches
//! are fetched in one [`Replicated::read`] (one GET per part it spans, with
//! failover), split by their stored lengths and checked against their CRCs;
//! then each block is decoded as the stream gets to it. So a read holds at
//! most one window of stored bytes and one decoded block, whatever its
//! length, and the pin lives as long as the stream.

use crate::blob_db::{BlobDb, ChunkId, Extent, ReadPin, StoredBlock};
use crate::block_cache::BlockCache;
use crate::repair::spawn_enqueue_repair;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::stream::{BoxStream, Stream, StreamExt};
use pigeonhole_blob::{collect_stream, ReplicaLayout, Replicated};
use pigeonhole_codec::decode_block_slice;
use pigeonhole_types::ByteRange;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

/// Most stored bytes one window fetches (a chunk of the default
/// `chunk.size` is one window).
pub const READ_WINDOW: usize = 32 * 1024 * 1024;

/// What a read needs of the store; cheap to clone, so a stream owns it.
#[derive(Clone)]
pub(crate) struct Reader {
    pub db: BlobDb,
    pub replicated: Arc<Replicated>,
    /// Decoded blocks.
    pub l1: Option<Arc<BlockCache>>,
    /// Stored blocks.
    pub l2: Option<Arc<BlockCache>>,
}

/// Consecutive blocks of one chunk, read together.
struct Window {
    chunk: ChunkId,
    blocks: Vec<StoredBlock>,
    layouts: Arc<[ReplicaLayout]>,
    /// The logical range of the chunk wanted.
    from: usize,
    to: usize,
}

/// A block of a loaded window.
enum Block {
    /// From L1.
    Decoded(Bytes),
    /// From L2 or the store: decoded when the stream gets to it.
    Stored(Bytes),
}

/// A window whose blocks are at hand.
struct Loaded {
    chunk: ChunkId,
    blocks: VecDeque<(StoredBlock, Block)>,
    from: usize,
    to: usize,
}

/// The bytes of a read, in order. Holds the read's pin: the chunks' parts
/// are not reclaimed while it lives. `Sync` (the S3 body needs it).
pub struct ReadStream {
    /// Behind a mutex only to be `Sync`; never locked (`get_mut`).
    inner: Mutex<BoxStream<'static, Result<Bytes>>>,
    _pin: Option<ReadPin>,
}

impl Stream for ReadStream {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.get_mut().inner.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.as_mut().poll_next(cx)
    }
}

impl ReadStream {
    /// Every byte, collected.
    pub async fn collect(mut self) -> Result<Bytes> {
        let mut pieces = Vec::new();
        while let Some(piece) = self.next().await {
            pieces.push(piece?);
        }
        if pieces.len() == 1 {
            return Ok(pieces.pop().expect("one piece"));
        }
        Ok(Bytes::from(pieces.concat()))
    }
}

/// Where a stream is.
struct State {
    reader: Reader,
    windows: VecDeque<Window>,
    current: Option<Loaded>,
}

impl State {
    async fn next(&mut self) -> Option<Result<Bytes>> {
        loop {
            if let Some(cur) = &mut self.current {
                if let Some((block, data)) = cur.blocks.pop_front() {
                    match self.reader.emit(cur.chunk, &block, data, cur.from, cur.to).await {
                        Ok(bytes) if bytes.is_empty() => continue,
                        Ok(bytes) => return Some(Ok(bytes)),
                        Err(e) => return Some(Err(self.fail(e))),
                    }
                }
                self.current = None;
            }
            let window = self.windows.pop_front()?;
            match self.reader.load(window).await {
                Ok(loaded) => self.current = Some(loaded),
                Err(e) => return Some(Err(self.fail(e))),
            }
        }
    }

    /// Nothing more after an error.
    fn fail(&mut self, e: anyhow::Error) -> anyhow::Error {
        self.windows.clear();
        self.current = None;
        e
    }

    /// Load the first window now, so a store that cannot serve the read
    /// fails it before any byte goes out.
    async fn prime(&mut self) -> Result<()> {
        if let Some(window) = self.windows.pop_front() {
            self.current = Some(self.reader.load(window).await?);
        }
        Ok(())
    }

    fn into_stream(self, pin: Option<ReadPin>) -> ReadStream {
        let stream = futures::stream::unfold(self, |mut st| async move { st.next().await.map(|item| (item, st)) });
        ReadStream { inner: Mutex::new(stream.boxed()), _pin: pin }
    }
}

impl Reader {
    /// Open a read of `range` of `extents` (`None`: all of them).
    pub async fn open(&self, extents: &[Extent], range: Option<ByteRange>) -> Result<ReadStream> {
        let ids: Vec<ChunkId> = extents.iter().map(|e| e.chunk).collect();
        let pin = self.db.pin_reads(&ids);
        for &chunk_id in &ids {
            if !self.db.chunk_readable(chunk_id) {
                return Err(anyhow::Error::new(crate::ChunkGone { chunk_id }));
            }
        }
        let windows = self.plan(extents, range)?;
        let mut state = State { reader: self.clone(), windows, current: None };
        state.prime().await?;
        Ok(state.into_stream(Some(pin)))
    }

    /// `from..to` of `chunk_id` from `layouts` only, unpinned (tests: one
    /// replica at a time).
    pub async fn open_chunk_with(
        &self,
        chunk_id: ChunkId,
        from: usize,
        to: usize,
        layouts: &[ReplicaLayout],
    ) -> Result<ReadStream> {
        let mut windows = VecDeque::new();
        self.plan_chunk(chunk_id, from, to, layouts.into(), &mut windows)?;
        let mut state = State { reader: self.clone(), windows, current: None };
        state.prime().await?;
        Ok(state.into_stream(None))
    }

    /// The windows of a read: per extent, the part of `range` it covers.
    fn plan(&self, extents: &[Extent], range: Option<ByteRange>) -> Result<VecDeque<Window>> {
        let mut windows = VecDeque::new();
        let mut cursor = 0u64;
        for ext in extents {
            let size = self.db.chunk_meta_size(ext.chunk).ok_or(crate::ChunkGone { chunk_id: ext.chunk })?;
            if ext.len < 0 {
                bail!("extent len must be >= 0");
            }
            let ext_off = ext.offset.max(0);
            let len = ext.len.min(size.saturating_sub(ext_off)).max(0) as u64;
            let (start, end) = (cursor, cursor + len);
            cursor = end;
            let (from, to) = match &range {
                None => (0, len),
                Some(r) if end <= r.start || start >= r.end => continue,
                Some(r) => (r.start.saturating_sub(start), r.end.saturating_sub(start).min(len)),
            };
            if from >= to {
                continue;
            }
            let (from, to) = (ext_off as usize + from as usize, ext_off as usize + to as usize);
            let layouts: Arc<[ReplicaLayout]> = self.db.get_replica_layouts_now(ext.chunk).into();
            if layouts.is_empty() {
                bail!("no parts for chunk {}", ext.chunk);
            }
            self.plan_chunk(ext.chunk, from, to, layouts, &mut windows)?;
        }
        Ok(windows)
    }

    /// Cut the blocks of `from..to` of a chunk into windows.
    fn plan_chunk(
        &self,
        chunk: ChunkId,
        from: usize,
        to: usize,
        layouts: Arc<[ReplicaLayout]>,
        windows: &mut VecDeque<Window>,
    ) -> Result<()> {
        if from > to {
            bail!("invalid range {from}..{to}");
        }
        let blocks = self.db.blocks_overlapping(chunk, from, to);
        let mut window: Vec<StoredBlock> = Vec::new();
        let mut stored = 0usize;
        for b in blocks {
            let len = b.stored_len as usize;
            if !window.is_empty() && stored + len > READ_WINDOW {
                let blocks = std::mem::take(&mut window);
                windows.push_back(Window { chunk, blocks, layouts: layouts.clone(), from, to });
                stored = 0;
            }
            stored += len;
            window.push(b);
        }
        if !window.is_empty() {
            windows.push_back(Window { chunk, blocks: window, layouts, from, to });
        }
        Ok(())
    }

    /// A window's blocks: from L1, L2, or else the store, those in one read.
    async fn load(&self, w: Window) -> Result<Loaded> {
        let mut found: Vec<Option<Block>> = Vec::with_capacity(w.blocks.len());
        for b in &w.blocks {
            found.push(self.cached(w.chunk, b.block_no).await);
        }
        let first = found.iter().position(Option::is_none);
        let last = found.iter().rposition(Option::is_none);
        if let (Some(first), Some(last)) = (first, last) {
            let fetched = self.fetch(w.chunk, &w.blocks[first..=last], &w.layouts).await?;
            for (i, stored) in (first..=last).zip(fetched) {
                if let Some(l2) = &self.l2 {
                    l2.insert(w.chunk, w.blocks[i].block_no, stored.clone()).await;
                }
                if found[i].is_none() {
                    found[i] = Some(Block::Stored(stored));
                }
            }
        }
        let blocks = w.blocks.into_iter().zip(found.into_iter().map(|b| b.expect("every block found"))).collect();
        Ok(Loaded { chunk: w.chunk, blocks, from: w.from, to: w.to })
    }

    /// A block from L1, else L2.
    async fn cached(&self, chunk: ChunkId, block_no: u32) -> Option<Block> {
        if let Some(l1) = &self.l1 {
            if let Some(hit) = l1.get(chunk, block_no).await {
                return Some(Block::Decoded(hit));
            }
        }
        let l2 = self.l2.as_ref()?;
        l2.get(chunk, block_no).await.map(Block::Stored)
    }

    /// The stored bytes of consecutive `blocks`, in one read from the
    /// replicas, each checked against its CRC (a damaged one is read from
    /// another replica).
    async fn fetch(&self, chunk: ChunkId, blocks: &[StoredBlock], layouts: &[ReplicaLayout]) -> Result<Vec<Bytes>> {
        let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else { return Ok(Vec::new()) };
        let range = first.block_no..last.block_no + 1;
        let hook = self.not_found_hook(chunk);
        let stream = self.replicated.read_with_hook(layouts, range, hook).await.context("replicated read blocks")?;
        let raw = collect_stream(stream).await?;
        let need: usize = blocks.iter().map(|b| b.stored_len as usize).sum();
        if raw.len() != need {
            bail!(
                "chunk {chunk}: blocks {}..={} read {} stored bytes, want {need}",
                first.block_no,
                last.block_no,
                raw.len()
            );
        }
        let mut out = Vec::with_capacity(blocks.len());
        let mut off = 0usize;
        for b in blocks {
            let len = b.stored_len as usize;
            let stored = raw.slice(off..off + len);
            off += len;
            if crc32fast::hash(&stored) == b.crc32 {
                out.push(stored);
            } else {
                out.push(self.read_block_around_damage(chunk, b, layouts).await?);
            }
        }
        Ok(out)
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
        let block_no = block.block_no;
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

    /// The part of a block within `from..to` of its chunk, decoded (and put
    /// in L1) if it was not.
    async fn emit(&self, chunk: ChunkId, block: &StoredBlock, data: Block, from: usize, to: usize) -> Result<Bytes> {
        let decoded = match data {
            Block::Decoded(bytes) => bytes,
            Block::Stored(stored) => {
                let decoded = decode_block_slice(&stored, block.codec, block.logical_len as usize)?;
                if let Some(l1) = &self.l1 {
                    l1.insert(chunk, block.block_no, decoded.clone()).await;
                }
                decoded
            }
        };
        let start = block.logical_off as usize;
        let local_from = from.saturating_sub(start).min(decoded.len());
        let local_to = to.saturating_sub(start).min(decoded.len());
        Ok(if local_from < local_to { decoded.slice(local_from..local_to) } else { Bytes::new() })
    }

    fn not_found_hook(&self, chunk_id: ChunkId) -> Option<pigeonhole_blob::NotFoundHook> {
        let db = self.db.clone();
        Some(Arc::new(move |instance_id: &str| {
            spawn_enqueue_repair(db.clone(), chunk_id, instance_id.to_string());
        }))
    }
}
