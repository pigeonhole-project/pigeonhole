//! Independent fixed-size frames packed into Telegram documents.
//!
//! Each frame compresses exactly once (`FRAME_SIZE` logical bytes). Frames are
//! concatenated on the wire; the `chunk_frames` index table maps stored/logical
//! ranges so Range reads decompress only the needed frames.

use crate::chunker::{self, ChunkCodec};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// One independent frame inside a `frames` chunk document.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FrameRecord {
    pub frame_no: i64,
    pub stored_off: i64,
    pub stored_len: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    /// Per-frame codec: `raw`, `gzip`, or `zstd` (never `frames`).
    pub codec: String,
}

impl FrameRecord {
    pub fn frame_codec(&self) -> ChunkCodec {
        match self.codec.as_str() {
            "gzip" => ChunkCodec::Gzip,
            "zstd" => ChunkCodec::Zstd,
            _ => ChunkCodec::Raw,
        }
    }
}

/// A finished on-wire chunk ready for `BlobStore::put`.
///
/// `_permits` holds ingest-budget slots for this chunk's logical bytes; they are
/// released when the chunk is dropped (after a successful put, or on cancel/error).
pub struct CompletedChunk {
    pub payload: Bytes,
    pub logical_size: i64,
    pub frames: Vec<FrameRecord>,
    _permits: Vec<BudgetPermit>,
}

impl std::fmt::Debug for CompletedChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedChunk")
            .field("payload_len", &self.payload.len())
            .field("logical_size", &self.logical_size)
            .field("frames", &self.frames.len())
            .field(
                "permits",
                &self._permits.iter().map(|p| p.bytes()).sum::<usize>(),
            )
            .finish()
    }
}

/// Held ingest-budget bytes; returned to the semaphore on drop.
///
/// Unlike `OwnedSemaphorePermit`, excess capacity can be released early via
/// [`BudgetPermit::release_excess`] (short last frame of a stream).
pub struct BudgetPermit {
    sem: Arc<Semaphore>,
    n: usize,
}

impl BudgetPermit {
    pub fn bytes(&self) -> usize {
        self.n
    }

    /// Return `excess` bytes to the budget immediately; the remainder stays held.
    pub fn release_excess(&mut self, excess: usize) {
        let excess = excess.min(self.n);
        if excess == 0 {
            return;
        }
        self.n -= excess;
        self.sem.add_permits(excess);
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        if self.n > 0 {
            self.sem.add_permits(self.n);
            self.n = 0;
        }
    }
}

impl std::fmt::Debug for BudgetPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetPermit")
            .field("bytes", &self.n)
            .finish()
    }
}

/// Process-wide byte budget for ingest buffers (Semaphore permits = bytes).
///
/// Permits are taken for a whole frame at a time; sealed chunks carry their
/// permits until uploaded and dropped.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    sem: Arc<Semaphore>,
    capacity: usize,
}

impl ByteBudget {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1024);
        Self {
            sem: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self, n: usize) -> Result<BudgetPermit> {
        let n = n.max(1);
        if n > self.capacity {
            bail!(
                "ingest acquire {n} exceeds budget capacity {}",
                self.capacity
            );
        }
        let permit = self
            .sem
            .clone()
            .acquire_many_owned(n as u32)
            .await
            .context("ingest memory budget closed")?;
        // Manage accounting ourselves so we can release_excess on short frames.
        permit.forget();
        Ok(BudgetPermit {
            sem: self.sem.clone(),
            n,
        })
    }
}

/// Packs a logical stream into Telegram-sized documents of independent frames.
pub struct FrameWriter {
    frame_size: usize,
    max_stored: usize,
    max_logical: usize,
    frame_codec: ChunkCodec,
    block: Vec<u8>,
    chunk_stored: Vec<u8>,
    chunk_logical: usize,
    frames: Vec<FrameRecord>,
    compress_calls: Arc<AtomicU64>,
    budget: Option<ByteBudget>,
    /// Whole-frame permit for the current incomplete `block` (released early on
    /// a short last frame via [`BudgetPermit::release_excess`]).
    block_permit: Option<BudgetPermit>,
    /// Permits for logical bytes already framed into the open (unsealed) chunk.
    chunk_permits: Vec<BudgetPermit>,
}

impl FrameWriter {
    pub fn new(
        frame_size: usize,
        max_stored: usize,
        max_logical: usize,
        policy: ChunkCodec,
        budget: Option<ByteBudget>,
        compress_calls: Arc<AtomicU64>,
    ) -> Self {
        let frame_size = frame_size.clamp(1024, max_logical);
        let max_stored = max_stored.clamp(1024, chunker::MAX_CHUNK_SIZE);
        let max_logical = max_logical.max(frame_size);
        Self {
            frame_size,
            max_stored,
            max_logical,
            frame_codec: policy.frame_codec(),
            block: Vec::with_capacity(frame_size),
            chunk_stored: Vec::new(),
            chunk_logical: 0,
            frames: Vec::new(),
            compress_calls,
            budget,
            block_permit: None,
            chunk_permits: Vec::new(),
        }
    }

    pub fn compress_calls(&self) -> u64 {
        self.compress_calls.load(Ordering::Relaxed)
    }

    /// Push stream bytes; returns completed chunks and how many input bytes were
    /// consumed.
    ///
    /// When a memory budget is configured, this returns as soon as one or more
    /// chunks are sealed so the caller can upload/drop them (freeing permits)
    /// before more input is reserved. Callers must loop until `consumed == data.len()`.
    ///
    /// Budget permits are acquired for a whole [`frame_size`] at the start of each
    /// block so concurrent PUTs cannot each hold a fragment of a frame and deadlock.
    pub async fn push(&mut self, mut data: &[u8]) -> Result<(Vec<CompletedChunk>, usize)> {
        let total = data.len();
        let mut out = Vec::new();
        while !data.is_empty() {
            if self.block.is_empty() {
                // Seal the open chunk before waiting for another whole frame so
                // the caller can free permits under a tight shared budget.
                if let Some(budget) = &self.budget {
                    if budget.available_permits() < self.frame_size && !self.chunk_stored.is_empty()
                    {
                        if let Some(c) = self.seal_chunk() {
                            out.push(c);
                            return Ok((out, total - data.len()));
                        }
                    }
                }
                self.acquire_frame_budget().await?;
            }
            let need = self.frame_size - self.block.len();
            let take = need.min(data.len());
            self.block.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.block.len() >= self.frame_size {
                let sealed = self.emit_frame().await?;
                if !sealed.is_empty() {
                    out.extend(sealed);
                    // With a budget, stop so the caller can free permits via put/drop.
                    if self.budget.is_some() {
                        return Ok((out, total - data.len()));
                    }
                }
            }
        }
        Ok((out, total))
    }

    /// Finish the stream: flush partial block and seal the last chunk.
    pub async fn finish(mut self) -> Result<Vec<CompletedChunk>> {
        let mut out = Vec::new();
        if !self.block.is_empty() {
            out.extend(self.emit_frame().await?);
        }
        if let Some(c) = self.seal_chunk() {
            out.push(c);
        }
        // Any leftover permits (empty stream edge cases) drop with `self`.
        Ok(out)
    }

    /// Compress current `block` into one frame; may seal the previous chunk first.
    async fn emit_frame(&mut self) -> Result<Vec<CompletedChunk>> {
        if self.block.is_empty() {
            return Ok(Vec::new());
        }
        let logical = std::mem::take(&mut self.block);
        let logical_len = logical.len();
        let mut frame_permit = self.block_permit.take();
        if let Some(permit) = frame_permit.as_mut() {
            // Short last block: return unused whole-frame reservation.
            permit.release_excess(permit.bytes().saturating_sub(logical_len));
        }
        let frame_codec = self.frame_codec;
        let calls = self.compress_calls.clone();
        let (payload, stored_codec) = tokio::task::spawn_blocking(move || {
            calls.fetch_add(1, Ordering::Relaxed);
            encode_frame(&logical, frame_codec)
        })
        .await
        .context("spawn_blocking encode_frame")??;

        let mut sealed = Vec::new();
        let would_stored = self.chunk_stored.len() + payload.len();
        let would_logical = self.chunk_logical + logical_len;
        if !self.chunk_stored.is_empty()
            && (would_stored > self.max_stored || would_logical > self.max_logical)
        {
            if let Some(c) = self.seal_chunk() {
                sealed.push(c);
            }
        }

        // Single frame larger than max_stored: still emit (should not happen for
        // frame_size << max_stored); if it does, seal as its own chunk.
        if self.chunk_stored.is_empty() && payload.len() > self.max_stored {
            bail!(
                "encoded frame {} exceeds max_stored {}",
                payload.len(),
                self.max_stored
            );
        }

        let frame_no = self.frames.len() as i64;
        let stored_off = self.chunk_stored.len() as i64;
        let logical_off = self.chunk_logical as i64;
        self.chunk_stored.extend_from_slice(&payload);
        self.chunk_logical += logical_len;
        if let Some(permit) = frame_permit {
            self.chunk_permits.push(permit);
        }
        self.frames.push(FrameRecord {
            frame_no,
            stored_off,
            stored_len: payload.len() as i64,
            logical_off,
            logical_len: logical_len as i64,
            codec: stored_codec.as_str().to_string(),
        });
        Ok(sealed)
    }

    fn seal_chunk(&mut self) -> Option<CompletedChunk> {
        if self.chunk_stored.is_empty() {
            return None;
        }
        let payload = Bytes::from(std::mem::take(&mut self.chunk_stored));
        let logical_size = self.chunk_logical as i64;
        let frames = std::mem::take(&mut self.frames);
        let permits = std::mem::take(&mut self.chunk_permits);
        self.chunk_logical = 0;
        Some(CompletedChunk {
            payload,
            logical_size,
            frames,
            _permits: permits,
        })
    }

    async fn acquire_frame_budget(&mut self) -> Result<()> {
        if self.block_permit.is_some() {
            return Ok(());
        }
        if let Some(budget) = &self.budget {
            self.block_permit = Some(budget.acquire(self.frame_size).await?);
        }
        Ok(())
    }
}

fn encode_frame(logical: &[u8], policy: ChunkCodec) -> Result<(Vec<u8>, ChunkCodec)> {
    if logical.is_empty() {
        return Ok((Vec::new(), ChunkCodec::Raw));
    }
    if policy == ChunkCodec::Raw {
        return Ok((logical.to_vec(), ChunkCodec::Raw));
    }
    let stored_codec = policy.frame_codec();
    let compressed = match stored_codec {
        ChunkCodec::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(logical).context("gzip frame")?;
            enc.finish().context("gzip finish")?
        }
        ChunkCodec::Zstd => zstd::bulk::compress(logical, 1).context("zstd frame")?,
        ChunkCodec::Raw | ChunkCodec::Frames => logical.to_vec(),
    };
    if compressed.len() < logical.len() {
        Ok((compressed, stored_codec))
    } else {
        Ok((logical.to_vec(), ChunkCodec::Raw))
    }
}

fn decode_frame(stored: &[u8], codec: ChunkCodec, max_logical: usize) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw | ChunkCodec::Frames => {
            if stored.len() > max_logical {
                bail!("raw frame {} exceeds max {max_logical}", stored.len());
            }
            Ok(stored.to_vec())
        }
        ChunkCodec::Gzip => gunzip_capped(stored, max_logical),
        ChunkCodec::Zstd => {
            let out = zstd::bulk::decompress(stored, max_logical).context("zstd frame decode")?;
            if out.len() > max_logical {
                bail!("zstd frame {} exceeds max {max_logical}", out.len());
            }
            Ok(out)
        }
    }
}

fn gunzip_capped(data: &[u8], max_out: usize) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = dec.read(&mut buf).context("gunzip frame")?;
        if n == 0 {
            break;
        }
        if out.len().saturating_add(n) > max_out {
            bail!("gzip frame output exceeds max_logical {max_out}");
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

/// Decode a byte range `[from, to)` within one `frames` chunk document.
pub fn decode_frames_range(
    stored: &[u8],
    frames: &[FrameRecord],
    from: usize,
    to: usize,
) -> Result<Bytes> {
    if from > to {
        bail!("invalid frame range {from}..{to}");
    }
    if from == to {
        return Ok(Bytes::new());
    }
    let mut out = Vec::with_capacity(to - from);
    let mut logical_cursor = 0usize;
    for fr in frames {
        let flen = fr.logical_len as usize;
        let frame_start = logical_cursor;
        let frame_end = logical_cursor + flen;
        logical_cursor = frame_end;
        if frame_end <= from || frame_start >= to {
            continue;
        }
        let soff = fr.stored_off as usize;
        let slen = fr.stored_len as usize;
        let end = soff.checked_add(slen).context("frame stored range")?;
        if end > stored.len() {
            bail!(
                "frame stored range {}..{} outside blob {}",
                soff,
                end,
                stored.len()
            );
        }
        let decoded = decode_frame(&stored[soff..end], fr.frame_codec(), flen.max(1))?;
        if decoded.len() != flen {
            bail!(
                "frame logical length mismatch: index {flen}, decoded {}",
                decoded.len()
            );
        }
        let local_from = from.saturating_sub(frame_start).min(flen);
        let local_to = to.saturating_sub(frame_start).min(flen);
        if local_from < local_to {
            out.extend_from_slice(&decoded[local_from..local_to]);
        }
    }
    if out.len() != to - from {
        bail!(
            "frames range decode produced {} bytes, expected {}",
            out.len(),
            to - from
        );
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn packs_zeros_with_few_compress_calls() {
        let calls = Arc::new(AtomicU64::new(0));
        let frame = 64 * 1024;
        let max_stored = 256 * 1024;
        let mut w = FrameWriter::new(
            frame,
            max_stored,
            8 * 1024 * 1024,
            ChunkCodec::Zstd,
            None,
            calls.clone(),
        );
        let n = 512 * 1024;
        let data = vec![0u8; n];
        let (mut chunks, consumed) = w.push(&data).await.unwrap();
        assert_eq!(consumed, n);
        chunks.extend(w.finish().await.unwrap());
        assert!(!chunks.is_empty());
        let expected_calls = (n + frame - 1) / frame;
        assert_eq!(calls.load(Ordering::Relaxed) as usize, expected_calls);
        let mut out = Vec::new();
        for c in &chunks {
            out.extend_from_slice(
                &decode_frames_range(&c.payload, &c.frames, 0, c.logical_size as usize).unwrap(),
            );
        }
        assert_eq!(out, data);
        assert!(chunks.iter().all(|c| c.payload.len() <= max_stored));
    }

    #[test]
    fn range_across_frame_boundary() {
        let f0 = encode_frame(&[1u8; 100], ChunkCodec::Raw).unwrap();
        let f1 = encode_frame(&[2u8; 100], ChunkCodec::Raw).unwrap();
        let mut stored = f0.0;
        let off1 = stored.len();
        stored.extend_from_slice(&f1.0);
        let frames = vec![
            FrameRecord {
                frame_no: 0,
                stored_off: 0,
                stored_len: off1 as i64,
                logical_off: 0,
                logical_len: 100,
                codec: "raw".into(),
            },
            FrameRecord {
                frame_no: 1,
                stored_off: off1 as i64,
                stored_len: f1.0.len() as i64,
                logical_off: 100,
                logical_len: 100,
                codec: "raw".into(),
            },
        ];
        let mid = decode_frames_range(&stored, &frames, 90, 110).unwrap();
        let mut expect = vec![1u8; 10];
        expect.extend(std::iter::repeat(2u8).take(10));
        assert_eq!(mid.as_ref(), expect.as_slice());
    }

    #[tokio::test]
    async fn budget_push_returns_early_so_caller_can_free_permits() {
        let budget = ByteBudget::new(64 * 1024);
        let calls = Arc::new(AtomicU64::new(0));
        let mut w = FrameWriter::new(
            16 * 1024,
            32 * 1024,
            1024 * 1024,
            ChunkCodec::Raw,
            Some(budget.clone()),
            calls,
        );
        let data = vec![7u8; 96 * 1024];
        let mut offset = 0;
        while offset < data.len() {
            let (sealed, consumed) = w.push(&data[offset..]).await.unwrap();
            assert!(consumed > 0);
            offset += consumed;
            // Simulate upload: drop sealed chunks to free budget.
            drop(sealed);
        }
        let last = w.finish().await.unwrap();
        drop(last);
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn budget_returns_on_writer_drop_mid_stream() {
        let budget = ByteBudget::new(32 * 1024);
        let calls = Arc::new(AtomicU64::new(0));
        let mut w = FrameWriter::new(
            8 * 1024,
            16 * 1024,
            1024 * 1024,
            ChunkCodec::Raw,
            Some(budget.clone()),
            calls,
        );
        let (sealed, _) = w.push(&[1u8; 24 * 1024]).await.unwrap();
        drop(sealed);
        // Writer may still hold an open block/chunk.
        assert!(budget.available_permits() <= budget.capacity());
        drop(w);
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    #[tokio::test]
    async fn short_last_frame_releases_excess_budget() {
        let frame = 16 * 1024;
        let budget = ByteBudget::new(2 * frame);
        let calls = Arc::new(AtomicU64::new(0));
        let mut w = FrameWriter::new(
            frame,
            64 * 1024,
            1024 * 1024,
            ChunkCodec::Raw,
            Some(budget.clone()),
            calls,
        );
        let n = frame + 100;
        let data = vec![9u8; n];
        let mut offset = 0;
        while offset < data.len() {
            let (sealed, consumed) = w.push(&data[offset..]).await.unwrap();
            offset += consumed;
            drop(sealed);
        }
        let last = w.finish().await.unwrap();
        let logical: i64 = last.iter().map(|c| c.logical_size).sum();
        assert_eq!(logical, n as i64);
        drop(last);
        assert_eq!(budget.available_permits(), budget.capacity());
    }

    /// Many concurrent writers with a budget of only a few frames must not
    /// deadlock: each acquires a whole frame or waits, never a fragment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn parallel_writers_whole_frame_budget_no_deadlock() {
        let frame = 64 * 1024;
        let budget = ByteBudget::new(4 * frame);
        let mut joins = Vec::new();
        for _ in 0..64 {
            let budget = budget.clone();
            joins.push(tokio::spawn(async move {
                let calls = Arc::new(AtomicU64::new(0));
                let mut w = FrameWriter::new(
                    frame,
                    256 * 1024,
                    8 * 1024 * 1024,
                    ChunkCodec::Raw,
                    Some(budget),
                    calls,
                );
                let n = 8 * 1024 * 1024;
                let piece = 256 * 1024;
                let mut offset = 0usize;
                while offset < n {
                    let end = (offset + piece).min(n);
                    let chunk = vec![1u8; end - offset];
                    let mut local = 0usize;
                    while local < chunk.len() {
                        let (sealed, consumed) = w.push(&chunk[local..]).await.unwrap();
                        local += consumed;
                        drop(sealed);
                    }
                    offset = end;
                }
                drop(w.finish().await.unwrap());
            }));
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            for j in joins {
                j.await.expect("join");
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "64 parallel 8 MiB writers under 4*frame_size budget timed out"
        );
        assert_eq!(budget.available_permits(), budget.capacity());
    }
}
