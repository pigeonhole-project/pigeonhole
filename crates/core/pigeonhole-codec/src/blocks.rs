//! Independent fixed-size frames packed into Telegram documents.
//!
//! Each frame compresses exactly once (`BLOCK_SIZE` logical bytes). Frames are
//! concatenated on the wire; the `chunk_blocks` index table maps stored/logical
//! ranges so Range reads decompress only the needed frames.

use crate::chunker::ChunkCodec;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// One independent frame inside a `frames` chunk document.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockRecord {
    pub block_no: i64,
    pub stored_off: i64,
    pub stored_len: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    /// Per-frame codec: `raw`, `gzip`, or `zstd` (never `frames`).
    pub codec: String,
}

impl BlockRecord {
    pub fn block_codec(&self) -> ChunkCodec {
        match self.codec.as_str() {
            "gzip" => ChunkCodec::Gzip,
            "zstd" => ChunkCodec::Zstd,
            _ => ChunkCodec::Raw,
        }
    }
}

/// Held ingest-budget bytes; returned to the semaphore on drop.
///
/// Unlike `OwnedSemaphorePermit`, excess capacity can be released early via
/// [`BudgetPermit::release_excess`] (a chunk sealed short of its reservation).
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
        f.debug_struct("BudgetPermit").field("bytes", &self.n).finish()
    }
}

/// Process-wide byte budget for ingest buffers (Semaphore permits = bytes).
///
/// The chunk store takes a whole chunk's share at once and holds it until
/// the chunk is uploaded.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    sem: Arc<Semaphore>,
    capacity: usize,
}

impl ByteBudget {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1024);
        Self { sem: Arc::new(Semaphore::new(capacity)), capacity }
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
            bail!("ingest acquire {n} exceeds budget capacity {}", self.capacity);
        }
        let t0 = std::time::Instant::now();
        let permit = self.sem.clone().acquire_many_owned(n as u32).await.context("ingest memory budget closed")?;
        let waited = t0.elapsed();
        if !waited.is_zero() {
            metrics::histogram!("pigeonhole_ingest_budget_wait_seconds").record(waited.as_secs_f64());
        }
        // Manage accounting ourselves so we can release_excess on short frames.
        permit.forget();
        Ok(BudgetPermit { sem: self.sem.clone(), n })
    }
}

/// Encode one logical block to stored bytes (raw fallback when compression does not shrink).
pub fn encode_block_bytes(logical: &[u8], policy: ChunkCodec) -> Result<(Bytes, String)> {
    let (v, c) = encode_block(logical, policy)?;
    Ok((Bytes::from(v), c.as_str().to_string()))
}

/// Decode one stored block to logical bytes.
pub fn decode_block_slice(stored: &[u8], codec: &str, max_logical: usize) -> Result<Bytes> {
    let c = match codec {
        "gzip" => ChunkCodec::Gzip,
        "zstd" => ChunkCodec::Zstd,
        _ => ChunkCodec::Raw,
    };
    Ok(Bytes::from(decode_block(stored, c, max_logical)?))
}

fn encode_block(logical: &[u8], policy: ChunkCodec) -> Result<(Vec<u8>, ChunkCodec)> {
    if logical.is_empty() {
        return Ok((Vec::new(), ChunkCodec::Raw));
    }
    if policy == ChunkCodec::Raw {
        return Ok((logical.to_vec(), ChunkCodec::Raw));
    }
    let stored_codec = policy.block_codec();
    let compressed = match stored_codec {
        ChunkCodec::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(logical).context("gzip frame")?;
            enc.finish().context("gzip finish")?
        }
        ChunkCodec::Zstd => zstd::bulk::compress(logical, 1).context("zstd frame")?,
        ChunkCodec::Raw | ChunkCodec::Blocks => logical.to_vec(),
    };
    if compressed.len() < logical.len() {
        Ok((compressed, stored_codec))
    } else {
        Ok((logical.to_vec(), ChunkCodec::Raw))
    }
}

fn decode_block(stored: &[u8], codec: ChunkCodec, max_logical: usize) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw | ChunkCodec::Blocks => {
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
pub fn decode_blocks_range(stored: &[u8], blocks: &[BlockRecord], from: usize, to: usize) -> Result<Bytes> {
    if from > to {
        bail!("invalid frame range {from}..{to}");
    }
    if from == to {
        return Ok(Bytes::new());
    }
    let mut out = Vec::with_capacity(to - from);
    let mut logical_cursor = 0usize;
    for fr in blocks {
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
            bail!("frame stored range {}..{} outside blob {}", soff, end, stored.len());
        }
        let decoded = decode_block(&stored[soff..end], fr.block_codec(), flen.max(1))?;
        if decoded.len() != flen {
            bail!("frame logical length mismatch: index {flen}, decoded {}", decoded.len());
        }
        let local_from = from.saturating_sub(frame_start).min(flen);
        let local_to = to.saturating_sub(frame_start).min(flen);
        if local_from < local_to {
            out.extend_from_slice(&decoded[local_from..local_to]);
        }
    }
    if out.len() != to - from {
        bail!("frames range decode produced {} bytes, expected {}", out.len(), to - from);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_across_frame_boundary() {
        let f0 = encode_block(&[1u8; 100], ChunkCodec::Raw).unwrap();
        let f1 = encode_block(&[2u8; 100], ChunkCodec::Raw).unwrap();
        let mut stored = f0.0;
        let off1 = stored.len();
        stored.extend_from_slice(&f1.0);
        let frames = vec![
            BlockRecord {
                block_no: 0,
                stored_off: 0,
                stored_len: off1 as i64,
                logical_off: 0,
                logical_len: 100,
                codec: "raw".into(),
            },
            BlockRecord {
                block_no: 1,
                stored_off: off1 as i64,
                stored_len: f1.0.len() as i64,
                logical_off: 100,
                logical_len: 100,
                codec: "raw".into(),
            },
        ];
        let mid = decode_blocks_range(&stored, &frames, 90, 110).unwrap();
        let mut expect = vec![1u8; 10];
        expect.extend(std::iter::repeat_n(2u8, 10));
        assert_eq!(mid.as_ref(), expect.as_slice());
    }

    #[tokio::test]
    async fn released_excess_and_dropped_permits_return_to_the_budget() {
        let budget = ByteBudget::new(64 * 1024);
        let mut p = budget.acquire(48 * 1024).await.unwrap();
        assert_eq!(budget.available_permits(), 16 * 1024);
        p.release_excess(40 * 1024);
        assert_eq!((p.bytes(), budget.available_permits()), (8 * 1024, 56 * 1024));
        drop(p);
        assert_eq!(budget.available_permits(), budget.capacity());
        assert!(budget.acquire(65 * 1024).await.is_err());
    }
}
