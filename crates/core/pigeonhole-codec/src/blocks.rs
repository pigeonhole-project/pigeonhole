//! Independent fixed-size blocks packed into a chunk's parts.
//!
//! Each block compresses exactly once (`block_size` logical bytes). Blocks
//! are concatenated on the wire; the chunk's record (the chunk store's
//! `StoredBlock`s) maps stored and logical ranges, so a range read
//! decompresses only the blocks it needs.

use crate::chunker::ChunkCodec;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Semaphore;

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

/// Encode one logical block to stored bytes, and the codec they are in
/// (raw when compression does not shrink it).
pub fn encode_block_bytes(logical: &[u8], policy: ChunkCodec) -> Result<(Bytes, ChunkCodec)> {
    let (v, c) = encode_block(logical, policy)?;
    Ok((Bytes::from(v), c))
}

/// Decode one stored block of `logical_len` bytes, in `codec` (`raw`,
/// `gzip` or `zstd`; `blocks` is a chunk's policy, never a block's). A
/// block that decodes to another length is an error.
pub fn decode_block_slice(stored: &[u8], codec: ChunkCodec, logical_len: usize) -> Result<Bytes> {
    if codec == ChunkCodec::Blocks {
        bail!("a block's codec is raw, gzip or zstd, not {codec}");
    }
    let decoded = decode_block(stored, codec, logical_len)?;
    if decoded.len() != logical_len {
        bail!("block decodes to {} bytes, its record says {logical_len}", decoded.len());
    }
    Ok(Bytes::from(decoded))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_decodes_to_its_length_in_its_codec() {
        let logical = vec![7u8; 4096];
        for policy in [ChunkCodec::Raw, ChunkCodec::Gzip, ChunkCodec::Zstd, ChunkCodec::Blocks] {
            let (stored, codec) = encode_block_bytes(&logical, policy).unwrap();
            assert_ne!(codec, ChunkCodec::Blocks, "{policy}");
            assert_eq!(decode_block_slice(&stored, codec, logical.len()).unwrap().as_ref(), logical.as_slice());
            assert!(decode_block_slice(&stored, codec, logical.len() - 1).is_err(), "{policy}: a longer block");
            assert!(decode_block_slice(&stored, codec, logical.len() + 1).is_err(), "{policy}: a shorter block");
        }
        assert!(decode_block_slice(&logical, ChunkCodec::Blocks, logical.len()).is_err());
        let (stored, codec) = encode_block_bytes(&[1, 2, 3], ChunkCodec::Zstd).unwrap();
        assert_eq!(codec, ChunkCodec::Raw, "a block that does not shrink is stored raw");
        assert_eq!(stored.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn a_stored_codec_is_one_of_four_names() {
        let json = serde_json::to_string(&ChunkCodec::Zstd).unwrap();
        assert_eq!(json, "\"zstd\"");
        assert!(serde_json::from_str::<ChunkCodec>("\"lz4\"").is_err(), "an unknown codec is an error");
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
