//! Codec helpers and ingest options shared by [`crate::ChunkStore`].
//!
//! Framed policies (`zstd` / `gzip`) pack data as independent fixed-size frames
//! ([`pigeonhole_codec::BlockWriter`]) so each block is compressed once. Chunk
//! codec stored in the index is [`ChunkCodec::Blocks`]. Legacy single-blob
//! `raw` / `gzip` / `zstd` chunks remain readable via [`decode_chunk`].

use pigeonhole_codec::{self as chunker, ChunkCodec};
use pigeonhole_codec::{ByteBudget, BlockRecord};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub use pigeonhole_codec::{codec_from_sql, codec_to_sql};

/// Optional streaming hasher updated with plaintext body bytes during ingest.
///
/// Gateways (e.g. S3 CRC/SHA) can implement this; the layer stays free of protocol crates.
pub trait IngestHasher: Send {
    fn update(&mut self, data: &[u8]);
}

impl IngestHasher for () {
    fn update(&mut self, _data: &[u8]) {}
}

#[derive(Clone)]
pub struct IngestOptions {
    pub chunk_size: usize,
    pub codec: ChunkCodec,
    pub block_size: usize,
    pub memory_budget: Option<ByteBudget>,
    /// Optional shared counter for tests.
    pub compress_calls: Option<Arc<AtomicU64>>,
}

impl IngestOptions {
    pub fn new(chunk_size: usize, codec: ChunkCodec) -> Self {
        Self {
            chunk_size,
            codec,
            block_size: chunker::DEFAULT_BLOCK_SIZE,
            memory_budget: None,
            compress_calls: None,
        }
    }
}

fn gunzip_capped(data: &[u8], max_out: usize) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = dec.read(&mut buf).context("gunzip")?;
        if n == 0 {
            break;
        }
        if out.len().saturating_add(n) > max_out {
            bail!("gzip output exceeds max_logical {max_out}");
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

/// Decode a legacy single-blob chunk (`raw` / `gzip` / `zstd`).
pub fn decode_chunk(stored: Bytes, codec: ChunkCodec, max_logical: usize) -> Result<Bytes> {
    let max_logical = max_logical.max(1);
    match codec {
        ChunkCodec::Raw => {
            if stored.len() > max_logical {
                bail!(
                    "raw chunk {} exceeds max_logical {max_logical}",
                    stored.len()
                );
            }
            Ok(stored)
        }
        ChunkCodec::Gzip => Ok(Bytes::from(gunzip_capped(stored.as_ref(), max_logical)?)),
        ChunkCodec::Zstd => {
            let out = zstd::bulk::decompress(stored.as_ref(), max_logical)
                .context("zstd decompress")?;
            if out.len() > max_logical {
                bail!(
                    "zstd output {} exceeds max_logical {max_logical}",
                    out.len()
                );
            }
            Ok(Bytes::from(out))
        }
        ChunkCodec::Blocks => bail!("decode_chunk does not handle frames; use decode_blocks_range"),
    }
}

pub async fn decode_chunk_async(
    stored: Bytes,
    codec: ChunkCodec,
    max_logical: usize,
) -> Result<Bytes> {
    if matches!(codec, ChunkCodec::Raw) || stored.len() < 64 * 1024 {
        return decode_chunk(stored, codec, max_logical);
    }
    tokio::task::spawn_blocking(move || decode_chunk(stored, codec, max_logical))
        .await
        .context("spawn_blocking decode")?
}

/// Decode a slice of a chunk, dispatching on codec.
pub async fn decode_chunk_slice_async(
    stored: Bytes,
    codec: ChunkCodec,
    blocks: &[BlockRecord],
    from: usize,
    to: usize,
    logical_size: usize,
) -> Result<Bytes> {
    match codec {
        ChunkCodec::Blocks => {
            let blocks = blocks.to_vec();
            tokio::task::spawn_blocking(move || {
                pigeonhole_codec::decode_blocks_range(stored.as_ref(), &blocks, from, to)
            })
            .await
            .context("spawn_blocking blocks decode")?
        }
        other => {
            let logical = decode_chunk_async(stored, other, logical_size.max(1)).await?;
            if from > to || to > logical.len() {
                bail!("slice {from}..{to} outside logical {}", logical.len());
            }
            Ok(logical.slice(from..to))
        }
    }
}

/// Sync encode helper for unit tests (legacy single-blob).
pub fn encode_chunk(logical: Bytes, policy: ChunkCodec) -> (Bytes, ChunkCodec) {
    if policy == ChunkCodec::Raw || logical.is_empty() {
        return (logical, ChunkCodec::Raw);
    }
    let fc = policy.block_codec();
    match compress_slice(&logical, fc) {
        Ok(c) if c.len() < logical.len() => {
            pigeonhole_blob::record_compression_ratio(c.len(), logical.len());
            (Bytes::from(c), fc)
        }
        _ => {
            pigeonhole_blob::record_compression_ratio(logical.len(), logical.len());
            (logical, ChunkCodec::Raw)
        }
    }
}

fn compress_slice(data: &[u8], codec: ChunkCodec) -> Result<Vec<u8>> {
    match codec {
        ChunkCodec::Raw | ChunkCodec::Blocks => Ok(data.to_vec()),
        ChunkCodec::Gzip => {
            let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
            enc.write_all(data).context("gzip write")?;
            enc.finish().context("gzip finish")
        }
        ChunkCodec::Zstd => zstd::bulk::compress(data, 1).context("zstd compress"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_zstd_decode_still_works() {
        let data = vec![7u8; 4096];
        let stored = compress_slice(&data, ChunkCodec::Zstd).unwrap();
        assert!(stored.len() < data.len());
        let got = decode_chunk(Bytes::from(stored), ChunkCodec::Zstd, data.len()).unwrap();
        assert_eq!(got.as_ref(), data.as_slice());
    }

    #[test]
    fn encode_raw_ignores_compressibility() {
        let data = Bytes::from(vec![0u8; 4096]);
        let (payload, codec) = encode_chunk(data.clone(), ChunkCodec::Raw);
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(payload, data);
    }
}
