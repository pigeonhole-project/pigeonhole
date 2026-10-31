//! Telegram getFile is limited to 20 MiB; on-wire chunk payloads must stay below that.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Hard ceiling: stored payload must be downloadable via getFile (< 20 MiB).
pub const MAX_CHUNK_SIZE: usize = 20 * 1024 * 1024 - 1;
/// Default max **stored** (on-wire) chunk size.
pub const DEFAULT_CHUNK_SIZE: usize = 19 * 1024 * 1024;
const _: () = assert!(DEFAULT_CHUNK_SIZE <= MAX_CHUNK_SIZE);
/// Leave headroom under `chunk.size` so a final encode block fits.
pub const COMPRESS_SIZE_MARGIN: usize = 256 * 1024;
/// Cap uncompressed bytes buffered / decoded per chunk.
pub const MAX_LOGICAL_CHUNK: usize = 256 * 1024 * 1024;
/// Default independent block size for `blocks` codec packing.
pub const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024;
/// Default process-wide ingest buffer budget.
pub const DEFAULT_INGEST_MEMORY_BUDGET: usize = 256 * 1024 * 1024;

/// How new object chunks are encoded before `sendDocument`.
///
/// Stored per-chunk codec may still be `raw` under a compress policy when the
/// probe shows no gain. New compressible uploads use [`ChunkCodec::Blocks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChunkCodec {
    /// Store bytes as-is (no compression attempt).
    Raw,
    /// gzip policy / per-block codec.
    Gzip,
    /// zstd policy / per-block codec.
    Zstd,
    /// Concatenated independent blocks (see `chunk_blocks` table). Default for new
    /// compressible uploads when config codec is `zstd` or `gzip`.
    #[default]
    Blocks,
}

impl ChunkCodec {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
            Self::Blocks => "blocks",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "raw" | "none" | "off" => Ok(Self::Raw),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zstd" | "zst" => Ok(Self::Zstd),
            "blocks" => Ok(Self::Blocks),
            other => bail!("unknown chunk codec {other:?}; expected raw|gzip|zstd|blocks"),
        }
    }

    /// Config policy that should pack with independent blocks.
    pub fn uses_block_packing(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd | Self::Blocks)
    }

    pub fn is_compressing(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd | Self::Blocks)
    }

    /// Per-block compression algorithm for a packing policy.
    pub fn block_codec(self) -> Self {
        match self {
            Self::Gzip => Self::Gzip,
            Self::Zstd | Self::Blocks => Self::Zstd,
            Self::Raw => Self::Raw,
        }
    }
}

impl std::fmt::Display for ChunkCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn validate_chunk_size(n: usize) -> Result<()> {
    if n == 0 {
        bail!("chunk.size must be >= 1");
    }
    if n > MAX_CHUNK_SIZE {
        bail!("chunk.size={n} exceeds Telegram getFile limit (max {MAX_CHUNK_SIZE} bytes, < 20 MiB)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_under_limit() {
        validate_chunk_size(DEFAULT_CHUNK_SIZE).unwrap();
    }

    #[test]
    fn rejects_20_mib() {
        assert!(validate_chunk_size(20 * 1024 * 1024).is_err());
    }

    #[test]
    fn codec_parse() {
        assert_eq!(ChunkCodec::parse("raw").unwrap(), ChunkCodec::Raw);
        assert_eq!(ChunkCodec::parse("GZIP").unwrap(), ChunkCodec::Gzip);
        assert_eq!(ChunkCodec::parse("zstd").unwrap(), ChunkCodec::Zstd);
        assert_eq!(ChunkCodec::parse("none").unwrap(), ChunkCodec::Raw);
        assert!(ChunkCodec::parse("lz4").is_err());
    }
}
