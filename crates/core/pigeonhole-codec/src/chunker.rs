//! Telegram getFile is limited to 20 MiB; on-wire chunk payloads must stay below that.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Hard ceiling: stored payload must be downloadable via getFile (< 20 MiB).
pub const MAX_CHUNK_SIZE: usize = 20 * 1024 * 1024 - 1;
/// Default max **stored** (on-wire) chunk size.
pub const DEFAULT_CHUNK_SIZE: usize = 19 * 1024 * 1024;
/// Default max **logical** chunk size for ChunkStore part packing (stage E).
pub const DEFAULT_LOGICAL_CHUNK_SIZE: usize = 64 * 1024 * 1024;
/// Probe window before committing to compression for a chunk.
pub const COMPRESS_PROBE_BYTES: usize = 128 * 1024;
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
    /// Opportunistic gzip (legacy single-block chunk). Prefer zstd/blocks.
    Gzip,
    /// Opportunistic zstd (legacy single-block chunk). Prefer [`ChunkCodec::Blocks`].
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
            "blocks" | "block" | "frames" | "frame" => Ok(Self::Blocks),
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
        bail!(
            "chunk.size={n} exceeds Telegram getFile limit (max {MAX_CHUNK_SIZE} bytes, < 20 MiB)"
        );
    }
    Ok(())
}

/// Target compressed/raw fill size: stay under `chunk_size` with a safety margin.
pub fn fill_target(chunk_size: usize) -> usize {
    let capped = chunk_size.min(MAX_CHUNK_SIZE).max(1024);
    let margin = COMPRESS_SIZE_MARGIN.min(capped / 8).max(256);
    capped.saturating_sub(margin).max(1024).min(capped)
}

/// Max logical (uncompressed) bytes held for one chunk before a forced flush.
pub fn max_logical_bytes(chunk_size: usize) -> usize {
    let stored = chunk_size.clamp(1, MAX_CHUNK_SIZE);
    MAX_LOGICAL_CHUNK.max(stored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_under_limit() {
        assert!(DEFAULT_CHUNK_SIZE <= MAX_CHUNK_SIZE);
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
        assert_eq!(ChunkCodec::parse("frames").unwrap(), ChunkCodec::Blocks);
        assert_eq!(ChunkCodec::parse("none").unwrap(), ChunkCodec::Raw);
        assert!(ChunkCodec::parse("lz4").is_err());
    }

    #[test]
    fn max_logical_defaults_to_256_mib() {
        assert_eq!(max_logical_bytes(DEFAULT_CHUNK_SIZE), MAX_LOGICAL_CHUNK);
        assert_eq!(max_logical_bytes(1024), MAX_LOGICAL_CHUNK);
        assert_eq!(MAX_LOGICAL_CHUNK, 256 * 1024 * 1024);
    }
}
