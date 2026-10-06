//! Telegram getFile is limited to 20 MiB; on-wire chunk payloads must stay below that.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Hard ceiling: stored payload must be downloadable via getFile (< 20 MiB).
pub const MAX_CHUNK_SIZE: usize = 20 * 1024 * 1024 - 1;
/// Default max **stored** (on-wire) chunk size.
pub const DEFAULT_CHUNK_SIZE: usize = 19 * 1024 * 1024;
/// Probe window before committing to compression for a chunk.
pub const COMPRESS_PROBE_BYTES: usize = 128 * 1024;
/// Leave headroom under `chunk.size` so a final encode frame fits.
pub const COMPRESS_SIZE_MARGIN: usize = 256 * 1024;

/// How new object chunks are encoded before `sendDocument`.
///
/// Stored per-chunk codec may still be `raw` under a compress policy when the
/// probe shows no gain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChunkCodec {
    /// Store bytes as-is (no compression attempt).
    Raw,
    /// Opportunistic gzip (legacy). Prefer [`ChunkCodec::Zstd`].
    Gzip,
    /// Opportunistic zstd (level 1); keep only when it shrinks.
    #[default]
    Zstd,
}

impl ChunkCodec {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "raw" | "none" | "off" => Ok(Self::Raw),
            "gzip" | "gz" => Ok(Self::Gzip),
            "zstd" | "zst" => Ok(Self::Zstd),
            other => bail!("unknown chunk codec {other:?}; expected raw|gzip|zstd"),
        }
    }

    pub fn is_compressing(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd)
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
        assert_eq!(ChunkCodec::parse("none").unwrap(), ChunkCodec::Raw);
        assert!(ChunkCodec::parse("lz4").is_err());
    }
}
