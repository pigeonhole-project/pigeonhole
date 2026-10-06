//! Telegram getFile is limited to 20 MiB; object/snapshot chunks must stay below that.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Hard ceiling: stored payload must be downloadable via getFile (< 20 MiB).
pub const MAX_CHUNK_SIZE: usize = 20 * 1024 * 1024 - 1;
/// Default logical chunk size (safety margin under the getFile limit).
pub const DEFAULT_CHUNK_SIZE: usize = 19 * 1024 * 1024;

/// How new object chunks are encoded before `sendDocument`.
///
/// Stored per-chunk codec may still be `raw` under `Gzip` when gzip does not shrink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChunkCodec {
    /// Store bytes as-is (no compression attempt).
    Raw,
    /// Opportunistic gzip: keep gzip only when strictly smaller than raw.
    #[default]
    Gzip,
}

impl ChunkCodec {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Gzip => "gzip",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "raw" | "none" | "off" => Ok(Self::Raw),
            "gzip" | "gz" => Ok(Self::Gzip),
            other => bail!("unknown chunk codec {other:?}; expected raw|gzip"),
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
        assert_eq!(ChunkCodec::parse("none").unwrap(), ChunkCodec::Raw);
        assert!(ChunkCodec::parse("zstd").is_err());
    }
}
