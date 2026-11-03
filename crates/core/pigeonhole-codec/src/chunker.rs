//! Chunk sizes and codecs.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Default `chunk.size`: most stored (on-wire) bytes in a chunk. Under
/// Discord's 20 MiB attachments, so a chunk is one message there too.
pub const DEFAULT_CHUNK_SIZE: usize = 19 * 1024 * 1024;
/// How far `chunk.block_size` stays under every store's message size: a
/// block is one message's worth at most, and its stored bytes never exceed
/// its logical ones (a block is stored raw unless compression shrinks it).
/// So this is headroom, not a header: it keeps a block size that equals a
/// store's limit out, whose parts would hold one block each.
pub const BLOCK_HEADROOM: usize = 64;
/// Most logical bytes in a chunk, and so the largest `chunk.size`.
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

/// A store whose messages are smaller takes a chunk in several, not an error.
pub fn validate_chunk_size(n: usize) -> Result<()> {
    if n == 0 {
        bail!("chunk.size must be >= 1");
    }
    if n > MAX_LOGICAL_CHUNK {
        bail!("chunk.size={n} exceeds the largest chunk ({MAX_LOGICAL_CHUNK} bytes, 256 MiB)");
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
    fn takes_up_to_256_mib() {
        validate_chunk_size(MAX_LOGICAL_CHUNK).unwrap();
        assert!(validate_chunk_size(MAX_LOGICAL_CHUNK + 1).is_err());
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
