use crate::chunker::ChunkCodec;
use crate::frames::FrameRecord;
use anyhow::{Context, Result};

/// One uploaded backend document (object/multipart part slice).
#[derive(Debug, Clone)]
pub struct UploadedChunk {
    pub part_no: i64,
    pub file_id: String,
    pub message_id: i64,
    pub logical_size: i64,
    pub codec: ChunkCodec,
    pub frames: Vec<FrameRecord>,
    /// CRC32 of on-wire (stored) bytes; filled at upload for L2 integrity.
    pub stored_crc32: Option<u32>,
}

pub fn codec_to_sql(c: ChunkCodec) -> &'static str {
    c.as_str()
}

pub fn codec_from_sql(s: &str) -> Result<ChunkCodec> {
    match s {
        "0" | "false" => Ok(ChunkCodec::Raw),
        "1" | "true" => Ok(ChunkCodec::Gzip),
        other => ChunkCodec::parse(other)
            .with_context(|| format!("invalid chunk codec in index: {other:?}")),
    }
}
