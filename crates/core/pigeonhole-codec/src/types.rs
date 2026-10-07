use crate::chunker::ChunkCodec;
use anyhow::{Context, Result};

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
