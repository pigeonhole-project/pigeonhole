//! Shared blob stream helpers.

use anyhow::Result;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use futures::Stream;
use pigeonhole_types::ByteRange;
use std::pin::Pin;

/// Stream of blob bytes returned by [`crate::BlobBackend::get`] / [`crate::DynBlobBackend::get`].
pub type BoxByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send>>;

/// Collect a [`BoxByteStream`] into a single [`Bytes`].
pub async fn collect_stream(mut stream: BoxByteStream) -> Result<Bytes> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(out))
}

/// One-shot stream wrapping already-buffered bytes (Memory / full Telegram get).
pub fn bytes_stream(data: Bytes) -> BoxByteStream {
    Box::pin(stream::once(async move { Ok(data) }))
}

/// Apply an optional byte range to a full buffer (local fall-back when backend
/// Range is unsupported or rejected).
pub fn slice_range(data: Bytes, range: Option<ByteRange>) -> Result<Bytes> {
    let Some(r) = range else {
        return Ok(data);
    };
    let start = r.start as usize;
    let end = r.end as usize;
    if start > end || end > data.len() {
        anyhow::bail!(
            "byte range {}..{} outside blob of {} bytes",
            r.start,
            r.end,
            data.len()
        );
    }
    Ok(data.slice(start..end))
}
