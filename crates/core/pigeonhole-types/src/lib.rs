//! Shared types without I/O.

use std::ops::Range;

pub mod metric_doc;
pub mod schedule;
mod stream;
pub use metric_doc::{MetricDoc, MetricKind};
pub use stream::{bytes_stream, collect_stream, slice_range, BoxByteStream};

/// Optional byte range for blob gets.
pub type ByteRange = Range<u64>;

/// Whether the backend can honour HTTP Range on download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSupport {
    None,
    /// Accept only a verified 206 + Content-Range starting at the requested offset.
    BestEffort,
}

/// Static capabilities of a blob backend; each store crate has its own.
#[derive(Debug, Clone)]
pub struct BackendLimits {
    pub max_blob_size: usize,
    pub supports_range: RangeSupport,
}

/// Runs blocking `f` (compression, file I/O) off the async workers: on a
/// blocking-pool thread, or, on wasm, which has no threads, right here.
pub async fn blocking<F, R>(f: F) -> anyhow::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(not(target_family = "wasm"))]
    {
        Ok(tokio::task::spawn_blocking(f).await?)
    }
    #[cfg(target_family = "wasm")]
    {
        Ok(f())
    }
}
