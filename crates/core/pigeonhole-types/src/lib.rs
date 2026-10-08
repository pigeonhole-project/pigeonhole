//! Shared types without I/O.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Range;

/// Result of deleting a backend message/attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// Already absent — treat as success.
    Gone,
    /// Transient or policy failure — retry later.
    Failed,
}

/// Stable backend identity, e.g. `tg:-100123` or `discord:987`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackendId(pub String);

impl BackendId {
    pub fn new(kind: &str, scope: &str) -> Self {
        Self(format!("{kind}:{scope}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn telegram(scope: &str) -> Self {
        Self::new("tg", scope)
    }

    pub fn discord(scope: &str) -> Self {
        Self::new("discord", scope)
    }

    pub fn memory() -> Self {
        Self::new("memory", "local")
    }
}

impl fmt::Display for BackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for BackendId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Immutable cache key used by block caches (L1/L2).
///
/// Opaque `key` is backend-agnostic (e.g. `chunk-{id}`); physical blob
/// addresses live in the blob layer (`BlobLocator`), not here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobKey {
    pub backend_id: BackendId,
    pub key: String,
}

impl BlobKey {
    pub fn new(backend_id: BackendId, key: impl Into<String>) -> Self {
        Self {
            backend_id,
            key: key.into(),
        }
    }
}

/// Optional byte range for blob gets.
pub type ByteRange = Range<u64>;

/// Whether the backend can honour HTTP Range on download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSupport {
    None,
    /// Accept only a verified 206 + Content-Range starting at the requested offset.
    BestEffort,
}

/// Static capabilities of a blob backend.
#[derive(Debug, Clone)]
pub struct BackendLimits {
    pub max_blob_size: usize,
    pub supports_range: RangeSupport,
    pub can_list: bool,
}

impl BackendLimits {
    pub fn telegram() -> Self {
        Self {
            // Bot API getFile hard cap is 20 MiB; keep the existing on-wire margin.
            max_blob_size: 20 * 1024 * 1024 - 1,
            supports_range: RangeSupport::BestEffort,
            can_list: false,
        }
    }

    pub fn memory() -> Self {
        Self {
            max_blob_size: 20 * 1024 * 1024 - 1,
            supports_range: RangeSupport::BestEffort,
            can_list: false,
        }
    }

    /// Discord attachment upload cap (~10 MiB) with on-wire margin.
    pub fn discord(max_blob_size: Option<usize>) -> Self {
        const DEFAULT: usize = 10 * 1024 * 1024 - 4096;
        Self {
            max_blob_size: max_blob_size.unwrap_or(DEFAULT),
            supports_range: RangeSupport::BestEffort,
            can_list: true,
        }
    }
}
