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

/// Content of a chat/channel bootstrap pin (manifest pointer).
#[derive(Debug, Clone)]
pub enum PinnedContent {
    Text { message_id: i64, text: String },
    Document { message_id: i64, file_id: String },
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

    pub fn telegram(chat_id: &str) -> Self {
        Self::new("tg", chat_id)
    }

    pub fn discord(channel_id: &str) -> Self {
        Self::new("discord", channel_id)
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

/// Opaque serializable blob locator for a specific backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Locator {
    Telegram {
        file_id: String,
        message_id: i64,
    },
    Memory {
        file_id: String,
        message_id: i64,
    },
    Discord {
        channel_id: String,
        message_id: i64,
        attachment_id: String,
        url: String,
    },
    /// Forward-compatible opaque JSON for unknown backends / snapshots.
    Other(serde_json::Value),
}

impl Locator {
    pub fn telegram(file_id: impl Into<String>, message_id: i64) -> Self {
        Self::Telegram {
            file_id: file_id.into(),
            message_id,
        }
    }

    pub fn memory(file_id: impl Into<String>, message_id: i64) -> Self {
        Self::Memory {
            file_id: file_id.into(),
            message_id,
        }
    }

    pub fn discord(
        channel_id: impl Into<String>,
        message_id: i64,
        attachment_id: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        Self::Discord {
            channel_id: channel_id.into(),
            message_id,
            attachment_id: attachment_id.into(),
            url: url.into(),
        }
    }

    pub fn file_id(&self) -> Option<&str> {
        match self {
            Self::Telegram { file_id, .. } | Self::Memory { file_id, .. } => Some(file_id),
            Self::Discord { attachment_id, .. } => Some(attachment_id),
            Self::Other(v) => v.get("file_id").and_then(|x| x.as_str()),
        }
    }

    pub fn message_id(&self) -> Option<i64> {
        match self {
            Self::Telegram { message_id, .. } | Self::Memory { message_id, .. } => Some(*message_id),
            Self::Discord { message_id, .. } => Some(*message_id),
            Self::Other(v) => v.get("message_id").and_then(|x| x.as_i64()),
        }
    }

    /// Legacy [`LegacyBlobStore::get`] key: `{message_id}:{attachment_id}`.
    pub fn discord_store_file_id(message_id: i64, attachment_id: &str) -> String {
        format!("{message_id}:{attachment_id}")
    }

    /// Parse a Discord store file id `{message_id}:{attachment_id}` (both snowflakes).
    pub fn parse_discord_store_file_id(file_id: &str) -> Option<(i64, &str)> {
        let (mid, aid) = file_id.split_once(':')?;
        let mid = mid.parse().ok()?;
        // Attachment ids are Discord snowflakes (decimal). Reject other `:` shapes
        // so Telegram file_ids are never misclassified.
        if aid.is_empty() || !aid.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        Some((mid, aid))
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

/// Immutable blob identity used by caches (Stage 3.5+) and replica tables.
///
/// Today: `(backend_id, locator)`. Later: content-addressed `(sha256, size)`
/// without changing cache key surfaces that store [`BlobKey`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobKey {
    pub backend_id: BackendId,
    pub locator: Locator,
}

impl BlobKey {
    pub fn new(backend_id: BackendId, locator: Locator) -> Self {
        Self {
            backend_id,
            locator,
        }
    }
}

/// Optional byte range for [`crate`]-adjacent blob gets.
pub type ByteRange = Range<u64>;

/// Hints passed to backend `put` (filename / caption for Telegram documents).
#[derive(Debug, Clone, Default)]
pub struct PutHint {
    pub filename: String,
    pub caption: String,
}

impl PutHint {
    pub fn new(filename: impl Into<String>, caption: impl Into<String>) -> Self {
        Self {
            filename: filename.into(),
            caption: caption.into(),
        }
    }
}

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
