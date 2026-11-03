//! Telegram storage, logged in as a bot: a [`BlobBackend`] over one channel,
//! and the channel operations the metadata log and snapshots use, all over a
//! [`ChannelApi`]. The MTProto client that implements it lives in
//! `pigeonhole-telegram-mtproto`; [`fake`] (feature `fake`) is an in-memory
//! channel for tests and the site's demo.
//!
//! Unlike the Bot API, a bot here may read any message of a channel by id
//! (`channels.getMessages`), so a channel can be listed (ids are dense) and a
//! blob is just its message id; files are not capped at 20 MiB and are read
//! in exact windows. Only channels and supergroups: their message ids are
//! per chat.

mod chain;
mod channel;
#[cfg(any(test, feature = "fake"))]
pub mod fake;
pub mod limits;
pub mod pool;
mod store;

pub use chain::ChainOptions;
pub use channel::{
    windows, ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, Sent, IDS_PER_CALL, TEXT_MAX_CHARS, WINDOW,
};
pub use limits::limits;
pub use pigeonhole_storage_api::BlobBackend;
pub use store::{blob_random_id, TelegramBlobStore, TelegramId};

/// What [`InstanceInfo::kind`](pigeonhole_storage_api::InstanceInfo::kind) says of this store.
pub const KIND: &str = "telegram";

/// MTProto: a bot posts files far larger than any chunk (256 MiB, the
/// codec's `MAX_LOGICAL_CHUNK`) and reads any 4 KiB-aligned window.
pub const BLOB_LIMITS: pigeonhole_types::BackendLimits = pigeonhole_types::BackendLimits {
    max_blob_size: 256 * 1024 * 1024,
    supports_range: pigeonhole_types::RangeSupport::BestEffort,
};
