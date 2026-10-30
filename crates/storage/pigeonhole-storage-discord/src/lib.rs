//! Discord storage: a [`BlobBackend`] over one channel of a guild, and the
//! [`Chain`] the metadata log and snapshots stand on (threads as
//! compare-and-set, docs/discord-log.md).
//!
//! The API is a trait ([`DiscordApi`]): [`DiscordChannel`] talks HTTP,
//! [`fake`] is an in-memory channel for tests. A blob is a message: small
//! ones as text ([`pigeonhole_storage_api::text`]), the rest as an attachment.

mod api;
mod chain;
mod client;
pub mod limits;
pub mod fake;
mod pacer;
mod store;

pub use api::{CdnStale, DcAttachment, DcMessage, DiscordApi, ThreadStart, CONTENT_MAX_CHARS, PAGE};
pub use client::{snowflake_bulk_deletable, snowflake_timestamp_ms, DiscordChannel, DiscordClient, DISCORD_EPOCH_MS};
pub use limits::limits;
pub use pigeonhole_storage_api::{BlobBackend, Chain, LeaseChain};
pub use store::{blob_nonce, discord_fingerprint, discord_location, DiscordBlobStore, DiscordId};

/// What [`InstanceInfo::kind`](pigeonhole_storage_api::InstanceInfo::kind) says of this store.
pub const KIND: &str = "discord";
