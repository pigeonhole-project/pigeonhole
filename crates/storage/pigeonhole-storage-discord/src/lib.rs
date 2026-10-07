//! Discord channel blob backend (Stage 4).

mod client;
mod store;

pub use client::{
    snowflake_bulk_deletable, snowflake_timestamp_ms, DiscordClient, RateBudget, DISCORD_EPOCH_MS,
};
pub use pigeonhole_blob::{
    LegacyBlobStore, BootstrapPointer, DeleteOutcome, PinnedContent, Sweepable, BlobBackend,
    TypedBootstrapPointer,
};
pub use store::{
    discord_fingerprint, discord_location, DiscordBackend, DiscordBlobStore, DiscordId,
};
