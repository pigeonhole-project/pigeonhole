//! Discord channel blob backend (Stage 4).

mod client;
mod store;

pub use client::{
    snowflake_bulk_deletable, snowflake_timestamp_ms, DiscordClient, RateBudget, DISCORD_EPOCH_MS,
};
pub use pigeonhole_blob::{
    DeleteOutcome, Sweepable, BlobBackend,
};
pub use store::{
    discord_fingerprint, discord_location, DiscordBackend, DiscordBlobStore, DiscordId,
};
