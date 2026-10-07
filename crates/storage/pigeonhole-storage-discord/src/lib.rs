//! Discord channel blob backend (Stage 4).

mod client;
mod store;

pub use client::DiscordClient;
pub use pigeonhole_blob::{BlobBackend, BootstrapPointer, DeleteOutcome, PinnedContent};
pub use store::{DiscordBackend, DiscordBlobStore};
