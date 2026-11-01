//! Discord over HTTP: the [`DiscordApi`](pigeonhole_storage_discord::DiscordApi)
//! the Discord store runs on. [`DiscordClient`] is one bot, [`DiscordChannel`]
//! one channel through it, [`PooledDiscord`] one channel through several bots.

mod client;
mod pacer;
mod pool;

pub use client::{DiscordChannel, DiscordClient};
pub use pool::{pooled, PooledDiscord};
