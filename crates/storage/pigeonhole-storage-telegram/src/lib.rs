//! Telegram Bot API client and [`LegacyBlobStore`] / [`BlobBackend`] adapter.
//!
//! See crate `README.md` for `deleteMessage` age limits (sweeper grace).

mod client;
mod store;

pub use client::{Chat, ChatMember, Document, Message, TelegramClient, TgUser, DELETE_MESSAGES_MAX};
pub use pigeonhole_blob::{LegacyBlobStore, BootstrapPointer, DeleteOutcome, PinnedContent};
pub use store::{TelegramBackend, TelegramBlobStore, TelegramId};
