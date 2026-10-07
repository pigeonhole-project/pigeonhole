//! Telegram Bot API client and [`BlobStore`] adapter.

mod client;
mod store;

pub use client::{Chat, ChatMember, Document, Message, TelegramClient, TgUser};
pub use pigeonhole_blob::{BlobBackend, BootstrapPointer, DeleteOutcome, PinnedContent};
pub use store::{TelegramBackend, TelegramBlobStore};
