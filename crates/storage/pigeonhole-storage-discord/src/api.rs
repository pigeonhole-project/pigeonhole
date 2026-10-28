//! The few operations on one Discord channel the backend, the log and the
//! snapshots need, as a trait: [`crate::DiscordChannel`] talks HTTP,
//! [`crate::fake`] is an in-memory channel for tests.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// Most message ids one bulk delete takes, and messages one listing returns.
pub const PAGE: usize = 100;

/// A message as far as we care.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DcMessage {
    pub id: u64,
    /// Discord's message type (0 = default, 18 = thread created, …).
    pub kind: u8,
    pub content: String,
    pub attachment: Option<DcAttachment>,
    /// The thread started from this message, if any (its id is the message's).
    pub thread: Option<String>,
}

/// A message's first attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DcAttachment {
    pub filename: String,
    pub size: u64,
    /// Signed CDN URL; it expires (a fetch of the message brings a fresh one).
    pub url: String,
}

/// What starting a thread from a message did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadStart {
    Created,
    /// The message already has a thread (code 160004).
    Taken,
    /// No such message.
    NoMessage,
}

/// A CDN URL that no longer works (403 / 404): fetch the message again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdnStale;

impl std::fmt::Display for CdnStale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("attachment URL expired")
    }
}

impl std::error::Error for CdnStale {}

/// One channel of a guild.
#[async_trait]
pub trait DiscordApi: Send + Sync + 'static {
    fn channel_id(&self) -> u64;

    /// Post `content`, with `file` attached if given. With one `nonce`, a
    /// repeat within a few minutes returns the first message instead of
    /// posting again (`enforce_nonce`; the nonce is per bot, across channels).
    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage>;

    /// Message `id`; `None` if it does not exist.
    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>>;

    /// Up to `limit` (≤ [`PAGE`]) messages strictly between `after` and
    /// `before`, **ascending** by id: the newest ones when only `before` (or
    /// nothing) is given, the oldest ones after `after` otherwise.
    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>>;

    /// Delete messages; missing ones are fine.
    async fn delete_messages(&self, ids: &[u64]) -> Result<()>;

    /// Start a thread named `name` from message `message_id` (at most one
    /// per message, ever: [`ThreadStart::Taken`] after).
    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart>;

    /// The name of the thread started from message `message_id`, if any;
    /// readable after the message is deleted (a thread outlives its starter).
    async fn thread_name(&self, message_id: u64) -> Result<Option<String>>;

    /// Archive the thread of message `message_id` (it still blocks a new one).
    async fn archive_thread(&self, message_id: u64) -> Result<()>;

    /// Delete the thread of message `message_id`; a missing one is fine.
    /// Only safe once the message is gone: on an existing message a new
    /// thread could be started again.
    async fn delete_thread(&self, message_id: u64) -> Result<()>;

    /// Bytes `[start, end)` of an attachment (all of it for `None`);
    /// [`CdnStale`] when the URL expired.
    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes>;
}
