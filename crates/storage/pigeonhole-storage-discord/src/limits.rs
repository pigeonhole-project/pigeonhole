//! Discord's classes of calls for the shared limiter, and their default
//! rates (docs/shared-limiter.md).

use pigeonhole_limits::{ChatLimiterConfig, Class, Rate};

/// Creating a message or a thread: 5 per 5 s per bot in a channel
/// (measured: docs/shared-limiter.md, § Spike findings).
pub const SEND: Class = Class("send");
/// Reading a channel, its messages, a thread.
pub const READ: Class = Class("read");
/// Deleting messages and threads, archiving threads.
pub const DELETE: Class = Class("delete");

/// The defaults: ~50 requests a second per bot.
pub fn limits() -> ChatLimiterConfig {
    ChatLimiterConfig {
        bot: Rate::new(40.0, 50.0),
        classes: vec![(SEND, Rate::new(1.0, 5.0)), (READ, Rate::new(5.0, 10.0)), (DELETE, Rate::new(1.0, 5.0))],
        upload_concurrency: 2,
        download_concurrency: 8,
    }
}
