//! Telegram's classes of calls for the shared limiter, and their default
//! rates (docs/shared-limiter.md).

use pigeonhole_limits::{ChatLimiterConfig, Class, Rate};

/// `messages.sendMedia` / `sendMessage`: ~20 messages a minute per bot in a
/// channel (measured: docs/shared-limiter.md, § Spike findings).
pub const SEND: Class = Class("send");
/// `channels.getMessages`: reads by id (the log's fence checks, listings).
pub const READ: Class = Class("read");
/// `channels.deleteMessages`.
pub const DELETE: Class = Class("delete");

/// The defaults: a bot ~30 calls a second; sends at 18 a minute, under the
/// channel's ~20.
pub fn limits() -> ChatLimiterConfig {
    ChatLimiterConfig {
        bot: Rate::new(25.0, 30.0),
        classes: vec![(SEND, Rate::new(0.3, 3.0)), (READ, Rate::new(2.0, 5.0)), (DELETE, Rate::new(1.0, 5.0))],
        upload_concurrency: 2,
        download_concurrency: 8,
    }
}
