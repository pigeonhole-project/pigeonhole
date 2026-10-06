/// Telegram Bot API downloads via getFile are limited to 20 MiB.
/// Keep chunks under that limit with a small safety margin.
pub const CHUNK_SIZE: usize = 19 * 1024 * 1024;

