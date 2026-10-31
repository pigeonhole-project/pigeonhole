//! Discord snowflakes: a message id carries the time it was made.

use std::time::Duration;

/// Discord snowflake epoch (2015-01-01T00:00:00.000Z), milliseconds.
pub const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// Bulk-delete only accepts messages younger than 14 days (API error 50034).
const BULK_DELETE_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
/// Safety margin so we do not race the 14-day cutoff.
const BULK_DELETE_AGE_SLACK: Duration = Duration::from_secs(60);

/// Milliseconds since Unix epoch encoded in a Discord snowflake.
pub fn snowflake_timestamp_ms(id: u64) -> u64 {
    (id >> 22) + DISCORD_EPOCH_MS
}

/// Whether Discord's bulk-delete endpoint will accept this message id by age.
pub fn snowflake_bulk_deletable(id: u64, unix_now_ms: u64) -> bool {
    let age_ms = unix_now_ms.saturating_sub(snowflake_timestamp_ms(id));
    age_ms < BULK_DELETE_MAX_AGE.saturating_sub(BULK_DELETE_AGE_SLACK).as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_deletable_by_snowflake_age() {
        let now_ms = DISCORD_EPOCH_MS + 30 * 24 * 60 * 60 * 1000;
        let young_id = (now_ms - 2 * 24 * 60 * 60 * 1000 - DISCORD_EPOCH_MS) << 22;
        let old_id = (now_ms - 20 * 24 * 60 * 60 * 1000 - DISCORD_EPOCH_MS) << 22;
        assert!(snowflake_bulk_deletable(young_id, now_ms));
        assert!(!snowflake_bulk_deletable(old_id, now_ms));
    }
}
