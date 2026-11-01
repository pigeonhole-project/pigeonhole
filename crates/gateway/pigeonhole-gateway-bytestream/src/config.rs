use pigeonhole_types::schedule::{Expiry, Schedule};
use std::time::Duration;

/// The schedules of the server's GC loops.
#[derive(Debug)]
pub struct Gc {
    /// The CAS GC: entries not read for its TTL are released.
    pub cas: Schedule<Expiry>,
    /// Pause between passes over incomplete uploads (`None`: only when asked).
    pub uploads: Schedule<Option<Duration>>,
}

impl Gc {
    /// Knobs nothing changes.
    pub fn fixed(cas: Expiry, uploads: Option<Duration>) -> Self {
        Self { cas: Schedule::fixed(cas), uploads: Schedule::fixed(uploads) }
    }

    /// No GC (tests).
    pub fn off() -> Self {
        Self::fixed(Expiry { every: None, ttl: None }, None)
    }
}

#[derive(Clone, Debug)]
pub struct BytestreamConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub instance_name: String,
    pub max_batch_total_size_bytes: i64,
    /// Incomplete ByteStream uploads older than this are dropped.
    pub upload_ttl_secs: u64,
}

impl Default for BytestreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: "127.0.0.1:8980".into(),
            instance_name: "pigeonhole".into(),
            max_batch_total_size_bytes: 4 * 1024 * 1024,
            upload_ttl_secs: 3600,
        }
    }
}
