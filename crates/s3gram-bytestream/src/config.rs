#[derive(Clone, Debug)]
pub struct BytestreamConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub instance_name: String,
    pub max_batch_total_size_bytes: i64,
    /// TTL after last read before CAS entries are queued for GC.
    pub gc_ttl_secs: u64,
}

impl Default for BytestreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: "127.0.0.1:8980".into(),
            instance_name: "s3gram".into(),
            max_batch_total_size_bytes: 4 * 1024 * 1024,
            gc_ttl_secs: 7 * 24 * 3600,
        }
    }
}
