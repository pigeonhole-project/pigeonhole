//! Block cache knobs (binary + config).

/// L1 holds decoded blocks, L2 stored (compressed) ones; a read looks in L1,
/// then L2, then the backend.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    /// L1 capacity (decoded bytes).
    pub l1_bytes: usize,
    /// L2 capacity (stored bytes); `None` = no L2.
    pub l2_bytes: Option<usize>,
    /// Chunks written put their stored blocks into L2.
    pub write_through: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            l1_bytes: 128 * 1024 * 1024,
            l2_bytes: Some(256 * 1024 * 1024),
            write_through: true,
        }
    }
}
