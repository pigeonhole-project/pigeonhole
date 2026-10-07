//! Runtime knobs for L1 block cache / metrics (binary + config).

use std::path::PathBuf;

/// Runtime knobs for L1 (and shared cache flags used by the binary).
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    pub memory_bytes: usize,
    pub disk_path: Option<PathBuf>,
    pub disk_bytes: Option<usize>,
    pub block_memory_bytes: usize,
    pub readahead_blocks: usize,
    pub write_through: bool,
    pub max_object_bytes: usize,
    pub metrics_interval_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            memory_bytes: 256 * 1024 * 1024,
            disk_path: None,
            disk_bytes: None,
            block_memory_bytes: 128 * 1024 * 1024,
            readahead_blocks: 2,
            write_through: true,
            max_object_bytes: 20 * 1024 * 1024,
            metrics_interval_secs: 300,
        }
    }
}
