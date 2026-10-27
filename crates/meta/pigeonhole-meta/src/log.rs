//! The tablet's command log: a [`LogStore`] that fences old generations
//! (see `docs/meta-backends.md` § LogStore contract).

use async_trait::async_trait;
use futures::stream::BoxStream;
use std::fmt;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.meta.v1.rs"));
}

pub use pb::Entry;

/// Position of an entry in the log: entries are numbered 1, 2, … in append
/// order; `0` is "before the first entry".
pub type Position = u64;

/// End of the committed log: every acknowledged entry is at or before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub position: Position,
}

/// Opaque image of the state as of some position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub data: Vec<u8>,
}

/// What a booting tablet finds in the log without any local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    /// Newest snapshot and the position it covers.
    pub snapshot: Option<(Position, Snapshot)>,
    pub head: Head,
    /// Newest claimed generation.
    pub gen: u64,
}

#[derive(Debug)]
pub enum LogError {
    /// A generation `current` ≥ the caller's has claimed the log; nothing the
    /// caller wrote in this call may be acknowledged.
    Fenced { current: u64 },
    /// Entries the caller needs are gone or corrupt (collected, truncated, a
    /// hole in the middle of the log).
    Lost(String),
    /// Infrastructure failure (I/O); the outcome of a write is unknown.
    Other(anyhow::Error),
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::Fenced { current } => write!(f, "fenced by generation {current}"),
            LogError::Lost(what) => write!(f, "log entries lost: {what}"),
            LogError::Other(e) => write!(f, "log store: {e:#}"),
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LogError::Other(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LogError {
    fn from(e: std::io::Error) -> Self {
        LogError::Other(e.into())
    }
}

impl From<anyhow::Error> for LogError {
    fn from(e: anyhow::Error) -> Self {
        LogError::Other(e)
    }
}

pub type LogResult<T> = Result<T, LogError>;

/// Entries in order with their positions.
pub type EntryStream = BoxStream<'static, LogResult<(Position, Entry)>>;

#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    /// Become generation `gen` (> any earlier claim, else `Fenced { current }`).
    /// Fences every older generation and returns the log head they left
    /// behind: every entry whose append was acknowledged to an older
    /// generation is ≤ that head.
    async fn claim(&self, gen: u64) -> LogResult<Head>;

    /// Durably append a batch for `gen`. `Ok` = committed, returns the
    /// position of the batch's last entry (the head if the batch is empty);
    /// `Fenced` = a newer generation exists, nothing about this batch may be
    /// acknowledged.
    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position>;

    /// Newest snapshot + log head (bootstrap without any local state);
    /// `None` if the log was never claimed.
    async fn discover(&self) -> LogResult<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Store a snapshot of the state as of `at` and drop entries ≤ `at` (collect).
    async fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> LogResult<()>;
}
