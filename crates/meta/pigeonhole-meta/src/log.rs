//! The tablet's command log: a [`LogStore`] that fences old generations
//! (see `docs/meta-backends.md` § LogStore contract).

use crate::snapshot::SnapshotRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::fmt;

pub mod chat;
#[cfg(any(test, feature = "testsuite"))]
pub mod conformance;
mod local;
mod memory;

pub use local::{LocalLogStore, DEFAULT_SEGMENT_BYTES};
pub use memory::MemoryLogStore;

/// Version of the log envelope ([`Batch`], [`Entry`]). Bump it on any change
/// to their encoding: a binary refuses to read batches of another format.
pub const LOG_FORMAT: u32 = 3;

/// One log entry: the Raft term it was written in and an opaque payload (a
/// tablet command; later an openraft entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: u64,
    pub payload: Vec<u8>,
}

/// One LogStore write. Entries get consecutive positions starting at
/// `first`, replacing any the log held at `first` and after (a truncate is
/// a batch with `first` ≤ the head and no entries). An empty batch at
/// `head + 1` changes nothing: the zero entry, a barrier, a vote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    /// [`LOG_FORMAT`]; first, so it can be read before the rest.
    pub format: u32,
    pub gen: u64,
    pub first: Position,
    pub entries: Vec<Entry>,
}

impl Batch {
    pub fn new(gen: u64, first: Position, entries: Vec<Entry>) -> Self {
        Self { format: LOG_FORMAT, gen, first, entries }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a batch always encodes")
    }

    /// `Lost` for a batch of another [`LOG_FORMAT`] or one that does not parse.
    pub fn decode(data: &[u8]) -> LogResult<Self> {
        let (format, _) = postcard::take_from_bytes::<u32>(data)
            .map_err(|e| LogError::Lost(format!("batch header: {e}")))?;
        if format != LOG_FORMAT {
            return Err(LogError::Lost(format!(
                "batch in log format {format}, this binary reads {LOG_FORMAT}"
            )));
        }
        postcard::from_bytes(data).map_err(|e| LogError::Lost(format!("batch: {e}")))
    }
}

/// Position of an entry in the log: entries are numbered 1, 2, … in append
/// order; `0` is "before the first entry".
pub type Position = u64;

/// Identity of one log history, fixed when the log is first claimed. A local
/// state applied up to some position is only meaningful for the same log: a
/// recreated log reuses positions for different entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId(pub u128);

impl LogId {
    pub fn random() -> Self {
        LogId(uuid::Uuid::new_v4().as_u128())
    }
}

impl fmt::Display for LogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl std::str::FromStr for LogId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u128::from_str_radix(s.trim(), 16).map(LogId)
    }
}

/// End of the committed log: every acknowledged entry is at or before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub log_id: LogId,
    pub position: Position,
}

/// Where the log starts: the state as of `at` is the stored snapshot
/// `snapshot`; entries ≤ `at` may be gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Base {
    pub at: Position,
    /// Term of the entry at `at` (0 for position 0).
    pub term: u64,
    /// Node that led that term (Raft's log id is term + leader + index).
    pub leader: u64,
    pub snapshot: SnapshotRef,
}

/// What a booting tablet finds in the log without any local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    /// `None`: the log starts at position 0 from empty states.
    pub base: Option<Base>,
    pub head: Head,
    /// Term of the entry at the head (the base's if the head is the base).
    pub head_term: u64,
    /// The last vote saved ([`LogStore::save_vote`]), opaque.
    pub vote: Option<Vec<u8>>,
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

    /// Durably append a batch for `gen` after the head. `Ok` = committed,
    /// returns the position of the batch's last entry (the head if the batch
    /// is empty); `Fenced` = a newer generation exists, nothing about this
    /// batch may be acknowledged.
    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position>;

    /// Durably drop the entries at `from` and after (a divergent tail);
    /// `base < from <= head + 1`. The next append continues at `from`.
    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()>;

    /// Durably store the Raft vote (opaque to the log).
    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()>;

    /// `Ok` if `gen` still holds the log, without writing (a lease check
    /// before side effects); `Fenced` otherwise.
    async fn confirm(&self, gen: u64) -> LogResult<()>;

    /// Base + log head (bootstrap without any local state); `None` if the
    /// log was never claimed.
    async fn discover(&self) -> LogResult<Option<Discovered>>;

    /// Entries after `from` up to `head`, in order (replay).
    fn read(&self, from: Position, head: Head) -> EntryStream;

    /// Start the log from `base` (a snapshot already stored) and drop
    /// entries ≤ `base.at` (collect). `base.at` may be past the head (a
    /// snapshot installed from elsewhere): the log is then empty up to it.
    async fn rebase(&self, gen: u64, base: Base) -> LogResult<()>;
}
