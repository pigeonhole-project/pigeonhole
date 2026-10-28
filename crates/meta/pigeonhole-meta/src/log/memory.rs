//! `memory` LogStore: the log in process memory (tests, throwaway runs).
//! Same contract as the others, no fsync; gone with the process.

use super::{Base, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position};
use anyhow::anyhow;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// A handle onto an in-memory log. [`Self::reopen`] gives another handle onto
/// the same log, as a new process opening it would get.
#[derive(Default)]
pub struct MemoryLogStore {
    shared: Arc<Mutex<Shared>>,
    /// The generation this handle claimed.
    owner: Mutex<Option<u64>>,
}

#[derive(Default)]
struct Shared {
    claimed: Option<u64>,
    log_id: Option<LogId>,
    base: Option<Base>,
    /// Entries after the base, in order.
    entries: Vec<(Position, Entry)>,
    head: Position,
    vote: Option<Vec<u8>>,
}

impl Shared {
    fn base_at(&self) -> Position {
        self.base.as_ref().map_or(0, |b| b.at)
    }

    fn head_term(&self) -> u64 {
        match self.entries.last() {
            Some((_, e)) => e.term,
            None => self.base.as_ref().map_or(0, |b| b.term),
        }
    }
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reopen(&self) -> Self {
        Self { shared: self.shared.clone(), owner: Mutex::new(None) }
    }

    /// The shared log, checked to be held by `gen` through this handle.
    fn owned(&self, gen: u64) -> LogResult<std::sync::MutexGuard<'_, Shared>> {
        let s = self.shared.lock().unwrap();
        let current = s.claimed.unwrap_or(0);
        if current > gen {
            return Err(LogError::Fenced { current });
        }
        if current != gen || *self.owner.lock().unwrap() != Some(gen) {
            return Err(LogError::Other(anyhow!("generation {gen} not claimed by this handle")));
        }
        Ok(s)
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        let mut s = self.shared.lock().unwrap();
        if let Some(current) = s.claimed.filter(|&c| gen <= c) {
            return Err(LogError::Fenced { current });
        }
        let log_id = *s.log_id.get_or_insert_with(LogId::random);
        s.claimed = Some(gen);
        *self.owner.lock().unwrap() = Some(gen);
        Ok(Head { log_id, position: s.head })
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        let mut s = self.owned(gen)?;
        for entry in batch {
            s.head += 1;
            let pos = s.head;
            s.entries.push((pos, entry.clone()));
        }
        Ok(s.head)
    }

    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()> {
        let mut s = self.owned(gen)?;
        if from <= s.base_at() || from > s.head + 1 {
            return Err(LogError::Other(anyhow!("truncate at {from} outside ({}, {}]", s.base_at(), s.head + 1)));
        }
        s.entries.retain(|(p, _)| *p < from);
        s.head = from - 1;
        Ok(())
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        self.owned(gen)?.vote = Some(vote.to_vec());
        Ok(())
    }

    async fn confirm(&self, gen: u64) -> LogResult<()> {
        self.owned(gen).map(|_| ())
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        let s = self.shared.lock().unwrap();
        let (Some(gen), Some(log_id)) = (s.claimed, s.log_id) else {
            return Ok(None);
        };
        Ok(Some(Discovered {
            base: s.base.clone(),
            head: Head { log_id, position: s.head },
            head_term: s.head_term(),
            vote: s.vote.clone(),
            gen,
        }))
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let s = self.shared.lock().unwrap();
        let base_at = s.base_at();
        let result = if s.log_id != Some(head.log_id) {
            Err(LogError::Lost(format!("log {} is not this one", head.log_id)))
        } else if from < base_at {
            Err(LogError::Lost(format!("entries after {from} collected up to base {base_at}")))
        } else if head.position > s.head {
            Err(LogError::Lost(format!("the log ends at {}, before {}", s.head, head.position)))
        } else {
            Ok(s.entries.iter().filter(|(p, _)| *p > from && *p <= head.position).cloned().collect::<Vec<_>>())
        };
        let items: Vec<LogResult<(Position, Entry)>> = match result {
            Ok(entries) => entries.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        };
        Box::pin(futures::stream::iter(items))
    }

    async fn rebase(&self, gen: u64, base: Base) -> LogResult<()> {
        let mut s = self.owned(gen)?;
        let old = s.base_at();
        if base.at < old {
            return Err(LogError::Other(anyhow!("rebase at {} behind the base {old}", base.at)));
        }
        let at = base.at;
        s.entries.retain(|(p, _)| *p > at);
        s.head = s.head.max(at);
        s.base = Some(base);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passes_the_logstore_conformance_suite() {
        crate::log::conformance::run_all(|| {
            let log = Arc::new(MemoryLogStore::new());
            move || {
                let log = log.clone();
                async move { Arc::new(log.reopen()) as Arc<dyn LogStore> }
            }
        })
        .await;
    }
}
