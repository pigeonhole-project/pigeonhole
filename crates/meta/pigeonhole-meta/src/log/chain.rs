//! The [`LogStore`] for every store that implements [`Chain`]
//! (docs/chain-log.md).
//!
//! The log is a chain of blobs. Two kinds, told apart by label:
//!
//! - `log-claim-<gen>`: generation `gen` claims the log;
//! - `log-batch-<gen>`: a [`Batch`] written by `gen`.
//!
//! Every blob carries the log's registers (id, base, vote, head term, head
//! position) as of itself and a link (id + crc) to its predecessor, so the
//! tail alone tells where the log stands and replay walks back from it.
//!
//! The fence is the chain: a writer puts its next blob after its own last
//! one, and the store lets exactly one successor win. A claim goes after
//! the tail; from then on every older writer's next put names a blob that
//! already has a successor, and loses. Generations along the chain only grow
//! (a claim links only after a tail of a lower generation), so the tail's
//! generation is the newest claim and a loser learns `current` from the
//! winner's label.

use super::{Base, Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position, LOG_FORMAT};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_storage_api::{collect_stream, After, BlobBackend, Chain};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Claim,
    Batch,
}

fn label(kind: Kind, gen: u64) -> String {
    match kind {
        Kind::Claim => format!("log-claim-{gen}"),
        Kind::Batch => format!("log-batch-{gen}"),
    }
}

/// The generation a label names; `None` for a blob that is not ours.
fn label_gen(label: &str) -> Option<u64> {
    let rest = label.strip_prefix("log-claim-").or_else(|| label.strip_prefix("log-batch-"))?;
    rest.parse().ok()
}

/// Link from a blob to its predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Prev<Id> {
    id: Id,
    /// crc32 of the predecessor: a replaced blob is detected.
    crc: u32,
}

/// One chain blob: a batch (empty for a claim) plus the registers as of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ChainDoc<Id> {
    format: u32,
    log_id: LogId,
    gen: u64,
    claim: bool,
    prev: Option<Prev<Id>>,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
    /// Term of the entry at the head once this blob is the tail.
    head_term: u64,
    batch: Batch,
}

impl<Id: Serialize + for<'de> Deserialize<'de>> ChainDoc<Id> {
    fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a log blob always encodes")
    }

    fn decode(data: &[u8]) -> LogResult<Self> {
        let (format, _) = postcard::take_from_bytes::<u32>(data)
            .map_err(|e| LogError::Lost(format!("log blob header: {e}")))?;
        if format != LOG_FORMAT {
            return Err(LogError::Lost(format!("log blob in format {format}, this binary reads {LOG_FORMAT}")));
        }
        postcard::from_bytes(data).map_err(|e| LogError::Lost(format!("log blob: {e}")))
    }
}

impl<Id> ChainDoc<Id> {
    fn head_position(&self) -> Position {
        self.batch.first - 1 + self.batch.entries.len() as u64
    }
}

/// The log as of one chain blob (its registers).
#[derive(Debug, Clone)]
struct Tip<Id> {
    at: Option<Prev<Id>>,
    position: Position,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
    head_term: u64,
}

impl<Id> Default for Tip<Id> {
    fn default() -> Self {
        Self { at: None, position: 0, base: None, vote: None, head_term: 0 }
    }
}

impl<Id: Clone> Tip<Id> {
    fn of(doc: &ChainDoc<Id>, at: Prev<Id>) -> Self {
        Self {
            position: doc.head_position(),
            base: doc.base.clone(),
            vote: doc.vote.clone(),
            head_term: doc.head_term,
            at: Some(at),
        }
    }

    fn base_at(&self) -> Position {
        self.base.as_ref().map_or(0, |b| b.at)
    }
}

/// Why an owner can no longer write.
#[derive(Debug, Clone)]
enum Failed {
    Fenced(u64),
    Broken(String),
}

struct Owner<Id> {
    gen: u64,
    log_id: LogId,
    tip: Tip<Id>,
    failed: Option<Failed>,
}

struct State<Id> {
    owner: Option<Owner<Id>>,
    /// What the last `discover` found (reads by a handle that did not claim).
    seen: Option<(LogId, Tip<Id>)>,
}

impl<Id> Default for State<Id> {
    fn default() -> Self {
        Self { owner: None, seen: None }
    }
}

/// One write: a batch at `first` and the registers that change with it.
struct Write {
    first: Position,
    entries: Vec<Entry>,
    head_term: u64,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
}

pub struct ChainLogStore<C: Chain> {
    chain: Arc<C>,
    state: Mutex<State<C::Id>>,
}

impl<C: Chain> ChainLogStore<C> {
    pub fn new(chain: Arc<C>) -> Self {
        Self { chain, state: Mutex::default() }
    }

    /// This handle's tip, if it holds generation `gen`.
    fn owned(&self, gen: u64) -> LogResult<(LogId, Tip<C::Id>)> {
        let state = self.state.lock().unwrap();
        let o = state
            .owner
            .as_ref()
            .filter(|o| o.gen == gen)
            .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;
        match &o.failed {
            Some(Failed::Fenced(current)) => Err(LogError::Fenced { current: *current }),
            Some(Failed::Broken(why)) => Err(LogError::Other(anyhow!("the log handle failed earlier: {why}"))),
            None => Ok((o.log_id, o.tip.clone())),
        }
    }

    fn fail(&self, failed: Failed) {
        if let Some(o) = self.state.lock().unwrap().owner.as_mut() {
            o.failed.get_or_insert(failed);
        }
    }

    fn reader(&self) -> Reader<C> {
        Reader { chain: self.chain.clone() }
    }

    /// The fence a lost link means: the winner's generation.
    fn fenced_by(winner_label: &str, gen: u64) -> LogError {
        match label_gen(winner_label) {
            Some(current) if current > gen => LogError::Fenced { current },
            _ => LogError::Lost(format!("blob {winner_label:?} took the place of generation {gen}'s")),
        }
    }

    /// Put `w` as generation `gen` after the owner's tip.
    async fn write(&self, gen: u64, w: Write) -> LogResult<Tip<C::Id>> {
        let (log_id, tip) = self.owned(gen)?;
        let doc = ChainDoc {
            format: LOG_FORMAT,
            log_id,
            gen,
            claim: false,
            prev: tip.at.clone(),
            base: w.base.or_else(|| tip.base.clone()),
            vote: w.vote.or_else(|| tip.vote.clone()),
            head_term: w.head_term,
            batch: Batch::new(gen, w.first, w.entries),
        };
        let bytes = doc.encode();
        let crc = crc32fast::hash(&bytes);
        let prev = tip.at.as_ref().map(|p| &p.id);
        match self.chain.put_after(prev, &label(Kind::Batch, gen), Bytes::from(bytes)).await {
            Ok(After::Won(id)) => {
                let tip = Tip::of(&doc, Prev { id, crc });
                if let Some(o) = self.state.lock().unwrap().owner.as_mut().filter(|o| o.gen == gen) {
                    o.tip = tip.clone();
                }
                Ok(tip)
            }
            Ok(After::Lost(winner)) => {
                let e = Self::fenced_by(&winner.label, gen);
                self.fail(match &e {
                    LogError::Fenced { current } => Failed::Fenced(*current),
                    other => Failed::Broken(other.to_string()),
                });
                Err(e)
            }
            Err(e) => {
                // It may still land and win: this handle no longer knows its tip.
                self.fail(Failed::Broken(format!("{e:#}")));
                Err(LogError::Other(e.context("put a log batch")))
            }
        }
    }
}

#[async_trait]
impl<C: Chain> LogStore for ChainLogStore<C> {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        loop {
            let (log_id, tip, tail_gen) = match self.chain.tail().await? {
                None => (LogId::random(), Tip::default(), 0),
                Some(tail) => {
                    let (doc, at) = self.reader().fetch(&tail.id).await?;
                    (doc.log_id, Tip::of(&doc, at), doc.gen)
                }
            };
            if tail_gen >= gen {
                return Err(LogError::Fenced { current: tail_gen });
            }
            let doc = ChainDoc {
                format: LOG_FORMAT,
                log_id,
                gen,
                claim: true,
                prev: tip.at.clone(),
                base: tip.base.clone(),
                vote: tip.vote.clone(),
                head_term: tip.head_term,
                batch: Batch::new(gen, tip.position + 1, Vec::new()),
            };
            let bytes = doc.encode();
            let crc = crc32fast::hash(&bytes);
            let prev = tip.at.as_ref().map(|p| &p.id);
            match self.chain.put_after(prev, &label(Kind::Claim, gen), Bytes::from(bytes)).await? {
                After::Won(id) => {
                    let position = tip.position;
                    let tip = Tip::of(&doc, Prev { id, crc });
                    self.state.lock().unwrap().owner = Some(Owner { gen, log_id, tip, failed: None });
                    return Ok(Head { log_id, position });
                }
                After::Lost(winner) => match label_gen(&winner.label) {
                    Some(current) if current >= gen => return Err(LogError::Fenced { current }),
                    // An older generation appended meanwhile: claim after it.
                    Some(_) => continue,
                    None => return Err(LogError::Lost(format!("blob {:?} is not a log blob", winner.label))),
                },
            }
        }
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        let (_, tip) = self.owned(gen)?;
        let head_term = batch.last().map_or(tip.head_term, |e| e.term);
        let w = Write { first: tip.position + 1, entries: batch.to_vec(), head_term, base: None, vote: None };
        Ok(self.write(gen, w).await?.position)
    }

    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()> {
        let (log_id, tip) = self.owned(gen)?;
        let base_at = tip.base_at();
        if from <= base_at || from > tip.position + 1 {
            return Err(LogError::Other(anyhow!("truncate at {from} outside ({base_at}, {}]", tip.position + 1)));
        }
        if from == tip.position + 1 {
            return Ok(());
        }
        let head_term = if from - 1 == base_at {
            tip.base.as_ref().map_or(0, |b| b.term)
        } else {
            let head = Head { log_id, position: from - 1 };
            let entries = self.reader().read_all(tip.clone(), from - 2, head).await?;
            entries.last().map_or(0, |(_, e)| e.term)
        };
        let w = Write { first: from, entries: Vec::new(), head_term, base: None, vote: None };
        self.write(gen, w).await.map(|_| ())
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        let (_, tip) = self.owned(gen)?;
        let w = Write {
            first: tip.position + 1,
            entries: Vec::new(),
            head_term: tip.head_term,
            base: None,
            vote: Some(vote.to_vec()),
        };
        self.write(gen, w).await.map(|_| ())
    }

    /// Nothing linked after our tip: no newer claim.
    async fn confirm(&self, gen: u64) -> LogResult<()> {
        let (_, tip) = self.owned(gen)?;
        let Some(at) = tip.at else {
            return Err(LogError::Other(anyhow!("generation {gen} holds no blob of the chain")));
        };
        match self.chain.successor(&at.id).await? {
            None => Ok(()),
            Some(next) => {
                let e = Self::fenced_by(&next.label, gen);
                if let LogError::Fenced { current } = e {
                    self.fail(Failed::Fenced(current));
                }
                Err(e)
            }
        }
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        if let Some(o) = self.state.lock().unwrap().owner.as_ref() {
            return Ok(Some(Discovered {
                base: o.tip.base.clone(),
                head: Head { log_id: o.log_id, position: o.tip.position },
                head_term: o.tip.head_term,
                vote: o.tip.vote.clone(),
                gen: o.gen,
            }));
        }
        let Some(tail) = self.chain.tail().await? else { return Ok(None) };
        let (doc, at) = self.reader().fetch(&tail.id).await?;
        let tip = Tip::of(&doc, at);
        let found = Discovered {
            base: tip.base.clone(),
            head: Head { log_id: doc.log_id, position: tip.position },
            head_term: tip.head_term,
            vote: tip.vote.clone(),
            gen: doc.gen,
        };
        self.state.lock().unwrap().seen = Some((doc.log_id, tip));
        Ok(Some(found))
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let tip = {
            let state = self.state.lock().unwrap();
            match (&state.owner, &state.seen) {
                (Some(o), _) if o.log_id == head.log_id => Some(o.tip.clone()),
                (_, Some((id, tip))) if *id == head.log_id => Some(tip.clone()),
                _ => None,
            }
        };
        let this = self.reader();
        let fut = async move {
            let tip = tip.ok_or_else(|| LogError::Lost(format!("no head of log {} known to this handle", head.log_id)))?;
            this.read_all(tip, from, head).await
        };
        Box::pin(
            futures::stream::once(fut)
                .map(|r| match r {
                    Ok(items) => futures::stream::iter(items.into_iter().map(Ok).collect::<Vec<_>>()),
                    Err(e) => futures::stream::iter(vec![Err(e)]),
                })
                .flatten(),
        )
    }

    async fn rebase(&self, gen: u64, base: Base) -> LogResult<()> {
        let (_, old_tip) = self.owned(gen)?;
        let (at, old_at) = (base.at, old_tip.base_at());
        if at < old_at {
            return Err(LogError::Other(anyhow!("rebase at {at} behind the base {old_at}")));
        }
        // Past the head (a snapshot from elsewhere): the log jumps to `at`.
        let (first, head_term) = if at > old_tip.position {
            (at + 1, base.term)
        } else {
            (old_tip.position + 1, old_tip.head_term)
        };
        let w = Write { first, entries: Vec::new(), head_term, base: Some(base), vote: None };
        let tip = self.write(gen, w).await?;

        // Collect (best effort): walk back as a reader from `at` would, to
        // the oldest blob it needs, and prune below it.
        let reader = self.reader();
        let mut keep = tip.at.clone();
        let mut limit = Position::MAX;
        let mut next = tip.at;
        while let Some(link) = next {
            let Ok(doc) = reader.follow(&link).await else { return Ok(()) };
            limit = limit.min(doc.batch.first);
            keep = Some(link);
            if limit <= at + 1 {
                break;
            }
            next = doc.prev;
        }
        if let Some(keep) = keep {
            if let Err(e) = self.chain.prune(&keep.id).await {
                tracing::debug!(error = %format!("{e:#}"), "log collection failed; retried at the next rebase");
            }
        }
        Ok(())
    }
}

/// What a read needs, detached from the store's borrow.
struct Reader<C> {
    chain: Arc<C>,
}

impl<C: Chain> Reader<C> {
    async fn download(&self, id: &C::Id) -> LogResult<Vec<u8>> {
        let stream = self.chain.get(id, None).await?;
        Ok(collect_stream(stream).await?.to_vec())
    }

    /// The blob `id` and a link to it.
    async fn fetch(&self, id: &C::Id) -> LogResult<(ChainDoc<C::Id>, Prev<C::Id>)> {
        let data = self.download(id).await?;
        let at = Prev { id: id.clone(), crc: crc32fast::hash(&data) };
        Ok((ChainDoc::decode(&data)?, at))
    }

    async fn follow(&self, link: &Prev<C::Id>) -> LogResult<ChainDoc<C::Id>> {
        let data = self.download(&link.id).await?;
        if crc32fast::hash(&data) != link.crc {
            return Err(LogError::Lost(format!("log blob {:?} changed", link.id)));
        }
        ChainDoc::decode(&data)
    }

    /// Entries `(from, head]`: walk back from `tip`; a newer batch replaces
    /// older entries from its `first` on.
    async fn read_all(self, tip: Tip<C::Id>, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
        if from >= head.position {
            return Ok(Vec::new());
        }
        if tip.position < head.position {
            return Err(LogError::Lost(format!("the log ends at {}, before {}", tip.position, head.position)));
        }
        let base_at = tip.base_at();
        if from < base_at {
            return Err(LogError::Lost(format!("entries after {from} collected up to base {base_at}")));
        }
        let mut out = BTreeMap::new();
        let mut limit = Position::MAX;
        let mut next = tip.at;
        while let Some(link) = next {
            let doc = self.follow(&link).await?;
            for (k, entry) in doc.batch.entries.into_iter().enumerate() {
                let pos = doc.batch.first + k as u64;
                if pos > from && pos < limit && pos <= head.position {
                    out.insert(pos, entry);
                }
            }
            limit = limit.min(doc.batch.first);
            if limit <= from + 1 {
                break;
            }
            next = doc.prev;
        }
        if out.len() as u64 != head.position - from {
            return Err(LogError::Lost(format!("the chain lacks entries in ({from}, {}]", head.position)));
        }
        Ok(out.into_iter().collect())
    }
}

#[cfg(test)]
mod tests;
