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
//!
//! **With a lease** ([`Lease`], docs/discord-lease.md) only lease records are
//! linked: the claim and `log-renew-<gen>`, each holding the log for the
//! lease's duration from the store's stamp on it. Batches are put unlinked
//! after the writer's tip; a writer keeps a batch only if the store answered
//! before its lease's deadline, renews before a write once a third of the
//! lease is gone, and a claimant waits out the newest lease record before it
//! reads the tail it links after. For a store whose links are scarce (a
//! Discord thread per link) and whose ids carry the store's time.

use super::{Base, Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position, LOG_FORMAT};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_storage_api::{collect_stream, After, BlobBackend, Chain};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// A lease instead of a link per batch (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// How long a lease record holds the log, from the store's stamp on it.
    pub duration: Duration,
    /// Allowance for the skew between the store's clocks, and the writer's.
    pub margin: Duration,
}

impl Lease {
    /// `duration` with a margin of 1 s (Discord's nodes agree within
    /// ~150 ms, docs/discord-lease.md § Spike findings).
    pub fn new(duration: Duration) -> Self {
        Self { duration, margin: Duration::from_secs(1).min(duration / 4) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Claim,
    Batch,
    /// A lease log's renewal.
    Renew,
}

fn label(kind: Kind, gen: u64) -> String {
    match kind {
        Kind::Claim => format!("log-claim-{gen}"),
        Kind::Batch => format!("log-batch-{gen}"),
        Kind::Renew => format!("log-renew-{gen}"),
    }
}

/// The generation a label names; `None` for a blob that is not ours.
fn label_gen(label: &str) -> Option<u64> {
    let rest = ["log-claim-", "log-batch-", "log-renew-"].iter().find_map(|p| label.strip_prefix(p))?;
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
    /// Older bases kept, newest first (`LogStore::rebase`'s `keep`).
    older: Vec<Base>,
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
    older: Vec<Base>,
    vote: Option<Vec<u8>>,
    head_term: u64,
}

impl<Id> Default for Tip<Id> {
    fn default() -> Self {
        Self { at: None, position: 0, base: None, older: Vec::new(), vote: None, head_term: 0 }
    }
}

impl<Id: Clone> Tip<Id> {
    fn of(doc: &ChainDoc<Id>, at: Prev<Id>) -> Self {
        Self {
            position: doc.head_position(),
            base: doc.base.clone(),
            older: doc.older.clone(),
            vote: doc.vote.clone(),
            head_term: doc.head_term,
            at: Some(at),
        }
    }

    fn base_at(&self) -> Position {
        self.base.as_ref().map_or(0, |b| b.at)
    }

    /// The oldest base kept: the log is readable after it.
    fn kept_from(&self) -> Position {
        self.older.last().map_or(self.base_at(), |b| b.at)
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
    /// A lease log: the newest lease record and when it was sent.
    held: Option<(Id, Instant)>,
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
    /// A new base and the older ones kept with it.
    base: Option<(Base, Vec<Base>)>,
    vote: Option<Vec<u8>>,
}

pub struct ChainLogStore<C: Chain> {
    chain: Arc<C>,
    lease: Option<Lease>,
    state: Arc<Mutex<State<C::Id>>>,
    /// One write at a time.
    writing: tokio::sync::Mutex<()>,
    /// A lease log's renewal in flight, off the writes' path.
    renewing: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The collection after the last rebase, in the background: on a chat
    /// it deletes one message (and thread) at a time, and the log's writer
    /// must not wait for it. Ends with the store.
    collecting: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<C: Chain> Drop for ChainLogStore<C> {
    fn drop(&mut self) {
        // A dropped (or crashed) incarnation stops touching the store.
        if let Some(task) = self.collecting.lock().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.renewing.lock().unwrap().take() {
            task.abort();
        }
    }
}

impl<C: Chain> ChainLogStore<C> {
    pub fn new(chain: Arc<C>) -> Self {
        Self {
            chain,
            lease: None,
            state: Arc::default(),
            writing: Default::default(),
            renewing: Mutex::default(),
            collecting: Mutex::default(),
        }
    }

    /// Hold the log by `lease` (the store must stamp its ids).
    pub fn with_lease(mut self, lease: Lease) -> Self {
        self.lease = Some(lease);
        self
    }

    fn stamp(&self, id: &C::Id) -> LogResult<u64> {
        self.chain
            .stamp_ms(id)
            .ok_or_else(|| LogError::Other(anyhow!("a lease log needs a store that stamps its blobs")))
    }

    /// The store's time now: the stamp of a probe blob, deleted after.
    async fn store_now(&self) -> LogResult<u64> {
        let probe = self.chain.put_loose("log-probe", Bytes::from_static(&[0])).await?;
        let now = self.stamp(&probe)?;
        if let Err(e) = self.chain.delete(&[C::key(&probe)]).await {
            tracing::debug!(error = %format!("{e:#}"), "a lease probe stays");
        }
        Ok(now)
    }

    /// Return once the store's clock is past `ms`.
    async fn wait_past(&self, ms: u64) -> LogResult<()> {
        loop {
            let now = self.store_now().await?;
            if now > ms {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(ms - now + 1)).await;
        }
    }

    /// The tip as of lease record `record` of generation `gen`: its newest
    /// batch stamped inside the lease, or the record itself.
    async fn lease_tip(&self, record: &C::Id, gen: u64, lease: Lease) -> LogResult<(ChainDoc<C::Id>, Prev<C::Id>)> {
        let expiry = self.stamp(record)? + lease.duration.as_millis() as u64;
        let batch = label(Kind::Batch, gen);
        // A renewal is outside the log: it names the writer's tip as of
        // itself, and batches go on from there (also while it was in flight).
        let (doc, at) = self.reader().fetch(record).await?;
        let mut newest = match (doc.claim, &doc.prev) {
            (false, Some(prev)) => prev.id.clone(),
            _ => return self.newest_batch(record, (doc, at), &batch, expiry).await,
        };
        let start = newest.clone();
        for b in self.chain.list_after(Some(&start), &batch).await? {
            if b.label == batch && self.stamp(&b.id)? < expiry {
                newest = b.id;
            }
        }
        self.reader().fetch(&newest).await
    }

    /// The newest batch after claim `record` stamped before `expiry`, or the
    /// claim itself (`claim`, fetched).
    async fn newest_batch(
        &self,
        record: &C::Id,
        claim: (ChainDoc<C::Id>, Prev<C::Id>),
        batch: &str,
        expiry: u64,
    ) -> LogResult<(ChainDoc<C::Id>, Prev<C::Id>)> {
        let mut newest = None;
        for b in self.chain.list_after(Some(record), batch).await? {
            if b.label == batch && self.stamp(&b.id)? < expiry {
                newest = Some(b.id);
            }
        }
        match newest {
            Some(id) => self.reader().fetch(&id).await,
            None => Ok(claim),
        }
    }

    /// The newest lease record this handle holds for `gen`.
    fn held(&self, gen: u64) -> Option<(C::Id, Instant)> {
        self.state.lock().unwrap().owner.as_ref().filter(|o| o.gen == gen).and_then(|o| o.held.clone())
    }

    /// Keep the lease: once a third of it is gone, link a renewal after our
    /// newest lease record in the background (a thread may wait seconds for
    /// the pacer); once two thirds are gone, wait for that renewal.
    async fn keep_lease(&self, gen: u64, lease: Lease) -> LogResult<()> {
        let Some((record, sent)) = self.held(gen) else { return Ok(()) };
        let age = sent.elapsed();
        if age < lease.duration / 3 {
            return Ok(());
        }
        let task = {
            let mut renewing = self.renewing.lock().unwrap();
            if renewing.as_ref().is_none_or(|t| t.is_finished()) {
                *renewing = Some(tokio::spawn(renew(self.chain.clone(), self.state.clone(), gen, record)));
            }
            if age < lease.duration * 2 / 3 {
                return Ok(());
            }
            renewing.take()
        };
        if let Some(task) = task {
            let t0 = Instant::now();
            let _ = task.await;
            crate::telemetry::record_lease_wait("renew", t0.elapsed());
        }
        // A failed renewal marked the owner.
        self.owned(gen).map(|_| ())
    }

    /// Our put lost to `winner`: fenced, or the chain is broken.
    fn lost_to(&self, winner: &str, gen: u64) -> LogError {
        let e = Self::fenced_by(winner, gen);
        self.fail(match &e {
            LogError::Fenced { current } => Failed::Fenced(*current),
            other => Failed::Broken(other.to_string()),
        });
        e
    }

    /// A lease log's claim: wait out the newest lease record `newest`, read
    /// the tip it leaves, link a claim after it. `Ok(None)`: a renewal
    /// linked first; look again.
    async fn claim_after_lease(&self, gen: u64, lease: Lease, newest: &C::Id, newest_gen: u64) -> LogResult<Option<Head>> {
        let t0 = Instant::now();
        let expiry = self.stamp(newest)? + lease.duration.as_millis() as u64;
        self.wait_past(expiry + lease.margin.as_millis() as u64).await?;
        crate::telemetry::record_lease_wait("claim", t0.elapsed());
        let (old, at) = self.lease_tip(newest, newest_gen, lease).await?;
        let tip = Tip::of(&old, at);
        let doc = ChainDoc {
            format: LOG_FORMAT,
            log_id: old.log_id,
            gen,
            claim: true,
            prev: tip.at.clone(),
            base: tip.base.clone(),
            older: tip.older.clone(),
            vote: tip.vote.clone(),
            head_term: tip.head_term,
            batch: Batch::new(gen, tip.position + 1, Vec::new()),
        };
        let bytes = doc.encode();
        let crc = crc32fast::hash(&bytes);
        let sent = Instant::now();
        match self.chain.put_after(Some(newest), &label(Kind::Claim, gen), Bytes::from(bytes)).await? {
            After::Won(id) => {
                tracing::info!(generation = gen, waited_ms = t0.elapsed().as_millis() as u64, "claimed the lease");
                let owner = Owner {
                    gen,
                    log_id: old.log_id,
                    tip: Tip::of(&doc, Prev { id: id.clone(), crc }),
                    failed: None,
                    held: Some((id, sent)),
                };
                self.state.lock().unwrap().owner = Some(owner);
                Ok(Some(Head { log_id: old.log_id, position: tip.position }))
            }
            After::Lost(winner) => match label_gen(&winner.label) {
                Some(current) if current >= gen => Err(LogError::Fenced { current }),
                _ => Ok(None),
            },
        }
    }

    /// Collect below the oldest base `tip` keeps, in the background: walk
    /// back as a reader from that base would, to the oldest blob it needs,
    /// and prune below it. Safe after a fence too: the bases are in the
    /// chain before this starts, and a newer incarnation keeps the same
    /// ones or newer, so it never needs what lies below. One collection at
    /// a time; while one runs, a rebase skips its own (the next one
    /// collects).
    fn collect_behind(&self, tip: &Tip<C::Id>) {
        let mut collecting = self.collecting.lock().unwrap();
        if collecting.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }
        let (reader, chain, at) = (self.reader(), self.chain.clone(), tip.kept_from());
        // A lease log's newest lease record is the chain's tail: never below.
        let floor = self.state.lock().unwrap().owner.as_ref().and_then(|o| o.held.clone()).map(|(id, _)| id);
        let mut next = tip.at.clone();
        *collecting = Some(tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            let mut keep = next.clone();
            let mut limit = Position::MAX;
            while let Some(link) = next {
                let Ok(doc) = reader.follow(&link).await else { return };
                limit = limit.min(doc.batch.first);
                keep = Some(link);
                if limit <= at + 1 {
                    break;
                }
                next = doc.prev;
            }
            let Some(mut keep) = keep.map(|k| k.id) else { return };
            if let Some(floor) = floor.filter(|f| C::key(f) < C::key(&keep)) {
                keep = floor;
            }
            let pruned = chain.prune(&keep).await;
            if let Err(e) = &pruned {
                tracing::warn!(error = %format!("{e:#}"), "log collection failed; retried at the next rebase");
            }
            crate::telemetry::record_collect(pruned.is_ok(), t0.elapsed());
        }));
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
        let _one = self.writing.lock().await;
        if let Some(lease) = self.lease {
            self.keep_lease(gen, lease).await?;
        }
        let (log_id, tip) = self.owned(gen)?;
        let (base, older) = match w.base {
            Some((base, older)) => (Some(base), older),
            None => (tip.base.clone(), tip.older.clone()),
        };
        let doc = ChainDoc {
            format: LOG_FORMAT,
            log_id,
            gen,
            claim: false,
            prev: tip.at.clone(),
            base,
            older,
            vote: w.vote.or_else(|| tip.vote.clone()),
            head_term: w.head_term,
            batch: Batch::new(gen, w.first, w.entries),
        };
        let w_first = doc.batch.first;
        let bytes = doc.encode();
        let crc = crc32fast::hash(&bytes);
        let prev = tip.at.as_ref().map(|p| &p.id);
        let put = match (self.lease, self.held(gen), prev) {
            (Some(lease), Some((_, sent)), Some(prev)) => {
                // Kept only if the store answered inside the lease.
                let deadline = sent + lease.duration.saturating_sub(lease.margin);
                match self.chain.put_unlinked(prev, &label(Kind::Batch, gen), Bytes::from(bytes)).await {
                    Ok(_) if Instant::now() >= deadline => {
                        self.fail(Failed::Broken("a batch was answered after the lease's deadline".into()));
                        return Err(LogError::Other(anyhow!("a log batch was answered after the lease's deadline")));
                    }
                    Ok(id) => Ok(After::Won(id)),
                    Err(e) => Err(e),
                }
            }
            _ => self.chain.put_after(prev, &label(Kind::Batch, gen), Bytes::from(bytes)).await,
        };
        match put {
            Ok(After::Won(id)) => {
                tracing::debug!(generation = gen, first = w_first, last = doc.head_position(), blob = ?id, "log batch linked");
                let tip = Tip::of(&doc, Prev { id, crc });
                if let Some(o) = self.state.lock().unwrap().owner.as_mut().filter(|o| o.gen == gen) {
                    o.tip = tip.clone();
                }
                Ok(tip)
            }
            Ok(After::Lost(winner)) => Err(self.lost_to(&winner.label, gen)),
            Err(e) => {
                // It may still land and win: this handle no longer knows its tip.
                self.fail(Failed::Broken(format!("{e:#}")));
                Err(LogError::Other(e.context("put a log batch")))
            }
        }
    }
}

/// Link a renewal of generation `gen`'s lease after its lease record
/// `record`. It names the writer's tip as of now (a hint for readers) but is
/// not in the log: batches keep naming batches.
async fn renew<C: Chain>(chain: Arc<C>, state: Arc<Mutex<State<C::Id>>>, gen: u64, record: C::Id) {
    let doc = {
        let state = state.lock().unwrap();
        let Some(o) = state.owner.as_ref().filter(|o| o.gen == gen && o.failed.is_none()) else { return };
        ChainDoc {
            format: LOG_FORMAT,
            log_id: o.log_id,
            gen,
            claim: false,
            prev: o.tip.at.clone(),
            base: o.tip.base.clone(),
            older: o.tip.older.clone(),
            vote: o.tip.vote.clone(),
            head_term: o.tip.head_term,
            batch: Batch::new(gen, o.tip.position + 1, Vec::new()),
        }
    };
    let sent = Instant::now();
    let put = chain.put_after(Some(&record), &label(Kind::Renew, gen), Bytes::from(doc.encode())).await;
    let mut state = state.lock().unwrap();
    let Some(o) = state.owner.as_mut().filter(|o| o.gen == gen) else { return };
    match put {
        Ok(After::Won(id)) => {
            tracing::debug!(generation = gen, blob = ?id, ms = sent.elapsed().as_millis() as u64, "lease renewed");
            o.held = Some((id, sent));
        }
        Ok(After::Lost(winner)) => {
            let failed = match label_gen(&winner.label) {
                Some(current) if current > gen => Failed::Fenced(current),
                _ => Failed::Broken(format!("blob {:?} took the place of generation {gen}'s renewal", winner.label)),
            };
            o.failed.get_or_insert(failed);
        }
        Err(e) => {
            o.failed.get_or_insert(Failed::Broken(format!("renew the lease: {e:#}")));
        }
    }
}

#[async_trait]
impl<C: Chain> LogStore for ChainLogStore<C> {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        if let Some(lease) = self.lease {
            for _ in 0..pigeonhole_storage_api::PUT_AFTER_TAIL_ATTEMPTS {
                let Some(newest) = self.chain.tail().await? else { break };
                let newest_gen = label_gen(&newest.label).unwrap_or(0);
                if newest_gen >= gen {
                    return Err(LogError::Fenced { current: newest_gen });
                }
                if let Some(head) = self.claim_after_lease(gen, lease, &newest.id, newest_gen).await? {
                    return Ok(head);
                }
            }
            if self.chain.tail().await?.is_some() {
                return Err(LogError::Other(anyhow!("claim: the lease chain does not settle")));
            }
        }
        // What `make` decided, for the last call (it may run more than once).
        enum Made<Id> {
            Fenced(u64),
            Claim { log_id: LogId, position: Position, doc: ChainDoc<Id>, crc: u32 },
        }
        let made: Mutex<Option<Made<C::Id>>> = Mutex::new(None);
        let make = |tail: Option<(&C::Id, &[u8])>| -> anyhow::Result<Option<Bytes>> {
            let (log_id, tip, tail_gen) = match tail {
                None => (LogId::random(), Tip::default(), 0),
                Some((id, data)) => {
                    let doc = ChainDoc::<C::Id>::decode(data)?;
                    let at = Prev { id: id.clone(), crc: crc32fast::hash(data) };
                    (doc.log_id, Tip::of(&doc, at), doc.gen)
                }
            };
            if tail_gen >= gen {
                *made.lock().unwrap() = Some(Made::Fenced(tail_gen));
                return Ok(None);
            }
            let doc = ChainDoc {
                format: LOG_FORMAT,
                log_id,
                gen,
                claim: true,
                prev: tip.at.clone(),
                base: tip.base.clone(),
                older: tip.older.clone(),
                vote: tip.vote.clone(),
                head_term: tip.head_term,
                batch: Batch::new(gen, tip.position + 1, Vec::new()),
            };
            let bytes = doc.encode();
            let crc = crc32fast::hash(&bytes);
            *made.lock().unwrap() = Some(Made::Claim { log_id, position: tip.position, doc, crc });
            Ok(Some(Bytes::from(bytes)))
        };
        let sent = Instant::now();
        let won = self.chain.put_after_tail(&label(Kind::Claim, gen), &make).await?;
        match (won, made.into_inner().unwrap()) {
            (None, Some(Made::Fenced(current))) => Err(LogError::Fenced { current }),
            (Some(id), Some(Made::Claim { log_id, position, doc, crc })) => {
                let held = self.lease.map(|_| (id.clone(), sent));
                let tip = Tip::of(&doc, Prev { id, crc });
                self.state.lock().unwrap().owner = Some(Owner { gen, log_id, tip, failed: None, held });
                Ok(Head { log_id, position })
            }
            _ => Err(LogError::Other(anyhow!("claim: the store and the claim disagree"))),
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
        // A lease log links only lease records: look from the newest.
        let Some(mut at) = self.held(gen).map(|(id, _)| id).or(tip.at.map(|a| a.id)) else {
            return Err(LogError::Other(anyhow!("generation {gen} holds no blob of the chain")));
        };
        // A successor of our own generation is our own append, linked but
        // not yet in `tip` (only this handle writes `gen`): look past it.
        loop {
            match self.chain.successor(&at).await? {
                None => return Ok(()),
                Some(next) if label_gen(&next.label) == Some(gen) => at = next.id,
                Some(next) => {
                    let e = Self::fenced_by(&next.label, gen);
                    if let LogError::Fenced { current } = e {
                        self.fail(Failed::Fenced(current));
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        if let Some(o) = self.state.lock().unwrap().owner.as_ref() {
            return Ok(Some(Discovered {
                base: o.tip.base.clone(),
                older: o.tip.older.clone(),
                head: Head { log_id: o.log_id, position: o.tip.position },
                head_term: o.tip.head_term,
                vote: o.tip.vote.clone(),
                gen: o.gen,
            }));
        }
        let Some(tail) = self.chain.tail().await? else { return Ok(None) };
        let (doc, at) = match self.lease {
            Some(lease) => self.lease_tip(&tail.id, label_gen(&tail.label).unwrap_or(0), lease).await?,
            None => self.reader().fetch(&tail.id).await?,
        };
        let tip = Tip::of(&doc, at);
        let found = Discovered {
            base: tip.base.clone(),
            older: tip.older.clone(),
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

    async fn rebase(&self, gen: u64, base: Base, keep: usize) -> LogResult<Vec<Base>> {
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
        // The bases kept, newest first; a repeated base counts once.
        let mut older: Vec<Base> = old_tip.base.iter().chain(&old_tip.older).filter(|b| **b != base).cloned().collect();
        let dropped = older.split_off(older.len().min(keep.max(1) - 1));
        let w = Write { first, entries: Vec::new(), head_term, base: Some((base, older)), vote: None };
        let tip = self.write(gen, w).await?;
        // The bases are durable; dropping what lies behind them is best effort.
        self.collect_behind(&tip);
        Ok(dropped)
    }

    async fn settle(&self) {
        let task = self.collecting.lock().unwrap().take();
        if let Some(task) = task {
            let _ = task.await;
        }
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
        let kept_from = tip.kept_from();
        if from < kept_from {
            return Err(LogError::Lost(format!("entries after {from} collected up to base {kept_from}")));
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
