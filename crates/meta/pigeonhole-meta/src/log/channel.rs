//! A [`LogStore`] in a channel whose messages can be read by id (Telegram
//! over MTProto), fenced by the **order** of its messages
//! (docs/telegram-mtproto.md § The log).
//!
//! The channel gives every message an id, atomically and in increasing order:
//! one total order of everything every writer posts, and readers see it. Two
//! kinds of documents, told apart by name:
//!
//! - `log-<gen>-claim.bin` — generation `gen` claims the log;
//! - `log-<gen>-batch.bin` — a [`Batch`] with the log's registers, linked to
//!   the previous batch of the chain (`prev`) and to `since`, the last
//!   message its writer had checked when it sent it.
//!
//! Validity depends only on lower ids, which never change: a claim is valid
//! iff no valid claim of a generation ≥ its own is below it; a batch of `g`
//! is valid iff no claim of a generation > `g` lies in `(since, batch)`. A
//! writer sends a batch only after the previous one is checked, so its checked
//! ranges are contiguous from its claim, and the first higher claim in them is
//! valid: the local check is exact. The head is the highest valid batch; a
//! deposed writer's batch that lands late is above the newer claim and invalid
//! for every reader alike.
//!
//! `append`: send the batch; if its id is `checked + 1` nothing can lie in
//! between and it is committed without a read, else read `(checked, id)`: a
//! higher claim there fences us. `claim`: send the claim, walk down from it to
//! the highest valid batch, then check the claims between.
//!
//! Empty ids (`messageEmpty`: deleted, or not visible yet) inside a range the
//! fence depends on are read again; one that stays empty makes the outcome
//! unknown (`Other`). Nothing above the oldest batch a reader needs is ever
//! deleted, and the probes `discover` sends stay.

use super::{
    Base, Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position, LOG_FORMAT,
};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Most ids one `get_messages` / `delete` takes.
pub const PAGE: usize = 100;

/// A channel message as the log sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMsg {
    /// Deleted, past the top, or not visible yet.
    Empty,
    /// Not a document.
    Other,
    Doc { name: String },
}

/// The channel operations the log and the snapshots need.
#[async_trait]
pub trait ChannelApi: Send + Sync + 'static {
    /// Post a document; its message id. A failed call may still post it, now
    /// or later: the protocol does not depend on it.
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<i64>;
    /// The messages `ids` (at most [`PAGE`]), in the order asked.
    async fn get_messages(&self, ids: &[i64]) -> anyhow::Result<Vec<ChannelMsg>>;
    /// The document of message `message_id`.
    async fn download(&self, message_id: i64) -> anyhow::Result<Vec<u8>>;
    /// Delete messages (at most [`PAGE`]); missing ids are fine.
    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct ChannelLogOptions {
    /// An empty id inside a range the fence depends on is read again this
    /// often…
    pub empty_retries: u32,
    /// …this far apart, before the outcome is unknown.
    pub empty_retry_delay: Duration,
}

impl Default for ChannelLogOptions {
    fn default() -> Self {
        Self { empty_retries: 5, empty_retry_delay: Duration::from_secs(2) }
    }
}

const PROBE: &str = "log-probe.bin";

fn claim_name(gen: u64) -> String {
    format!("log-{gen}-claim.bin")
}

fn batch_name(gen: u64) -> String {
    format!("log-{gen}-batch.bin")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Claim(u64),
    Batch(u64),
    Probe,
}

fn kind(msg: &ChannelMsg) -> Option<Kind> {
    let ChannelMsg::Doc { name } = msg else { return None };
    if name == PROBE {
        return Some(Kind::Probe);
    }
    let rest = name.strip_prefix("log-")?;
    let (gen, what) = rest.split_once('-')?;
    let gen = gen.parse().ok()?;
    match what {
        "claim.bin" => Some(Kind::Claim(gen)),
        "batch.bin" => Some(Kind::Batch(gen)),
        _ => None,
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ClaimDoc {
    format: u32,
    gen: u64,
}

/// Link from a batch to the previous one of its chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Link {
    message_id: i64,
    /// crc32 of the linked document: a replaced or edited message is detected.
    crc: u32,
}

/// One batch message: a batch plus the log's registers as of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BatchDoc {
    format: u32,
    log_id: LogId,
    gen: u64,
    /// The writer had checked every message up to this id when it sent this.
    since: i64,
    prev: Option<Link>,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
    /// Term of the entry at the head once this batch is the head.
    head_term: u64,
    /// Every log message with an id at or below this was collected.
    collected: i64,
    batch: Batch,
}

impl BatchDoc {
    fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a log message always encodes")
    }

    fn decode(data: &[u8]) -> LogResult<Self> {
        let (format, _) = postcard::take_from_bytes::<u32>(data)
            .map_err(|e| LogError::Lost(format!("log message header: {e}")))?;
        if format != LOG_FORMAT {
            return Err(LogError::Lost(format!(
                "log message in format {format}, this binary reads {LOG_FORMAT}"
            )));
        }
        postcard::from_bytes(data).map_err(|e| LogError::Lost(format!("log message: {e}")))
    }

    fn head_position(&self) -> Position {
        self.batch.first - 1 + self.batch.entries.len() as u64
    }
}

/// The log as of one batch (its registers).
#[derive(Debug, Clone, Default)]
struct Tip {
    link: Option<Link>,
    position: Position,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
    head_term: u64,
    collected: i64,
}

impl Tip {
    fn of(doc: &BatchDoc, link: Link) -> Self {
        Self {
            position: doc.head_position(),
            base: doc.base.clone(),
            vote: doc.vote.clone(),
            head_term: doc.head_term,
            collected: doc.collected,
            link: Some(link),
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

struct Owner {
    gen: u64,
    log_id: LogId,
    /// Our last message whose range below is checked.
    checked: i64,
    tip: Tip,
    failed: Option<Failed>,
}

#[derive(Default)]
struct State {
    owner: Option<Owner>,
    /// What the last `discover` found (reads by a handle that did not claim).
    seen: Option<(LogId, Tip)>,
}

/// One write: a batch at `first` and the registers that change with it.
struct Write {
    first: Position,
    entries: Vec<Entry>,
    head_term: u64,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
}

/// What a walk down from `top` found.
struct Survey {
    /// The highest valid batch.
    head: Option<(BatchDoc, Link)>,
    /// The newest valid claim's generation (0: never claimed).
    gen: u64,
}

pub struct ChannelLogStore<C> {
    chat: Arc<C>,
    opts: ChannelLogOptions,
    state: Mutex<State>,
}

impl<C: ChannelApi> ChannelLogStore<C> {
    pub fn new(chat: Arc<C>, opts: ChannelLogOptions) -> Self {
        Self { chat, opts, state: Mutex::default() }
    }

    async fn page(&self, lo: i64, hi: i64) -> LogResult<Vec<(i64, ChannelMsg)>> {
        let ids: Vec<i64> = (lo..=hi).collect();
        let msgs = self.chat.get_messages(&ids).await?;
        if msgs.len() != ids.len() {
            return Err(LogError::Other(anyhow!("getMessages answered {} of {} ids", msgs.len(), ids.len())));
        }
        Ok(ids.into_iter().zip(msgs).collect())
    }

    /// Make sure `view` holds every id in `[lo, hi]`, reading whole pages
    /// down from the highest missing id (a walk down reads each id once).
    async fn fill(&self, view: &mut BTreeMap<i64, ChannelMsg>, lo: i64, hi: i64) -> LogResult<()> {
        let mut top = hi;
        while top >= lo.max(1) {
            if view.contains_key(&top) {
                top -= 1;
                continue;
            }
            let bottom = (top - PAGE as i64 + 1).max(1);
            view.extend(self.page(bottom, top).await?);
            top = bottom - 1;
        }
        Ok(())
    }

    /// Read the empty ids of `(lo, hi)` again until they show up; `Other`
    /// if one stays empty.
    async fn settle(&self, view: &mut BTreeMap<i64, ChannelMsg>, lo: i64, hi: i64) -> LogResult<()> {
        for attempt in 0..=self.opts.empty_retries {
            let empty: Vec<i64> =
                view.range(lo + 1..hi).filter(|(_, m)| **m == ChannelMsg::Empty).map(|(id, _)| *id).collect();
            if empty.is_empty() {
                return Ok(());
            }
            if attempt == self.opts.empty_retries {
                return Err(LogError::Other(anyhow!(
                    "message {} between {lo} and {hi} stays empty: deleted from the log channel, or not visible",
                    empty[0]
                )));
            }
            tokio::time::sleep(self.opts.empty_retry_delay).await;
            for ids in empty.chunks(PAGE) {
                let msgs = self.chat.get_messages(ids).await?;
                view.extend(ids.iter().copied().zip(msgs));
            }
        }
        Ok(())
    }

    /// The highest claim generation in `(lo, hi)` above `gen`, if any.
    fn higher_claim(view: &BTreeMap<i64, ChannelMsg>, lo: i64, hi: i64, gen: u64) -> Option<u64> {
        view.range(lo + 1..hi)
            .filter_map(|(_, m)| match kind(m) {
                Some(Kind::Claim(k)) if k > gen => Some(k),
                _ => None,
            })
            .max()
    }

    async fn fetch(&self, message_id: i64) -> LogResult<(BatchDoc, Link)> {
        let data = self.chat.download(message_id).await?;
        let link = Link { message_id, crc: crc32fast::hash(&data) };
        Ok((BatchDoc::decode(&data)?, link))
    }

    /// Whether batch `doc` at `id` is valid: no higher claim in `(since, id)`.
    async fn valid(&self, view: &mut BTreeMap<i64, ChannelMsg>, doc: &BatchDoc, id: i64) -> LogResult<bool> {
        // Top down: the first higher claim settles it, without reading the
        // rest of the range (it may reach below collected messages).
        let mut hi = id - 1;
        while hi > doc.since {
            let lo = (doc.since + 1).max(hi - PAGE as i64 + 1);
            self.fill(view, lo, hi).await?;
            if Self::higher_claim(view, lo - 1, hi + 1, doc.gen).is_some() {
                return Ok(false);
            }
            hi = lo - 1;
        }
        self.settle(view, doc.since, id).await?;
        Ok(Self::higher_claim(view, doc.since, id, doc.gen).is_none())
    }

    /// Walk down from `top` (exclusive) to the highest valid batch, then
    /// find the newest valid claim above it.
    async fn survey(&self, top: i64) -> LogResult<Survey> {
        let mut view = BTreeMap::new();
        let mut head = None;
        let mut id = top - 1;
        while id >= 1 {
            self.fill(&mut view, (id - PAGE as i64 + 1).max(1), id).await?;
            if let Some(Kind::Batch(gen)) = view.get(&id).and_then(kind) {
                let (doc, link) = self.fetch(id).await?;
                if doc.gen != gen {
                    return Err(LogError::Lost(format!("log message {id} is named for generation {gen}, holds {}", doc.gen)));
                }
                if self.valid(&mut view, &doc, id).await? {
                    head = Some((doc, link));
                    break;
                }
            }
            id -= 1;
        }
        // Claims above the head: a valid one exceeds every valid one below
        // it. Below the head none exceeds the head's generation.
        let floor = head.as_ref().map_or(0, |(_, l): &(BatchDoc, Link)| l.message_id);
        if head.is_some() {
            self.settle(&mut view, floor, top).await?;
        }
        let mut gen = head.as_ref().map_or(0, |(d, _)| d.gen);
        for (_, m) in view.range(floor + 1..top) {
            if let Some(Kind::Claim(k)) = kind(m) {
                gen = gen.max(k);
            }
        }
        Ok(Survey { head, gen })
    }

    /// This handle's write position, if it holds generation `gen`.
    fn owned(&self, gen: u64) -> LogResult<(LogId, i64, Tip)> {
        let state = self.state.lock().unwrap();
        let o = state
            .owner
            .as_ref()
            .filter(|o| o.gen == gen)
            .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;
        match &o.failed {
            Some(Failed::Fenced(current)) => Err(LogError::Fenced { current: *current }),
            Some(Failed::Broken(why)) => Err(LogError::Other(anyhow!("the log handle failed earlier: {why}"))),
            None => Ok((o.log_id, o.checked, o.tip.clone())),
        }
    }

    fn fail(&self, failed: Failed) {
        if let Some(o) = self.state.lock().unwrap().owner.as_mut() {
            o.failed.get_or_insert(failed);
        }
    }

    /// Send `w` as generation `gen` after the owner's tip and prove no newer
    /// claim came before it.
    async fn write(&self, gen: u64, w: Write) -> LogResult<(Tip, i64)> {
        let (log_id, checked, tip) = self.owned(gen)?;
        let doc = BatchDoc {
            format: LOG_FORMAT,
            log_id,
            gen,
            since: checked,
            prev: tip.link,
            base: w.base.or_else(|| tip.base.clone()),
            vote: w.vote.or_else(|| tip.vote.clone()),
            head_term: w.head_term,
            collected: tip.collected,
            batch: Batch::new(gen, w.first, w.entries),
        };
        let bytes = doc.encode();
        let crc = crc32fast::hash(&bytes);
        let sent = match self.chat.send_document(&batch_name(gen), bytes).await {
            Ok(id) if id > checked => Ok(id),
            Ok(id) => Err(anyhow!("the channel gave message id {id}, not above {checked}")),
            Err(e) => Err(e),
        };
        let sent = sent.map_err(|e| {
            // It may still land: this handle cannot know its own tip any more.
            self.fail(Failed::Broken(format!("{e:#}")));
            LogError::Other(e.context("send a log batch"))
        })?;
        if sent != checked + 1 {
            let mut view = BTreeMap::new();
            let checked_range = async {
                self.fill(&mut view, checked + 1, sent - 1).await?;
                self.settle(&mut view, checked, sent).await?;
                Ok::<_, LogError>(Self::higher_claim(&view, checked, sent, gen))
            };
            match checked_range.await {
                Ok(None) => {}
                Ok(Some(current)) => {
                    self.fail(Failed::Fenced(current));
                    return Err(LogError::Fenced { current });
                }
                Err(e) => {
                    self.fail(Failed::Broken(e.to_string()));
                    return Err(e);
                }
            }
        }
        let tip = Tip::of(&doc, Link { message_id: sent, crc });
        if let Some(o) = self.state.lock().unwrap().owner.as_mut().filter(|o| o.gen == gen) {
            o.tip = tip.clone();
            o.checked = sent;
        }
        Ok((tip, doc.since))
    }

    fn reader(&self) -> ReadCtx<C> {
        ReadCtx { chat: self.chat.clone() }
    }

    /// Delete our log messages in `(after, below)` (best effort).
    async fn collect(&self, after: i64, below: i64) {
        let mut lo = after + 1;
        while lo < below {
            let hi = (lo + PAGE as i64 - 1).min(below - 1);
            let Ok(page) = self.page(lo, hi).await else { return };
            let ours: Vec<i64> = page.iter().filter(|(_, m)| kind(m).is_some()).map(|(id, _)| *id).collect();
            if !ours.is_empty() && self.chat.delete(&ours).await.is_err() {
                return;
            }
            if let Some(o) = self.state.lock().unwrap().owner.as_mut() {
                o.tip.collected = o.tip.collected.max(hi);
            }
            lo = hi + 1;
        }
    }
}

#[async_trait]
impl<C: ChannelApi> LogStore for ChannelLogStore<C> {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        let doc = postcard::to_stdvec(&ClaimDoc { format: LOG_FORMAT, gen }).expect("a claim encodes");
        let c = self.chat.send_document(&claim_name(gen), doc).await?;
        let survey = self.survey(c).await?;
        if survey.gen >= gen {
            return Err(LogError::Fenced { current: survey.gen });
        }
        let fresh = survey.head.is_none();
        let (log_id, tip) = match survey.head {
            Some((doc, link)) => (doc.log_id, Tip::of(&doc, link)),
            None => (LogId::random(), Tip::default()),
        };
        let position = tip.position;
        self.state.lock().unwrap().owner = Some(Owner { gen, log_id, checked: c, tip, failed: None });
        if fresh {
            // The first batch fixes the log's id for every later reader.
            let w = Write { first: 1, entries: Vec::new(), head_term: 0, base: None, vote: None };
            self.write(gen, w).await?;
        }
        Ok(Head { log_id, position })
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        let (_, _, tip) = self.owned(gen)?;
        let head_term = batch.last().map_or(tip.head_term, |e| e.term);
        let w = Write { first: tip.position + 1, entries: batch.to_vec(), head_term, base: None, vote: None };
        Ok(self.write(gen, w).await?.0.position)
    }

    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()> {
        let (log_id, _, tip) = self.owned(gen)?;
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
        let (_, _, tip) = self.owned(gen)?;
        let w = Write {
            first: tip.position + 1,
            entries: Vec::new(),
            head_term: tip.head_term,
            base: None,
            vote: Some(vote.to_vec()),
        };
        self.write(gen, w).await.map(|_| ())
    }

    /// An empty batch: the only way to learn the top of the channel is to
    /// post above it.
    async fn confirm(&self, gen: u64) -> LogResult<()> {
        self.append(gen, &[]).await.map(|_| ())
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
        // The top is only learnt by posting: a probe (kept, so that no
        // range a fence check reads has a hole).
        let probe = self.chat.send_document(PROBE, vec![0]).await?;
        let survey = self.survey(probe).await?;
        let Some((doc, link)) = survey.head else {
            if survey.gen == 0 {
                return Ok(None);
            }
            // Claimed, but no batch landed: an empty log of no id yet.
            let head = Head { log_id: LogId(0), position: 0 };
            return Ok(Some(Discovered { base: None, head, head_term: 0, vote: None, gen: survey.gen }));
        };
        let tip = Tip::of(&doc, link);
        let found = Discovered {
            base: tip.base.clone(),
            head: Head { log_id: doc.log_id, position: tip.position },
            head_term: tip.head_term,
            vote: tip.vote.clone(),
            gen: survey.gen,
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
        let (_, _, old_tip) = self.owned(gen)?;
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
        let (tip, since) = self.write(gen, w).await?;

        // Collect (best effort): walk back as a reader from `at` would, to
        // the oldest batch it needs; keep it, and keep the rebase batch's
        // checked range whole (a claim validates it).
        let mut keep = tip.link.map_or(0, |l| l.message_id);
        let mut limit = Position::MAX;
        let mut next = tip.link;
        while let Some(link) = next {
            let Ok(doc) = self.reader().follow(link).await else { return Ok(()) };
            keep = link.message_id;
            limit = limit.min(doc.batch.first);
            if limit <= at + 1 {
                break;
            }
            next = doc.prev;
        }
        self.collect(old_tip.collected, keep.min(since)).await;
        Ok(())
    }
}

/// What a read needs, detached from the store's borrow.
struct ReadCtx<C> {
    chat: Arc<C>,
}

impl<C: ChannelApi> ReadCtx<C> {
    async fn follow(&self, link: Link) -> LogResult<BatchDoc> {
        let data = self.chat.download(link.message_id).await?;
        if crc32fast::hash(&data) != link.crc {
            return Err(LogError::Lost(format!("log message {} changed", link.message_id)));
        }
        BatchDoc::decode(&data)
    }

    /// Entries `(from, head]`: walk back from `tip`; a newer batch replaces
    /// older entries from its `first` on.
    async fn read_all(self, tip: Tip, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
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
        let mut next = tip.link;
        while let Some(link) = next {
            let doc = self.follow(link).await?;
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
pub(crate) mod fake;
#[cfg(test)]
mod tests;
