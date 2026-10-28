//! A [`LogStore`] in a chat (Telegram), over two registers read with one call
//! (meta-backends.md § `telegram`):
//!
//! - **head** — the pinned message: the newest pinned message *by sending
//!   date*. Every log message is a document: a batch with its generation, the
//!   log's registers (base, vote, head term) and a link to the previous log
//!   message, so the pinned head leads back through the chain to the base. A
//!   batch replaces older entries from its `first` on, so a truncate is a
//!   message too.
//! - **fence** — the chat description, `pigeonhole-log id=<log id> gen=<g>`.
//!
//! `append(g)`: send the batch linked to the current head, pin it, read the
//! description: `Ok` only if it still holds `g`. `claim(g)`: write the
//! description, then **wait out the straggler window** before reading the head:
//! a deposed writer may still be sending one batch, which then tops the pin and
//! links to its own (complete) chain; after the window every such straggler
//! has landed and the head is stable. Messages are never unpinned (on a live
//! bot, unpinning the head hid the pin for about a minute); a straggler that
//! lands anyway is overridden by pinning a newer message.
//!
//! The chat API is a trait ([`ChatApi`]) so the protocol runs against a fake
//! chat in tests; the binary adapts the Telegram client.

use super::{
    Base, Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position,
    LOG_FORMAT,
};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A message sent to the chat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatDoc {
    pub message_id: i64,
    pub download_ref: String,
}

/// What one `getChat` returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Registers {
    pub description: Option<String>,
    /// The pinned message (newest pinned by sending date), if it is a document.
    pub pinned: Option<ChatDoc>,
    /// Something is pinned but it is not a document.
    pub pinned_other: bool,
}

/// The few chat operations the log needs.
#[async_trait]
pub trait ChatApi: Send + Sync + 'static {
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<ChatDoc>;
    async fn pin(&self, message_id: i64) -> anyhow::Result<()>;
    async fn registers(&self) -> anyhow::Result<Registers>;
    /// `Ok(false)`: the description already had this exact value.
    async fn set_description(&self, text: &str) -> anyhow::Result<bool>;
    async fn download(&self, download_ref: &str) -> anyhow::Result<Vec<u8>>;
    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct ChatLogOptions {
    /// How long a claim waits after writing the description before it reads
    /// the head. Must exceed the longest a deposed writer's send can take
    /// (its request timeout).
    pub straggler_window: Duration,
    /// A pinned head can be briefly invisible; retry reading it this often…
    pub head_retries: u32,
    /// …this far apart.
    pub head_retry_delay: Duration,
}

impl Default for ChatLogOptions {
    fn default() -> Self {
        Self {
            straggler_window: Duration::from_secs(90),
            head_retries: 12,
            head_retry_delay: Duration::from_secs(10),
        }
    }
}

const DESCRIPTION_PREFIX: &str = "pigeonhole-log";

fn description(log_id: LogId, gen: u64) -> String {
    format!("{DESCRIPTION_PREFIX} id={log_id} gen={gen}")
}

/// `None` for an empty description; `Err` for a description that is not ours
/// (the chat is used for something else).
fn parse_description(text: Option<&str>) -> LogResult<Option<(LogId, u64)>> {
    let Some(text) = text.map(str::trim).filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    let foreign = || {
        LogError::Other(anyhow!(
            "chat description {text:?} is not a pigeonhole log; the log needs a dedicated chat"
        ))
    };
    let mut words = text.split_whitespace();
    if words.next() != Some(DESCRIPTION_PREFIX) {
        return Err(foreign());
    }
    let (mut id, mut gen) = (None, None);
    for w in words {
        if let Some(v) = w.strip_prefix("id=") {
            id = v.parse::<LogId>().ok();
        } else if let Some(v) = w.strip_prefix("gen=") {
            gen = v.parse::<u64>().ok();
        }
    }
    match (id, gen) {
        (Some(id), Some(gen)) => Ok(Some((id, gen))),
        _ => Err(foreign()),
    }
}

/// Link from a log message to the one before it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Link {
    message_id: i64,
    download_ref: String,
    /// crc32 of the linked document: a replaced or edited message is detected.
    crc: u32,
}

/// One log message: a batch plus the log's registers as of it, so the
/// pinned head alone tells the base, the head's term and the vote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LogDoc {
    format: u32,
    log_id: LogId,
    gen: u64,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
    /// Term of the entry at the head once this message is the head.
    head_term: u64,
    /// Every log message with an id at or below this was collected.
    collected: i64,
    prev: Option<Link>,
    batch: Batch,
}

impl LogDoc {
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

    /// Position of the last entry the log holds once this message is the head.
    fn head_position(&self) -> Position {
        self.batch.first - 1 + self.batch.entries.len() as u64
    }

    fn base_at(&self) -> Position {
        self.base.as_ref().map_or(0, |b| b.at)
    }
}

/// The log as this handle knows it (its head message's registers).
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
    fn of(doc: &LogDoc, link: Link) -> Self {
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

struct Owner {
    gen: u64,
    log_id: LogId,
    tip: Tip,
}

/// One write: a batch at `first` and the registers that change with it.
struct Write {
    first: Position,
    entries: Vec<Entry>,
    head_term: u64,
    base: Option<Base>,
    vote: Option<Vec<u8>>,
}

pub struct ChatLogStore<C> {
    chat: Arc<C>,
    opts: ChatLogOptions,
    owner: Mutex<Option<Owner>>,
}

impl<C: ChatApi> ChatLogStore<C> {
    pub fn new(chat: Arc<C>, opts: ChatLogOptions) -> Self {
        Self { chat, opts, owner: Mutex::new(None) }
    }

    async fn registers(&self) -> LogResult<Registers> {
        Ok(self.chat.registers().await?)
    }

    async fn fetch(&self, doc: &ChatDoc) -> LogResult<(LogDoc, Link)> {
        let data = self.chat.download(&doc.download_ref).await?;
        let crc = crc32fast::hash(&data);
        let link = Link { message_id: doc.message_id, download_ref: doc.download_ref.clone(), crc };
        Ok((LogDoc::decode(&data)?, link))
    }

    async fn follow(&self, link: &Link) -> LogResult<LogDoc> {
        let data = self.chat.download(&link.download_ref).await?;
        if crc32fast::hash(&data) != link.crc {
            return Err(LogError::Lost(format!("log message {} changed", link.message_id)));
        }
        LogDoc::decode(&data)
    }

    /// The pinned head, retried while the pin is briefly invisible; `None`
    /// if nothing is pinned even then (claimed, nothing appended yet).
    async fn head_doc(&self, log_id: LogId) -> LogResult<Option<(LogDoc, Link)>> {
        for attempt in 0..=self.opts.head_retries {
            let regs = self.registers().await?;
            if let Some(doc) = regs.pinned {
                let (head, link) = self.fetch(&doc).await?;
                if head.log_id != log_id {
                    return Err(LogError::Lost(format!("pinned message {} is from another log", doc.message_id)));
                }
                return Ok(Some((head, link)));
            }
            if regs.pinned_other {
                return Err(LogError::Other(anyhow!(
                    "the pinned message is not a log message; the log needs a dedicated chat"
                )));
            }
            if attempt < self.opts.head_retries {
                tokio::time::sleep(self.opts.head_retry_delay).await;
            }
        }
        Ok(None)
    }

    /// This handle's tip, if it holds generation `gen`.
    fn owned(&self, gen: u64) -> LogResult<(LogId, Tip)> {
        let owner = self.owner.lock().unwrap();
        let o = owner
            .as_ref()
            .filter(|o| o.gen == gen)
            .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;
        Ok((o.log_id, o.tip.clone()))
    }

    /// The generation in the description now (for a `Fenced` answer).
    async fn current_gen(&self) -> LogResult<u64> {
        Ok(parse_description(self.registers().await?.description.as_deref())?.map_or(0, |(_, g)| g))
    }

    /// Send `w` linked to the owner's tip as generation `gen`, pin it, and
    /// prove the fence still holds `gen`. Overrides a newer straggler on top.
    async fn write(&self, gen: u64, w: Write) -> LogResult<Tip> {
        let (log_id, mut tip) = self.owned(gen)?;
        let mut w = Some(w);
        // A straggler may top our pin; then link a newer (empty) batch to our
        // tip and pin that. Bounded: each fenced writer has at most one.
        for _ in 0..4 {
            let this = w.take().unwrap_or_else(|| Write {
                first: tip.position + 1,
                entries: Vec::new(),
                head_term: tip.head_term,
                base: None,
                vote: None,
            });
            let doc = LogDoc {
                format: LOG_FORMAT,
                log_id,
                gen,
                base: this.base.or_else(|| tip.base.clone()),
                vote: this.vote.or_else(|| tip.vote.clone()),
                head_term: this.head_term,
                collected: tip.collected,
                prev: tip.link.clone(),
                batch: Batch::new(gen, this.first, this.entries),
            };
            let bytes = doc.encode();
            let crc = crc32fast::hash(&bytes);
            let sent = self.chat.send_document(&format!("log-{gen}-{}.bin", doc.head_position()), bytes).await?;
            self.chat.pin(sent.message_id).await?;
            let regs = self.registers().await?;
            match parse_description(regs.description.as_deref())? {
                Some((id, g)) if id == log_id && g == gen => {}
                Some((_, g)) => return Err(LogError::Fenced { current: g }),
                None => return Err(LogError::Other(anyhow!("the log's description is gone"))),
            }
            tip = Tip::of(&doc, Link { message_id: sent.message_id, download_ref: sent.download_ref.clone(), crc });
            if let Some(o) = self.owner.lock().unwrap().as_mut() {
                o.tip = tip.clone();
            }
            if regs.pinned.as_ref().is_some_and(|p| p.message_id == sent.message_id) {
                return Ok(tip);
            }
        }
        Err(LogError::Other(anyhow!("could not keep the log head pinned")))
    }

    fn reader(&self) -> ReadCtx<C> {
        ReadCtx { chat: self.chat.clone(), opts: self.opts.clone() }
    }
}

#[async_trait]
impl<C: ChatApi> LogStore for ChatLogStore<C> {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        let before = parse_description(self.registers().await?.description.as_deref())?;
        if let Some((_, current)) = before {
            if gen <= current {
                return Err(LogError::Fenced { current });
            }
        }
        let log_id = before.map_or_else(LogId::random, |(id, _)| id);
        if !self.chat.set_description(&description(log_id, gen)).await? {
            return Err(LogError::Fenced { current: gen });
        }
        tokio::time::sleep(self.opts.straggler_window).await;

        match parse_description(self.registers().await?.description.as_deref())? {
            Some((id, g)) if id == log_id && g == gen => {}
            Some((_, g)) => return Err(LogError::Fenced { current: g }),
            None => return Err(LogError::Other(anyhow!("the log's description is gone"))),
        }
        let tip = if before.is_none() {
            let regs = self.registers().await?;
            if regs.pinned.is_some() || regs.pinned_other {
                return Err(LogError::Other(anyhow!(
                    "a new log needs a chat with nothing pinned (a dedicated chat)"
                )));
            }
            Tip::default()
        } else {
            match self.head_doc(log_id).await? {
                Some((doc, link)) => Tip::of(&doc, link),
                None => Tip::default(),
            }
        };
        let position = tip.position;
        *self.owner.lock().unwrap() = Some(Owner { gen, log_id, tip });
        Ok(Head { log_id, position })
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
            let entries = self.reader().read_all(from - 2, head).await?;
            entries.last().map_or(0, |(_, e)| e.term)
        };
        let w = Write { first: from, entries: Vec::new(), head_term, base: None, vote: None };
        self.write(gen, w).await.map(|_| ())
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        let (_, tip) = match self.owned(gen) {
            Ok(owned) => owned,
            Err(_) => return Err(LogError::Fenced { current: self.current_gen().await? }),
        };
        let w = Write {
            first: tip.position + 1,
            entries: Vec::new(),
            head_term: tip.head_term,
            base: None,
            vote: Some(vote.to_vec()),
        };
        self.write(gen, w).await.map(|_| ())
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        let Some((log_id, gen)) = parse_description(self.registers().await?.description.as_deref())? else {
            return Ok(None);
        };
        let Some((head, _)) = self.head_doc(log_id).await? else {
            let head = Head { log_id, position: 0 };
            return Ok(Some(Discovered { base: None, head, head_term: 0, vote: None, gen }));
        };
        Ok(Some(Discovered {
            head: Head { log_id, position: head.head_position() },
            head_term: head.head_term,
            vote: head.vote,
            base: head.base,
            gen,
        }))
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let this = self.reader();
        let fut = async move { this.read_all(from, head).await };
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
        let (log_id, old_tip) = match self.owned(gen) {
            Ok(owned) => owned,
            Err(_) => return Err(LogError::Fenced { current: self.current_gen().await? }),
        };
        let (at, old_at) = (base.at, old_tip.base_at());
        if at > old_tip.position || at < old_at {
            return Err(LogError::Other(anyhow!("rebase at {at} outside [{old_at}, {}]", old_tip.position)));
        }
        let w = Write {
            first: old_tip.position + 1,
            entries: Vec::new(),
            head_term: old_tip.head_term,
            base: Some(base),
            vote: None,
        };
        let tip = self.write(gen, w).await?;

        // Collect (best effort: a leftover message is unreachable garbage).
        // Walk back as a reader from `at` would; it stops once newer messages
        // cover every position after `at`, and everything behind that point
        // down to the last collection is garbage.
        let mut limit = Position::MAX;
        let mut next = tip.link.clone();
        let mut garbage = None;
        while let Some(link) = next {
            let Ok(doc) = self.follow(&link).await else { break };
            limit = limit.min(doc.batch.first);
            if limit <= at + 1 {
                garbage = doc.prev;
                break;
            }
            next = doc.prev;
        }
        let mut ids = Vec::new();
        let mut next = garbage.clone();
        while let Some(link) = next.filter(|l| l.message_id > old_tip.collected) {
            ids.push(link.message_id);
            next = match self.follow(&link).await {
                Ok(doc) if doc.log_id == log_id => doc.prev,
                _ => None,
            };
        }
        if !ids.is_empty() {
            let _ = self.chat.delete(&ids).await;
        }
        if let (Some(g), Some(o)) = (garbage, self.owner.lock().unwrap().as_mut()) {
            o.tip.collected = o.tip.collected.max(g.message_id);
        }
        Ok(())
    }
}

/// What a read needs, detached from the store's borrow.
struct ReadCtx<C> {
    chat: Arc<C>,
    opts: ChatLogOptions,
}

impl<C: ChatApi> ReadCtx<C> {
    /// Entries `(from, head]`: walk back from the pinned head; a newer
    /// message's batch replaces older entries from its `first` on.
    async fn read_all(self, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
        if from >= head.position {
            return Ok(Vec::new());
        }
        let store = ChatLogStore { chat: self.chat, opts: self.opts, owner: Mutex::new(None) };
        let (tip, _) = store
            .head_doc(head.log_id)
            .await?
            .ok_or_else(|| LogError::Lost(format!("no pinned head, expected entries up to {}", head.position)))?;
        if tip.head_position() < head.position {
            return Err(LogError::Lost(format!(
                "the log ends at {}, before {}",
                tip.head_position(),
                head.position
            )));
        }
        let base_at = tip.base_at();
        if from < base_at {
            return Err(LogError::Lost(format!("entries after {from} collected up to base {base_at}")));
        }
        let mut out = std::collections::BTreeMap::new();
        let mut limit = Position::MAX;
        let mut doc = tip;
        loop {
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
            let Some(prev) = doc.prev else { break };
            doc = store.follow(&prev).await?;
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
