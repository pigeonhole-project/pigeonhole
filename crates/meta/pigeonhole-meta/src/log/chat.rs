//! A [`LogStore`] in a chat (Telegram), over two registers read with one call
//! (meta-backends.md § `telegram`):
//!
//! - **head** — the pinned message: the newest pinned message *by sending
//!   date*. Every log message is a document: a batch (or a snapshot) with its
//!   generation and a link to the previous log message, so the pinned head
//!   leads back through the whole chain to the last snapshot.
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
    Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position,
    Snapshot, LOG_FORMAT,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Body {
    Batch(Batch),
    /// The state as of `at`; the chain ends here.
    Snapshot { at: Position, data: Vec<u8> },
}

/// One log message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LogDoc {
    format: u32,
    log_id: LogId,
    gen: u64,
    prev: Option<Link>,
    body: Body,
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
        match &self.body {
            Body::Batch(b) => b.first + b.entries.len() as u64 - 1,
            Body::Snapshot { at, .. } => *at,
        }
    }
}

/// Head of the chain as this handle knows it.
#[derive(Debug, Clone)]
struct Tip {
    link: Option<Link>,
    position: Position,
}

struct Owner {
    gen: u64,
    log_id: LogId,
    tip: Tip,
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
        let link = Link { message_id: doc.message_id, download_ref: doc.download_ref.clone(), crc: crc32fast::hash(&data) };
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

    /// Send `body` linked to the owner's tip as generation `gen`, pin it, and
    /// prove the fence still holds `gen`. Overrides a newer straggler on top.
    async fn write(&self, gen: u64, body: impl Fn(&Tip) -> Body) -> LogResult<Tip> {
        let (log_id, mut tip) = {
            let owner = self.owner.lock().unwrap();
            let o = owner
                .as_ref()
                .filter(|o| o.gen == gen)
                .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;
            (o.log_id, o.tip.clone())
        };
        let mut body = body(&tip);
        // A straggler may top our pin; then link a newer (empty) batch to our
        // tip and pin that. Bounded: each fenced writer has at most one.
        for _ in 0..4 {
            let prev = match &body {
                Body::Snapshot { .. } => None,
                Body::Batch(_) => tip.link.clone(),
            };
            let doc = LogDoc { format: LOG_FORMAT, log_id, gen, prev, body };
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
            tip = Tip {
                link: Some(Link { message_id: sent.message_id, download_ref: sent.download_ref.clone(), crc }),
                position: doc.head_position(),
            };
            if let Some(o) = self.owner.lock().unwrap().as_mut() {
                o.tip = tip.clone();
            }
            if regs.pinned.as_ref().is_some_and(|p| p.message_id == sent.message_id) {
                return Ok(tip);
            }
            body = Body::Batch(Batch::new(gen, tip.position + 1, Vec::new()));
        }
        Err(LogError::Other(anyhow!("could not keep the log head pinned")))
    }

    /// Log messages from the head back to (and including) the first snapshot,
    /// or to the start of the log; newest first.
    async fn chain(&self, head: (LogDoc, Link), stop_at: Position) -> LogResult<Vec<(LogDoc, Link)>> {
        let mut out = vec![head];
        loop {
            let (doc, _) = out.last().unwrap();
            if matches!(doc.body, Body::Snapshot { .. }) || doc.head_position() <= stop_at {
                return Ok(out);
            }
            let Some(prev) = doc.prev.clone() else {
                return Ok(out);
            };
            let doc = self.follow(&prev).await?;
            out.push((doc, prev));
        }
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
            Tip { link: None, position: 0 }
        } else {
            match self.head_doc(log_id).await? {
                Some((doc, link)) => Tip { position: doc.head_position(), link: Some(link) },
                None => Tip { link: None, position: 0 },
            }
        };
        let position = tip.position;
        *self.owner.lock().unwrap() = Some(Owner { gen, log_id, tip });
        Ok(Head { log_id, position })
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        let entries = batch.to_vec();
        let tip = self
            .write(gen, |tip| Body::Batch(Batch::new(gen, tip.position + 1, entries.clone())))
            .await?;
        Ok(tip.position)
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        let Some((log_id, gen)) = parse_description(self.registers().await?.description.as_deref())? else {
            return Ok(None);
        };
        let Some(head) = self.head_doc(log_id).await? else {
            return Ok(Some(Discovered { snapshot: None, head: Head { log_id, position: 0 }, gen }));
        };
        let position = head.0.head_position();
        let chain = self.chain(head, 0).await?;
        let snapshot = chain.iter().find_map(|(doc, _)| match &doc.body {
            Body::Snapshot { at, data } => Some((*at, Snapshot { data: data.clone() })),
            Body::Batch(_) => None,
        });
        Ok(Some(Discovered { snapshot, head: Head { log_id, position }, gen }))
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let this = ReadCtx { chat: self.chat.clone(), opts: self.opts.clone() };
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

    async fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> LogResult<()> {
        let owned = {
            let owner = self.owner.lock().unwrap();
            owner.as_ref().filter(|o| o.gen == gen).map(|o| (o.tip.clone(), o.log_id))
        };
        let Some((old_tip, log_id)) = owned else {
            let regs = self.registers().await?;
            let current = parse_description(regs.description.as_deref())?.map_or(0, |(_, g)| g);
            return Err(LogError::Fenced { current });
        };
        if at != old_tip.position {
            return Err(LogError::Other(anyhow!(
                "a chat log snapshots only at its head ({}), not at {at}",
                old_tip.position
            )));
        }
        let data = snapshot.data;
        self.write(gen, move |_| Body::Snapshot { at, data: data.clone() }).await?;

        // Collect the chain behind the snapshot (best effort: a leftover
        // message is unreachable garbage, not a problem).
        if let Some(link) = old_tip.link {
            let mut ids = Vec::new();
            let mut next = Some(link);
            while let Some(link) = next {
                ids.push(link.message_id);
                next = match self.follow(&link).await {
                    Ok(doc) if doc.log_id == log_id => doc.prev,
                    _ => None,
                };
            }
            let _ = self.chat.delete(&ids).await;
        }
        Ok(())
    }
}

/// What a read stream needs, detached from the store's borrow.
struct ReadCtx<C> {
    chat: Arc<C>,
    opts: ChatLogOptions,
}

impl<C: ChatApi> ReadCtx<C> {
    async fn read_all(self, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
        let store = ChatLogStore { chat: self.chat, opts: self.opts, owner: Mutex::new(None) };
        if from >= head.position {
            return Ok(Vec::new());
        }
        let tip = store
            .head_doc(head.log_id)
            .await?
            .ok_or_else(|| LogError::Lost(format!("no pinned head, expected entries up to {}", head.position)))?;
        if tip.0.head_position() < head.position {
            return Err(LogError::Lost(format!(
                "the log ends at {}, before {}",
                tip.0.head_position(),
                head.position
            )));
        }
        let chain = store.chain(tip, from).await?;
        let mut out = Vec::new();
        let mut next = from + 1;
        for (doc, _) in chain.into_iter().rev() {
            match doc.body {
                Body::Snapshot { at, .. } => {
                    if at > from {
                        return Err(LogError::Lost(format!(
                            "entries after {from} collected up to snapshot {at}"
                        )));
                    }
                }
                Body::Batch(batch) => {
                    for (i, entry) in batch.entries.into_iter().enumerate() {
                        let pos = batch.first + i as u64;
                        if pos == next && pos <= head.position {
                            out.push((pos, entry));
                            next += 1;
                        }
                    }
                }
            }
        }
        if next <= head.position && from < head.position {
            return Err(LogError::Lost(format!("the chain ends before position {next}")));
        }
        Ok(out)
    }
}

#[cfg(test)]
pub(crate) mod fake;
#[cfg(test)]
mod tests;
