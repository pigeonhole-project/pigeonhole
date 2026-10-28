//! [`Chain`] over a channel, decided by the **order** of message ids
//! (docs/chain-log.md, docs/telegram-mtproto.md § The log).
//!
//! A chain blob is a document named `<label>~<prev>.bin` (`prev` = the id of
//! the message it follows, `root` for the first). The channel gives every
//! message an id, atomically and in increasing order, and any reader sees
//! that order: the successor of `p` is the **first** message, by id, that
//! names `p`. A message names a `p` that already existed, so a put after `p`
//! that lands at `b` wins iff nothing in `(p, b)` names `p` — and when
//! `b == p + 1` nothing can lie in between: no read at all.
//!
//! A writer only puts after a blob it won (or found as the tail), so every
//! winner is in the chain and the tail is the newest winner: found walking
//! down from the top, checking each candidate's own range.
//!
//! The top of a channel is only learnt by posting: `tail` and `successor`
//! post a probe (kept: deleting it would leave an empty id in a range some
//! check reads). An empty id inside a checked range is read again a few
//! times (a message not visible yet shows up), then taken as deleted.

use crate::channel::{ChannelApi, ChannelMessage, IDS_PER_CALL};
use crate::store::{Located, TelegramBlobStore, TelegramId};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{After, BlobBackend, Chain, Linked};
use std::collections::BTreeMap;
use std::time::Duration;

/// How a chain treats ids that read empty inside a range it checks.
#[derive(Debug, Clone)]
pub struct ChainOptions {
    /// Read such an id again this often…
    pub empty_retries: u32,
    /// …this far apart, then take it as deleted.
    pub empty_retry_delay: Duration,
}

impl Default for ChainOptions {
    fn default() -> Self {
        Self { empty_retries: 5, empty_retry_delay: Duration::from_secs(2) }
    }
}

const PROBE: &str = "chain-probe.bin";

fn chain_name(label: &str, prev: Option<i64>) -> String {
    match prev {
        Some(p) => format!("{label}~{p}.bin"),
        None => format!("{label}~root.bin"),
    }
}

/// A chain blob's label and the id it follows (`None`: the first).
fn parse_name(name: &str) -> Option<(&str, Option<i64>)> {
    let (label, prev) = name.strip_suffix(".bin")?.rsplit_once('~')?;
    match prev {
        "root" => Some((label, None)),
        p => p.parse().ok().map(|p| (label, Some(p))),
    }
}

/// What a page of the channel says, as far as the chain cares.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Empty,
    /// A chain blob: its label and what it follows.
    Member(String, Option<i64>),
    Probe,
    Foreign,
}

fn seen(msg: &ChannelMessage) -> Seen {
    match msg {
        ChannelMessage::Empty => Seen::Empty,
        ChannelMessage::Other => Seen::Foreign,
        ChannelMessage::Document { name, .. } if name == PROBE => Seen::Probe,
        ChannelMessage::Document { name, .. } => match parse_name(name) {
            Some((label, prev)) => Seen::Member(label.to_string(), prev),
            None => Seen::Foreign,
        },
    }
}

type View = BTreeMap<i64, Seen>;

impl<C: ChannelApi> TelegramBlobStore<C> {
    async fn page(&self, view: &mut View, lo: i64, hi: i64) -> Result<()> {
        let ids: Vec<i64> = (lo..=hi).collect();
        let msgs = self.reader.read(&ids).await?;
        if msgs.len() != ids.len() {
            bail!("getMessages answered {} of {} ids", msgs.len(), ids.len());
        }
        view.extend(ids.into_iter().zip(msgs.iter().map(seen)));
        Ok(())
    }

    /// Make sure `view` holds every id in `[lo, hi]`, reading whole pages
    /// down from the highest missing id.
    async fn fill(&self, view: &mut View, lo: i64, hi: i64) -> Result<()> {
        let mut top = hi;
        while top >= lo.max(1) {
            if view.contains_key(&top) {
                top -= 1;
                continue;
            }
            let bottom = (top - IDS_PER_CALL as i64 + 1).max(1);
            self.page(view, bottom, top).await?;
            top = bottom - 1;
        }
        Ok(())
    }

    /// Read the empty ids of `(lo, hi)` again a few times: a message not
    /// visible yet shows up; one that stays empty was deleted.
    async fn settle(&self, view: &mut View, lo: i64, hi: i64) -> Result<()> {
        for attempt in 0..=self.chain.empty_retries {
            let empty: Vec<i64> =
                view.range(lo + 1..hi).filter(|(_, s)| **s == Seen::Empty).map(|(id, _)| *id).collect();
            if empty.is_empty() {
                return Ok(());
            }
            if attempt == self.chain.empty_retries {
                tracing::debug!(lo, hi, deleted = ?empty, "empty ids in a checked range: taken as deleted");
                return Ok(());
            }
            tokio::time::sleep(self.chain.empty_retry_delay).await;
            for ids in empty.chunks(IDS_PER_CALL) {
                let msgs = self.reader.read(ids).await?;
                view.extend(ids.iter().copied().zip(msgs.iter().map(seen)));
            }
        }
        Ok(())
    }

    /// The first member in `(lo, hi)` that names `prev`, read and settled.
    async fn first_naming(&self, view: &mut View, lo: i64, hi: i64, prev: Option<i64>) -> Result<Option<Linked<TelegramId>>> {
        self.fill(view, lo + 1, hi - 1).await?;
        self.settle(view, lo, hi).await?;
        Ok(view.range(lo + 1..hi).find_map(|(id, s)| match s {
            Seen::Member(label, p) if *p == prev => {
                Some(Linked { id: TelegramId { message_id: *id }, label: label.clone() })
            }
            _ => None,
        }))
    }

    /// Post a document; its id.
    async fn post(&self, name: &str, data: Bytes) -> Result<i64> {
        let size = data.len() as u64;
        // One random_id per put (a retry inside the call posts once); two
        // writers putting the same name must still get two messages.
        let random_id = uuid::Uuid::new_v4().as_u64_pair().0 as i64;
        let sent = self.reader.channel.send_document(name, data, random_id).await?;
        if let Some(file) = sent.file {
            self.reader.remember(sent.message_id, Located { size, file });
        }
        Ok(sent.message_id)
    }

    /// The top of the channel: the id of a probe posted now.
    async fn top(&self) -> Result<i64> {
        let _upload = self.limiter.acquire_upload().await;
        self.post(PROBE, Bytes::from_static(&[0])).await
    }
}

#[async_trait]
impl<C: ChannelApi> Chain for TelegramBlobStore<C> {
    async fn put_after(&self, prev: Option<&TelegramId>, label: &str, data: Bytes) -> Result<After<TelegramId>> {
        if label.contains('~') {
            bail!("a chain label may not contain '~': {label:?}");
        }
        if data.is_empty() || data.len() > self.limits().max_blob_size {
            bail!("chain blob of {} bytes outside (0, {}]", data.len(), self.limits().max_blob_size);
        }
        let prev = prev.map(|p| p.message_id);
        let b = {
            let _upload = self.limiter.acquire_upload().await;
            self.post(&chain_name(label, prev), data).await?
        };
        let lo = prev.unwrap_or(0);
        if b <= lo {
            bail!("the channel gave message id {b}, not above {lo}");
        }
        if prev.is_some() && b == lo + 1 {
            return Ok(After::Won(TelegramId { message_id: b }));
        }
        match self.first_naming(&mut View::new(), lo, b, prev).await? {
            Some(winner) => Ok(After::Lost(winner)),
            None => Ok(After::Won(TelegramId { message_id: b })),
        }
    }

    async fn tail(&self) -> Result<Option<Linked<TelegramId>>> {
        let top = self.top().await?;
        let mut view = View::new();
        let mut id = top - 1;
        while id >= 1 {
            self.fill(&mut view, (id - IDS_PER_CALL as i64 + 1).max(1), id).await?;
            if let Some(Seen::Member(label, prev)) = view.get(&id).cloned() {
                // A member that won its link is in the chain; the newest is the tail.
                if self.first_naming(&mut view, prev.unwrap_or(0), id, prev).await?.is_none() {
                    return Ok(Some(Linked { id: TelegramId { message_id: id }, label }));
                }
            }
            id -= 1;
        }
        Ok(None)
    }

    async fn successor(&self, id: &TelegramId) -> Result<Option<Linked<TelegramId>>> {
        let top = self.top().await?;
        let at = id.message_id;
        self.first_naming(&mut View::new(), at, top, Some(at)).await
    }

    /// Our messages (members, losers, probes) below `below`, walking down
    /// until a page holds none of ours: what lies further down was pruned
    /// before.
    async fn prune(&self, below: &TelegramId) -> Result<()> {
        let mut hi = below.message_id - 1;
        while hi >= 1 {
            let lo = (hi - IDS_PER_CALL as i64 + 1).max(1);
            let mut view = View::new();
            self.page(&mut view, lo, hi).await?;
            let ours: Vec<i64> = view
                .iter()
                .filter(|(_, s)| matches!(s, Seen::Member(..) | Seen::Probe))
                .map(|(id, _)| *id)
                .collect();
            if ours.is_empty() {
                break;
            }
            let keys: Vec<i64> = ours;
            self.delete(&keys).await.map_err(|e| anyhow!("prune {lo}..={hi}: {e:#}"))?;
            hi = lo - 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_roundtrip() {
        assert_eq!(chain_name("log-batch-3", Some(41)), "log-batch-3~41.bin");
        assert_eq!(parse_name("log-batch-3~41.bin"), Some(("log-batch-3", Some(41))));
        assert_eq!(parse_name("log-claim-1~root.bin"), Some(("log-claim-1", None)));
        for foreign in ["ph-00.bin", "photo.jpg", "x~y.bin", PROBE] {
            assert_eq!(parse_name(foreign), None, "{foreign}");
        }
    }
}
