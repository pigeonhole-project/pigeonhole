//! [`Chain`] over a channel, decided by the **order** of message ids
//! (docs/chain-log.md, docs/telegram-mtproto.md § The log).
//!
//! A chain blob is a document named `<label>~<prev>.bin` (`prev` = the id of
//! the message it follows). The channel gives every
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
//! Members get deleted (a collection prunes below the log's base), the
//! successor of `p` among them: a writer deposed long ago, whose tip is `p`,
//! would then find nothing in `(p, b)` that names `p`. But the chain went on
//! past `p`, and what it went on with names blobs newer than `p`: a member
//! in `(p, b)` that names one is just as final a "no" (and its label tells
//! the writer who holds the log now).
//!
//! The first member follows the **anchor**, a message posted once, before
//! it, and never pruned: its id is the floor of the first member's range,
//! so a chain started on a channel with a long history reads the ids since
//! the anchor, not the channel. Of two stores posting an anchor at once the
//! older one is the anchor (the other deletes its own). The store finds it
//! once per process: when it walks the channel finding no member, or by
//! reading the channel up to the top before it posts one.
//!
//! The top of a channel is only learnt by posting: `tail` and `successor`
//! post a probe (kept: deleting it would leave an empty id in a range some
//! check reads). An empty id inside a checked range is read again a few
//! times (a message not visible yet shows up), then taken as deleted; only
//! the ids below the first member that decides the check are, since one
//! above it cannot change the answer.

use crate::channel::{ChannelApi, ChannelMessage, IDS_PER_CALL};
use crate::store::{blob_name, TelegramBlobStore, TelegramId};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_storage_api::chain_name::{chain_name, parse_chain_name};
use pigeonhole_storage_api::{
    check_blob, newest_loose_at_tail, put_after_tail_by_links, put_loose_linked, After, BlobBackend, Chain, Linked,
    MakeAfter,
};
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
const ANCHOR: &str = "chain-anchor.bin";

/// What the store knows of its chain's anchor.
#[derive(Debug, Default)]
pub(crate) struct Anchor {
    /// The anchor, once found.
    found: Option<i64>,
    /// The ids up to here were read and held no anchor.
    none_upto: i64,
}

impl Anchor {
    /// Learn from `view`, which holds every id in `[1, upto]`: its oldest
    /// anchor is the chain's; with none, there is none up to `upto`.
    fn learn(&mut self, view: &View, upto: i64) {
        if self.found.is_none() {
            self.found = view.range(..=upto).find(|(_, s)| **s == Seen::Anchor).map(|(id, _)| *id);
            self.none_upto = self.none_upto.max(upto);
        }
    }
}

/// What a page of the channel says, as far as the chain cares.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Empty,
    /// A chain blob: its label and what it follows.
    Member(String, i64),
    Probe,
    Anchor,
    Foreign,
}

fn seen(msg: &ChannelMessage) -> Seen {
    if *msg == ChannelMessage::Empty {
        return Seen::Empty;
    }
    match blob_name(msg) {
        Some(PROBE) => Seen::Probe,
        Some(ANCHOR) => Seen::Anchor,
        Some(name) => match parse_chain_name(name) {
            Some((label, prev)) => Seen::Member(label.to_string(), prev),
            None => Seen::Foreign,
        },
        None => Seen::Foreign,
    }
}

type View = BTreeMap<i64, Seen>;

/// The member of `(prev, hi)` that decides what follows `prev`, by id:
/// the first that names `prev`, else the first that names a newer blob.
fn decisive(view: &View, prev: i64, hi: i64) -> Option<(i64, String)> {
    let first = |follows: &dyn Fn(i64) -> bool| {
        view.range(prev + 1..hi).find_map(|(id, s)| match s {
            Seen::Member(label, p) if follows(*p) => Some((*id, label.clone())),
            _ => None,
        })
    };
    first(&|p| p == prev).or_else(|| first(&|p| p > prev))
}

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

    /// Read the empty ids of `(lo, bound)` again a few times, where
    /// `bound` is what the view says the check needs read (it can only fall
    /// as messages show up): a message not visible yet shows up; one that
    /// stays empty was deleted.
    async fn settle(&self, view: &mut View, lo: i64, bound: impl Fn(&View) -> i64) -> Result<()> {
        for attempt in 0..=self.chain.empty_retries {
            let hi = bound(view);
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

    /// What `prev` is followed by in `(prev, hi)`, read and settled: the
    /// first member that names it, else the first that names a newer one
    /// (the chain went on past `prev`; its successor was deleted). Only the
    /// empty ids below that member are settled: one above cannot be first.
    async fn first_naming(&self, view: &mut View, prev: i64, hi: i64) -> Result<Option<Linked<TelegramId>>> {
        self.fill(view, prev + 1, hi - 1).await?;
        self.settle(view, prev, |view| decisive(view, prev, hi).map_or(hi, |(id, _)| id)).await?;
        Ok(decisive(view, prev, hi).map(|(id, label)| Linked { id: TelegramId { message_id: id }, label }))
    }

    /// The chain's anchor: found once per process, posted if the channel
    /// has none. The first member's range starts at it.
    async fn anchor(&self) -> Result<i64> {
        let mut known = self.anchor.lock().await;
        if let Some(a) = known.found {
            return Ok(a);
        }
        let mut view = View::new();
        if known.none_upto == 0 {
            // Never looked in this process: read the channel up to its top.
            let top = self.top().await?;
            self.fill(&mut view, 1, top - 1).await?;
            known.learn(&view, top - 1);
            if let Some(a) = known.found {
                return Ok(a);
            }
        }
        let looked = known.none_upto;
        let mine = self.post(ANCHOR, Bytes::from_static(&[0])).await?;
        // Another store may have posted one since: the oldest is the anchor.
        self.fill(&mut view, looked + 1, mine - 1).await?;
        self.settle(&mut view, looked, |_| mine).await?;
        known.learn(&view, mine);
        let anchor = *known.found.get_or_insert(mine);
        if anchor != mine {
            if let Err(e) = self.delete(&[mine]).await {
                tracing::debug!(store = %self.instance().id, message_id = mine, error = %format!("{e:#}"), "a second anchor stays");
            }
        }
        tracing::debug!(store = %self.instance().id, anchor, mine, "the chain's anchor");
        Ok(anchor)
    }

    /// Post a blob (text if it fits); its id.
    async fn post(&self, name: &str, data: Bytes) -> Result<i64> {
        // One random_id per put (a retry inside the call posts once); two
        // writers putting the same name must still get two messages.
        let random_id = uuid::Uuid::new_v4().as_u64_pair().0 as i64;
        self.post_blob(name, data, random_id).await
    }

    /// The top of the channel: the id of a probe posted now.
    async fn top(&self) -> Result<i64> {
        self.post(PROBE, Bytes::from_static(&[0])).await
    }
}

#[async_trait]
impl<C: ChannelApi> Chain for TelegramBlobStore<C> {
    async fn put_after(&self, prev: Option<&TelegramId>, label: &str, data: Bytes) -> Result<After<TelegramId>> {
        check_blob(label, &data, self.limits())?;
        // The first member follows the anchor.
        let prev = match prev {
            Some(p) => p.message_id,
            None => self.anchor().await?,
        };
        let b = self.post(&chain_name(label, prev), data).await?;
        if b <= prev {
            bail!("the channel gave message id {b}, not above {prev}");
        }
        let store = &self.instance().id;
        if b == prev + 1 {
            tracing::debug!(%store, label, prev, message_id = b, "linked (next id, no read)");
            return Ok(After::Won(TelegramId { message_id: b }));
        }
        match self.first_naming(&mut View::new(), prev, b).await? {
            Some(winner) => {
                tracing::debug!(%store, label, prev, message_id = b, winner = winner.id.message_id, winner_label = %winner.label, "lost the link");
                Ok(After::Lost(winner))
            }
            None => {
                tracing::debug!(%store, label, prev, message_id = b, "linked");
                Ok(After::Won(TelegramId { message_id: b }))
            }
        }
    }

    /// The newest member that won its link, walking down from the top to
    /// the anchor (to the bottom while it is not known; a walk that finds no
    /// member learns where the anchor is).
    async fn tail(&self) -> Result<Option<Linked<TelegramId>>> {
        let top = self.top().await?;
        let floor = self.anchor.lock().await.found.unwrap_or(0);
        let mut view = View::new();
        let mut id = top - 1;
        while id > floor {
            self.fill(&mut view, (id - IDS_PER_CALL as i64 + 1).max(floor + 1), id).await?;
            if let Some(Seen::Member(label, prev)) = view.get(&id).cloned() {
                // A member that won its link is in the chain; the newest is the tail.
                if self.first_naming(&mut view, prev, id).await?.is_none() {
                    tracing::debug!(store = %self.instance().id, message_id = id, %label, top, "found the tail");
                    return Ok(Some(Linked { id: TelegramId { message_id: id }, label }));
                }
            }
            id -= 1;
        }
        if floor == 0 {
            self.anchor.lock().await.learn(&view, top - 1);
        }
        Ok(None)
    }

    async fn successor(&self, id: &TelegramId) -> Result<Option<Linked<TelegramId>>> {
        let top = self.top().await?;
        self.first_naming(&mut View::new(), id.message_id, top).await
    }

    /// Our messages (members, losers, probes) below `below`, walking down
    /// until a page holds none of ours: what lies further down was pruned
    /// before. The anchor stays.
    async fn prune(&self, below: &TelegramId) -> Result<()> {
        let mut hi = below.message_id - 1;
        while hi >= 1 {
            // Background work: only on spare budget, never ahead of appends.
            self.limiter.wait_idle(crate::limits::DELETE).await;
            let lo = (hi - IDS_PER_CALL as i64 + 1).max(1);
            let mut view = View::new();
            self.page(&mut view, lo, hi).await?;
            let ours: Vec<i64> =
                view.iter().filter(|(_, s)| matches!(s, Seen::Member(..) | Seen::Probe)).map(|(id, _)| *id).collect();
            if ours.is_empty() {
                break;
            }
            let keys: Vec<i64> = ours;
            self.delete(&keys).await.map_err(|e| anyhow!("prune {lo}..={hi}: {e:#}"))?;
            tracing::debug!(store = %self.instance().id, lo, hi, deleted = keys.len(), "pruned");
            hi = lo - 1;
        }
        Ok(())
    }

    async fn put_after_tail(
        &self,
        hint: Option<&TelegramId>,
        label: &str,
        make: &MakeAfter<'_, TelegramId>,
    ) -> Result<Option<TelegramId>> {
        put_after_tail_by_links(self, hint, label, make).await
    }

    /// Linked: a bot cannot list the channel, it finds the newest blob by
    /// its links. After `after` when given: no probe to learn the top.
    async fn put_loose(&self, after: Option<&TelegramId>, label: &str, data: Bytes) -> Result<TelegramId> {
        put_loose_linked(self, after, label, data).await
    }

    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<TelegramId>>> {
        newest_loose_at_tail(self, prefix).await
    }
}
