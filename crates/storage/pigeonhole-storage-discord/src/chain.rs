//! [`Chain`] over a channel, decided by **threads** (docs/discord-log.md,
//! docs/chain-log.md).
//!
//! A message can start at most one thread (the thread takes its id; a second
//! attempt answers `160004`): a create-once operation, so a compare-and-set
//! on each message's "next" pointer. A put after `p` posts the blob, then
//! starts a thread on `p` named `next <id> <label>`; created → the blob won,
//! taken → `p`'s thread names the winner. Snowflake order is never used.
//!
//! The first blob of a chain links from the **anchor**, a message the store
//! posts once (the oldest one wins if two race to post it) and never prunes.
//! A chain blob is named `<label>~<prev>.bin`, as on Telegram (the first
//! names the anchor).
//!
//! The tail is the newest member that won its link: the thread of the
//! message it names as `prev` names it back. A thread outlives its starter
//! and stays readable, so when members are deleted (pruning, a removed
//! snapshot) the link into the first surviving member is kept: it still
//! tells that member won. Every other link of a deleted member goes. A
//! thread is only ever deleted after its message: on a message that still
//! exists a new thread could be started (the compare-and-set would reopen),
//! on a deleted one it cannot. The anchor and its link stay.
//!
//! Links are archived right away (archived threads still answer `160004`;
//! the guild's active-thread limit). Starting a thread more than ~4 messages
//! back posts a "thread created" system message (type 18): skipped like any
//! foreign message.

use crate::api::{DcMessage, DiscordApi, ThreadStart, PAGE};

use crate::store::{blob_name, DiscordBlobStore, DiscordId};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_storage_api::chain_name::{chain_name, label_of, parse_chain_name};
use pigeonhole_storage_api::{
    check_blob, put_after_tail_by_links, After, BlobBackend, Chain, LeaseChain, Linked, MakeAfter,
};
use std::collections::HashMap;

const ANCHOR: &str = "chain-anchor.bin";
/// The text of a clock probe ([`LeaseChain::now_ms`]).
const PROBE: &str = "clock probe";
/// Thread names hold 100 characters.
const THREAD_NAME_MAX: usize = 100;

/// A chain blob's label and what it follows.
fn parse_name(name: &str) -> Option<(&str, u64)> {
    parse_chain_name(name)
}

fn thread_name(next: u64, label: &str) -> String {
    format!("next {next} {label}")
}

/// The successor a link thread names: its id and label.
fn parse_thread(name: &str) -> Option<(u64, &str)> {
    let rest = name.strip_prefix("next ")?;
    let (id, label) = rest.split_once(' ')?;
    Some((id.parse().ok()?, label))
}

fn is_member(m: &DcMessage) -> bool {
    blob_name(m).and_then(parse_name).is_some()
}

fn is_anchor(m: &DcMessage) -> bool {
    blob_name(m) == Some(ANCHOR)
}

impl<A: DiscordApi> DiscordBlobStore<A> {
    fn nonce() -> String {
        format!("c{}", &uuid::Uuid::new_v4().simple().to_string()[..24])
    }

    /// The oldest anchor message in the channel, if any.
    async fn find_anchor(&self) -> Result<Option<u64>> {
        let mut after = 0;
        loop {
            let page = self.api.list(None, Some(after), PAGE).await?;
            if let Some(m) = page.iter().find(|m| is_anchor(m)) {
                return Ok(Some(m.id));
            }
            match page.last() {
                Some(m) if page.len() == PAGE => after = m.id,
                _ => return Ok(None),
            }
        }
    }

    /// The anchor the first blob links from; posted if the channel has none.
    async fn anchor(&self) -> Result<u64> {
        let mut known = self.anchor.lock().await;
        if let Some(a) = *known {
            return Ok(a);
        }
        let found = match self.find_anchor().await? {
            Some(a) => a,
            None => {
                let mine = self.post_blob(ANCHOR, Bytes::from_static(&[0]), &Self::nonce()).await?.id;
                tracing::debug!(store = %self.instance().id, message_id = mine, "posted the chain's anchor");
                // Two stores may post one at once: the oldest is the anchor.
                let oldest = self.find_anchor().await?.context("the anchor just posted is not listed")?;
                if oldest != mine {
                    self.api.delete_messages(&[mine]).await.ok();
                }
                oldest
            }
        };
        *known = Some(found);
        Ok(found)
    }

    /// After chain members `gone` were deleted: drop the links that no
    /// longer lead to a surviving member. A member's incoming link (on its
    /// `prev`) goes once `prev` is gone too; its outgoing link goes once its
    /// successor is gone. So only the link into the first survivor stays.
    pub(crate) async fn retire_links(&self, gone: &[u64]) -> Result<()> {
        let anchor = *self.anchor.lock().await;
        let gone_set: std::collections::HashSet<u64> = gone.iter().copied().collect();
        let exists = |id: u64| {
            let api = self.api.clone();
            let gone = gone_set.contains(&id);
            async move { Ok::<bool, anyhow::Error>(!gone && api.get_message(id).await?.is_some()) }
        };
        for &m in gone {
            let name = self.names.lock().unwrap().pop(&m);
            let Some((_, p)) = name.as_deref().and_then(parse_name) else { continue };
            // Incoming: the link on `prev`, if it names `m` (gone). An
            // unlinked blob or a loser names `prev` too, while `prev`'s link
            // names another: maybe the first survivor, which it tells won.
            let into_m = self.link_of(p).await?.is_some_and(|l| l.id.message_id == m);
            if into_m && Some(p) != anchor && !exists(p).await? {
                self.api.delete_thread(p).await?;
                tracing::debug!(store = %self.instance().id, thread = p, "retired a link");
            }
            // Outgoing: the link on `m` (gone) to a successor that is gone too.
            if let Some(next) = self.link_of(m).await? {
                if !exists(next.id.message_id).await? {
                    self.api.delete_thread(m).await?;
                    tracing::debug!(store = %self.instance().id, thread = m, "retired a link");
                }
            }
        }
        Ok(())
    }

    /// What `p`'s thread names, if it has one (also after `p` is gone).
    async fn link_of(&self, p: u64) -> Result<Option<Linked<DiscordId>>> {
        let name = self.api.thread_name(p).await?;
        Ok(name
            .as_deref()
            .and_then(parse_thread)
            .map(|(id, label)| Linked { id: DiscordId { message_id: id }, label: label.to_string() }))
    }
}

#[async_trait]
impl<A: DiscordApi> Chain for DiscordBlobStore<A> {
    async fn put_after(&self, prev: Option<&DiscordId>, label: &str, data: Bytes) -> Result<After<DiscordId>> {
        check_blob(label, &data, self.limits())?;
        // The first member follows the anchor.
        let from = match prev {
            Some(p) => p.message_id,
            None => self.anchor().await?,
        };
        let b = self.post_blob(&chain_name(label, from), data, &Self::nonce()).await?.id;
        let name = thread_name(b, label);
        if name.chars().count() > THREAD_NAME_MAX {
            bail!("thread name {name:?} is over {THREAD_NAME_MAX} characters");
        }
        let store = &self.instance().id;
        match self.api.start_thread(from, &name).await? {
            ThreadStart::Created => {
                tracing::debug!(%store, label, from, message_id = b, "linked (thread created)");
                // Off the append's path: an archived link still answers
                // 160004; archiving only keeps the guild's active threads low.
                self.archive_later(from);
                Ok(After::Won(DiscordId { message_id: b }))
            }
            ThreadStart::Taken => {
                let winner =
                    self.link_of(from).await?.with_context(|| format!("message {from} has an unreadable link"))?;
                // Our own link: an attempt of this start made the thread, but
                // its answer was lost and the client's retry was told 160004.
                if winner.id.message_id == b {
                    tracing::debug!(%store, label, from, message_id = b, "linked (thread taken by our own retried start)");
                    self.archive_later(from);
                    return Ok(After::Won(DiscordId { message_id: b }));
                }
                tracing::debug!(%store, label, from, message_id = b, winner = winner.id.message_id, winner_label = %winner.label, "lost the link (thread taken)");
                Ok(After::Lost(winner))
            }
            // A deposed writer's tip, pruned below a newer base: a link on
            // it names its successor; once that is pruned too, the chain went
            // on past it to the tail.
            ThreadStart::NoMessage => {
                let winner = match self.link_of(from).await? {
                    Some(next) => Some(next),
                    None => self.tail().await?.filter(|t| t.id.message_id > from),
                };
                let winner = winner.with_context(|| format!("message {from} to link from is gone"))?;
                tracing::debug!(%store, label, from, message_id = b, winner = winner.id.message_id, winner_label = %winner.label, "lost the link (message gone)");
                Ok(After::Lost(winner))
            }
        }
    }

    /// The newest member that won its link (its `prev`'s thread names it):
    /// every winner is in the chain (a writer puts only after a winner or
    /// the tail), and a successor would be newer. A listing carries each
    /// message's thread, so `prev`'s is read from the pages listed; only a
    /// `prev` not among them (deleted, or further down) costs a read of its
    /// thread.
    async fn tail(&self) -> Result<Option<Linked<DiscordId>>> {
        let mut before = None;
        // The thread (or none) of every message listed so far.
        let mut listed: HashMap<u64, Option<String>> = HashMap::new();
        loop {
            let page = self.api.list(before, None, PAGE).await?;
            listed.extend(page.iter().map(|m| (m.id, m.thread.clone())));
            for m in page.iter().rev() {
                let Some((label, from)) = blob_name(m).and_then(parse_name) else { continue };
                let link = match listed.get(&from) {
                    Some(thread) => thread.as_deref().and_then(parse_thread).map(|(id, _)| id),
                    None => self.link_of(from).await?.map(|l| l.id.message_id),
                };
                if link == Some(m.id) {
                    tracing::debug!(store = %self.instance().id, message_id = m.id, label, "found the tail");
                    self.remember(m);
                    return Ok(Some(Linked { id: DiscordId { message_id: m.id }, label: label.to_string() }));
                }
            }
            match page.first() {
                Some(m) if page.len() == PAGE => before = Some(m.id),
                _ => return Ok(None),
            }
        }
    }

    async fn successor(&self, id: &DiscordId) -> Result<Option<Linked<DiscordId>>> {
        self.link_of(id.message_id).await
    }

    /// A plain message: a channel lists, so the newest is found without a
    /// link (whose thread the pacer would hold for seconds): `after` is not
    /// needed.
    async fn put_loose(&self, _after: Option<&DiscordId>, label: &str, data: Bytes) -> Result<DiscordId> {
        check_blob(label, &data, self.limits())?;
        let m = self.post_blob(label, data, &Self::nonce()).await?;
        Ok(DiscordId { message_id: m.id })
    }

    /// The newest message whose name starts with `prefix`, loose or linked.
    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<DiscordId>>> {
        let mut before = None;
        loop {
            let page = self.api.list(before, None, PAGE).await?;
            for m in page.iter().rev() {
                let Some(name) = blob_name(m).filter(|n| n.starts_with(prefix)) else { continue };
                let label = label_of(name).to_string();
                self.remember(m);
                return Ok(Some(Linked { id: DiscordId { message_id: m.id }, label }));
            }
            match page.first() {
                Some(m) if page.len() == PAGE => before = Some(m.id),
                _ => return Ok(None),
            }
        }
    }

    async fn put_after_tail(
        &self,
        hint: Option<&DiscordId>,
        label: &str,
        make: &MakeAfter<'_, DiscordId>,
    ) -> Result<Option<DiscordId>> {
        put_after_tail_by_links(self, hint, label, make).await
    }

    /// Our messages below `below` (not the anchor), walking down until a page
    /// holds none of ours, and their links but the one into `below`.
    async fn prune(&self, below: &DiscordId) -> Result<()> {
        let mut before = Some(below.message_id);
        loop {
            // Background work: only on spare budget, never ahead of appends.
            self.limiter.wait_idle(crate::limits::DELETE).await;
            let page = self.api.list(before, None, PAGE).await?;
            let ours: Vec<&DcMessage> = page.iter().filter(|m| is_member(m)).collect();
            if ours.is_empty() {
                return Ok(());
            }
            for m in &ours {
                self.remember(m);
            }
            let ids: Vec<u64> = ours.iter().map(|m| m.id).collect();
            self.api.delete_messages(&ids).await?;
            tracing::debug!(store = %self.instance().id, below = below.message_id, deleted = ids.len(), "pruned");
            self.retire_links(&ids).await?;
            match page.first() {
                Some(m) if page.len() == PAGE => before = Some(m.id),
                _ => return Ok(()),
            }
        }
    }
}

#[async_trait]
impl<A: DiscordApi> LeaseChain for DiscordBlobStore<A> {
    /// A snowflake's millisecond timestamp.
    fn stamp_ms(&self, id: &DiscordId) -> u64 {
        crate::snowflake::snowflake_timestamp_ms(id.message_id)
    }

    /// A probe: a plain text message (no blob, so no reader takes it for
    /// one), its snowflake read, deleted right after. The `Date` header has
    /// whole seconds only (docs/discord-lease.md § Spike findings).
    async fn now_ms(&self) -> Result<u64> {
        let probe = self.api.send(PROBE, None, &Self::nonce()).await?.id;
        if let Err(e) = self.api.delete_messages(&[probe]).await {
            tracing::debug!(store = %self.instance().id, message_id = probe, error = %format!("{e:#}"), "a clock probe stays");
        }
        Ok(crate::snowflake::snowflake_timestamp_ms(probe))
    }

    /// Named like a member (`<label>~<prev>.bin`), so pruned like one, but
    /// with no thread: one POST.
    async fn put_unlinked(&self, prev: &DiscordId, label: &str, data: Bytes) -> Result<DiscordId> {
        check_blob(label, &data, self.limits())?;
        let m = self.post_blob(&chain_name(label, prev.message_id), data, &Self::nonce()).await?;
        tracing::debug!(store = %self.instance().id, label, prev = prev.message_id, message_id = m.id, "put unlinked");
        Ok(DiscordId { message_id: m.id })
    }

    async fn list_after(&self, after: Option<&DiscordId>, prefix: &str) -> Result<Vec<Linked<DiscordId>>> {
        let mut after = after.map_or(0, |a| a.message_id);
        let mut out = Vec::new();
        loop {
            let page = self.api.list(None, Some(after), PAGE).await?;
            for m in &page {
                let Some(name) = blob_name(m).filter(|n| n.starts_with(prefix)) else { continue };
                let label = label_of(name).to_string();
                self.remember(m);
                out.push(Linked { id: DiscordId { message_id: m.id }, label });
            }
            match page.last() {
                Some(m) if page.len() == PAGE => after = m.id,
                _ => return Ok(out),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_threads_roundtrip() {
        assert_eq!(parse_name(ANCHOR), None);
        assert_eq!(parse_thread(&thread_name(77, "log-batch-2")), Some((77, "log-batch-2")));
        assert_eq!(parse_thread("hello"), None);
    }
}
