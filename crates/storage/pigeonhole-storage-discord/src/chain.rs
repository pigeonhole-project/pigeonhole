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
//! A chain blob is named `<label>~<prev>.bin`, as on Telegram.
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
use pigeonhole_blob::{After, Chain, Linked};

const ANCHOR: &str = "chain-anchor.bin";
/// Thread names hold 100 characters.
const THREAD_NAME_MAX: usize = 100;

fn chain_name(label: &str, prev: Option<u64>) -> String {
    match prev {
        Some(p) => format!("{label}~{p}.bin"),
        None => format!("{label}~root.bin"),
    }
}

/// A chain blob's label and what it follows (`None`: the anchor).
fn parse_name(name: &str) -> Option<(&str, Option<u64>)> {
    let (label, prev) = name.strip_suffix(".bin")?.rsplit_once('~')?;
    match prev {
        "root" => Some((label, None)),
        p => p.parse().ok().map(|p| (label, Some(p))),
    }
}

/// A chain blob's label (`None` for anything else).
fn label_of(name: &str) -> Option<&str> {
    parse_name(name).map(|(label, _)| label)
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
    blob_name(m).and_then(label_of).is_some()
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
            let Some((_, prev)) = name.as_deref().and_then(parse_name) else { continue };
            // Incoming: the link on `prev` names `m`, which is gone.
            if let Some(p) = prev {
                if Some(p) != anchor && !exists(p).await? {
                    self.api.delete_thread(p).await?;
                }
            }
            // Outgoing: the link on `m` (gone) to a successor that is gone too.
            if let Some(next) = self.link_of(m).await? {
                if !exists(next.id.message_id).await? {
                    self.api.delete_thread(m).await?;
                }
            }
        }
        Ok(())
    }

    /// What `p`'s thread names, if it has one (also after `p` is gone).
    async fn link_of(&self, p: u64) -> Result<Option<Linked<DiscordId>>> {
        let name = self.api.thread_name(p).await?;
        Ok(name.as_deref().and_then(parse_thread).map(|(id, label)| Linked {
            id: DiscordId { message_id: id },
            label: label.to_string(),
        }))
    }
}

#[async_trait]
impl<A: DiscordApi> Chain for DiscordBlobStore<A> {
    async fn put_after(&self, prev: Option<&DiscordId>, label: &str, data: Bytes) -> Result<After<DiscordId>> {
        if label.contains(['~', ' ']) {
            bail!("a chain label may not contain '~' or spaces: {label:?}");
        }
        let prev = prev.map(|p| p.message_id);
        let from = match prev {
            Some(p) => p,
            None => self.anchor().await?,
        };
        let b = self.post_blob(&chain_name(label, prev), data, &Self::nonce()).await?.id;
        let name = thread_name(b, label);
        if name.chars().count() > THREAD_NAME_MAX {
            bail!("thread name {name:?} is over {THREAD_NAME_MAX} characters");
        }
        match self.api.start_thread(from, &name).await? {
            ThreadStart::Created => {
                if let Err(e) = self.api.archive_thread(from).await {
                    tracing::debug!(error = %format!("{e:#}"), from, "archiving a link failed");
                }
                Ok(After::Won(DiscordId { message_id: b }))
            }
            ThreadStart::Taken => {
                let winner = self.link_of(from).await?.with_context(|| format!("message {from} has an unreadable link"))?;
                Ok(After::Lost(winner))
            }
            ThreadStart::NoMessage => bail!("message {from} to link from is gone"),
        }
    }

    /// The newest member that won its link (its `prev`'s thread names it):
    /// every winner is in the chain (a writer puts only after a winner or
    /// the tail), and a successor would be newer.
    async fn tail(&self) -> Result<Option<Linked<DiscordId>>> {
        let mut before = None;
        loop {
            let page = self.api.list(before, None, PAGE).await?;
            for m in page.iter().rev() {
                let Some((label, prev)) = blob_name(m).and_then(parse_name) else { continue };
                let from = match prev {
                    Some(p) => p,
                    None => match *self.anchor.lock().await {
                        Some(a) => a,
                        None => match self.find_anchor().await? {
                            Some(a) => a,
                            None => continue,
                        },
                    },
                };
                if self.link_of(from).await?.is_some_and(|l| l.id.message_id == m.id) {
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

    /// Our messages below `below` (not the anchor), walking down until a page
    /// holds none of ours, and their links but the one into `below`.
    async fn prune(&self, below: &DiscordId) -> Result<()> {
        let mut before = Some(below.message_id);
        loop {
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
            self.retire_links(&ids).await?;
            match page.first() {
                Some(m) if page.len() == PAGE => before = Some(m.id),
                _ => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_threads_roundtrip() {
        assert_eq!(label_of(&chain_name("log-batch-3", Some(41))), Some("log-batch-3"));
        assert_eq!(label_of(&chain_name("log-claim-1", None)), Some("log-claim-1"));
        assert_eq!(label_of("ph-00.bin"), None);
        assert_eq!(label_of(ANCHOR), None);
        assert_eq!(parse_thread(&thread_name(77, "log-batch-2")), Some((77, "log-batch-2")));
        assert_eq!(parse_thread("hello"), None);
    }
}
