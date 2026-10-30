//! Where frames go: a node's outbox (a channel), read by every other node.

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// One outbox as a node sees it: its own, to post into, or a peer's, to read.
/// Positions are the chat's message ids: increasing, in posting order.
#[async_trait]
pub trait Mailbox: Send + Sync + 'static {
    /// Post one frame; its position.
    async fn post(&self, frame: Bytes) -> Result<u64>;

    /// Frames after position `after`, oldest first, at most `limit`.
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>>;

    /// The position of the newest frame (0 if none): where a reader that
    /// does not care for the past starts.
    async fn head(&self) -> Result<u64>;

    /// Return once something may have been posted after `after`, or after
    /// `timeout`. A mailbox the chat pushes to wakes early; one that has
    /// only polling just sleeps.
    async fn wait(&self, _after: u64, timeout: Duration) {
        tokio::time::sleep(timeout).await;
    }
}

/// An in-memory chat for tests: one outbox per node, with faults.
///
/// - [`FakeChat::isolate`] cuts a node off: its posts and reads fail, and
///   so do others' reads of its outbox (a partition).
/// - [`FakeChat::set_loss`] and [`FakeChat::set_duplication`] drop or
///   double posts, in every 1 of `n` posts (deterministic).
#[derive(Clone, Default)]
pub struct FakeChat {
    inner: Arc<Chat>,
}

#[derive(Default)]
struct Chat {
    state: Mutex<State>,
    posted: Notify,
}

#[derive(Default)]
struct State {
    outboxes: HashMap<u64, Vec<Bytes>>,
    isolated: HashSet<u64>,
    lose_every: Option<u64>,
    duplicate_every: Option<u64>,
    posts: u64,
    /// Posts made, per outbox, including lost ones.
    attempts: HashMap<u64, u64>,
}

impl FakeChat {
    pub fn new() -> Self {
        Self::default()
    }

    /// Node `owner`'s outbox as node `viewer` sees it (`viewer == owner`:
    /// its own, to post into).
    pub fn mailbox(&self, viewer: u64, owner: u64) -> Arc<dyn Mailbox> {
        self.inner.state.lock().unwrap().outboxes.entry(owner).or_default();
        Arc::new(FakeMailbox { chat: self.inner.clone(), viewer, owner })
    }

    pub fn isolate(&self, node: u64, isolated: bool) {
        let mut s = self.inner.state.lock().unwrap();
        if isolated {
            s.isolated.insert(node);
        } else {
            s.isolated.remove(&node);
        }
        drop(s);
        self.inner.posted.notify_waiters();
    }

    /// Lose every `n`th post (it reports success); `None`: none.
    pub fn set_loss(&self, n: Option<u64>) {
        self.inner.state.lock().unwrap().lose_every = n;
    }

    /// Store every `n`th post twice; `None`: none.
    pub fn set_duplication(&self, n: Option<u64>) {
        self.inner.state.lock().unwrap().duplicate_every = n;
    }

    /// Posts node `owner` made, lost ones included.
    pub fn posts_by(&self, owner: u64) -> u64 {
        self.inner.state.lock().unwrap().attempts.get(&owner).copied().unwrap_or(0)
    }
}

struct FakeMailbox {
    chat: Arc<Chat>,
    viewer: u64,
    owner: u64,
}

#[async_trait]
impl Mailbox for FakeMailbox {
    async fn post(&self, frame: Bytes) -> Result<u64> {
        if self.viewer != self.owner {
            bail!("node {} cannot post into node {}'s outbox", self.viewer, self.owner);
        }
        let mut s = self.chat.state.lock().unwrap();
        if s.isolated.contains(&self.viewer) {
            bail!("node {} is cut off", self.viewer);
        }
        s.posts += 1;
        *s.attempts.entry(self.owner).or_default() += 1;
        let n = s.posts;
        let lost = s.lose_every.is_some_and(|k| n.is_multiple_of(k));
        let twice = s.duplicate_every.is_some_and(|k| n.is_multiple_of(k));
        let outbox = s.outboxes.entry(self.owner).or_default();
        if !lost {
            outbox.push(frame.clone());
            if twice {
                outbox.push(frame);
            }
        }
        let pos = outbox.len() as u64;
        drop(s);
        self.chat.posted.notify_waiters();
        Ok(pos)
    }

    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Bytes)>> {
        let s = self.chat.state.lock().unwrap();
        if s.isolated.contains(&self.viewer) || s.isolated.contains(&self.owner) {
            bail!("node {} cannot reach node {}'s outbox", self.viewer, self.owner);
        }
        let outbox = &s.outboxes[&self.owner];
        Ok(outbox
            .iter()
            .enumerate()
            .skip(after as usize)
            .take(limit)
            .map(|(i, f)| (i as u64 + 1, f.clone()))
            .collect())
    }

    async fn head(&self) -> Result<u64> {
        Ok(self.chat.state.lock().unwrap().outboxes[&self.owner].len() as u64)
    }

    async fn wait(&self, after: u64, timeout: Duration) {
        let posted = self.chat.posted.notified();
        tokio::pin!(posted);
        posted.as_mut().enable();
        if self.head().await.unwrap_or(0) > after {
            return;
        }
        let _ = tokio::time::timeout(timeout, posted).await;
    }
}
