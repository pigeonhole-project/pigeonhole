//! Where frames go: a node's outbox (a channel), read by every other node.

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

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

    /// Forget the frames before position `keep_from` (its owner's outbox
    /// only): they are past any use. Readers past them read on as before; a
    /// reader below them skips to what is left. A chat that cannot delete
    /// keeps them.
    async fn prune(&self, _keep_from: u64) -> Result<()> {
        Ok(())
    }
}

/// An in-memory chat for tests: one outbox per node, with faults.
///
/// - [`FakeChat::isolate`] cuts a node off: its posts and reads fail, and
///   so do others' reads of its outbox (a partition).
/// - [`FakeChat::set_loss`] and [`FakeChat::set_duplication`] drop or
///   double posts, in every 1 of `n` posts (deterministic).
/// - [`FakeChat::set_delay`] shows each post to readers only after a
///   delay of its own (in order within an outbox, as a chat does).
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
    /// Each post, and when readers first see it.
    outboxes: HashMap<u64, Vec<(Instant, Bytes)>>,
    isolated: HashSet<u64>,
    lose_every: Option<u64>,
    duplicate_every: Option<u64>,
    /// Most delay of a post; each gets a pseudo-random share of it.
    delay: Option<Duration>,
    posts: u64,
    /// Posts made, per outbox, including lost ones.
    attempts: HashMap<u64, u64>,
    /// Posts pruned, per outbox: the first this many are gone.
    pruned: HashMap<u64, usize>,
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

    /// Show each post after up to `max` (`None`: at once), later posts of
    /// an outbox never before earlier ones.
    pub fn set_delay(&self, max: Option<Duration>) {
        self.inner.state.lock().unwrap().delay = max;
    }

    /// Posts node `owner` made, lost ones included.
    pub fn posts_by(&self, owner: u64) -> u64 {
        self.inner.state.lock().unwrap().attempts.get(&owner).copied().unwrap_or(0)
    }

    /// Frames still in node `owner`'s outbox (not pruned).
    pub fn kept(&self, owner: u64) -> u64 {
        let s = self.inner.state.lock().unwrap();
        let all = s.outboxes.get(&owner).map_or(0, Vec::len);
        (all - s.pruned.get(&owner).copied().unwrap_or(0)) as u64
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
        let delay = s.delay.map_or(Duration::ZERO, |max| spread(n, max));
        let outbox = s.outboxes.entry(self.owner).or_default();
        // Not before the post ahead of it: a chat shows an outbox in order.
        let after = outbox.last().map_or_else(Instant::now, |(at, _)| *at);
        let shown = (Instant::now() + delay).max(after);
        if !lost {
            outbox.push((shown, frame.clone()));
            if twice {
                outbox.push((shown, frame));
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
        let shown = shown(&s.outboxes[&self.owner]);
        let from = (after as usize).max(s.pruned.get(&self.owner).copied().unwrap_or(0));
        Ok(shown.iter().enumerate().skip(from).take(limit).map(|(i, (_, f))| (i as u64 + 1, f.clone())).collect())
    }

    async fn head(&self) -> Result<u64> {
        Ok(shown(&self.chat.state.lock().unwrap().outboxes[&self.owner]).len() as u64)
    }

    async fn wait(&self, after: u64, timeout: Duration) {
        let posted = self.chat.posted.notified();
        tokio::pin!(posted);
        posted.as_mut().enable();
        if self.head().await.unwrap_or(0) > after {
            return;
        }
        // A post made but not shown yet wakes nobody when it shows: wait
        // until then at most.
        let next = self.chat.state.lock().unwrap().outboxes[&self.owner]
            .iter()
            .map(|(at, _)| *at)
            .find(|at| *at > Instant::now());
        let timeout = next.map_or(timeout, |at| timeout.min(at.saturating_duration_since(Instant::now())));
        let _ = tokio::time::timeout(timeout, posted).await;
    }

    async fn prune(&self, keep_from: u64) -> Result<()> {
        if self.viewer != self.owner {
            bail!("node {} cannot prune node {}'s outbox", self.viewer, self.owner);
        }
        let mut s = self.chat.state.lock().unwrap();
        let State { outboxes, pruned, .. } = &mut *s;
        let outbox = outboxes.get_mut(&self.owner).expect("made with the mailbox");
        let done = pruned.entry(self.owner).or_default();
        // Positions start at 1: keep_from is the frame at index keep_from - 1.
        let gone = (keep_from.saturating_sub(1) as usize).min(outbox.len());
        for (_, f) in &mut outbox[(*done).min(gone)..gone] {
            *f = Bytes::new();
        }
        *done = (*done).max(gone);
        Ok(())
    }
}

/// The posts of an outbox readers see now: those shown already.
fn shown(outbox: &[(Instant, Bytes)]) -> &[(Instant, Bytes)] {
    let now = Instant::now();
    &outbox[..outbox.partition_point(|(at, _)| *at <= now)]
}

/// Post `n`'s share of `max`: spread over it, the same for the same `n`.
fn spread(n: u64, max: Duration) -> Duration {
    let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 31;
    max.mul_f64((x % 1000) as f64 / 1000.0)
}
