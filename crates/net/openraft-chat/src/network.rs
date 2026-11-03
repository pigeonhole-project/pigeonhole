//! The transport: [`ChatNetwork`] is openraft's network factory for one node.
//! It posts what the node sends into its own outbox, one frame per tick, and
//! reads every peer's outbox for what is addressed to it.

use crate::frame::{open, seal, Body, Envelope, Frame, Key, FRAME_VERSION};
use crate::mailbox::Mailbox;
use crate::telemetry;
use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{AnyError, Raft, RaftTypeConfig};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// What a node does with a request a peer sent: openraft's `Raft` does it;
/// tests may stand in for it.
#[async_trait]
pub trait Handler<C: RaftTypeConfig>: Send + Sync + 'static {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RaftError<C::NodeId>>;
    async fn vote(&self, req: VoteRequest<C::NodeId>) -> Result<VoteResponse<C::NodeId>, RaftError<C::NodeId>>;
    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse<C::NodeId>, RaftError<C::NodeId, InstallSnapshotError>>;
}

#[async_trait]
impl<C: RaftTypeConfig> Handler<C> for Raft<C> {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RaftError<C::NodeId>> {
        Raft::append_entries(self, req).await
    }
    async fn vote(&self, req: VoteRequest<C::NodeId>) -> Result<VoteResponse<C::NodeId>, RaftError<C::NodeId>> {
        Raft::vote(self, req).await
    }
    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<C>,
    ) -> Result<InstallSnapshotResponse<C::NodeId>, RaftError<C::NodeId, InstallSnapshotError>> {
        Raft::install_snapshot(self, req).await
    }
}

/// The cluster's transport settings (docs/cluster.md § Timing).
#[derive(Clone, Debug)]
pub struct Settings {
    /// Frames of another cluster are dropped.
    pub cluster: u128,
    /// HMAC key of every frame.
    pub key: Key,
    /// At most one post per tick.
    pub tick: Duration,
    /// How long a reader waits for a push before it reads anyway.
    pub poll: Duration,
    /// How long a frame stays in the outbox (`None`: forever): older ones
    /// are pruned, once per `keep`, on a tick with nothing to send. Past
    /// twice the longest election timeout no call waits for a frame.
    pub keep: Option<Duration>,
}

/// openraft's network over chat outboxes, for one node.
pub struct ChatNetwork<C: RaftTypeConfig> {
    inner: Arc<Inner<C>>,
}

impl<C: RaftTypeConfig> Clone for ChatNetwork<C> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

struct Inner<C: RaftTypeConfig> {
    me: C::NodeId,
    settings: Settings,
    outbox: Arc<dyn Mailbox>,
    peers: HashMap<C::NodeId, Arc<dyn Mailbox>>,
    queue: Mutex<Vec<Envelope<C>>>,
    queued: Arc<Notify>,
    /// Calls waiting for their response, by rpc id.
    waiting: Mutex<HashMap<u64, Waiting<C>>>,
    next_rpc: AtomicU64,
    seq: AtomicU64,
    /// The highest seq seen from each peer.
    seen: Mutex<HashMap<C::NodeId, u64>>,
    handler: OnceLock<Arc<dyn Handler<C>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// This run's posts not pruned yet, oldest first: position and when.
    posted: Mutex<VecDeque<(u64, tokio::time::Instant)>>,
    /// When the outbox was last pruned (or the run started).
    pruned_at: Mutex<tokio::time::Instant>,
}

/// A call waiting for its response: the peer asked (only its response
/// completes the call) and where the response goes.
type Waiting<C> = (<C as RaftTypeConfig>::NodeId, oneshot::Sender<Body<C>>);

impl<C: RaftTypeConfig> Drop for Inner<C> {
    fn drop(&mut self) {
        for t in self.tasks.get_mut().unwrap().drain(..) {
            t.abort();
        }
    }
}

/// Why a call got no response.
#[derive(Debug)]
pub enum CallError {
    Timeout,
    /// The response was not what the request asks for.
    Mismatch,
}

impl<C: RaftTypeConfig> ChatNetwork<C> {
    /// Node `me`, posting into `outbox` and reading `peers`' outboxes. It
    /// sends at once; it answers requests once [`Self::serve`] gives it a
    /// handler (the node's `Raft`, created with this network).
    pub fn new(
        me: C::NodeId,
        settings: Settings,
        outbox: Arc<dyn Mailbox>,
        peers: HashMap<C::NodeId, Arc<dyn Mailbox>>,
    ) -> Self {
        // Seq and rpc ids from the clock: they keep growing across restarts,
        // so a frame or a response meant for an earlier run of this node is
        // never taken for one of this run.
        let seq = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64);
        let inner = Arc::new(Inner {
            me,
            settings,
            outbox,
            peers,
            queue: Mutex::default(),
            queued: Arc::new(Notify::new()),
            waiting: Mutex::default(),
            next_rpc: AtomicU64::new(seq),
            seq: AtomicU64::new(seq),
            seen: Mutex::default(),
            handler: OnceLock::new(),
            tasks: Mutex::default(),
            posted: Mutex::default(),
            pruned_at: Mutex::new(tokio::time::Instant::now()),
        });
        let sender = tokio::spawn(send_loop(Arc::downgrade(&inner)));
        inner.tasks.lock().unwrap().push(sender);
        Self { inner }
    }

    /// Answer peers' requests with `handler`, and read their responses.
    /// Reading starts at each outbox's head as of this call: frames posted
    /// before it (to a node that was not up) are not replayed; every frame
    /// posted after it is read.
    pub async fn serve(&self, handler: Arc<dyn Handler<C>>) {
        if self.inner.handler.set(handler).is_err() {
            return;
        }
        for (peer, mailbox) in &self.inner.peers {
            let start = mailbox.head().await.ok();
            let task = tokio::spawn(read_loop(Arc::downgrade(&self.inner), peer.clone(), mailbox.clone(), start));
            self.inner.tasks.lock().unwrap().push(task);
        }
    }

    /// Send `body` to `target` and wait up to `ttl` for its response.
    /// Dropped before then (openraft's own timeout around it), the call
    /// stops waiting all the same: it is no longer in [`Self::pending_calls`]
    /// and counts as a timeout.
    pub async fn call(&self, target: C::NodeId, body: Body<C>, ttl: Duration) -> Result<Body<C>, CallError> {
        let rpc = self.inner.next_rpc.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner.waiting.lock().unwrap().insert(rpc, (target.clone(), tx));
        let mut call =
            Call { inner: &self.inner, rpc, peer: target.to_string(), kind: body.rpc(), t0: Instant::now(), ok: false };
        self.inner.enqueue(Envelope { to: target, rpc, body });
        let got = match tokio::time::timeout(ttl, rx).await {
            Ok(Ok(body)) => Ok(body),
            _ => Err(CallError::Timeout),
        };
        call.ok = got.is_ok();
        got
    }

    /// Calls waiting for their response now.
    pub fn pending_calls(&self) -> usize {
        self.inner.waiting.lock().unwrap().len()
    }

    fn client(&self, target: C::NodeId) -> ChatClient<C> {
        ChatClient { net: self.clone(), target }
    }
}

/// A call in flight: however it ends (answered, timed out, dropped), it
/// stops waiting and is counted.
struct Call<'a, C: RaftTypeConfig> {
    inner: &'a Inner<C>,
    rpc: u64,
    peer: String,
    kind: &'static str,
    t0: Instant,
    ok: bool,
}

impl<C: RaftTypeConfig> Drop for Call<'_, C> {
    fn drop(&mut self) {
        self.inner.waiting.lock().unwrap().remove(&self.rpc);
        let outcome = if self.ok { "ok" } else { "timeout" };
        telemetry::record_call(std::mem::take(&mut self.peer), self.kind, outcome, self.t0.elapsed());
    }
}

impl<C: RaftTypeConfig> Inner<C> {
    /// Prune the outbox if it is due (once per `keep`) and nothing waits to
    /// be sent: every frame older than `keep` goes, but the newest frame
    /// stays (a chat may need one to anchor the rest). The first prune
    /// comes a `keep` after the run starts, so the frames of the run before
    /// stay that long for peers to read.
    async fn prune(&self) {
        let Some(keep) = self.settings.keep else { return };
        let now = tokio::time::Instant::now();
        if now.duration_since(*self.pruned_at.lock().unwrap()) < keep || !self.queue.lock().unwrap().is_empty() {
            return;
        }
        let keep_from = {
            let mut posted = self.posted.lock().unwrap();
            let mut last = None;
            while posted.len() > 1 && posted.front().is_some_and(|(_, at)| now.duration_since(*at) >= keep) {
                last = posted.pop_front();
            }
            match (posted.front(), last) {
                (Some((pos, _)), _) => *pos,
                (None, Some((pos, _))) => pos,
                (None, None) => return,
            }
        };
        *self.pruned_at.lock().unwrap() = now;
        let t0 = Instant::now();
        let pruned = self.outbox.prune(keep_from).await;
        telemetry::record_prune(pruned.is_ok(), t0.elapsed());
        match pruned {
            Ok(()) => debug!(node = %self.me, keep_from, "pruned the outbox"),
            Err(e) => warn!(node = %self.me, keep_from, error = %format!("{e:#}"), "prune failed"),
        }
    }

    /// A response from `peer` to call `rpc`: it completes the call if one
    /// waits for it from that peer. Any other (a late one, one to another
    /// peer's call, one to an earlier run of this node) is dropped.
    fn complete(&self, peer: &C::NodeId, rpc: u64, body: Body<C>) {
        let mut waiting = self.waiting.lock().unwrap();
        match waiting.get(&rpc) {
            Some((asked, _)) if asked == peer => {
                let (_, tx) = waiting.remove(&rpc).expect("just seen");
                let _ = tx.send(body);
            }
            _ => {
                drop(waiting);
                telemetry::record_stray(peer.to_string());
                debug!(node = %self.me, %peer, rpc, "a response no call waits for dropped");
            }
        }
    }

    fn enqueue(&self, env: Envelope<C>) {
        self.queue.lock().unwrap().push(env);
        self.queued.notify_one();
    }
}

/// Post whatever is queued, one frame per tick.
async fn send_loop<C: RaftTypeConfig>(inner: std::sync::Weak<Inner<C>>) {
    loop {
        let Some(this) = inner.upgrade() else { return };
        let envelopes = std::mem::take(&mut *this.queue.lock().unwrap());
        if envelopes.is_empty() {
            // notify_one keeps a permit for a later notified(), so nothing
            // queued between take and here is missed.
            let queued = this.queued.clone();
            drop(this);
            queued.notified().await;
            continue;
        }
        let frame = Frame::<C> {
            version: FRAME_VERSION,
            cluster: this.settings.cluster,
            from: this.me.clone(),
            seq: this.seq.fetch_add(1, Ordering::Relaxed),
            envelopes,
        };
        let n = frame.envelopes.len();
        let sealed = seal(&frame, &this.settings.key);
        for env in &frame.envelopes {
            let (kind, reply) = (env.body.kind(), !env.body.is_request());
            debug!(node = %this.me, to = %env.to, kind, reply, via = "chat", bytes = sealed.len(), "raft frame sent");
        }
        let t0 = Instant::now();
        let posted = this.outbox.post(sealed).await;
        telemetry::record_post(posted.is_ok(), n, t0.elapsed());
        match posted {
            Ok(pos) => {
                debug!(node = %this.me, pos, envelopes = n, "posted a frame");
                this.posted.lock().unwrap().push_back((pos, tokio::time::Instant::now()));
            }
            // The calls time out; openraft retries as on a broken connection.
            Err(e) => warn!(node = %this.me, envelopes = n, error = %format!("{e:#}"), "post failed"),
        }
        let tick = this.settings.tick;
        drop(this);
        tokio::time::sleep(tick).await;
        if let Some(this) = inner.upgrade() {
            this.prune().await;
        }
    }
}

/// Read `peer`'s outbox and handle what is addressed to this node.
/// `start`: the position to read after; `None` (the outbox could not be
/// reached at `serve`): its head once it can be.
async fn read_loop<C: RaftTypeConfig>(
    inner: std::sync::Weak<Inner<C>>,
    peer: C::NodeId,
    mailbox: Arc<dyn Mailbox>,
    start: Option<u64>,
) {
    let mut pos = loop {
        if let Some(start) = start {
            break start;
        }
        match mailbox.head().await {
            Ok(head) => break head,
            Err(_) => {
                let Some(this) = inner.upgrade() else { return };
                let poll = this.settings.poll;
                drop(this);
                tokio::time::sleep(poll).await;
            }
        }
    };
    loop {
        let Some(this) = inner.upgrade() else { return };
        let poll = this.settings.poll;
        match mailbox.read_after(pos, 100).await {
            Ok(frames) if !frames.is_empty() => {
                for (at, bytes) in frames {
                    pos = at;
                    this.receive(peer.clone(), &bytes);
                }
            }
            Ok(_) => {
                drop(this);
                mailbox.wait(pos, poll).await;
            }
            Err(e) => {
                telemetry::record_read_error(peer.to_string());
                debug!(node = %this.me, %peer, error = %format!("{e:#}"), "read failed");
                drop(this);
                tokio::time::sleep(poll).await;
            }
        }
    }
}

impl<C: RaftTypeConfig> Inner<C> {
    fn receive(self: &Arc<Self>, peer: C::NodeId, bytes: &[u8]) {
        let frame = match open::<C>(bytes, &self.settings.key) {
            Ok(f) => f,
            Err(e) => {
                telemetry::record_frame(peer.to_string(), "bad_frame");
                warn!(node = %self.me, %peer, error = %format!("{e:#}"), "frame dropped");
                return;
            }
        };
        if frame.cluster != self.settings.cluster || frame.from != peer {
            telemetry::record_frame(peer.to_string(), "foreign");
            warn!(node = %self.me, %peer, from = %frame.from, "frame of another cluster or sender dropped");
            return;
        }
        {
            let mut seen = self.seen.lock().unwrap();
            let last = seen.entry(peer.clone()).or_insert(0);
            if frame.seq <= *last {
                telemetry::record_frame(peer.to_string(), "duplicate");
                return;
            }
            *last = frame.seq;
        }
        telemetry::record_frame(peer.to_string(), "ok");
        for env in frame.envelopes {
            if env.to != self.me {
                continue;
            }
            let (kind, reply) = (env.body.kind(), !env.body.is_request());
            debug!(node = %self.me, from = %peer, kind, reply, via = "chat", "raft frame received");
            if env.body.is_request() {
                let Some(handler) = self.handler.get().cloned() else { continue };
                telemetry::record_served(peer.to_string(), env.body.rpc());
                let this = Arc::downgrade(self);
                let peer = peer.clone();
                tokio::spawn(async move {
                    let body = match env.body {
                        Body::AppendEntries(r) => Body::Appended(handler.append_entries(r).await),
                        Body::Vote(r) => Body::Voted(handler.vote(r).await),
                        Body::InstallSnapshot(r) => Body::Installed(handler.install_snapshot(r).await),
                        _ => unreachable!("is_request"),
                    };
                    if let Some(this) = this.upgrade() {
                        this.enqueue(Envelope { to: peer, rpc: env.rpc, body });
                    }
                });
            } else {
                self.complete(&peer, env.rpc, env.body);
            }
        }
    }
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for ChatNetwork<C> {
    type Network = ChatClient<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        self.client(target)
    }
}

/// One peer, as openraft's network sees it.
pub struct ChatClient<C: RaftTypeConfig> {
    net: ChatNetwork<C>,
    target: C::NodeId,
}

impl<C: RaftTypeConfig> ChatClient<C> {
    fn failed<E: std::error::Error>(
        &self,
        action: RPCTypes,
        ttl: Duration,
        e: CallError,
    ) -> RPCError<C::NodeId, C::Node, E> {
        match e {
            CallError::Timeout => RPCError::Timeout(Timeout {
                action,
                id: self.net.inner.me.clone(),
                target: self.target.clone(),
                timeout: ttl,
            }),
            CallError::Mismatch => {
                RPCError::Network(NetworkError::new(&AnyError::error("a response of the wrong kind")))
            }
        }
    }
}

impl<C: RaftTypeConfig> RaftNetwork<C> for ChatClient<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        let ttl = option.hard_ttl();
        match self.net.call(self.target.clone(), Body::AppendEntries(rpc), ttl).await {
            Ok(Body::Appended(r)) => r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target.clone(), e))),
            Ok(_) => Err(self.failed(RPCTypes::AppendEntries, ttl, CallError::Mismatch)),
            Err(e) => Err(self.failed(RPCTypes::AppendEntries, ttl, e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<C::NodeId>,
        RPCError<C::NodeId, C::Node, RaftError<C::NodeId, InstallSnapshotError>>,
    > {
        let ttl = option.hard_ttl();
        match self.net.call(self.target.clone(), Body::InstallSnapshot(rpc), ttl).await {
            Ok(Body::Installed(r)) => r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target.clone(), e))),
            Ok(_) => Err(self.failed(RPCTypes::InstallSnapshot, ttl, CallError::Mismatch)),
            Err(e) => Err(self.failed(RPCTypes::InstallSnapshot, ttl, e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C::NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<C::NodeId>, RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>> {
        let ttl = option.hard_ttl();
        match self.net.call(self.target.clone(), Body::Vote(rpc), ttl).await {
            Ok(Body::Voted(r)) => {
                if let Ok(v) = &r {
                    debug!(node = %self.target, to = %self.net.inner.me, granted = v.vote_granted, via = "chat", "raft vote answered");
                }
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target.clone(), e)))
            }
            Ok(_) => Err(self.failed(RPCTypes::Vote, ttl, CallError::Mismatch)),
            Err(e) => Err(self.failed(RPCTypes::Vote, ttl, e)),
        }
    }
}
