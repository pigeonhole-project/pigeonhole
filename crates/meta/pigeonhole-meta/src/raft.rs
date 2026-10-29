//! The tablet's Raft (openraft): a node's log over a [`LogStore`], its state
//! machine over the namespaces and a [`SnapshotStore`].
//!
//! A Raft entry carries a batch of commands (openraft 0.9 writes and flushes
//! one entry per `client_write`, so [`crate::MetaStore`] groups concurrent
//! proposals itself) and is stored as our [`Entry`] (its term, and the whole
//! openraft entry as the payload); Raft index `i` is our position `i + 1`. A snapshot
//! is put into the node's snapshot store when it is built or received, and the
//! log is rebased on the newest one when openraft purges it.

use crate::log::{Base, Entry, Head, LogError, LogId as HistoryId, LogStore, Position};
use crate::snapshot::{SnapshotRef, SnapshotStore, StoredSnapshot};
use crate::{apply_command, take_snapshot, Command, States, Stop, TabletSnapshot};
use anyhow::Context;
use futures::TryStreamExt;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    AnyError, BasicNode, EntryPayload, LogState, RaftLogReader, RaftNetwork, RaftNetworkFactory,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};

openraft::declare_raft_types!(
    /// The tablet's Raft types: a batch of commands in, their encoded
    /// outcomes out.
    pub TypeConfig:
        D = Vec<Command>,
        R = Vec<Vec<u8>>,
        NodeId = u64,
        Node = BasicNode,
        AsyncRuntime = Runtime,
);

#[cfg(not(madsim))]
type Runtime = openraft::TokioRuntime;
#[cfg(madsim)]
type Runtime = crate::sim_runtime::MadsimRuntime;

pub type NodeId = u64;
pub type RaftEntry = openraft::Entry<TypeConfig>;
pub type RaftLogId = openraft::LogId<NodeId>;
pub type Raft = openraft::Raft<TypeConfig>;
type SErr = StorageError<NodeId>;

/// The only node of a cluster of one.
pub const SOLO: NodeId = 1;

pub fn position(id: &RaftLogId) -> Position {
    id.index + 1
}

fn log_id(term: u64, leader: NodeId, index: u64) -> RaftLogId {
    openraft::LogId::new(openraft::CommittedLeaderId::new(term, leader), index)
}

/// A snapshot as the node stores it: Raft's meta plus every namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,
    pub tablet: TabletSnapshot,
}

impl RaftSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a snapshot always encodes")
    }

    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        let snap: Self = postcard::from_bytes(data).context("decode raft snapshot")?;
        Ok(snap)
    }
}

/// The node's current snapshot (what the log is rebased on next).
#[derive(Clone)]
pub(crate) struct CurrentSnapshot {
    pub(crate) meta: SnapshotMeta<NodeId, BasicNode>,
    pub(crate) data: Vec<u8>,
    pub(crate) snapshot: SnapshotRef,
}

/// State the adapters share with [`crate::MetaStore`].
pub(crate) struct Shared {
    pub(crate) stop: watch::Sender<Option<Stop>>,
    /// Position of the last applied entry.
    pub(crate) applied: AtomicU64,
    /// Position the log starts from.
    pub(crate) based: AtomicU64,
    pub(crate) current: Mutex<Option<CurrentSnapshot>>,
    /// The base this node started from or last rebased on: the log's own,
    /// or an older one it keeps when the newest snapshot could not be
    /// loaded. Raft sees the log as purged up to it.
    pub(crate) base: Mutex<Option<Base>>,
}

impl Shared {
    fn stop(&self, stop: Stop) {
        self.stop.send_if_modified(|s| {
            if s.is_none() {
                *s = Some(stop);
                true
            } else {
                false
            }
        });
    }

    /// A log error: a fence stops the tablet for good.
    fn log_error(&self, e: LogError) -> SErr {
        match e {
            LogError::Fenced { current } => self.stop(Stop::Fenced { current }),
            ref other => self.stop(Stop::Failed(format!("log: {other}"))),
        }
        StorageIOError::write_logs(AnyError::error(e.to_string())).into()
    }
}

fn read_err(e: impl ToString) -> SErr {
    StorageIOError::read_logs(AnyError::error(e.to_string())).into()
}

/// Written entries kept in memory for reads.
const TAIL_KEEP: u64 = 4096;

/// One write to the node's log, done by the writer task in order.
enum Op {
    Append(Vec<RaftEntry>, LogFlushed<TypeConfig>),
    Truncate(Position, oneshot::Sender<Result<(), SErr>>),
    Rebase(Base, oneshot::Sender<Result<(), SErr>>),
    Vote(Vec<u8>, oneshot::Sender<Result<(), SErr>>),
}

/// A node's log: openraft's [`RaftLogStorage`] over a [`LogStore`].
///
/// `append` only queues: the entries are readable at once, and a writer task
/// writes everything queued meanwhile as one batch (one fsync, one chat
/// message) before reporting them flushed. That is group commit: openraft
/// hands over one client write at a time and keeps accepting more while the
/// previous batch is being written. Truncate, purge and votes go through the
/// same queue, so writes stay in order.
#[derive(Clone)]
pub(crate) struct LogAdapter {
    log: Arc<dyn LogStore>,
    history: HistoryId,
    /// Head including queued entries.
    head: Arc<Mutex<Position>>,
    /// The tail of the log in memory: queued entries not yet in the log
    /// store, and the last [`TAIL_KEEP`] written ones. Commits read the
    /// entries they apply back; serving them from here keeps that off the
    /// store (a disk walk, a chat download).
    tail: Arc<Mutex<std::collections::BTreeMap<Position, RaftEntry>>>,
    /// Head of what the log store holds.
    durable: Arc<AtomicU64>,
    shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Op>,
}

/// Bases (and their snapshots) the log keeps: the newest `keep`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keep(pub(crate) usize);

impl LogAdapter {
    /// Start the writer task for `log`, claimed as `gen`, whose head is `head`.
    pub(crate) fn new(
        log: Arc<dyn LogStore>,
        gen: u64,
        history: HistoryId,
        head: Position,
        shared: Arc<Shared>,
        snapshots: Arc<dyn SnapshotStore>,
        keep: Keep,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let this = Self {
            log,
            history,
            head: Arc::new(Mutex::new(head)),
            tail: Arc::default(),
            durable: Arc::new(AtomicU64::new(head)),
            shared,
            tx,
        };
        tokio::spawn(this.clone().write_loop(gen, snapshots, keep, rx));
        this
    }

    async fn write_loop(self, gen: u64, snapshots: Arc<dyn SnapshotStore>, keep: Keep, mut rx: mpsc::UnboundedReceiver<Op>) {
        let mut next = None;
        loop {
            let op = match next.take() {
                Some(op) => op,
                None => match rx.recv().await {
                    Some(op) => op,
                    None => return,
                },
            };
            match op {
                Op::Append(entries, callback) => {
                    // Everything queued behind it goes in the same batch.
                    let mut entries = entries;
                    let mut callbacks = vec![callback];
                    while let Ok(op) = rx.try_recv() {
                        match op {
                            Op::Append(more, callback) => {
                                entries.extend(more);
                                callbacks.push(callback);
                            }
                            other => {
                                next = Some(other);
                                break;
                            }
                        }
                    }
                    let ours: Vec<Entry> = entries
                        .iter()
                        .map(|e| Entry {
                            term: e.log_id.leader_id.term,
                            payload: postcard::to_stdvec(e).expect("an entry always encodes"),
                        })
                        .collect();
                    let (bytes, t0) = (ours.iter().map(|e| e.payload.len()).sum(), std::time::Instant::now());
                    let appended = self.log.append(gen, &ours).await;
                    crate::telemetry::record_append(appended.is_ok(), ours.len(), bytes, t0.elapsed());
                    let result = match appended {
                        Ok(last) => {
                            self.durable.store(last, Ordering::SeqCst);
                            let mut tail = self.tail.lock().unwrap();
                            let keep_from = last.saturating_sub(TAIL_KEEP) + 1;
                            *tail = tail.split_off(&keep_from);
                            Ok(())
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            self.shared.log_error(e);
                            Err(msg)
                        }
                    };
                    for callback in callbacks {
                        callback.log_io_completed(result.clone().map_err(std::io::Error::other));
                    }
                }
                Op::Truncate(from, done) => {
                    let t0 = std::time::Instant::now();
                    let r = self.log.truncate(gen, from).await.map_err(|e| self.shared.log_error(e));
                    crate::telemetry::record_log_write("truncate", r.is_ok(), t0.elapsed());
                    if r.is_ok() {
                        self.durable.store(from - 1, Ordering::SeqCst);
                        self.tail.lock().unwrap().retain(|p, _| *p < from);
                    }
                    let _ = done.send(r);
                }
                Op::Rebase(base, done) => {
                    let (at, new_ref) = (base.at, base.snapshot.clone());
                    let t0 = std::time::Instant::now();
                    let r = self.log.rebase(gen, base.clone(), keep.0).await.map_err(|e| self.shared.log_error(e));
                    crate::telemetry::record_log_write("rebase", r.is_ok(), t0.elapsed());
                    let r = r.map(|dropped| {
                        tracing::info!(at, snapshot = %new_ref.0, "log rebased on a snapshot");
                        self.durable.fetch_max(at, Ordering::SeqCst);
                        self.tail.lock().unwrap().retain(|p, _| *p > at);
                        self.shared.based.store(at, Ordering::SeqCst);
                        *self.shared.base.lock().unwrap() = Some(base);
                        dropped
                    });
                    let dropped = r.as_ref().map(|d| d.clone()).unwrap_or_default();
                    let _ = done.send(r.map(|_| ()));
                    // Nothing starts from them any more (best effort).
                    for old in dropped.into_iter().filter(|o| o.snapshot != new_ref) {
                        if let Err(e) = snapshots.remove(&old.snapshot).await {
                            tracing::warn!(snapshot = %old.snapshot.0, error = %format!("{e:#}"), "removing an old base failed");
                        }
                    }
                }
                Op::Vote(vote, done) => {
                    let t0 = std::time::Instant::now();
                    let r = self.log.save_vote(gen, &vote).await.map_err(|e| self.shared.log_error(e));
                    crate::telemetry::record_log_write("vote", r.is_ok(), t0.elapsed());
                    let _ = done.send(r);
                }
            }
        }
    }

    /// Hand `op` to the writer and wait for it.
    async fn write(&self, op: impl FnOnce(oneshot::Sender<Result<(), SErr>>) -> Op) -> Result<(), SErr> {
        let (done, rx) = oneshot::channel();
        let gone = || -> SErr { StorageIOError::write_logs(AnyError::error("the log writer is gone")).into() };
        self.tx.send(op(done)).map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?
    }

    async fn read_positions(&self, from: Position, to: Position) -> Result<Vec<RaftEntry>, SErr> {
        let base = self.shared.based.load(Ordering::SeqCst);
        let head = *self.head.lock().unwrap();
        let (from, to) = (from.max(base), to.min(head));
        if from >= to {
            return Ok(Vec::new());
        }
        let mut out: std::collections::BTreeMap<Position, RaftEntry> =
            self.tail.lock().unwrap().range(from + 1..=to).map(|(p, e)| (*p, e.clone())).collect();
        // Whatever the tail lacks is older: read it from the store.
        let missing_to = out.keys().next().map_or(to, |first| first - 1);
        let durable = self.durable.load(Ordering::SeqCst).min(missing_to);
        if durable > from {
            let entries: Vec<(Position, Entry)> = self
                .log
                .read(from, Head { log_id: self.history, position: durable })
                .try_collect()
                .await
                .map_err(read_err)?;
            for (pos, e) in entries {
                let entry = postcard::from_bytes(&e.payload).map_err(|err| read_err(format!("entry {pos}: {err}")))?;
                out.entry(pos).or_insert(entry);
            }
        }
        Ok(out.into_values().collect())
    }
}

impl RaftLogReader<TypeConfig> for LogAdapter {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, SErr> {
        // Raft indices [start, end) are our positions (start, end].
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e + 1,
            Bound::Excluded(&e) => e,
            Bound::Unbounded => u64::MAX,
        };
        self.read_positions(start, end).await
    }
}

impl RaftLogStorage<TypeConfig> for LogAdapter {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, SErr> {
        let d = self.log.discover().await.map_err(read_err)?.ok_or_else(|| read_err("the log was never claimed"))?;
        let base = self.shared.base.lock().unwrap().clone();
        let purged = base.as_ref().filter(|b| b.at > 0).map(|b| log_id(b.term, b.leader, b.at - 1));
        let head = d.head.position;
        let base_at = base.as_ref().map_or(0, |b| b.at);
        let last = if head > base_at {
            self.read_positions(head - 1, head).await?.pop().map(|e| e.log_id)
        } else {
            purged
        };
        Ok(LogState { last_purged_log_id: purged, last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), SErr> {
        let bytes = postcard::to_stdvec(vote).expect("a vote always encodes");
        self.write(|done| Op::Vote(bytes, done)).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, SErr> {
        let d = self.log.discover().await.map_err(read_err)?;
        match d.and_then(|d| d.vote) {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes).map_err(|e| read_err(format!("vote: {e}")))?)),
            None => Ok(None),
        }
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), SErr>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<RaftEntry> = entries.into_iter().collect();
        let Some(first) = entries.first() else {
            callback.log_io_completed(Ok(()));
            return Ok(());
        };
        {
            let mut head = self.head.lock().unwrap();
            if position(&first.log_id) != *head + 1 {
                return Err(StorageIOError::write_logs(AnyError::error(format!(
                    "append at {} but the log continues at {}",
                    position(&first.log_id),
                    *head + 1
                )))
                .into());
            }
            let mut tail = self.tail.lock().unwrap();
            for e in &entries {
                tail.insert(position(&e.log_id), e.clone());
            }
            *head += entries.len() as u64;
        }
        self.tx
            .send(Op::Append(entries, callback))
            .map_err(|_| StorageError::from(StorageIOError::write_logs(AnyError::error("the log writer is gone"))))
    }

    async fn truncate(&mut self, id: RaftLogId) -> Result<(), SErr> {
        let from = position(&id);
        self.write(|done| Op::Truncate(from, done)).await?;
        *self.head.lock().unwrap() = from - 1;
        Ok(())
    }

    async fn purge(&mut self, id: RaftLogId) -> Result<(), SErr> {
        let current = self.shared.current.lock().unwrap().clone();
        let Some(current) = current.filter(|c| c.meta.last_log_id.is_some_and(|l| l.index >= id.index)) else {
            return Err(StorageIOError::write_logs(AnyError::error(format!(
                "purge up to {id} without a snapshot covering it"
            )))
            .into());
        };
        let base = Base { at: position(&id), term: id.leader_id.term, leader: id.leader_id.node_id, snapshot: current.snapshot };
        self.write(|done| Op::Rebase(base, done)).await?;
        let mut head = self.head.lock().unwrap();
        *head = (*head).max(position(&id));
        Ok(())
    }
}

/// The node's state machine: the namespaces, plus what Raft tracks of them.
pub(crate) struct StateMachine {
    pub(crate) states: Arc<States>,
    pub(crate) snapshots: Arc<dyn SnapshotStore>,
    pub(crate) history: HistoryId,
    pub(crate) last_applied: Option<RaftLogId>,
    pub(crate) membership: StoredMembership<NodeId, BasicNode>,
    pub(crate) shared: Arc<Shared>,
}

impl StateMachine {
    /// Store `snap` in the node's snapshot store and make it current.
    async fn keep(
        snapshots: &dyn SnapshotStore,
        history: HistoryId,
        shared: &Shared,
        snap: &RaftSnapshot,
    ) -> Result<CurrentSnapshot, SErr> {
        let t0 = std::time::Instant::now();
        let data = snap.encode();
        let at = snap.meta.last_log_id.as_ref().map_or(0, position);
        let stored = StoredSnapshot { log_id: history, at, data: data.clone() };
        let snapshot = snapshots
            .put(&stored)
            .await
            .map_err(|e| StorageError::from(StorageIOError::write_snapshot(None, AnyError::error(format!("{e:#}")))))?;
        crate::telemetry::record_snapshot(data.len(), t0.elapsed());
        tracing::info!(at, bytes = data.len(), ms = t0.elapsed().as_millis() as u64, "snapshot stored");
        let current = CurrentSnapshot { meta: snap.meta.clone(), data, snapshot };
        let old = shared.current.lock().unwrap().replace(current.clone());
        // A newer one replaced it before the log moved onto it: unused (the
        // one the node starts from stays until the log drops it).
        let base_ref = shared.base.lock().unwrap().as_ref().map(|b| b.snapshot.clone());
        if let Some(old) = old.filter(|o| o.snapshot != current.snapshot && Some(&o.snapshot) != base_ref.as_ref()) {
            if let Err(e) = snapshots.remove(&old.snapshot).await {
                tracing::warn!(snapshot = %old.snapshot.0, error = %format!("{e:#}"), "removing an unused snapshot failed");
            }
        }
        Ok(current)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<RaftLogId>, StoredMembership<NodeId, BasicNode>), SErr> {
        Ok((self.last_applied, self.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<Vec<u8>>>, SErr>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        for entry in entries {
            let pos = position(&entry.log_id);
            let reply = match entry.payload {
                EntryPayload::Blank => Vec::new(),
                EntryPayload::Normal(batch) => {
                    let mut replies = Vec::with_capacity(batch.len());
                    for cmd in &batch {
                        match apply_command(&self.states, pos, cmd) {
                            Ok(reply) => replies.push(reply),
                            Err(e) => {
                                let msg = format!("{e:#}");
                                self.shared.stop(Stop::Failed(msg.clone()));
                                return Err(StorageIOError::apply(entry.log_id, AnyError::error(msg)).into());
                            }
                        }
                    }
                    replies
                }
                EntryPayload::Membership(m) => {
                    self.membership = StoredMembership::new(Some(entry.log_id), m);
                    Vec::new()
                }
            };
            self.last_applied = Some(entry.log_id);
            self.shared.applied.store(pos, Ordering::SeqCst);
            out.push(reply);
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Taken here, between applies, so it matches `last_applied` exactly.
        let position = self.last_applied.as_ref().map_or(0, position);
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone(),
            snapshot_id: format!("{position}-{}", uuid::Uuid::new_v4().simple()),
        };
        SnapshotBuilder {
            snap: RaftSnapshot { meta, tablet: take_snapshot(&self.states, position) },
            snapshots: self.snapshots.clone(),
            history: self.history,
            shared: self.shared.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SErr> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SErr> {
        let snap = RaftSnapshot::decode(snapshot.get_ref())
            .map_err(|e| StorageError::from(StorageIOError::read_snapshot(None, AnyError::error(format!("{e:#}")))))?;
        crate::restore_states(&self.states, &snap.tablet)
            .map_err(|e| StorageError::from(StorageIOError::read_snapshot(None, AnyError::error(format!("{e:#}")))))?;
        let snap = RaftSnapshot { meta: meta.clone(), tablet: snap.tablet };
        Self::keep(self.snapshots.as_ref(), self.history, &self.shared, &snap).await?;
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        self.shared.applied.store(meta.last_log_id.as_ref().map_or(0, position), Ordering::SeqCst);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, SErr> {
        let current = self.shared.current.lock().unwrap().clone();
        Ok(current.map(|c| Snapshot { meta: c.meta, snapshot: Box::new(Cursor::new(c.data)) }))
    }
}

/// A snapshot taken between applies, stored when built.
pub(crate) struct SnapshotBuilder {
    snap: RaftSnapshot,
    snapshots: Arc<dyn SnapshotStore>,
    history: HistoryId,
    shared: Arc<Shared>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, SErr> {
        let current = StateMachine::keep(self.snapshots.as_ref(), self.history, &self.shared, &self.snap).await?;
        Ok(Snapshot { meta: current.meta, snapshot: Box::new(Cursor::new(current.data)) })
    }
}

/// No peers: a cluster of one never sends an RPC.
pub struct NoNetwork;

impl RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network = NoNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoNetwork
    }
}

fn unreachable<E: std::error::Error>() -> RPCError<NodeId, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&AnyError::error("a cluster of one has no peers")))
}

impl RaftNetwork<TypeConfig> for NoNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>>
    {
        Err(unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable())
    }
}

/// The meta of a snapshot that starts a cluster of one at `index` (restore).
pub fn solo_meta(index: u64) -> SnapshotMeta<NodeId, BasicNode> {
    let id = log_id(1, SOLO, index);
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(SOLO, BasicNode::default());
    let membership = openraft::Membership::new(vec![[SOLO].into_iter().collect()], nodes);
    SnapshotMeta {
        last_log_id: Some(id),
        last_membership: StoredMembership::new(Some(id), membership),
        snapshot_id: format!("restore-{index}"),
    }
}

pub fn base_of(meta: &SnapshotMeta<NodeId, BasicNode>, snapshot: SnapshotRef) -> Base {
    let id = meta.last_log_id.expect("a restore snapshot has a log id");
    Base { at: position(&id), term: id.leader_id.term, leader: id.leader_id.node_id, snapshot }
}
