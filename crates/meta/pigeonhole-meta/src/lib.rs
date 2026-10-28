//! Metadata tablet (see `docs/meta-backends.md`).
//!
//! Every metadata change is a **command** owned by one layer (namespace).
//! [`MetaStore::propose`] is the only write path: it hands the command to
//! Raft ([`raft`], openraft), which appends it to the node's [`LogStore`],
//! commits it and applies it in order; only then the caller gets its
//! outcome. The log is the source of truth; each namespace keeps its state in
//! memory ([`StateMachine`], read via [`MetaStore::read`]).
//!
//! Commands must be deterministic: ids, timestamps and locators are chosen by
//! the proposer and carried in the command, never generated in `apply`. That is
//! what lets the log rebuild the same state elsewhere.
//!
//! A command can fail on state (e.g. retain of a reclaimed chunk). `apply` then
//! returns `Ok(Err(reject))` and leaves the state untouched, but the entry
//! still consumes a position — every replay reaches the same verdict.
//!
//! Encodings (postcard): commands carry their namespace's
//! [`StateMachine::VERSION`] and batches the [`LOG_FORMAT`]; a binary refuses a
//! log written in another format instead of misreading it. States encode the
//! same way for snapshots ([`encode_state`] / [`decode_state`]).
//!
//! Every [`MetaOptions::snapshot_every`] entries (and, with new entries,
//! every [`MetaOptions::snapshot_interval`]) Raft snapshots all states into
//! one [`TabletSnapshot`], stored in the [`SnapshotStore`]; the log is then
//! [rebased](LogStore::rebase) on it, which drops the log behind it.
//!
//! Boot ([`MetaStore::open`]): claim the next generation of the node's log
//! (a second process on it is fenced), load the snapshot the log starts from,
//! start Raft (a cluster of one for now): it elects itself, appends a blank
//! entry and applies the log. Once a newer generation claims the log the
//! tablet stops ([`Stop::Fenced`]) and every later `propose` fails.

use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::marker::PhantomData;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::watch;

pub mod log;
pub mod raft;
pub mod snapshot;

pub use log::{
    Base, Batch, Discovered, Entry, EntryStream, Head, LocalLogStore, LogError, LogId, LogResult,
    LogStore, MemoryLogStore, Position, LOG_FORMAT,
};
pub use snapshot::{LocalSnapshotStore, MemorySnapshotStore, SnapshotRef, SnapshotStore, StoredSnapshot};

/// A rejection that cannot happen (a namespace whose commands never fail).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum Never {}

/// Outcome of a command: its reply or its deterministic rejection.
pub type Outcome<S> = std::result::Result<<S as StateMachine>::Reply, <S as StateMachine>::Reject>;

/// Typed in-memory state of one namespace. The log (and later its snapshots)
/// is the only durable copy: every boot rebuilds the state from it.
///
/// The state is serialized for snapshots, so ordered containers only
/// (`BTreeMap`): equal states must encode to equal bytes. Derived indexes are
/// `#[serde(skip)]` and recomputed in [`Self::rebuild`].
pub trait StateMachine: Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable name used to route commands (`chunk`, `s3`, `cas`).
    const NAME: &'static str;
    /// Encoding version of [`Self::Command`] and of the state itself: bump it
    /// on any change to either shape (variant order, fields), or replay and
    /// snapshot loading would misread old data. Old logs and snapshots then
    /// refuse to load instead.
    const VERSION: u32;

    type Command: Serialize + DeserializeOwned + Send + 'static;
    /// Returned to the proposer; serialized, since the proposer may be
    /// another node.
    type Reply: Serialize + DeserializeOwned + Send + 'static;
    type Reject: Serialize + DeserializeOwned + Send + 'static;

    /// Apply one command; must be deterministic. Validate before mutating: a
    /// rejection must leave the state untouched. `Err` is a command that cannot
    /// be applied at all (a bug); it stops the tablet.
    fn apply(&mut self, command: Self::Command) -> Result<Outcome<Self>>;

    /// Recompute `#[serde(skip)]` fields after [`decode_state`].
    fn rebuild(&mut self) {}
}

/// Snapshot encoding of a state.
pub fn encode_state<S: StateMachine>(state: &S) -> Vec<u8> {
    postcard::to_stdvec(state).expect("state always encodes")
}

/// Inverse of [`encode_state`], indexes rebuilt.
pub fn decode_state<S: StateMachine>(data: &[u8]) -> Result<S> {
    let mut state: S = postcard::from_bytes(data).with_context(|| format!("decode {} state", S::NAME))?;
    state.rebuild();
    Ok(state)
}

/// A command for `S`, built by the layer that owns `S` and opaque to anyone
/// else: a caller may order and propose it ([`Proposal`]) without seeing it.
pub struct Prepared<S: StateMachine> {
    command: Command,
    _s: PhantomData<fn() -> S>,
}

impl<S: StateMachine> Prepared<S> {
    pub fn new(command: &S::Command) -> Self {
        Self { command: self::command::<S>(command), _s: PhantomData }
    }
}

impl<S: StateMachine> fmt::Debug for Prepared<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Prepared<{}>", S::NAME)
    }
}

/// Commands to commit as one entry ([`MetaStore::propose_all`]).
#[derive(Default)]
pub struct Proposal {
    commands: Vec<Command>,
}

/// Where the outcome of one command of a [`Proposal`] is.
#[derive(Debug, PartialEq, Eq)]
pub struct Slot<S> {
    index: usize,
    _s: PhantomData<fn() -> S>,
}

impl<S> Clone for Slot<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Slot<S> {}

impl Proposal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push<S: StateMachine>(&mut self, prepared: Prepared<S>) -> Slot<S> {
        self.commands.push(prepared.command);
        Slot { index: self.commands.len() - 1, _s: PhantomData }
    }

    pub fn extend<S: StateMachine>(&mut self, prepared: impl IntoIterator<Item = Prepared<S>>) -> Vec<Slot<S>> {
        prepared.into_iter().map(|p| self.push(p)).collect()
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

/// The outcomes of a committed [`Proposal`].
pub struct Outcomes {
    outcomes: Vec<Option<Vec<u8>>>,
}

impl Outcomes {
    /// The outcome of the command at `slot` (each taken once).
    pub fn take<S: StateMachine>(&mut self, slot: Slot<S>) -> Result<Outcome<S>> {
        let bytes = self
            .outcomes
            .get_mut(slot.index)
            .and_then(Option::take)
            .with_context(|| format!("no outcome at {} (taken already?)", slot.index))?;
        postcard::from_bytes(&bytes).with_context(|| format!("decode the outcome of a {} command", S::NAME))
    }
}

/// One layer command: what an entry's payload holds. `version` is the
/// namespace's command format ([`StateMachine::VERSION`]) it was encoded with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Command {
    pub namespace: String,
    pub version: u32,
    pub command: Vec<u8>,
}

impl Command {
    /// The log entry carrying this command, written in `term`.
    pub fn entry(&self, term: u64) -> Entry {
        Entry { term, payload: postcard::to_stdvec(self).expect("a command always encodes") }
    }

    pub fn decode(entry: &Entry) -> Result<Self> {
        postcard::from_bytes(&entry.payload).context("decode the command of an entry")
    }
}

/// `command` for `S`, ready to go into an entry.
pub fn command<S: StateMachine>(command: &S::Command) -> Command {
    Command {
        namespace: S::NAME.to_string(),
        version: S::VERSION,
        command: postcard::to_stdvec(command).expect("command always encodes"),
    }
}

/// Version of the [`TabletSnapshot`] envelope.
pub const SNAPSHOT_FORMAT: u32 = 1;

/// Every namespace's state as of `position`, in one stored snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct TabletSnapshot {
    /// [`SNAPSHOT_FORMAT`]; first, so it can be read before the rest.
    pub format: u32,
    pub position: Position,
    /// `(namespace, VERSION, encode_state)`, ordered by namespace.
    pub states: Vec<(String, u32, Vec<u8>)>,
}

impl TabletSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("a snapshot always encodes")
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let (format, _) = postcard::take_from_bytes::<u32>(data).context("snapshot header")?;
        if format != SNAPSHOT_FORMAT {
            bail!("snapshot in format {format}, this binary reads {SNAPSHOT_FORMAT}");
        }
        postcard::from_bytes(data).context("decode snapshot")
    }
}

/// Object-safe face of a [`StateMachine`] behind its lock.
trait AnyState: Send + Sync {
    /// Apply and encode the [`Outcome`] for the proposer.
    fn apply(&self, command: &Command) -> Result<Vec<u8>>;
    /// `(VERSION, encode_state)`.
    fn snapshot(&self) -> (u32, Vec<u8>);
    fn restore(&self, version: u32, data: &[u8]) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

struct StateCell<S>(RwLock<S>);

impl<S: StateMachine> AnyState for StateCell<S> {
    fn apply(&self, command: &Command) -> Result<Vec<u8>> {
        if command.version != S::VERSION {
            bail!(
                "{} command in format {}, this binary reads {}",
                S::NAME,
                command.version,
                S::VERSION
            );
        }
        let cmd: S::Command =
            postcard::from_bytes(&command.command).with_context(|| format!("decode {} command", S::NAME))?;
        let outcome = self.0.write().unwrap().apply(cmd)?;
        Ok(postcard::to_stdvec(&outcome).expect("an outcome always encodes"))
    }

    fn snapshot(&self) -> (u32, Vec<u8>) {
        (S::VERSION, encode_state(&*self.0.read().unwrap()))
    }

    fn restore(&self, version: u32, data: &[u8]) -> Result<()> {
        if version != S::VERSION {
            bail!("{} state in format {version}, this binary reads {}", S::NAME, S::VERSION);
        }
        *self.0.write().unwrap() = decode_state(data)?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The namespaces a tablet serves, fixed at [`MetaStore::open`]: replay must
/// be able to apply every entry in the log.
#[derive(Default)]
pub struct Namespaces {
    states: Vec<(&'static str, Arc<dyn AnyState>)>,
}

impl Namespaces {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serve `S`, starting from its empty state.
    pub fn with<S: StateMachine>(mut self) -> Self {
        self.states.push((S::NAME, Arc::new(StateCell(RwLock::new(S::default())))));
        self
    }
}

/// Why the tablet stopped accepting proposals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    /// A newer generation claimed the log.
    Fenced { current: u64 },
    /// The log failed or a command could not be applied; the outcome of the
    /// last command is unknown until the next boot replays the log.
    Failed(String),
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stop::Fenced { current } => write!(f, "tablet fenced by generation {current}"),
            Stop::Failed(why) => write!(f, "tablet stopped: {why}"),
        }
    }
}

impl std::error::Error for Stop {}

/// Tablet knobs (`[meta]`).
#[derive(Clone, Debug)]
pub struct MetaOptions {
    /// Take a snapshot (and collect the log behind it) after this many
    /// entries since the last one; 0 = never.
    pub snapshot_every: u64,
    /// Also take one when this long has passed and there are new entries,
    /// even with no traffic; zero = never. Bounds how stale the newest
    /// snapshot can get.
    pub snapshot_interval: Duration,
    /// The snapshot store can survive the log (another disk, a chat), so a
    /// lossy restore from it is possible: see [`MetaStore::restorable_position`].
    pub snapshots_outlive_log: bool,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self { snapshot_every: 10_000, snapshot_interval: Duration::ZERO, snapshots_outlive_log: false }
    }
}

pub(crate) type States = HashMap<&'static str, Arc<dyn AnyState>>;

/// A proposal waiting for its batch: one or more commands that go into one
/// entry together, answered with the entry's position and their outcomes.
struct Pending {
    commands: Vec<Command>,
    reply: tokio::sync::oneshot::Sender<Result<(Position, Vec<Vec<u8>>)>>,
}

/// Commands per Raft entry at most, unless one proposal alone has more (a
/// proposal is never split across entries).
const MAX_BATCH: usize = 1024;

/// Group commit: openraft 0.9 writes and flushes one entry per
/// `client_write`, so proposals queued while one is in flight go together in
/// the next entry (one fsync, one chat message).
async fn batcher(raft: raft::Raft, shared: Arc<raft::Shared>, mut rx: tokio::sync::mpsc::UnboundedReceiver<Pending>) {
    while let Some(first) = rx.recv().await {
        let mut size = first.commands.len();
        let mut batch = vec![first];
        let mut next = None;
        while size < MAX_BATCH {
            match rx.try_recv() {
                Ok(p) if size + p.commands.len() > MAX_BATCH => {
                    next = Some(p);
                    break;
                }
                Ok(p) => {
                    size += p.commands.len();
                    batch.push(p);
                }
                Err(_) => break,
            }
        }
        write_batch(&raft, &shared, batch).await;
        if let Some(p) = next {
            write_batch(&raft, &shared, vec![p]).await;
        }
    }
}

async fn write_batch(raft: &raft::Raft, shared: &raft::Shared, batch: Vec<Pending>) {
    let commands = batch.iter().flat_map(|p| p.commands.iter().cloned()).collect();
    match raft.client_write(commands).await {
        Ok(resp) => {
            let position = raft::position(&resp.log_id);
            let mut outcomes = resp.data.into_iter();
            for p in batch {
                let mine = outcomes.by_ref().take(p.commands.len()).collect();
                let _ = p.reply.send(Ok((position, mine)));
            }
        }
        Err(e) => {
            let stop = shared.stop.borrow().clone().unwrap_or_else(|| Stop::Failed(e.to_string()));
            shared.stop.send_replace(Some(stop.clone()));
            for p in batch {
                let _ = p.reply.send(Err(stop.clone().into()));
            }
        }
    }
}

/// The metadata tablet shared by every layer.
pub struct MetaStore {
    states: Arc<States>,
    gen: u64,
    raft: raft::Raft,
    log: Arc<dyn LogStore>,
    shared: Arc<raft::Shared>,
    snapshots_outlive_log: bool,
    stop: watch::Receiver<Option<Stop>>,
    /// Proposals waiting for the batcher.
    queue: tokio::sync::mpsc::UnboundedSender<Pending>,
    /// The periodic snapshot task; ends with the store.
    ticker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for MetaStore {
    fn drop(&mut self) {
        if let Some(t) = self.ticker.lock().unwrap().take() {
            t.abort();
        }
        let raft = self.raft.clone();
        tokio::spawn(async move {
            let _ = raft.shutdown().await;
        });
    }
}

impl MetaStore {
    /// Boot the tablet: claim the next generation of the node's `log`, load
    /// the snapshot it starts from out of `snapshots`, start Raft as a cluster
    /// of one and wait until it has applied the log.
    pub async fn open(
        log: Arc<dyn LogStore>,
        snapshots: Arc<dyn SnapshotStore>,
        namespaces: Namespaces,
        opts: MetaOptions,
    ) -> Result<Arc<Self>> {
        let mut states = States::new();
        for (name, state) in namespaces.states {
            if states.insert(name, state).is_some() {
                bail!("namespace {name:?} given twice");
            }
        }
        let states = Arc::new(states);

        let known = log.discover().await.context("discover log")?;
        if known.is_none() {
            // A lost log would otherwise boot empty, and the sweeper would
            // take every blob for an orphan.
            if let Some(s) = snapshots.newest().await.context("look for stored snapshots")? {
                bail!(
                    "the log was never claimed but the snapshot store holds log {} at {}: \
                     run `pigeonhole restore --lossy`, or use an empty snapshot store",
                    s.log_id,
                    s.at
                );
            }
        }
        let (gen, head) = claim_next(log.as_ref(), known.map_or(0, |d| d.gen)).await?;
        let d = log.discover().await.context("discover log")?.context("a claimed log is discovered")?;

        let (stop_tx, stop) = watch::channel(None);
        let shared = Arc::new(raft::Shared {
            stop: stop_tx,
            applied: AtomicU64::new(0),
            based: AtomicU64::new(d.base.as_ref().map_or(0, |b| b.at)),
            current: Mutex::new(None),
            base_ref: Mutex::new(d.base.as_ref().map(|b| b.snapshot.clone())),
        });
        let mut sm = raft::StateMachine {
            states: states.clone(),
            snapshots: snapshots.clone(),
            history: head.log_id,
            last_applied: None,
            membership: Default::default(),
            shared: shared.clone(),
        };
        if let Some(base) = &d.base {
            let stored = snapshots
                .get(&base.snapshot)
                .await
                .with_context(|| format!("load snapshot {:?} the log starts from", base.snapshot.0))?;
            if stored.log_id != head.log_id {
                bail!("snapshot {:?} is of log {}, not of log {}", base.snapshot.0, stored.log_id, head.log_id);
            }
            let snap = raft::RaftSnapshot::decode(&stored.data)?;
            if snap.meta.last_log_id.as_ref().map_or(0, raft::position) < base.at {
                bail!("snapshot {:?} is behind the log's base {}", base.snapshot.0, base.at);
            }
            restore_states(&states, &snap.tablet)?;
            sm.last_applied = snap.meta.last_log_id;
            sm.membership = snap.meta.last_membership.clone();
            shared.applied.store(snap.tablet.position, Ordering::SeqCst);
            *shared.current.lock().unwrap() =
                Some(raft::CurrentSnapshot { meta: snap.meta, data: stored.data, snapshot: base.snapshot.clone() });
        }
        let fresh = d.head.position == 0 && d.base.is_none() && d.vote.is_none();

        let adapter =
            raft::LogAdapter::new(log.clone(), gen, head.log_id, d.head.position, shared.clone(), snapshots.clone());
        let config = openraft::Config {
            cluster_name: "pigeonhole".into(),
            heartbeat_interval: 50,
            election_timeout_min: 100,
            election_timeout_max: 150,
            snapshot_policy: match opts.snapshot_every {
                0 => openraft::SnapshotPolicy::Never,
                n => openraft::SnapshotPolicy::LogsSinceLast(n),
            },
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .context("raft config")?;
        let raft = raft::Raft::new(raft::SOLO, Arc::new(config), raft::NoNetwork, adapter, sm)
            .await
            .context("start raft")?;
        if fresh {
            let mut nodes = std::collections::BTreeMap::new();
            nodes.insert(raft::SOLO, openraft::BasicNode::default());
            raft.initialize(nodes).await.context("initialize a cluster of one")?;
        }
        let (queue, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(batcher(raft.clone(), shared.clone(), rx));
        let store = Self {
            states,
            gen,
            queue,
            raft: raft.clone(),
            log,
            shared: shared.clone(),
            snapshots_outlive_log: opts.snapshots_outlive_log,
            stop,
            ticker: Mutex::new(None),
        };
        // Leader, and everything the log holds applied.
        raft.wait(Some(Duration::from_secs(30)))
            .state(openraft::ServerState::Leader, "become the leader of a cluster of one")
            .await
            .map_err(|e| store.stopped_by(e))
            .context("boot the tablet")?;
        store.confirm_active().await.context("boot the tablet")?;
        let ticker = (!opts.snapshot_interval.is_zero()).then(|| {
            let (raft, shared, every) = (raft.clone(), shared.clone(), opts.snapshot_interval);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let snapshotted = raft.metrics().borrow().snapshot.as_ref().map_or(0, raft::position);
                    if shared.applied.load(Ordering::SeqCst) > snapshotted && raft.trigger().snapshot().await.is_err() {
                        return;
                    }
                }
            })
        });
        *store.ticker.lock().unwrap() = ticker;
        Ok(Arc::new(store))
    }

    /// Boot over a [`LocalLogStore`] in `dir`, snapshots in the same directory.
    pub async fn open_local(dir: impl AsRef<Path>, namespaces: Namespaces) -> Result<Arc<Self>> {
        let log = LocalLogStore::open(dir.as_ref())?;
        let snapshots = LocalSnapshotStore::new(dir.as_ref());
        Self::open(Arc::new(log), Arc::new(snapshots), namespaces, MetaOptions::default()).await
    }

    pub fn has_namespace(&self, name: &str) -> bool {
        self.states.contains_key(name)
    }

    /// Read `S`'s state under a short read lock (never held across `.await`;
    /// copy out what you need).
    ///
    /// Panics if the tablet was opened without `S`: layers check
    /// [`Self::has_namespace`] when they open.
    pub fn read<S: StateMachine, R>(&self, f: impl FnOnce(&S) -> R) -> R {
        let cell = self
            .states
            .get(S::NAME)
            .unwrap_or_else(|| panic!("tablet has no namespace {:?}", S::NAME))
            .as_any()
            .downcast_ref::<StateCell<S>>()
            .unwrap_or_else(|| panic!("namespace {:?} has another state type", S::NAME));
        f(&cell.0.read().unwrap())
    }

    /// Generation this tablet claimed on boot.
    pub fn generation(&self) -> u64 {
        self.gen
    }

    /// The stop reason, or `err` turned into one (the tablet is stopped).
    fn stopped_by(&self, err: impl fmt::Display) -> anyhow::Error {
        if let Some(stop) = self.stop.borrow().clone() {
            return stop.into();
        }
        let stop = Stop::Failed(err.to_string());
        self.shared.stop.send_replace(Some(stop.clone()));
        stop.into()
    }

    /// Commit `command` for `S` and apply it. `Err` = the tablet could not
    /// commit it (fenced, stopped, I/O); `Ok(Err(_))` = `S` rejected it.
    pub async fn propose<S: StateMachine>(&self, command: S::Command) -> Result<Outcome<S>> {
        Ok(self.propose_at::<S>(command).await?.1)
    }

    /// [`Self::propose`], also returning the entry's log position.
    pub async fn propose_at<S: StateMachine>(&self, command: S::Command) -> Result<(Position, Outcome<S>)> {
        if !self.has_namespace(S::NAME) {
            bail!("unknown namespace {:?}", S::NAME);
        }
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop.into());
        }
        let mut proposal = Proposal::new();
        let slot = proposal.push(Prepared::<S>::new(&command));
        let (position, mut outcomes) = self.propose_all_at(proposal).await?;
        Ok((position, outcomes.take(slot)?))
    }

    /// Commit every command of `proposal` in one entry, in order, and apply
    /// each in its own namespace: all or none are durable, and a rejection of
    /// one undoes nothing else. Outcomes are taken by the slots `push` gave.
    pub async fn propose_all(&self, proposal: Proposal) -> Result<Outcomes> {
        Ok(self.propose_all_at(proposal).await?.1)
    }

    /// [`Self::propose_all`], also returning the entry's log position.
    pub async fn propose_all_at(&self, proposal: Proposal) -> Result<(Position, Outcomes)> {
        for c in &proposal.commands {
            if !self.has_namespace(&c.namespace) {
                bail!("unknown namespace {:?}", c.namespace);
            }
        }
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop.into());
        }
        if proposal.commands.is_empty() {
            return Ok((self.applied_position(), Outcomes { outcomes: Vec::new() }));
        }
        let (reply, rx) = tokio::sync::oneshot::channel();
        if self.queue.send(Pending { commands: proposal.commands, reply }).is_err() {
            bail!("the tablet's batcher is gone");
        }
        let (position, outcomes) = rx.await.context("the tablet's batcher dropped the proposal")??;
        Ok((position, Outcomes { outcomes: outcomes.into_iter().map(Some).collect() }))
    }

    /// Prove the tablet still holds its generation: Raft confirms it leads
    /// and has applied everything, and the log confirms nobody claimed it
    /// since. `Err` = fenced or stopped. Call it right before a side effect
    /// the log cannot undo, such as deleting backend blobs.
    pub async fn confirm_active(&self) -> Result<()> {
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop.into());
        }
        if let Err(e) = self.raft.ensure_linearizable().await {
            return Err(self.stopped_by(e));
        }
        match self.log.confirm(self.gen).await {
            Ok(()) => Ok(()),
            Err(LogError::Fenced { current }) => {
                let stop = Stop::Fenced { current };
                self.shared.stop.send_replace(Some(stop.clone()));
                Err(stop.into())
            }
            Err(e) => Err(anyhow::Error::new(e).context("confirm the log's generation")),
        }
    }

    /// Whether the tablet has stopped (fenced or failed).
    pub fn is_stopped(&self) -> bool {
        self.stop.borrow().is_some()
    }

    /// Resolves once the tablet stops accepting proposals.
    pub async fn stopped(&self) -> Stop {
        let mut rx = self.stop.clone();
        let stop = match rx.wait_for(Option::is_some).await {
            Ok(stop) => stop.clone().unwrap(),
            Err(_) => Stop::Failed("the tablet is gone".into()),
        };
        stop
    }

    /// Position of the snapshot the log now starts from (0: none yet).
    pub fn base_position(&self) -> Position {
        self.shared.based.load(Ordering::SeqCst)
    }

    /// When snapshots outlive the log ([`MetaOptions::snapshots_outlive_log`]):
    /// [`Self::base_position`], the least a lossy restore brings back.
    /// `None` when there is no such restore.
    pub fn restorable_position(&self) -> Option<Position> {
        self.snapshots_outlive_log.then(|| self.base_position())
    }

    /// Position of the last applied entry (0 before the first).
    pub fn applied_position(&self) -> Position {
        self.shared.applied.load(Ordering::SeqCst)
    }
}

/// Claim `1 + max(known generations)`; a racing newer claim moves the floor up.
async fn claim_next(log: &dyn LogStore, known: u64) -> Result<(u64, Head)> {
    let mut gen = known + 1;
    for _ in 0..16 {
        match log.claim(gen).await {
            Ok(head) => return Ok((gen, head)),
            Err(LogError::Fenced { current }) => gen = gen.max(current + 1),
            Err(e) => return Err(anyhow::Error::new(e).context(format!("claim generation {gen}"))),
        }
    }
    bail!("could not claim a generation (last tried {gen})")
}

/// Start a new log from the newest stored snapshot (after the log is lost):
/// a cluster of one with the states as of that snapshot. Lossy: whatever the
/// old log held after the snapshot is gone. Refuses a log that has entries.
/// Returns the position the snapshot covered in the old log.
pub async fn restore_lossy(log: &dyn LogStore, snapshots: &dyn SnapshotStore) -> Result<Position> {
    let known = log.discover().await.context("discover log")?;
    if known.as_ref().is_some_and(|d| d.head.position != 0 || d.base.is_some()) {
        bail!("the log is not empty; restore only seeds a new log");
    }
    let newest = snapshots.newest().await?.context("no stored snapshot to restore from")?;
    let old = raft::RaftSnapshot::decode(&newest.data)?;
    let old_position = old.tablet.position;
    let meta = raft::solo_meta(0);
    let tablet = TabletSnapshot { position: 1, ..old.tablet };
    let (gen, head) = claim_next(log, known.map_or(0, |d| d.gen)).await?;
    let data = raft::RaftSnapshot { meta: meta.clone(), tablet }.encode();
    let stored = StoredSnapshot { log_id: head.log_id, at: 1, data };
    let snapshot = snapshots.put(&stored).await.context("store the restored snapshot")?;
    log.rebase(gen, raft::base_of(&meta, snapshot)).await.context("start the new log from it")?;
    Ok(old_position)
}

/// Apply one command to its namespace; the encoded outcome.
pub(crate) fn apply_command(states: &States, pos: Position, command: &Command) -> Result<Vec<u8>> {
    let state = states
        .get(command.namespace.as_str())
        .with_context(|| format!("entry {pos}: unknown namespace {:?}", command.namespace))?;
    state
        .apply(command)
        .with_context(|| format!("apply {} entry {pos}", command.namespace))
}

/// Encode every state as of `position` (between applies, so nothing is half
/// applied).
pub(crate) fn take_snapshot(states: &States, position: Position) -> TabletSnapshot {
    let mut snap: Vec<(String, u32, Vec<u8>)> = states
        .iter()
        .map(|(name, state)| {
            let (version, data) = state.snapshot();
            (name.to_string(), version, data)
        })
        .collect();
    snap.sort_by(|a, b| a.0.cmp(&b.0));
    TabletSnapshot { format: SNAPSHOT_FORMAT, position, states: snap }
}

/// Load every namespace's state from `snap`.
pub(crate) fn restore_states(states: &States, snap: &TabletSnapshot) -> Result<()> {
    for (name, version, data) in &snap.states {
        let state = states
            .get(name.as_str())
            .with_context(|| format!("snapshot has unknown namespace {name:?}"))?;
        state.restore(*version, data)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
