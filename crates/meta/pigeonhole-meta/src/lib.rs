//! Metadata tablet (see `docs/meta-backends.md`).
//!
//! Every metadata change is a **command** owned by one layer (namespace).
//! [`MetaStore::propose`] is the only write path: it queues the command, a
//! batcher appends a batch of queued commands to the [`LogStore`] under the
//! tablet's generation, applies them in order and only then answers the
//! callers. The log is the source of truth; each namespace keeps its state in
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
//! every [`MetaOptions::snapshot_interval`]) the batcher encodes all states
//! into one [`TabletSnapshot`], stores it in the [`SnapshotStore`] in the
//! background and then [rebases](LogStore::rebase) the log on it, which drops
//! the log behind it.
//!
//! Boot ([`MetaStore::open`]): claim the next generation, load the snapshot
//! the log starts from and replay the log after it, append the zero entry, go active. Once a newer
//! generation claims the log the tablet stops ([`Stop::Fenced`]) and every
//! later `propose` fails.

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

pub mod log;
pub mod snapshot;

pub use log::{
    Base, Batch, Discovered, Entry, EntryStream, Head, LocalLogStore, LogError, LogId, LogResult,
    LogStore, MemoryLogStore, Position, LOG_FORMAT,
};
pub use snapshot::{LocalSnapshotStore, MemorySnapshotStore, SnapshotRef, SnapshotStore, StoredSnapshot};

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
    /// Returned to the proposer in-process; never serialized.
    type Reply: Send + 'static;
    type Reject: Send + 'static;

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

/// [`Outcome`] behind `dyn Any`, so one batcher serves every namespace.
type AnyOutcome = Box<dyn Any + Send>;

/// Object-safe face of a [`StateMachine`] behind its lock.
trait AnyState: Send + Sync {
    fn apply(&self, entry: &Command) -> Result<AnyOutcome>;
    /// `(VERSION, encode_state)`.
    fn snapshot(&self) -> (u32, Vec<u8>);
    fn restore(&self, version: u32, data: &[u8]) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

struct StateCell<S>(RwLock<S>);

impl<S: StateMachine> AnyState for StateCell<S> {
    fn apply(&self, entry: &Command) -> Result<AnyOutcome> {
        if entry.version != S::VERSION {
            bail!(
                "{} command in format {}, this binary reads {}",
                S::NAME,
                entry.version,
                S::VERSION
            );
        }
        let command: S::Command =
            postcard::from_bytes(&entry.command).with_context(|| format!("decode {} command", S::NAME))?;
        let outcome = self.0.write().unwrap().apply(command)?;
        Ok(Box::new(outcome))
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
    /// last batch is unknown until the next boot replays the log.
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
    /// Extra time to wait for more commands before appending a batch. Zero =
    /// append as soon as the previous append finished (batches still form from
    /// whatever queued meanwhile).
    pub batch_max_delay: Duration,
    pub batch_max_entries: usize,
    /// Take a snapshot (and collect the log behind it) after this many
    /// entries since the last one; 0 = never.
    pub snapshot_every: u64,
    /// Also take one when this long has passed since the last and there are
    /// new entries, even with no traffic; zero = never. Bounds how stale the
    /// newest snapshot can get.
    pub snapshot_interval: Duration,
    /// The snapshot store can survive the log (another disk, a chat), so a
    /// lossy restore from it is possible: see [`MetaStore::restorable_position`].
    pub snapshots_outlive_log: bool,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            batch_max_delay: Duration::ZERO,
            batch_max_entries: 1024,
            snapshot_every: 10_000,
            snapshot_interval: Duration::ZERO,
            snapshots_outlive_log: false,
        }
    }
}

type States = HashMap<&'static str, Arc<dyn AnyState>>;

struct Pending {
    /// `None` = a barrier ([`MetaStore::confirm_active`]): no entry, answered
    /// once its batch is appended.
    entry: Option<Command>,
    reply: oneshot::Sender<Result<(Position, AnyOutcome)>>,
}

/// The metadata tablet shared by every layer.
pub struct MetaStore {
    states: Arc<States>,
    gen: u64,
    /// Position of the last applied entry.
    applied: Arc<AtomicU64>,
    /// Position of the snapshot the log starts from.
    based: Arc<AtomicU64>,
    snapshots_outlive_log: bool,
    queue: mpsc::UnboundedSender<Pending>,
    stop: watch::Receiver<Option<Stop>>,
}

impl MetaStore {
    /// Boot the tablet: claim the next generation of `log`, load the snapshot
    /// it starts from out of `snapshots`, replay the rest, append the zero entry.
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
        let (base, head_term) = recover(&states, log.as_ref(), snapshots.as_ref(), head).await?;
        let base_at = base.as_ref().map_or(0, |b| b.at);

        let zero = log.append(gen, &[]).await.context("append zero entry")?;
        if zero != head.position {
            bail!("zero entry of generation {gen} at {zero}, expected {}", head.position);
        }

        let applied = Arc::new(AtomicU64::new(head.position));
        let (queue, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop) = watch::channel(None);
        let based = Arc::new(AtomicU64::new(base_at));
        let snapshots_outlive_log = opts.snapshots_outlive_log;
        let (stored_tx, stored_rx) = mpsc::unbounded_channel();
        tokio::spawn(batcher(
            Batcher {
                states: states.clone(),
                applied: applied.clone(),
                log_id: head.log_id,
                snapshot_at: base_at,
                head_term,
                snapshot_tried: tokio::time::Instant::now(),
                putting: false,
                stored_tx,
                base,
                based: based.clone(),
                log,
                snapshots,
                gen,
                opts,
                stop: stop_tx,
            },
            rx,
            stored_rx,
        ));
        Ok(Arc::new(Self { states, gen, applied, based, snapshots_outlive_log, queue, stop }))
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

    /// Append `command` for `S` to the log and apply it. `Err` = the tablet
    /// could not commit it (fenced, stopped, I/O); `Ok(Err(_))` = `S` rejected it.
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
        let (reply, rx) = oneshot::channel();
        if self.queue.send(Pending { entry: Some(self::command::<S>(&command)), reply }).is_err() {
            bail!("tablet batcher is gone");
        }
        let (position, outcome) = rx.await.context("tablet batcher dropped the proposal")??;
        let outcome = outcome
            .downcast::<Outcome<S>>()
            .unwrap_or_else(|_| panic!("namespace {:?} has another state type", S::NAME));
        Ok((position, *outcome))
    }

    /// Prove the tablet still holds its generation: a round trip through the
    /// log (an empty batch if nothing else is queued). `Err` = fenced or
    /// stopped. Call it right before a side effect the log cannot undo, such
    /// as deleting backend blobs.
    pub async fn confirm_active(&self) -> Result<()> {
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop.into());
        }
        let (reply, rx) = oneshot::channel();
        if self.queue.send(Pending { entry: None, reply }).is_err() {
            bail!("tablet batcher is gone");
        }
        rx.await.context("tablet batcher dropped the barrier")??;
        Ok(())
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
            Err(_) => Stop::Failed("tablet batcher is gone".into()),
        };
        stop
    }

    /// Position of the snapshot the log now starts from (0: none yet).
    pub fn base_position(&self) -> Position {
        self.based.load(Ordering::SeqCst)
    }

    /// When snapshots outlive the log ([`MetaOptions::snapshots_outlive_log`]):
    /// [`Self::base_position`], the least a lossy restore brings back.
    /// `None` when there is no such restore.
    pub fn restorable_position(&self) -> Option<Position> {
        self.snapshots_outlive_log.then(|| self.base_position())
    }

    /// Position of the last applied entry (0 before the first).
    pub fn applied_position(&self) -> Position {
        self.applied.load(Ordering::SeqCst)
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

/// Load the newest snapshot into the empty states and replay the log after
/// it up to `head`. Returns the snapshot's position (0 without one).
/// Load the snapshot the log starts from (if any) and replay the rest;
/// returns the base and the term of the entry at the head.
async fn recover(
    states: &States,
    log: &dyn LogStore,
    snapshots: &dyn SnapshotStore,
    head: Head,
) -> Result<(Option<Base>, u64)> {
    let discovered = log.discover().await.context("discover log")?;
    let head_term = discovered.as_ref().map_or(0, |d| d.head_term);
    let base = discovered.and_then(|d| d.base);
    let from = match &base {
        Some(base) => {
            let at = base.at;
            let stored = snapshots
                .get(&base.snapshot)
                .await
                .with_context(|| format!("load snapshot {:?} the log starts from", base.snapshot.0))?;
            if stored.log_id != head.log_id || stored.at != at {
                bail!(
                    "snapshot {:?} is of log {} at {}, but log {} starts at {at}",
                    base.snapshot.0,
                    stored.log_id,
                    stored.at,
                    head.log_id
                );
            }
            let snap = TabletSnapshot::decode(&stored.data)?;
            if snap.position != at {
                bail!("snapshot stored at {at} says it covers {}", snap.position);
            }
            for (name, version, data) in &snap.states {
                let state = states
                    .get(name.as_str())
                    .with_context(|| format!("snapshot has unknown namespace {name:?}"))?;
                state.restore(*version, data)?;
            }
            at
        }
        None => 0,
    };
    if from > head.position {
        bail!(LogError::Lost(format!("snapshot at {from} is past the log head {}", head.position)));
    }
    replay(states, log, from, head).await?;
    Ok((base, head_term))
}

/// Start a new log from the newest stored snapshot (after the log is lost):
/// the states as of that snapshot, positions numbered from 0 again. Lossy:
/// whatever the old log held after the snapshot is gone. Refuses a log that
/// has entries. Returns the position the snapshot covered in the old log.
pub async fn restore_lossy(log: &dyn LogStore, snapshots: &dyn SnapshotStore) -> Result<Position> {
    let known = log.discover().await.context("discover log")?;
    if known.as_ref().is_some_and(|d| d.head.position != 0 || d.base.is_some()) {
        bail!("the log is not empty; restore only seeds a new log");
    }
    let newest = snapshots.newest().await?.context("no stored snapshot to restore from")?;
    let mut snap = TabletSnapshot::decode(&newest.data)?;
    let old = snap.position;
    snap.position = 0;
    let (gen, head) = claim_next(log, known.map_or(0, |d| d.gen)).await?;
    let stored = StoredSnapshot { log_id: head.log_id, at: 0, data: snap.encode() };
    let snapshot = snapshots.put(&stored).await.context("store the restored snapshot")?;
    log.rebase(gen, Base { at: 0, term: 0, snapshot }).await.context("start the new log from it")?;
    Ok(old)
}

/// Apply log entries `(from, head]`.
async fn replay(states: &States, log: &dyn LogStore, from: Position, head: Head) -> Result<()> {
    if from == head.position {
        return Ok(());
    }
    let mut entries = log.read(from, head);
    let mut next = from + 1;
    while let Some((pos, entry)) = entries.try_next().await.context("read log")? {
        if pos != next {
            bail!(LogError::Lost(format!("replay expected {next}, got {pos}")));
        }
        apply_entry(states, pos, &entry)?;
        next += 1;
    }
    if next - 1 != head.position {
        bail!(LogError::Lost(format!("replay stopped at {}, head is {}", next - 1, head.position)));
    }
    Ok(())
}

fn apply_entry(states: &States, pos: Position, entry: &Entry) -> Result<AnyOutcome> {
    let entry = &Command::decode(entry).with_context(|| format!("entry {pos}"))?;
    let state = states
        .get(entry.namespace.as_str())
        .with_context(|| format!("entry {pos}: unknown namespace {:?}", entry.namespace))?;
    state
        .apply(entry)
        .with_context(|| format!("apply {} entry {pos}", entry.namespace))
}

/// Encode every state as of `position` (between batches, so nothing is half
/// applied).
fn take_snapshot(states: &States, position: Position) -> TabletSnapshot {
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

struct Batcher {
    states: Arc<States>,
    applied: Arc<AtomicU64>,
    log_id: LogId,
    /// Position of the newest snapshot taken (stored or not).
    snapshot_at: Position,
    /// Term of the entry at the applied position.
    head_term: u64,
    /// When the last snapshot was taken (or the tablet booted).
    snapshot_tried: tokio::time::Instant,
    /// A snapshot is being stored; the next one waits for it.
    putting: bool,
    /// Where a finished put reports `(position, result)`.
    stored_tx: mpsc::UnboundedSender<(Position, u64, Result<SnapshotRef>)>,
    /// What the log starts from now.
    base: Option<Base>,
    based: Arc<AtomicU64>,
    log: Arc<dyn LogStore>,
    snapshots: Arc<dyn SnapshotStore>,
    gen: u64,
    opts: MetaOptions,
    stop: watch::Sender<Option<Stop>>,
}

/// Sleep until `at`, or forever.
async fn until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Group commit: take what is queued, append it, apply it, answer. Ends when
/// every [`MetaStore`] handle is dropped.
async fn batcher(
    mut b: Batcher,
    mut rx: mpsc::UnboundedReceiver<Pending>,
    mut stored_rx: mpsc::UnboundedReceiver<(Position, u64, Result<SnapshotRef>)>,
) {
    let max = b.opts.batch_max_entries.max(1);
    loop {
        let first = tokio::select! {
            first = rx.recv() => first,
            Some((at, term, stored)) = stored_rx.recv() => {
                if let Some(stop) = b.rebase(at, term, stored).await {
                    return stop_for_good(b, stop, Vec::new(), rx).await;
                }
                continue;
            }
            () = until(b.snapshot_deadline()) => {
                b.maybe_snapshot();
                continue;
            }
        };
        let Some(first) = first else { return };
        let mut batch = vec![first];
        if !b.opts.batch_max_delay.is_zero() {
            let deadline = tokio::time::Instant::now() + b.opts.batch_max_delay;
            while batch.len() < max {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(p)) => batch.push(p),
                    _ => break,
                }
            }
        }
        while batch.len() < max {
            match rx.try_recv() {
                Ok(p) => batch.push(p),
                Err(_) => break,
            }
        }

        let entries: Vec<Entry> = batch.iter().filter_map(|p| p.entry.as_ref()).map(|c| c.entry(b.gen)).collect();
        let result = match b.log.append(b.gen, &entries).await {
            Ok(last) => {
                let first = last + 1 - entries.len() as u64;
                // A failure half way leaves the batch partly applied and its
                // callers unanswered: stop; the next boot rebuilds from the log.
                let mut applied = Vec::with_capacity(entries.len());
                let mut failure = None;
                for (i, entry) in entries.iter().enumerate() {
                    match apply_entry(&b.states, first + i as u64, entry) {
                        Ok(a) => applied.push(a),
                        Err(e) => {
                            failure = Some(Stop::Failed(format!("{e:#}")));
                            break;
                        }
                    }
                }
                match failure {
                    None => {
                        b.applied.store(last, Ordering::SeqCst);
                        if !entries.is_empty() {
                            b.head_term = b.gen;
                        }
                        Ok((first, applied))
                    }
                    Some(stop) => Err(stop),
                }
            }
            Err(LogError::Fenced { current }) => Err(Stop::Fenced { current }),
            Err(e) => Err(Stop::Failed(format!("append: {e}"))),
        };
        let (stop, unanswered) = match result {
            Ok((first, applied)) => {
                b.maybe_snapshot();
                let mut outcomes = applied.into_iter();
                let mut position = first;
                for p in batch {
                    let reply = match p.entry {
                        Some(_) => {
                            position += 1;
                            (position - 1, outcomes.next().expect("one outcome per entry"))
                        }
                        None => (position - 1, Box::new(()) as AnyOutcome),
                    };
                    let _ = p.reply.send(Ok(reply));
                }
                continue;
            }
            Err(stop) => (stop, batch),
        };
        return stop_for_good(b, stop, unanswered, rx).await;
    }
}

/// Stopped for good: fail `unanswered` and whatever is still queued or arrives.
async fn stop_for_good(b: Batcher, stop: Stop, unanswered: Vec<Pending>, mut rx: mpsc::UnboundedReceiver<Pending>) {
    b.stop.send_replace(Some(stop.clone()));
    for p in unanswered {
        let _ = p.reply.send(Err(stop.clone().into()));
    }
    while let Some(p) = rx.recv().await {
        let _ = p.reply.send(Err(stop.clone().into()));
    }
}

impl Batcher {
    /// When an idle tablet should wake up to snapshot by time: there are
    /// entries after the last snapshot, an interval is set, and no put runs.
    fn snapshot_deadline(&self) -> Option<tokio::time::Instant> {
        let interval = self.opts.snapshot_interval;
        let behind = self.applied.load(Ordering::SeqCst) > self.snapshot_at;
        (!interval.is_zero() && behind && !self.putting).then(|| self.snapshot_tried + interval)
    }

    /// Take a snapshot when due (between batches, so nothing is half applied)
    /// and store it in the background: a slow store (a chat) must not hold up
    /// proposals. The log is rebased once the put reports back.
    fn maybe_snapshot(&mut self) {
        if self.putting {
            return;
        }
        let position = self.applied.load(Ordering::SeqCst);
        let every = self.opts.snapshot_every;
        let by_count = every != 0 && position >= self.snapshot_at + every;
        let by_time = self.snapshot_deadline().is_some_and(|at| tokio::time::Instant::now() >= at);
        if !by_count && !by_time {
            return;
        }
        self.snapshot_tried = tokio::time::Instant::now();
        self.snapshot_at = position;
        self.putting = true;
        let stored = StoredSnapshot {
            log_id: self.log_id,
            at: position,
            data: take_snapshot(&self.states, position).encode(),
        };
        let (snapshots, tx, term) = (self.snapshots.clone(), self.stored_tx.clone(), self.head_term);
        tokio::spawn(async move {
            let _ = tx.send((position, term, snapshots.put(&stored).await));
        });
    }

    /// A put finished: start the log from it and drop the snapshot it started
    /// from before. Only a fence stops the tablet: on any failure the log
    /// keeps its old base and the next snapshot tries again.
    async fn rebase(&mut self, at: Position, term: u64, stored: Result<SnapshotRef>) -> Option<Stop> {
        self.putting = false;
        let snapshot = match stored {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(position = at, error = %format!("{e:#}"), "storing a snapshot failed; the log is kept");
                return None;
            }
        };
        let base = Base { at, term, snapshot };
        match self.log.rebase(self.gen, base.clone()).await {
            Ok(()) => {
                self.based.store(at, Ordering::SeqCst);
                let new = base.snapshot.clone();
                if let Some(old) = self.base.replace(base) {
                    if old.snapshot != new {
                        self.forget(old.snapshot);
                    }
                }
                None
            }
            Err(LogError::Fenced { current }) => Some(Stop::Fenced { current }),
            Err(e) => {
                tracing::warn!(position = at, error = %e, "rebasing the log failed; the log is kept");
                self.forget(base.snapshot);
                None
            }
        }
    }

    /// Remove a snapshot nothing starts from (best effort, in the background).
    fn forget(&self, r: SnapshotRef) {
        let snapshots = self.snapshots.clone();
        tokio::spawn(async move {
            if let Err(e) = snapshots.remove(&r).await {
                tracing::debug!(snapshot = %r.0, error = %format!("{e:#}"), "removing an old snapshot failed");
            }
        });
    }
}

#[cfg(test)]
mod tests;
