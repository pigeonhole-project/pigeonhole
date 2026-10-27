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
//! Every [`MetaOptions::snapshot_every`] entries the batcher encodes all
//! states into one [`TabletSnapshot`] and hands it to
//! [`LogStore::install_snapshot`], which drops the log behind it.
//!
//! Boot ([`MetaStore::open`]): claim the next generation, load the newest
//! snapshot and replay the log after it, append the zero entry, go active. Once a newer
//! generation claims the log the tablet stops ([`Stop::Fenced`]) and every
//! later `propose` fails.

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

pub mod export;
pub mod log;

pub use log::{
    Batch, Discovered, Entry, EntryStream, Head, LocalLogStore, LogError, LogId, LogResult,
    LogStore, Position, Snapshot, LOG_FORMAT,
};

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

/// A log entry carrying `command` for `S`.
pub fn entry<S: StateMachine>(command: &S::Command) -> Entry {
    Entry {
        namespace: S::NAME.to_string(),
        version: S::VERSION,
        command: postcard::to_stdvec(command).expect("command always encodes"),
    }
}

/// Version of the [`TabletSnapshot`] envelope.
pub const SNAPSHOT_FORMAT: u32 = 1;

/// Every namespace's state as of `position`, in one LogStore snapshot.
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
    fn apply(&self, entry: &Entry) -> Result<AnyOutcome>;
    /// `(VERSION, encode_state)`.
    fn snapshot(&self) -> (u32, Vec<u8>);
    fn restore(&self, version: u32, data: &[u8]) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

struct StateCell<S>(RwLock<S>);

impl<S: StateMachine> AnyState for StateCell<S> {
    fn apply(&self, entry: &Entry) -> Result<AnyOutcome> {
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
#[derive(Debug, Clone)]
pub struct MetaOptions {
    /// Extra time to wait for more commands before appending a batch. Zero =
    /// append as soon as the previous append finished (batches still form from
    /// whatever queued meanwhile).
    pub batch_max_delay: Duration,
    pub batch_max_entries: usize,
    /// Take a snapshot (and collect the log behind it) after this many
    /// entries since the last one; 0 = never.
    pub snapshot_every: u64,
    /// Also copy every snapshot into this directory (see [`export`]).
    pub snapshot_export: Option<PathBuf>,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            batch_max_delay: Duration::ZERO,
            batch_max_entries: 1024,
            snapshot_every: 10_000,
            snapshot_export: None,
        }
    }
}

type States = HashMap<&'static str, Arc<dyn AnyState>>;

struct Pending {
    /// `None` = a barrier ([`MetaStore::confirm_active`]): no entry, answered
    /// once its batch is appended.
    entry: Option<Entry>,
    reply: oneshot::Sender<Result<(Position, AnyOutcome)>>,
}

/// The metadata tablet shared by every layer.
pub struct MetaStore {
    states: Arc<States>,
    gen: u64,
    /// Position of the last applied entry.
    applied: Arc<AtomicU64>,
    queue: mpsc::UnboundedSender<Pending>,
    stop: watch::Receiver<Option<Stop>>,
}

impl MetaStore {
    /// Boot the tablet: claim the next generation of `log`, replay the whole
    /// log into the namespaces' empty states, append the zero entry.
    pub async fn open(
        log: Arc<dyn LogStore>,
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

        let known = log.discover().await.context("discover log")?.map_or(0, |d| d.gen);
        let (gen, head) = claim_next(log.as_ref(), known).await?;
        let snapshot_at = recover(&states, log.as_ref(), head).await?;

        let zero = log.append(gen, &[]).await.context("append zero entry")?;
        if zero != head.position {
            bail!("zero entry of generation {gen} at {zero}, expected {}", head.position);
        }

        let applied = Arc::new(AtomicU64::new(head.position));
        let (queue, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop) = watch::channel(None);
        tokio::spawn(batcher(
            Batcher {
                states: states.clone(),
                applied: applied.clone(),
                snapshot_at,
                log,
                gen,
                opts,
                stop: stop_tx,
            },
            rx,
        ));
        Ok(Arc::new(Self { states, gen, applied, queue, stop }))
    }

    /// Boot over a [`LocalLogStore`] in `dir`.
    pub async fn open_local(dir: impl AsRef<Path>, namespaces: Namespaces) -> Result<Arc<Self>> {
        let log = LocalLogStore::open(dir.as_ref())?;
        Self::open(Arc::new(log), namespaces, MetaOptions::default()).await
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
        if self.queue.send(Pending { entry: Some(entry::<S>(&command)), reply }).is_err() {
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
async fn recover(states: &States, log: &dyn LogStore, head: Head) -> Result<Position> {
    let snapshot = log.discover().await.context("discover log")?.and_then(|d| d.snapshot);
    let from = match snapshot {
        Some((at, snapshot)) => {
            let snap = TabletSnapshot::decode(&snapshot.data)?;
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
    Ok(from)
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
    /// Position of the newest installed snapshot.
    snapshot_at: Position,
    log: Arc<dyn LogStore>,
    gen: u64,
    opts: MetaOptions,
    stop: watch::Sender<Option<Stop>>,
}

/// Group commit: take what is queued, append it, apply it, answer. Ends when
/// every [`MetaStore`] handle is dropped.
async fn batcher(mut b: Batcher, mut rx: mpsc::UnboundedReceiver<Pending>) {
    let max = b.opts.batch_max_entries.max(1);
    while let Some(first) = rx.recv().await {
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

        let entries: Vec<Entry> = batch.iter().filter_map(|p| p.entry.clone()).collect();
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
                // Snapshot before answering: once `propose` returns, a snapshot
                // (and export) due at this batch is in place. The batch is
                // committed either way, so a fence here still answers Ok.
                let stop = b.maybe_snapshot().await;
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
                match stop {
                    None => continue,
                    Some(stop) => (stop, Vec::new()),
                }
            }
            Err(stop) => (stop, batch),
        };
        b.stop.send_replace(Some(stop.clone()));
        for p in unanswered {
            let _ = p.reply.send(Err(stop.clone().into()));
        }
        // Stopped for good: fail whatever is still queued or arrives.
        while let Some(p) = rx.recv().await {
            let _ = p.reply.send(Err(stop.clone().into()));
        }
        return;
    }
}

impl Batcher {
    /// Snapshot when due. Only a fence stops the tablet: a failed snapshot
    /// leaves the log intact, so it is retried after the next batch.
    async fn maybe_snapshot(&mut self) -> Option<Stop> {
        let position = self.applied.load(Ordering::SeqCst);
        let every = self.opts.snapshot_every;
        if every == 0 || position < self.snapshot_at + every {
            return None;
        }
        let data = take_snapshot(&self.states, position).encode();
        let copy = self.opts.snapshot_export.clone().map(|dir| (dir, data.clone()));
        match self.log.install_snapshot(self.gen, position, Snapshot { data }).await {
            Ok(()) => {
                self.snapshot_at = position;
                if let Some((dir, data)) = copy {
                    let written =
                        tokio::task::spawn_blocking(move || export::write_export(&dir, position, &data)).await;
                    match written {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => tracing::warn!(position, error = %e, "snapshot export failed"),
                        Err(e) => tracing::warn!(position, error = %e, "snapshot export task failed"),
                    }
                }
                None
            }
            Err(LogError::Fenced { current }) => Some(Stop::Fenced { current }),
            Err(e) => {
                tracing::warn!(position, error = %e, "snapshot failed; the log is kept");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests;
