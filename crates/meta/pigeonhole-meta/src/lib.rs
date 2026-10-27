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
//! returns [`Applied::Reject`] and leaves the state untouched, but the entry
//! still consumes a position — every replay reaches the same verdict.
//!
//! Boot ([`MetaStore::open`]): claim the next generation, replay the whole log
//! into empty states, append the zero entry, go active. Once a newer
//! generation claims the log the tablet stops ([`Stop::Fenced`]) and every
//! later `propose` fails.

use anyhow::{bail, Context, Result};
use futures::TryStreamExt;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

pub mod log;

pub use log::{
    Discovered, Entry, EntryStream, Head, LocalLogStore, LogError, LogId, LogResult, LogStore,
    Position, Snapshot,
};

/// Outcome of applying one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// The command took effect; `payload` is the layer's encoded response.
    Commit(Vec<u8>),
    /// Deterministic failure, state untouched; `payload` is the layer's
    /// encoded error.
    Reject(Vec<u8>),
}

/// Typed in-memory state of one namespace. The log (and later its snapshots)
/// is the only durable copy: every boot rebuilds the state from it.
pub trait StateMachine: Default + Send + Sync + 'static {
    /// Stable name used to route commands (`chunk`, `s3`, `cas`).
    const NAME: &'static str;

    /// Apply one encoded command; must be deterministic. Validate before
    /// mutating: an [`Applied::Reject`] must leave the state untouched. `Err`
    /// is a corrupt command, never a state-dependent rejection.
    fn apply(&mut self, command: &[u8]) -> Result<Applied>;

    /// Logical dump of the state (the layer's own encoding) and its inverse.
    /// Equal states must give equal bytes.
    fn snapshot(&self) -> Vec<u8>;
    fn restore(data: &[u8]) -> Result<Self>;
}

/// Object-safe face of a [`StateMachine`] behind its lock.
trait AnyState: Send + Sync {
    fn apply(&self, command: &[u8]) -> Result<Applied>;
    fn as_any(&self) -> &dyn Any;
}

struct StateCell<S>(RwLock<S>);

impl<S: StateMachine> AnyState for StateCell<S> {
    fn apply(&self, command: &[u8]) -> Result<Applied> {
        self.0.write().unwrap().apply(command)
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

/// Result of [`MetaStore::propose`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Position of the entry in the log.
    pub position: Position,
    pub applied: Applied,
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
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            batch_max_delay: Duration::ZERO,
            batch_max_entries: 1024,
        }
    }
}

type States = HashMap<&'static str, Arc<dyn AnyState>>;

struct Pending {
    entry: Entry,
    reply: oneshot::Sender<Result<Response>>,
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
        if head.position > 0 {
            replay(&states, log.as_ref(), head).await?;
        }

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

    /// Append `command` in `namespace` to the log and apply it.
    pub async fn propose(&self, namespace: &str, command: &[u8]) -> Result<Response> {
        if !self.has_namespace(namespace) {
            bail!("unknown namespace {namespace:?}");
        }
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop.into());
        }
        let (reply, rx) = oneshot::channel();
        let entry = Entry {
            namespace: namespace.to_owned(),
            command: command.to_vec(),
        };
        if self.queue.send(Pending { entry, reply }).is_err() {
            bail!("tablet batcher is gone");
        }
        rx.await.context("tablet batcher dropped the proposal")?
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

/// Apply the whole log `(0, head]` to empty states.
async fn replay(states: &States, log: &dyn LogStore, head: Head) -> Result<()> {
    if let Some(d) = log.discover().await.context("discover log")? {
        if let Some((at, _)) = d.snapshot {
            bail!("log has a snapshot at {at}; snapshot restore is not implemented yet");
        }
    }
    let mut entries = log.read(0, head);
    let mut next = 1;
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

fn apply_entry(states: &States, pos: Position, entry: &Entry) -> Result<Applied> {
    let state = states
        .get(entry.namespace.as_str())
        .with_context(|| format!("entry {pos}: unknown namespace {:?}", entry.namespace))?;
    state
        .apply(&entry.command)
        .with_context(|| format!("apply {} entry {pos}", entry.namespace))
}

struct Batcher {
    states: Arc<States>,
    applied: Arc<AtomicU64>,
    log: Arc<dyn LogStore>,
    gen: u64,
    opts: MetaOptions,
    stop: watch::Sender<Option<Stop>>,
}

/// Group commit: take what is queued, append it, apply it, answer. Ends when
/// every [`MetaStore`] handle is dropped.
async fn batcher(b: Batcher, mut rx: mpsc::UnboundedReceiver<Pending>) {
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

        let entries: Vec<Entry> = batch.iter().map(|p| p.entry.clone()).collect();
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
        match result {
            Ok((first, applied)) => {
                for (i, (p, applied)) in batch.into_iter().zip(applied).enumerate() {
                    let position = first + i as u64;
                    let _ = p.reply.send(Ok(Response { position, applied }));
                }
            }
            Err(stop) => {
                b.stop.send_replace(Some(stop.clone()));
                for p in batch {
                    let _ = p.reply.send(Err(stop.clone().into()));
                }
                // Stopped for good: fail whatever is still queued or arrives.
                while let Some(p) = rx.recv().await {
                    let _ = p.reply.send(Err(stop.clone().into()));
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests;
