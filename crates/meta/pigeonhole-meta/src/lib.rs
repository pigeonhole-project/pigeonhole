//! Metadata tablet (see `docs/meta-backends.md`).
//!
//! Every metadata change is a **command** owned by one layer (namespace).
//! [`MetaStore::propose`] is the only write path: it queues the command, a
//! batcher appends a batch of queued commands to the [`LogStore`] under the
//! tablet's generation, applies them in order to SQLite and only then answers
//! the callers. The log is the source of truth; the SQLite file is a cache that
//! records the last applied position with the state. Reads go straight to
//! [`MetaStore::pool`].
//!
//! Commands must be deterministic: ids, timestamps and locators are chosen by
//! the proposer and carried in the command, never generated in `apply`. That is
//! what lets the log rebuild the same state elsewhere.
//!
//! A command can fail on state (e.g. retain of a reclaimed chunk). `apply` then
//! returns [`Applied::Reject`]: its writes are rolled back, but the entry still
//! consumes a position — every replay reaches the same verdict.
//!
//! Boot ([`MetaStore::open`]): claim the next generation, replay the log
//! entries the SQLite file has not applied yet, append the zero entry, go
//! active. Once a newer generation claims the log the tablet stops
//! ([`Stop::Fenced`]) and every later `propose` fails.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{SqliteConnection, SqlitePool};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
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
    /// Keep the command's writes; `payload` is the layer's encoded response.
    Commit(Vec<u8>),
    /// Deterministic failure: roll back the command's writes; `payload` is the
    /// layer's encoded error.
    Reject(Vec<u8>),
}

/// One layer's slice of the state machine.
#[async_trait]
pub trait Namespace: Send + Sync + 'static {
    /// Stable name used to route commands (`chunk`, `s3`, `cas`).
    fn name(&self) -> &'static str;

    /// Create the layer's tables (idempotent).
    async fn init(&self, conn: &mut SqliteConnection) -> Result<()>;

    /// Apply one encoded command. `Err` is an infrastructure failure (I/O,
    /// corrupt command), never a state-dependent rejection.
    async fn apply(&self, conn: &mut SqliteConnection, command: &[u8]) -> Result<Applied>;
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
    /// The log or the SQLite file failed; the outcome of the last batch is
    /// unknown until the next boot replays the log.
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

type Namespaces = HashMap<&'static str, Arc<dyn Namespace>>;

struct Pending {
    entry: Entry,
    reply: oneshot::Sender<Result<Response>>,
}

/// The metadata tablet shared by every layer.
pub struct MetaStore {
    pool: SqlitePool,
    namespaces: Arc<Namespaces>,
    gen: u64,
    queue: mpsc::UnboundedSender<Pending>,
    stop: watch::Receiver<Option<Stop>>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta_tablet (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    gen INTEGER NOT NULL,
    log_id TEXT,
    last_applied_position INTEGER NOT NULL
)
"#;

/// Entries applied per SQLite transaction during replay.
const REPLAY_CHUNK: usize = 1024;

impl MetaStore {
    /// Boot the tablet: open (or create) the SQLite file at `url`
    /// (`sqlite:path`), create every namespace's tables, claim the next
    /// generation of `log`, replay what the file has not applied, append the
    /// zero entry. Namespaces are fixed for the tablet's lifetime: replay must
    /// be able to apply every entry in the log.
    pub async fn open(
        url: &str,
        log: Arc<dyn LogStore>,
        namespaces: Vec<Arc<dyn Namespace>>,
        opts: MetaOptions,
    ) -> Result<Arc<Self>> {
        let sqlite = SqliteConnectOptions::from_str(url)
            .with_context(|| format!("parse sqlite url {url}"))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            // The log is durable; the file is a cache. With WAL, NORMAL skips
            // the fsync per commit: power loss may drop the newest
            // transactions, but `last_applied_position` drops with them and
            // boot replays them from the log. A process crash loses nothing.
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(sqlite)
            .await
            .with_context(|| format!("connect {url}"))?;

        let mut by_name = Namespaces::new();
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(SCHEMA).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO meta_tablet (singleton, gen, last_applied_position) VALUES (0, 0, 0) \
             ON CONFLICT (singleton) DO NOTHING",
        )
        .execute(&mut *tx)
        .await?;
        for ns in namespaces {
            let name = ns.name();
            ns.init(&mut tx).await.with_context(|| format!("init namespace {name}"))?;
            if by_name.insert(name, ns).is_some() {
                bail!("namespace {name:?} given twice");
            }
        }
        tx.commit().await?;
        let namespaces = Arc::new(by_name);

        let (known_gen, _, _) = tablet_row(&pool).await?;
        let (gen, head) = claim_next(log.as_ref(), known_gen).await?;
        sqlx::query("UPDATE meta_tablet SET gen = ? WHERE singleton = 0")
            .bind(gen as i64)
            .execute(&pool)
            .await?;

        let (_, log_id, mut applied) = tablet_row(&pool).await?;
        if log_id.is_some_and(|id| id != head.log_id) || (log_id.is_none() && applied > 0) {
            // Applied from another history: positions mean different entries.
            discard_state(&pool, &namespaces).await?;
            applied = 0;
        }
        sqlx::query("UPDATE meta_tablet SET log_id = ? WHERE singleton = 0")
            .bind(head.log_id.to_string())
            .execute(&pool)
            .await?;
        if applied > head.position {
            bail!(LogError::Lost(format!(
                "SQLite state is at {applied}, the log ends at {}",
                head.position
            )));
        }
        if applied < head.position {
            replay(&pool, &namespaces, log.as_ref(), applied, head).await?;
        }

        let zero = log.append(gen, &[]).await.context("append zero entry")?;
        if zero != head.position {
            bail!("zero entry of generation {gen} at {zero}, expected {}", head.position);
        }

        let (queue, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop) = watch::channel(None);
        tokio::spawn(batcher(
            Batcher {
                pool: pool.clone(),
                namespaces: namespaces.clone(),
                log,
                gen,
                opts,
                stop: stop_tx,
            },
            rx,
        ));
        Ok(Arc::new(Self { pool, namespaces, gen, queue, stop }))
    }

    /// Boot over a [`LocalLogStore`] in `<db path>.log` next to the SQLite file.
    pub async fn open_local(url: &str, namespaces: Vec<Arc<dyn Namespace>>) -> Result<Arc<Self>> {
        let log = LocalLogStore::open(local_log_dir(url)?)?;
        Self::open(url, Arc::new(log), namespaces, MetaOptions::default()).await
    }

    /// Read-only access for layer queries. Never write command-owned tables
    /// through this pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn has_namespace(&self, name: &str) -> bool {
        self.namespaces.contains_key(name)
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
    pub async fn applied_position(&self) -> Result<Position> {
        Ok(tablet_row(&self.pool).await?.2)
    }
}

/// Every table except the tablet's own bookkeeping, rows rendered with SQL
/// `quote()` and sorted: two files with equal dumps hold the same state.
pub async fn dump_tables(pool: &SqlitePool) -> Result<Vec<(String, Vec<String>)>> {
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' AND name != 'meta_tablet' ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    let mut out = Vec::new();
    for (table,) in tables {
        let cols: Vec<(String,)> = sqlx::query_as("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(pool)
            .await?;
        let row = cols
            .iter()
            .map(|(c,)| format!("quote(\"{c}\")"))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let mut rows: Vec<String> = sqlx::query_as::<_, (String,)>(&format!("SELECT {row} FROM \"{table}\""))
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(r,)| r)
            .collect();
        rows.sort_unstable();
        out.push((table, rows));
    }
    Ok(out)
}

/// `sqlite:path[?query]` → `path.log`.
fn local_log_dir(url: &str) -> Result<PathBuf> {
    let path = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))
        .with_context(|| format!("not a sqlite url: {url}"))?;
    let path = path.split('?').next().unwrap_or(path);
    if path.is_empty() || path.starts_with(':') {
        bail!("{url}: the local log needs an on-disk database");
    }
    Ok(PathBuf::from(format!("{path}.log")))
}

async fn tablet_row(pool: &SqlitePool) -> Result<(u64, Option<LogId>, Position)> {
    let (gen, log_id, pos): (i64, Option<String>, i64) = sqlx::query_as(
        "SELECT gen, log_id, last_applied_position FROM meta_tablet WHERE singleton = 0",
    )
    .fetch_one(pool)
    .await?;
    let log_id = log_id
        .map(|s| s.parse::<LogId>().with_context(|| format!("meta_tablet.log_id {s:?}")))
        .transpose()?;
    Ok((gen as u64, log_id, pos as Position))
}

/// Empty every table but the tablet's own row and re-run each namespace's
/// `init`, so the log can be replayed from the start.
async fn discard_state(pool: &SqlitePool, namespaces: &Namespaces) -> Result<()> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("PRAGMA defer_foreign_keys = ON").execute(&mut *tx).await?;
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' AND name != 'meta_tablet'",
    )
    .fetch_all(&mut *tx)
    .await?;
    for (table,) in tables {
        sqlx::query(&format!("DELETE FROM \"{table}\"")).execute(&mut *tx).await?;
    }
    for ns in namespaces.values() {
        ns.init(&mut tx).await.with_context(|| format!("init namespace {}", ns.name()))?;
    }
    sqlx::query("UPDATE meta_tablet SET log_id = NULL, last_applied_position = 0 WHERE singleton = 0")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Claim `1 + max(known generations)`; a racing or forgotten newer claim
/// moves the floor up.
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

/// Apply log entries `(applied, head]` that the SQLite file has not seen.
async fn replay(
    pool: &SqlitePool,
    namespaces: &Namespaces,
    log: &dyn LogStore,
    applied: Position,
    head: Head,
) -> Result<()> {
    if let Some(d) = log.discover().await.context("discover log")? {
        if let Some((at, _)) = d.snapshot {
            if at > applied {
                bail!("log snapshot at {at} is ahead of SQLite ({applied}); snapshot restore is not implemented yet");
            }
        }
    }
    let mut entries = log.read(applied, head);
    let mut chunk = Vec::with_capacity(REPLAY_CHUNK);
    let mut first = applied + 1;
    loop {
        let next = entries.try_next().await.context("read log")?;
        let done = next.is_none();
        if let Some((pos, entry)) = next {
            if pos != first + chunk.len() as u64 {
                bail!(LogError::Lost(format!("replay expected {}, got {pos}", first + chunk.len() as u64)));
            }
            chunk.push(entry);
        }
        if chunk.len() == REPLAY_CHUNK || (done && !chunk.is_empty()) {
            apply_batch(pool, namespaces, first, &chunk).await?;
            first += chunk.len() as u64;
            chunk.clear();
        }
        if done {
            break;
        }
    }
    if first - 1 != head.position {
        bail!(LogError::Lost(format!("replay stopped at {}, head is {}", first - 1, head.position)));
    }
    Ok(())
}

/// Apply entries at positions `first..` in one transaction, advancing
/// `last_applied_position` with them (so a crash re-applies all or nothing).
async fn apply_batch(
    pool: &SqlitePool,
    namespaces: &Namespaces,
    first: Position,
    entries: &[Entry],
) -> Result<Vec<Applied>> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let (last,): (i64,) =
        sqlx::query_as("SELECT last_applied_position FROM meta_tablet WHERE singleton = 0")
            .fetch_one(&mut *tx)
            .await?;
    if last as u64 + 1 != first {
        bail!("apply at {first}, but SQLite is at {last}");
    }
    let mut out = Vec::with_capacity(entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let pos = first + i as u64;
        let ns = namespaces
            .get(entry.namespace.as_str())
            .with_context(|| format!("entry {pos}: unknown namespace {:?}", entry.namespace))?;
        sqlx::query("SAVEPOINT command").execute(&mut *tx).await?;
        let applied = ns
            .apply(&mut tx, &entry.command)
            .await
            .with_context(|| format!("apply {} entry {pos}", entry.namespace))?;
        if matches!(applied, Applied::Reject(_)) {
            sqlx::query("ROLLBACK TO command").execute(&mut *tx).await?;
        }
        sqlx::query("RELEASE command").execute(&mut *tx).await?;
        out.push(applied);
    }
    sqlx::query("UPDATE meta_tablet SET last_applied_position = ? WHERE singleton = 0")
        .bind((first - 1 + entries.len() as u64) as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(out)
}

struct Batcher {
    pool: SqlitePool,
    namespaces: Arc<Namespaces>,
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
                apply_batch(&b.pool, &b.namespaces, first, &entries)
                    .await
                    .map(|applied| (first, applied))
                    .map_err(|e| Stop::Failed(format!("apply batch at {first}: {e:#}")))
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
