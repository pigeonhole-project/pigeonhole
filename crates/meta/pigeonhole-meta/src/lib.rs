//! Metadata state machine (see `docs/meta-backends.md`).
//!
//! Every metadata change is a **command** owned by one layer (namespace). A
//! command is applied by that layer's [`Namespace::apply`] inside one SQLite
//! transaction; [`MetaStore::propose`] is the only write path. Reads go straight
//! to [`MetaStore::pool`].
//!
//! Commands must be deterministic: ids, timestamps and locators are chosen by
//! the proposer and carried in the command, never generated in `apply`. That is
//! what lets the same command log rebuild the same state elsewhere (raft).
//!
//! A command can fail on state (e.g. retain of a reclaimed chunk). `apply` then
//! returns [`Applied::Reject`]: its writes are rolled back, but the entry still
//! consumes an index — every replica reaches the same verdict.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{SqliteConnection, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub mod log;

pub use log::{
    Discovered, Entry, EntryStream, Head, LogError, LogResult, LogStore, Position, Snapshot,
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
    /// Index of the entry in the command sequence.
    pub index: u64,
    pub applied: Applied,
}

/// SQLite-backed state machine shared by every registered namespace.
pub struct MetaStore {
    pool: SqlitePool,
    namespaces: RwLock<HashMap<&'static str, Arc<dyn Namespace>>>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta_applied (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
    last_index INTEGER NOT NULL
)
"#;

impl MetaStore {
    /// Open (or create) the database at `url` (`sqlite:path`).
    pub async fn open(url: &str) -> Result<Arc<Self>> {
        let opts = SqliteConnectOptions::from_str(url)
            .with_context(|| format!("parse sqlite url {url}"))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .with_context(|| format!("connect {url}"))?;
        sqlx::query(SCHEMA).execute(&pool).await?;
        Ok(Arc::new(Self {
            pool,
            namespaces: RwLock::new(HashMap::new()),
        }))
    }

    /// Read-only access for layer queries. Never write through this pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Register a layer and create its schema.
    pub async fn register(&self, ns: Arc<dyn Namespace>) -> Result<()> {
        let name = ns.name();
        if self.namespaces.read().unwrap().contains_key(name) {
            bail!("namespace {name:?} already registered");
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        ns.init(&mut tx).await.with_context(|| format!("init namespace {name}"))?;
        tx.commit().await?;
        self.namespaces.write().unwrap().insert(name, ns);
        Ok(())
    }

    /// Apply `command` in `namespace` as the next entry.
    pub async fn propose(&self, namespace: &str, command: &[u8]) -> Result<Response> {
        let ns = self
            .namespaces
            .read()
            .unwrap()
            .get(namespace)
            .cloned()
            .with_context(|| format!("unknown namespace {namespace:?}"))?;

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let (last,): (i64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT last_index FROM meta_applied WHERE singleton = 0), 0)",
        )
        .fetch_one(&mut *tx)
        .await?;
        let index = last + 1;

        sqlx::query("SAVEPOINT command").execute(&mut *tx).await?;
        let applied = ns
            .apply(&mut tx, command)
            .await
            .with_context(|| format!("apply {namespace} entry {index}"))?;
        let undo = match applied {
            Applied::Commit(_) => "RELEASE command",
            Applied::Reject(_) => "ROLLBACK TO command",
        };
        sqlx::query(undo).execute(&mut *tx).await?;
        if matches!(applied, Applied::Reject(_)) {
            sqlx::query("RELEASE command").execute(&mut *tx).await?;
        }

        sqlx::query(
            r#"
            INSERT INTO meta_applied (singleton, last_index) VALUES (0, ?)
            ON CONFLICT (singleton) DO UPDATE SET last_index = excluded.last_index
            "#,
        )
        .bind(index)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Response {
            index: index as u64,
            applied,
        })
    }

    /// Index of the last applied entry (0 before the first).
    pub async fn applied_index(&self) -> Result<u64> {
        let (last,): (i64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT last_index FROM meta_applied WHERE singleton = 0), 0)",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(last as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Toy namespace: command = signed delta for a counter; going negative is rejected.
    struct Counter;

    #[async_trait]
    impl Namespace for Counter {
        fn name(&self) -> &'static str {
            "counter"
        }

        async fn init(&self, conn: &mut SqliteConnection) -> Result<()> {
            sqlx::query("CREATE TABLE IF NOT EXISTS counter (v INTEGER NOT NULL)")
                .execute(&mut *conn)
                .await?;
            sqlx::query("INSERT INTO counter SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counter)")
                .execute(&mut *conn)
                .await?;
            Ok(())
        }

        async fn apply(&self, conn: &mut SqliteConnection, command: &[u8]) -> Result<Applied> {
            let delta = i64::from_le_bytes(command.try_into().context("8-byte delta")?);
            sqlx::query("UPDATE counter SET v = v + ?")
                .bind(delta)
                .execute(&mut *conn)
                .await?;
            let (v,): (i64,) = sqlx::query_as("SELECT v FROM counter")
                .fetch_one(&mut *conn)
                .await?;
            if v < 0 {
                return Ok(Applied::Reject(b"negative".to_vec()));
            }
            Ok(Applied::Commit(v.to_le_bytes().to_vec()))
        }
    }

    async fn open() -> (Arc<MetaStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}", dir.path().join("meta.db").display());
        let store = MetaStore::open(&url).await.unwrap();
        store.register(Arc::new(Counter)).await.unwrap();
        (store, dir)
    }

    async fn value(store: &MetaStore) -> i64 {
        sqlx::query_as::<_, (i64,)>("SELECT v FROM counter")
            .fetch_one(store.pool())
            .await
            .unwrap()
            .0
    }

    #[tokio::test]
    async fn commit_and_reject_both_consume_an_index() {
        let (store, _dir) = open().await;
        let r = store.propose("counter", &5i64.to_le_bytes()).await.unwrap();
        assert_eq!(r.index, 1);
        assert_eq!(r.applied, Applied::Commit(5i64.to_le_bytes().to_vec()));

        let r = store.propose("counter", &(-7i64).to_le_bytes()).await.unwrap();
        assert_eq!(r.index, 2);
        assert_eq!(r.applied, Applied::Reject(b"negative".to_vec()));
        assert_eq!(value(&store).await, 5, "rejected command must not leave writes");
        assert_eq!(store.applied_index().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn unknown_namespace_and_double_register_fail() {
        let (store, _dir) = open().await;
        assert!(store.propose("nope", &[]).await.is_err());
        assert!(store.register(Arc::new(Counter)).await.is_err());
        assert_eq!(store.applied_index().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn concurrent_proposals_get_distinct_indexes() {
        let (store, _dir) = open().await;
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let s = store.clone();
                tokio::spawn(async move { s.propose("counter", &1i64.to_le_bytes()).await })
            })
            .collect();
        let mut indexes = Vec::new();
        for t in tasks {
            indexes.push(t.await.unwrap().unwrap().index);
        }
        indexes.sort_unstable();
        assert_eq!(indexes, (1..=32).collect::<Vec<_>>());
        assert_eq!(value(&store).await, 32);
    }
}
