//! Chunk-store metadata: queries plus the proposer side of the `chunk` namespace.
//!
//! Chunk state (instances, chunks, blocks, parts, roots) changes only through
//! commands applied by [`crate::commands::ChunkNamespace`]. Node-local
//! operational tables (put watermarks, sweep cursor, repair queue) are written
//! directly: they are progress of this node, not replicated state.

use crate::commands::{pb, ChunkNamespace, NAMESPACE};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use pigeonhole_blob::{InstanceInfo, PartLayout, ReplicaLayout, BlobLocator};
use pigeonhole_meta::{Applied, MetaStore};
use prost::Message;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Internal integer chunk id (row in `chunks`).
pub type ChunkId = i64;

/// Chunk lifecycle for reclaim races with `retain`.
pub const CHUNK_STATE_LIVE: &str = "live";
pub const CHUNK_STATE_RECLAIMING: &str = "reclaiming";

/// `retain` failed because the chunk is missing, `refs = 0`, or already reclaiming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkGone {
    pub chunk_id: ChunkId,
}

impl std::fmt::Display for ChunkGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chunk {} gone", self.chunk_id)
    }
}

impl std::error::Error for ChunkGone {}

/// True when `err` (or a cause) is [`ChunkGone`].
pub fn is_chunk_gone(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ChunkGone>().is_some()
        || err.chain().any(|c| c.downcast_ref::<ChunkGone>().is_some())
}

/// Logical byte range within a chunk (`offset`/`len` are logical bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extent {
    pub chunk: ChunkId,
    pub offset: i64,
    pub len: i64,
}

/// Chunk-store view of the metadata state machine.
#[derive(Clone)]
pub struct BlobDb {
    pool: SqlitePool,
    meta: Arc<MetaStore>,
    /// Next chunk id to hand out; ids are chosen by the proposer (see commands.rs).
    next_chunk_id: Arc<AtomicI64>,
}

impl std::fmt::Debug for BlobDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobDb").finish_non_exhaustive()
    }
}

/// One stored block row (offsets inside a part are derived from `stored_len`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBlock {
    pub block_no: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    pub stored_len: i64,
    pub codec: String,
}

/// Current schema. No upgrades: an incompatible file is recreated by hand.
pub(crate) const SCHEMA: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS instances (
        id TEXT PRIMARY KEY NOT NULL,
        kind TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        location TEXT NOT NULL,
        state TEXT NOT NULL
    )
    "#,
    r#"
    CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_location_active
    ON instances(location)
    WHERE state = 'read-write'
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY NOT NULL,
        logical_size INTEGER NOT NULL,
        crc32 INTEGER NOT NULL,
        refs INTEGER NOT NULL DEFAULT 0,
        block_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'live'
    )
    "#,
    // High-water mark for chunk ids. `chunks.id` is a plain INTEGER PRIMARY KEY,
    // which SQLite re-issues once the highest row is deleted; ids must never be
    // reused (block cache and extents are keyed by chunk id).
    r#"
    CREATE TABLE IF NOT EXISTS chunk_id_seq (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
        last INTEGER NOT NULL
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS chunk_blocks (
        chunk_id INTEGER NOT NULL REFERENCES chunks(id),
        block_no INTEGER NOT NULL,
        logical_off INTEGER NOT NULL,
        logical_len INTEGER NOT NULL,
        stored_len INTEGER NOT NULL,
        codec TEXT NOT NULL,
        PRIMARY KEY (chunk_id, block_no)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS chunk_parts (
        chunk_id INTEGER NOT NULL REFERENCES chunks(id),
        instance_id TEXT NOT NULL REFERENCES instances(id),
        part_no INTEGER NOT NULL,
        first_block INTEGER NOT NULL,
        block_count INTEGER NOT NULL,
        sort_key BLOB NOT NULL,
        locator BLOB NOT NULL,
        PRIMARY KEY (chunk_id, instance_id, part_no),
        UNIQUE (instance_id, sort_key)
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS roots (
        name TEXT PRIMARY KEY NOT NULL,
        extents_json TEXT NOT NULL DEFAULT '[]'
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS put_watermarks (
        instance_id TEXT NOT NULL,
        at TEXT NOT NULL,
        max_key BLOB NOT NULL
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS sweep_cursor (
        instance_id TEXT PRIMARY KEY NOT NULL,
        after BLOB
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS repair_queue (
        chunk_id INTEGER NOT NULL REFERENCES chunks(id),
        instance_id TEXT NOT NULL,
        enqueued_at TEXT NOT NULL,
        PRIMARY KEY (chunk_id, instance_id)
    )
    "#,
];

impl BlobDb {
    /// Open a standalone metadata store at `database_url` with only this namespace.
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::open(MetaStore::open(database_url).await?).await
    }

    /// Register the `chunk` namespace on `meta`.
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        meta.register(Arc::new(ChunkNamespace)).await?;
        let db = Self {
            pool: meta.pool().clone(),
            meta,
            next_chunk_id: Arc::new(AtomicI64::new(0)),
        };
        db.reseed_chunk_ids().await?;
        Ok(db)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Restart id allocation above every id the state has seen (open / restore).
    pub(crate) async fn reseed_chunk_ids(&self) -> Result<()> {
        let (last,): (i64,) = sqlx::query_as(
            r#"
            SELECT MAX(
                COALESCE((SELECT last FROM chunk_id_seq WHERE singleton = 0), 0),
                COALESCE((SELECT MAX(id) FROM chunks), 0)
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        self.next_chunk_id.fetch_max(last + 1, Ordering::SeqCst);
        Ok(())
    }

    /// Propose one chunk command; `Err(reason)` is a deterministic rejection.
    async fn propose(&self, op: pb::command::Op) -> Result<Result<(), pb::rejection::Reason>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(_) => Ok(Ok(())),
            Applied::Reject(bytes) => Ok(Err(pb::Rejection::decode(bytes.as_slice())?
                .reason
                .context("rejection without reason")?)),
        }
    }

    /// Propose a command that has no state-dependent failure.
    async fn propose_infallible(&self, op: pb::command::Op) -> Result<()> {
        match self.propose(op).await? {
            Ok(()) => Ok(()),
            Err(reason) => bail!("unexpected rejection {reason:?}"),
        }
    }

    pub async fn sync_instances(&self, configured: &[InstanceInfo]) -> Result<()> {
        let instances = configured
            .iter()
            .map(|i| pb::Instance {
                id: i.id.clone(),
                kind: i.kind.as_str().into(),
                fingerprint: i.fingerprint.clone(),
                location: i.location.clone(),
                state: i.role.as_str().into(),
            })
            .collect();
        self.propose_infallible(pb::command::Op::SyncInstances(pb::SyncInstances { instances }))
            .await
    }

    pub async fn list_instance_fingerprints(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query("SELECT id, fingerprint FROM instances")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>(0), r.get::<String, _>(1)))
            .collect())
    }

    /// Chunk row with refs = 1 and no blocks or parts (tests).
    #[cfg(test)]
    pub(crate) async fn insert_chunk(&self, logical_size: i64, crc32: u32) -> Result<ChunkId> {
        self.commit_chunk(logical_size, crc32, &[], &[]).await
    }

    /// Atomically insert chunk + blocks + parts after a successful quorum write.
    pub async fn commit_chunk(
        &self,
        logical_size: i64,
        crc32: u32,
        blocks: &[StoredBlock],
        replicas: &[ReplicaLayout],
    ) -> Result<ChunkId> {
        let chunk_id = self.next_chunk_id.fetch_add(1, Ordering::SeqCst);
        self.propose_infallible(pb::command::Op::CommitChunk(pb::CommitChunk {
            chunk_id,
            logical_size,
            crc32,
            blocks: blocks.iter().map(pb::Block::from).collect(),
            replicas: replicas.iter().map(pb::Replica::from).collect(),
            created_at: Utc::now().to_rfc3339(),
        }))
        .await?;
        Ok(chunk_id)
    }

    pub async fn set_root(&self, name: &str, extents: &[Extent]) -> Result<()> {
        self.propose_infallible(pb::command::Op::SetRoot(pb::SetRoot {
            name: name.into(),
            extents: extents.iter().map(pb::Extent::from).collect(),
        }))
        .await
    }

    pub async fn get_root(&self, name: &str) -> Result<Option<Vec<Extent>>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT extents_json FROM roots WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            None => Ok(None),
            Some((json,)) => {
                let extents: Vec<Extent> =
                    serde_json::from_str(&json).context("parse root extents_json")?;
                Ok(Some(extents))
            }
        }
    }

    /// All named roots (for purge / release-all).
    pub async fn list_roots(&self) -> Result<Vec<(String, Vec<Extent>)>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT name, extents_json FROM roots ORDER BY name")
                .fetch_all(&self.pool)
                .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (name, json) in rows {
            let extents: Vec<Extent> =
                serde_json::from_str(&json).context("parse root extents_json")?;
            out.push((name, extents));
        }
        Ok(out)
    }

    pub async fn delete_root(&self, name: &str) -> Result<()> {
        self.propose_infallible(pb::command::Op::DeleteRoot(pb::DeleteRoot { name: name.into() }))
            .await
    }

    /// Atomically bump refs for every id. Fails with [`ChunkGone`] (nothing retained)
    /// if any chunk is missing, has `refs = 0`, or is `reclaiming`.
    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        match self
            .propose(pb::command::Op::Retain(pb::Retain { chunk_ids: ids.to_vec() }))
            .await?
        {
            Ok(()) => Ok(()),
            Err(pb::rejection::Reason::ChunkGone(chunk_id)) => {
                pigeonhole_blob::record_chunk_gone();
                Err(anyhow::Error::new(ChunkGone { chunk_id }))
            }
            Err(other) => bail!("unexpected retain rejection {other:?}"),
        }
    }

    /// Decrement refs; errors on double-release (`refs` already 0 / missing / reclaiming).
    pub async fn release(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        match self
            .propose(pb::command::Op::Release(pb::Release { chunk_ids: ids.to_vec() }))
            .await?
        {
            Ok(()) => Ok(()),
            Err(pb::rejection::Reason::DoubleRelease(id)) => {
                pigeonhole_blob::record_double_release();
                bail!("double release or missing chunk {id}")
            }
            Err(other) => bail!("unexpected release rejection {other:?}"),
        }
    }

    pub async fn chunk_meta(&self, chunk_id: i64) -> Result<Option<(i64, u32, i64)>> {
        let row: Option<(i64, i64, i64)> =
            sqlx::query_as("SELECT logical_size, crc32, refs FROM chunks WHERE id = ?")
                .bind(chunk_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(size, crc, refs)| (size, crc as u32, refs)))
    }

    pub async fn chunk_block_count(&self, chunk_id: i64) -> Result<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT block_count FROM chunks WHERE id = ?")
                .bind(chunk_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn get_blocks(&self, chunk_id: i64) -> Result<Vec<StoredBlock>> {
        let rows: Vec<(i64, i64, i64, i64, String)> = sqlx::query_as(
            r#"
            SELECT block_no, logical_off, logical_len, stored_len, codec
            FROM chunk_blocks WHERE chunk_id = ? ORDER BY block_no
            "#,
        )
        .bind(chunk_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(block_no, logical_off, logical_len, stored_len, codec)| StoredBlock {
                    block_no,
                    logical_off,
                    logical_len,
                    stored_len,
                    codec,
                },
            )
            .collect())
    }

    /// Replica layouts for a chunk (for [`pigeonhole_blob::Replicated::read`]).
    ///
    /// `block_stored_lens` are reconstructed from `chunk_blocks` for Range GETs.
    pub async fn get_replica_layouts(&self, chunk_id: i64) -> Result<Vec<ReplicaLayout>> {
        let blocks = self.get_blocks(chunk_id).await?;
        let stored_lens: Vec<u32> = blocks
            .iter()
            .map(|b| b.stored_len.max(0) as u32)
            .collect();

        let rows: Vec<(String, i64, i64, i64, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            r#"
            SELECT instance_id, part_no, first_block, block_count, sort_key, locator
            FROM chunk_parts WHERE chunk_id = ?
            ORDER BY instance_id, part_no
            "#,
        )
        .bind(chunk_id)
        .fetch_all(&self.pool)
        .await?;

        let mut by_inst: std::collections::BTreeMap<String, Vec<PartLayout>> =
            std::collections::BTreeMap::new();
        for (instance_id, _part_no, first_block, block_count, sort_key, locator) in rows {
            let first = first_block.max(0) as u32;
            let count = block_count.max(0) as u32;
            let end = (first as usize).saturating_add(count as usize);
            let lenses = if end <= stored_lens.len() {
                stored_lens[first as usize..end].to_vec()
            } else {
                Vec::new()
            };
            by_inst.entry(instance_id).or_default().push(PartLayout {
                first_block: first,
                block_count: count,
                locator: BlobLocator {
                    key: sort_key,
                    locator,
                },
                block_stored_lens: lenses,
            });
        }
        Ok(by_inst
            .into_iter()
            .map(|(instance, parts)| ReplicaLayout { instance, parts })
            .collect())
    }

    /// Record the highest sort key observed from a successful backend put.
    pub async fn record_put_watermark(&self, instance_id: &str, max_key: &[u8]) -> Result<()> {
        self.record_put_watermark_at(instance_id, max_key, Utc::now())
            .await
    }

    /// Test / restore helper: record a watermark at an explicit timestamp.
    pub async fn record_put_watermark_at(
        &self,
        instance_id: &str,
        max_key: &[u8],
        at: chrono::DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO put_watermarks (instance_id, at, max_key)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(instance_id)
        .bind(at.to_rfc3339())
        .bind(max_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Max sort key among watermarks with `at <= now - grace` (sweep upper bound).
    pub async fn sweep_watermark(
        &self,
        instance_id: &str,
        grace: std::time::Duration,
    ) -> Result<Option<Vec<u8>>> {
        let cutoff = Utc::now() - chrono::Duration::from_std(grace).unwrap_or(chrono::Duration::zero());
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            r#"
            SELECT max_key FROM put_watermarks
            WHERE instance_id = ? AND at <= ?
            ORDER BY max_key DESC
            LIMIT 1
            "#,
        )
        .bind(instance_id)
        .bind(cutoff.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(k,)| k))
    }

    pub async fn get_sweep_cursor(&self, instance_id: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(Option<Vec<u8>>,)> =
            sqlx::query_as("SELECT after FROM sweep_cursor WHERE instance_id = ?")
                .bind(instance_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(a,)| a))
    }

    pub async fn set_sweep_cursor(
        &self,
        instance_id: &str,
        after: Option<&[u8]>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO sweep_cursor (instance_id, after) VALUES (?, ?)
            ON CONFLICT(instance_id) DO UPDATE SET after = excluded.after
            "#,
        )
        .bind(instance_id)
        .bind(after)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Sort keys of parts belonging to chunks with `refs > 0` on this instance.
    pub async fn live_part_keys(&self, instance_id: &str) -> Result<Vec<Vec<u8>>> {
        let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
            r#"
            SELECT p.sort_key
            FROM chunk_parts p
            INNER JOIN chunks c ON c.id = p.chunk_id
            WHERE p.instance_id = ? AND c.refs > 0 AND c.state = 'live'
            "#,
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// Which of `keys` belong to live (`refs > 0`, `state = live`) chunks on this instance.
    /// Used by the sweeper per-batch (never cache across batches).
    pub async fn live_part_keys_among(
        &self,
        instance_id: &str,
        keys: &[Vec<u8>],
    ) -> Result<HashSet<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(HashSet::new());
        }
        // SQLite: bind each key; chunk size is sweep batch (≤100).
        let mut placeholders = String::new();
        for i in 0..keys.len() {
            if i > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let sql = format!(
            r#"
            SELECT p.sort_key
            FROM chunk_parts p
            INNER JOIN chunks c ON c.id = p.chunk_id
            WHERE p.instance_id = ? AND c.refs > 0 AND c.state = 'live'
              AND p.sort_key IN ({placeholders})
            "#
        );
        let mut q = sqlx::query_as::<_, (Vec<u8>,)>(&sql).bind(instance_id);
        for k in keys {
            q = q.bind(k);
        }
        let rows = q.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// Chunk ids ready for reclaim: `refs = 0` and still `live`, plus crash leftovers
    /// already marked `reclaiming`.
    pub async fn list_zero_ref_chunks(&self) -> Result<Vec<ChunkId>> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            r#"
            SELECT id FROM chunks
            WHERE (refs = 0 AND state = 'live') OR state = 'reclaiming'
            ORDER BY id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Atomically mark a zero-ref live chunk as reclaiming. Returns false if another
    /// worker won the race or `retain` revived the chunk.
    pub async fn try_begin_reclaim(&self, chunk_id: ChunkId) -> Result<bool> {
        match self
            .propose(pb::command::Op::BeginReclaim(pb::BeginReclaim { chunk_id }))
            .await?
        {
            Ok(()) => Ok(true),
            Err(pb::rejection::Reason::NotReclaimable(_)) => Ok(false),
            Err(other) => bail!("unexpected begin_reclaim rejection {other:?}"),
        }
    }

    /// Delete chunk_parts, chunk_blocks, and the chunk row (after backend deletes).
    pub async fn delete_chunk_metadata(&self, chunk_id: ChunkId) -> Result<()> {
        self.propose_infallible(pb::command::Op::DropChunk(pb::DropChunk { chunk_id }))
            .await
    }

    /// Enqueue `repair(chunk_id, instance_id)` (idempotent).
    pub async fn enqueue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        let at = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO repair_queue (chunk_id, instance_id, enqueued_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chunk_id, instance_id) DO NOTHING
            "#,
        )
        .bind(chunk_id)
        .bind(instance_id)
        .bind(&at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Remove a completed / cancelled repair job.
    pub async fn dequeue_repair(&self, chunk_id: ChunkId, instance_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM repair_queue WHERE chunk_id = ? AND instance_id = ?")
            .bind(chunk_id)
            .bind(instance_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Pop up to `limit` jobs, prioritizing chunks with the fewest replicas.
    pub async fn take_repair_jobs(&self, limit: usize) -> Result<Vec<(ChunkId, String)>> {
        let limit = limit.max(1) as i64;
        let rows: Vec<(i64, String)> = sqlx::query_as(
            r#"
            SELECT q.chunk_id, q.instance_id
            FROM repair_queue q
            LEFT JOIN (
                SELECT chunk_id, COUNT(DISTINCT instance_id) AS n
                FROM chunk_parts
                GROUP BY chunk_id
            ) r ON r.chunk_id = q.chunk_id
            ORDER BY COALESCE(r.n, 0) ASC, q.enqueued_at ASC, q.chunk_id ASC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Live chunks (`refs > 0`) missing a replica on `instance_id`.
    pub async fn chunks_missing_instance(&self, instance_id: &str) -> Result<Vec<ChunkId>> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            r#"
            SELECT c.id
            FROM chunks c
            WHERE c.refs > 0
              AND NOT EXISTS (
                  SELECT 1 FROM chunk_parts p
                  WHERE p.chunk_id = c.id AND p.instance_id = ?
              )
            ORDER BY c.id
            "#,
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Replace all parts for one instance replica in a single transaction.
    pub async fn commit_instance_replica(
        &self,
        chunk_id: ChunkId,
        layout: &ReplicaLayout,
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::AddReplica(pb::AddReplica {
            chunk_id,
            replica: Some(pb::Replica::from(layout)),
        }))
        .await
    }

    /// Pending repair queue depth (tests / metrics).
    pub async fn repair_queue_len(&self) -> Result<u64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM repair_queue")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_blob::{InstanceKind, InstanceRole};

    #[tokio::test]
    async fn migrate_and_root_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "tg-main".into(),
            kind: InstanceKind::Telegram,
            fingerprint: "tg:1:-100".into(),
            location: "tg:chat:-100".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let fps = db.list_instance_fingerprints().await.unwrap();
        assert_eq!(fps, vec![("tg-main".into(), "tg:1:-100".into())]);

        let chunk_id = db.insert_chunk(32, 0xdeadbeef).await.unwrap();
        let layouts = vec![ReplicaLayout {
            instance: "tg-main".into(),
            parts: vec![PartLayout {
                first_block: 0,
                block_count: 0,
                locator: BlobLocator {
                    key: vec![0, 0, 0, 1],
                    locator: b"loc".to_vec(),
                },
                block_stored_lens: vec![],
            }],
        }];
        // Direct part insert via commit of empty blocks + one part.
        let id2 = db
            .commit_chunk(
                32,
                1,
                &[],
                &layouts,
            )
            .await
            .unwrap();
        let _ = chunk_id;
        db.set_root(
            "s3/index",
            &[Extent {
                chunk: id2,
                offset: 0,
                len: 32,
            }],
        )
        .await
        .unwrap();
        let root = db.get_root("s3/index").await.unwrap().unwrap();
        assert_eq!(root[0].chunk, id2);
        db.retain(&[id2]).await.unwrap();
        db.release(&[id2]).await.unwrap();
        assert_eq!(db.chunk_meta(id2).await.unwrap().unwrap().2, 1);
    }

    #[tokio::test]
    async fn missing_from_config_becomes_retired() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let a = InstanceInfo {
            id: "a".into(),
            kind: InstanceKind::Memory,
            fingerprint: "memory:local".into(),
            location: "memory:local".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[a.clone()]).await.unwrap();
        db.sync_instances(&[]).await.unwrap();
        let state: (String,) = sqlx::query_as("SELECT state FROM instances WHERE id = 'a'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(state.0, "retired");
    }

    #[tokio::test]
    async fn reclaim_then_retain_returns_chunk_gone() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let id = db
            .commit_chunk(
                8,
                1,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 1,
                        locator: BlobLocator {
                            key: vec![0, 0, 0, 0, 0, 0, 0, 1],
                            locator: b"{}".to_vec(),
                        },
                        block_stored_lens: vec![8],
                    }],
                }],
            )
            .await
            .unwrap();
        db.release(&[id]).await.unwrap();
        assert!(db.try_begin_reclaim(id).await.unwrap());
        let err = db.retain(&[id]).await.unwrap_err();
        assert!(is_chunk_gone(&err), "{err}");
        // Multi-id: one reclaiming → none retained.
        let id2 = db
            .commit_chunk(
                4,
                2,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 1,
                        locator: BlobLocator {
                            key: vec![0, 0, 0, 0, 0, 0, 0, 2],
                            locator: b"{}".to_vec(),
                        },
                        block_stored_lens: vec![4],
                    }],
                }],
            )
            .await
            .unwrap();
        let err = db.retain(&[id2, id]).await.unwrap_err();
        assert!(is_chunk_gone(&err), "{err}");
        let meta = db.chunk_meta(id2).await.unwrap().unwrap();
        assert_eq!(meta.2, 1, "id2 refs must be unchanged after rolled-back retain");
    }

    #[tokio::test]
    async fn reclaimed_chunk_ids_are_never_reused() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        db.sync_instances(&[InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        }])
        .await
        .unwrap();
        let commit = |key: u8| {
            let db = db.clone();
            async move {
                db.commit_chunk(
                    8,
                    1,
                    &[],
                    &[ReplicaLayout {
                        instance: "mem".into(),
                        parts: vec![PartLayout {
                            first_block: 0,
                            block_count: 1,
                            locator: BlobLocator {
                                key: vec![0, 0, 0, 0, 0, 0, 0, key],
                                locator: b"{}".to_vec(),
                            },
                            block_stored_lens: vec![8],
                        }],
                    }],
                )
                .await
                .unwrap()
            }
        };
        // Reclaim the highest id: plain INTEGER PRIMARY KEY would hand it out again,
        // and anything keyed by chunk id (block cache, stale extents) would alias.
        let old = commit(1).await;
        db.release(&[old]).await.unwrap();
        assert!(db.try_begin_reclaim(old).await.unwrap());
        db.delete_chunk_metadata(old).await.unwrap();
        let new = commit(2).await;
        assert!(new > old, "chunk id {old} reused as {new}");

        // Same after a restore: the high-water mark travels in the checkpoint.
        db.release(&[new]).await.unwrap();
        assert!(db.try_begin_reclaim(new).await.unwrap());
        db.delete_chunk_metadata(new).await.unwrap();
        let cp = db.export_checkpoint().await.unwrap();
        let url2 = format!("sqlite:{}?mode=rwc", dir.path().join("restored.db").display());
        let restored = BlobDb::connect(&url2).await.unwrap();
        restored.import_checkpoint(&cp).await.unwrap();
        let after = restored.insert_chunk(8, 1).await.unwrap();
        assert!(after > new, "chunk id {new} reused as {after} after restore");
    }

    #[tokio::test]
    async fn double_release_errors() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let info = InstanceInfo {
            id: "mem".into(),
            kind: InstanceKind::Memory,
            fingerprint: "memory:mem".into(),
            location: "memory:mem".into(),
            role: InstanceRole::ReadWrite,
        };
        db.sync_instances(&[info]).await.unwrap();
        let id = db
            .commit_chunk(
                1,
                1,
                &[],
                &[ReplicaLayout {
                    instance: "mem".into(),
                    parts: vec![PartLayout {
                        first_block: 0,
                        block_count: 0,
                        locator: BlobLocator {
                            key: vec![1],
                            locator: b"{}".to_vec(),
                        },
                        block_stored_lens: vec![],
                    }],
                }],
            )
            .await
            .unwrap();
        db.release(&[id]).await.unwrap();
        let err = db.release(&[id]).await.unwrap_err();
        assert!(
            err.to_string().contains("double release"),
            "{err}"
        );
    }
}
