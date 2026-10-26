//! Separate `blob.db` metadata for the blob layer (stage E schema).
//!
//! Gateways keep their own SQLite indexes; this DB owns instances, chunks,
//! parts, chunk blocks, roots (extent lists), sweeper state, and repair queue.

use anyhow::{Context, Result};
use chrono::Utc;
use pigeonhole_blob::{InstanceInfo, PartLayout, ReplicaLayout, BlobLocator};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;

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

/// Connection to the blob-layer metadata database.
#[derive(Clone, Debug)]
pub struct BlobDb {
    pool: SqlitePool,
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

/// Current `blob.db` schema. No upgrades: an incompatible file is recreated by hand.
const SCHEMA: &[&str] = &[
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

/// Allocate the next chunk id inside `tx` (which must hold the write lock).
async fn next_chunk_id(tx: &mut sqlx::SqliteConnection) -> Result<ChunkId> {
    let (next,): (i64,) = sqlx::query_as(
        r#"
        SELECT MAX(
            COALESCE((SELECT last FROM chunk_id_seq WHERE singleton = 0), 0),
            COALESCE((SELECT MAX(id) FROM chunks), 0)
        ) + 1
        "#,
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chunk_id_seq (singleton, last) VALUES (0, ?)
        ON CONFLICT (singleton) DO UPDATE SET last = excluded.last
        "#,
    )
    .bind(next)
    .execute(&mut *tx)
    .await?;
    Ok(next)
}

impl BlobDb {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await
            .with_context(|| format!("connect blob db {database_url}"))?;
        let db = Self { pool };
        db.migrate().await?;
        Ok(db)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn migrate(&self) -> Result<()> {
        for pragma in [
            "PRAGMA foreign_keys = ON",
            "PRAGMA journal_mode = WAL",
            "PRAGMA busy_timeout = 5000",
        ] {
            sqlx::query(pragma).execute(&self.pool).await?;
        }
        for ddl in SCHEMA {
            sqlx::query(ddl).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn sync_instances(&self, configured: &[InstanceInfo]) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let existing: Vec<(String,)> = sqlx::query_as("SELECT id FROM instances")
            .fetch_all(&mut *tx)
            .await?;
        let configured_ids: std::collections::HashSet<&str> =
            configured.iter().map(|i| i.id.as_str()).collect();

        for (id,) in &existing {
            if !configured_ids.contains(id.as_str()) {
                sqlx::query("UPDATE instances SET state = 'retired' WHERE id = ?")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        for inst in configured {
            sqlx::query(
                r#"
                INSERT INTO instances (id, kind, fingerprint, location, state)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    kind = excluded.kind,
                    fingerprint = excluded.fingerprint,
                    location = excluded.location,
                    state = excluded.state
                "#,
            )
            .bind(&inst.id)
            .bind(inst.kind.as_str())
            .bind(&inst.fingerprint)
            .bind(&inst.location)
            .bind(inst.role.as_str())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
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

    /// Insert a single part row (legacy migrate / repair).
    pub async fn add_part(
        &self,
        chunk_id: i64,
        instance_id: &str,
        part_no: i64,
        first_block: i64,
        block_count: i64,
        sort_key: &[u8],
        locator: &[u8],
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO chunk_parts
              (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(chunk_id)
        .bind(instance_id)
        .bind(part_no)
        .bind(first_block)
        .bind(block_count)
        .bind(sort_key)
        .bind(locator)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Insert a chunk row with refs=1; returns chunk_id.
    pub async fn insert_chunk(
        &self,
        logical_size: i64,
        crc32: u32,
        block_count: i64,
    ) -> Result<i64> {
        let at = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let chunk_id = next_chunk_id(&mut tx).await?;
        sqlx::query(
            r#"
            INSERT INTO chunks (id, logical_size, crc32, refs, block_count, created_at)
            VALUES (?, ?, ?, 1, ?, ?)
            "#,
        )
        .bind(chunk_id)
        .bind(logical_size)
        .bind(i64::from(crc32))
        .bind(block_count)
        .bind(&at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(chunk_id)
    }

    /// Atomically insert chunk + blocks + parts after a successful quorum write.
    pub async fn commit_chunk(
        &self,
        logical_size: i64,
        crc32: u32,
        blocks: &[StoredBlock],
        replicas: &[ReplicaLayout],
    ) -> Result<ChunkId> {
        let at = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let chunk_id = next_chunk_id(&mut tx).await?;
        sqlx::query(
            r#"
            INSERT INTO chunks (id, logical_size, crc32, refs, block_count, created_at)
            VALUES (?, ?, ?, 1, ?, ?)
            "#,
        )
        .bind(chunk_id)
        .bind(logical_size)
        .bind(i64::from(crc32))
        .bind(blocks.len() as i64)
        .bind(&at)
        .execute(&mut *tx)
        .await?;

        for b in blocks {
            sqlx::query(
                r#"
                INSERT INTO chunk_blocks
                  (chunk_id, block_no, logical_off, logical_len, stored_len, codec)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(chunk_id)
            .bind(b.block_no)
            .bind(b.logical_off)
            .bind(b.logical_len)
            .bind(b.stored_len)
            .bind(&b.codec)
            .execute(&mut *tx)
            .await?;
        }

        // Reject duplicate sort keys inside one commit (would violate UNIQUE and
        // indicate a packer / fan-out bug).
        {
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            for rep in replicas {
                for part in &rep.parts {
                    let k = (rep.instance.as_str(), part.locator.key.as_slice());
                    if !seen.insert(k) {
                        anyhow::bail!(
                            "duplicate sort_key in commit_chunk for instance {}",
                            rep.instance
                        );
                    }
                }
            }
        }

        for rep in replicas {
            for (part_no, part) in rep.parts.iter().enumerate() {
                sqlx::query(
                    r#"
                    INSERT INTO chunk_parts
                      (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#,
                )
                .bind(chunk_id)
                .bind(&rep.instance)
                .bind(part_no as i64)
                .bind(i64::from(part.first_block))
                .bind(i64::from(part.block_count))
                .bind(&part.locator.key)
                .bind(&part.locator.locator)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "insert chunk_parts chunk={chunk_id} instance={} part={part_no} key_hex={}: {e}",
                        rep.instance,
                        hex::encode(&part.locator.key)
                    )
                })?;
            }
        }

        tx.commit().await?;
        Ok(chunk_id)
    }

    pub async fn set_root(&self, name: &str, extents: &[Extent]) -> Result<()> {
        let json = serde_json::to_string(extents).context("serialize root extents")?;
        sqlx::query(
            r#"
            INSERT INTO roots (name, extents_json) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET extents_json = excluded.extents_json
            "#,
        )
        .bind(name)
        .bind(&json)
        .execute(&self.pool)
        .await?;
        Ok(())
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
        sqlx::query("DELETE FROM roots WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Atomically bump refs for every id. Fails with [`ChunkGone`] (and rolls back)
    /// if any chunk is missing, has `refs = 0`, or is `reclaiming`.
    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for &id in ids {
            let res = sqlx::query(
                r#"
                UPDATE chunks
                SET refs = refs + 1
                WHERE id = ? AND refs > 0 AND state = 'live'
                "#,
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
            if res.rows_affected() != 1 {
                // Explicit rollback before returning typed error.
                tx.rollback().await.ok();
                pigeonhole_blob::record_chunk_gone();
                return Err(anyhow::Error::new(ChunkGone { chunk_id: id }));
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Decrement refs; errors on double-release (`refs` already 0 / missing / reclaiming).
    pub async fn release(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for &id in ids {
            let res = sqlx::query(
                r#"
                UPDATE chunks
                SET refs = refs - 1
                WHERE id = ? AND refs > 0 AND state = 'live'
                "#,
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
            if res.rows_affected() != 1 {
                tx.rollback().await.ok();
                pigeonhole_blob::record_double_release();
                anyhow::bail!("double release or missing chunk {id}");
            }
        }
        tx.commit().await?;
        Ok(())
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

    pub async fn replace_blocks(&self, chunk_id: i64, blocks: &[StoredBlock]) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM chunk_blocks WHERE chunk_id = ?")
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        for fr in blocks {
            sqlx::query(
                r#"
                INSERT INTO chunk_blocks
                  (chunk_id, block_no, logical_off, logical_len, stored_len, codec)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(chunk_id)
            .bind(fr.block_no)
            .bind(fr.logical_off)
            .bind(fr.logical_len)
            .bind(fr.stored_len)
            .bind(&fr.codec)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("UPDATE chunks SET block_count = ? WHERE id = ?")
            .bind(blocks.len() as i64)
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
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
        let res = sqlx::query(
            r#"
            UPDATE chunks
            SET state = 'reclaiming'
            WHERE id = ? AND refs = 0 AND state = 'live'
            "#,
        )
        .bind(chunk_id)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 1 {
            return Ok(true);
        }
        // Already reclaiming (restart path)?
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM chunks WHERE id = ?")
                .bind(chunk_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(matches!(row.as_ref().map(|(s,)| s.as_str()), Some(CHUNK_STATE_RECLAIMING)))
    }

    /// Delete chunk_parts, chunk_blocks, and the chunk row (after backend deletes).
    pub async fn delete_chunk_metadata(&self, chunk_id: ChunkId) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM chunk_parts WHERE chunk_id = ?")
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunk_blocks WHERE chunk_id = ?")
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM repair_queue WHERE chunk_id = ?")
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chunks WHERE id = ?")
            .bind(chunk_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
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
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM chunk_parts WHERE chunk_id = ? AND instance_id = ?")
            .bind(chunk_id)
            .bind(&layout.instance)
            .execute(&mut *tx)
            .await?;
        for (part_no, part) in layout.parts.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO chunk_parts
                  (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(chunk_id)
            .bind(&layout.instance)
            .bind(part_no as i64)
            .bind(i64::from(part.first_block))
            .bind(i64::from(part.block_count))
            .bind(&part.locator.key)
            .bind(&part.locator.locator)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
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

        let chunk_id = db.insert_chunk(32, 0xdeadbeef, 0).await.unwrap();
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
        let after = restored.insert_chunk(8, 1, 0).await.unwrap();
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
