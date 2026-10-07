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

/// Internal integer chunk id (row in `chunks`).
pub type ChunkId = i64;

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
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&self.pool)
            .await?;

        // Stage B renames for existing DBs.
        Self::rename_table_if_exists(&self.pool, "blobs", "chunks").await?;
        Self::rename_table_if_exists(&self.pool, "replicas", "chunk_replicas").await?;
        Self::rename_column_if_exists(&self.pool, "chunk_replicas", "blob_id", "chunk_id").await?;
        Self::rename_column_if_exists(&self.pool, "chunk_blocks", "blob_id", "chunk_id").await?;
        Self::rename_column_if_exists(&self.pool, "roots", "blob_id", "chunk_id").await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY NOT NULL,
                kind TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                location TEXT NOT NULL,
                state TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_location_active
            ON instances(location)
            WHERE state = 'read-write'
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create chunks with the stage-E shape when missing.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY NOT NULL,
                logical_size INTEGER NOT NULL,
                crc32 INTEGER NOT NULL,
                refs INTEGER NOT NULL DEFAULT 0,
                block_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Upgrade pre-E chunks: size → logical_size, add block_count.
        Self::rename_column_if_exists(&self.pool, "chunks", "size", "logical_size").await?;
        Self::add_column_if_missing(
            &self.pool,
            "chunks",
            "block_count",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;

        // Legacy single-replica table (migrated → chunk_parts below).
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunk_replicas (
                chunk_id INTEGER NOT NULL REFERENCES chunks(id),
                instance_id TEXT NOT NULL REFERENCES instances(id),
                sort_key BLOB NOT NULL,
                locator BLOB NOT NULL,
                PRIMARY KEY (chunk_id, instance_id),
                UNIQUE (instance_id, sort_key)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Self::rename_table_if_exists(&self.pool, "chunk_frames", "chunk_blocks").await?;
        Self::rename_column_if_exists(&self.pool, "chunk_blocks", "frame_no", "block_no").await?;

        // Pre-E tables may still have a unused `stored_off` column; new installs omit it.
        // Offsets inside a part are derived from sum(stored_len), not from this column.
        if !Self::table_exists(&self.pool, "chunk_blocks").await? {
            sqlx::query(
                r#"
                CREATE TABLE chunk_blocks (
                    chunk_id INTEGER NOT NULL REFERENCES chunks(id),
                    block_no INTEGER NOT NULL,
                    logical_off INTEGER NOT NULL,
                    logical_len INTEGER NOT NULL,
                    stored_len INTEGER NOT NULL,
                    codec TEXT NOT NULL,
                    PRIMARY KEY (chunk_id, block_no)
                )
                "#,
            )
            .execute(&self.pool)
            .await?;
        }

        sqlx::query(
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
        )
        .execute(&self.pool)
        .await?;

        self.migrate_replicas_to_parts().await?;

        // Fill block_count on chunks from chunk_blocks when still zero.
        sqlx::query(
            r#"
            UPDATE chunks SET block_count = (
                SELECT COUNT(*) FROM chunk_blocks b WHERE b.chunk_id = chunks.id
            )
            WHERE block_count = 0
              AND EXISTS (SELECT 1 FROM chunk_blocks b WHERE b.chunk_id = chunks.id)
            "#,
        )
        .execute(&self.pool)
        .await?;

        self.migrate_roots_to_extents().await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS put_watermarks (
                instance_id TEXT NOT NULL,
                at TEXT NOT NULL,
                max_key BLOB NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sweep_cursor (
                instance_id TEXT PRIMARY KEY NOT NULL,
                after BLOB
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS repair_queue (
                chunk_id INTEGER NOT NULL REFERENCES chunks(id),
                instance_id TEXT NOT NULL,
                enqueued_at TEXT NOT NULL,
                PRIMARY KEY (chunk_id, instance_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn migrate_replicas_to_parts(&self) -> Result<()> {
        let replicas_exist = Self::table_exists(&self.pool, "chunk_replicas").await?;
        if !replicas_exist {
            return Ok(());
        }
        let rows: Vec<(i64, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT chunk_id, instance_id, sort_key, locator FROM chunk_replicas",
        )
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for (chunk_id, instance_id, sort_key, locator) in rows {
            let block_count: (i64,) = sqlx::query_as(
                "SELECT COALESCE((SELECT block_count FROM chunks WHERE id = ?), 0)",
            )
            .bind(chunk_id)
            .fetch_one(&self.pool)
            .await?;
            let mut bc = block_count.0;
            if bc == 0 {
                let counted: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM chunk_blocks WHERE chunk_id = ?",
                )
                .bind(chunk_id)
                .fetch_one(&self.pool)
                .await?;
                bc = counted.0;
            }
            // Legacy: one replica blob held all blocks → part_no 0.
            sqlx::query(
                r#"
                INSERT OR IGNORE INTO chunk_parts
                  (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
                VALUES (?, ?, 0, 0, ?, ?, ?)
                "#,
            )
            .bind(chunk_id)
            .bind(&instance_id)
            .bind(bc)
            .bind(&sort_key)
            .bind(&locator)
            .execute(&self.pool)
            .await?;
        }
        sqlx::query("DELETE FROM chunk_replicas")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn migrate_roots_to_extents(&self) -> Result<()> {
        if !Self::table_exists(&self.pool, "roots").await? {
            sqlx::query(
                r#"
                CREATE TABLE roots (
                    name TEXT PRIMARY KEY NOT NULL,
                    extents_json TEXT NOT NULL DEFAULT '[]'
                )
                "#,
            )
            .execute(&self.pool)
            .await?;
            return Ok(());
        }

        if Self::column_exists(&self.pool, "roots", "extents_json").await?
            && !Self::column_exists(&self.pool, "roots", "chunk_id").await?
        {
            return Ok(());
        }

        // In-place: add extents_json; legacy chunk_id column may remain unused.
        Self::add_column_if_missing(
            &self.pool,
            "roots",
            "extents_json",
            "TEXT NOT NULL DEFAULT '[]'",
        )
        .await?;

        if Self::column_exists(&self.pool, "roots", "chunk_id").await? {
            let rows: Vec<(String, i64, String)> = sqlx::query_as(
                "SELECT name, chunk_id, extents_json FROM roots",
            )
            .fetch_all(&self.pool)
            .await?;
            for (name, chunk_id, json) in rows {
                if json != "[]" && !json.is_empty() {
                    continue;
                }
                let size: Option<(i64,)> =
                    sqlx::query_as("SELECT logical_size FROM chunks WHERE id = ?")
                        .bind(chunk_id)
                        .fetch_optional(&self.pool)
                        .await?;
                let size = size.map(|s| s.0).unwrap_or(0);
                let extents = vec![Extent {
                    chunk: chunk_id,
                    offset: 0,
                    len: size,
                }];
                let new_json = serde_json::to_string(&extents)?;
                sqlx::query("UPDATE roots SET extents_json = ? WHERE name = ?")
                    .bind(&new_json)
                    .bind(&name)
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn table_exists(pool: &SqlitePool, name: &str) -> Result<bool> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(name)
        .fetch_optional(pool)
        .await?;
        Ok(exists.is_some())
    }

    async fn column_exists(pool: &SqlitePool, table: &str, col: &str) -> Result<bool> {
        if !Self::table_exists(pool, table).await? {
            return Ok(false);
        }
        let cols: Vec<(i64, String)> =
            sqlx::query_as(&format!("SELECT cid, name FROM pragma_table_info('{table}')"))
                .fetch_all(pool)
                .await?;
        Ok(cols.iter().any(|(_, n)| n == col))
    }

    async fn add_column_if_missing(
        pool: &SqlitePool,
        table: &str,
        col: &str,
        decl: &str,
    ) -> Result<()> {
        if Self::column_exists(pool, table, col).await? {
            return Ok(());
        }
        if !Self::table_exists(pool, table).await? {
            return Ok(());
        }
        sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {col} {decl}"))
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn rename_table_if_exists(
        pool: &SqlitePool,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(from)
        .fetch_optional(pool)
        .await?;
        if exists.is_none() {
            return Ok(());
        }
        let dest_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(to)
        .fetch_optional(pool)
        .await?;
        if dest_exists.is_some() {
            return Ok(());
        }
        sqlx::query(&format!("ALTER TABLE {from} RENAME TO {to}"))
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn rename_column_if_exists(
        pool: &SqlitePool,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<()> {
        let table_exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_optional(pool)
        .await?;
        if table_exists.is_none() {
            return Ok(());
        }
        let cols: Vec<(i64, String)> =
            sqlx::query_as(&format!("SELECT cid, name FROM pragma_table_info('{table}')"))
                .fetch_all(pool)
                .await?;
        let names: Vec<&str> = cols.iter().map(|(_, n)| n.as_str()).collect();
        if !names.contains(&from) || names.contains(&to) {
            return Ok(());
        }
        sqlx::query(&format!("ALTER TABLE {table} RENAME COLUMN {from} TO {to}"))
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Upsert configured instances; rows missing from config become `retired`.
    pub async fn sync_instances(&self, configured: &[InstanceInfo]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
        let res = sqlx::query(
            r#"
            INSERT INTO chunks (logical_size, crc32, refs, block_count, created_at)
            VALUES (?, ?, 1, ?, ?)
            "#,
        )
        .bind(logical_size)
        .bind(i64::from(crc32))
        .bind(block_count)
        .bind(&at)
        .execute(&self.pool)
        .await?;
        Ok(res.last_insert_rowid())
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
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            r#"
            INSERT INTO chunks (logical_size, crc32, refs, block_count, created_at)
            VALUES (?, ?, 1, ?, ?)
            "#,
        )
        .bind(logical_size)
        .bind(i64::from(crc32))
        .bind(blocks.len() as i64)
        .bind(&at)
        .execute(&mut *tx)
        .await?;
        let chunk_id = res.last_insert_rowid();

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

    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        for id in ids {
            sqlx::query("UPDATE chunks SET refs = refs + 1 WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn release(&self, ids: &[i64]) -> Result<()> {
        for id in ids {
            sqlx::query("UPDATE chunks SET refs = MAX(refs - 1, 0) WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
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
        let mut tx = self.pool.begin().await?;
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
            WHERE p.instance_id = ? AND c.refs > 0
            "#,
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// Chunk ids with `refs = 0` (physical reclaim candidates).
    pub async fn list_zero_ref_chunks(&self) -> Result<Vec<ChunkId>> {
        let rows: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM chunks WHERE refs = 0 ORDER BY id")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Delete chunk_parts, chunk_blocks, and the chunk row (after backend deletes).
    pub async fn delete_chunk_metadata(&self, chunk_id: ChunkId) -> Result<()> {
        let mut tx = self.pool.begin().await?;
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
        let mut tx = self.pool.begin().await?;
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
        db.release(&[id2]).await.unwrap();
        db.retain(&[id2]).await.unwrap();
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
    async fn migrates_legacy_replicas_to_parts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        // Build a pre-E schema manually.
        let url = format!("sqlite:{}?mode=rwc", path.display());
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE instances (
                id TEXT PRIMARY KEY, kind TEXT, fingerprint TEXT, location TEXT, state TEXT
            );
            CREATE TABLE chunks (
                id INTEGER PRIMARY KEY, size INTEGER, crc32 INTEGER, refs INTEGER, created_at TEXT
            );
            CREATE TABLE chunk_replicas (
                chunk_id INTEGER, instance_id TEXT, sort_key BLOB, locator BLOB,
                PRIMARY KEY (chunk_id, instance_id)
            );
            CREATE TABLE chunk_blocks (
                chunk_id INTEGER, block_no INTEGER, stored_off INTEGER, stored_len INTEGER,
                logical_off INTEGER, logical_len INTEGER, codec TEXT,
                PRIMARY KEY (chunk_id, block_no)
            );
            CREATE TABLE roots (name TEXT PRIMARY KEY, chunk_id INTEGER);
            INSERT INTO instances VALUES ('mem','memory','memory:m','memory:m','read-write');
            INSERT INTO chunks VALUES (1, 100, 0, 1, 't');
            INSERT INTO chunk_replicas VALUES (1, 'mem', x'01', x'02');
            INSERT INTO chunk_blocks VALUES (1, 0, 0, 50, 0, 50, 'raw');
            INSERT INTO chunk_blocks VALUES (1, 1, 50, 50, 50, 50, 'raw');
            INSERT INTO roots VALUES ('s3/index', 1);
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        drop(pool);

        let db = BlobDb::connect(&url).await.unwrap();
        let layouts = db.get_replica_layouts(1).await.unwrap();
        assert_eq!(layouts.len(), 1);
        assert_eq!(layouts[0].parts.len(), 1);
        assert_eq!(layouts[0].parts[0].first_block, 0);
        assert_eq!(layouts[0].parts[0].block_count, 2);
        assert_eq!(layouts[0].parts[0].block_stored_lens, vec![50, 50]);
        let root = db.get_root("s3/index").await.unwrap().unwrap();
        assert_eq!(root[0].chunk, 1);
        assert_eq!(root[0].len, 100);
        let blocks = db.get_blocks(1).await.unwrap();
        assert_eq!(blocks.len(), 2);
        // Legacy stored_off may remain; readers ignore it.
    }

    #[tokio::test]
    async fn migrated_parts_are_readable_via_layer() {
        use crate::ingest::IngestOptions;
        use crate::layer::ChunkStore;
        use pigeonhole_blob::{erase, CheapestFirst, Replicated};
        use pigeonhole_codec::ChunkCodec;
        use pigeonhole_storage_memory::MemoryBlobStore;
        use pigeonhole_types::{BackendLimits, RangeSupport};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let url = format!("sqlite:{}?mode=rwc", path.display());

        // Seed legacy schema + one replica blob in memory store.
        let store = MemoryBlobStore::with_limits(BackendLimits {
            max_blob_size: 1024 * 1024,
            supports_range: RangeSupport::BestEffort,
            can_list: false,
        })
        .with_instance_id("mem");
        let payload = bytes::Bytes::from(vec![3u8; 200]);
        let id = {
            use pigeonhole_blob::BlobBackend;
            BlobBackend::put(&store, payload.clone()).await.unwrap()
        };
        let loc = pigeonhole_blob::store_id::<MemoryBlobStore>(&id).unwrap();

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE instances (
                id TEXT PRIMARY KEY, kind TEXT, fingerprint TEXT, location TEXT, state TEXT
            );
            CREATE TABLE chunks (
                id INTEGER PRIMARY KEY, size INTEGER, crc32 INTEGER, refs INTEGER, created_at TEXT
            );
            CREATE TABLE chunk_replicas (
                chunk_id INTEGER, instance_id TEXT, sort_key BLOB, locator BLOB,
                PRIMARY KEY (chunk_id, instance_id)
            );
            CREATE TABLE chunk_blocks (
                chunk_id INTEGER, block_no INTEGER, stored_off INTEGER, stored_len INTEGER,
                logical_off INTEGER, logical_len INTEGER, codec TEXT,
                PRIMARY KEY (chunk_id, block_no)
            );
            CREATE TABLE roots (name TEXT PRIMARY KEY, chunk_id INTEGER);
            INSERT INTO instances VALUES ('mem','memory','memory:mem','memory:mem','read-write');
            INSERT INTO chunks VALUES (1, 200, 0, 1, 't');
            INSERT INTO chunk_blocks VALUES (1, 0, 0, 200, 0, 200, 'raw');
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO chunk_replicas (chunk_id, instance_id, sort_key, locator) VALUES (1, 'mem', ?, ?)",
        )
        .bind(&loc.key)
        .bind(&loc.locator)
        .execute(&pool)
        .await
        .unwrap();
        drop(pool);

        let db = BlobDb::connect(&url).await.unwrap();
        let layouts = db.get_replica_layouts(1).await.unwrap();
        assert_eq!(layouts[0].parts.len(), 1);

        let rep = Arc::new(
            Replicated::new(
                vec![Arc::new(erase(store))],
                1,
                Arc::new(CheapestFirst::new()),
            )
            .unwrap(),
        );
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open_replicated(db, rep, opts).await.unwrap();
        let got = layer
            .read(
                &[Extent {
                    chunk: 1,
                    offset: 0,
                    len: 200,
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(got, payload);
    }
}
