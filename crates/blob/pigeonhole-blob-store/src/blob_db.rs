//! Separate `blob.db` metadata for the blob layer (stage 1.4).
//!
//! Gateways keep their own SQLite indexes; this DB owns instances, blobs,
//! replicas, chunk frames (by blob_id), roots, and sweeper state.

use anyhow::{Context, Result};
use chrono::Utc;
use pigeonhole_blob::InstanceInfo;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};

/// Connection to the blob-layer metadata database.
#[derive(Clone, Debug)]
pub struct BlobDb {
    pool: SqlitePool,
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

        // At most one read-write writer per location is enforced in config;
        // DB keeps a partial unique index for non-retired rows when possible.
        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_location_active
            ON instances(location)
            WHERE state = 'read-write'
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS blobs (
                id INTEGER PRIMARY KEY NOT NULL,
                size INTEGER NOT NULL,
                crc32 INTEGER NOT NULL,
                refs INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS replicas (
                blob_id INTEGER NOT NULL REFERENCES blobs(id),
                instance_id TEXT NOT NULL REFERENCES instances(id),
                sort_key BLOB NOT NULL,
                locator BLOB NOT NULL,
                PRIMARY KEY (blob_id, instance_id),
                UNIQUE (instance_id, sort_key)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chunk_frames (
                blob_id INTEGER NOT NULL REFERENCES blobs(id),
                frame_no INTEGER NOT NULL,
                stored_off INTEGER NOT NULL,
                stored_len INTEGER NOT NULL,
                logical_off INTEGER NOT NULL,
                logical_len INTEGER NOT NULL,
                codec TEXT NOT NULL,
                PRIMARY KEY (blob_id, frame_no)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS roots (
                name TEXT PRIMARY KEY NOT NULL,
                blob_id INTEGER NOT NULL REFERENCES blobs(id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

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

    /// Insert a blob row with refs=1; returns blob_id.
    pub async fn insert_blob(&self, size: i64, crc32: u32) -> Result<i64> {
        let at = Utc::now().to_rfc3339();
        let res = sqlx::query(
            "INSERT INTO blobs (size, crc32, refs, created_at) VALUES (?, ?, 1, ?)",
        )
        .bind(size)
        .bind(i64::from(crc32))
        .bind(&at)
        .execute(&self.pool)
        .await?;
        Ok(res.last_insert_rowid())
    }

    pub async fn add_replica(
        &self,
        blob_id: i64,
        instance_id: &str,
        sort_key: &[u8],
        locator: &[u8],
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO replicas (blob_id, instance_id, sort_key, locator)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(blob_id)
        .bind(instance_id)
        .bind(sort_key)
        .bind(locator)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_root(&self, name: &str, blob_id: i64) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO roots (name, blob_id) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET blob_id = excluded.blob_id
            "#,
        )
        .bind(name)
        .bind(blob_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_root(&self, name: &str) -> Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT blob_id FROM roots WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn retain(&self, ids: &[i64]) -> Result<()> {
        for id in ids {
            sqlx::query("UPDATE blobs SET refs = refs + 1 WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn release(&self, ids: &[i64]) -> Result<()> {
        for id in ids {
            sqlx::query("UPDATE blobs SET refs = MAX(refs - 1, 0) WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
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

        let blob_id = db.insert_blob(32, 0xdeadbeef).await.unwrap();
        db.add_replica(blob_id, "tg-main", &[0, 0, 0, 1], b"loc")
            .await
            .unwrap();
        db.set_root("s3/index", blob_id).await.unwrap();
        assert_eq!(db.get_root("s3/index").await.unwrap(), Some(blob_id));
        db.release(&[blob_id]).await.unwrap();
        db.retain(&[blob_id]).await.unwrap();
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
}
