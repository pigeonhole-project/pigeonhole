//! Migrate legacy s3gram index blob rows into `blob.db` (stage E / F).

use crate::blob_db::{BlobDb, Extent, StoredBlock};
use crate::instances::{telegram_fingerprint, telegram_location, InstanceConfig};
use anyhow::{Context, Result};
use pigeonhole_blob::{InstanceInfo, InstanceKind, InstanceRole, BlobLocator};
use serde::Serialize;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

#[derive(Debug, Default, Serialize)]
pub struct MigrateReport {
    pub instances: usize,
    pub blobs: usize,
    pub replicas: usize,
    pub frames: usize,
    pub roots: usize,
    pub dry_run: bool,
    pub notes: Vec<String>,
}

#[derive(sqlx::FromRow)]
struct LegacyBlob {
    file_id: String,
    message_id: i64,
    size: i64,
    refcount: i64,
    chat_id: String,
}

#[derive(sqlx::FromRow)]
struct LegacyFrame {
    file_id: String,
    block_no: i64,
    stored_len: i64,
    logical_off: i64,
    logical_len: i64,
    codec: String,
}

/// Copy `blobs` / `chunk_blocks` / snapshot root from a legacy index DB into BlobDb.
pub async fn migrate_index_to_blob_db(
    legacy_database_url: &str,
    blob_db: &BlobDb,
    instance: &InstanceConfig,
    dry_run: bool,
) -> Result<MigrateReport> {
    let mut report = MigrateReport {
        dry_run,
        ..Default::default()
    };

    let url = if legacy_database_url.starts_with("sqlite:") && !legacy_database_url.contains('?') {
        format!("{legacy_database_url}?mode=rwc")
    } else {
        legacy_database_url.to_string()
    };
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .with_context(|| format!("connect legacy index {url}"))?;

    let blobs = sqlx::query_as::<_, LegacyBlob>(
        "SELECT file_id, message_id, size, refcount, chat_id FROM blobs",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let frames = sqlx::query_as::<_, LegacyFrame>(
        r#"
        SELECT file_id, block_no, stored_len, logical_off, logical_len, codec
        FROM chunk_blocks
        "#,
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    report.notes.push(format!(
        "legacy: {} blob rows, {} frame rows",
        blobs.len(),
        frames.len()
    ));

    if !dry_run {
        blob_db
            .sync_instances(std::slice::from_ref(&instance.info))
            .await?;
    }
    report.instances = 1;

    let mut file_to_blob: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    for b in &blobs {
        report.blobs += 1;
        report.replicas += 1;
        if dry_run {
            continue;
        }
        let crc = 0u32;
        let chunk_id = blob_db.insert_chunk(b.size, crc, 0).await?;
        if b.refcount > 1 {
            let extra = (b.refcount - 1) as usize;
            let ids = vec![chunk_id; extra];
            blob_db.retain(&ids).await?;
        } else if b.refcount == 0 {
            blob_db.release(&[chunk_id]).await?;
        }
        let stored = BlobLocator {
            key: (b.message_id as u64).to_be_bytes().to_vec(),
            locator: serde_json::to_vec(&serde_json::json!({
                "file_id": b.file_id,
                "message_id": b.message_id,
            }))?,
        };
        blob_db
            .add_part(
                chunk_id,
                &instance.info.id,
                0,
                0,
                0,
                &stored.key,
                &stored.locator,
            )
            .await?;
        file_to_blob.insert(b.file_id.clone(), chunk_id);
    }

    let mut by_file: std::collections::HashMap<String, Vec<StoredBlock>> =
        std::collections::HashMap::new();
    for fr in &frames {
        report.frames += 1;
        by_file.entry(fr.file_id.clone()).or_default().push(StoredBlock {
            block_no: fr.block_no,
            logical_off: fr.logical_off,
            logical_len: fr.logical_len,
            stored_len: fr.stored_len,
            codec: fr.codec.clone(),
        });
    }
    if !dry_run {
        for (file_id, mut frames) in by_file {
            frames.sort_by_key(|f| f.block_no);
            if let Some(&chunk_id) = file_to_blob.get(&file_id) {
                let n = frames.len() as i64;
                blob_db.replace_blocks(chunk_id, &frames).await?;
                sqlx::query(
                    "UPDATE chunk_parts SET block_count = ? WHERE chunk_id = ? AND part_no = 0",
                )
                .bind(n)
                .bind(chunk_id)
                .execute(blob_db.pool())
                .await?;
            } else {
                report.notes.push(format!(
                    "orphan frames for file_id {file_id} (no blobs row); skipped"
                ));
            }
        }
    }

    if let Some(fid) = get_meta(&pool, "snapshot_file_id").await? {
        if fid.is_empty() || fid == "-" {
            report.notes.push("no snapshot_file_id meta".into());
        } else if let Some(&chunk_id) = file_to_blob.get(&fid) {
            report.roots += 1;
            report.notes.push(format!(
                "map snapshot_file_id {fid} → root s3/index chunk_id={chunk_id}"
            ));
            if !dry_run {
                let (size, _, _) = blob_db
                    .chunk_meta(chunk_id)
                    .await?
                    .unwrap_or((0, 0, 0));
                blob_db
                    .set_root(
                        "s3/index",
                        &[Extent {
                            chunk: chunk_id,
                            offset: 0,
                            len: size,
                        }],
                    )
                    .await?;
            }
        } else {
            report.notes.push(format!(
                "snapshot_file_id {fid} not found among migrated blobs; root not set"
            ));
        }
    }

    Ok(report)
}

async fn get_meta(pool: &SqlitePool, key: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
    Ok(row.map(|r| r.0))
}

/// True when a legacy s3gram index still has `blobs` rows (needs migrate → blob.db).
pub async fn legacy_index_has_blobs(legacy_database_url: &str) -> Result<bool> {
    let url = if legacy_database_url.starts_with("sqlite:") && !legacy_database_url.contains('?') {
        format!("{legacy_database_url}?mode=rwc")
    } else {
        legacy_database_url.to_string()
    };
    let pool = match SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(p) => p,
        Err(_) => return Ok(false),
    };
    let exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'blobs'",
    )
    .fetch_optional(&pool)
    .await?;
    if exists.is_none() {
        return Ok(false);
    }
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
        .fetch_one(&pool)
        .await?;
    Ok(n > 0)
}

/// Build a default telegram/memory instance for migration from legacy config fields.
pub fn default_instance_for_migrate(
    kind: InstanceKind,
    bot_token: &str,
    scope_id: &str,
) -> Result<InstanceConfig> {
    let (fingerprint, location) = match kind {
        InstanceKind::Telegram => (
            telegram_fingerprint(bot_token, scope_id)?,
            telegram_location(scope_id),
        ),
        InstanceKind::Discord => (
            format!("dc:migrate:{scope_id}"),
            format!("dc:channel:{scope_id}"),
        ),
        InstanceKind::Memory => (
            crate::instances::memory_fingerprint(),
            crate::instances::memory_location(),
        ),
    };
    Ok(InstanceConfig {
        info: InstanceInfo {
            id: "default".into(),
            kind,
            fingerprint,
            location,
            role: InstanceRole::ReadWrite,
        },
        bot_token_env: String::new(),
        bot_token: bot_token.to_string(),
        scope_id: scope_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn legacy_pool(url: &str) -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .connect(url)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE blobs (
                file_id TEXT PRIMARY KEY,
                message_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                refcount INTEGER NOT NULL,
                chat_id TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn dry_run_counts_legacy_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let idx_url = format!("sqlite:{}?mode=rwc", dir.path().join("i.db").display());
        let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("b.db").display());
        let pool = legacy_pool(&idx_url).await;
        sqlx::query(
            r#"
            INSERT INTO blobs (file_id, message_id, size, refcount, chat_id)
            VALUES ('fid-1', 42, 100, 1, '-100')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let blob_db = BlobDb::connect(&blob_url).await.unwrap();
        let inst = default_instance_for_migrate(InstanceKind::Memory, "", "local").unwrap();
        let report = migrate_index_to_blob_db(&idx_url, &blob_db, &inst, true)
            .await
            .unwrap();
        assert_eq!(report.blobs, 1);
        assert!(report.dry_run);
        assert!(blob_db.is_empty_metadata().await.unwrap());
    }

    #[tokio::test]
    async fn migrate_writes_replica_and_root() {
        let dir = tempfile::tempdir().unwrap();
        let idx_url = format!("sqlite:{}?mode=rwc", dir.path().join("i.db").display());
        let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("b.db").display());
        let pool = legacy_pool(&idx_url).await;
        sqlx::query(
            r#"
            INSERT INTO blobs (file_id, message_id, size, refcount, chat_id)
            VALUES ('snap-fid', 7, 50, 1, '')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO meta (key, value) VALUES ('snapshot_file_id', 'snap-fid')")
            .execute(&pool)
            .await
            .unwrap();

        let blob_db = BlobDb::connect(&blob_url).await.unwrap();
        let inst = default_instance_for_migrate(InstanceKind::Memory, "", "local").unwrap();
        let report = migrate_index_to_blob_db(&idx_url, &blob_db, &inst, false)
            .await
            .unwrap();
        assert_eq!(report.blobs, 1);
        assert_eq!(report.roots, 1);
        assert!(blob_db.get_root("s3/index").await.unwrap().is_some());
        let layouts = blob_db.get_replica_layouts(1).await.unwrap();
        assert_eq!(layouts.len(), 1);
        assert_eq!(layouts[0].parts.len(), 1);
    }
}
