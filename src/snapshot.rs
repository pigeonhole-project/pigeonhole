use crate::index::Index;
use crate::storage::{BlobStore, DeleteOutcome};
use anyhow::{Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

const META_HASH: &str = "snapshot_hash";
const META_MESSAGE_ID: &str = "snapshot_message_id";
const META_FILE_ID: &str = "snapshot_file_id";
const META_PARTS: &str = "snapshot_parts";
const CAPTION: &str = "s3gram-index-snapshot";
const FILENAME: &str = "s3gram-index.json.gz";
const MANIFEST_NAME: &str = "s3gram-index.manifest.json";
/// Keep under Telegram getFile limit (20 MiB) with margin.
const MAX_PART: usize = 19 * 1024 * 1024;
const PENDING_DELETE_MAX_ATTEMPTS: i64 = 20;
const PENDING_DELETE_POLL_SECS: u64 = 30;

#[derive(Debug)]
pub enum PushOutcome {
    Unchanged {
        hash: String,
    },
    Uploaded {
        hash: String,
        /// file_id of the restore handle: single gzip part, or the manifest document.
        file_id: String,
        message_id: i64,
        replaced_message_id: Option<i64>,
        parts: usize,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotManifest {
    version: u32,
    hash: String,
    /// Ordered gzip part file_ids (concatenated = full gzip stream).
    parts: Vec<SnapshotPart>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotPart {
    file_id: String,
    message_id: i64,
    size: i64,
}

fn gzip_json(json: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(json)?;
    Ok(enc.finish()?)
}

pub fn gunzip_bytes(data: &[u8]) -> Result<Vec<u8>> {
    if data.starts_with(&[0x1f, 0x8b]) {
        let mut dec = GzDecoder::new(data);
        let mut out = Vec::new();
        dec.read_to_end(&mut out)?;
        Ok(out)
    } else {
        // Legacy uncompressed JSON snapshots.
        Ok(data.to_vec())
    }
}

/// Export the SQLite index to the service Telegram chat if it changed.
/// Multi-part snapshots upload a small manifest document whose file_id is the restore handle.
pub async fn push_if_changed(
    index: &Index,
    store: &dyn BlobStore,
) -> Result<PushOutcome> {
    let snap = index.export_snapshot().await.context("export snapshot")?;
    let json = serde_json::to_vec(&snap).context("serialize snapshot")?;
    let hash = hex::encode(Sha256::digest(&json));

    if let Some(prev) = index.get_meta(META_HASH).await? {
        if prev == hash {
            return Ok(PushOutcome::Unchanged { hash });
        }
    }

    let compressed = gzip_json(&json).context("gzip snapshot")?;
    let old_message_ids = load_old_message_ids(index).await?;

    let mut parts: Vec<SnapshotPart> = Vec::new();
    if compressed.len() <= MAX_PART {
        let (file_id, message_id) = store
            .put(Bytes::from(compressed), FILENAME, CAPTION)
            .await
            .context("upload snapshot")?;
        parts.push(SnapshotPart {
            file_id,
            message_id,
            size: 0,
        });
    } else {
        for (i, chunk) in compressed.chunks(MAX_PART).enumerate() {
            let name = format!("s3gram-index-{i:04}.json.gz.part");
            let (file_id, message_id) = store
                .put(Bytes::copy_from_slice(chunk), &name, CAPTION)
                .await
                .with_context(|| format!("upload snapshot part {i}"))?;
            parts.push(SnapshotPart {
                file_id,
                message_id,
                size: chunk.len() as i64,
            });
        }
    }

    // Restore handle: single part's file_id, or a tiny manifest listing all part file_ids.
    let (handle_file_id, handle_message_id) = if parts.len() == 1 {
        (parts[0].file_id.clone(), parts[0].message_id)
    } else {
        let manifest = SnapshotManifest {
            version: 1,
            hash: hash.clone(),
            parts: parts.clone(),
        };
        let body = serde_json::to_vec(&manifest)?;
        let (file_id, message_id) = store
            .put(Bytes::from(body), MANIFEST_NAME, CAPTION)
            .await
            .context("upload snapshot manifest")?;
        (file_id, message_id)
    };

    for old_id in &old_message_ids {
        if parts.iter().any(|p| p.message_id == *old_id) || *old_id == handle_message_id {
            continue;
        }
        match store.delete_message(*old_id).await {
            Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
            Ok(DeleteOutcome::Failed) => {
                let _ = index.queue_tg_delete("", *old_id).await;
            }
            Err(e) => {
                warn!(old_id, error = %e, "failed to delete previous snapshot message");
                let _ = index.queue_tg_delete("", *old_id).await;
            }
        }
    }

    let parts_json = serde_json::to_string(&parts)?;
    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_MESSAGE_ID, &handle_message_id.to_string())
        .await?;
    index.set_meta(META_FILE_ID, &handle_file_id).await?;
    index.set_meta(META_PARTS, &parts_json).await?;

    info!(
        message_id = handle_message_id,
        parts = parts.len(),
        %hash,
        "index snapshot uploaded to Telegram"
    );

    Ok(PushOutcome::Uploaded {
        hash,
        file_id: handle_file_id,
        message_id: handle_message_id,
        replaced_message_id: old_message_ids.first().copied(),
        parts: parts.len(),
    })
}

async fn load_old_message_ids(index: &Index) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    if let Some(parts) = index.get_meta(META_PARTS).await? {
        if let Ok(v) = serde_json::from_str::<Vec<SnapshotPart>>(&parts) {
            ids.extend(v.into_iter().map(|p| p.message_id));
        } else if let Ok(v) = serde_json::from_str::<Vec<serde_json::Value>>(&parts) {
            ids.extend(
                v.iter()
                    .filter_map(|p| p.get("message_id").and_then(|x| x.as_i64())),
            );
        }
    }
    if let Some(mid) = index
        .get_meta(META_MESSAGE_ID)
        .await?
        .and_then(|s| s.parse().ok())
    {
        if !ids.contains(&mid) {
            ids.push(mid);
        }
    }
    Ok(ids)
}

/// Download snapshot bytes.
///
/// `file_id_hint` may be:
/// - a single gzip snapshot document
/// - a manifest JSON listing part file_ids (clean-machine restore)
/// - omitted → use local meta (same machine)
pub async fn download_snapshot_bytes(
    store: &dyn BlobStore,
    index: &Index,
    file_id_hint: Option<&str>,
) -> Result<Vec<u8>> {
    let fid = match file_id_hint {
        Some(f) => f.to_string(),
        None => index
            .get_meta(META_FILE_ID)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no snapshot file_id"))?,
    };

    let data = store.get(&fid).await?;

    // Manifest (multi-part restore handle).
    if let Ok(manifest) = serde_json::from_slice::<SnapshotManifest>(&data) {
        if manifest.version == 1 && !manifest.parts.is_empty() {
            let mut buf = Vec::new();
            for p in &manifest.parts {
                let chunk = store.get(&p.file_id).await?;
                buf.extend_from_slice(&chunk);
            }
            return gunzip_bytes(&buf);
        }
    }

    // Same-machine multi-part via local meta (legacy path without manifest handle).
    if file_id_hint.is_none() {
        if let Some(parts_json) = index.get_meta(META_PARTS).await? {
            if let Ok(parts) = serde_json::from_str::<Vec<SnapshotPart>>(&parts_json) {
                if parts.len() > 1 {
                    let mut buf = Vec::new();
                    for p in &parts {
                        let chunk = store.get(&p.file_id).await?;
                        buf.extend_from_slice(&chunk);
                    }
                    return gunzip_bytes(&buf);
                }
            }
        }
    }

    gunzip_bytes(&data)
}

pub fn spawn_periodic(
    index: Index,
    store: std::sync::Arc<dyn BlobStore>,
    gate: Arc<Mutex<()>>,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        info!("periodic index snapshots disabled (SNAPSHOT_INTERVAL_SECS=0)");
        return;
    }
    tokio::spawn(async move {
        let period = std::time::Duration::from_secs(interval_secs);
        info!(interval_secs, "periodic index snapshots enabled");
        loop {
            tokio::time::sleep(period).await;
            let _guard = gate.lock().await;
            match push_if_changed(&index, store.as_ref()).await {
                Ok(PushOutcome::Unchanged { .. }) => {
                    tracing::debug!("index snapshot unchanged, skip upload");
                }
                Ok(PushOutcome::Uploaded { .. }) => {}
                Err(e) => warn!(error = %e, "periodic index snapshot failed"),
            }
        }
    });
}

/// Independent of snapshot interval — always drains pending deletes.
pub fn spawn_pending_deletes(index: Index, store: std::sync::Arc<dyn BlobStore>) {
    tokio::spawn(async move {
        info!(
            poll_secs = PENDING_DELETE_POLL_SECS,
            max_attempts = PENDING_DELETE_MAX_ATTEMPTS,
            "pending delete worker enabled"
        );
        let period = std::time::Duration::from_secs(PENDING_DELETE_POLL_SECS);
        loop {
            tokio::time::sleep(period).await;
            if let Ok(n) = index
                .drop_exhausted_pending_tg_deletes(PENDING_DELETE_MAX_ATTEMPTS)
                .await
            {
                if n > 0 {
                    warn!(n, "dropped exhausted pending deletes");
                }
            }
            let Ok(pending) = index
                .list_pending_tg_deletes(50, PENDING_DELETE_MAX_ATTEMPTS)
                .await
            else {
                continue;
            };
            for (chat_id, message_id, _attempts) in pending {
                match store.delete_message(message_id).await {
                    Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {
                        let _ = index.clear_pending_tg_delete(&chat_id, message_id).await;
                    }
                    Ok(DeleteOutcome::Failed) => {
                        let _ = index
                            .bump_pending_tg_delete(&chat_id, message_id, "delete not confirmed")
                            .await;
                    }
                    Err(e) => {
                        warn!(error = %e, message_id, "pending delete retry failed");
                        let _ = index
                            .bump_pending_tg_delete(&chat_id, message_id, &e.to_string())
                            .await;
                    }
                }
            }
        }
    });
}
