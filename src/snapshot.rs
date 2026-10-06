use crate::index::Index;
use crate::telegram::TelegramClient;
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
/// Keep under Telegram getFile limit (20 MiB) with margin.
const MAX_PART: usize = 19 * 1024 * 1024;

#[derive(Debug)]
pub enum PushOutcome {
    Unchanged {
        hash: String,
    },
    Uploaded {
        hash: String,
        file_id: String,
        message_id: i64,
        replaced_message_id: Option<i64>,
        parts: usize,
    },
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
/// Snapshot is gzip-compressed; split into ≤19 MiB parts when needed so getFile can restore.
pub async fn push_if_changed(
    index: &Index,
    tg: &TelegramClient,
    service_chat_id: &str,
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

    let mut parts: Vec<(String, i64, i64)> = Vec::new();
    if compressed.len() <= MAX_PART {
        let (file_id, message_id) = tg
            .send_document(
                service_chat_id,
                Bytes::from(compressed),
                FILENAME,
                CAPTION,
            )
            .await
            .context("upload snapshot")?;
        parts.push((file_id, message_id, 0));
    } else {
        for (i, chunk) in compressed.chunks(MAX_PART).enumerate() {
            let name = format!("s3gram-index-{i:04}.json.gz.part");
            let (file_id, message_id) = tg
                .send_document(
                    service_chat_id,
                    Bytes::copy_from_slice(chunk),
                    &name,
                    CAPTION,
                )
                .await
                .with_context(|| format!("upload snapshot part {i}"))?;
            parts.push((file_id, message_id, chunk.len() as i64));
        }
    }

    for old_id in &old_message_ids {
        if parts.iter().any(|(_, mid, _)| mid == old_id) {
            continue;
        }
        if let Err(e) = tg.delete_message(service_chat_id, *old_id).await {
            warn!(old_id, error = %e, "failed to delete previous snapshot message");
        }
    }

    let primary = &parts[0];
    let parts_json = serde_json::to_string(
        &parts
            .iter()
            .map(|(f, m, s)| serde_json::json!({"file_id": f, "message_id": m, "size": s}))
            .collect::<Vec<_>>(),
    )?;

    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_MESSAGE_ID, &primary.1.to_string())
        .await?;
    index.set_meta(META_FILE_ID, &primary.0).await?;
    index.set_meta(META_PARTS, &parts_json).await?;

    info!(
        message_id = primary.1,
        parts = parts.len(),
        %hash,
        "index snapshot uploaded to Telegram"
    );

    Ok(PushOutcome::Uploaded {
        hash,
        file_id: primary.0.clone(),
        message_id: primary.1,
        replaced_message_id: old_message_ids.first().copied(),
        parts: parts.len(),
    })
}

async fn load_old_message_ids(index: &Index) -> Result<Vec<i64>> {
    if let Some(parts) = index.get_meta(META_PARTS).await? {
        if let Ok(v) = serde_json::from_str::<Vec<serde_json::Value>>(&parts) {
            return Ok(v
                .iter()
                .filter_map(|p| p.get("message_id").and_then(|x| x.as_i64()))
                .collect());
        }
    }
    Ok(index
        .get_meta(META_MESSAGE_ID)
        .await?
        .and_then(|s| s.parse().ok())
        .into_iter()
        .collect())
}

/// Download snapshot bytes from one file_id or a multi-part meta list.
pub async fn download_snapshot_bytes(
    tg: &TelegramClient,
    index: &Index,
    file_id_hint: Option<&str>,
) -> Result<Vec<u8>> {
    if let Some(parts_json) = index.get_meta(META_PARTS).await? {
        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(&parts_json) {
            if parts.len() > 1 {
                let mut buf = Vec::new();
                for p in parts {
                    let fid = p
                        .get("file_id")
                        .and_then(|x| x.as_str())
                        .ok_or_else(|| anyhow::anyhow!("bad snapshot part"))?;
                    let chunk = tg.download_file(fid).await?;
                    buf.extend_from_slice(&chunk);
                }
                return gunzip_bytes(&buf);
            }
        }
    }
    let fid = match file_id_hint {
        Some(f) => f.to_string(),
        None => index
            .get_meta(META_FILE_ID)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no snapshot file_id"))?,
    };
    let data = tg.download_file(&fid).await?;
    gunzip_bytes(&data)
}

pub fn spawn_periodic(
    index: Index,
    tg: TelegramClient,
    service_chat_id: String,
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
            match push_if_changed(&index, &tg, &service_chat_id).await {
                Ok(PushOutcome::Unchanged { .. }) => {
                    tracing::debug!("index snapshot unchanged, skip upload");
                }
                Ok(PushOutcome::Uploaded { .. }) => {}
                Err(e) => warn!(error = %e, "periodic index snapshot failed"),
            }
            // Best-effort retry of Telegram deletes that previously failed.
            if let Ok(pending) = index.list_pending_tg_deletes(50).await {
                for (chat_id, message_id) in pending {
                    match tg.delete_message(&chat_id, message_id).await {
                        Ok(true) => {
                            let _ = index.clear_pending_tg_delete(&chat_id, message_id).await;
                        }
                        Ok(false) => {}
                        Err(e) => warn!(error = %e, chat_id, message_id, "pending delete retry failed"),
                    }
                }
            }
        }
    });
}
