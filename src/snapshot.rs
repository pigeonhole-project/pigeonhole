use crate::index::Index;
use crate::telegram::TelegramClient;
use anyhow::{Context, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

const META_HASH: &str = "snapshot_hash";
const META_MESSAGE_ID: &str = "snapshot_message_id";
const META_FILE_ID: &str = "snapshot_file_id";
const CAPTION: &str = "s3gram-index-snapshot";
const FILENAME: &str = "s3gram-index.json";

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
    },
}

/// Export the SQLite index to Telegram if it changed since the last push.
/// When uploading a new snapshot, deletes the previous snapshot message (best-effort).
pub async fn push_if_changed(index: &Index, tg: &TelegramClient) -> Result<PushOutcome> {
    let snap = index.export_snapshot().await.context("export snapshot")?;
    let json = serde_json::to_vec(&snap).context("serialize snapshot")?;
    let hash = hex::encode(Sha256::digest(&json));

    if let Some(prev) = index.get_meta(META_HASH).await? {
        if prev == hash {
            return Ok(PushOutcome::Unchanged { hash });
        }
    }

    let old_message_id = index
        .get_meta(META_MESSAGE_ID)
        .await?
        .and_then(|s| s.parse::<i64>().ok());

    let (file_id, message_id) = tg
        .send_document(Bytes::from(json), FILENAME, CAPTION)
        .await
        .context("upload snapshot")?;

    if let Some(old_id) = old_message_id {
        if old_id != message_id {
            if let Err(e) = tg.delete_message(old_id).await {
                warn!(old_id, error = %e, "failed to delete previous snapshot message");
            }
        }
    }

    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_MESSAGE_ID, &message_id.to_string())
        .await?;
    index.set_meta(META_FILE_ID, &file_id).await?;

    info!(
        message_id,
        replaced = ?old_message_id,
        %hash,
        "index snapshot uploaded to Telegram"
    );

    Ok(PushOutcome::Uploaded {
        hash,
        file_id,
        message_id,
        replaced_message_id: old_message_id,
    })
}

pub fn spawn_periodic(
    index: Index,
    tg: TelegramClient,
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
            match push_if_changed(&index, &tg).await {
                Ok(PushOutcome::Unchanged { .. }) => {
                    tracing::debug!("index snapshot unchanged, skip upload");
                }
                Ok(PushOutcome::Uploaded { .. }) => {}
                Err(e) => warn!(error = %e, "periodic index snapshot failed"),
            }
        }
    });
}
