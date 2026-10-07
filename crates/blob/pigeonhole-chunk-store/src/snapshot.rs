//! Index snapshots in Telegram: immutable gzip parts + pinned manifest pointer.
//!
//! Bootstrap after disk loss needs only `BOT_TOKEN` + `CHAT_ID`:
//! `getChat` → pinned manifest → download parts → verify sha256 → import SQLite.

use pigeonhole_blob::{store_delete_message, store_get, store_put, LegacyBlobStore, BootstrapPointer, DeleteOutcome, PinnedContent};
use pigeonhole_codec as chunker;
use pigeonhole_index::Index;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::Utc;
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
const META_GENERATION: &str = "snapshot_generation";
const CAPTION: &str = "pigeonhole-index-snapshot";
const FILENAME: &str = "pigeonhole-index.json.gz";
const MANIFEST_DOC_NAME: &str = "s3gram-manifest.json";
/// Bot API sendMessage text limit.
const TG_TEXT_MAX: usize = 4096;
const PENDING_DELETE_MAX_ATTEMPTS: i64 = 20;
const PENDING_DELETE_POLL_SECS: u64 = 30;
const PIN_MANIFEST_FORMAT: u32 = 1;

#[derive(Debug)]
pub enum PushOutcome {
    Unchanged {
        hash: String,
    },
    Uploaded {
        hash: String,
        /// Restore handle: pinned manifest message_id (text or document).
        message_id: i64,
        generation: u64,
        parts: usize,
    },
}

/// Pinned bootstrap manifest (immutable). Mutable state is which message is pinned.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PinManifest {
    pub format: u32,
    pub generation: u64,
    pub created_at: String,
    /// SHA-256 of the uncompressed index JSON.
    pub sha256: String,
    /// Uncompressed index JSON size in bytes.
    #[serde(default)]
    pub index_bytes: u64,
    pub parts: Vec<PinManifestPart>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PinManifestPart {
    pub message_id: i64,
    pub file_id: String,
    pub size: i64,
}

/// Legacy document-sidecar manifest (pre-pin). Still accepted when restoring by file_id.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LegacyDocManifest {
    version: u32,
    hash: String,
    parts: Vec<PinManifestPart>,
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

pub fn parse_pin_manifest(text: &str) -> Result<PinManifest> {
    let m: PinManifest = serde_json::from_str(text).context("parse pin manifest JSON")?;
    validate_pin_manifest(&m)?;
    Ok(m)
}

fn parse_pin_manifest_bytes(data: &[u8]) -> Result<PinManifest> {
    let m: PinManifest = serde_json::from_slice(data).context("parse pin manifest JSON")?;
    validate_pin_manifest(&m)?;
    Ok(m)
}

fn validate_pin_manifest(m: &PinManifest) -> Result<()> {
    if m.format != PIN_MANIFEST_FORMAT {
        bail!("unsupported pin manifest format {}", m.format);
    }
    if m.parts.is_empty() {
        bail!("pin manifest has no parts");
    }
    if m.sha256.len() != 64 || !m.sha256.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("pin manifest sha256 must be 64 hex chars");
    }
    Ok(())
}

/// Export the SQLite index when it changed: upload parts, pin a new immutable manifest,
/// unpin/delete the previous generation.
pub async fn push_if_changed(
    index: &Index,
    store: &dyn LegacyBlobStore,
    pin: &dyn BootstrapPointer,
    chunk_size: usize,
) -> Result<PushOutcome> {
    let max_part = chunk_size.clamp(1, chunker::MAX_CHUNK_SIZE);
    let snap = index.export_snapshot().await.context("export snapshot")?;
    let json = serde_json::to_vec(&snap).context("serialize snapshot")?;
    let hash = hex::encode(Sha256::digest(&json));

    if let Some(prev) = index.get_meta(META_HASH).await? {
        if prev == hash {
            return Ok(PushOutcome::Unchanged { hash });
        }
    }

    let generation = index
        .get_meta(META_GENERATION)
        .await?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1);

    let compressed = gzip_json(&json).context("gzip snapshot")?;
    let old_message_ids = load_old_message_ids(index).await?;

    let mut parts: Vec<PinManifestPart> = Vec::new();
    if compressed.len() <= max_part {
        let (file_id, message_id) = store_put(store, Bytes::from(compressed), FILENAME, CAPTION)
            .await
            .context("upload snapshot")?;
        parts.push(PinManifestPart {
            file_id,
            message_id,
            size: 0,
        });
    } else {
        for (i, chunk) in compressed.chunks(max_part).enumerate() {
            let name = format!("pigeonhole-index-{i:04}.json.gz.part");
            let (file_id, message_id) = store_put(store, Bytes::copy_from_slice(chunk), &name, CAPTION)
                .await
                .with_context(|| format!("upload snapshot part {i}"))?;
            parts.push(PinManifestPart {
                file_id,
                message_id,
                size: chunk.len() as i64,
            });
        }
    }

    let manifest = PinManifest {
        format: PIN_MANIFEST_FORMAT,
        generation,
        created_at: Utc::now().to_rfc3339(),
        sha256: hash.clone(),
        index_bytes: json.len() as u64,
        parts: parts.clone(),
    };
    let manifest_body = serde_json::to_string(&manifest)?;

    // Prefer a text message (easy getChat → pinned_message.text). Fall back to a
    // tiny document if the JSON ever exceeds Telegram's text limit.
    let (manifest_message_id, manifest_file_id) = if manifest_body.len() <= TG_TEXT_MAX {
        let mid = pin
            .send_text(&manifest_body)
            .await
            .context("send pin manifest text")?;
        (mid, String::new())
    } else {
        let (file_id, mid) = store_put(
            store,
            Bytes::from(manifest_body.into_bytes()),
            MANIFEST_DOC_NAME,
            CAPTION,
        )
            .await
            .context("upload pin manifest document")?;
        (mid, file_id)
    };

    // Atomic-ish pointer swap: pin new, then unpin/delete old.
    pin.pin_message(manifest_message_id)
        .await
        .context("pin new snapshot manifest")?;

    for old_id in &old_message_ids {
        if parts.iter().any(|p| p.message_id == *old_id) || *old_id == manifest_message_id {
            continue;
        }
        if let Err(e) = pin.unpin_message(*old_id).await {
            debug_unpin_err(*old_id, &e);
        }
        match store_delete_message(store, *old_id).await {
            Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
            Ok(DeleteOutcome::Failed) => {
                let _ = index.queue_tg_delete(pin.scope_id(), *old_id).await;
            }
            Err(e) => {
                warn!(old_id, error = %e, "failed to delete previous snapshot message");
                let _ = index.queue_tg_delete(pin.scope_id(), *old_id).await;
            }
        }
    }

    let parts_json = serde_json::to_string(&parts)?;
    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_MESSAGE_ID, &manifest_message_id.to_string())
        .await?;
    index
        .set_meta(
            META_FILE_ID,
            if manifest_file_id.is_empty() {
                // Text manifest: restore handle is pin, not a file_id.
                parts.first().map(|p| p.file_id.as_str()).unwrap_or("")
            } else {
                &manifest_file_id
            },
        )
        .await?;
    index.set_meta(META_PARTS, &parts_json).await?;
    index
        .set_meta(META_GENERATION, &generation.to_string())
        .await?;

    info!(
        message_id = manifest_message_id,
        generation,
        parts = parts.len(),
        %hash,
        "index snapshot pinned in Telegram"
    );

    Ok(PushOutcome::Uploaded {
        hash,
        message_id: manifest_message_id,
        generation,
        parts: parts.len(),
    })
}

fn debug_unpin_err(old_id: i64, e: &anyhow::Error) {
    tracing::debug!(old_id, error = %e, "unpin previous manifest (ignored)");
}

async fn load_old_message_ids(index: &Index) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    if let Some(parts) = index.get_meta(META_PARTS).await? {
        if let Ok(v) = serde_json::from_str::<Vec<PinManifestPart>>(&parts) {
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

async fn download_parts(store: &dyn LegacyBlobStore, parts: &[PinManifestPart]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for p in parts {
        let chunk = store_get(store, &p.file_id)
            .await
            .with_context(|| format!("download snapshot part file_id={}", p.file_id))?;
        buf.extend_from_slice(&chunk);
    }
    gunzip_bytes(&buf)
}

fn verify_sha256(json: &[u8], expected_hex: &str) -> Result<()> {
    let got = hex::encode(Sha256::digest(json));
    if got != expected_hex {
        bail!("snapshot sha256 mismatch: expected {expected_hex}, got {got}");
    }
    Ok(())
}

/// Result of bootstrapping from the chat pin (bytes + pointer to record in meta).
pub struct PinnedRestore {
    pub bytes: Vec<u8>,
    pub manifest_message_id: i64,
    pub manifest_file_id: String,
    pub manifest: PinManifest,
}

/// Bootstrap from the chat's pinned manifest (no local meta / file_id required).
pub async fn download_from_pinned(
    pin: &dyn BootstrapPointer,
    store: &dyn LegacyBlobStore,
) -> Result<PinnedRestore> {
    let scope = pin.scope_id().to_string();
    let pinned = pin
        .get_pinned()
        .await
        .context("get pinned content")?
        .ok_or_else(|| anyhow::anyhow!("chat {scope} has no pinned message (run a snapshot first)"))?;

    let (manifest_message_id, manifest_file_id, manifest, legacy_bytes) = match pinned {
        PinnedContent::Text { text, message_id } => {
            info!(message_id, "using pinned text manifest");
            (message_id, String::new(), parse_pin_manifest(&text)?, None)
        }
        PinnedContent::Document {
            file_id,
            message_id,
        } => {
            info!(message_id, %file_id, "using pinned document manifest");
            let data = store_get(store, &file_id).await.context("download pinned manifest")?;
            if let Ok(m) = parse_pin_manifest_bytes(&data) {
                (message_id, file_id, m, None)
            } else if let Ok(legacy) = serde_json::from_slice::<LegacyDocManifest>(&data) {
                if legacy.hash.len() != 64 || !legacy.hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    bail!("legacy document manifest hash must be 64 hex chars");
                }
                if legacy.parts.is_empty() {
                    bail!("legacy document manifest has no parts");
                }
                (
                    message_id,
                    file_id,
                    PinManifest {
                        format: PIN_MANIFEST_FORMAT,
                        generation: 0,
                        created_at: String::new(),
                        sha256: legacy.hash,
                        index_bytes: 0,
                        parts: legacy.parts,
                    },
                    None,
                )
            } else if data.starts_with(&[0x1f, 0x8b]) {
                // Single gzip document pinned (very old) — treat as payload.
                // No separate integrity hash in this legacy form.
                (
                    message_id,
                    file_id,
                    PinManifest {
                        format: PIN_MANIFEST_FORMAT,
                        generation: 0,
                        created_at: String::new(),
                        sha256: String::new(),
                        index_bytes: 0,
                        parts: Vec::new(),
                    },
                    Some(gunzip_bytes(&data)?),
                )
            } else {
                bail!(
                    "pinned document is neither a pin manifest, legacy manifest, nor gzip snapshot"
                );
            }
        }
    };

    if let Some(bytes) = legacy_bytes {
        return Ok(PinnedRestore {
            bytes,
            manifest_message_id,
            manifest_file_id,
            manifest,
        });
    }

    info!(
        generation = manifest.generation,
        parts = manifest.parts.len(),
        "downloading snapshot parts from pin manifest"
    );
    let json = download_parts(store, &manifest.parts).await?;
    verify_sha256(&json, &manifest.sha256)?;
    Ok(PinnedRestore {
        bytes: json,
        manifest_message_id,
        manifest_file_id,
        manifest,
    })
}

/// Persist pin pointer so the next `push_if_changed` can unpin/delete this generation.
pub async fn record_restored_pin(index: &Index, restored: &PinnedRestore) -> Result<()> {
    let parts_json = serde_json::to_string(&restored.manifest.parts)?;
    if !restored.manifest.sha256.is_empty() {
        index.set_meta(META_HASH, &restored.manifest.sha256).await?;
    }
    index
        .set_meta(META_MESSAGE_ID, &restored.manifest_message_id.to_string())
        .await?;
    let file_id = if restored.manifest_file_id.is_empty() {
        restored
            .manifest
            .parts
            .first()
            .map(|p| p.file_id.as_str())
            .unwrap_or("")
    } else {
        restored.manifest_file_id.as_str()
    };
    index.set_meta(META_FILE_ID, file_id).await?;
    index.set_meta(META_PARTS, &parts_json).await?;
    index
        .set_meta(
            META_GENERATION,
            &restored.manifest.generation.to_string(),
        )
        .await?;
    Ok(())
}

/// Download snapshot bytes.
///
/// `file_id_hint` may be:
/// - a single gzip snapshot document
/// - a legacy document manifest listing part file_ids
/// - omitted → use local meta (same machine)
pub async fn download_snapshot_bytes(
    store: &dyn LegacyBlobStore,
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

    let data = store_get(store, &fid).await?;

    if let Ok(manifest) = serde_json::from_slice::<PinManifest>(&data) {
        if manifest.format == PIN_MANIFEST_FORMAT && !manifest.parts.is_empty() {
            let json = download_parts(store, &manifest.parts).await?;
            verify_sha256(&json, &manifest.sha256)?;
            return Ok(json);
        }
    }

    // Legacy document manifest.
    if let Ok(manifest) = serde_json::from_slice::<LegacyDocManifest>(&data) {
        if manifest.version == 1 && !manifest.parts.is_empty() {
            let json = download_parts(store, &manifest.parts).await?;
            if manifest.hash.len() == 64 {
                verify_sha256(&json, &manifest.hash)?;
            }
            return Ok(json);
        }
    }

    // Same-machine multi-part via local meta (legacy path without manifest handle).
    if file_id_hint.is_none() {
        if let Some(parts_json) = index.get_meta(META_PARTS).await? {
            if let Ok(parts) = serde_json::from_str::<Vec<PinManifestPart>>(&parts_json) {
                if parts.len() > 1 {
                    return download_parts(store, &parts).await;
                }
            }
        }
    }

    gunzip_bytes(&data)
}

pub fn spawn_periodic(
    index: Index,
    store: Arc<dyn LegacyBlobStore>,
    pin: Arc<dyn BootstrapPointer>,
    gate: Arc<Mutex<()>>,
    interval_secs: u64,
    chunk_size: usize,
) {
    if interval_secs == 0 {
        info!("periodic index snapshots disabled (snapshot.interval_secs=0)");
        return;
    }
    tokio::spawn(async move {
        let period = std::time::Duration::from_secs(interval_secs);
        info!(
            interval_secs,
            chunk_size, "periodic index snapshots enabled (pin manifest)"
        );
        loop {
            tokio::time::sleep(period).await;
            let _guard = gate.lock().await;
            match push_if_changed(&index, store.as_ref(), pin.as_ref(), chunk_size).await {
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
pub fn spawn_pending_deletes(index: Index, store: Arc<dyn LegacyBlobStore>) {
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
                match store_delete_message(store.as_ref(), message_id).await {
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

/// Stage 2.2: store a gateway index snapshot via [`ChunkStore`] + durable root.
///
/// Does **not** pin a Telegram/Discord message itself — the blob-layer superblock
/// (`commit_root`) is the single commit point for roots.
pub async fn push_gateway_snapshot(
    index: &Index,
    layer: &crate::layer::ChunkStore,
    dur: &crate::durability::Durability,
    root_name: &str,
) -> Result<PushOutcome> {
    let snap = index.export_snapshot().await.context("export snapshot")?;
    let json = serde_json::to_vec(&snap).context("serialize snapshot")?;
    let hash = hex::encode(Sha256::digest(&json));

    if let Some(prev) = index.get_meta(META_HASH).await? {
        if prev == hash {
            return Ok(PushOutcome::Unchanged { hash });
        }
    }

    let generation = index
        .get_meta(META_GENERATION)
        .await?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1);

    let compressed = gzip_json(&json).context("gzip snapshot")?;
    let chunk_id = layer
        .put_small(Bytes::from(compressed))
        .await
        .context("put_small gateway snapshot")?;
    crate::durability::commit_root(layer.db(), dur, root_name, chunk_id)
        .await
        .context("commit_root gateway snapshot")?;

    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_GENERATION, &generation.to_string())
        .await?;
    index
        .set_meta(META_FILE_ID, &format!("blob:{chunk_id}"))
        .await?;
    index
        .set_meta(META_MESSAGE_ID, "0")
        .await?;

    info!(%hash, generation, %root_name, chunk_id, "gateway snapshot stored via blob layer");
    Ok(PushOutcome::Uploaded {
        hash,
        message_id: 0,
        generation,
        parts: 1,
    })
}

/// Load a gateway snapshot blob referenced by root and import into `index`.
pub async fn restore_gateway_snapshot(
    index: &Index,
    layer: &crate::layer::ChunkStore,
    root_name: &str,
) -> Result<()> {
    let chunk_id = layer
        .get_root(root_name)
        .await?
        .with_context(|| format!("missing root {root_name}"))?;
    let data = layer
        .read(
            &[crate::layer::ChunkRef { chunk_id }],
            None,
        )
        .await
        .context("read gateway snapshot blob")?;
    let json = gunzip_bytes(&data).context("gunzip gateway snapshot")?;
    let snap: pigeonhole_index::IndexSnapshot =
        serde_json::from_slice(&json).context("parse gateway snapshot")?;
    index.import_snapshot(&snap).await.context("import snapshot")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_manifest_roundtrip() {
        let m = PinManifest {
            format: 1,
            generation: 7,
            created_at: "2026-01-01T00:00:00Z".into(),
            sha256: "a".repeat(64),
            index_bytes: 42,
            parts: vec![PinManifestPart {
                message_id: 9,
                file_id: "BQACAg".into(),
                size: 100,
            }],
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.len() < TG_TEXT_MAX);
        let parsed = parse_pin_manifest(&s).unwrap();
        assert_eq!(parsed, m);
    }

    #[test]
    fn parse_rejects_bad_format() {
        let s = r#"{"format":99,"generation":1,"created_at":"x","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","parts":[{"message_id":1,"file_id":"f","size":1}]}"#;
        assert!(parse_pin_manifest(s).is_err());
    }

    #[test]
    fn parse_rejects_empty_or_short_sha256() {
        let s = r#"{"format":1,"generation":1,"created_at":"x","sha256":"","parts":[{"message_id":1,"file_id":"f","size":1}]}"#;
        assert!(parse_pin_manifest(s).is_err());
        let bytes = br#"{"format":1,"generation":1,"created_at":"x","sha256":"abcd","parts":[{"message_id":1,"file_id":"f","size":1}]}"#;
        assert!(parse_pin_manifest_bytes(bytes).is_err());
    }

    #[tokio::test]
    async fn gateway_snapshot_via_layer_roundtrip() {
        use crate::durability::{Durability, Superblock};
        use crate::ingest::IngestOptions;
        use crate::layer::ChunkStore;
        use async_trait::async_trait;
        use pigeonhole_blob::TypedBootstrapPointer;
        use pigeonhole_codec::ChunkCodec;
        use pigeonhole_storage_memory::MemoryBlobStore;
        use std::sync::{Arc, Mutex as StdMutex};

        struct MemPin {
            data: StdMutex<Option<Bytes>>,
        }
        #[async_trait]
        impl TypedBootstrapPointer for MemPin {
            async fn read(&self) -> Result<Option<Bytes>> {
                Ok(self.data.lock().unwrap().clone())
            }
            async fn swap(&self, new: Bytes) -> Result<()> {
                *self.data.lock().unwrap() = Some(new);
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
        let idx_url = format!("sqlite:{}?mode=rwc", dir.path().join("s3.db").display());
        let db = crate::BlobDb::connect(&blob_url).await.unwrap();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open(db, MemoryBlobStore::new(), opts)
            .await
            .unwrap();
        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let info_id = layer.write_instance_id().to_string();
        let fp = layer.write_backend().instance().fingerprint.clone();
        let dur = Durability::new(
            layer.write_backend(),
            pin,
            Superblock::new(0, info_id, fp),
        );
        dur.checkpoint(layer.db()).await.unwrap();

        let index = Index::connect(&idx_url).await.unwrap();
        index.create_bucket("demo", "").await.unwrap();

        let out = push_gateway_snapshot(&index, &layer, &dur, "s3/index")
            .await
            .unwrap();
        assert!(matches!(out, PushOutcome::Uploaded { .. }));

        let idx2_url = format!("sqlite:{}?mode=rwc", dir.path().join("s3b.db").display());
        let index2 = Index::connect(&idx2_url).await.unwrap();
        restore_gateway_snapshot(&index2, &layer, "s3/index")
            .await
            .unwrap();
        let buckets = index2.list_buckets().await.unwrap();
        assert!(buckets.iter().any(|b| b.name == "demo"));
    }
}
