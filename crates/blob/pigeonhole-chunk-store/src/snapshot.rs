//! Legacy Telegram pin-manifest helpers (bootstrap after disk loss).
//!
//! Gateway index snapshots themselves are stored via [`ChunkStore::set_root`]
//! (`s3/index`, `cas/index`) in the gateway crates (stage F).

use pigeonhole_blob::{store_get, LegacyBlobStore, BootstrapPointer, PinnedContent};

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::io::Read;
use tracing::info;

const PIN_MANIFEST_FORMAT: u32 = 1;

/// Pinned bootstrap manifest (immutable). Mutable state is which message is pinned.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PinManifest {
    pub format: u32,
    pub generation: u64,
    pub created_at: String,
    pub sha256: String,
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

#[derive(Debug, serde::Deserialize)]
struct LegacyDocManifest {
    #[serde(default)]
    version: u32,
    hash: String,
    parts: Vec<PinManifestPart>,
}

pub fn gunzip_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)?;
    Ok(out)
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
    if expected_hex.is_empty() {
        return Ok(());
    }
    let got = hex::encode(Sha256::digest(json));
    if got != expected_hex {
        bail!("snapshot sha256 mismatch: expected {expected_hex}, got {got}");
    }
    Ok(())
}

/// Result of bootstrapping from the chat pin.
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
            let data = store_get(store, &file_id)
                .await
                .context("download pinned manifest")?;
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

/// Download a gzip snapshot (or multi-part pin/legacy manifest) by `file_id`.
pub async fn download_snapshot_bytes(
    store: &dyn LegacyBlobStore,
    file_id: &str,
) -> Result<Vec<u8>> {
    let data = store_get(store, file_id).await?;

    if let Ok(manifest) = serde_json::from_slice::<PinManifest>(&data) {
        if manifest.format == PIN_MANIFEST_FORMAT && !manifest.parts.is_empty() {
            let json = download_parts(store, &manifest.parts).await?;
            verify_sha256(&json, &manifest.sha256)?;
            return Ok(json);
        }
    }

    if let Ok(manifest) = serde_json::from_slice::<LegacyDocManifest>(&data) {
        if manifest.version == 1 && !manifest.parts.is_empty() {
            let json = download_parts(store, &manifest.parts).await?;
            if manifest.hash.len() == 64 {
                verify_sha256(&json, &manifest.hash)?;
            }
            return Ok(json);
        }
    }

    gunzip_bytes(&data)
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
}
