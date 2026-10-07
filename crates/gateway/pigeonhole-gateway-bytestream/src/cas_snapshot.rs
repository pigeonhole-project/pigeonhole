//! Persist the CAS index snapshot as chunk-store root (`cas/index`).

use crate::cas_index::CasIndex;
use anyhow::{Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use pigeonhole_chunk_store::{ChunkStore, Extent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use tracing::info;

const META_HASH: &str = "cas_snapshot_hash";
const META_GENERATION: &str = "cas_snapshot_generation";
pub const ROOT_NAME: &str = "cas/index";

#[derive(Serialize, Deserialize)]
struct CasSnapshot {
    entries: Vec<crate::cas_index::CasEntry>,
}

fn gzip_json(json: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(json)?;
    Ok(enc.finish()?)
}

fn gunzip_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let mut dec = GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out)?;
    Ok(out)
}

pub async fn push_cas_snapshot(cas: &CasIndex, store: &ChunkStore) -> Result<()> {
    let entries = cas.export_entries().await?;
    let snap = CasSnapshot { entries };
    let json = serde_json::to_vec(&snap)?;
    let hash = hex::encode(Sha256::digest(&json));
    if let Some(prev) = cas.get_meta(META_HASH).await? {
        if prev == hash {
            return Ok(());
        }
    }
    let generation = cas
        .get_meta(META_GENERATION)
        .await?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1);

    let prev = store.get_root(ROOT_NAME).await?;
    let compressed = gzip_json(&json)?;
    let chunk_id = store.put_small(Bytes::from(compressed)).await?;
    let (size, _, _) = store
        .db()
        .chunk_meta(chunk_id)
        .await?
        .context("missing cas snapshot chunk")?;
    store
        .set_root(
            ROOT_NAME,
            &[Extent {
                chunk: chunk_id,
                offset: 0,
                len: size,
            }],
        )
        .await?;
    if let Some(old) = prev {
        let ids: Vec<_> = old.iter().map(|e| e.chunk).collect();
        let _ = store.release(&ids).await;
    }
    cas.set_meta(META_HASH, &hash).await?;
    cas.set_meta(META_GENERATION, &generation.to_string())
        .await?;
    info!(%hash, generation, chunk_id, "cas index snapshot stored");
    Ok(())
}

pub async fn restore_cas_snapshot(cas: &CasIndex, store: &ChunkStore) -> Result<()> {
    let extents = store
        .get_root(ROOT_NAME)
        .await?
        .with_context(|| format!("missing root {ROOT_NAME}"))?;
    let data = store.read(&extents, None).await?;
    let json = gunzip_bytes(&data)?;
    let snap: CasSnapshot = serde_json::from_slice(&json)?;
    cas.import_entries(&snap.entries).await?;
    Ok(())
}
