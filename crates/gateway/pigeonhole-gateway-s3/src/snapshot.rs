//! Persist the S3 index snapshot as a chunk-store root (`s3/index`).

use crate::index::Index;
use anyhow::{Context, Result};
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use pigeonhole_chunk_store::{ChunkStore, Extent};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use tracing::info;

const META_HASH: &str = "snapshot_hash";
const META_GENERATION: &str = "snapshot_generation";
pub const ROOT_NAME: &str = "s3/index";

#[derive(Debug)]
pub enum PushOutcome {
    Unchanged { hash: String },
    Uploaded {
        hash: String,
        generation: u64,
        chunk_id: i64,
    },
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

/// Export index → gzip → `put_small` → `set_root("s3/index", …)`.
pub async fn push_index_snapshot(index: &Index, store: &ChunkStore) -> Result<PushOutcome> {
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

    let prev_root = store.get_root(ROOT_NAME).await?;
    let compressed = gzip_json(&json).context("gzip snapshot")?;
    let chunk_id = store
        .put_small(Bytes::from(compressed))
        .await
        .context("put_small s3 index snapshot")?;
    let (size, _, _) = store
        .db()
        .chunk_meta(chunk_id)
        .await?
        .context("missing snapshot chunk")?;
    let extents = vec![Extent {
        chunk: chunk_id,
        offset: 0,
        len: size,
    }];
    store.set_root(ROOT_NAME, &extents).await?;

    if let Some(old) = prev_root {
        let ids: Vec<_> = old.iter().map(|e| e.chunk).collect();
        let _ = store.release(&ids).await;
    }

    index.set_meta(META_HASH, &hash).await?;
    index
        .set_meta(META_GENERATION, &generation.to_string())
        .await?;

    info!(%hash, generation, chunk_id, "s3 index snapshot stored");
    Ok(PushOutcome::Uploaded {
        hash,
        generation,
        chunk_id,
    })
}

/// Load root `s3/index` and import into the local index.
pub async fn restore_index_snapshot(index: &Index, store: &ChunkStore) -> Result<()> {
    let extents = store
        .get_root(ROOT_NAME)
        .await?
        .with_context(|| format!("missing root {ROOT_NAME}"))?;
    let data = store
        .read(&extents, None)
        .await
        .context("read s3 index snapshot")?;
    let json = gunzip_bytes(&data).context("gunzip snapshot")?;
    let snap = serde_json::from_slice(&json).context("parse snapshot")?;
    index.import_snapshot(&snap).await.context("import snapshot")?;
    Ok(())
}
