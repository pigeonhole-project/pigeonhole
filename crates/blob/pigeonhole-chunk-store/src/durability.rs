//! Journal segments + superblock for `blob.db` durability (stage E.5).
//!
//! Segments and checkpoints are stored as Replicated parts; the superblock
//! keeps per-instance locator lists and is published to every read-write pin.

use crate::blob_db::{BlobDb, ChunkId, Extent};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, EncodedBlock, Replicated, SharedBackend, TypedBootstrapPointer, BlobLocator,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const SUPERBLOCK_FORMAT: u32 = 2;

/// Per-instance list of part locators (checkpoint segment or journal segment).
pub type InstanceParts = BTreeMap<String, Vec<BlobLocator>>;

/// Pin contents: generation fencing + pointers to checkpoint/log segments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Superblock {
    pub format: u32,
    pub generation: u64,
    /// Writer identity (primary instance id).
    pub instance_id: String,
    pub fingerprint: String,
    /// Per-instance checkpoint part locators.
    pub checkpoint: InstanceParts,
    /// Journal segments since checkpoint; each entry is per-instance parts.
    pub log: Vec<InstanceParts>,
    /// Committed roots (extent lists) at this generation.
    pub roots: BTreeMap<String, Vec<Extent>>,
    /// Hex sha256 of the canonical JSON without this field.
    pub sha256: String,
}

impl Superblock {
    pub fn new(
        generation: u64,
        instance_id: impl Into<String>,
        fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            format: SUPERBLOCK_FORMAT,
            generation,
            instance_id: instance_id.into(),
            fingerprint: fingerprint.into(),
            checkpoint: BTreeMap::new(),
            log: Vec::new(),
            roots: BTreeMap::new(),
            sha256: String::new(),
        }
    }

    /// Serialize with a freshly computed `sha256`.
    pub fn seal(&mut self) -> Result<Bytes> {
        self.sha256.clear();
        let body = serde_json::to_vec(&CanonicalSuperblock::from(&*self))
            .context("serialize superblock body")?;
        self.sha256 = hex::encode(Sha256::digest(&body));
        let bytes = serde_json::to_vec(self).context("serialize sealed superblock")?;
        Ok(Bytes::from(bytes))
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        let sb: Superblock = serde_json::from_slice(data).context("parse superblock JSON")?;
        if sb.format != SUPERBLOCK_FORMAT && sb.format != 1 {
            bail!("unsupported superblock format {}", sb.format);
        }
        // Format 1 used flat locator lists + ChunkId roots — reject for E writers.
        if sb.format == 1 {
            bail!("superblock format 1 is no longer supported; restore via migrate");
        }
        let mut check = sb.clone();
        check.sha256.clear();
        let body = serde_json::to_vec(&CanonicalSuperblock::from(&check))
            .context("canonicalize superblock")?;
        let expect = hex::encode(Sha256::digest(&body));
        if expect != sb.sha256 {
            bail!("superblock sha256 mismatch");
        }
        Ok(sb)
    }
}

/// Fields hashed into `sha256` (everything except the digest itself).
#[derive(Serialize)]
struct CanonicalSuperblock<'a> {
    format: u32,
    generation: u64,
    instance_id: &'a str,
    fingerprint: &'a str,
    checkpoint: &'a InstanceParts,
    log: &'a [InstanceParts],
    roots: &'a BTreeMap<String, Vec<Extent>>,
}

impl<'a> From<&'a Superblock> for CanonicalSuperblock<'a> {
    fn from(s: &'a Superblock) -> Self {
        Self {
            format: s.format,
            generation: s.generation,
            instance_id: &s.instance_id,
            fingerprint: &s.fingerprint,
            checkpoint: &s.checkpoint,
            log: &s.log,
            roots: &s.roots,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum JournalOp {
    SetRoot {
        name: String,
        extents: Vec<Extent>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JournalSegment {
    pub ops: Vec<JournalOp>,
}

/// Logical checkpoint of blob-layer tables (portable across sqlite paths).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub format: u32,
    pub instances: Vec<CheckpointInstance>,
    pub blobs: Vec<CheckpointBlob>,
    pub parts: Vec<CheckpointPart>,
    #[serde(alias = "frames")]
    pub blocks: Vec<CheckpointBlock>,
    pub roots: Vec<(String, Vec<Extent>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInstance {
    pub id: String,
    pub kind: String,
    pub fingerprint: String,
    pub location: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBlob {
    pub id: ChunkId,
    pub logical_size: i64,
    pub crc32: i64,
    pub refs: i64,
    pub block_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPart {
    pub chunk_id: ChunkId,
    pub instance_id: String,
    pub part_no: i64,
    pub first_block: i64,
    pub block_count: i64,
    pub sort_key: Vec<u8>,
    pub locator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBlock {
    pub chunk_id: ChunkId,
    #[serde(alias = "frame_no")]
    pub block_no: i64,
    pub logical_off: i64,
    pub logical_len: i64,
    pub stored_len: i64,
    pub codec: String,
}

impl BlobDb {
    pub async fn export_checkpoint(&self) -> Result<CheckpointPayload> {
        let instances = sqlx::query_as::<_, (String, String, String, String, String)>(
            "SELECT id, kind, fingerprint, location, state FROM instances",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|(id, kind, fingerprint, location, state)| CheckpointInstance {
            id,
            kind,
            fingerprint,
            location,
            state,
        })
        .collect();

        let blobs = sqlx::query_as::<_, (i64, i64, i64, i64, i64, String)>(
            "SELECT id, logical_size, crc32, refs, block_count, created_at FROM chunks",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(
            |(id, logical_size, crc32, refs, block_count, created_at)| CheckpointBlob {
                id,
                logical_size,
                crc32,
                refs,
                block_count,
                created_at,
            },
        )
        .collect();

        let parts = sqlx::query_as::<_, (i64, String, i64, i64, i64, Vec<u8>, Vec<u8>)>(
            r#"
            SELECT chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator
            FROM chunk_parts
            "#,
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(
            |(chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)| {
                CheckpointPart {
                    chunk_id,
                    instance_id,
                    part_no,
                    first_block,
                    block_count,
                    sort_key,
                    locator,
                }
            },
        )
        .collect();

        let blocks = sqlx::query_as::<_, (i64, i64, i64, i64, i64, String)>(
            r#"
            SELECT chunk_id, block_no, logical_off, logical_len, stored_len, codec
            FROM chunk_blocks
            "#,
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(
            |(chunk_id, block_no, logical_off, logical_len, stored_len, codec)| CheckpointBlock {
                chunk_id,
                block_no,
                logical_off,
                logical_len,
                stored_len,
                codec,
            },
        )
        .collect();

        let root_rows: Vec<(String, String)> =
            sqlx::query_as("SELECT name, extents_json FROM roots")
                .fetch_all(self.pool())
                .await?;
        let mut roots = Vec::new();
        for (name, json) in root_rows {
            let extents: Vec<Extent> = serde_json::from_str(&json)?;
            roots.push((name, extents));
        }

        Ok(CheckpointPayload {
            format: 2,
            instances,
            blobs,
            parts,
            blocks,
            roots,
        })
    }

    /// Replace local tables with a checkpoint (empty DB or `--force` path).
    pub async fn import_checkpoint(&self, cp: &CheckpointPayload) -> Result<()> {
        if cp.format != 2 && cp.format != 1 {
            bail!("unsupported checkpoint format {}", cp.format);
        }
        let mut tx = self.pool().begin().await?;
        for table in [
            "chunk_blocks",
            "chunk_parts",
            "chunk_replicas",
            "roots",
            "chunks",
            "put_watermarks",
            "sweep_cursor",
            "instances",
        ] {
            // chunk_replicas may be empty leftover; ignore missing.
            let _ = sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&mut *tx)
                .await;
        }
        for i in &cp.instances {
            sqlx::query(
                r#"
                INSERT INTO instances (id, kind, fingerprint, location, state)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(&i.id)
            .bind(&i.kind)
            .bind(&i.fingerprint)
            .bind(&i.location)
            .bind(&i.state)
            .execute(&mut *tx)
            .await?;
        }
        for b in &cp.blobs {
            sqlx::query(
                r#"
                INSERT INTO chunks (id, logical_size, crc32, refs, block_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(b.id)
            .bind(b.logical_size)
            .bind(b.crc32)
            .bind(b.refs)
            .bind(b.block_count)
            .bind(&b.created_at)
            .execute(&mut *tx)
            .await?;
        }
        for r in &cp.parts {
            sqlx::query(
                r#"
                INSERT INTO chunk_parts
                  (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(r.chunk_id)
            .bind(&r.instance_id)
            .bind(r.part_no)
            .bind(r.first_block)
            .bind(r.block_count)
            .bind(&r.sort_key)
            .bind(&r.locator)
            .execute(&mut *tx)
            .await?;
        }
        for f in &cp.blocks {
            sqlx::query(
                r#"
                INSERT INTO chunk_blocks
                  (chunk_id, block_no, logical_off, logical_len, stored_len, codec)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(f.chunk_id)
            .bind(f.block_no)
            .bind(f.logical_off)
            .bind(f.logical_len)
            .bind(f.stored_len)
            .bind(&f.codec)
            .execute(&mut *tx)
            .await?;
        }
        for (name, extents) in &cp.roots {
            let json = serde_json::to_string(extents)?;
            sqlx::query("INSERT INTO roots (name, extents_json) VALUES (?, ?)")
                .bind(name)
                .bind(&json)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn apply_journal_ops(&self, ops: &[JournalOp]) -> Result<()> {
        for op in ops {
            match op {
                JournalOp::SetRoot { name, extents } => {
                    self.set_root(name, extents).await?;
                }
            }
        }
        Ok(())
    }
}

/// One pin target (read-write instance bootstrap).
pub struct PinTarget {
    pub instance_id: String,
    pub fingerprint: String,
    pub pin: Arc<dyn TypedBootstrapPointer>,
    /// Backend used to fetch parts belonging to this instance.
    pub backend: SharedBackend,
}

/// In-memory journal buffer + flush/checkpoint against a replicated group.
pub struct Durability {
    replicated: Arc<Replicated>,
    pins: Vec<PinTarget>,
    pending: Mutex<Vec<JournalOp>>,
    /// Last sealed superblock (local view).
    current: Mutex<Superblock>,
    /// When the last superblock was published (for age gauges).
    last_superblock_at: tokio::sync::Mutex<Option<std::time::Instant>>,
    /// When the last full checkpoint completed.
    last_checkpoint_at: tokio::sync::Mutex<Option<std::time::Instant>>,
}

impl Durability {
    /// Single-backend convenience (tests / one-instance deployments).
    pub fn new(
        backend: SharedBackend,
        pin: Arc<dyn TypedBootstrapPointer>,
        genesis: Superblock,
    ) -> Self {
        let info = backend.instance().clone();
        let replicated = Arc::new(
            Replicated::new(
                vec![backend.clone()],
                1,
                Arc::new(pigeonhole_blob::CheapestFirst::new()),
            )
            .expect("single-member Replicated"),
        );
        Self::new_replicated(
            replicated,
            vec![PinTarget {
                instance_id: info.id,
                fingerprint: info.fingerprint,
                pin,
                backend,
            }],
            genesis,
        )
    }

    pub fn new_replicated(
        replicated: Arc<Replicated>,
        pins: Vec<PinTarget>,
        genesis: Superblock,
    ) -> Self {
        Self {
            replicated,
            pins,
            pending: Mutex::new(Vec::new()),
            current: Mutex::new(genesis),
            last_superblock_at: tokio::sync::Mutex::new(None),
            last_checkpoint_at: tokio::sync::Mutex::new(None),
        }
    }

    /// Age of the last published superblock, if any.
    pub async fn superblock_age(&self) -> Option<std::time::Duration> {
        self.last_superblock_at
            .lock()
            .await
            .map(|t| t.elapsed())
    }

    /// Age of the last full checkpoint, if any.
    pub async fn checkpoint_age(&self) -> Option<std::time::Duration> {
        self.last_checkpoint_at
            .lock()
            .await
            .map(|t| t.elapsed())
    }

    pub async fn enqueue(&self, op: JournalOp) {
        self.pending.lock().await.push(op);
    }

    /// Put `data` via Replicated as one raw block; return per-instance locators.
    async fn put_replicated_parts(&self, data: Bytes) -> Result<InstanceParts> {
        let len = data.len() as u32;
        let mut w = self.replicated.chunk_writer();
        w.push(EncodedBlock {
            stored: data,
            logical_len: len,
            codec: "raw".into(),
        })
        .await?;
        let layouts = w.finish().await?;
        let mut map = BTreeMap::new();
        for layout in layouts {
            let locs: Vec<BlobLocator> = layout.parts.into_iter().map(|p| p.locator).collect();
            map.insert(layout.instance, locs);
        }
        Ok(map)
    }

    async fn fetch_instance_parts(&self, parts: &InstanceParts) -> Result<Bytes> {
        let mut last_err = None;
        for pin in &self.pins {
            if let Some(locs) = parts.get(&pin.instance_id) {
                match Self::download_parts(&pin.backend, locs).await {
                    Ok(buf) => return Ok(buf),
                    Err(e) => last_err = Some(e),
                }
            }
        }
        for (inst, locs) in parts {
            let Some(backend) = self
                .replicated
                .members()
                .iter()
                .find(|m| m.instance().id == *inst)
            else {
                continue;
            };
            match Self::download_parts(backend, locs).await {
                Ok(buf) => return Ok(buf),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no available instance for parts")))
    }

    async fn download_parts(backend: &SharedBackend, locs: &[BlobLocator]) -> Result<Bytes> {
        let mut buf = Vec::new();
        for loc in locs {
            let part = collect_stream(backend.get(loc, None).await?).await?;
            buf.extend_from_slice(&part);
        }
        Ok(Bytes::from(buf))
    }

    /// Put pending ops as one journal segment, append to superblock, publish pins.
    pub async fn flush_journal(&self) -> Result<()> {
        self.ensure_not_fenced().await?;
        let ops = {
            let mut g = self.pending.lock().await;
            std::mem::take(&mut *g)
        };
        if ops.is_empty() {
            return Ok(());
        }
        let seg = JournalSegment { ops: ops.clone() };
        let bytes = Bytes::from(serde_json::to_vec(&seg).context("serialize journal segment")?);
        let parts = self
            .put_replicated_parts(bytes)
            .await
            .context("put journal segment")?;

        let sealed = {
            let mut sb = self.current.lock().await;
            sb.log.push(parts);
            sb.generation = sb.generation.saturating_add(1);
            for op in &ops {
                let JournalOp::SetRoot { name, extents } = op;
                sb.roots.insert(name.clone(), extents.clone());
            }
            sb.seal()?
        };
        self.publish_all(sealed).await?;
        Ok(())
    }

    /// Full checkpoint: export DB → put → new superblock with empty log.
    pub async fn checkpoint(&self, db: &BlobDb) -> Result<()> {
        self.ensure_not_fenced().await?;
        self.flush_journal().await?;

        let cp = db.export_checkpoint().await?;
        let bytes = Bytes::from(serde_json::to_vec(&cp).context("serialize checkpoint")?);
        let parts = self
            .put_replicated_parts(bytes)
            .await
            .context("put checkpoint")?;

        let sealed = {
            let mut sb = self.current.lock().await;
            sb.checkpoint = parts;
            sb.log.clear();
            sb.roots = cp.roots.iter().cloned().collect();
            sb.generation = sb.generation.saturating_add(1);
            sb.seal()?
        };
        self.publish_all(sealed).await?;
        *self.last_checkpoint_at.lock().await = Some(std::time::Instant::now());
        Ok(())
    }

    /// Read all pins, take max valid generation, download checkpoint + log, rebuild `db`.
    pub async fn restore_into(&self, db: &BlobDb) -> Result<Superblock> {
        let sb = self
            .read_best_superblock()
            .await?
            .context("no superblock pin")?;

        if sb.checkpoint.is_empty() {
            bail!("superblock has empty checkpoint");
        }
        let cp_bytes = self.fetch_instance_parts(&sb.checkpoint).await?;
        let cp: CheckpointPayload =
            serde_json::from_slice(&cp_bytes).context("parse checkpoint payload")?;
        db.import_checkpoint(&cp).await?;

        for segment_parts in &sb.log {
            let seg_bytes = self.fetch_instance_parts(segment_parts).await?;
            let seg: JournalSegment =
                serde_json::from_slice(&seg_bytes).context("parse journal segment")?;
            db.apply_journal_ops(&seg.ops).await?;
        }

        *self.current.lock().await = sb.clone();
        Ok(sb)
    }

    /// Among all readable pins, pick the highest generation with valid hash + fingerprint.
    pub async fn read_best_superblock(&self) -> Result<Option<Superblock>> {
        let mut best: Option<Superblock> = None;
        for pin in &self.pins {
            let Some(raw) = pin.pin.read().await.context("read pin")? else {
                continue;
            };
            let sb = match Superblock::parse(&raw) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if sb.fingerprint != pin.fingerprint
                && !self
                    .pins
                    .iter()
                    .any(|p| p.fingerprint == sb.fingerprint)
            {
                continue;
            }
            match &best {
                None => best = Some(sb),
                Some(b) if sb.generation > b.generation => best = Some(sb),
                _ => {}
            }
        }
        Ok(best)
    }

    pub async fn generation(&self) -> u64 {
        self.current.lock().await.generation
    }

    /// Sort keys of checkpoint / journal parts for `instance_id`, plus the pin message key.
    pub async fn system_keys(&self, instance_id: &str) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        {
            let sb = self.current.lock().await;
            if let Some(locs) = sb.checkpoint.get(instance_id) {
                for loc in locs {
                    keys.push(loc.key.clone());
                }
            }
            for seg in &sb.log {
                if let Some(locs) = seg.get(instance_id) {
                    for loc in locs {
                        keys.push(loc.key.clone());
                    }
                }
            }
        }
        if let Some(pin) = self.pins.iter().find(|p| p.instance_id == instance_id) {
            if let Some(k) = pin.pin.pin_key().await.context("pin_key")? {
                keys.push(k);
            }
        }
        Ok(keys)
    }

    async fn publish_all(&self, sealed: Bytes) -> Result<()> {
        for pin in &self.pins {
            pin.pin
                .swap(sealed.clone())
                .await
                .with_context(|| format!("publish superblock to {}", pin.instance_id))?;
        }
        *self.last_superblock_at.lock().await = Some(std::time::Instant::now());
        Ok(())
    }

    /// Replace local + pinned superblock (fencing / explicit publish).
    pub async fn publish_superblock(&self, mut sb: Superblock) -> Result<()> {
        self.ensure_not_fenced().await?;
        let sealed = sb.seal()?;
        self.publish_all(sealed).await?;
        *self.current.lock().await = sb;
        Ok(())
    }

    /// Stop if any pin's generation is strictly greater than our local view.
    pub async fn ensure_not_fenced(&self) -> Result<()> {
        let local = self.generation().await;
        for pin in &self.pins {
            let Some(raw) = pin.pin.read().await? else {
                continue;
            };
            let remote = match Superblock::parse(&raw) {
                Ok(s) => s,
                Err(_) => continue,
            };
            if remote.generation > local {
                bail!(
                    "fenced: remote superblock generation {} > local {}; refusing to write",
                    remote.generation,
                    local
                );
            }
        }
        Ok(())
    }
}

/// Commit `set_root` as the durable point: local DB + journal flush + pin.
pub async fn commit_root(
    db: &BlobDb,
    dur: &Durability,
    name: &str,
    extents: &[Extent],
) -> Result<()> {
    dur.ensure_not_fenced().await?;
    db.set_root(name, extents).await?;
    dur.enqueue(JournalOp::SetRoot {
        name: name.to_string(),
        extents: extents.to_vec(),
    })
    .await;
    dur.flush_journal().await?;
    Ok(())
}

impl BlobDb {
    pub async fn is_empty_metadata(&self) -> Result<bool> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM chunks")
            .fetch_one(self.pool())
            .await?;
        let (r,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM roots")
            .fetch_one(self.pool())
            .await?;
        Ok(n == 0 && r == 0)
    }
}

fn fingerprint_ok(sb: &Superblock, dur: &Durability, expected: &str) -> bool {
    sb.fingerprint == expected || dur.pins.iter().any(|p| p.fingerprint == sb.fingerprint)
}

/// Stage G boot: empty DB → restore from superblocks (if any); else publish `generation+1`.
///
/// - Empty local DB + pin → `restore_into`, then fence with `generation+1`.
/// - Empty local DB + no pin → first boot; publish genesis as generation 1.
/// - Non-empty DB → keep local data; fence by publishing `generation+1` (unless `force`).
/// - `force` → overwrite non-empty DB from superblocks (CLI `restore --force`).
pub async fn start_or_restore(
    db: &BlobDb,
    dur: &Durability,
    expected_fingerprint: &str,
    force: bool,
) -> Result<Superblock> {
    let empty = db.is_empty_metadata().await?;

    if empty || force {
        match dur.read_best_superblock().await? {
            Some(_) => {
                let sb = dur.restore_into(db).await?;
                if !fingerprint_ok(&sb, dur, expected_fingerprint) {
                    bail!(
                        "superblock fingerprint {:?} != config {:?}",
                        sb.fingerprint,
                        expected_fingerprint
                    );
                }
                let mut next = sb.clone();
                next.generation = next.generation.saturating_add(1);
                dur.publish_superblock(next).await?;
                Ok(sb)
            }
            None if empty && !force => {
                let mut sb = dur.current.lock().await.clone();
                if !fingerprint_ok(&sb, dur, expected_fingerprint) {
                    sb.fingerprint = expected_fingerprint.to_string();
                }
                sb.generation = sb.generation.max(1);
                dur.publish_superblock(sb.clone()).await?;
                Ok(sb)
            }
            None => bail!("restore --force requires a superblock pin; none found"),
        }
    } else {
        // Non-empty local DB: fencing only.
        let base = match dur.read_best_superblock().await? {
            Some(remote) => {
                if !fingerprint_ok(&remote, dur, expected_fingerprint) {
                    bail!(
                        "superblock fingerprint {:?} != config {:?}",
                        remote.fingerprint,
                        expected_fingerprint
                    );
                }
                *dur.current.lock().await = remote.clone();
                remote
            }
            None => dur.current.lock().await.clone(),
        };
        let mut next = base.clone();
        next.generation = next.generation.saturating_add(1).max(1);
        dur.publish_superblock(next).await?;
        Ok(base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::IngestOptions;
    use crate::layer::ChunkStore;
    use async_trait::async_trait;
    use pigeonhole_blob::BlobBackend;
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;
    use std::sync::Mutex as StdMutex;

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

    #[tokio::test]
    async fn journal_flush_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("a.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let mem = MemoryBlobStore::new();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open(db.clone(), mem, opts).await.unwrap();
        let backend = layer.write_backend();
        let info = backend.instance().clone();

        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let genesis = Superblock::new(0, info.id.clone(), info.fingerprint.clone());
        let dur = Durability::new(backend.clone(), pin.clone(), genesis);

        let blob = layer
            .put_small(Bytes::from_static(b"hello-root"))
            .await
            .unwrap();
        dur.checkpoint(layer.db()).await.unwrap();
        assert!(pin.read().await.unwrap().is_some());

        let extents = vec![Extent {
            chunk: blob,
            offset: 0,
            len: 10,
        }];
        commit_root(layer.db(), &dur, "s3/index", &extents)
            .await
            .unwrap();
        assert_eq!(layer.get_root("s3/index").await.unwrap(), Some(extents.clone()));
        let gen_after = dur.generation().await;
        assert!(gen_after >= 2);

        let url2 = format!("sqlite:{}?mode=rwc", dir.path().join("b.db").display());
        let db2 = BlobDb::connect(&url2).await.unwrap();
        let genesis2 = Superblock::new(0, info.id, info.fingerprint);
        let dur2 = Durability::new(backend, pin, genesis2);
        let sb = dur2.restore_into(&db2).await.unwrap();
        assert_eq!(sb.roots.get("s3/index"), Some(&extents));
        assert_eq!(db2.get_root("s3/index").await.unwrap(), Some(extents));
    }

    #[test]
    fn superblock_hash_roundtrip() {
        let mut sb = Superblock::new(3, "tg-main", "tg:1:-100");
        sb.roots.insert(
            "cas/index".into(),
            vec![Extent {
                chunk: 9,
                offset: 0,
                len: 1,
            }],
        );
        let bytes = sb.seal().unwrap();
        let parsed = Superblock::parse(&bytes).unwrap();
        assert_eq!(parsed.generation, 3);
        assert_eq!(parsed.roots.get("cas/index").unwrap()[0].chunk, 9);
    }

    #[tokio::test]
    async fn start_or_restore_fences_stale_writer() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("a.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let mem = MemoryBlobStore::new();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = ChunkStore::open(db.clone(), mem, opts).await.unwrap();
        let backend = layer.write_backend();
        let info = backend.instance().clone();
        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });

        let dur_a = Durability::new(
            backend.clone(),
            pin.clone(),
            Superblock::new(0, info.id.clone(), info.fingerprint.clone()),
        );
        layer.put_small(Bytes::from_static(b"x")).await.unwrap();
        dur_a.checkpoint(layer.db()).await.unwrap();

        let url_b = format!("sqlite:{}?mode=rwc", dir.path().join("b.db").display());
        let db_b = BlobDb::connect(&url_b).await.unwrap();
        let dur_b = Durability::new(
            backend.clone(),
            pin.clone(),
            Superblock::new(0, info.id.clone(), info.fingerprint.clone()),
        );
        let restored = start_or_restore(&db_b, &dur_b, &info.fingerprint, false)
            .await
            .unwrap();
        assert!(restored.generation >= 1);
        assert!(dur_b.generation().await > restored.generation);

        let err = dur_a.ensure_not_fenced().await.unwrap_err();
        assert!(err.to_string().contains("fenced"));

        // Non-empty DB: fencing bump succeeds (keeps local data).
        let gen_before = dur_b.generation().await;
        let _ = start_or_restore(&db_b, &dur_b, &info.fingerprint, false)
            .await
            .unwrap();
        assert!(dur_b.generation().await > gen_before);
    }

    #[tokio::test]
    async fn two_pins_take_higher_generation() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("a.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let a = MemoryBlobStore::new().with_instance_id("a");
        let b = MemoryBlobStore::new().with_instance_id("b");
        let fp_a = a.instance().fingerprint.clone();
        let fp_b = b.instance().fingerprint.clone();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 32 * 1024;
        let backend_a: SharedBackend = Arc::new(pigeonhole_blob::erase(a));
        let backend_b: SharedBackend = Arc::new(pigeonhole_blob::erase(b));
        let rep = Arc::new(
            Replicated::new(
                vec![backend_a.clone(), backend_b.clone()],
                2,
                Arc::new(pigeonhole_blob::CheapestFirst::new()),
            )
            .unwrap(),
        );
        let layer = ChunkStore::open_replicated(db.clone(), rep.clone(), opts)
            .await
            .unwrap();
        let pin_a = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let pin_b = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let pins = vec![
            PinTarget {
                instance_id: "a".into(),
                fingerprint: fp_a.clone(),
                pin: pin_a.clone(),
                backend: backend_a,
            },
            PinTarget {
                instance_id: "b".into(),
                fingerprint: fp_b,
                pin: pin_b.clone(),
                backend: backend_b,
            },
        ];
        let dur = Durability::new_replicated(rep, pins, Superblock::new(0, "a", fp_a.clone()));
        layer.put_small(Bytes::from_static(b"hi")).await.unwrap();
        dur.checkpoint(layer.db()).await.unwrap();
        let gen = dur.generation().await;

        // Overwrite pin A with a stale lower generation; pin B keeps the higher one.
        let mut stale = Superblock::new(0, "a", fp_a);
        let stale_bytes = stale.seal().unwrap();
        pin_a.swap(stale_bytes).await.unwrap();

        let best = dur.read_best_superblock().await.unwrap().unwrap();
        assert_eq!(best.generation, gen);
        assert!(best.generation > 0);
    }
}
