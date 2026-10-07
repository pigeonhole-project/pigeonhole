//! Journal segments + superblock for `blob.db` durability (stage 2.1).
//!
//! Segments and checkpoints are stored as raw backend puts (locators in the
//! superblock), not as rows in `blobs` — so restore can bootstrap without a map.

use crate::blob_db::BlobDb;
use crate::layer::BlobId;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use pigeonhole_blob::{collect_stream, SharedBackend, StoredId, TypedBootstrapPointer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const SUPERBLOCK_FORMAT: u32 = 1;

/// Pin contents: generation fencing + pointers to checkpoint/log segments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Superblock {
    pub format: u32,
    pub generation: u64,
    pub instance_id: String,
    pub fingerprint: String,
    /// Locators of checkpoint payload parts (usually one).
    pub checkpoint: Vec<StoredId>,
    /// Locators of journal segments since the checkpoint, oldest first.
    pub log: Vec<Vec<StoredId>>,
    /// Committed roots at this generation.
    pub roots: BTreeMap<String, BlobId>,
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
            checkpoint: Vec::new(),
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
        if sb.format != SUPERBLOCK_FORMAT {
            bail!("unsupported superblock format {}", sb.format);
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
    checkpoint: &'a [StoredId],
    log: &'a [Vec<StoredId>],
    roots: &'a BTreeMap<String, BlobId>,
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
    SetRoot { name: String, blob_id: BlobId },
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
    pub replicas: Vec<CheckpointReplica>,
    #[serde(alias = "frames")]
    pub blocks: Vec<CheckpointBlock>,
    pub roots: Vec<(String, BlobId)>,
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
    pub id: BlobId,
    pub size: i64,
    pub crc32: i64,
    pub refs: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointReplica {
    pub blob_id: BlobId,
    pub instance_id: String,
    pub sort_key: Vec<u8>,
    pub locator: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointBlock {
    pub blob_id: BlobId,
    #[serde(alias = "frame_no")]
    pub block_no: i64,
    pub stored_off: i64,
    pub stored_len: i64,
    pub logical_off: i64,
    pub logical_len: i64,
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

        let blobs = sqlx::query_as::<_, (i64, i64, i64, i64, String)>(
            "SELECT id, size, crc32, refs, created_at FROM blobs",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|(id, size, crc32, refs, created_at)| CheckpointBlob {
            id,
            size,
            crc32,
            refs,
            created_at,
        })
        .collect();

        let replicas = sqlx::query_as::<_, (i64, String, Vec<u8>, Vec<u8>)>(
            "SELECT blob_id, instance_id, sort_key, locator FROM replicas",
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(|(blob_id, instance_id, sort_key, locator)| CheckpointReplica {
            blob_id,
            instance_id,
            sort_key,
            locator,
        })
        .collect();

        let blocks = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, String)>(
            r#"
            SELECT blob_id, block_no, stored_off, stored_len, logical_off, logical_len, codec
            FROM chunk_blocks
            "#,
        )
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(
            |(blob_id, block_no, stored_off, stored_len, logical_off, logical_len, codec)| {
                CheckpointBlock {
                    blob_id,
                    block_no,
                    stored_off,
                    stored_len,
                    logical_off,
                    logical_len,
                    codec,
                }
            },
        )
        .collect();

        let roots = sqlx::query_as::<_, (String, i64)>("SELECT name, blob_id FROM roots")
            .fetch_all(self.pool())
            .await?;

        Ok(CheckpointPayload {
            format: 1,
            instances,
            blobs,
            replicas,
            blocks,
            roots,
        })
    }

    /// Replace local tables with a checkpoint (empty DB or `--force` path).
    pub async fn import_checkpoint(&self, cp: &CheckpointPayload) -> Result<()> {
        if cp.format != 1 {
            bail!("unsupported checkpoint format {}", cp.format);
        }
        let mut tx = self.pool().begin().await?;
        for table in [
            "chunk_blocks",
            "replicas",
            "roots",
            "blobs",
            "put_watermarks",
            "sweep_cursor",
            "instances",
        ] {
            sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&mut *tx)
                .await?;
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
                INSERT INTO blobs (id, size, crc32, refs, created_at)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(b.id)
            .bind(b.size)
            .bind(b.crc32)
            .bind(b.refs)
            .bind(&b.created_at)
            .execute(&mut *tx)
            .await?;
        }
        for r in &cp.replicas {
            sqlx::query(
                r#"
                INSERT INTO replicas (blob_id, instance_id, sort_key, locator)
                VALUES (?, ?, ?, ?)
                "#,
            )
            .bind(r.blob_id)
            .bind(&r.instance_id)
            .bind(&r.sort_key)
            .bind(&r.locator)
            .execute(&mut *tx)
            .await?;
        }
        for f in &cp.blocks {
            sqlx::query(
                r#"
                INSERT INTO chunk_blocks
                  (blob_id, block_no, stored_off, stored_len, logical_off, logical_len, codec)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(f.blob_id)
            .bind(f.block_no)
            .bind(f.stored_off)
            .bind(f.stored_len)
            .bind(f.logical_off)
            .bind(f.logical_len)
            .bind(&f.codec)
            .execute(&mut *tx)
            .await?;
        }
        for (name, blob_id) in &cp.roots {
            sqlx::query("INSERT INTO roots (name, blob_id) VALUES (?, ?)")
                .bind(name)
                .bind(blob_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn apply_journal_ops(&self, ops: &[JournalOp]) -> Result<()> {
        for op in ops {
            match op {
                JournalOp::SetRoot { name, blob_id } => {
                    self.set_root(name, *blob_id).await?;
                }
            }
        }
        Ok(())
    }
}

/// In-memory journal buffer + flush/checkpoint against a write backend.
pub struct Durability {
    backend: SharedBackend,
    pin: Arc<dyn TypedBootstrapPointer>,
    pending: Mutex<Vec<JournalOp>>,
    /// Last sealed superblock (local view).
    current: Mutex<Superblock>,
}

impl Durability {
    pub fn new(
        backend: SharedBackend,
        pin: Arc<dyn TypedBootstrapPointer>,
        genesis: Superblock,
    ) -> Self {
        Self {
            backend,
            pin,
            pending: Mutex::new(Vec::new()),
            current: Mutex::new(genesis),
        }
    }

    pub async fn enqueue(&self, op: JournalOp) {
        self.pending.lock().await.push(op);
    }

    /// Put pending ops as one journal segment, append to superblock, swap pin.
    pub async fn flush_journal(&self) -> Result<()> {
        self.ensure_not_fenced().await?;
        let ops = {
            let mut g = self.pending.lock().await;
            std::mem::take(&mut *g)
        };
        if ops.is_empty() {
            return Ok(());
        }
        let seg = JournalSegment { ops };
        let bytes = Bytes::from(serde_json::to_vec(&seg).context("serialize journal segment")?);
        let loc = self
            .backend
            .put(bytes)
            .await
            .context("put journal segment")?;

        let mut sb = self.current.lock().await;
        sb.log.push(vec![loc]);
        sb.generation = sb.generation.saturating_add(1);
        // Refresh roots from set_root ops in this segment for the pin.
        for op in &seg.ops {
            let JournalOp::SetRoot { name, blob_id } = op;
            sb.roots.insert(name.clone(), *blob_id);
        }
        let sealed = sb.seal()?;
        self.pin.swap(sealed).await.context("swap superblock pin")?;
        Ok(())
    }

    /// Full checkpoint: export DB → put → new superblock with empty log.
    pub async fn checkpoint(&self, db: &BlobDb) -> Result<()> {
        self.ensure_not_fenced().await?;
        // Drain journal into the DB first (caller should have applied ops locally).
        self.flush_journal().await?;

        let cp = db.export_checkpoint().await?;
        let bytes = Bytes::from(serde_json::to_vec(&cp).context("serialize checkpoint")?);
        let loc = self.backend.put(bytes).await.context("put checkpoint")?;

        let mut sb = self.current.lock().await;
        sb.checkpoint = vec![loc];
        sb.log.clear();
        sb.roots = cp.roots.iter().cloned().collect();
        sb.generation = sb.generation.saturating_add(1);
        let sealed = sb.seal()?;
        self.pin.swap(sealed).await.context("swap superblock after checkpoint")?;
        Ok(())
    }

    /// Read pin, verify, download checkpoint + log, rebuild `db`.
    pub async fn restore_into(&self, db: &BlobDb) -> Result<Superblock> {
        let Some(raw) = self.pin.read().await.context("read superblock pin")? else {
            bail!("no superblock pin");
        };
        let sb = Superblock::parse(&raw)?;
        // Fingerprint check is caller's responsibility against config.

        if sb.checkpoint.is_empty() {
            bail!("superblock has empty checkpoint");
        }
        let mut cp_bytes = Vec::new();
        for loc in &sb.checkpoint {
            let part = collect_stream(self.backend.get(loc, None).await?).await?;
            cp_bytes.extend_from_slice(&part);
        }
        let cp: CheckpointPayload =
            serde_json::from_slice(&cp_bytes).context("parse checkpoint payload")?;
        db.import_checkpoint(&cp).await?;

        for segment_locs in &sb.log {
            let mut seg_bytes = Vec::new();
            for loc in segment_locs {
                let part = collect_stream(self.backend.get(loc, None).await?).await?;
                seg_bytes.extend_from_slice(&part);
            }
            let seg: JournalSegment =
                serde_json::from_slice(&seg_bytes).context("parse journal segment")?;
            db.apply_journal_ops(&seg.ops).await?;
        }

        *self.current.lock().await = sb.clone();
        Ok(sb)
    }

    pub async fn generation(&self) -> u64 {
        self.current.lock().await.generation
    }

    /// Replace local + pinned superblock (fencing / explicit publish).
    pub async fn publish_superblock(&self, mut sb: Superblock) -> Result<()> {
        let sealed = sb.seal()?;
        self.pin.swap(sealed).await.context("publish superblock")?;
        *self.current.lock().await = sb;
        Ok(())
    }

    /// Stop if the pin's generation is strictly greater than our local view.
    pub async fn ensure_not_fenced(&self) -> Result<()> {
        let local = self.generation().await;
        let Some(raw) = self.pin.read().await? else {
            return Ok(());
        };
        let remote = Superblock::parse(&raw)?;
        if remote.generation > local {
            bail!(
                "fenced: remote superblock generation {} > local {}; refusing to write",
                remote.generation,
                local
            );
        }
        Ok(())
    }
}

/// Commit `set_root` as the durable point: local DB + journal flush + pin.
pub async fn commit_root(
    db: &BlobDb,
    dur: &Durability,
    name: &str,
    blob_id: BlobId,
) -> Result<()> {
    dur.ensure_not_fenced().await?;
    db.set_root(name, blob_id).await?;
    dur.enqueue(JournalOp::SetRoot {
        name: name.to_string(),
        blob_id,
    })
    .await;
    dur.flush_journal().await?;
    Ok(())
}

impl BlobDb {
    pub async fn is_empty_metadata(&self) -> Result<bool> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
            .fetch_one(self.pool())
            .await?;
        let (r,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM roots")
            .fetch_one(self.pool())
            .await?;
        Ok(n == 0 && r == 0)
    }
}

/// Stage 2.3: restore from pin into `db`, then fence by publishing generation+1.
///
/// Refuses a non-empty `db` unless `force` is set (same policy as legacy restore).
pub async fn start_or_restore(
    db: &BlobDb,
    dur: &Durability,
    expected_fingerprint: &str,
    force: bool,
) -> Result<Superblock> {
    if !force && !db.is_empty_metadata().await? {
        bail!("blob.db is not empty; pass force=true to overwrite (like restore --force)");
    }
    let sb = dur.restore_into(db).await?;
    if sb.fingerprint != expected_fingerprint {
        bail!(
            "superblock fingerprint {:?} != config {:?}",
            sb.fingerprint,
            expected_fingerprint
        );
    }

    // Fencing: publish generation+1 so a stale writer with a lower generation stops.
    let mut next = sb.clone();
    next.generation = next.generation.saturating_add(1);
    dur.publish_superblock(next).await?;
    Ok(sb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::IngestOptions;
    use crate::layer::BlobLayer;
    use async_trait::async_trait;
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
        let layer = BlobLayer::open(db.clone(), mem, opts).await.unwrap();
        let backend = layer.write_backend();
        let info = backend.instance().clone();

        let pin = Arc::new(MemPin {
            data: StdMutex::new(None),
        });
        let genesis = Superblock::new(0, info.id.clone(), info.fingerprint.clone());
        let dur = Durability::new(backend.clone(), pin.clone(), genesis);

        // Seed: put_small + checkpoint so restore has a base.
        let blob = layer.put_small(Bytes::from_static(b"hello-root")).await.unwrap();
        dur.checkpoint(layer.db()).await.unwrap();
        assert!(pin.read().await.unwrap().is_some());

        commit_root(layer.db(), &dur, "s3/index", blob)
            .await
            .unwrap();
        assert_eq!(layer.get_root("s3/index").await.unwrap(), Some(blob));
        let gen_after = dur.generation().await;
        assert!(gen_after >= 2);

        // Fresh DB + same pin/backend → restore.
        let url2 = format!("sqlite:{}?mode=rwc", dir.path().join("b.db").display());
        let db2 = BlobDb::connect(&url2).await.unwrap();
        let genesis2 = Superblock::new(0, info.id, info.fingerprint);
        let dur2 = Durability::new(backend, pin, genesis2);
        let sb = dur2.restore_into(&db2).await.unwrap();
        assert_eq!(sb.roots.get("s3/index"), Some(&blob));
        assert_eq!(db2.get_root("s3/index").await.unwrap(), Some(blob));
    }

    #[test]
    fn superblock_hash_roundtrip() {
        let mut sb = Superblock::new(3, "tg-main", "tg:1:-100");
        sb.roots.insert("cas/index".into(), 9);
        let bytes = sb.seal().unwrap();
        let parsed = Superblock::parse(&bytes).unwrap();
        assert_eq!(parsed.generation, 3);
        assert_eq!(parsed.roots.get("cas/index"), Some(&9));
    }

    #[tokio::test]
    async fn start_or_restore_fences_stale_writer() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("a.db").display());
        let db = BlobDb::connect(&url).await.unwrap();
        let mem = MemoryBlobStore::new();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let layer = BlobLayer::open(db.clone(), mem, opts).await.unwrap();
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

        // Process B restores and fences (generation bump).
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

        // Stale A still thinks it has the old generation → fenced.
        let err = dur_a.ensure_not_fenced().await.unwrap_err();
        assert!(err.to_string().contains("fenced"));

        // Non-empty refuse without force.
        let err = start_or_restore(&db_b, &dur_b, &info.fingerprint, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not empty"));
    }
}
