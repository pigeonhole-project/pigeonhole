//! The chunk-store namespace of the metadata state machine.
//!
//! [`ChunkNamespace::apply`] is the only code that writes chunk state (instances,
//! chunks, blocks, parts, the chunk id high-water mark). It must stay
//! deterministic: no clocks, no id generation, no metrics — those belong to the
//! proposer in [`crate::BlobDb`].

use crate::blob_db::{ChunkId, Extent, StoredBlock, SCHEMA};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use pigeonhole_blob::{PartLayout, ReplicaLayout};
use pigeonhole_meta::{Applied, Namespace};
use prost::Message;
use sqlx::SqliteConnection;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.chunk.v1.rs"));
}

pub(crate) const NAMESPACE: &str = "chunk";

pub(crate) struct ChunkNamespace;

#[async_trait]
impl Namespace for ChunkNamespace {
    fn name(&self) -> &'static str {
        NAMESPACE
    }

    async fn init(&self, conn: &mut SqliteConnection) -> Result<()> {
        for ddl in SCHEMA {
            sqlx::query(ddl).execute(&mut *conn).await?;
        }
        Ok(())
    }

    async fn apply(&self, conn: &mut SqliteConnection, command: &[u8]) -> Result<Applied> {
        use pb::command::Op;
        let cmd = pb::Command::decode(command).context("decode chunk command")?;
        match cmd.op.context("empty chunk command")? {
            Op::SyncInstances(c) => sync_instances(conn, c).await,
            Op::CommitChunk(c) => commit_chunk(conn, c).await,
            Op::Retain(c) => retain(conn, &c.chunk_ids).await,
            Op::Release(c) => release(conn, &c.chunk_ids).await,
            Op::BeginReclaim(c) => begin_reclaim(conn, c.chunk_id).await,
            Op::DropChunk(c) => drop_chunk(conn, c.chunk_id).await,
            Op::AddReplica(c) => add_replica(conn, c).await,
        }
    }
}

fn ok() -> Result<Applied> {
    Ok(Applied::Commit(Vec::new()))
}

fn reject(reason: pb::rejection::Reason) -> Result<Applied> {
    Ok(Applied::Reject(
        pb::Rejection {
            reason: Some(reason),
        }
        .encode_to_vec(),
    ))
}

async fn sync_instances(conn: &mut SqliteConnection, c: pb::SyncInstances) -> Result<Applied> {
    let existing: Vec<(String,)> = sqlx::query_as("SELECT id FROM chunk_instances")
        .fetch_all(&mut *conn)
        .await?;
    let configured: std::collections::HashSet<&str> =
        c.instances.iter().map(|i| i.id.as_str()).collect();
    for (id,) in &existing {
        if !configured.contains(id.as_str()) {
            sqlx::query("UPDATE chunk_instances SET state = 'retired' WHERE id = ?")
                .bind(id)
                .execute(&mut *conn)
                .await?;
        }
    }
    for i in &c.instances {
        sqlx::query(
            r#"
            INSERT INTO chunk_instances (id, kind, fingerprint, location, state)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind = excluded.kind,
                fingerprint = excluded.fingerprint,
                location = excluded.location,
                state = excluded.state
            "#,
        )
        .bind(&i.id)
        .bind(&i.kind)
        .bind(&i.fingerprint)
        .bind(&i.location)
        .bind(&i.state)
        .execute(&mut *conn)
        .await?;
    }
    ok()
}

async fn commit_chunk(conn: &mut SqliteConnection, c: pb::CommitChunk) -> Result<Applied> {
    // Reject duplicate sort keys inside one commit (would violate UNIQUE and
    // indicate a packer / fan-out bug).
    let mut seen = std::collections::HashSet::new();
    for rep in &c.replicas {
        for part in &rep.parts {
            if !seen.insert((rep.instance.as_str(), part.sort_key.as_slice())) {
                bail!("duplicate sort_key in commit_chunk for instance {}", rep.instance);
            }
        }
    }

    sqlx::query(
        r#"
        INSERT INTO chunks (id, logical_size, crc32, refs, block_count, created_at)
        VALUES (?, ?, ?, 1, ?, ?)
        "#,
    )
    .bind(c.chunk_id)
    .bind(c.logical_size)
    .bind(i64::from(c.crc32))
    .bind(c.blocks.len() as i64)
    .bind(&c.created_at)
    .execute(&mut *conn)
    .await
    .with_context(|| format!("insert chunk {}", c.chunk_id))?;
    sqlx::query(
        r#"
        INSERT INTO chunk_id_seq (singleton, last) VALUES (0, ?)
        ON CONFLICT (singleton) DO UPDATE SET last = MAX(last, excluded.last)
        "#,
    )
    .bind(c.chunk_id)
    .execute(&mut *conn)
    .await?;

    for b in &c.blocks {
        sqlx::query(
            r#"
            INSERT INTO chunk_blocks
              (chunk_id, block_no, logical_off, logical_len, stored_len, codec)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(c.chunk_id)
        .bind(b.block_no)
        .bind(b.logical_off)
        .bind(b.logical_len)
        .bind(b.stored_len)
        .bind(&b.codec)
        .execute(&mut *conn)
        .await?;
    }
    for rep in &c.replicas {
        insert_parts(conn, c.chunk_id, rep).await?;
    }
    ok()
}

async fn insert_parts(conn: &mut SqliteConnection, chunk_id: ChunkId, rep: &pb::Replica) -> Result<()> {
    for (part_no, part) in rep.parts.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO chunk_parts
              (chunk_id, instance_id, part_no, first_block, block_count, sort_key, locator)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(chunk_id)
        .bind(&rep.instance)
        .bind(part_no as i64)
        .bind(i64::from(part.first_block))
        .bind(i64::from(part.block_count))
        .bind(&part.sort_key)
        .bind(&part.locator)
        .execute(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "insert chunk_parts chunk={chunk_id} instance={} part={part_no} key_hex={}",
                rep.instance,
                hex::encode(&part.sort_key)
            )
        })?;
    }
    Ok(())
}

async fn retain(conn: &mut SqliteConnection, ids: &[ChunkId]) -> Result<Applied> {
    for &id in ids {
        let res = sqlx::query(
            "UPDATE chunks SET refs = refs + 1 WHERE id = ? AND refs > 0 AND state = 'live'",
        )
        .bind(id)
        .execute(&mut *conn)
        .await?;
        if res.rows_affected() != 1 {
            return reject(pb::rejection::Reason::ChunkGone(id));
        }
    }
    ok()
}

async fn release(conn: &mut SqliteConnection, ids: &[ChunkId]) -> Result<Applied> {
    for &id in ids {
        let res = sqlx::query(
            "UPDATE chunks SET refs = refs - 1 WHERE id = ? AND refs > 0 AND state = 'live'",
        )
        .bind(id)
        .execute(&mut *conn)
        .await?;
        if res.rows_affected() != 1 {
            return reject(pb::rejection::Reason::DoubleRelease(id));
        }
    }
    ok()
}


async fn begin_reclaim(conn: &mut SqliteConnection, id: ChunkId) -> Result<Applied> {
    let res = sqlx::query(
        "UPDATE chunks SET state = 'reclaiming' WHERE id = ? AND refs = 0 AND state = 'live'",
    )
    .bind(id)
    .execute(&mut *conn)
    .await?;
    if res.rows_affected() == 1 {
        return ok();
    }
    // Already reclaiming (a pass interrupted by a crash) counts as started.
    let state: Option<(String,)> = sqlx::query_as("SELECT state FROM chunks WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
    match state.as_ref().map(|(s,)| s.as_str()) {
        Some(crate::blob_db::CHUNK_STATE_RECLAIMING) => ok(),
        _ => reject(pb::rejection::Reason::NotReclaimable(id)),
    }
}

async fn drop_chunk(conn: &mut SqliteConnection, id: ChunkId) -> Result<Applied> {
    for sql in [
        "DELETE FROM chunk_parts WHERE chunk_id = ?",
        "DELETE FROM chunk_blocks WHERE chunk_id = ?",
        "DELETE FROM chunk_repair_queue WHERE chunk_id = ?",
        "DELETE FROM chunks WHERE id = ?",
    ] {
        sqlx::query(sql).bind(id).execute(&mut *conn).await?;
    }
    ok()
}

async fn add_replica(conn: &mut SqliteConnection, c: pb::AddReplica) -> Result<Applied> {
    let rep = c.replica.context("add_replica without replica")?;
    sqlx::query("DELETE FROM chunk_parts WHERE chunk_id = ? AND instance_id = ?")
        .bind(c.chunk_id)
        .bind(&rep.instance)
        .execute(&mut *conn)
        .await?;
    insert_parts(conn, c.chunk_id, &rep).await?;
    ok()
}

// --- conversions -----------------------------------------------------------

impl From<pb::Extent> for Extent {
    fn from(e: pb::Extent) -> Self {
        Self {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        }
    }
}

impl From<&Extent> for pb::Extent {
    fn from(e: &Extent) -> Self {
        Self {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        }
    }
}

impl From<&StoredBlock> for pb::Block {
    fn from(b: &StoredBlock) -> Self {
        Self {
            block_no: b.block_no,
            logical_off: b.logical_off,
            logical_len: b.logical_len,
            stored_len: b.stored_len,
            codec: b.codec.clone(),
        }
    }
}

impl From<&ReplicaLayout> for pb::Replica {
    fn from(r: &ReplicaLayout) -> Self {
        Self {
            instance: r.instance.clone(),
            parts: r
                .parts
                .iter()
                .map(|p: &PartLayout| pb::Part {
                    first_block: p.first_block,
                    block_count: p.block_count,
                    sort_key: p.locator.key.clone(),
                    locator: p.locator.locator.clone(),
                })
                .collect(),
        }
    }
}
