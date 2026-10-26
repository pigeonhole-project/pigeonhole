//! The CAS namespace of the metadata state machine. [`CasNamespace::apply`] is
//! the only writer of `cas_blobs`; timestamps come in the command.

use crate::cas_index::SCHEMA;
use anyhow::{Context, Result};
use async_trait::async_trait;
use pigeonhole_chunk_store::Extent;
use pigeonhole_meta::{Applied, Namespace};
use prost::Message;
use sqlx::SqliteConnection;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.cas.v1.rs"));
}

pub(crate) const NAMESPACE: &str = "cas";

pub(crate) struct CasNamespace;

#[async_trait]
impl Namespace for CasNamespace {
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
        let cmd = pb::Command::decode(command).context("decode cas command")?;
        let reply = match cmd.op.context("empty cas command")? {
            Op::PutEntry(c) => put_entry(conn, c).await?.encode_to_vec(),
            Op::TouchEntry(c) => {
                touch(conn, &c.hash, c.size, &c.at).await?;
                Vec::new()
            }
            Op::DropEntry(c) => drop_entry(conn, &c.hash, c.size).await?.encode_to_vec(),
        };
        Ok(Applied::Commit(reply))
    }
}

pub(crate) fn to_pb(extents: &[Extent]) -> Vec<pb::Extent> {
    extents
        .iter()
        .map(|e| pb::Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect()
}

pub(crate) fn from_pb(extents: Vec<pb::Extent>) -> Vec<Extent> {
    extents
        .into_iter()
        .map(|e| Extent {
            chunk: e.chunk,
            offset: e.offset,
            len: e.len,
        })
        .collect()
}

async fn existing(conn: &mut SqliteConnection, hash: &str, size: i64) -> Result<Option<Vec<Extent>>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT extents_json FROM cas_blobs WHERE hash = ? AND size = ?")
            .bind(hash)
            .bind(size)
            .fetch_optional(&mut *conn)
            .await?;
    row.map(|(json,)| serde_json::from_str(&json).context("parse extents_json"))
        .transpose()
}

async fn touch(conn: &mut SqliteConnection, hash: &str, size: i64, at: &str) -> Result<()> {
    sqlx::query("UPDATE cas_blobs SET last_access = ? WHERE hash = ? AND size = ?")
        .bind(at)
        .bind(hash)
        .bind(size)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn put_entry(conn: &mut SqliteConnection, c: pb::PutEntry) -> Result<pb::PutResult> {
    if let Some(extents) = existing(conn, &c.hash, c.size).await? {
        touch(conn, &c.hash, c.size, &c.at).await?;
        return Ok(pb::PutResult {
            existed: true,
            existing: to_pb(&extents),
        });
    }
    let json = serde_json::to_string(&from_pb(c.extents))?;
    sqlx::query("INSERT INTO cas_blobs (hash, size, extents_json, last_access) VALUES (?, ?, ?, ?)")
        .bind(&c.hash)
        .bind(c.size)
        .bind(&json)
        .bind(&c.at)
        .execute(&mut *conn)
        .await?;
    Ok(pb::PutResult::default())
}

async fn drop_entry(conn: &mut SqliteConnection, hash: &str, size: i64) -> Result<pb::Released> {
    let Some(extents) = existing(conn, hash, size).await? else {
        return Ok(pb::Released::default());
    };
    sqlx::query("DELETE FROM cas_blobs WHERE hash = ? AND size = ?")
        .bind(hash)
        .bind(size)
        .execute(&mut *conn)
        .await?;
    let mut chunk_ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
    chunk_ids.sort_unstable();
    chunk_ids.dedup();
    Ok(pb::Released { chunk_ids })
}
