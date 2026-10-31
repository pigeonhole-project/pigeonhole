//! pigeonhole in one process, on in-memory stores: the same stack the binary
//! builds for `kind = "memory"` stores, without HTTP, for the site's demo in
//! the browser (wasm32-wasip1 in a Web Worker).
//!
//! Two blob stores, `telegram` and `discord`, form one replica group (every
//! chunk to both); the metadata log and its snapshots live in a third. The
//! S3 calls go straight to [`S3Gateway`], the same handlers the server runs.

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole_blob::{erase, CheapestFirst, InstanceInfo, InstanceRole, MetricsBackend, Replicated};
use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions};
use pigeonhole_chunk_store::{RepairConfig, RepairStats, Repairer, SweepConfig, SweepStats, Sweeper};
use pigeonhole_gateway_s3::{Index, S3Gateway, S3Options};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaOptions, MetaStore, Namespaces};
use pigeonhole_storage_memory::MemoryBlobStore;
use s3s::dto::{CopyObjectInput, CreateBucketInput, DeleteObjectInput, GetObjectInput, ListObjectsV2Input};
use s3s::dto::{PutObjectInput, StreamingBlob};
use s3s::{S3Request, S3};
use std::sync::Arc;

#[cfg(target_family = "wasm")]
mod web;

/// The two blob stores, by the names the site shows.
pub const STORES: [&str; 2] = ["telegram", "discord"];

/// How the demo cuts objects; small chunks so a small file makes several.
pub struct Options {
    pub chunk_size: usize,
    pub block_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self { chunk_size: 4 << 20, block_size: 1 << 20 }
    }
}

pub struct Node {
    pub meta: Arc<MetaStore>,
    pub store: Arc<ChunkStore>,
    pub s3: S3Gateway,
}

fn instance(id: &str) -> InstanceInfo {
    InstanceInfo {
        id: id.to_string(),
        kind: "memory",
        fingerprint: format!("mem:{id}"),
        location: format!("mem:{id}"),
        role: InstanceRole::ReadWrite,
    }
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

impl Node {
    pub async fn open(opts: &Options) -> Result<Self> {
        let log = Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
        let snapshots = Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
        let namespaces = Index::register(BlobDb::register(Namespaces::new()));
        let meta = MetaStore::open(log, snapshots, namespaces, MetaOptions::default())
            .await
            .context("boot metadata tablet")?;
        let db = BlobDb::open(meta.clone()).await.context("open chunk metadata")?;

        let members = STORES
            .iter()
            .map(|id| {
                let mem = Arc::new(MemoryBlobStore::new().with_instance_info(instance(id)));
                MetricsBackend::wrap(Arc::new(erase(mem)))
            })
            .collect();
        let replicated = Replicated::new(members, STORES.len(), Arc::new(CheapestFirst::new()))?;

        let mut s3_opts = S3Options::for_test();
        s3_opts.chunk_size = opts.chunk_size;
        s3_opts.block_size = opts.block_size;
        let mut ingest = IngestOptions::new(s3_opts.chunk_size, s3_opts.chunk_codec);
        ingest.block_size = s3_opts.block_size;
        let store = Arc::new(ChunkStore::open_replicated(db, Arc::new(replicated), ingest).await?);
        let s3 = S3Gateway::new(s3_opts, Index::open(meta.clone()).await?, store.clone());
        Ok(Self { meta, store, s3 })
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.s3.create_bucket(req(CreateBucketInput { bucket: bucket.into(), ..Default::default() })).await?;
        Ok(())
    }

    pub async fn put(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        let input = PutObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(body)),
            ..Default::default()
        };
        self.s3.put_object(req(input)).await?;
        Ok(())
    }

    /// The object, or the bytes `first..=last` of it.
    pub async fn get(&self, bucket: &str, key: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let input = GetObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            range: range.map(|(first, last)| s3s::dto::Range::Int { first, last: Some(last) }),
            ..Default::default()
        };
        let out = self.s3.get_object(req(input)).await?;
        let mut body = out.output.body.context("no body")?;
        let mut buf = BytesMut::new();
        while let Some(chunk) = futures_next(&mut body).await {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    }

    pub async fn copy(&self, bucket: &str, from: &str, to: &str) -> Result<()> {
        let input = CopyObjectInput::builder()
            .bucket(bucket.into())
            .key(to.into())
            .copy_source(s3s::dto::CopySource::Bucket { bucket: bucket.into(), key: from.into(), version_id: None })
            .build()?;
        self.s3.copy_object(req(input)).await?;
        Ok(())
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let input = DeleteObjectInput { bucket: bucket.into(), key: key.into(), ..Default::default() };
        self.s3.delete_object(req(input)).await?;
        Ok(())
    }

    /// The keys in `bucket` with their sizes.
    pub async fn list(&self, bucket: &str) -> Result<Vec<(String, i64)>> {
        let input = ListObjectsV2Input { bucket: bucket.into(), ..Default::default() };
        let out = self.s3.list_objects_v2(req(input)).await?;
        Ok(out
            .output
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|o| (o.key.unwrap_or_default(), o.size.unwrap_or(0)))
            .collect())
    }

    /// One repair pass: copy chunks that lost a replica back to full.
    pub async fn repair(&self) -> Result<RepairStats> {
        let repairer = Repairer::new(self.store.db().clone(), self.store.replicated().clone(), RepairConfig::default());
        repairer.repair_once().await
    }

    /// One sweeper pass: delete posts that no chunk points to.
    pub async fn sweep(&self) -> Result<SweepStats> {
        let replicated = self.store.replicated();
        let sweeper = Sweeper::new(
            self.store.db().clone(),
            replicated.members().to_vec(),
            SweepConfig::default(),
            replicated.pins().clone(),
        );
        sweeper.sweep_once().await
    }
}

async fn futures_next(body: &mut StreamingBlob) -> Option<Result<Bytes>> {
    use futures::StreamExt;
    body.next().await.map(|r| r.map_err(|e| anyhow::anyhow!("{e}")))
}
