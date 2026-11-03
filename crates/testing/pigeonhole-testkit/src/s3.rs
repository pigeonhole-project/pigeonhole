//! For tests of the S3 gateway, called directly (no HTTP): requests,
//! bodies, self-checking payloads, and a node (gateway, chunk store, sweeper)
//! over a tablet and backends the test chooses.

use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_blob::{CheapestFirst, Replicated, SharedBackend};
use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions, SweepConfig, Sweeper};
use pigeonhole_gateway_s3::{Index, S3Gateway, S3Options};
use pigeonhole_meta::{MetaStore, Namespaces};
use s3s::dto::{GetObjectInput, StreamingBlob};
use s3s::{S3ErrorCode, S3Request, S3};
use std::sync::Arc;

/// `input` as a request s3s would hand the gateway (no credentials).
pub fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: http::Uri::from_static("/"),
        headers: http::HeaderMap::new(),
        extensions: http::Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// The error code of a failed call.
pub fn code(err: s3s::S3Error) -> S3ErrorCode {
    err.code().clone()
}

/// A body read to its end (none: empty); `Err` if it broke off.
pub async fn read_body(body: Option<StreamingBlob>) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let Some(mut body) = body else { return Ok(out) };
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// [`read_body`], which must not break off.
pub async fn collect_body(body: Option<StreamingBlob>) -> Vec<u8> {
    read_body(body).await.expect("the body reads to its end")
}

/// GET `bucket/key` whole: `None` for `NoSuchKey`, `Err` for any other failure.
pub async fn get_object(s3: &S3Gateway, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
    let input = GetObjectInput { bucket: bucket.into(), key: key.into(), ..Default::default() };
    let out = match s3.get_object(req(input)).await {
        Ok(r) => r.output,
        Err(e) if *e.code() == S3ErrorCode::NoSuchKey => return Ok(None),
        Err(e) => return Err(format!("get {key}: {e:?}")),
    };
    read_body(out.body).await.map(Some).map_err(|e| format!("get {key}: body: {e}"))
}

/// Bytes of a payload's header: its seed and its length.
pub const HEADER: usize = 16;

/// `[seed u64 LE][len u64 LE][xorshift(seed) bytes…]`: any intact payload
/// checks itself ([`verify_payload`]), whatever interleaving wrote it.
pub fn payload(seed: u64, len: usize) -> Bytes {
    let len = len.max(HEADER);
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&seed.to_le_bytes());
    out.extend_from_slice(&(len as u64).to_le_bytes());
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

/// Whether `data` is a whole [`payload`], and if not, how it is not.
pub fn verify_payload(data: &[u8]) -> Result<(), String> {
    if data.len() < HEADER {
        return Err(format!("short object: {} bytes", data.len()));
    }
    let seed = u64::from_le_bytes(data[..8].try_into().unwrap());
    let len = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
    if len != data.len() {
        return Err(format!("length header {len} != body {}", data.len()));
    }
    if payload(seed, len).as_ref() != data {
        return Err(format!("damaged body (seed {seed:#x}, len {len})"));
    }
    Ok(())
}

/// [`verify_payload`] as a yes or no.
pub fn intact(data: &[u8]) -> bool {
    verify_payload(data).is_ok()
}

/// Add the namespaces of a node's tablet (s3 and chunk, as the binary has
/// them) to `namespaces`: the `register` of [`crate::tablet`].
pub fn register(namespaces: Namespaces) -> Namespaces {
    Index::register(BlobDb::register(namespaces))
}

/// A backend over `store`, as a member of a node.
pub fn member<B: pigeonhole_blob::BlobBackend>(store: Arc<B>) -> SharedBackend {
    Arc::new(pigeonhole_blob::erase(store))
}

/// Ingest options: `chunk_size` and `block_size`, zstd.
pub fn ingest(chunk_size: usize, block_size: usize) -> IngestOptions {
    let mut opts = IngestOptions::new(chunk_size, pigeonhole_chunk_store::ChunkCodec::Zstd);
    opts.block_size = block_size;
    opts
}

/// A gateway and its chunk store over one tablet, as the binary runs them.
pub struct Node {
    pub s3: S3Gateway,
    pub store: Arc<ChunkStore>,
    /// The store's members, in the order given.
    pub backends: Vec<SharedBackend>,
    pub meta: Arc<MetaStore>,
}

impl Node {
    /// A gateway over `meta` (opened with [`register`]) whose chunks go to
    /// every one of `backends` (one success is enough), uploads pinned
    /// until committed as in the binary.
    pub async fn open(
        meta: Arc<MetaStore>,
        backends: Vec<SharedBackend>,
        ingest: IngestOptions,
        opts: S3Options,
    ) -> Self {
        let rep =
            Arc::new(Replicated::new(backends.clone(), 1, Arc::new(CheapestFirst::new())).expect("a replica set"));
        let db = BlobDb::open(meta.clone()).await.expect("the chunk namespace");
        let store = Arc::new(ChunkStore::open_replicated(db, rep, ingest).await.expect("open the chunk store"));
        let index = Index::open(meta.clone()).await.expect("the s3 namespace");
        let s3 = pigeonhole_gateway_s3::build_s3_gateway(opts, index, store.clone());
        Self { s3, store, backends, meta }
    }

    /// A sweeper over every member, `batch_size` keys a page, no pause.
    pub fn sweeper(&self, batch_size: usize) -> Sweeper {
        Sweeper::new(
            self.store.db().clone(),
            self.backends.clone(),
            SweepConfig { batch_size, interval: None },
            self.store.replicated().pins().clone(),
        )
    }
}
