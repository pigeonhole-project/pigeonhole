use crate::cas::CasStore;
use crate::cas_index::CasIndex;
use crate::config::BytestreamConfig;
use crate::digest::{digest_hash_hex, verify_sha256};
use crate::reapi::{
    action_cache_server::{ActionCache, ActionCacheServer},
    batch_read_blobs_response, batch_update_blobs_response,
    capabilities_server::{Capabilities, CapabilitiesServer},
    compressor, content_addressable_storage_server::{
        ContentAddressableStorage, ContentAddressableStorageServer,
    },
    digest_function, ActionCacheUpdateCapabilities, ActionResult, BatchReadBlobsRequest,
    BatchReadBlobsResponse, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse,
    FindMissingBlobsRequest, FindMissingBlobsResponse, GetActionResultRequest,
    GetCapabilitiesRequest, GetChunkMappingRequest, GetTreeRequest, RegisterChunkMappingRequest,
    RegisterChunkMappingResponse, ServerCapabilities, SplitBlobRequest, SplitBlobResponse,
    SpliceBlobRequest, SpliceBlobResponse, UpdateActionResultRequest,
};
use crate::bytestream_pb::byte_stream_server::{ByteStream, ByteStreamServer};
use crate::bytestream_pb::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use prost::Message;
use pigeonhole_chunk_store::ChunkStore;
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

/// gRPC default is 4 MiB; ByteStream / Batch* need headroom above a single write frame.
const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

macro_rules! limit_svc {
    ($svc:expr) => {
        $svc.max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    };
}

#[derive(Clone)]
pub struct ReapiState {
    cas: CasStore,
    instance: String,
    max_batch_total_size_bytes: i64,
    upload_ttl: Duration,
    uploads: Arc<Mutex<HashMap<String, PendingUpload>>>,
}

struct PendingUpload {
    hash_hex: String,
    size: i64,
    committed_size: i64,
    complete: bool,
    last_activity: Instant,
}

fn make_state(cfg: &BytestreamConfig, cas: CasIndex, store: Arc<ChunkStore>) -> ReapiState {
    let state = ReapiState {
        cas: CasStore::new(cas, store),
        instance: cfg.instance_name.clone(),
        max_batch_total_size_bytes: cfg.max_batch_total_size_bytes,
        upload_ttl: Duration::from_secs(cfg.upload_ttl_secs.max(60)),
        uploads: Arc::new(Mutex::new(HashMap::new())),
    };
    spawn_cas_gc(state.clone(), cfg.gc_ttl_secs);
    spawn_upload_gc(state.clone());
    state
}

pub async fn serve(cfg: BytestreamConfig, cas: CasIndex, store: Arc<ChunkStore>) -> Result<()> {
    let state = make_state(&cfg, cas, store);

    let addr = cfg.listen_addr.parse().context("parse bytestream listen_addr")?;
    info!(%addr, instance = %cfg.instance_name, "REAPI bytestream listening");

    tonic::transport::Server::builder()
        .add_service(limit_svc!(CapabilitiesServer::new(state.clone())))
        .add_service(limit_svc!(ContentAddressableStorageServer::new(
            state.clone()
        )))
        .add_service(limit_svc!(ActionCacheServer::new(state.clone())))
        .add_service(limit_svc!(ByteStreamServer::new(state)))
        .serve(addr)
        .await
        .context("bytestream serve")?;
    Ok(())
}

/// Bind `127.0.0.1:0` and return `(addr, join handle)` for in-process tests.
pub async fn serve_ephemeral(
    cfg: BytestreamConfig,
    cas: CasIndex,
    store: Arc<ChunkStore>,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral")?;
    let addr = listener.local_addr()?;
    let mut cfg = cfg;
    cfg.listen_addr = addr.to_string();

    let state = make_state(&cfg, cas, store);

    let svc_cap = CapabilitiesServer::new(state.clone());
    let svc_cas = ContentAddressableStorageServer::new(state.clone());
    let svc_ac = ActionCacheServer::new(state.clone());
    let svc_bs = ByteStreamServer::new(state);

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(limit_svc!(svc_cap))
            .add_service(limit_svc!(svc_cas))
            .add_service(limit_svc!(svc_ac))
            .add_service(limit_svc!(svc_bs))
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .context("ephemeral serve")?;
        Ok(())
    });
    Ok((addr, handle))
}

fn spawn_upload_gc(state: ReapiState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let ttl = state.upload_ttl;
            let mut uploads = state.uploads.lock().await;
            let now = Instant::now();
            uploads.retain(|_, u| now.duration_since(u.last_activity) < ttl);
        }
    });
}

fn spawn_cas_gc(state: ReapiState, ttl_secs: u64) {
    if ttl_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let ttl = ChronoDuration::seconds(ttl_secs as i64);
        let cas = state.cas.cas.clone();
        let store = state.cas.store.clone();
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await;
            let cutoff = Utc::now() - ttl;
            if let Ok(stale) = cas.stale_before(cutoff).await {
                for row in stale {
                    match cas.release(&row.hash, row.size).await {
                        Ok(ids) => {
                            if let Err(e) = store.release(&ids).await {
                                warn!(
                                    error = %e,
                                    hash = %row.hash,
                                    size = row.size,
                                    "cas gc chunk release"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                hash = %row.hash,
                                size = row.size,
                                "cas gc release"
                            );
                        }
                    }
                }
            }
        }
    });
}

fn grpc_status(code: tonic::Code, msg: impl Into<String>) -> Status {
    Status::new(code, msg.into())
}

#[tonic::async_trait]
impl Capabilities for ReapiState {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        Ok(Response::new(ServerCapabilities {
            cache_capabilities: Some(crate::reapi::CacheCapabilities {
                digest_functions: vec![digest_function::Value::Sha256 as i32],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                max_batch_total_size_bytes: self.max_batch_total_size_bytes,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for ReapiState {
    type GetTreeStream = ReceiverStream<Result<crate::reapi::GetTreeResponse, Status>>;
    type GetChunkMappingStream =
        ReceiverStream<Result<crate::reapi::GetChunkMappingResponse, Status>>;

    async fn get_tree(
        &self,
        _request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(grpc_status(tonic::Code::Unimplemented, "GetTree not supported"))
    }

    async fn split_blob(
        &self,
        _request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        Err(grpc_status(
            tonic::Code::Unimplemented,
            "SplitBlob not supported",
        ))
    }

    async fn get_chunk_mapping(
        &self,
        _request: Request<GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Err(grpc_status(
            tonic::Code::Unimplemented,
            "GetChunkMapping not supported",
        ))
    }

    async fn splice_blob(
        &self,
        _request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        Err(grpc_status(
            tonic::Code::Unimplemented,
            "SpliceBlob not supported",
        ))
    }

    async fn register_chunk_mapping(
        &self,
        _request: Request<Streaming<RegisterChunkMappingRequest>>,
    ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
        Err(grpc_status(
            tonic::Code::Unimplemented,
            "RegisterChunkMapping not supported",
        ))
    }

    async fn find_missing_blobs(
        &self,
        request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let req = request.into_inner();
        let missing = self
            .cas
            .find_missing(&req.blob_digests)
            .await
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?;
        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests: missing,
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let req = request.into_inner();
        let mut total = 0i64;
        let mut responses = Vec::with_capacity(req.requests.len());
        for r in req.requests {
            total += r.data.len() as i64;
            if total > self.max_batch_total_size_bytes {
                return Err(grpc_status(
                    tonic::Code::InvalidArgument,
                    "batch exceeds max_batch_total_size_bytes",
                ));
            }
            let digest = r
                .digest
                .as_ref()
                .ok_or_else(|| grpc_status(tonic::Code::InvalidArgument, "missing digest"))?;
            let hash_hex = digest_hash_hex(digest)
                .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
            match verify_sha256(&r.data, &hash_hex, digest.size_bytes) {
                Ok(()) => {
                    if let Err(e) = self
                        .cas
                        .put_bytes(&hash_hex, digest.size_bytes, Bytes::from(r.data))
                        .await
                    {
                        responses.push(batch_update_blobs_response::Response {
                            digest: Some(digest.clone()),
                            status: Some(google_rpc_status(tonic::Code::Internal, e.to_string())),
                        });
                    } else {
                        responses.push(batch_update_blobs_response::Response {
                            digest: Some(digest.clone()),
                            status: None,
                        });
                    }
                }
                Err(e) => {
                    responses.push(batch_update_blobs_response::Response {
                        digest: Some(digest.clone()),
                        status: Some(google_rpc_status(
                            tonic::Code::InvalidArgument,
                            e.to_string(),
                        )),
                    });
                }
            }
        }
        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let req = request.into_inner();
        let mut total = 0i64;
        let mut responses = Vec::with_capacity(req.digests.len());
        for digest in req.digests {
            total += digest.size_bytes;
            if total > self.max_batch_total_size_bytes {
                return Err(grpc_status(
                    tonic::Code::InvalidArgument,
                    "batch exceeds max_batch_total_size_bytes",
                ));
            }
            let hash_hex = digest_hash_hex(&digest)
                .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
            match self.cas.get_bytes(&hash_hex, digest.size_bytes).await {
                Ok(Some(data)) => {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(digest),
                        data: data.to_vec(),
                        compressor: compressor::Value::Identity as i32,
                        status: None,
                    });
                }
                Ok(None) => {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(digest),
                        data: Vec::new(),
                        compressor: compressor::Value::Identity as i32,
                        status: Some(google_rpc_status(
                            tonic::Code::NotFound,
                            "missing blob".into(),
                        )),
                    });
                }
                Err(e) => {
                    responses.push(batch_read_blobs_response::Response {
                        digest: Some(digest),
                        data: Vec::new(),
                        compressor: compressor::Value::Identity as i32,
                        status: Some(google_rpc_status(tonic::Code::Internal, e.to_string())),
                    });
                }
            }
        }
        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }
}

#[tonic::async_trait]
impl ActionCache for ReapiState {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        let digest = req
            .action_digest
            .as_ref()
            .ok_or_else(|| grpc_status(tonic::Code::InvalidArgument, "missing action_digest"))?;
        let hash_hex = digest_hash_hex(digest)
            .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
        let data = self
            .cas
            .get_bytes(&hash_hex, digest.size_bytes)
            .await
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?
            .ok_or_else(|| grpc_status(tonic::Code::NotFound, "missing action result"))?;
        let result = ActionResult::decode(data.as_ref())
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?;
        Ok(Response::new(result))
    }

    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let req = request.into_inner();
        let digest = req
            .action_digest
            .as_ref()
            .ok_or_else(|| grpc_status(tonic::Code::InvalidArgument, "missing action_digest"))?;
        let result = req
            .action_result
            .as_ref()
            .ok_or_else(|| grpc_status(tonic::Code::InvalidArgument, "missing action_result"))?;
        let bytes = result.encode_to_vec();
        if bytes.len() as i64 != digest.size_bytes {
            return Err(grpc_status(
                tonic::Code::InvalidArgument,
                "action_result encoded size does not match action_digest.size_bytes",
            ));
        }
        let hash_hex = digest_hash_hex(digest)
            .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
        verify_sha256(&bytes, &hash_hex, digest.size_bytes)
            .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
        self.cas
            .put_bytes(&hash_hex, digest.size_bytes, Bytes::from(bytes))
            .await
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?;
        Ok(Response::new(result.clone()))
    }

}

#[tonic::async_trait]
impl ByteStream for ReapiState {
    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        let (hash_hex, size) = parse_blob_resource(&self.instance, &req.resource_name)
            .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
        let offset = req.read_offset;
        if offset < 0 || offset > size {
            return Err(grpc_status(tonic::Code::OutOfRange, "read_offset out of range"));
        }
        let mut body = self
            .cas
            .read_range(&hash_hex, size, offset, req.read_limit)
            .await
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?
            .ok_or_else(|| grpc_status(tonic::Code::NotFound, "blob not found"))?;

        let (tx, rx) = mpsc::channel(4);
        const CHUNK: usize = 256 * 1024;
        tokio::spawn(async move {
            let mut pending = Bytes::new();
            loop {
                while pending.len() >= CHUNK {
                    let piece = pending.slice(0..CHUNK);
                    pending = pending.slice(CHUNK..);
                    if tx
                        .send(Ok(ReadResponse {
                            data: piece.to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                match body.next().await {
                    Some(Ok(more)) => {
                        if pending.is_empty() {
                            pending = more;
                        } else {
                            let mut buf = Vec::with_capacity(pending.len() + more.len());
                            buf.extend_from_slice(&pending);
                            buf.extend_from_slice(&more);
                            pending = Bytes::from(buf);
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx
                            .send(Err(grpc_status(tonic::Code::Internal, e.to_string())))
                            .await;
                        return;
                    }
                    None => {
                        if !pending.is_empty() {
                            let _ = tx
                                .send(Ok(ReadResponse {
                                    data: pending.to_vec(),
                                }))
                                .await;
                        }
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let mut resource_name = String::new();
        let mut upload_key = String::new();
        let mut expected_hash = String::new();
        let mut expected_size = 0i64;
        let mut hasher = Sha256::new();
        let mut committed_size = 0i64;
        let mut finished = false;

        let (body_tx, body_rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(8);
        let mut body_tx = Some(body_tx);
        let mut body_rx = Some(body_rx);
        let mut ingest_handle: Option<tokio::task::JoinHandle<Result<()>>> = None;

        while let Some(msg) = stream.next().await {
            let req = msg.map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?;
            if resource_name.is_empty() {
                resource_name = req.resource_name.clone();
                let parsed = parse_upload_resource(&self.instance, &resource_name)
                    .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
                upload_key = parsed.upload_id;
                expected_hash = parsed.hash_hex;
                expected_size = parsed.size;

                let cas = self.cas.clone();
                let hash = expected_hash.clone();
                let size = expected_size;
                let rx = body_rx
                    .take()
                    .ok_or_else(|| grpc_status(tonic::Code::Internal, "duplicate upload init"))?;
                ingest_handle = Some(tokio::spawn(async move {
                    cas.put_stream(&hash, size, ReceiverStream::new(rx)).await
                }));

                let mut uploads = self.uploads.lock().await;
                uploads.insert(
                    upload_key.clone(),
                    PendingUpload {
                        hash_hex: expected_hash.clone(),
                        size: expected_size,
                        committed_size: 0,
                        complete: false,
                        last_activity: Instant::now(),
                    },
                );
            } else if req.resource_name != resource_name {
                return Err(grpc_status(
                    tonic::Code::InvalidArgument,
                    "resource_name changed mid-stream",
                ));
            }

            if req.write_offset != committed_size {
                return Err(grpc_status(
                    tonic::Code::InvalidArgument,
                    format!(
                        "write_offset {} != committed {committed_size}",
                        req.write_offset
                    ),
                ));
            }

            if !req.data.is_empty() {
                hasher.update(&req.data);
                committed_size += req.data.len() as i64;
                let tx = body_tx.as_ref().ok_or_else(|| {
                    grpc_status(tonic::Code::Internal, "ingest channel missing")
                })?;
                if tx.send(Ok(Bytes::from(req.data))).await.is_err() {
                    return Err(grpc_status(
                        tonic::Code::Internal,
                        "ingest channel closed",
                    ));
                }
            }

            {
                let mut uploads = self.uploads.lock().await;
                if let Some(u) = uploads.get_mut(&upload_key) {
                    u.committed_size = committed_size;
                    u.last_activity = Instant::now();
                    if req.finish_write {
                        u.complete = true;
                    }
                }
            }

            if req.finish_write {
                finished = true;
                break;
            }
        }

        // Close ingest body so put_stream can finish.
        drop(body_tx.take());

        let Some(handle) = ingest_handle else {
            return Err(grpc_status(
                tonic::Code::InvalidArgument,
                "empty write stream",
            ));
        };

        if !finished {
            let _ = handle.await;
            let mut uploads = self.uploads.lock().await;
            uploads.remove(&upload_key);
            return Err(grpc_status(
                tonic::Code::InvalidArgument,
                "finish_write not set",
            ));
        }

        let digest = hex::encode(hasher.finalize());
        if digest != expected_hash || committed_size != expected_size {
            // Finish/cancel ingest; remove any CAS row written under the claimed digest.
            let _ = handle.await;
            if let Ok(ids) = self.cas.cas.release(&expected_hash, expected_size).await {
                let _ = self.cas.store.release(&ids).await;
            }
            let mut uploads = self.uploads.lock().await;
            uploads.remove(&upload_key);
            return Err(grpc_status(
                tonic::Code::InvalidArgument,
                "digest mismatch",
            ));
        }

        handle
            .await
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?
            .map_err(|e| grpc_status(tonic::Code::Internal, e.to_string()))?;

        let mut uploads = self.uploads.lock().await;
        uploads.remove(&upload_key);

        Ok(Response::new(WriteResponse { committed_size }))
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let req = request.into_inner();
        let parsed = parse_upload_resource(&self.instance, &req.resource_name)
            .map_err(|e| grpc_status(tonic::Code::InvalidArgument, e.to_string()))?;
        let uploads = self.uploads.lock().await;
        let Some(upload) = uploads.get(&parsed.upload_id) else {
            return Err(grpc_status(tonic::Code::NotFound, "unknown upload"));
        };
        Ok(Response::new(QueryWriteStatusResponse {
            committed_size: upload.committed_size,
            complete: upload.complete,
        }))
    }
}

fn google_rpc_status(code: tonic::Code, message: String) -> crate::google::rpc::Status {
    crate::google::rpc::Status {
        code: code as i32,
        message,
        details: Vec::new(),
    }
}

struct UploadResource {
    upload_id: String,
    hash_hex: String,
    size: i64,
}

fn parse_blob_resource(instance: &str, name: &str) -> Result<(String, i64)> {
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() != 4 || parts[0] != instance || parts[1] != "blobs" {
        bail!("expected {{instance}}/blobs/{{hash}}/{{size}}, got {name:?}");
    }
    let size: i64 = parts[3].parse().context("size")?;
    Ok((parts[2].to_string(), size))
}

fn parse_upload_resource(instance: &str, name: &str) -> Result<UploadResource> {
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() != 6
        || parts[0] != instance
        || parts[1] != "uploads"
        || parts[3] != "blobs"
    {
        bail!("expected {{instance}}/uploads/{{uuid}}/blobs/{{hash}}/{{size}}, got {name:?}");
    }
    let size: i64 = parts[5].parse().context("size")?;
    Ok(UploadResource {
        upload_id: parts[2].to_string(),
        hash_hex: parts[4].to_string(),
        size,
    })
}

#[cfg(test)]
mod resource_parse {
    use super::*;

    #[test]
    fn blob_path() {
        let (h, s) = parse_blob_resource("s3gram", "s3gram/blobs/abc/42").unwrap();
        assert_eq!(h, "abc");
        assert_eq!(s, 42);
    }

    #[test]
    fn upload_path() {
        let u = parse_upload_resource("s3gram", "s3gram/uploads/u1/blobs/dead/9").unwrap();
        assert_eq!(u.upload_id, "u1");
        assert_eq!(u.hash_hex, "dead");
        assert_eq!(u.size, 9);
    }
}
