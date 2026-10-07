//! In-process REAPI tests (localhost only, no external network).

use bytes::Bytes;
use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions};
use pigeonhole_codec::ChunkCodec;
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_gateway_bytestream::cas_index::CasIndex;
use pigeonhole_gateway_bytestream::config::BytestreamConfig;
use pigeonhole_gateway_bytestream::digest::sha256_hex;
use pigeonhole_gateway_bytestream::google::bytestream::byte_stream_client::ByteStreamClient;
use pigeonhole_gateway_bytestream::google::bytestream::{ReadRequest, WriteRequest};
use pigeonhole_gateway_bytestream::reapi::content_addressable_storage_client::ContentAddressableStorageClient;
use pigeonhole_gateway_bytestream::reapi::{digest_function, Digest, FindMissingBlobsRequest};
use pigeonhole_gateway_bytestream::server::serve_ephemeral;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

async fn test_harness() -> (
    ContentAddressableStorageClient<Channel>,
    ByteStreamClient<Channel>,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let cas_url = format!("sqlite:{}?mode=rwc", dir.path().join("cas.db").display());
    let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
    let cas = CasIndex::connect(&cas_url).await.unwrap();
    let db = BlobDb::connect(&blob_url).await.unwrap();
    let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
    opts.block_size = 64 * 1024;
    let store = Arc::new(
        ChunkStore::open(db, MemoryBlobStore::new(), opts)
            .await
            .unwrap(),
    );
    let cfg = BytestreamConfig {
        enabled: true,
        instance_name: "s3gram".into(),
        gc_ttl_secs: 0,
        ..Default::default()
    };
    let (addr, _handle) = serve_ephemeral(cfg, cas, store).await.unwrap();
    let uri = format!("http://{addr}");
    let channel = Channel::from_shared(uri).unwrap().connect().await.unwrap();
    const MAX_MSG: usize = 32 * 1024 * 1024;
    let cas = ContentAddressableStorageClient::new(channel.clone())
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    let bs = ByteStreamClient::new(channel)
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    (cas, bs, "s3gram".into())
}

fn digest_for(data: &[u8]) -> Digest {
    Digest {
        hash: sha256_hex(data),
        size_bytes: data.len() as i64,
    }
}

fn find_req(instance: &str, blob_digests: Vec<Digest>) -> FindMissingBlobsRequest {
    FindMissingBlobsRequest {
        instance_name: instance.to_string(),
        blob_digests,
        digest_function: digest_function::Value::Sha256 as i32,
    }
}

#[tokio::test]
async fn bytestream_write_read_large_blob() {
    let (_cas, mut bs, inst) = test_harness().await;
    let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let d = digest_for(&payload);
    let upload_id = uuid::Uuid::new_v4().to_string();
    let resource = format!("{inst}/uploads/{upload_id}/blobs/{}/{}", d.hash, d.size_bytes);

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(WriteRequest {
        resource_name: resource.clone(),
        write_offset: 0,
        finish_write: false,
        data: payload[..256 * 1024].to_vec(),
    })
    .await
    .unwrap();
    tx.send(WriteRequest {
        resource_name: resource.clone(),
        write_offset: 256 * 1024,
        finish_write: true,
        data: payload[256 * 1024..].to_vec(),
    })
    .await
    .unwrap();
    drop(tx);

    let resp = bs
        .write(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.committed_size, payload.len() as i64);

    let read_name = format!("{inst}/blobs/{}/{}", d.hash, d.size_bytes);
    let mut stream = bs
        .read(ReadRequest {
            resource_name: read_name,
            read_offset: 0,
            read_limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        got.extend_from_slice(&chunk.data);
    }
    assert_eq!(got, payload);
}

#[tokio::test]
async fn bytestream_wrong_hash_rejected() {
    let (_cas, mut bs, inst) = test_harness().await;
    let payload = b"hello cas".to_vec();
    let mut d = digest_for(&payload);
    d.hash = "0".repeat(64);
    let upload_id = uuid::Uuid::new_v4().to_string();
    let resource = format!("{inst}/uploads/{upload_id}/blobs/{}/{}", d.hash, d.size_bytes);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteRequest {
        resource_name: resource,
        write_offset: 0,
        finish_write: true,
        data: payload,
    })
    .await
    .unwrap();
    drop(tx);
    let err = bs.write(ReceiverStream::new(rx)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn find_missing_and_batch() {
    let (mut cas, mut bs, inst) = test_harness().await;
    let a = b"alpha".to_vec();
    let b = b"bravo".to_vec();
    let da = digest_for(&a);
    let db = digest_for(&b);

    let missing = cas
        .find_missing_blobs(find_req(&inst, vec![da.clone(), db.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(missing.missing_blob_digests.len(), 2);

    let upload_id = uuid::Uuid::new_v4().to_string();
    let resource = format!("{inst}/uploads/{upload_id}/blobs/{}/{}", da.hash, da.size_bytes);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteRequest {
        resource_name: resource,
        write_offset: 0,
        finish_write: true,
        data: a.clone(),
    })
    .await
    .unwrap();
    drop(tx);
    bs.write(ReceiverStream::new(rx)).await.unwrap();

    let missing = cas
        .find_missing_blobs(find_req(&inst, vec![da.clone(), db.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(missing.missing_blob_digests.len(), 1);
    assert_eq!(missing.missing_blob_digests[0].hash, db.hash);

    // Repeat write → retain path (no error).
    let upload_id2 = uuid::Uuid::new_v4().to_string();
    let resource2 = format!(
        "{inst}/uploads/{upload_id2}/blobs/{}/{}",
        da.hash, da.size_bytes
    );
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteRequest {
        resource_name: resource2,
        write_offset: 0,
        finish_write: true,
        data: a,
    })
    .await
    .unwrap();
    drop(tx);
    let resp = bs
        .write(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.committed_size, da.size_bytes);
}

#[tokio::test]
async fn bytestream_read_range() {
    let (_cas, mut bs, inst) = test_harness().await;
    let payload: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
    let d = digest_for(&payload);
    let upload_id = uuid::Uuid::new_v4().to_string();
    let resource = format!("{inst}/uploads/{upload_id}/blobs/{}/{}", d.hash, d.size_bytes);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteRequest {
        resource_name: resource,
        write_offset: 0,
        finish_write: true,
        data: payload.clone(),
    })
    .await
    .unwrap();
    drop(tx);
    bs.write(ReceiverStream::new(rx)).await.unwrap();

    let read_name = format!("{inst}/blobs/{}/{}", d.hash, d.size_bytes);
    let mut stream = bs
        .read(ReadRequest {
            resource_name: read_name,
            read_offset: 100,
            read_limit: 50,
        })
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        got.extend_from_slice(&chunk.data);
    }
    assert_eq!(got, &payload[100..150]);
    let _ = Bytes::new();
}
