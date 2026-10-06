//! In-process REAPI tests (localhost only, no external network).

use bytes::Bytes;
use s3gram_blob::{BlobStore, MemoryBlobStore};
use s3gram_bytestream::config::BytestreamConfig;
use s3gram_bytestream::digest::sha256_hex;
use s3gram_bytestream::google::bytestream::byte_stream_client::ByteStreamClient;
use s3gram_bytestream::google::bytestream::{ReadRequest, WriteRequest};
use s3gram_bytestream::reapi::content_addressable_storage_client::ContentAddressableStorageClient;
use s3gram_bytestream::reapi::{digest_function, Digest, FindMissingBlobsRequest};
use s3gram_bytestream::server::serve_ephemeral;
use s3gram_index::Index;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

async fn test_harness() -> (
    ContentAddressableStorageClient<Channel>,
    ByteStreamClient<Channel>,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}?mode=rwc", dir.path().join("t.db").display());
    let index = Index::connect(&url).await.unwrap();
    let mem = Arc::new(MemoryBlobStore::new());
    let store: Arc<dyn BlobStore> = mem.clone();
    let cfg = BytestreamConfig {
        enabled: true,
        instance_name: "s3gram".into(),
        gc_ttl_secs: 0,
        ..Default::default()
    };
    let (addr, _handle) = serve_ephemeral(cfg, index, store, String::new())
        .await
        .unwrap();
    let uri = format!("http://{addr}");
    let channel = Channel::from_shared(uri).unwrap().connect().await.unwrap();
    let cas = ContentAddressableStorageClient::new(channel.clone());
    let bs = ByteStreamClient::new(channel);
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
async fn find_missing_after_bytestream_write() {
    let (mut cas, mut bs, inst) = test_harness().await;
    let payload = Bytes::from_static(b"find-me");
    let d = digest_for(&payload);

    let missing = cas
        .find_missing_blobs(find_req(&inst, vec![d.clone()]))
        .await
        .unwrap()
        .into_inner()
        .missing_blob_digests;
    assert_eq!(missing.len(), 1);

    let upload_id = uuid::Uuid::new_v4().to_string();
    let resource = format!("{inst}/uploads/{upload_id}/blobs/{}/{}", d.hash, d.size_bytes);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(WriteRequest {
        resource_name: resource,
        write_offset: 0,
        finish_write: true,
        data: payload.to_vec(),
    })
    .await
    .unwrap();
    drop(tx);
    bs.write(ReceiverStream::new(rx)).await.unwrap();

    let missing = cas
        .find_missing_blobs(find_req(&inst, vec![d.clone()]))
        .await
        .unwrap()
        .into_inner()
        .missing_blob_digests;
    assert!(missing.is_empty());
}

#[tokio::test]
async fn bytestream_read_range() {
    let (_cas, mut bs, inst) = test_harness().await;
    let payload = b"0123456789abcdef".to_vec();
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
            read_offset: 4,
            read_limit: 6,
        })
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        got.extend_from_slice(&chunk.data);
    }
    assert_eq!(got, b"456789");
}
