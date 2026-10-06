//! In-process S3 API tests over MemoryBlobStore + temp SQLite.

use bytes::Bytes;
use futures::StreamExt;
use s3gram::config::Config;
use s3gram::index::Index;
use s3gram::storage::{BlobStore, MemoryBlobStore};
use s3gram::build_s3gram;
use http::{HeaderMap, Method, Uri};
use http::Extensions;
use s3s::dto::*;
use s3s::{S3, S3Request};
use std::sync::Arc;

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

async fn setup() -> (s3gram::service::S3gram, Arc<MemoryBlobStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.db");
    let url = format!("sqlite:{}?mode=rwc", db.display());
    let cfg = Config::for_test(&url);
    let index = Index::connect(&url).await.unwrap();
    let mem = Arc::new(MemoryBlobStore::new());
    let store: Arc<dyn BlobStore> = mem.clone();
    let s3 = build_s3gram(cfg, index, store);
    (s3, mem, dir)
}

async fn collect_body(body: Option<StreamingBlob>) -> Vec<u8> {
    let mut out = Vec::new();
    let Some(mut body) = body else {
        return out;
    };
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test]
async fn empty_object_put_get() {
    let (s3, mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let put = s3
        .put_object(req(PutObjectInput {
            bucket: "demo".into(),
            key: "dir/".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::new())),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(put.e_tag.is_some());

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "dir/".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.content_length, Some(0));
    assert!(collect_body(got.body).await.is_empty());
    assert_eq!(mem.len(), 0);
}

#[tokio::test]
async fn put_get_roundtrip_and_range() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let data = Bytes::from(vec![7u8; 100_000]);
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "big.bin".into(),
        body: Some(StreamingBlob::from_bytes(data.clone())),
        ..Default::default()
    }))
    .await
    .unwrap();

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "big.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(collect_body(got.body).await, data.as_ref());

    let ranged = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "big.bin".into(),
            range: Some(Range::Int {
                first: 10,
                last: Some(19),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(ranged.content_length, Some(10));
    assert_eq!(collect_body(ranged.body).await, &data[10..20]);
}

#[tokio::test]
async fn content_md5_mismatch_rejected() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "demo".into(),
            key: "x".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"abc"))),
            // Valid 16-byte MD5 digest, but not matching body "abc"
            content_md5: Some("rL0Y20xC+Fzt72VPzMSk2A==".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), s3s::S3ErrorCode::BadDigest);
}

#[tokio::test]
async fn content_md5_invalid_short_is_invalid_digest() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let err = s3
        .put_object(req(PutObjectInput {
            bucket: "demo".into(),
            key: "x".into(),
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"bar"))),
            // base64("abracadabra") — valid base64 but not 16-byte MD5
            content_md5: Some("YWJyYWNhZGFicmE=".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidDigest);
}

#[tokio::test]
async fn get_bucket_location_and_list_v1() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "a.txt".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"hi"))),
        ..Default::default()
    }))
    .await
    .unwrap();

    let loc = s3
        .get_bucket_location(req(GetBucketLocationInput {
            bucket: "demo".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(loc.location_constraint.is_none()); // us-east-1

    let listed = s3
        .list_objects(req(ListObjectsInput {
            bucket: "demo".into(),
            max_keys: Some(5000),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(listed.max_keys, Some(1000));
    assert_eq!(listed.contents.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn list_parts_and_multipart_uploads() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let created = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "demo".into(),
            key: "mp.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let upload_id = created.upload_id.unwrap();

    let part = s3
        .upload_part(req(UploadPartInput {
            bucket: "demo".into(),
            key: "mp.bin".into(),
            upload_id: upload_id.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"part-one"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;

    let parts = s3
        .list_parts(req(ListPartsInput {
            bucket: "demo".into(),
            key: "mp.bin".into(),
            upload_id: upload_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(parts.parts.as_ref().unwrap().len(), 1);
    assert_eq!(
        parts.parts.as_ref().unwrap()[0].e_tag.as_ref().unwrap(),
        part.e_tag.as_ref().unwrap()
    );

    let uploads = s3
        .list_multipart_uploads(req(ListMultipartUploadsInput {
            bucket: "demo".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(uploads.uploads.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn object_tagging_roundtrip() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "t.txt".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"hi"))),
        tagging: Some("Hello=World&foo=bar".into()),
        ..Default::default()
    }))
    .await
    .unwrap();

    let got = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "demo".into(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(got.tag_set.len(), 2);

    let head = s3
        .head_object(req(HeadObjectInput {
            bucket: "demo".into(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(head.tag_count, Some(2));

    s3.delete_object_tagging(req(DeleteObjectTaggingInput {
        bucket: "demo".into(),
        key: "t.txt".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let empty = s3
        .get_object_tagging(req(GetObjectTaggingInput {
            bucket: "demo".into(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert!(empty.tag_set.is_empty());
}

#[tokio::test]
async fn upload_part_copy_range() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "src.bin".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(
            b"0123456789abcdef",
        ))),
        ..Default::default()
    }))
    .await
    .unwrap();

    let created = s3
        .create_multipart_upload(req(CreateMultipartUploadInput {
            bucket: "demo".into(),
            key: "dst.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let upload_id = created.upload_id.unwrap();

    let copy_input = UploadPartCopyInput::builder()
        .bucket("demo".into())
        .key("dst.bin".into())
        .upload_id(upload_id.clone())
        .part_number(1)
        .copy_source(CopySource::Bucket {
            bucket: "demo".into(),
            key: "src.bin".into(),
            version_id: None,
        })
        .copy_source_range(Some("bytes=0-3".into()))
        .build()
        .unwrap();
    let copied = s3.upload_part_copy(req(copy_input)).await.unwrap().output;
    let part_etag = copied
        .copy_part_result
        .as_ref()
        .and_then(|r| r.e_tag.clone())
        .unwrap();

    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "demo".into(),
        key: "dst.bin".into(),
        upload_id,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                e_tag: Some(part_etag),
                part_number: Some(1),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }))
    .await
    .unwrap();

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "dst.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(collect_body(got.body).await, b"0123");
}

#[tokio::test]
async fn list_object_versions_as_null_current() {
    let (s3, _mem, _dir) = setup().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "a.txt".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"hi"))),
        ..Default::default()
    }))
    .await
    .unwrap();

    let out = s3
        .list_object_versions(req(ListObjectVersionsInput {
            bucket: "demo".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    let versions = out.versions.expect("versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].key.as_deref(), Some("a.txt"));
    assert_eq!(versions[0].version_id.as_deref(), Some("null"));
    assert_eq!(versions[0].is_latest, Some(true));
    assert_eq!(out.is_truncated, Some(false));
}
