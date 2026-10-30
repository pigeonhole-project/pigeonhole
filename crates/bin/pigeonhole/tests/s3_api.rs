//! In-process S3 API tests over ChunkStore(Memory) + temp SQLite.

use bytes::Bytes;
use futures::StreamExt;
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::{build_s3_gateway, BlobDb, ChunkStore, IngestOptions};
use http::{HeaderMap, Method, Uri};
use http::Extensions;
use s3s::dto::*;
use s3s::{S3ErrorCode, S3Request, S3Response, S3};
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

async fn setup_with_mem() -> (
    pigeonhole::service::S3Gateway,
    Arc<MemoryBlobStore>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let idx_url = dir.path().join("s3.log");
    let blob_url = dir.path().join("blob.log");
    let cfg = Config::for_test(dir.path());
    let index = Index::open(pigeonhole_testkit::tablet(&idx_url, Index::register).await).await.unwrap();
    let mem = Arc::new(MemoryBlobStore::new());
    let db = BlobDb::open(pigeonhole_testkit::tablet(&blob_url, BlobDb::register).await).await.unwrap();
    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    // ChunkStore takes ownership of a MemoryBlobStore; clone state is not shared.
    // For blob-count tests, open with a clone of the same Arc via erase — use one mem.
    let store_mem = MemoryBlobStore::new();
    // We cannot share Arc into ChunkStore::open which takes ownership.
    // Expose counts via chunk metadata instead in updated tests.
    let store = Arc::new(
        ChunkStore::open(db, store_mem, opts).await.unwrap(),
    );
    let s3 = build_s3_gateway(cfg.s3_options(), index, store);
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
    let (s3, _mem, _dir) = setup_with_mem().await;
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
    let extents = s3.index.get_extents("demo", "dir/").await.unwrap().unwrap();
    assert!(extents.is_empty());
}

#[tokio::test]
async fn put_get_roundtrip_and_range() {
    let (s3, _mem, _dir) = setup_with_mem().await;
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
async fn compressible_object_roundtrip_and_snapshot() {
    let (s3, _mem, _dir) = setup_with_mem().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

    let data = Bytes::from(vec![b'z'; 200_000]);
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "zeros.bin".into(),
        body: Some(StreamingBlob::from_bytes(data.clone())),
        ..Default::default()
    }))
    .await
    .unwrap();

    let extents = s3
        .index
        .get_extents("demo", "zeros.bin")
        .await
        .unwrap()
        .unwrap();
    assert!(!extents.is_empty());
    assert_eq!(
        extents.iter().map(|e| e.len).sum::<i64>(),
        data.len() as i64
    );

    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "zeros.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    assert_eq!(collect_body(got.body).await, data.as_ref());

    let extents = s3.index.get_extents("demo", "zeros.bin").await.unwrap().unwrap();
    assert!(!extents.is_empty());
}

#[tokio::test]
async fn content_md5_mismatch_rejected() {
    let (s3, _mem, _dir) = setup_with_mem().await;
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
            content_md5: Some("rL0Y20xC+Fzt72VPzMSk2A==".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), s3s::S3ErrorCode::BadDigest);
}

#[tokio::test]
async fn content_md5_invalid_short_is_invalid_digest() {
    let (s3, _mem, _dir) = setup_with_mem().await;
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
            content_md5: Some("YWJyYWNhZGFicmE=".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidDigest);
}

#[tokio::test]
async fn get_bucket_location_and_list_v1() {
    let (s3, _mem, _dir) = setup_with_mem().await;
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
    assert!(loc.location_constraint.is_none());

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
    let (s3, _mem, _dir) = setup_with_mem().await;
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
    let (s3, _mem, _dir) = setup_with_mem().await;
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
    let (s3, _mem, _dir) = setup_with_mem().await;
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

    let src_ext = s3
        .index
        .get_extents("demo", "src.bin")
        .await
        .unwrap()
        .unwrap();
    let src_chunk = src_ext[0].chunk;

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
    // Extent slice reuses the same chunk (retain); no re-ingest.
    let part_ext = s3
        .index
        .get_multipart_part_extents(&upload_id, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(part_ext[0].chunk, src_chunk);
    assert_eq!(part_ext[0].len, 4);
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
async fn upload_part_copy_whole_object_reuses_chunks() {
    let (s3, _mem, _dir) = setup_with_mem().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let body = Bytes::from_static(b"whole-object-shallow-copy");
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "src.bin".into(),
        body: Some(StreamingBlob::from_bytes(body.clone())),
        ..Default::default()
    }))
    .await
    .unwrap();
    let src_ext = s3
        .index
        .get_extents("demo", "src.bin")
        .await
        .unwrap()
        .unwrap();
    let src_ids: Vec<_> = src_ext.iter().map(|e| e.chunk).collect();

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
        .build()
        .unwrap();
    let copied = s3.upload_part_copy(req(copy_input)).await.unwrap().output;
    let part_ext = s3
        .index
        .get_multipart_part_extents(&upload_id, 1)
        .await
        .unwrap()
        .unwrap();
    let part_ids: Vec<_> = part_ext.iter().map(|e| e.chunk).collect();
    assert_eq!(part_ids, src_ids, "whole-object copy must reuse chunk ids");
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
    assert_eq!(collect_body(got.body).await, body.as_ref());
}

#[tokio::test]
async fn list_object_versions_as_null_current() {
    let (s3, _mem, _dir) = setup_with_mem().await;
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

#[tokio::test]
async fn delete_everything_releases_every_object_chunk() {
    let (s3, _mem, _dir) = setup_with_mem().await;
    for bucket in ["a", "b"] {
        s3.create_bucket(req(CreateBucketInput { bucket: bucket.into(), ..Default::default() }))
            .await
            .unwrap();
        for key in ["x", "y"] {
            s3.put_object(req(PutObjectInput {
                bucket: bucket.into(),
                key: key.into(),
                body: Some(StreamingBlob::from_bytes(Bytes::from(format!("{bucket}/{key}")))),
                ..Default::default()
            }))
            .await
            .unwrap();
        }
    }
    let mut held = Vec::new();
    for (bucket, key) in [("a", "x"), ("a", "y"), ("b", "x"), ("b", "y")] {
        let extents = s3.index.get_extents(bucket, key).await.unwrap().unwrap();
        held.extend(extents.iter().map(|e| e.chunk));
    }
    let mut released = s3.index.delete_everything().await.unwrap();
    released.sort_unstable();
    held.sort_unstable();
    assert_eq!(released, held);
    assert_eq!(s3.index.count_objects().await.unwrap(), 0);
    assert!(s3.index.list_buckets().await.unwrap().is_empty());
}

/// One tablet for the index and the chunk store, as in the binary.
async fn setup_one_tablet(chunk_size: usize) -> (pigeonhole::service::S3Gateway, Arc<pigeonhole_meta::MetaStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::for_test(dir.path());
    cfg.chunk_size = chunk_size;
    cfg.block_size = 64 * 1024;
    let meta = pigeonhole_testkit::tablet(dir.path().join("meta"), |ns| Index::register(BlobDb::register(ns))).await;
    let index = Index::open(meta.clone()).await.unwrap();
    let db = BlobDb::open(meta.clone()).await.unwrap();
    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    let store = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
    (build_s3_gateway(cfg.s3_options(), index, store), meta, dir)
}

#[tokio::test]
async fn a_put_is_one_log_entry_and_a_bad_digest_commits_nothing() {
    let (s3, meta, _dir) = setup_one_tablet(64 * 1024).await;
    s3.create_bucket(req(CreateBucketInput { bucket: "b".into(), ..Default::default() })).await.unwrap();
    let payload = Bytes::from((0..300_000u32).map(|i| (i * 7 % 251) as u8).collect::<Vec<_>>());
    let tagged = |body: Bytes, md5: Option<String>| PutObjectInput {
        bucket: "b".into(),
        key: "k".into(),
        body: Some(StreamingBlob::from_bytes(body)),
        tagging: Some("a=1".into()),
        content_md5: md5,
        ..Default::default()
    };

    let before = meta.applied_position();
    s3.put_object(req(tagged(payload.clone(), None))).await.unwrap();
    assert_eq!(meta.applied_position(), before + 1, "chunks, object and tags in one entry");
    let tags = s3
        .get_object_tagging(req(GetObjectTaggingInput { bucket: "b".into(), key: "k".into(), ..Default::default() }))
        .await
        .unwrap();
    assert_eq!(tags.output.tag_set.len(), 1);

    let before = meta.applied_position();
    let wrong = "AAAAAAAAAAAAAAAAAAAAAA==".to_string(); // md5 of nothing we send
    assert!(s3.put_object(req(tagged(payload.clone(), Some(wrong)))).await.is_err());
    assert_eq!(meta.applied_position(), before, "a bad digest commits nothing");

    // Overwrite: the old object's chunks are released after the answer.
    let before = meta.applied_position();
    s3.put_object(req(tagged(payload, None))).await.unwrap();
    assert_eq!(meta.applied_position(), before + 1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while meta.applied_position() < before + 2 {
        assert!(tokio::time::Instant::now() < deadline, "the release never came");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

fn code(err: s3s::S3Error) -> S3ErrorCode {
    err.code().clone()
}

fn put(key: &str, body: &str, if_match: Option<&str>, if_none_match: bool) -> PutObjectInput {
    PutObjectInput {
        bucket: "b".into(),
        key: key.into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from(body.to_string()))),
        if_match: if_match.map(|e| ETagCondition::ETag(ETag::Strong(e.into()))),
        if_none_match: if_none_match.then_some(ETagCondition::Any),
        ..Default::default()
    }
}

/// docs/conditional-writes.md § Semantics through the gateway.
#[tokio::test]
async fn conditional_writes_follow_the_table() {
    let (s3, _meta, _dir) = setup_one_tablet(64 * 1024).await;
    s3.create_bucket(req(CreateBucketInput { bucket: "b".into(), ..Default::default() })).await.unwrap();
    let etag = |r: S3Response<PutObjectOutput>| r.output.e_tag.unwrap().value().to_string();

    // Absent key: If-Match is 404, If-None-Match writes.
    let err = s3.put_object(req(put("k", "one", Some("00"), false))).await.unwrap_err();
    assert_eq!(code(err), S3ErrorCode::NoSuchKey);
    let one = etag(s3.put_object(req(put("k", "one", None, true))).await.unwrap());

    // Present: If-None-Match and a stale If-Match are 412; the current ETag writes.
    let err = s3.put_object(req(put("k", "two", None, true))).await.unwrap_err();
    assert_eq!(code(err), S3ErrorCode::PreconditionFailed);
    let err = s3.put_object(req(put("k", "two", Some("00"), false))).await.unwrap_err();
    assert_eq!(code(err), S3ErrorCode::PreconditionFailed);
    let two = etag(s3.put_object(req(put("k", "two", Some(&one), false))).await.unwrap());
    let err = s3.put_object(req(put("k", "three", Some(&one), false))).await.unwrap_err();
    assert_eq!(code(err), S3ErrorCode::PreconditionFailed, "a superseded ETag");

    // Copy onto an existing key with If-None-Match.
    let copy = CopyObjectInput::builder()
        .bucket("b".into())
        .key("k".into())
        .copy_source(CopySource::Bucket { bucket: "b".into(), key: "k".into(), version_id: None })
        .if_none_match(Some(ETagCondition::Any))
        .build()
        .unwrap();
    assert_eq!(code(s3.copy_object(req(copy)).await.unwrap_err()), S3ErrorCode::PreconditionFailed);

    // Multipart: checked at Complete against the object as it is then.
    let upload = s3
        .create_multipart_upload(req(CreateMultipartUploadInput { bucket: "b".into(), key: "k".into(), ..Default::default() }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let part = s3
        .upload_part(req(UploadPartInput {
            bucket: "b".into(),
            key: "k".into(),
            upload_id: upload.clone(),
            part_number: 1,
            body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"part"))),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output
        .e_tag;
    let complete = |cond: CompleteMultipartUploadInput| CompleteMultipartUploadInput {
        bucket: "b".into(),
        key: "k".into(),
        upload_id: upload.clone(),
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart { part_number: Some(1), e_tag: part.clone(), ..Default::default() }]),
        }),
        ..cond
    };
    let err = s3
        .complete_multipart_upload(req(complete(CompleteMultipartUploadInput {
            if_none_match: Some(ETagCondition::Any),
            ..Default::default()
        })))
        .await
        .unwrap_err();
    assert_eq!(code(err), S3ErrorCode::PreconditionFailed);
    s3.complete_multipart_upload(req(complete(CompleteMultipartUploadInput {
        if_match: Some(ETagCondition::ETag(ETag::Strong(two.clone()))),
        ..Default::default()
    })))
    .await
    .unwrap();

    // Delete with If-Match.
    let del = |e: &str| DeleteObjectInput {
        bucket: "b".into(),
        key: "k".into(),
        if_match: Some(ETagCondition::ETag(ETag::Strong(e.into()))),
        ..Default::default()
    };
    assert_eq!(code(s3.delete_object(req(del(&two))).await.unwrap_err()), S3ErrorCode::PreconditionFailed);
    let current = s3.index.get_object("b", "k").await.unwrap().unwrap().etag;
    s3.delete_object(req(del(&current))).await.unwrap();
    assert_eq!(code(s3.delete_object(req(del(&current))).await.unwrap_err()), S3ErrorCode::NoSuchKey);

    // If-Match: * (any object) and If-None-Match: <etag> (not that one), as
    // s3-tests' test_put_object_if_match has them.
    let star = |key: &str, body: &str| PutObjectInput { if_match: Some(ETagCondition::Any), ..put(key, body, None, false) };
    let not = |key: &str, body: &str, e: &str| PutObjectInput {
        if_none_match: Some(ETagCondition::ETag(ETag::Strong(e.into()))),
        ..put(key, body, None, false)
    };
    assert_eq!(code(s3.put_object(req(star("x", "1"))).await.unwrap_err()), S3ErrorCode::NoSuchKey);
    let x1 = etag(s3.put_object(req(not("x", "1", "00"))).await.unwrap());
    assert_eq!(code(s3.put_object(req(not("x", "2", &x1))).await.unwrap_err()), S3ErrorCode::PreconditionFailed);
    s3.put_object(req(not("x", "2", "00"))).await.unwrap();
    s3.put_object(req(star("x", "3"))).await.unwrap();
    let mut both = star("x", "4");
    both.if_none_match = Some(ETagCondition::Any);
    assert_eq!(code(s3.put_object(req(both)).await.unwrap_err()), S3ErrorCode::InvalidArgument);
}

/// Racing `If-None-Match: *` puts of one key: exactly one wins, the losers
/// get 412, and their chunks are released (not left referenced).
#[tokio::test]
async fn racing_creates_one_wins_and_losers_release_their_chunks() {
    let (s3, _meta, _dir) = setup_one_tablet(64 * 1024).await;
    s3.create_bucket(req(CreateBucketInput { bucket: "b".into(), ..Default::default() })).await.unwrap();
    let s3 = Arc::new(s3);
    let tasks: Vec<_> = (0..8)
        .map(|i| {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.put_object(req(put("lock", &format!("writer {i}"), None, true))).await })
        })
        .collect();
    let mut won = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => won += 1,
            Err(e) => assert_eq!(code(e), S3ErrorCode::PreconditionFailed),
        }
    }
    assert_eq!(won, 1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while s3.store.db().stats().live != 1 {
        assert!(tokio::time::Instant::now() < deadline, "losers' chunks still live: {:?}", s3.store.db().stats());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// One tablet, objects up to 1 KiB inline (docs/inline-objects.md).
async fn setup_inline() -> (pigeonhole::service::S3Gateway, Arc<pigeonhole_meta::MetaStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::for_test(dir.path());
    let meta = pigeonhole_testkit::tablet(dir.path().join("meta"), |ns| Index::register(BlobDb::register(ns))).await;
    let index = Index::open(meta.clone()).await.unwrap();
    let db = BlobDb::open(meta.clone()).await.unwrap();
    let opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    let store = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
    let s3opts = pigeonhole::service::S3Options { inline_max: Some(1024), ..cfg.s3_options() };
    let s3 = build_s3_gateway(s3opts, index, store);
    s3.create_bucket(req(CreateBucketInput { bucket: "b".into(), ..Default::default() })).await.unwrap();
    (s3, meta, dir)
}

async fn get_all(s3: &pigeonhole::service::S3Gateway, key: &str, range: Option<Range>) -> Vec<u8> {
    let out = s3
        .get_object(req(GetObjectInput { bucket: "b".into(), key: key.into(), range, ..Default::default() }))
        .await
        .unwrap();
    collect_body(out.output.body).await
}

fn put_len(key: &str, data: &[u8], with_length: bool) -> PutObjectInput {
    PutObjectInput {
        bucket: "b".into(),
        key: key.into(),
        content_length: with_length.then_some(data.len() as i64),
        body: Some(StreamingBlob::from_bytes(Bytes::copy_from_slice(data))),
        ..Default::default()
    }
}

#[tokio::test]
async fn small_objects_are_inline_large_ones_chunked() {
    let (s3, meta, _dir) = setup_inline().await;
    let small = vec![7u8; 1024];
    let large = vec![9u8; 1025];
    for with_length in [true, false] {
        let before = meta.applied_position();
        s3.put_object(req(put_len("small", &small, with_length))).await.unwrap();
        assert_eq!(meta.applied_position(), before + 1, "one entry");
        s3.put_object(req(put_len("large", &large, with_length))).await.unwrap();
        let st = s3.index.stats();
        assert_eq!((st.inline_objects, st.inline_bytes), (1, 1024), "with_length={with_length}");
        assert!(s3.index.get_extents("b", "small").await.unwrap().unwrap().is_empty());
        assert!(!s3.index.get_extents("b", "large").await.unwrap().unwrap().is_empty());
        assert_eq!(get_all(&s3, "small", None).await, small);
        assert_eq!(get_all(&s3, "large", None).await, large);
    }
    // A range of an inline object; its ETag is the MD5, as for any object.
    let r = Range::Int { first: 10, last: Some(19) };
    assert_eq!(get_all(&s3, "small", Some(r)).await, vec![7u8; 10]);
    let put = s3.put_object(req(put_len("small", &small, true))).await.unwrap();
    let head = s3.head_object(req(HeadObjectInput { bucket: "b".into(), key: "small".into(), ..Default::default() })).await.unwrap();
    // md5 of 1024 × 0x07, as AWS would give.
    assert_eq!(put.output.e_tag.unwrap().value(), "254a7725306413d2a8e39ad05ffa018a");
    assert_eq!(head.output.e_tag.unwrap().value(), "254a7725306413d2a8e39ad05ffa018a");
    // An empty object is inline too.
    s3.put_object(req(put_len("empty", b"", true))).await.unwrap();
    assert_eq!(get_all(&s3, "empty", None).await, b"");
}

#[tokio::test]
async fn an_inline_put_checks_its_digest_and_commits_nothing_when_wrong() {
    let (s3, meta, _dir) = setup_inline().await;
    let before = meta.applied_position();
    let mut bad = put_len("k", b"hello", true);
    bad.content_md5 = Some("AAAAAAAAAAAAAAAAAAAAAA==".into());
    assert!(s3.put_object(req(bad)).await.is_err());
    assert_eq!(meta.applied_position(), before);
    assert_eq!(s3.index.stats().inline_objects, 0);
}

#[tokio::test]
async fn copies_from_an_inline_object() {
    let (s3, _meta, _dir) = setup_inline().await;
    let data: Vec<u8> = (0..200u8).collect();
    s3.put_object(req(put_len("src", &data, true))).await.unwrap();
    let copy = CopyObjectInput::builder()
        .bucket("b".into())
        .key("dst".into())
        .copy_source(CopySource::Bucket { bucket: "b".into(), key: "src".into(), version_id: None })
        .build()
        .unwrap();
    s3.copy_object(req(copy)).await.unwrap();
    assert_eq!(get_all(&s3, "dst", None).await, data);
    assert_eq!(s3.index.stats().inline_objects, 2);

    // A part copied from an inline source, then completed.
    let upload = s3
        .create_multipart_upload(req(CreateMultipartUploadInput { bucket: "b".into(), key: "mp".into(), ..Default::default() }))
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let part = s3
        .upload_part_copy(req(UploadPartCopyInput::builder()
            .bucket("b".into())
            .key("mp".into())
            .upload_id(upload.clone())
            .part_number(1)
            .copy_source(CopySource::Bucket { bucket: "b".into(), key: "src".into(), version_id: None })
            .copy_source_range(Some("bytes=50-149".into()))
            .build()
            .unwrap()))
        .await
        .unwrap()
        .output
        .copy_part_result
        .unwrap()
        .e_tag;
    s3.complete_multipart_upload(req(CompleteMultipartUploadInput {
        bucket: "b".into(),
        key: "mp".into(),
        upload_id: upload,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart { part_number: Some(1), e_tag: part, ..Default::default() }]),
        }),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(get_all(&s3, "mp", None).await, data[50..150].to_vec());
}

#[tokio::test]
async fn an_inline_rewrite_releases_the_chunks_it_replaces() {
    let (s3, _meta, _dir) = setup_inline().await;
    s3.put_object(req(put_len("k", &vec![1u8; 5000], true))).await.unwrap();
    assert_eq!(s3.store.db().stats().live, 1);
    s3.put_object(req(put_len("k", b"tiny", true))).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while s3.store.db().stats().live != 0 {
        assert!(tokio::time::Instant::now() < deadline, "the old chunk is still live");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(get_all(&s3, "k", None).await, b"tiny");
}

#[tokio::test]
async fn the_cooler_moves_inline_objects_out_and_reads_do_not_notice() {
    let (s3, _meta, _dir) = setup_inline().await;
    for i in 0..5 {
        s3.put_object(req(put_len(&format!("k{i}"), format!("value {i}").as_bytes(), true))).await.unwrap();
    }
    assert_eq!(s3.index.stats().inline_objects, 5);
    let none = pigeonhole::service::CoolPolicy { min_inline: std::time::Duration::from_secs(3600), ..Default::default() };
    assert_eq!(s3.cool_pass(&none).await.unwrap(), pigeonhole::service::CoolStats::default(), "all too fresh");

    let now = pigeonhole::service::CoolPolicy {
        cool_after: Some(std::time::Duration::ZERO),
        min_inline: std::time::Duration::ZERO,
        batch: 3,
        ..Default::default()
    };
    let stats = s3.cool_pass(&now).await.unwrap();
    assert_eq!((stats.by_age, stats.rejected), (3, 0));
    s3.cool_pass(&now).await.unwrap();
    let st = s3.index.stats();
    assert_eq!((st.inline_objects, st.inline_bytes), (0, 0));
    assert_eq!(s3.store.db().stats().live, 5);
    for i in 0..5 {
        assert_eq!(get_all(&s3, &format!("k{i}"), None).await, format!("value {i}").as_bytes());
    }
}

/// The cooler racing rewrites and deletes: the last write of every key
/// reads back, and no chunk is left live that no object points at.
#[tokio::test]
async fn the_cooler_never_loses_a_rewrite_or_leaks_a_chunk() {
    let (s3, _meta, _dir) = setup_inline().await;
    let s3 = Arc::new(s3);
    let keys = 8;
    let writer = {
        let s3 = s3.clone();
        tokio::spawn(async move {
            for round in 0..30 {
                for k in 0..keys {
                    let key = format!("k{k}");
                    if (round + k) % 7 == 0 {
                        let _ = s3.delete_object(req(DeleteObjectInput { bucket: "b".into(), key, ..Default::default() })).await;
                    } else {
                        let v = format!("round {round} key {k}");
                        s3.put_object(req(put_len(&key, v.as_bytes(), true))).await.unwrap();
                    }
                }
            }
        })
    };
    let policy = pigeonhole::service::CoolPolicy {
        cool_after: Some(std::time::Duration::ZERO),
        min_inline: std::time::Duration::ZERO,
        ..Default::default()
    };
    let mut cooled = 0;
    while !writer.is_finished() {
        let st = s3.cool_pass(&policy).await.unwrap();
        cooled += st.by_age;
        // A pass with nothing to do never yields; let the writer run.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    writer.await.unwrap();
    assert!(cooled > 0, "the cooler did move something");

    let mut chunked = 0;
    for k in 0..keys {
        let key = format!("k{k}");
        let last = 29;
        let deleted_last = (last + k) % 7 == 0;
        match s3.index.get_object("b", &key).await.unwrap() {
            None => assert!(deleted_last, "{key} vanished"),
            Some(_) => {
                assert!(!deleted_last, "{key} survived its delete");
                assert_eq!(get_all(&s3, &key, None).await, format!("round {last} key {k}").as_bytes());
                chunked += s3.index.get_extents("b", &key).await.unwrap().unwrap().len();
            }
        }
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while s3.store.db().stats().live as usize != chunked {
        assert!(tokio::time::Instant::now() < deadline, "live chunks {} != referenced {chunked}", s3.store.db().stats().live);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// With inlining off (or the threshold lowered), a copy of a still-inline
/// object is stored as chunks, as any new write then is.
#[tokio::test]
async fn a_copy_follows_the_current_threshold() {
    let (s3, _meta, _dir) = setup_inline().await;
    s3.put_object(req(put_len("src", b"0123456789", true))).await.unwrap();
    let mut off = s3.clone();
    off.opts.inline_max = None;
    let copy = CopyObjectInput::builder()
        .bucket("b".into())
        .key("dst".into())
        .copy_source(CopySource::Bucket { bucket: "b".into(), key: "src".into(), version_id: None })
        .build()
        .unwrap();
    off.copy_object(req(copy)).await.unwrap();
    assert!(s3.index.get_extents("b", "src").await.unwrap().unwrap().is_empty(), "the source stays inline");
    assert!(!s3.index.get_extents("b", "dst").await.unwrap().unwrap().is_empty(), "the copy is chunked");
    assert_eq!(get_all(&s3, "dst", None).await, b"0123456789");
    let (src, dst) = (s3.index.get_object("b", "src").await.unwrap().unwrap(), s3.index.get_object("b", "dst").await.unwrap().unwrap());
    assert_eq!(src.etag, dst.etag);
}
