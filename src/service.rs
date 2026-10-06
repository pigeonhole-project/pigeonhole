//! S3 API implementation on top of SQLite index + BlobStore (via s3s).

use crate::config::Config;
use crate::index::{parse_rfc3339, DeleteBucketResult, Index, OrphanMsg};
use crate::ingest::ingest_stream_to_store;
use crate::storage::{BlobStore, DeleteOutcome};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use futures::StreamExt;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3, S3Request, S3Response, S3Result};
use std::ops::Not;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Clone)]
pub struct S3gram {
    pub cfg: Config,
    pub index: Index,
    pub store: Arc<dyn BlobStore>,
    pub snapshot_gate: Arc<Mutex<()>>,
}

impl S3gram {
    fn chat_id(&self) -> &str {
        &self.cfg.chat_id
    }

    async fn cleanup_orphans(&self, orphans: Vec<OrphanMsg>) {
        let mut seen = std::collections::HashSet::new();
        for (_chat, message_id) in orphans {
            if !seen.insert(message_id) {
                continue;
            }
            match self.store.delete_message(message_id).await {
                Ok(DeleteOutcome::Deleted | DeleteOutcome::Gone) => {}
                Ok(DeleteOutcome::Failed) => {
                    let _ = self
                        .index
                        .queue_tg_delete(self.chat_id(), message_id)
                        .await;
                }
                Err(e) => {
                    warn!(error = %e, message_id, "orphan delete failed");
                    let _ = self
                        .index
                        .queue_tg_delete(self.chat_id(), message_id)
                        .await;
                }
            }
        }
    }

    fn map_err(e: impl std::fmt::Display) -> s3s::S3Error {
        s3_error!(InternalError, "{}", e)
    }
}

fn ts(rfc3339: &str) -> Timestamp {
    Timestamp::from(std::time::SystemTime::from(parse_rfc3339(rfc3339)))
}

fn etag_hex(hex: &str) -> ETag {
    ETag::Strong(hex.to_string())
}

#[async_trait]
impl S3 for S3gram {
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.index.list_buckets().await.map_err(Self::map_err)?;
        let buckets = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(ts(&b.created_at)),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                display_name: Some("s3gram".into()),
                id: Some("s3gram".into()),
            }),
            ..Default::default()
        }))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let name = req.input.bucket;
        let created = self
            .index
            .create_bucket(&name, self.chat_id())
            .await
            .map_err(Self::map_err)?;
        if !created && self.index.bucket_exists(&name).await.map_err(Self::map_err)? {
            // Idempotent OK for aws mb retries.
            return Ok(S3Response::new(CreateBucketOutput::default()));
        }
        info!(bucket = %name, "CreateBucket ok");
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        match self
            .index
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            DeleteBucketResult::Deleted => Ok(S3Response::new(DeleteBucketOutput::default())),
            DeleteBucketResult::NotFound => Err(s3_error!(NoSuchBucket)),
            DeleteBucketResult::NotEmpty => Err(s3_error!(BucketNotEmpty)),
        }
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if !self
            .index
            .bucket_exists(&req.input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let prefix = input.prefix.as_deref().unwrap_or("");
        let max_keys = i64::from(input.max_keys.unwrap_or(1000));
        let start_after = input
            .continuation_token
            .as_deref()
            .or(input.start_after.as_deref());

        let (objects, common, truncated, next) = self
            .index
            .list_objects(
                &input.bucket,
                prefix,
                input.delimiter.as_deref(),
                max_keys,
                start_after,
            )
            .await
            .map_err(Self::map_err)?;

        let key_count = (objects.len() + common.len()) as i32;
        let contents = objects
            .into_iter()
            .map(|o| Object {
                key: Some(o.key),
                e_tag: Some(etag_hex(&o.etag)),
                size: Some(o.size),
                last_modified: Some(ts(&o.mtime)),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let common_prefixes = common
            .into_iter()
            .map(|p| CommonPrefix {
                prefix: Some(p),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: Some(max_keys as i32),
            key_count: Some(key_count),
            is_truncated: Some(truncated),
            contents: contents.is_empty().not().then_some(contents),
            common_prefixes: common_prefixes.is_empty().not().then_some(common_prefixes),
            continuation_token: input.continuation_token,
            next_continuation_token: next,
            start_after: input.start_after,
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let stream = body.map(|r| r.map_err(|e| anyhow::anyhow!(e)));

        let (etag, total_size, uploaded) = ingest_stream_to_store(&self.store, stream)
            .await
            .map_err(Self::map_err)?;

        let content_type = input.content_type.as_deref();
        let user_meta: Vec<(String, String)> = input
            .metadata
            .unwrap_or_default()
            .into_iter()
            .collect();

        let orphans = self
            .index
            .put_object(
                &input.bucket,
                &input.key,
                &etag,
                total_size,
                content_type,
                &uploaded,
                self.chat_id(),
                &user_meta,
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

        info!(
            bucket = %input.bucket,
            key = %input.key,
            size = total_size,
            parts = uploaded.len(),
            "PutObject ok"
        );

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(etag_hex(&etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let meta = self
            .index
            .get_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let chunks = self
            .index
            .get_chunks(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?;

        let total = meta.size as u64;
        let (start, end_inclusive) = match input.range {
            None => (0u64, total.saturating_sub(1)),
            Some(Range::Int { first, last }) => {
                let last = last.unwrap_or(total.saturating_sub(1)).min(total.saturating_sub(1));
                if first >= total || first > last {
                    return Err(s3_error!(InvalidRange));
                }
                (first, last)
            }
            Some(Range::Suffix { length }) => {
                if length == 0 || total == 0 {
                    return Err(s3_error!(InvalidRange));
                }
                let start = total.saturating_sub(length);
                (start, total.saturating_sub(1))
            }
        };
        let length = if total == 0 {
            0
        } else {
            end_inclusive.saturating_sub(start) + 1
        };

        // Prefetch ranged bytes so the response stream is Sync (dyn BlobStore futures are not).
        let parts = collect_chunk_bytes(&self.store, &chunks, start, length)
            .await
            .map_err(Self::map_err)?;
        let body_stream = stream::iter(parts.into_iter().map(Ok::<Bytes, std::io::Error>));
        let content_range = input.range.as_ref().map(|_| {
            format!("bytes {start}-{end_inclusive}/{total}")
        });

        let user_meta = self
            .index
            .get_user_metadata(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?;
        let metadata = if user_meta.is_empty() {
            None
        } else {
            Some(user_meta.into_iter().collect())
        };

        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::wrap(body_stream)),
            content_length: Some(length as i64),
            content_range,
            content_type: meta.content_type,
            e_tag: Some(etag_hex(&meta.etag)),
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let meta = self
            .index
            .get_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let user_meta = self
            .index
            .get_user_metadata(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?;
        let metadata = if user_meta.is_empty() {
            None
        } else {
            Some(user_meta.into_iter().collect())
        };
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(meta.size),
            content_type: meta.content_type,
            e_tag: Some(etag_hex(&meta.etag)),
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let orphans = self
            .index
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(Self::map_err)?;
        if let Some(orphans) = orphans {
            self.cleanup_orphans(orphans).await;
        }
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let quiet = input.delete.quiet.unwrap_or(false);
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        let mut all_orphans = Vec::new();

        for obj in input.delete.objects {
            let key = obj.key;
            match self.index.delete_object(&input.bucket, &key).await {
                Ok(Some(orphans)) => {
                    all_orphans.extend(orphans);
                    if !quiet {
                        deleted.push(DeletedObject {
                            key: Some(key),
                            ..Default::default()
                        });
                    }
                }
                Ok(None) => {
                    if !quiet {
                        deleted.push(DeletedObject {
                            key: Some(key),
                            ..Default::default()
                        });
                    }
                }
                Err(e) => {
                    errors.push(Error {
                        key: Some(key),
                        code: Some("InternalError".into()),
                        message: Some(e.to_string()),
                        ..Default::default()
                    });
                }
            }
        }
        self.cleanup_orphans(all_orphans).await;

        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: deleted.is_empty().not().then_some(deleted),
            errors: errors.is_empty().not().then_some(errors),
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.to_string(), key.to_string()),
            _ => return Err(s3_error!(NotImplemented)),
        };

        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        if self
            .index
            .get_object(&src_bucket, &src_key)
            .await
            .map_err(Self::map_err)?
            .is_none()
        {
            return Err(s3_error!(NoSuchKey));
        }

        let copy_source_meta = input
            .metadata_directive
            .as_ref()
            .map(|d| d.as_str())
            != Some(MetadataDirective::REPLACE);
        let content_type = if copy_source_meta {
            None
        } else {
            input.content_type.as_deref()
        };
        let user_meta: Vec<(String, String)> = if copy_source_meta {
            vec![]
        } else {
            input.metadata.unwrap_or_default().into_iter().collect()
        };

        let (dst, orphans) = self
            .index
            .copy_object(
                &src_bucket,
                &src_key,
                &input.bucket,
                &input.key,
                content_type,
                &user_meta,
                copy_source_meta,
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(etag_hex(&dst.etag)),
                last_modified: Some(ts(&dst.mtime)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        let user_meta: Vec<(String, String)> =
            input.metadata.unwrap_or_default().into_iter().collect();
        let upload_id = uuid::Uuid::new_v4().to_string();
        self.index
            .create_multipart_upload(
                &upload_id,
                &input.bucket,
                &input.key,
                input.content_type.as_deref(),
                &user_meta,
            )
            .await
            .map_err(Self::map_err)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let upload_id = input.upload_id;
        let part_number = i64::from(input.part_number);

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let stream = body.map(|r| r.map_err(|e| anyhow::anyhow!(e)));
        let (etag, size, uploaded) = ingest_stream_to_store(&self.store, stream)
            .await
            .map_err(Self::map_err)?;

        let orphans = self
            .index
            .put_multipart_part(
                &upload_id,
                part_number,
                &etag,
                size,
                &uploaded,
                self.chat_id(),
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(etag_hex(&etag)),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = input.upload_id;
        let upload = self
            .index
            .get_multipart_upload(&upload_id)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;

        let parts = input
            .multipart_upload
            .and_then(|m| m.parts)
            .unwrap_or_default();
        if parts.is_empty() {
            return Err(s3_error!(InvalidArgument, "empty parts list"));
        }

        let mut md5_concat = md5::Md5::new();
        use md5::Digest;
        let mut total_size: i64 = 0;
        let mut part_numbers = Vec::new();
        for p in &parts {
            let pn = i64::from(
                p.part_number
                    .ok_or_else(|| s3_error!(InvalidArgument, "missing PartNumber"))?,
            );
            let client_etag = p
                .e_tag
                .as_ref()
                .ok_or_else(|| s3_error!(InvalidArgument, "missing ETag"))?;
            let part = self
                .index
                .get_multipart_part(&upload_id, pn)
                .await
                .map_err(Self::map_err)?
                .ok_or_else(|| s3_error!(InvalidPart))?;
            let normalized = client_etag.value();
            if part.etag != normalized {
                return Err(s3_error!(InvalidPart));
            }
            let digest = hex::decode(&part.etag).map_err(|_| s3_error!(InvalidPart))?;
            md5_concat.update(&digest);
            total_size += part.size;
            part_numbers.push(pn);
        }
        let etag = format!("{:x}-{}", md5_concat.finalize(), part_numbers.len());

        self.index
            .complete_multipart_upload(&upload, &part_numbers, &etag, total_size)
            .await
            .map_err(Self::map_err)?;

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(upload.bucket),
            key: Some(upload.key),
            e_tag: Some(etag_hex(&etag)),
            location: Some(format!("/{}/{}", input.bucket, input.key)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let upload_id = req.input.upload_id;
        match self
            .index
            .abort_multipart_upload(&upload_id)
            .await
            .map_err(Self::map_err)?
        {
            None => Err(s3_error!(NoSuchUpload)),
            Some(orphans) => {
                self.cleanup_orphans(orphans).await;
                Ok(S3Response::new(AbortMultipartUploadOutput::default()))
            }
        }
    }
}

async fn collect_chunk_bytes(
    store: &Arc<dyn BlobStore>,
    chunks: &[crate::index::Chunk],
    mut start: u64,
    mut remaining: u64,
) -> anyhow::Result<Vec<Bytes>> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    for chunk in chunks {
        if remaining == 0 {
            break;
        }
        let chunk_size = chunk.size as u64;
        let chunk_end = offset + chunk_size;
        if chunk_end <= start {
            offset = chunk_end;
            continue;
        }
        let data = store.get(&chunk.file_id).await?;
        let local_start = start.saturating_sub(offset) as usize;
        let take = (chunk_size - local_start as u64).min(remaining) as usize;
        out.push(data.slice(local_start..local_start + take));
        remaining -= take as u64;
        start += take as u64;
        offset = chunk_end;
    }
    Ok(out)
}

