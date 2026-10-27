//! S3 gateway index: buckets/objects/multipart with extent lists.
//!
//! Queries read the tablet's in-memory [`S3State`]; every change goes through
//! an `s3` command (see [`crate::commands`]). Chunk refcounts live in the
//! chunk store ([`ChunkStore::retain`] / [`release`]).

use crate::commands::{pb, ObjectRec, S3State, UploadRec, NAMESPACE};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Applied, MetaStore, Namespaces};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::sync::Arc;

#[derive(Clone)]
pub struct Index {
    meta: Arc<MetaStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub created_at: String,
    pub scope_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub mtime: String,
}

/// Snapshot of a copy source, see [`Index::get_copy_source`].
#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub extents: Vec<Extent>,
    pub user_meta: Vec<(String, String)>,
    pub checksums_json: String,
}

#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub tags: Vec<(String, String)>,
    pub checksum_algorithm: Option<String>,
    pub initiated_at: String,
}

#[derive(Debug, Clone)]
pub struct MultipartPart {
    pub upload_id: String,
    pub part_number: i64,
    pub etag: String,
    pub size: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeleteBucketResult {
    Deleted,
    NotFound,
    NotEmpty,
}

impl Index {
    /// Open a standalone tablet over a local log in `log_dir` with only this
    /// namespace.
    pub async fn connect(log_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let namespaces = Self::register(Namespaces::new());
        Self::open(MetaStore::open_local(log_dir, namespaces).await?).await
    }

    /// Add the `s3` namespace to the set passed to [`MetaStore::open`].
    pub fn register(namespaces: Namespaces) -> Namespaces {
        namespaces.with::<S3State>()
    }

    /// Use `meta`, which must have been opened with [`Self::register`].
    pub async fn open(meta: Arc<MetaStore>) -> Result<Self> {
        if !meta.has_namespace(NAMESPACE) {
            anyhow::bail!("metadata tablet opened without the s3 namespace");
        }
        Ok(Self { meta })
    }

    fn read<R>(&self, f: impl FnOnce(&S3State) -> R) -> R {
        self.meta.read::<S3State, _>(f)
    }

    /// Propose one s3 command; `Ok(Err(reason))` is a deterministic rejection.
    async fn propose(
        &self,
        op: pb::command::Op,
    ) -> Result<std::result::Result<Vec<ChunkId>, pb::rejection::Reason>> {
        let cmd = pb::Command { op: Some(op) }.encode_to_vec();
        match self.meta.propose(NAMESPACE, &cmd).await?.applied {
            Applied::Commit(bytes) => Ok(Ok(pb::Released::decode(bytes.as_slice())?.chunk_ids)),
            Applied::Reject(bytes) => Ok(Err(pb::Rejection::decode(bytes.as_slice())?
                .reason
                .context("rejection without reason")?)),
        }
    }

    async fn propose_infallible(&self, op: pb::command::Op) -> Result<Vec<ChunkId>> {
        match self.propose(op).await? {
            Ok(ids) => Ok(ids),
            Err(reason) => anyhow::bail!("unexpected s3 rejection {reason:?}"),
        }
    }

    /// Returns false when the bucket already exists.
    pub async fn create_bucket(&self, name: &str, scope_id: &str) -> Result<bool> {
        let op = pb::command::Op::CreateBucket(pb::CreateBucket {
            name: name.into(),
            scope_id: scope_id.into(),
            created_at: Utc::now().to_rfc3339(),
        });
        match self.propose(op).await? {
            Ok(_) => Ok(true),
            Err(pb::rejection::Reason::BucketExists(_)) => Ok(false),
            Err(other) => anyhow::bail!("unexpected create_bucket rejection {other:?}"),
        }
    }

    pub async fn bucket_scope_id(&self, name: &str) -> Result<Option<String>> {
        Ok(self.read(|s| s.buckets.get(name).map(|b| b.scope_id.clone())))
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<DeleteBucketResult> {
        use pb::rejection::Reason;
        let op = pb::command::Op::DeleteBucket(pb::DeleteBucket { name: name.into() });
        Ok(match self.propose(op).await? {
            Ok(_) => DeleteBucketResult::Deleted,
            Err(Reason::BucketNotFound(_)) => DeleteBucketResult::NotFound,
            Err(Reason::BucketNotEmpty(_)) => DeleteBucketResult::NotEmpty,
            Err(other) => anyhow::bail!("unexpected delete_bucket rejection {other:?}"),
        })
    }

    pub async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        Ok(self.read(|s| {
            s.buckets
                .iter()
                .map(|(name, b)| Bucket {
                    name: name.clone(),
                    created_at: b.created_at.clone(),
                    scope_id: b.scope_id.clone(),
                })
                .collect()
        }))
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool> {
        Ok(self.read(|s| s.buckets.contains_key(name)))
    }

    /// Insert/replace object. Returns previous extent chunk ids (caller should `release`).
    #[allow(clippy::too_many_arguments)]
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        extents: &[Extent],
        user_meta: &[(String, String)],
        checksums_json: &str,
    ) -> Result<Vec<ChunkId>> {
        let mtime = Utc::now().to_rfc3339();
        self.put_object_at(bucket, key, etag, size, content_type, extents, user_meta, checksums_json, &mtime)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_object_at(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        extents: &[Extent],
        user_meta: &[(String, String)],
        checksums_json: &str,
        mtime: &str,
    ) -> Result<Vec<ChunkId>> {
        self.propose_infallible(pb::command::Op::PutObject(pb::PutObject {
            bucket: bucket.into(),
            key: key.into(),
            etag: etag.into(),
            size,
            content_type: content_type.map(Into::into),
            extents: pb_extents(extents),
            user_meta: pb_pairs(user_meta),
            checksums_json: checksums_json.into(),
            mtime: mtime.into(),
        }))
        .await
    }

    fn object<R>(&self, bucket: &str, key: &str, f: impl FnOnce(&ObjectRec) -> R) -> Option<R> {
        self.read(|s| s.objects.get(&(bucket.to_string(), key.to_string())).map(f))
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.object(bucket, key, |o| object_meta(bucket, key, o)))
    }

    pub async fn get_extents(&self, bucket: &str, key: &str) -> Result<Option<Vec<Extent>>> {
        Ok(self.object(bucket, key, |o| o.extents.clone()))
    }

    pub async fn get_object_checksums_json(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>> {
        Ok(self.object(bucket, key, |o| o.checksums_json.clone()))
    }

    /// Ordered by name; empty for a missing object.
    pub async fn get_user_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(String, String)>> {
        Ok(self.object(bucket, key, |o| pairs(&o.user_meta)).unwrap_or_default())
    }

    /// Ordered by tag key; empty for a missing object.
    pub async fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(String, String)>> {
        Ok(self.object(bucket, key, |o| pairs(&o.tags)).unwrap_or_default())
    }

    pub async fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: &[(String, String)],
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::PutObjectTags(pb::PutObjectTags {
            bucket: bucket.into(),
            key: key.into(),
            tags: pb_pairs(tags),
        }))
        .await?;
        Ok(())
    }

    pub async fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<()> {
        self.propose_infallible(pb::command::Op::DeleteObjectTags(pb::DeleteObjectTags {
            bucket: bucket.into(),
            key: key.into(),
        }))
        .await?;
        Ok(())
    }

    /// Delete object; returns extents' chunk ids to `release`, or `None` if missing.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Vec<ChunkId>>> {
        let op = pb::command::Op::DeleteObject(pb::DeleteObject {
            bucket: bucket.into(),
            key: key.into(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(pb::rejection::Reason::NoSuchKey(_)) => Ok(None),
            Err(other) => anyhow::bail!("unexpected delete_object rejection {other:?}"),
        }
    }

    /// Consistent view of a copy source: object row, extents, user metadata, and
    /// checksums under one read lock (no mix of two versions).
    pub async fn get_copy_source(&self, bucket: &str, key: &str) -> Result<Option<SourceSnapshot>> {
        Ok(self.object(bucket, key, |o| SourceSnapshot {
            etag: o.etag.clone(),
            size: o.size,
            content_type: o.content_type.clone(),
            extents: o.extents.clone(),
            user_meta: pairs(&o.user_meta),
            checksums_json: o.checksums_json.clone(),
        }))
    }

    /// Shallow copy of `src` (from [`Self::get_copy_source`]) to `dst`. The caller must
    /// `retain` the source extents' chunks **before** calling this, so the new object
    /// never points at chunks it does not hold a ref on. Returns the destination row as
    /// written and the previous destination chunk ids to `release`.
    pub async fn copy_object(
        &self,
        src: &SourceSnapshot,
        dst_bucket: &str,
        dst_key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        copy_source_meta: bool,
    ) -> Result<(ObjectMeta, Vec<ChunkId>)> {
        let ct = content_type.or(src.content_type.as_deref());
        let meta = if copy_source_meta {
            &src.user_meta[..]
        } else {
            user_meta
        };
        let mtime = Utc::now().to_rfc3339();
        let old = self
            .put_object_at(
                dst_bucket,
                dst_key,
                &src.etag,
                src.size,
                ct,
                &src.extents,
                meta,
                &src.checksums_json,
                &mtime,
            )
            .await?;

        // The row as written, not re-read: a concurrent delete or overwrite of
        // `dst` may already be applied after this put.
        let dst = ObjectMeta {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            etag: src.etag.clone(),
            size: src.size,
            content_type: ct.map(Into::into),
            mtime,
        };
        Ok((dst, old))
    }

    /// Up to `limit` objects of `bucket` with keys in `(lower, upper)` bounds,
    /// in byte order.
    fn scan_objects(
        &self,
        bucket: &str,
        lower: Bound<&str>,
        upper: Option<&str>,
        limit: usize,
    ) -> Vec<ObjectMeta> {
        let start = match lower {
            Bound::Included(k) => Bound::Included((bucket.to_string(), k.to_string())),
            Bound::Excluded(k) => Bound::Excluded((bucket.to_string(), k.to_string())),
            Bound::Unbounded => Bound::Included((bucket.to_string(), String::new())),
        };
        self.read(|s| {
            s.objects
                .range((start, Bound::Unbounded))
                .take_while(|((b, k), _)| b == bucket && upper.is_none_or(|hi| k.as_str() < hi))
                .take(limit)
                .map(|((b, k), o)| object_meta(b, k, o))
                .collect()
        })
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i64,
        start_after: Option<&str>,
    ) -> Result<(Vec<ObjectMeta>, Vec<String>, bool, Option<String>)> {
        let max_keys = max_keys.clamp(1, 1000) as usize;
        let range_end = exclusive_prefix_end(prefix);

        if let Some(delim) = delimiter {
            let batch = (max_keys * 32).clamp(64, 10_000);
            let mut cursor: Option<(bool, String)> = match start_after {
                Some(after) if after.ends_with(delim) => {
                    exclusive_prefix_end(after).map(|e| (true, e))
                }
                Some(after) => Some((false, after.to_string())),
                None => None,
            };
            let mut collected: Vec<(String, DelimEntry)> = Vec::new();
            let mut exhausted = false;

            loop {
                let lower = match &cursor {
                    Some((true, c)) if c.as_str() >= prefix => Bound::Included(c.as_str()),
                    Some((false, c)) if c.as_str() >= prefix => Bound::Excluded(c.as_str()),
                    _ => Bound::Included(prefix),
                };
                let objs = self.scan_objects(bucket, lower, range_end.as_deref(), batch);
                if objs.len() < batch {
                    exhausted = true;
                }

                let batch_entries = group_delimiter_entries(prefix, delim, objs);
                let mut advanced = false;
                for e in batch_entries {
                    if let Some(after) = start_after {
                        if e.0.as_str() <= after {
                            continue;
                        }
                    }
                    if collected.last().map(|(k, _)| k) == Some(&e.0) {
                        continue;
                    }
                    let is_prefix = matches!(e.1, DelimEntry::Prefix(_));
                    let sort_key = e.0.clone();
                    collected.push(e);
                    advanced = true;
                    if is_prefix {
                        if let Some(end) = exclusive_prefix_end(&sort_key) {
                            cursor = Some((true, end));
                        } else {
                            cursor = Some((false, sort_key));
                        }
                    } else {
                        cursor = Some((false, sort_key));
                    }
                    if collected.len() > max_keys {
                        break;
                    }
                }

                if collected.len() > max_keys || exhausted || !advanced {
                    break;
                }
            }

            let truncated = collected.len() > max_keys;
            collected.truncate(max_keys);
            let next = if truncated {
                collected.last().map(|(k, _)| k.clone())
            } else {
                None
            };
            let mut objects = Vec::new();
            let mut common = Vec::new();
            for (_, e) in collected {
                match e {
                    DelimEntry::Object(o) => objects.push(o),
                    DelimEntry::Prefix(p) => common.push(p),
                }
            }
            return Ok((objects, common, truncated, next));
        }

        let lower = match start_after {
            Some(after) if after >= prefix => Bound::Excluded(after),
            _ => Bound::Included(prefix),
        };
        let mut objs = self.scan_objects(bucket, lower, range_end.as_deref(), max_keys + 1);
        let truncated = objs.len() > max_keys;
        objs.truncate(max_keys);
        let next = if truncated {
            objs.last().map(|o| o.key.clone())
        } else {
            None
        };
        Ok((objs, Vec::new(), truncated, next))
    }

    pub async fn count_objects(&self) -> Result<i64> {
        Ok(self.read(|s| s.objects.len() as i64))
    }

    pub async fn count_buckets(&self) -> Result<i64> {
        Ok(self.read(|s| s.buckets.len() as i64))
    }

    /// Abort every upload, delete every object and bucket, one command each.
    /// Returns the chunk ids the removed uploads and objects held, to `release`.
    pub async fn delete_everything(&self) -> Result<Vec<ChunkId>> {
        let mut released = Vec::new();
        let uploads: Vec<String> = self.read(|s| s.uploads.keys().cloned().collect());
        for upload_id in uploads {
            released.extend(self.abort_multipart_upload(&upload_id).await?.unwrap_or_default());
        }
        let objects: Vec<(String, String)> = self.read(|s| s.objects.keys().cloned().collect());
        for (bucket, key) in objects {
            released.extend(self.delete_object(&bucket, &key).await?.unwrap_or_default());
        }
        for bucket in self.list_buckets().await? {
            self.delete_bucket(&bucket.name).await?;
        }
        Ok(released)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        tags: &[(String, String)],
        checksum_algorithm: Option<&str>,
    ) -> Result<()> {
        self.propose_infallible(pb::command::Op::CreateUpload(pb::CreateUpload {
            upload_id: upload_id.into(),
            bucket: bucket.into(),
            key: key.into(),
            content_type: content_type.map(Into::into),
            user_meta: pb_pairs(user_meta),
            tags: pb_pairs(tags),
            checksum_algorithm: checksum_algorithm.map(Into::into),
            initiated_at: Utc::now().to_rfc3339(),
        }))
        .await?;
        Ok(())
    }

    pub async fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        Ok(self.read(|s| s.uploads.get(upload_id).map(|u| multipart_upload(upload_id, u))))
    }

    /// Store part extents. Returns previous part chunk ids to `release`.
    pub async fn put_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
        etag: &str,
        size: i64,
        extents: &[Extent],
    ) -> Result<Vec<ChunkId>> {
        self.propose_infallible(pb::command::Op::PutPart(pb::PutPart {
            upload_id: upload_id.into(),
            part_number,
            etag: etag.into(),
            size,
            extents: pb_extents(extents),
        }))
        .await
    }

    pub async fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultipartPart>> {
        Ok(self.read(|s| {
            let p = s.uploads.get(upload_id)?.parts.get(&part_number)?;
            Some(MultipartPart {
                upload_id: upload_id.into(),
                part_number,
                etag: p.etag.clone(),
                size: p.size,
            })
        }))
    }

    pub async fn get_multipart_part_extents(
        &self,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<Vec<Extent>>> {
        Ok(self.read(|s| {
            Some(s.uploads.get(upload_id)?.parts.get(&part_number)?.extents.clone())
        }))
    }

    /// Assemble object from parts (ownership transfer). Returns old object chunk ids to release.
    pub async fn complete_multipart_upload(
        &self,
        upload: &MultipartUpload,
        part_numbers: &[i64],
        etag: &str,
        total_size: i64,
    ) -> Result<Vec<ChunkId>> {
        let op = pb::command::Op::CompleteUpload(pb::CompleteUpload {
            upload_id: upload.upload_id.clone(),
            part_numbers: part_numbers.to_vec(),
            etag: etag.into(),
            total_size,
            mtime: Utc::now().to_rfc3339(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(ids),
            Err(pb::rejection::Reason::MissingPart(pn)) => anyhow::bail!("missing part {pn}"),
            Err(other) => anyhow::bail!("complete_multipart_upload rejected: {other:?}"),
        }
    }

    /// Abort upload; returns all part chunk ids to `release`, or `None` if missing.
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<ChunkId>>> {
        let op = pb::command::Op::AbortUpload(pb::AbortUpload {
            upload_id: upload_id.into(),
        });
        match self.propose(op).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(pb::rejection::Reason::NoSuchUpload(_)) => Ok(None),
            Err(other) => anyhow::bail!("unexpected abort rejection {other:?}"),
        }
    }

    /// Uploads of `bucket` under `prefix`, ordered by `(key, upload_id)`,
    /// after the `(key_marker, upload_id_marker)` position.
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: i64,
    ) -> Result<(Vec<MultipartUpload>, bool, Option<(String, String)>)> {
        let max_uploads = max_uploads.clamp(1, 1000) as usize;
        let key_marker = key_marker.filter(|s| !s.is_empty());
        let upload_id_marker = upload_id_marker.filter(|s| !s.is_empty());
        let mut rows: Vec<MultipartUpload> = self.read(|s| {
            s.uploads
                .iter()
                .filter(|(_, u)| u.bucket == bucket && u.key.starts_with(prefix))
                .filter(|(id, u)| match (key_marker, upload_id_marker) {
                    (Some(km), Some(um)) => {
                        u.key.as_str() > km || (u.key == km && id.as_str() > um)
                    }
                    (Some(km), None) => u.key.as_str() > km,
                    (None, _) => true,
                })
                .map(|(id, u)| multipart_upload(id, u))
                .collect()
        });
        rows.sort_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
        let truncated = rows.len() > max_uploads;
        rows.truncate(max_uploads);
        let next = if truncated {
            rows.last().map(|u| (u.key.clone(), u.upload_id.clone()))
        } else {
            None
        };
        Ok((rows, truncated, next))
    }

    pub async fn list_parts(
        &self,
        upload_id: &str,
        part_number_marker: Option<i64>,
        max_parts: i64,
    ) -> Result<(Vec<MultipartPart>, bool, Option<i64>)> {
        let max_parts = max_parts.clamp(1, 1000) as usize;
        let marker = part_number_marker.unwrap_or(0);
        let mut rows: Vec<MultipartPart> = self.read(|s| {
            let Some(u) = s.uploads.get(upload_id) else {
                return Vec::new();
            };
            u.parts
                .range(marker.saturating_add(1)..)
                .take(max_parts + 1)
                .map(|(n, p)| MultipartPart {
                    upload_id: upload_id.into(),
                    part_number: *n,
                    etag: p.etag.clone(),
                    size: p.size,
                })
                .collect()
        });
        let truncated = rows.len() > max_parts;
        rows.truncate(max_parts);
        let next = if truncated {
            rows.last().map(|p| p.part_number)
        } else {
            None
        };
        Ok((rows, truncated, next))
    }
}

fn object_meta(bucket: &str, key: &str, o: &ObjectRec) -> ObjectMeta {
    ObjectMeta {
        bucket: bucket.into(),
        key: key.into(),
        etag: o.etag.clone(),
        size: o.size,
        content_type: o.content_type.clone(),
        mtime: o.mtime.clone(),
    }
}

fn multipart_upload(upload_id: &str, u: &UploadRec) -> MultipartUpload {
    MultipartUpload {
        upload_id: upload_id.into(),
        bucket: u.bucket.clone(),
        key: u.key.clone(),
        content_type: u.content_type.clone(),
        user_meta: pairs(&u.user_meta),
        tags: pairs(&u.tags),
        checksum_algorithm: u.checksum_algorithm.clone(),
        initiated_at: u.initiated_at.clone(),
    }
}

fn pairs(map: &std::collections::BTreeMap<String, String>) -> Vec<(String, String)> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Unique chunk ids referenced by an extent list (sorted).
pub fn unique_chunk_ids(extents: &[Extent]) -> Vec<ChunkId> {
    let mut ids: Vec<_> = extents.iter().map(|e| e.chunk).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Slice an extent list to cover `[start, start+length)` logical bytes.
pub fn slice_extents(extents: &[Extent], start: u64, length: u64) -> Vec<Extent> {
    if length == 0 {
        return Vec::new();
    }
    let end = start.saturating_add(length);
    let mut cursor = 0u64;
    let mut out = Vec::new();
    for ext in extents {
        let elen = if ext.len < 0 { 0u64 } else { ext.len as u64 };
        let estart = cursor;
        let eend = cursor + elen;
        cursor = eend;
        if eend <= start || estart >= end {
            continue;
        }
        let from = start.saturating_sub(estart);
        let to = end.min(eend).saturating_sub(estart);
        if to > from {
            out.push(Extent {
                chunk: ext.chunk,
                offset: ext.offset + from as i64,
                len: (to - from) as i64,
            });
        }
    }
    out
}

enum DelimEntry {
    Object(ObjectMeta),
    Prefix(String),
}

fn exclusive_prefix_end(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(b) = bytes.last_mut() {
        if *b != 0xff {
            *b += 1;
            return Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        bytes.pop();
    }
    None
}

fn group_delimiter_entries(
    prefix: &str,
    delim: &str,
    objs: Vec<ObjectMeta>,
) -> Vec<(String, DelimEntry)> {
    let mut entries: Vec<(String, DelimEntry)> = Vec::new();
    let mut seen_cp = std::collections::BTreeSet::new();

    for obj in objs {
        let Some(rest) = obj.key.strip_prefix(prefix) else {
            continue;
        };
        if let Some(idx) = rest.find(delim) {
            let cp = format!("{prefix}{}", &rest[..=idx]);
            if seen_cp.insert(cp.clone()) {
                entries.push((cp.clone(), DelimEntry::Prefix(cp)));
            }
        } else {
            entries.push((obj.key.clone(), DelimEntry::Object(obj)));
        }
    }
    entries
}

pub fn parse_rfc3339(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn pb_extents(extents: &[Extent]) -> Vec<pb::Extent> {
    crate::commands::to_pb(extents)
}

fn pb_pairs(pairs: &[(String, String)]) -> Vec<pb::Pair> {
    pairs
        .iter()
        .map(|(name, value)| pb::Pair {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}
