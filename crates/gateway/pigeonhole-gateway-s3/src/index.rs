//! S3 gateway index: buckets/objects/multipart with extent lists.
//!
//! Queries read the tablet's in-memory [`S3State`]; every change goes through
//! an `s3` command (see [`crate::commands`]). Chunk refcounts live in the
//! chunk store ([`ChunkStore::retain`] / [`release`]).

use crate::commands::{Body, Expect, ObjectRec, S3Command, S3Reject, S3State, UploadRec, NAMESPACE};
use anyhow::Result;
use chrono::{DateTime, Utc};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{MetaStore, Namespaces, Prepared};
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::sync::Arc;

/// A conditional write refused (docs/conditional-writes.md): the key's
/// object was not what the write expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejected(pub S3Reject);

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conditional write refused: {:?}", self.0)
    }
}

impl std::error::Error for Rejected {}

#[derive(Clone)]
pub struct Index {
    meta: Arc<MetaStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub name: String,
    pub created_at: String,
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

/// One version of an object as a GET sees it, read under one lock.
#[derive(Debug, Clone)]
pub struct ObjectView {
    pub meta: ObjectMeta,
    pub body: Body,
    pub user_meta: Vec<(String, String)>,
    pub tag_count: usize,
    pub checksums_json: String,
}

/// Snapshot of a copy source, see [`Index::get_copy_source`].
#[derive(Debug, Clone)]
pub struct SourceSnapshot {
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub body: Body,
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

    /// The tablet the index lives in.
    pub fn meta(&self) -> &Arc<MetaStore> {
        &self.meta
    }

    fn read<R>(&self, f: impl FnOnce(&S3State) -> R) -> R {
        self.meta.read::<S3State, _>(f)
    }

    /// Bucket / object / upload counts and object bytes, for gauges (a pass
    /// over every object).
    pub fn stats(&self) -> IndexStats {
        self.read(|s| IndexStats {
            buckets: s.buckets.len() as u64,
            objects: s.objects.len() as u64,
            object_bytes: s.objects.values().map(|o| o.size.max(0) as u64).sum(),
            uploads: s.uploads.len() as u64,
            inline_objects: s.objects.values().filter(|o| matches!(o.body, Body::Inline(_))).count() as u64,
            inline_bytes: s.inline_bytes,
        })
    }

    /// Propose one s3 command; `Ok(Err(reason))` is a deterministic rejection.
    async fn propose(
        &self,
        cmd: S3Command,
    ) -> Result<std::result::Result<Vec<ChunkId>, S3Reject>> {
        self.meta.propose::<S3State>(cmd).await
    }

    /// A conditional write: its refusal is a [`Rejected`] error.
    async fn propose_expecting(&self, cmd: S3Command) -> Result<Vec<ChunkId>> {
        match self.propose(cmd).await? {
            Ok(ids) => Ok(ids),
            Err(reason @ (S3Reject::PreconditionFailed | S3Reject::NoSuchKey)) => Err(Rejected(reason).into()),
            Err(reason) => anyhow::bail!("unexpected s3 rejection {reason:?}"),
        }
    }

    async fn propose_infallible(&self, cmd: S3Command) -> Result<Vec<ChunkId>> {
        match self.propose(cmd).await? {
            Ok(ids) => Ok(ids),
            Err(reason) => anyhow::bail!("unexpected s3 rejection {reason:?}"),
        }
    }

    /// Whether a conditional write would be refused now. Only to fail early,
    /// before the body is read: the command decides when it applies.
    pub fn check_expect(&self, bucket: &str, key: &str, expect: Option<&Expect>) -> Result<()> {
        self.read(|s| s.check(bucket, key, expect)).map_err(|r| Rejected(r).into())
    }

    /// Returns false when the bucket already exists.
    pub async fn create_bucket(&self, name: &str) -> Result<bool> {
        let cmd = S3Command::CreateBucket {
            name: name.into(),
            created_at: Utc::now().to_rfc3339(),
        };
        match self.propose(cmd).await? {
            Ok(_) => Ok(true),
            Err(S3Reject::BucketExists) => Ok(false),
            Err(other) => anyhow::bail!("unexpected create_bucket rejection {other:?}"),
        }
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<DeleteBucketResult> {
        Ok(match self.propose(S3Command::DeleteBucket { name: name.into() }).await? {
            Ok(_) => DeleteBucketResult::Deleted,
            Err(S3Reject::BucketNotFound) => DeleteBucketResult::NotFound,
            Err(S3Reject::BucketNotEmpty) => DeleteBucketResult::NotEmpty,
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
        body: Body,
        user_meta: &[(String, String)],
        checksums_json: &str,
    ) -> Result<Vec<ChunkId>> {
        let mtime = Utc::now().to_rfc3339();
        self.put_object_at(bucket, key, etag, size, content_type, body, user_meta, checksums_json, &mtime, None)
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
        body: Body,
        user_meta: &[(String, String)],
        checksums_json: &str,
        mtime: &str,
        expect: Option<Expect>,
    ) -> Result<Vec<ChunkId>> {
        self.propose_expecting(S3Command::PutObject {
            bucket: bucket.into(),
            key: key.into(),
            etag: etag.into(),
            size,
            content_type: content_type.map(Into::into),
            body,
            user_meta: user_meta.to_vec(),
            tags: Vec::new(),
            checksums_json: checksums_json.into(),
            mtime: mtime.into(),
            expect,
        })
        .await
    }

    /// The `PutObject` of [`Self::put_object`] with its tags, to commit with
    /// the object's chunks in one entry. Outcome: the previous object's chunk
    /// ids, or `BucketNotFound`, or a refusal of `expect` (`PreconditionFailed`,
    /// `NoSuchKey`).
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_put_object(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
        size: i64,
        content_type: Option<&str>,
        body: Body,
        user_meta: &[(String, String)],
        tags: &[(String, String)],
        checksums_json: &str,
        expect: Option<Expect>,
    ) -> Prepared<S3State> {
        Prepared::new(&S3Command::PutObject {
            bucket: bucket.into(),
            key: key.into(),
            etag: etag.into(),
            size,
            content_type: content_type.map(Into::into),
            body,
            user_meta: user_meta.to_vec(),
            tags: tags.to_vec(),
            checksums_json: checksums_json.into(),
            mtime: Utc::now().to_rfc3339(),
            expect,
        })
    }

    /// Inline objects to move out to chunks at `now` (docs/inline-objects.md).
    pub fn cool_candidates(&self, now: DateTime<Utc>, policy: &crate::CoolPolicy) -> Vec<crate::CoolCandidate> {
        self.read(|s| crate::cool::candidates(s, now, policy))
    }

    /// The `Cool` of `c`, to commit with the chunk its bytes were ingested
    /// as. Outcome: nothing to release, or `PreconditionFailed` / `NoSuchKey`
    /// if the object changed meanwhile (then the chunk is the caller's to
    /// release).
    pub fn prepare_cool(&self, c: &crate::CoolCandidate, extents: &[Extent]) -> Prepared<S3State> {
        Prepared::new(&S3Command::Cool {
            bucket: c.bucket.clone(),
            key: c.key.clone(),
            etag: c.etag.clone(),
            extents: extents.to_vec(),
        })
    }

    /// The `PutPart` of [`Self::put_multipart_part`]. Outcome: the replaced
    /// part's chunk ids, or `NoSuchUpload`.
    pub fn prepare_put_part(
        &self,
        upload_id: &str,
        part_number: i64,
        etag: &str,
        size: i64,
        extents: &[Extent],
    ) -> Prepared<S3State> {
        Prepared::new(&S3Command::PutPart {
            upload_id: upload_id.into(),
            part_number,
            etag: etag.into(),
            size,
            extents: extents.to_vec(),
        })
    }

    fn object<R>(&self, bucket: &str, key: &str, f: impl FnOnce(&ObjectRec) -> R) -> Option<R> {
        self.read(|s| s.objects.get(&(bucket.to_string(), key.to_string())).map(f))
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.object(bucket, key, |o| object_meta(bucket, key, o)))
    }

    /// Everything a GET needs from one version of the object.
    pub async fn get_object_view(&self, bucket: &str, key: &str) -> Result<Option<ObjectView>> {
        Ok(self.object(bucket, key, |o| ObjectView {
            meta: object_meta(bucket, key, o),
            body: o.body.clone(),
            user_meta: pairs(&o.user_meta),
            tag_count: o.tags.len(),
            checksums_json: o.checksums_json.clone(),
        }))
    }

    pub async fn get_extents(&self, bucket: &str, key: &str) -> Result<Option<Vec<Extent>>> {
        Ok(self.object(bucket, key, |o| o.body.extents().to_vec()))
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
        self.propose_infallible(S3Command::PutObjectTags {
            bucket: bucket.into(),
            key: key.into(),
            tags: tags.to_vec(),
        })
        .await?;
        Ok(())
    }

    pub async fn delete_object_tags(&self, bucket: &str, key: &str) -> Result<()> {
        self.propose_infallible(S3Command::PutObjectTags {
            bucket: bucket.into(),
            key: key.into(),
            tags: Vec::new(),
        })
        .await?;
        Ok(())
    }

    /// Delete object; returns extents' chunk ids to `release`, or `None` if
    /// missing. With `expect` (`If-Match`), a missing or different object is
    /// a [`Rejected`] error.
    pub async fn delete_object(&self, bucket: &str, key: &str, expect: Option<Expect>) -> Result<Option<Vec<ChunkId>>> {
        let conditional = expect.is_some();
        let cmd = S3Command::DeleteObject { bucket: bucket.into(), key: key.into(), expect };
        match self.propose(cmd).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(S3Reject::NoSuchKey) if !conditional => Ok(None),
            Err(reason @ (S3Reject::PreconditionFailed | S3Reject::NoSuchKey)) => Err(Rejected(reason).into()),
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
            body: o.body.clone(),
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
        expect: Option<Expect>,
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
                src.body.clone(),
                meta,
                &src.checksums_json,
                &mtime,
                expect,
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

    /// [`Self::copy_object`] of `src` with `body` instead of its own (an
    /// inline source stored as fresh chunks), to commit with those chunks
    /// in one entry. The destination's meta as it will be written.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_copy_object(
        &self,
        src: &SourceSnapshot,
        body: Body,
        dst_bucket: &str,
        dst_key: &str,
        content_type: Option<&str>,
        user_meta: &[(String, String)],
        copy_source_meta: bool,
        expect: Option<Expect>,
    ) -> (Prepared<S3State>, ObjectMeta) {
        let ct = content_type.or(src.content_type.as_deref());
        let meta = if copy_source_meta { &src.user_meta[..] } else { user_meta };
        let mtime = Utc::now().to_rfc3339();
        let put = Prepared::new(&S3Command::PutObject {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            etag: src.etag.clone(),
            size: src.size,
            content_type: ct.map(Into::into),
            body,
            user_meta: meta.to_vec(),
            tags: Vec::new(),
            checksums_json: src.checksums_json.clone(),
            mtime: mtime.clone(),
            expect,
        });
        let dst = ObjectMeta {
            bucket: dst_bucket.into(),
            key: dst_key.into(),
            etag: src.etag.clone(),
            size: src.size,
            content_type: ct.map(Into::into),
            mtime,
        };
        (put, dst)
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

    /// One page of `bucket`'s keys under `prefix`, after `start_after`, keys
    /// that go on past a `delimiter` rolled up into common prefixes.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i64,
        start_after: Option<&str>,
    ) -> Result<ListPage> {
        let max_keys = max_keys.clamp(1, 1000) as usize;
        // An empty delimiter is no delimiter.
        Ok(match delimiter.filter(|d| !d.is_empty()) {
            Some(delim) => self.list_delimited(bucket, prefix, delim, max_keys, start_after),
            None => self.list_flat(bucket, prefix, max_keys, start_after),
        })
    }

    fn list_flat(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: usize,
        start_after: Option<&str>,
    ) -> ListPage {
        let cursor =
            start_after.map_or_else(|| Cursor::From(prefix.into()), |a| Cursor::After(a.into()));
        let end = exclusive_prefix_end(prefix);
        let mut objects = self.scan_objects(
            bucket,
            cursor.lower_bound(prefix),
            end.as_deref(),
            max_keys + 1,
        );
        let truncated = objects.len() > max_keys;
        objects.truncate(max_keys);
        let next = truncated
            .then(|| objects.last().map(|o| o.key.clone()))
            .flatten();
        ListPage {
            objects,
            prefixes: Vec::new(),
            truncated,
            next,
        }
    }

    /// Scans in batches: a common prefix may stand for any number of keys,
    /// and the scan skips past each one it rolls up.
    fn list_delimited(
        &self,
        bucket: &str,
        prefix: &str,
        delim: &str,
        max_keys: usize,
        start_after: Option<&str>,
    ) -> ListPage {
        let batch = (max_keys * 32).clamp(64, 10_000);
        let end = exclusive_prefix_end(prefix);
        let mut cursor = match start_after {
            Some(after) if is_common_prefix(after, prefix, delim) => Cursor::past(after),
            Some(after) => Some(Cursor::After(after.into())),
            None => Some(Cursor::From(prefix.into())),
        };
        let mut page = DelimitedPage::new(max_keys, start_after);
        while let Some(c) = cursor.take() {
            let objects = self.scan_objects(bucket, c.lower_bound(prefix), end.as_deref(), batch);
            let exhausted = objects.len() < batch;
            for (key, entry) in group_delimiter_entries(prefix, delim, objects) {
                cursor = match entry {
                    DelimEntry::Prefix(_) => Cursor::past(&key),
                    DelimEntry::Object(_) => Some(Cursor::After(key.clone())),
                };
                if page.push(key, entry) == Fill::Full {
                    return page.finish();
                }
            }
            if exhausted {
                break;
            }
        }
        page.finish()
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
            released.extend(self.delete_object(&bucket, &key, None).await?.unwrap_or_default());
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
        self.propose_infallible(S3Command::CreateUpload {
            upload_id: upload_id.into(),
            bucket: bucket.into(),
            key: key.into(),
            content_type: content_type.map(Into::into),
            user_meta: user_meta.to_vec(),
            tags: tags.to_vec(),
            checksum_algorithm: checksum_algorithm.map(Into::into),
            initiated_at: Utc::now().to_rfc3339(),
        })
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
        self.propose_infallible(S3Command::PutPart {
            upload_id: upload_id.into(),
            part_number,
            etag: etag.into(),
            size,
            extents: extents.to_vec(),
        })
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
        expect: Option<Expect>,
    ) -> Result<Vec<ChunkId>> {
        let cmd = S3Command::CompleteUpload {
            upload_id: upload.upload_id.clone(),
            part_numbers: part_numbers.to_vec(),
            etag: etag.into(),
            total_size,
            mtime: Utc::now().to_rfc3339(),
            expect,
        };
        match self.propose(cmd).await? {
            Ok(ids) => Ok(ids),
            Err(S3Reject::MissingPart(pn)) => anyhow::bail!("missing part {pn}"),
            Err(reason @ (S3Reject::PreconditionFailed | S3Reject::NoSuchKey)) => Err(Rejected(reason).into()),
            Err(other) => anyhow::bail!("complete_multipart_upload rejected: {other:?}"),
        }
    }

    /// Abort upload; returns all part chunk ids to `release`, or `None` if missing.
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<Option<Vec<ChunkId>>> {
        match self.propose(S3Command::AbortUpload { upload_id: upload_id.into() }).await? {
            Ok(ids) => Ok(Some(ids)),
            Err(S3Reject::NoSuchUpload) => Ok(None),
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

/// One page of a listing.
pub struct ListPage {
    pub objects: Vec<ObjectMeta>,
    pub prefixes: Vec<String>,
    pub truncated: bool,
    /// The last key or common prefix of a truncated page: where the next
    /// page goes on from.
    pub next: Option<String>,
}

/// Where a scan starts.
enum Cursor {
    /// At this key.
    From(String),
    /// Past this key.
    After(String),
}

impl Cursor {
    /// Past every key that starts with `cp` (`None`: there is nothing past).
    fn past(cp: &str) -> Option<Cursor> {
        exclusive_prefix_end(cp).map(Cursor::From)
    }

    /// The scan's lower bound, at `prefix` at the lowest.
    fn lower_bound<'a>(&'a self, prefix: &'a str) -> Bound<&'a str> {
        match self {
            Cursor::From(k) if k.as_str() >= prefix => Bound::Included(k),
            Cursor::After(k) if k.as_str() >= prefix => Bound::Excluded(k),
            _ => Bound::Included(prefix),
        }
    }
}

enum DelimEntry {
    Object(ObjectMeta),
    Prefix(String),
}

#[derive(PartialEq)]
enum Fill {
    More,
    /// One entry over `max_keys`: the page is known to be truncated.
    Full,
}

/// Collects a delimited page: entries after `start_after`, once each, up to
/// one over `max_keys`.
struct DelimitedPage<'a> {
    max_keys: usize,
    start_after: Option<&'a str>,
    entries: Vec<(String, DelimEntry)>,
}

impl<'a> DelimitedPage<'a> {
    fn new(max_keys: usize, start_after: Option<&'a str>) -> Self {
        Self {
            max_keys,
            start_after,
            entries: Vec::new(),
        }
    }

    fn push(&mut self, key: String, entry: DelimEntry) -> Fill {
        let seen = self.start_after.is_some_and(|after| key.as_str() <= after)
            || self.entries.last().is_some_and(|(last, _)| *last == key);
        if !seen {
            self.entries.push((key, entry));
        }
        if self.entries.len() > self.max_keys {
            Fill::Full
        } else {
            Fill::More
        }
    }

    fn finish(mut self) -> ListPage {
        let truncated = self.entries.len() > self.max_keys;
        self.entries.truncate(self.max_keys);
        let next = truncated
            .then(|| self.entries.last().map(|(k, _)| k.clone()))
            .flatten();
        let (mut objects, mut prefixes) = (Vec::new(), Vec::new());
        for (_, e) in self.entries {
            match e {
                DelimEntry::Object(o) => objects.push(o),
                DelimEntry::Prefix(p) => prefixes.push(p),
            }
        }
        ListPage {
            objects,
            prefixes,
            truncated,
            next,
        }
    }
}

/// The least string above every string that starts with `prefix` (`None`:
/// there is none). The last character steps to the next one, not the last
/// byte: a byte step can leave invalid UTF-8 (`¿` is `C2 BF`), and keys sort
/// by UTF-8 bytes, which is code point order.
fn exclusive_prefix_end(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = (last as u32 + 1..=char::MAX as u32).find_map(char::from_u32);
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Whether `s` is a common prefix of this listing (`prefix`, then text
/// without `delim`, then `delim`), not a key that merely ends with `delim`.
fn is_common_prefix(s: &str, prefix: &str, delim: &str) -> bool {
    s.strip_prefix(prefix)
        .and_then(|rest| rest.find(delim).map(|i| i + delim.len() == rest.len()))
        .unwrap_or(false)
}

/// Each object under `prefix` as itself, or as the common prefix it rolls up
/// into (repeated for each object under it: they are adjacent in key order).
fn group_delimiter_entries(
    prefix: &str,
    delim: &str,
    objs: Vec<ObjectMeta>,
) -> Vec<(String, DelimEntry)> {
    objs.into_iter()
        .filter_map(|obj| {
            let rest = obj.key.strip_prefix(prefix)?;
            Some(match rest.find(delim) {
                // Up to and including the whole delimiter (it may be longer
                // than one byte).
                Some(idx) => {
                    let cp = format!("{prefix}{}", &rest[..idx + delim.len()]);
                    (cp.clone(), DelimEntry::Prefix(cp))
                }
                None => (obj.key.clone(), DelimEntry::Object(obj)),
            })
        })
        .collect()
}

pub fn parse_rfc3339(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

/// What [`Index::stats`] counts.
#[derive(Debug, Default, Clone, Copy)]
pub struct IndexStats {
    pub buckets: u64,
    pub objects: u64,
    /// Logical bytes of all objects.
    pub object_bytes: u64,
    /// Multipart uploads in progress.
    pub uploads: u64,
    /// Objects stored inline, and their bytes (docs/inline-objects.md).
    pub inline_objects: u64,
    pub inline_bytes: u64,
}

#[cfg(test)]
mod list_tests {

    //! `list_objects` against a model of the S3 listing it implements: keys
    //! under the prefix, rolled up into common prefixes at the first
    //! delimiter after it, in byte order, after `start_after`, `max_keys` a
    //! page; and paging with `next` returns the whole listing once.

    use super::*;
    use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaStore, Namespaces};
    use pigeonhole_storage_memory::MemoryBlobStore;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
            &xs[(self.next() % xs.len() as u64) as usize]
        }
        fn word(&mut self, alphabet: &[&str], max: u64) -> String {
            (0..=self.next() % max).map(|_| *self.pick(alphabet)).collect()
        }
    }

    /// The model: every entry (object key or common prefix) in order.
    fn model(keys: &[String], prefix: &str, delim: Option<&str>) -> Vec<(String, bool)> {
        let mut out: Vec<(String, bool)> = Vec::new();
        for k in keys {
            let Some(rest) = k.strip_prefix(prefix) else { continue };
            let entry = match delim.filter(|d| !d.is_empty()).and_then(|d| rest.find(d).map(|i| (d, i))) {
                Some((d, i)) => (format!("{prefix}{}", &rest[..i + d.len()]), true),
                None => (k.clone(), false),
            };
            out.push(entry);
        }
        out.sort();
        out.dedup();
        out
    }

    async fn index() -> Index {
        let log = Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
        let snaps = Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
        let meta = MetaStore::open(log, snaps, Index::register(Namespaces::new()), Default::default()).await.unwrap();
        Index::open(meta).await.unwrap()
    }

    #[tokio::test]
    async fn list_objects_matches_the_model_and_pages_cover_it_once() {
        // Characters around the edges: '/', a multi-byte delimiter, bytes
        // whose UTF-8 ends in 0xBF ('¿'), and the top of ASCII.
        let alphabet = ["a", "b", "/", "-", "¿", "é", "z", "\u{7f}"];
        let delimiters = [None, Some(""), Some("/"), Some("-"), Some("--"), Some("¿")];
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for case in 0..400 {
            let ix = index().await;
            let bucket = format!("bucket{case}");
            ix.create_bucket(&bucket).await.unwrap();
            let mut keys: Vec<String> = (0..25).map(|_| rng.word(&alphabet, 6)).collect();
            keys.sort();
            keys.dedup();
            for k in &keys {
                ix.put_object(&bucket, k, "etag", 0, None, Body::Chunks(vec![]), &[], "{}").await.unwrap();
            }
            for _ in 0..8 {
                let prefix = if rng.next() % 3 == 0 { String::new() } else { rng.word(&alphabet, 2) };
                let delim = *rng.pick(&delimiters);
                let max_keys = 1 + (rng.next() % 5) as i64;
                let want = model(&keys, &prefix, delim);
                // Page through it.
                let mut got: Vec<(String, bool)> = Vec::new();
                let mut after: Option<String> = None;
                for _ in 0..200 {
                    let ListPage { objects: objs, prefixes, truncated, next } =
                        ix.list_objects(&bucket, &prefix, delim, max_keys, after.as_deref()).await.unwrap();
                    let mut page: Vec<(String, bool)> = objs.into_iter().map(|o| (o.key, false)).collect();
                    page.extend(prefixes.into_iter().map(|p| (p, true)));
                    page.sort();
                    assert!(page.len() as i64 <= max_keys);
                    got.extend(page);
                    if !truncated {
                        break;
                    }
                    after = Some(next.expect("a truncated page names where to go on"));
                }
                assert_eq!(
                    got, want,
                    "case {case}: keys {keys:?}, prefix {prefix:?}, delimiter {delim:?}, max_keys {max_keys}"
                );
            }
        }
    }

    /// A `start_after` inside a common prefix that spans more than a scan
    /// batch: the scan goes on past the prefix instead of ending the listing.
    #[tokio::test]
    async fn a_start_after_inside_a_big_common_prefix_still_lists_what_follows() {
        let ix = index().await;
        ix.create_bucket("b").await.unwrap();
        for i in 0..100 {
            ix.put_object("b", &format!("a/k{i:03}"), "etag", 0, None, Body::Chunks(vec![]), &[], "{}")
                .await
                .unwrap();
        }
        ix.put_object("b", "c", "etag", 0, None, Body::Chunks(vec![]), &[], "{}")
            .await
            .unwrap();
        let page = ix
            .list_objects("b", "", Some("/"), 1, Some("a/b"))
            .await
            .unwrap();
        let keys: Vec<_> = page.objects.into_iter().map(|o| o.key).collect();
        assert_eq!(
            (keys, page.prefixes, page.truncated),
            (vec!["c".to_string()], vec![], false)
        );
    }

    #[test]
    fn the_prefix_end_steps_a_character() {
        use super::exclusive_prefix_end as end;
        assert_eq!(end("a/").as_deref(), Some("a0"));
        assert_eq!(end("¿").as_deref(), Some("À"), "C2 BF steps to C3 80, not to invalid C2 C0");
        assert_eq!(end("\u{d7ff}").as_deref(), Some("\u{e000}"), "surrogates are skipped");
        assert_eq!(end(&format!("a{}", char::MAX)).as_deref(), Some("b"));
        assert_eq!(end(""), None);
    }
}
