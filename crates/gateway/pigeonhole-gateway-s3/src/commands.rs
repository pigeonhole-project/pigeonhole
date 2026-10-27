//! The S3 namespace of the metadata tablet.
//!
//! [`S3State`] holds buckets, objects and multipart uploads in memory;
//! [`S3State::apply`] is the only code that changes them. It must stay
//! deterministic: timestamps and upload ids come in the command (chosen by
//! [`crate::index::Index`]). Every check runs before the first mutation, so a
//! rejected command leaves the state untouched.

use anyhow::{bail, Context, Result};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Applied, StateMachine};
use prost::Message;
use std::collections::BTreeMap;

pub(crate) mod pb {
    include!(concat!(env!("OUT_DIR"), "/pigeonhole.s3.v1.rs"));
}

use pb::rejection::Reason;

pub(crate) const NAMESPACE: &str = "s3";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketRec {
    pub created_at: String,
    pub scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectRec {
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub mtime: String,
    pub checksums_json: String,
    pub extents: Vec<Extent>,
    /// Name → value, ordered by name.
    pub user_meta: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UploadRec {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub user_meta: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub checksum_algorithm: Option<String>,
    pub initiated_at: String,
    pub parts: BTreeMap<i64, PartRec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartRec {
    pub etag: String,
    pub size: i64,
    pub extents: Vec<Extent>,
}

/// Buckets, objects (by `(bucket, key)`, byte order) and uploads (by id).
#[derive(Debug, Default)]
pub struct S3State {
    pub(crate) buckets: BTreeMap<String, BucketRec>,
    pub(crate) objects: BTreeMap<(String, String), ObjectRec>,
    pub(crate) uploads: BTreeMap<String, UploadRec>,
}

impl StateMachine for S3State {
    const NAME: &'static str = NAMESPACE;

    fn apply(&mut self, command: &[u8]) -> Result<Applied> {
        use pb::command::Op;
        let cmd = pb::Command::decode(command).context("decode s3 command")?;
        match cmd.op.context("empty s3 command")? {
            Op::CreateBucket(c) => Ok(self.create_bucket(c)),
            Op::DeleteBucket(c) => Ok(self.delete_bucket(&c.name)),
            Op::PutObject(c) => Ok(self.put_object(c)),
            Op::DeleteObject(c) => Ok(self.delete_object(c.bucket, c.key)),
            Op::PutObjectTags(c) => Ok(self.put_tags(c.bucket, c.key, c.tags)),
            Op::DeleteObjectTags(c) => Ok(self.put_tags(c.bucket, c.key, Vec::new())),
            Op::CreateUpload(c) => self.create_upload(c),
            Op::PutPart(c) => Ok(self.put_part(c)),
            Op::CompleteUpload(c) => Ok(self.complete_upload(c)),
            Op::AbortUpload(c) => Ok(self.abort_upload(&c.upload_id)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let buckets = self
            .buckets
            .iter()
            .map(|(name, b)| pb::BucketState {
                name: name.clone(),
                created_at: b.created_at.clone(),
                scope_id: b.scope_id.clone(),
            })
            .collect();
        let objects = self
            .objects
            .iter()
            .map(|((bucket, key), o)| pb::ObjectState {
                bucket: bucket.clone(),
                key: key.clone(),
                etag: o.etag.clone(),
                size: o.size,
                content_type: o.content_type.clone(),
                mtime: o.mtime.clone(),
                checksums_json: o.checksums_json.clone(),
                extents: to_pb(&o.extents),
                user_meta: map_to_pairs(&o.user_meta),
                tags: map_to_pairs(&o.tags),
            })
            .collect();
        let uploads = self
            .uploads
            .iter()
            .map(|(id, u)| pb::UploadState {
                upload_id: id.clone(),
                bucket: u.bucket.clone(),
                key: u.key.clone(),
                content_type: u.content_type.clone(),
                user_meta: map_to_pairs(&u.user_meta),
                tags: map_to_pairs(&u.tags),
                checksum_algorithm: u.checksum_algorithm.clone(),
                initiated_at: u.initiated_at.clone(),
                parts: u
                    .parts
                    .iter()
                    .map(|(n, p)| pb::PartState {
                        part_number: *n,
                        etag: p.etag.clone(),
                        size: p.size,
                        extents: to_pb(&p.extents),
                    })
                    .collect(),
            })
            .collect();
        pb::Snapshot { buckets, objects, uploads }.encode_to_vec()
    }

    fn restore(data: &[u8]) -> Result<Self> {
        let snap = pb::Snapshot::decode(data).context("decode s3 snapshot")?;
        let buckets = snap
            .buckets
            .into_iter()
            .map(|b| (b.name, BucketRec { created_at: b.created_at, scope_id: b.scope_id }))
            .collect();
        let objects = snap
            .objects
            .into_iter()
            .map(|o| {
                let rec = ObjectRec {
                    etag: o.etag,
                    size: o.size,
                    content_type: o.content_type,
                    mtime: o.mtime,
                    checksums_json: o.checksums_json,
                    extents: from_pb(&o.extents),
                    user_meta: pairs_to_map(o.user_meta),
                    tags: pairs_to_map(o.tags),
                };
                ((o.bucket, o.key), rec)
            })
            .collect();
        let uploads = snap
            .uploads
            .into_iter()
            .map(|u| {
                let parts = u
                    .parts
                    .into_iter()
                    .map(|p| {
                        let rec = PartRec { etag: p.etag, size: p.size, extents: from_pb(&p.extents) };
                        (p.part_number, rec)
                    })
                    .collect();
                let rec = UploadRec {
                    bucket: u.bucket,
                    key: u.key,
                    content_type: u.content_type,
                    user_meta: pairs_to_map(u.user_meta),
                    tags: pairs_to_map(u.tags),
                    checksum_algorithm: u.checksum_algorithm,
                    initiated_at: u.initiated_at,
                    parts,
                };
                (u.upload_id, rec)
            })
            .collect();
        Ok(Self { buckets, objects, uploads })
    }
}

impl S3State {
    fn create_bucket(&mut self, c: pb::CreateBucket) -> Applied {
        if self.buckets.contains_key(&c.name) {
            return reject(Reason::BucketExists(true));
        }
        self.buckets.insert(c.name, BucketRec { created_at: c.created_at, scope_id: c.scope_id });
        ok()
    }

    /// Refuses a bucket with objects; its unfinished uploads go with it.
    fn delete_bucket(&mut self, name: &str) -> Applied {
        if !self.buckets.contains_key(name) {
            return reject(Reason::BucketNotFound(true));
        }
        if self.bucket_objects(name).next().is_some() {
            return reject(Reason::BucketNotEmpty(true));
        }
        self.buckets.remove(name);
        self.uploads.retain(|_, u| u.bucket != name);
        ok()
    }

    pub(crate) fn bucket_objects<'a>(
        &'a self,
        bucket: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a ObjectRec)> + 'a {
        self.objects
            .range((bucket.to_string(), String::new())..)
            .take_while(move |((b, _), _)| b == bucket)
            .map(|((_, key), o)| (key, o))
    }

    /// Insert or replace an object; returns the replaced version's chunks.
    fn write_object(&mut self, bucket: String, key: String, rec: ObjectRec) -> Vec<ChunkId> {
        let old = self.objects.insert((bucket, key), rec);
        old.map(|o| chunk_ids(&o.extents)).unwrap_or_default()
    }

    fn put_object(&mut self, c: pb::PutObject) -> Applied {
        if !self.buckets.contains_key(&c.bucket) {
            return reject(Reason::BucketNotFound(true));
        }
        let rec = ObjectRec {
            etag: c.etag,
            size: c.size,
            content_type: c.content_type,
            mtime: c.mtime,
            checksums_json: c.checksums_json,
            extents: from_pb(&c.extents),
            user_meta: pairs_to_map(c.user_meta),
            tags: BTreeMap::new(),
        };
        released(self.write_object(c.bucket, c.key, rec))
    }

    fn delete_object(&mut self, bucket: String, key: String) -> Applied {
        match self.objects.remove(&(bucket, key)) {
            Some(o) => released(chunk_ids(&o.extents)),
            None => reject(Reason::NoSuchKey(true)),
        }
    }

    fn put_tags(&mut self, bucket: String, key: String, tags: Vec<pb::Pair>) -> Applied {
        match self.objects.get_mut(&(bucket, key)) {
            Some(o) => {
                o.tags = pairs_to_map(tags);
                ok()
            }
            None => reject(Reason::NoSuchKey(true)),
        }
    }

    fn create_upload(&mut self, c: pb::CreateUpload) -> Result<Applied> {
        if !self.buckets.contains_key(&c.bucket) {
            return Ok(reject(Reason::BucketNotFound(true)));
        }
        if self.uploads.contains_key(&c.upload_id) {
            bail!("upload id {} reused", c.upload_id);
        }
        let rec = UploadRec {
            bucket: c.bucket,
            key: c.key,
            content_type: c.content_type,
            user_meta: pairs_to_map(c.user_meta),
            tags: pairs_to_map(c.tags),
            checksum_algorithm: c.checksum_algorithm,
            initiated_at: c.initiated_at,
            parts: BTreeMap::new(),
        };
        self.uploads.insert(c.upload_id, rec);
        Ok(ok())
    }

    /// Store a part; returns the replaced part's chunks.
    fn put_part(&mut self, c: pb::PutPart) -> Applied {
        let Some(upload) = self.uploads.get_mut(&c.upload_id) else {
            return reject(Reason::NoSuchUpload(true));
        };
        let rec = PartRec { etag: c.etag, size: c.size, extents: from_pb(&c.extents) };
        let old = upload.parts.insert(c.part_number, rec);
        released(old.map(|p| chunk_ids(&p.extents)).unwrap_or_default())
    }

    /// Assemble the object from parts; part extents move to the object (no
    /// release), parts not listed are dropped with the upload.
    fn complete_upload(&mut self, c: pb::CompleteUpload) -> Applied {
        let Some(upload) = self.uploads.get(&c.upload_id) else {
            return reject(Reason::NoSuchUpload(true));
        };
        let mut extents = Vec::new();
        for &pn in &c.part_numbers {
            let Some(part) = upload.parts.get(&pn) else {
                return reject(Reason::MissingPart(pn));
            };
            extents.extend_from_slice(&part.extents);
        }
        let upload = self.uploads.remove(&c.upload_id).expect("checked above");
        let rec = ObjectRec {
            etag: c.etag,
            size: c.total_size,
            content_type: upload.content_type,
            mtime: c.mtime,
            checksums_json: "{}".into(),
            extents,
            user_meta: upload.user_meta,
            tags: upload.tags,
        };
        released(self.write_object(upload.bucket, upload.key, rec))
    }

    /// Drop an upload; returns every part's chunks.
    fn abort_upload(&mut self, upload_id: &str) -> Applied {
        let Some(upload) = self.uploads.remove(upload_id) else {
            return reject(Reason::NoSuchUpload(true));
        };
        released(upload.parts.values().flat_map(|p| chunk_ids(&p.extents)).collect())
    }
}

fn ok() -> Applied {
    Applied::Commit(Vec::new())
}

fn released(mut chunk_ids: Vec<ChunkId>) -> Applied {
    chunk_ids.sort_unstable();
    chunk_ids.dedup();
    Applied::Commit(pb::Released { chunk_ids }.encode_to_vec())
}

fn reject(reason: Reason) -> Applied {
    Applied::Reject(pb::Rejection { reason: Some(reason) }.encode_to_vec())
}

fn chunk_ids(extents: &[Extent]) -> Vec<ChunkId> {
    extents.iter().map(|e| e.chunk).collect()
}

pub(crate) fn to_pb(extents: &[Extent]) -> Vec<pb::Extent> {
    extents
        .iter()
        .map(|e| pb::Extent { chunk: e.chunk, offset: e.offset, len: e.len })
        .collect()
}

fn from_pb(extents: &[pb::Extent]) -> Vec<Extent> {
    extents
        .iter()
        .map(|e| Extent { chunk: e.chunk, offset: e.offset, len: e.len })
        .collect()
}

/// Later duplicates of a name win, as a repeated header would.
fn pairs_to_map(pairs: Vec<pb::Pair>) -> BTreeMap<String, String> {
    pairs.into_iter().map(|p| (p.name, p.value)).collect()
}

fn map_to_pairs(map: &BTreeMap<String, String>) -> Vec<pb::Pair> {
    map.iter()
        .map(|(name, value)| pb::Pair { name: name.clone(), value: value.clone() })
        .collect()
}
