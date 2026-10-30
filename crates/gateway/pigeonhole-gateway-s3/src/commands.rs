//! The S3 namespace of the metadata tablet.
//!
//! [`S3State`] holds buckets, objects and multipart uploads in memory;
//! [`S3State::apply`] is the only code that changes them. It must stay
//! deterministic: timestamps and upload ids come in the command (chosen by
//! [`crate::index::Index`]). Every check runs before the first mutation, so a
//! rejected command leaves the state untouched.

use anyhow::{bail, Result};
use pigeonhole_chunk_store::{ChunkId, Extent};
use pigeonhole_meta::{Outcome, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const NAMESPACE: &str = "s3";

/// `(name, value)` pairs: user metadata or tags. Later duplicates of a name
/// win, as a repeated header would.
pub type Pairs = Vec<(String, String)>;

/// What a write expects of its key's current object: an S3 conditional
/// write (docs/conditional-writes.md), checked when the command applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expect {
    /// `If-None-Match: *`: no object under the key.
    Absent,
    /// `If-Match: <etag>`: the object, with this ETag (hex, unquoted).
    ETag(String),
    /// `If-Match: *`: any object under the key.
    Present,
    /// `If-None-Match: <etag>`: no object with this ETag (none at all is fine).
    NotETag(String),
}

/// Where an object's bytes are (docs/inline-objects.md).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Body {
    /// In chunks of the chunk store.
    Chunks(Vec<Extent>),
    /// In the record itself: a small object, in the log and snapshots.
    Inline(Vec<u8>),
}

impl Body {
    /// The chunk extents; none for an inline body.
    pub fn extents(&self) -> &[Extent] {
        match self {
            Body::Chunks(extents) => extents,
            Body::Inline(_) => &[],
        }
    }

    /// Bytes held in the metadata.
    pub fn inline_len(&self) -> u64 {
        match self {
            Body::Chunks(_) => 0,
            Body::Inline(bytes) => bytes.len() as u64,
        }
    }
}

/// Commands of the `s3` namespace. Changing the shape of a variant or the
/// order of variants needs a new [`S3State::VERSION`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Command {
    CreateBucket { name: String, created_at: String },
    DeleteBucket { name: String },
    /// Insert or replace an object with its tags (user metadata and tags of
    /// the old version go).
    PutObject {
        bucket: String,
        key: String,
        etag: String,
        size: i64,
        content_type: Option<String>,
        body: Body,
        user_meta: Pairs,
        tags: Pairs,
        checksums_json: String,
        mtime: String,
        expect: Option<Expect>,
    },
    DeleteObject { bucket: String, key: String, expect: Option<Expect> },
    /// Replace an object's tags (empty = delete them).
    PutObjectTags { bucket: String, key: String, tags: Pairs },
    CreateUpload {
        upload_id: String,
        bucket: String,
        key: String,
        content_type: Option<String>,
        user_meta: Pairs,
        tags: Pairs,
        checksum_algorithm: Option<String>,
        initiated_at: String,
    },
    PutPart { upload_id: String, part_number: i64, etag: String, size: i64, extents: Vec<Extent> },
    /// Assemble the object from parts; part extents move to the object (no release).
    CompleteUpload {
        upload_id: String,
        part_numbers: Vec<i64>,
        etag: String,
        total_size: i64,
        mtime: String,
        expect: Option<Expect>,
    },
    AbortUpload { upload_id: String },
    /// Move an inline object out to chunks (the cooler); its bytes, ETag and
    /// everything else stay. Rejected if the object is no longer the inline
    /// one with `etag`: a rewrite or delete always wins over cooling.
    Cool { bucket: String, key: String, etag: String, extents: Vec<Extent> },
}

/// Deterministic failure of an [`S3Command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Reject {
    BucketExists,
    BucketNotFound,
    BucketNotEmpty,
    NoSuchKey,
    NoSuchUpload,
    MissingPart(i64),
    /// The key's object is not what the write's [`Expect`] said.
    PreconditionFailed,
}

/// Reply of every committed command: chunk ids the caller must release
/// (replaced or removed), sorted and unique; empty when nothing was freed.
pub type Released = Vec<ChunkId>;

type Applied = std::result::Result<Released, S3Reject>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BucketRec {
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObjectRec {
    pub etag: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub mtime: String,
    pub checksums_json: String,
    pub body: Body,
    /// Name → value, ordered by name.
    pub user_meta: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PartRec {
    pub etag: String,
    pub size: i64,
    pub extents: Vec<Extent>,
}

/// Buckets, objects (by `(bucket, key)`, byte order) and uploads (by id).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct S3State {
    pub(crate) buckets: BTreeMap<String, BucketRec>,
    pub(crate) objects: BTreeMap<(String, String), ObjectRec>,
    pub(crate) uploads: BTreeMap<String, UploadRec>,
    /// Sum of the inline bodies (derived).
    #[serde(skip)]
    pub(crate) inline_bytes: u64,
}

impl StateMachine for S3State {
    const NAME: &'static str = NAMESPACE;
    const VERSION: u32 = 5;
    type Command = S3Command;
    type Reply = Released;
    type Reject = S3Reject;

    fn apply(&mut self, command: S3Command) -> Result<Outcome<Self>> {
        Ok(match command {
            S3Command::CreateBucket { name, created_at } => self.create_bucket(name, created_at),
            S3Command::DeleteBucket { name } => self.delete_bucket(&name),
            S3Command::PutObject {
                bucket,
                key,
                etag,
                size,
                content_type,
                body,
                user_meta,
                tags,
                checksums_json,
                mtime,
                expect,
            } => {
                let rec = ObjectRec {
                    etag,
                    size,
                    content_type,
                    mtime,
                    checksums_json,
                    body,
                    user_meta: user_meta.into_iter().collect(),
                    tags: tags.into_iter().collect(),
                };
                self.put_object(bucket, key, rec, expect.as_ref())
            }
            S3Command::DeleteObject { bucket, key, expect } => self.delete_object(bucket, key, expect.as_ref()),
            S3Command::PutObjectTags { bucket, key, tags } => match self.objects.get_mut(&(bucket, key)) {
                Some(o) => {
                    o.tags = tags.into_iter().collect();
                    ok()
                }
                None => Err(S3Reject::NoSuchKey),
            },
            S3Command::CreateUpload {
                upload_id,
                bucket,
                key,
                content_type,
                user_meta,
                tags,
                checksum_algorithm,
                initiated_at,
            } => {
                let rec = UploadRec {
                    bucket,
                    key,
                    content_type,
                    user_meta: user_meta.into_iter().collect(),
                    tags: tags.into_iter().collect(),
                    checksum_algorithm,
                    initiated_at,
                    parts: BTreeMap::new(),
                };
                self.create_upload(upload_id, rec)?
            }
            S3Command::PutPart { upload_id, part_number, etag, size, extents } => {
                let Some(upload) = self.uploads.get_mut(&upload_id) else {
                    return Ok(Err(S3Reject::NoSuchUpload));
                };
                let old = upload.parts.insert(part_number, PartRec { etag, size, extents });
                released(old.map(|p| chunk_ids(&p.extents)).unwrap_or_default())
            }
            S3Command::CompleteUpload { upload_id, part_numbers, etag, total_size, mtime, expect } => {
                self.complete_upload(&upload_id, &part_numbers, etag, total_size, mtime, expect.as_ref())
            }
            S3Command::AbortUpload { upload_id } => {
                let Some(upload) = self.uploads.remove(&upload_id) else {
                    return Ok(Err(S3Reject::NoSuchUpload));
                };
                released(upload.parts.values().flat_map(|p| chunk_ids(&p.extents)).collect())
            }
            S3Command::Cool { bucket, key, etag, extents } => self.cool(bucket, key, etag, extents),
        })
    }

    fn rebuild(&mut self) {
        self.inline_bytes = self.objects.values().map(|o| o.body.inline_len()).sum();
    }
}

impl S3State {
    /// Whether the object under `(bucket, key)` is what `expect` says (none
    /// expected: anything is).
    pub(crate) fn check(&self, bucket: &str, key: &str, expect: Option<&Expect>) -> std::result::Result<(), S3Reject> {
        let current = self.objects.get(&(bucket.to_string(), key.to_string()));
        match (expect, current) {
            (None, _) | (Some(Expect::Absent | Expect::NotETag(_)), None) => Ok(()),
            (Some(Expect::ETag(e)), Some(o)) if o.etag == *e => Ok(()),
            (Some(Expect::NotETag(e)), Some(o)) if o.etag != *e => Ok(()),
            (Some(Expect::Present), Some(_)) => Ok(()),
            (Some(Expect::ETag(_) | Expect::Present), None) => Err(S3Reject::NoSuchKey),
            (Some(_), Some(_)) => Err(S3Reject::PreconditionFailed),
        }
    }

    fn create_bucket(&mut self, name: String, created_at: String) -> Applied {
        if self.buckets.contains_key(&name) {
            return Err(S3Reject::BucketExists);
        }
        self.buckets.insert(name, BucketRec { created_at });
        ok()
    }

    /// Write `rec` under `(bucket, key)` if that holds what `expect` says.
    fn put_object(&mut self, bucket: String, key: String, rec: ObjectRec, expect: Option<&Expect>) -> Applied {
        if !self.buckets.contains_key(&bucket) {
            return Err(S3Reject::BucketNotFound);
        }
        self.check(&bucket, &key, expect)?;
        released(self.write_object(bucket, key, rec))
    }

    fn delete_object(&mut self, bucket: String, key: String, expect: Option<&Expect>) -> Applied {
        self.check(&bucket, &key, expect)?;
        match self.remove_object(bucket, key) {
            Some(o) => released(chunk_ids(o.body.extents())),
            None => Err(S3Reject::NoSuchKey),
        }
    }

    /// A reused upload id is a bug of ours, not a refusal.
    fn create_upload(&mut self, upload_id: String, rec: UploadRec) -> Result<Applied> {
        if !self.buckets.contains_key(&rec.bucket) {
            return Ok(Err(S3Reject::BucketNotFound));
        }
        if self.uploads.contains_key(&upload_id) {
            bail!("upload id {upload_id} reused");
        }
        self.uploads.insert(upload_id, rec);
        Ok(ok())
    }

    /// Move an inline object out to `extents`, unless it changed (no longer
    /// `etag`, or not inline any more).
    fn cool(&mut self, bucket: String, key: String, etag: String, extents: Vec<Extent>) -> Applied {
        self.check(&bucket, &key, Some(&Expect::ETag(etag)))?;
        let o = self.objects.get_mut(&(bucket, key)).expect("checked above");
        let Body::Inline(bytes) = &o.body else {
            return Err(S3Reject::PreconditionFailed);
        };
        self.inline_bytes -= bytes.len() as u64;
        o.body = Body::Chunks(extents);
        ok()
    }

    /// Refuses a bucket with objects; its unfinished uploads go with it.
    fn delete_bucket(&mut self, name: &str) -> Applied {
        if !self.buckets.contains_key(name) {
            return Err(S3Reject::BucketNotFound);
        }
        if self.bucket_objects(name).next().is_some() {
            return Err(S3Reject::BucketNotEmpty);
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
        self.inline_bytes += rec.body.inline_len();
        let Some(old) = self.objects.insert((bucket, key), rec) else { return Vec::new() };
        self.inline_bytes -= old.body.inline_len();
        chunk_ids(old.body.extents())
    }

    fn remove_object(&mut self, bucket: String, key: String) -> Option<ObjectRec> {
        let old = self.objects.remove(&(bucket, key))?;
        self.inline_bytes -= old.body.inline_len();
        Some(old)
    }

    /// Parts not listed are dropped with the upload.
    fn complete_upload(
        &mut self,
        upload_id: &str,
        part_numbers: &[i64],
        etag: String,
        total_size: i64,
        mtime: String,
        expect: Option<&Expect>,
    ) -> Applied {
        let Some(upload) = self.uploads.get(upload_id) else {
            return Err(S3Reject::NoSuchUpload);
        };
        self.check(&upload.bucket, &upload.key, expect)?;
        let mut extents = Vec::new();
        for &pn in part_numbers {
            let Some(part) = upload.parts.get(&pn) else {
                return Err(S3Reject::MissingPart(pn));
            };
            extents.extend_from_slice(&part.extents);
        }
        let upload = self.uploads.remove(upload_id).expect("checked above");
        let rec = ObjectRec {
            etag,
            size: total_size,
            content_type: upload.content_type,
            mtime,
            checksums_json: "{}".into(),
            body: Body::Chunks(extents),
            user_meta: upload.user_meta,
            tags: upload.tags,
        };
        released(self.write_object(upload.bucket, upload.key, rec))
    }
}

fn ok() -> Applied {
    Ok(Vec::new())
}

fn released(mut chunk_ids: Vec<ChunkId>) -> Applied {
    chunk_ids.sort_unstable();
    chunk_ids.dedup();
    Ok(chunk_ids)
}

fn chunk_ids(extents: &[Extent]) -> Vec<ChunkId> {
    extents.iter().map(|e| e.chunk).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails when the encoding of a command changes: bump `VERSION` and update
    /// the expected bytes (old logs then refuse to replay, as they should).
    #[test]
    fn command_encoding_is_pinned() {
        let cmd = S3Command::DeleteObject { bucket: "b".into(), key: "k".into(), expect: None };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [3, 1, b'b', 1, b'k', 0]);
        let cmd = S3Command::DeleteObject { bucket: "b".into(), key: "k".into(), expect: Some(Expect::ETag("e".into())) };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [3, 1, b'b', 1, b'k', 1, 1, 1, b'e']);
        let cmd = S3Command::CreateBucket { name: "b".into(), created_at: "t".into() };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [0, 1, b'b', 1, b't']);
        assert_eq!(S3State::VERSION, 5);
    }

    #[test]
    fn rejected_commands_leave_the_state_untouched() {
        let mut s = S3State::default();
        let put = |key: &str| S3Command::PutObject {
            bucket: "b".into(),
            key: key.into(),
            etag: "e".into(),
            size: 1,
            content_type: None,
            body: Body::Chunks(vec![Extent { chunk: 7, offset: 0, len: 1 }]),
            user_meta: vec![],
            tags: vec![],
            checksums_json: "{}".into(),
            mtime: "t".into(),
            expect: None,
        };
        assert_eq!(s.apply(put("k")).unwrap(), Err(S3Reject::BucketNotFound));
        let create = S3Command::CreateBucket { name: "b".into(), created_at: "t".into() };
        assert_eq!(s.apply(create.clone()).unwrap(), Ok(vec![]));
        assert_eq!(s.apply(create).unwrap(), Err(S3Reject::BucketExists));
        assert_eq!(s.apply(put("k")).unwrap(), Ok(vec![]));
        assert_eq!(s.apply(put("k")).unwrap(), Ok(vec![7]), "overwrite releases the old chunks");
        let before = pigeonhole_meta::encode_state(&s);
        let done = S3Command::CompleteUpload {
            upload_id: "u".into(),
            part_numbers: vec![1],
            etag: "e".into(),
            total_size: 1,
            mtime: "t".into(),
            expect: None,
        };
        assert_eq!(s.apply(done).unwrap(), Err(S3Reject::NoSuchUpload));
        assert_eq!(s.apply(S3Command::DeleteBucket { name: "b".into() }).unwrap(), Err(S3Reject::BucketNotEmpty));
        assert_eq!(pigeonhole_meta::encode_state(&s), before);
    }

    /// docs/conditional-writes.md § Semantics, row by row; a refusal changes nothing.
    #[test]
    fn conditional_writes_follow_the_table() {
        let mut s = S3State::default();
        s.apply(S3Command::CreateBucket { name: "b".into(), created_at: "t".into() }).unwrap().unwrap();
        let put = |etag: &str, expect: Option<Expect>| S3Command::PutObject {
            bucket: "b".into(),
            key: "k".into(),
            etag: etag.into(),
            size: 1,
            content_type: None,
            body: Body::Inline(etag.as_bytes().to_vec()),
            user_meta: vec![],
            tags: vec![],
            checksums_json: "{}".into(),
            mtime: "t".into(),
            expect,
        };
        let del = |expect: Option<Expect>| S3Command::DeleteObject { bucket: "b".into(), key: "k".into(), expect };
        let etag = |e: &str| Some(Expect::ETag(e.into()));

        // Absent key.
        assert_eq!(s.apply(put("1", etag("1"))).unwrap(), Err(S3Reject::NoSuchKey));
        assert_eq!(s.apply(del(etag("1"))).unwrap(), Err(S3Reject::NoSuchKey));
        assert_eq!(s.apply(put("1", Some(Expect::Absent))).unwrap(), Ok(vec![]));

        // Present, ETag "1".
        let before = pigeonhole_meta::encode_state(&s);
        assert_eq!(s.apply(put("2", Some(Expect::Absent))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(s.apply(put("2", etag("0"))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(s.apply(del(etag("0"))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(pigeonhole_meta::encode_state(&s), before);
        assert_eq!(s.apply(put("2", etag("1"))).unwrap(), Ok(vec![]));
        assert_eq!(s.apply(del(etag("2"))).unwrap(), Ok(vec![]));
        assert!(s.objects.is_empty());
    }

    /// docs/inline-objects.md: inline bodies are counted; a rewrite moves an
    /// object between inline and chunks; `Cool` loses to any rewrite.
    #[test]
    fn inline_bodies_and_cooling() {
        let mut s = S3State::default();
        s.apply(S3Command::CreateBucket { name: "b".into(), created_at: "t".into() }).unwrap().unwrap();
        let put = |key: &str, etag: &str, body: Body| S3Command::PutObject {
            bucket: "b".into(),
            key: key.into(),
            etag: etag.into(),
            size: 3,
            content_type: None,
            body,
            user_meta: vec![],
            tags: vec![],
            checksums_json: "{}".into(),
            mtime: "t".into(),
            expect: None,
        };
        let cool = |key: &str, etag: &str, chunk: ChunkId| S3Command::Cool {
            bucket: "b".into(),
            key: key.into(),
            etag: etag.into(),
            extents: vec![Extent { chunk, offset: 0, len: 3 }],
        };
        let chunks = |chunk: ChunkId| Body::Chunks(vec![Extent { chunk, offset: 0, len: 3 }]);

        assert_eq!(s.apply(put("a", "1", Body::Inline(b"abc".to_vec()))).unwrap(), Ok(vec![]));
        assert_eq!(s.apply(put("b", "1", Body::Inline(b"xyz".to_vec()))).unwrap(), Ok(vec![]));
        assert_eq!(s.inline_bytes, 6);

        // Inline over chunks releases them; chunks over inline release nothing.
        assert_eq!(s.apply(put("c", "1", chunks(9))).unwrap(), Ok(vec![]));
        assert_eq!(s.apply(put("c", "2", Body::Inline(b"new".to_vec()))).unwrap(), Ok(vec![9]));
        assert_eq!(s.apply(put("c", "3", chunks(10))).unwrap(), Ok(vec![]));
        assert_eq!(s.inline_bytes, 6);

        // Cooling: accepted for the same inline version, refused otherwise.
        assert_eq!(s.apply(cool("a", "1", 11)).unwrap(), Ok(vec![]));
        assert_eq!(s.objects[&("b".into(), "a".into())].body, chunks(11));
        assert_eq!(s.inline_bytes, 3);
        assert_eq!(s.apply(cool("a", "1", 12)).unwrap(), Err(S3Reject::PreconditionFailed), "no longer inline");
        assert_eq!(s.apply(cool("b", "0", 12)).unwrap(), Err(S3Reject::PreconditionFailed), "rewritten meanwhile");
        assert_eq!(s.apply(cool("gone", "1", 12)).unwrap(), Err(S3Reject::NoSuchKey));

        // Delete drops the count; a replay (decode + rebuild) recounts it.
        assert_eq!(s.apply(S3Command::DeleteObject { bucket: "b".into(), key: "b".into(), expect: None }).unwrap(), Ok(vec![]));
        assert_eq!(s.inline_bytes, 0);
        s.apply(put("d", "1", Body::Inline(b"0123456789".to_vec()))).unwrap().unwrap();
        let replayed: S3State = pigeonhole_meta::decode_state(&pigeonhole_meta::encode_state(&s)).unwrap();
        assert_eq!(replayed.inline_bytes, 10);
    }
}
