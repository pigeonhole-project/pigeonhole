//! The S3 namespace of the metadata tablet.
//!
//! [`S3State`] holds buckets, objects and multipart uploads in memory;
//! [`S3State::apply`] is the only code that changes them. It must stay
//! deterministic: timestamps and upload ids come in the command (chosen by
//! [`crate::index::Index`]). Every check runs before the first mutation, so a
//! rejected command leaves the state untouched.

use anyhow::Result;
use md5::{Digest, Md5};
use pigeonhole_chunk_store::{unique_chunk_ids, ChunkId, Extent};
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
    CreateBucket {
        name: String,
        created_at: String,
    },
    DeleteBucket {
        name: String,
    },
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
    DeleteObject {
        bucket: String,
        key: String,
        expect: Option<Expect>,
    },
    /// Delete several objects of one bucket, each if it is what its
    /// `expect` says (a missing key without one counts as deleted). Reply:
    /// [`S3Reply::Deleted`].
    DeleteObjects {
        bucket: String,
        objects: Vec<(String, Option<Expect>)>,
    },
    /// Replace an object's tags (empty = delete them).
    PutObjectTags {
        bucket: String,
        key: String,
        tags: Pairs,
    },
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
    PutPart {
        upload_id: String,
        part_number: i64,
        etag: String,
        size: i64,
        extents: Vec<Extent>,
    },
    /// Assemble the object from `parts` (`(number, etag)`, ascending; each
    /// must be the part stored under that number). Its size and ETag come
    /// from the parts assembled; their extents move to the object, and the
    /// chunks of the parts not listed are released.
    CompleteUpload {
        upload_id: String,
        parts: Vec<(i64, String)>,
        mtime: String,
        expect: Option<Expect>,
    },
    AbortUpload {
        upload_id: String,
    },
    /// Move an inline object out to chunks (the cooler); its bytes, ETag and
    /// everything else stay. Rejected if the object is no longer the inline
    /// one with `etag`: a rewrite or delete always wins over cooling.
    Cool {
        bucket: String,
        key: String,
        etag: String,
        extents: Vec<Extent>,
    },
}

/// Deterministic failure of an [`S3Command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Reject {
    BucketExists,
    BucketNotFound,
    BucketNotEmpty,
    NoSuchKey,
    NoSuchUpload,
    /// CompleteUpload: no part under this number, or not with the ETag listed.
    InvalidPart(i64),
    /// The key's object is not what the write's [`Expect`] said.
    PreconditionFailed,
    /// CreateUpload: the upload id is taken (ids are random: a bug of ours,
    /// but a proposal's content, so a rejection and not an apply error).
    UploadExists,
    /// CompleteUpload: part numbers not ascending, or one listed twice.
    InvalidPartOrder,
}

/// Chunk ids the caller must release (replaced or removed), sorted; empty
/// when nothing was freed. An object or a part holds one ref on each chunk
/// it points into, so a chunk is here once for each holder that let it go.
pub type Released = Vec<ChunkId>;

/// Reply of a committed command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Reply {
    /// Every command but `DeleteObjects`.
    Released(Released),
    /// `DeleteObjects`: the keys not deleted, and why; every other key is
    /// gone (or was never there).
    Deleted { released: Released, refused: Vec<(String, S3Reject)> },
}

impl S3Reply {
    /// The chunks to release.
    pub fn released(self) -> Released {
        match self {
            S3Reply::Released(ids) | S3Reply::Deleted { released: ids, .. } => ids,
        }
    }
}

type Applied = std::result::Result<S3Reply, S3Reject>;

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
    const VERSION: u32 = 6;
    type Command = S3Command;
    type Reply = S3Reply;
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
            S3Command::DeleteObjects { bucket, objects } => self.delete_objects(bucket, objects),
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
                self.create_upload(upload_id, rec)
            }
            S3Command::PutPart { upload_id, part_number, etag, size, extents } => {
                let Some(upload) = self.uploads.get_mut(&upload_id) else {
                    return Ok(Err(S3Reject::NoSuchUpload));
                };
                let old = upload.parts.insert(part_number, PartRec { etag, size, extents });
                released(old.iter().map(|p| &p.extents[..]))
            }
            S3Command::CompleteUpload { upload_id, parts, mtime, expect } => {
                self.complete_upload(&upload_id, &parts, mtime, expect.as_ref())
            }
            S3Command::AbortUpload { upload_id } => {
                let Some(upload) = self.uploads.remove(&upload_id) else {
                    return Ok(Err(S3Reject::NoSuchUpload));
                };
                released(upload.parts.values().map(|p| &p.extents[..]))
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
        released([&self.write_object(bucket, key, rec)[..]])
    }

    fn delete_object(&mut self, bucket: String, key: String, expect: Option<&Expect>) -> Applied {
        self.check(&bucket, &key, expect)?;
        match self.remove_object(bucket, key) {
            Some(o) => released([o.body.extents()]),
            None => Err(S3Reject::NoSuchKey),
        }
    }

    /// Each object as [`Self::delete_object`] would, on its own.
    fn delete_objects(&mut self, bucket: String, objects: Vec<(String, Option<Expect>)>) -> Applied {
        if !self.buckets.contains_key(&bucket) {
            return Err(S3Reject::BucketNotFound);
        }
        let (mut gone, mut refused) = (Vec::new(), Vec::new());
        for (key, expect) in objects {
            if let Err(reason) = self.check(&bucket, &key, expect.as_ref()) {
                refused.push((key, reason));
                continue;
            }
            gone.extend(self.remove_object(bucket.clone(), key));
        }
        Ok(S3Reply::Deleted { released: freed(gone.iter().map(|o| o.body.extents())), refused })
    }

    fn create_upload(&mut self, upload_id: String, rec: UploadRec) -> Applied {
        if !self.buckets.contains_key(&rec.bucket) {
            return Err(S3Reject::BucketNotFound);
        }
        if self.uploads.contains_key(&upload_id) {
            return Err(S3Reject::UploadExists);
        }
        self.uploads.insert(upload_id, rec);
        ok()
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

    /// Refuses a bucket with objects; its unfinished uploads go with it,
    /// their parts' chunks to release.
    fn delete_bucket(&mut self, name: &str) -> Applied {
        if !self.buckets.contains_key(name) {
            return Err(S3Reject::BucketNotFound);
        }
        if self.bucket_objects(name).next().is_some() {
            return Err(S3Reject::BucketNotEmpty);
        }
        self.buckets.remove(name);
        let ids: Vec<String> =
            self.uploads.iter().filter(|(_, u)| u.bucket == name).map(|(id, _)| id.clone()).collect();
        let dropped: Vec<UploadRec> = ids.iter().filter_map(|id| self.uploads.remove(id)).collect();
        released(dropped.iter().flat_map(|u| u.parts.values()).map(|p| &p.extents[..]))
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

    /// Insert or replace an object; returns the replaced version's extents.
    fn write_object(&mut self, bucket: String, key: String, rec: ObjectRec) -> Vec<Extent> {
        self.inline_bytes += rec.body.inline_len();
        let Some(old) = self.objects.insert((bucket, key), rec) else { return Vec::new() };
        self.inline_bytes -= old.body.inline_len();
        old.body.extents().to_vec()
    }

    fn remove_object(&mut self, bucket: String, key: String) -> Option<ObjectRec> {
        let old = self.objects.remove(&(bucket, key))?;
        self.inline_bytes -= old.body.inline_len();
        Some(old)
    }

    /// The object assembled from `parts` (checked against the parts
    /// stored); the parts not listed are dropped with the upload.
    fn complete_upload(
        &mut self,
        upload_id: &str,
        parts: &[(i64, String)],
        mtime: String,
        expect: Option<&Expect>,
    ) -> Applied {
        let Some(upload) = self.uploads.get(upload_id) else {
            return Err(S3Reject::NoSuchUpload);
        };
        if parts.is_empty() {
            return Err(S3Reject::InvalidPart(0));
        }
        if parts.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err(S3Reject::InvalidPartOrder);
        }
        let mut extents = Vec::new();
        let mut size = 0;
        for (pn, etag) in parts {
            match upload.parts.get(pn) {
                Some(part) if part.etag == *etag => {
                    extents.extend_from_slice(&part.extents);
                    size += part.size;
                }
                _ => return Err(S3Reject::InvalidPart(*pn)),
            }
        }
        let etag = multipart_etag(parts.iter().map(|(_, e)| e.as_str())).ok_or(S3Reject::InvalidPart(0))?;
        self.check(&upload.bucket, &upload.key, expect)?;
        let mut upload = self.uploads.remove(upload_id).expect("checked above");
        let listed: Vec<PartRec> = parts.iter().filter_map(|(pn, _)| upload.parts.remove(pn)).collect();
        let key = (upload.bucket.clone(), upload.key.clone());
        let rec = ObjectRec {
            etag,
            size,
            content_type: upload.content_type,
            mtime,
            checksums_json: "{}".into(),
            body: Body::Chunks(extents),
            user_meta: upload.user_meta,
            tags: upload.tags,
        };
        let old = self.write_object(upload.bucket, upload.key, rec);
        // The listed parts' refs go to the object, which holds one per chunk:
        // a chunk two parts share gives one back.
        let mut surplus = freed(listed.iter().map(|p| &p.extents[..]));
        let object = self.objects.get(&key).expect("written above");
        for id in unique_chunk_ids(object.body.extents()) {
            let at = surplus.binary_search(&id).expect("each part's chunks are listed");
            surplus.remove(at);
        }
        let mut ids = freed(std::iter::once(&old[..]).chain(upload.parts.values().map(|p| &p.extents[..])));
        ids.extend(surplus);
        ids.sort_unstable();
        Ok(S3Reply::Released(ids))
    }
}

/// The ETag of a multipart object made of parts with these ETags: the MD5
/// of their MD5s, then `-` and their count. `None` if one is not an MD5 in
/// hex.
pub(crate) fn multipart_etag<'a>(part_etags: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut md5 = Md5::new();
    let mut count = 0;
    for etag in part_etags {
        md5.update(hex::decode(etag).ok()?);
        count += 1;
    }
    Some(format!("{:x}-{count}", md5.finalize()))
}

fn ok() -> Applied {
    Ok(S3Reply::Released(Vec::new()))
}

/// The chunks `holders` (objects, parts) let go, to release.
fn released<'a>(holders: impl IntoIterator<Item = &'a [Extent]>) -> Applied {
    Ok(S3Reply::Released(freed(holders)))
}

/// One ref for each chunk of each holder, sorted (see [`Released`]).
fn freed<'a>(holders: impl IntoIterator<Item = &'a [Extent]>) -> Released {
    let mut ids: Released = holders.into_iter().flat_map(unique_chunk_ids).collect();
    ids.sort_unstable();
    ids
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
        let cmd =
            S3Command::DeleteObject { bucket: "b".into(), key: "k".into(), expect: Some(Expect::ETag("e".into())) };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [3, 1, b'b', 1, b'k', 1, 1, 1, b'e']);
        let cmd = S3Command::CreateBucket { name: "b".into(), created_at: "t".into() };
        assert_eq!(postcard::to_stdvec(&cmd).unwrap(), [0, 1, b'b', 1, b't']);
        assert_eq!(S3State::VERSION, 6);
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
        assert_eq!(s.apply(create.clone()).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.apply(create).unwrap(), Err(S3Reject::BucketExists));
        assert_eq!(s.apply(put("k")).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.apply(put("k")).unwrap(), Ok(S3Reply::Released(vec![7])), "overwrite releases the old chunks");
        let before = pigeonhole_meta::encode_state(&s);
        let done = S3Command::CompleteUpload {
            upload_id: "u".into(),
            parts: vec![(1, "e".into())],
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
        assert_eq!(s.apply(put("1", Some(Expect::Absent))).unwrap(), Ok(S3Reply::Released(vec![])));

        // Present, ETag "1".
        let before = pigeonhole_meta::encode_state(&s);
        assert_eq!(s.apply(put("2", Some(Expect::Absent))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(s.apply(put("2", etag("0"))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(s.apply(del(etag("0"))).unwrap(), Err(S3Reject::PreconditionFailed));
        assert_eq!(pigeonhole_meta::encode_state(&s), before);
        assert_eq!(s.apply(put("2", etag("1"))).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.apply(del(etag("2"))).unwrap(), Ok(S3Reply::Released(vec![])));
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

        assert_eq!(s.apply(put("a", "1", Body::Inline(b"abc".to_vec()))).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.apply(put("b", "1", Body::Inline(b"xyz".to_vec()))).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.inline_bytes, 6);

        // Inline over chunks releases them; chunks over inline release nothing.
        assert_eq!(s.apply(put("c", "1", chunks(9))).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.apply(put("c", "2", Body::Inline(b"new".to_vec()))).unwrap(), Ok(S3Reply::Released(vec![9])));
        assert_eq!(s.apply(put("c", "3", chunks(10))).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.inline_bytes, 6);

        // Cooling: accepted for the same inline version, refused otherwise.
        assert_eq!(s.apply(cool("a", "1", 11)).unwrap(), Ok(S3Reply::Released(vec![])));
        assert_eq!(s.objects[&("b".into(), "a".into())].body, chunks(11));
        assert_eq!(s.inline_bytes, 3);
        assert_eq!(s.apply(cool("a", "1", 12)).unwrap(), Err(S3Reject::PreconditionFailed), "no longer inline");
        assert_eq!(s.apply(cool("b", "0", 12)).unwrap(), Err(S3Reject::PreconditionFailed), "rewritten meanwhile");
        assert_eq!(s.apply(cool("gone", "1", 12)).unwrap(), Err(S3Reject::NoSuchKey));

        // Delete drops the count; a replay (decode + rebuild) recounts it.
        assert_eq!(
            s.apply(S3Command::DeleteObject { bucket: "b".into(), key: "b".into(), expect: None }).unwrap(),
            Ok(S3Reply::Released(vec![]))
        );
        assert_eq!(s.inline_bytes, 0);
        s.apply(put("d", "1", Body::Inline(b"0123456789".to_vec()))).unwrap().unwrap();
        let replayed: S3State = pigeonhole_meta::decode_state(&pigeonhole_meta::encode_state(&s)).unwrap();
        assert_eq!(replayed.inline_bytes, 10);
    }

    fn create(s: &mut S3State, bucket: &str) {
        s.apply(S3Command::CreateBucket { name: bucket.into(), created_at: "t".into() }).unwrap().unwrap();
    }

    fn extents(chunks: &[ChunkId]) -> Vec<Extent> {
        chunks.iter().map(|&chunk| Extent { chunk, offset: 0, len: 4 }).collect()
    }

    fn upload(s: &mut S3State, id: &str, bucket: &str, parts: &[(i64, &str, &[ChunkId])]) {
        let create = S3Command::CreateUpload {
            upload_id: id.into(),
            bucket: bucket.into(),
            key: "k".into(),
            content_type: None,
            user_meta: vec![],
            tags: vec![],
            checksum_algorithm: None,
            initiated_at: "t".into(),
        };
        s.apply(create).unwrap().unwrap();
        for &(part_number, etag, chunks) in parts {
            let put = S3Command::PutPart {
                upload_id: id.into(),
                part_number,
                etag: etag.into(),
                size: 4 * chunks.len() as i64,
                extents: extents(chunks),
            };
            s.apply(put).unwrap().unwrap();
        }
    }

    fn complete(id: &str, parts: &[(i64, &str)]) -> S3Command {
        S3Command::CompleteUpload {
            upload_id: id.into(),
            parts: parts.iter().map(|&(n, e)| (n, e.to_string())).collect(),
            mtime: "t".into(),
            expect: None,
        }
    }

    const E1: &str = "00000000000000000000000000000001";
    const E2: &str = "00000000000000000000000000000002";
    const E3: &str = "00000000000000000000000000000003";

    /// CompleteMultipartUpload is decided by the parts as they are when it
    /// applies: listed ones must be there with their ETags, in ascending
    /// order; size and ETag are theirs; the parts left out are released,
    /// and a chunk two listed parts share gives back its extra ref.
    #[test]
    fn complete_checks_the_parts_it_assembles() {
        let mut s = S3State::default();
        create(&mut s, "b");
        upload(&mut s, "u", "b", &[(1, E1, &[10, 11]), (2, E2, &[11]), (3, E3, &[12])]);
        let before = pigeonhole_meta::encode_state(&s);
        assert_eq!(s.apply(complete("u", &[(2, E2), (1, E1)])).unwrap(), Err(S3Reject::InvalidPartOrder));
        assert_eq!(s.apply(complete("u", &[(1, E1), (1, E1)])).unwrap(), Err(S3Reject::InvalidPartOrder));
        assert_eq!(s.apply(complete("u", &[(1, E1), (2, E3)])).unwrap(), Err(S3Reject::InvalidPart(2)));
        assert_eq!(s.apply(complete("u", &[(1, E1), (4, E1)])).unwrap(), Err(S3Reject::InvalidPart(4)));
        assert_eq!(s.apply(complete("u", &[])).unwrap(), Err(S3Reject::InvalidPart(0)));
        assert_eq!(s.apply(complete("gone", &[(1, E1)])).unwrap(), Err(S3Reject::NoSuchUpload));
        assert_eq!(pigeonhole_meta::encode_state(&s), before, "refusals change nothing");

        // Part 3 left out: its chunk is released; 11 is in parts 1 and 2,
        // the object keeps one ref on it.
        assert_eq!(s.apply(complete("u", &[(1, E1), (2, E2)])).unwrap(), Ok(S3Reply::Released(vec![11, 12])));
        let o = &s.objects[&("b".to_string(), "k".to_string())];
        assert_eq!(o.size, 12);
        assert_eq!(Some(o.etag.clone()), multipart_etag([E1, E2]));
        assert!(o.etag.ends_with("-2"));
        assert_eq!(o.body, Body::Chunks(extents(&[10, 11, 11])));
        assert!(s.uploads.is_empty());
    }

    #[test]
    fn deleting_a_bucket_releases_its_uploads() {
        let mut s = S3State::default();
        create(&mut s, "b");
        create(&mut s, "other");
        upload(&mut s, "u1", "b", &[(1, E1, &[1, 2]), (2, E2, &[2])]);
        upload(&mut s, "u2", "other", &[(1, E1, &[3])]);
        let got = s.apply(S3Command::DeleteBucket { name: "b".into() }).unwrap();
        assert_eq!(got, Ok(S3Reply::Released(vec![1, 2, 2])), "one ref per part");
        assert_eq!(s.uploads.keys().collect::<Vec<_>>(), ["u2"]);
    }

    #[test]
    fn delete_objects_answers_each_key() {
        let mut s = S3State::default();
        create(&mut s, "b");
        let put = |key: &str, chunk: ChunkId| S3Command::PutObject {
            bucket: "b".into(),
            key: key.into(),
            etag: format!("e{chunk}"),
            size: 4,
            content_type: None,
            body: Body::Chunks(extents(&[chunk])),
            user_meta: vec![],
            tags: vec![],
            checksums_json: "{}".into(),
            mtime: "t".into(),
            expect: None,
        };
        for (key, chunk) in [("a", 1), ("b", 2), ("c", 3)] {
            s.apply(put(key, chunk)).unwrap().unwrap();
        }
        let objects = vec![
            ("a".to_string(), None),
            ("b".to_string(), Some(Expect::ETag("wrong".into()))),
            ("c".to_string(), Some(Expect::ETag("e3".into()))),
            ("missing".to_string(), None),
            ("missing too".to_string(), Some(Expect::ETag("e9".into()))),
        ];
        let got = s.apply(S3Command::DeleteObjects { bucket: "b".into(), objects: objects.clone() }).unwrap();
        let refused =
            vec![("b".to_string(), S3Reject::PreconditionFailed), ("missing too".to_string(), S3Reject::NoSuchKey)];
        assert_eq!(got, Ok(S3Reply::Deleted { released: vec![1, 3], refused }));
        assert_eq!(s.objects.len(), 1);
        let nobucket = S3Command::DeleteObjects { bucket: "nope".into(), objects };
        assert_eq!(s.apply(nobucket).unwrap(), Err(S3Reject::BucketNotFound));
    }
}
