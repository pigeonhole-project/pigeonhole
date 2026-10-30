//! S3 API implementation on top of gateway index + ChunkStore (via s3s).

use crate::checksums::{self, copy_checksums, IncomingChecksums};
use crate::commands::{Body, Expect, S3Reject, S3State};
use crate::cool::{CoolPolicy, CoolReason};
use crate::index::{
    parse_rfc3339, slice_extents, unique_chunk_ids, DeleteBucketResult, Index, ListPage, ObjectMeta, ObjectView, Rejected,
    SourceSnapshot,
};
use pigeonhole_meta::{Outcome, Prepared, Proposal};
use pigeonhole_chunk_store::Extent;
use pigeonhole_chunk_store::{ChunkStore, IngestOptions, Ingested, PendingChunks};
use pigeonhole_codec::{self as chunker, ByteBudget, ChunkCodec};
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use md5::Digest;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3, S3Request, S3Response, S3Result};
use std::ops::Not;
use std::sync::Arc;
use tracing::info;


/// GET re-reads the object this many times if its chunks are reclaimed underneath it.
const GET_ATTEMPTS: u32 = 5;

/// What one [`S3Gateway::cool_pass`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CoolStats {
    /// Moved out for not being rewritten for `cool_after`.
    pub by_age: u64,
    /// Moved out to bring the inline bytes under budget.
    pub by_budget: u64,
    /// Changed meanwhile: left as they are.
    pub rejected: u64,
}

/// What the S3 gateway takes from the configuration.
#[derive(Clone, Debug)]
pub struct S3Options {
    /// Answered to GetBucketLocation.
    pub region: String,
    /// Max on-wire chunk size in bytes.
    pub chunk_size: usize,
    pub chunk_codec: ChunkCodec,
    pub block_size: usize,
    /// Process-wide ingest buffer budget (shared across PUTs).
    pub ingest_budget: Option<ByteBudget>,
    /// Objects up to this size are stored inline in the metadata
    /// (docs/inline-objects.md); `None`: never.
    pub inline_max: Option<usize>,
}

impl S3Options {
    /// The defaults, for in-process tests.
    pub fn for_test() -> Self {
        Self {
            region: "us-east-1".into(),
            chunk_size: chunker::DEFAULT_CHUNK_SIZE,
            chunk_codec: ChunkCodec::Zstd,
            block_size: chunker::DEFAULT_BLOCK_SIZE,
            ingest_budget: Some(ByteBudget::new(chunker::DEFAULT_INGEST_MEMORY_BUDGET)),
            inline_max: None,
        }
    }
}

#[derive(Clone)]
pub struct S3Gateway {
    pub opts: S3Options,
    pub index: Index,
    pub store: Arc<ChunkStore>,
}

impl S3Gateway {
    pub fn new(opts: S3Options, index: Index, store: Arc<ChunkStore>) -> Self {
        Self { opts, index, store }
    }

    fn ingest_options(&self) -> IngestOptions {
        let mut opts = IngestOptions::new(self.opts.chunk_size, self.opts.chunk_codec);
        opts.block_size = self.opts.block_size;
        opts.memory_budget = self.opts.ingest_budget.clone();
        opts
    }

    async fn release_ids(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        if let Err(e) = self.store.release(ids).await {
            tracing::warn!(error = %e, "chunk release failed");
        }
    }

    /// Release chunks nothing points at any more, after the answer: they go
    /// into whatever entry is next. A crash before leaves their `refs` one
    /// too high (space, never data).
    fn release_later(&self, ids: Vec<i64>) {
        if ids.is_empty() {
            return;
        }
        let store = self.store.clone();
        // After the answer, under the request's span still.
        tokio::spawn(tracing::Instrument::in_current_span(async move {
            if let Err(e) = store.release(&ids).await {
                tracing::warn!(error = %e, "chunk release failed");
            }
        }));
    }

    /// Commit an ingest's chunks and `command` as one entry; the outcome of
    /// `command`. The chunks are committed even if `command` is rejected: the
    /// caller releases them then.
    async fn commit_with(
        &self,
        chunks: pigeonhole_chunk_store::PendingChunks,
        command: Prepared<S3State>,
    ) -> S3Result<Outcome<S3State>> {
        let mut proposal = Proposal::new();
        let slot = proposal.push(command);
        let mut outcomes =
            self.store.commit_along(chunks, self.index.meta(), proposal).await.map_err(Self::map_err)?;
        outcomes.take(slot).map_err(Self::map_err)
    }

    /// One pass of the cooler (docs/inline-objects.md § The cooler): move
    /// the inline objects `policy` picks out to chunks, each as its chunk and
    /// a `Cool` in one entry. An object rewritten or deleted meanwhile keeps
    /// its new state; the chunk made for it is released.
    pub async fn cool_pass(&self, policy: &CoolPolicy) -> anyhow::Result<CoolStats> {
        let mut stats = CoolStats::default();
        for c in self.index.cool_candidates(chrono::Utc::now(), policy) {
            let data = Bytes::from(c.bytes.clone());
            let ingested = self.store.ingest(futures::stream::iter([Ok(data)]), Some(self.ingest_options())).await?;
            let cool = self.index.prepare_cool(&c, &ingested.extents);
            let outcome = self.commit_with(ingested.chunks, cool).await.map_err(|e| anyhow::anyhow!("cool {}/{}: {e:?}", c.bucket, c.key))?;
            match outcome {
                Ok(_) => match c.reason {
                    CoolReason::Age => stats.by_age += 1,
                    CoolReason::Budget => stats.by_budget += 1,
                },
                Err(_) => {
                    self.release_ids(&unique_chunk_ids(&ingested.extents)).await;
                    stats.rejected += 1;
                }
            }
        }
        Ok(stats)
    }

    /// Commit `command` alone (nothing to upload); its outcome.
    async fn commit(&self, command: Prepared<S3State>) -> S3Result<Outcome<S3State>> {
        let mut proposal = Proposal::new();
        let slot = proposal.push(command);
        let mut outcomes = self.index.meta().propose_all(proposal).await.map_err(Self::map_err)?;
        outcomes.take(slot).map_err(Self::map_err)
    }

    fn map_err(e: impl std::fmt::Display) -> s3s::S3Error {
        s3_error!(InternalError, "{}", e)
    }

    /// A write's error: a refused condition as S3 answers it (412 / 404),
    /// anything else as [`Self::map_err`].
    fn write_err(e: anyhow::Error) -> s3s::S3Error {
        match e.downcast_ref::<Rejected>() {
            Some(Rejected(reject)) => Self::refused(*reject),
            None => Self::map_err(format!("{e:#}")),
        }
    }

    fn refused(reject: S3Reject) -> s3s::S3Error {
        match reject {
            S3Reject::PreconditionFailed => s3_error!(PreconditionFailed),
            S3Reject::NoSuchKey => s3_error!(NoSuchKey),
            S3Reject::BucketNotFound => s3_error!(NoSuchBucket),
            other => Self::map_err(format!("rejected: {other:?}")),
        }
    }

    /// Snapshot a copy source and take a ref on the chunks `select` picks from it.
    ///
    /// The ref is taken before the caller publishes anything that points at those
    /// chunks. If the source is deleted or overwritten between the snapshot and
    /// `retain`, its chunks may already be unreferenced (`ChunkGone`); re-read and
    /// retry, and report `NoSuchKey` only when the source is really gone.
    async fn pin_copy_source<F>(
        &self,
        bucket: &str,
        key: &str,
        select: F,
    ) -> S3Result<(SourceSnapshot, Vec<Extent>)>
    where
        F: Fn(&SourceSnapshot) -> S3Result<Vec<Extent>>,
    {
        const ATTEMPTS: usize = 3;
        for _ in 0..ATTEMPTS {
            let src = self
                .index
                .get_copy_source(bucket, key)
                .await
                .map_err(Self::map_err)?
                .ok_or_else(|| s3_error!(NoSuchKey))?;
            let extents = select(&src)?;
            let ids = unique_chunk_ids(&extents);
            if ids.is_empty() {
                return Ok((src, extents));
            }
            match self.store.retain(&ids).await {
                Ok(()) => return Ok((src, extents)),
                Err(e) if pigeonhole_chunk_store::is_chunk_gone(&e) => continue,
                Err(e) => return Err(Self::map_err(e)),
            }
        }
        Err(s3_error!(NoSuchKey, "source changed during copy"))
    }

    fn etag_of(md5: &[u8; 16]) -> String {
        hex::encode(md5)
    }

    /// `NoSuchBucket` unless `bucket` exists.
    async fn require_bucket(&self, bucket: &str) -> S3Result<()> {
        if self.index.bucket_exists(bucket).await.map_err(Self::map_err)? {
            Ok(())
        } else {
            Err(s3_error!(NoSuchBucket))
        }
    }

    /// Store `data` as new chunks (not committed yet).
    async fn ingest_bytes(&self, data: Bytes) -> S3Result<Ingested> {
        self.store.ingest(futures::stream::iter([Ok(data)]), Some(self.ingest_options())).await.map_err(Self::map_err)
    }

    /// Commit `command` with the chunks of `extents` (`chunks`: those not
    /// committed yet). The chunks the command replaced are released after
    /// the answer; if it is refused, nothing points at `extents`: they are
    /// released now and the refusal is answered as `refuse` says.
    async fn commit_new(
        &self,
        chunks: Option<PendingChunks>,
        extents: &[Extent],
        command: Prepared<S3State>,
        refuse: fn(S3Reject) -> s3s::S3Error,
    ) -> S3Result<()> {
        let outcome = match chunks {
            Some(chunks) => self.commit_with(chunks, command).await?,
            None => self.commit(command).await?,
        };
        match outcome {
            Ok(old) => {
                self.release_later(old);
                Ok(())
            }
            Err(reject) => {
                self.release_ids(&unique_chunk_ids(extents)).await;
                Err(refuse(reject))
            }
        }
    }

    /// A part's write refused: its upload is gone, or something unexpected.
    fn part_refused(reject: S3Reject) -> s3s::S3Error {
        match reject {
            S3Reject::NoSuchUpload => s3_error!(NoSuchUpload),
            other => Self::map_err(format!("part rejected: {other:?}")),
        }
    }

    /// Read a PUT's body: up to `inline_max` (and the declared length fits)
    /// it stays in memory to be inlined (docs/inline-objects.md), else it is
    /// ingested as chunks.
    async fn take_body<S>(&self, mut stream: S, content_length: Option<i64>) -> S3Result<TakenBody>
    where
        S: futures::Stream<Item = anyhow::Result<Bytes>> + Unpin,
    {
        let max = self.opts.inline_max.map(|m| m as u64).filter(|&max| content_length.is_none_or(|n| (n as u64) <= max));
        let mut head = Vec::new();
        if let Some(max) = max {
            if let Some(data) = read_at_most(&mut stream, max, &mut head).await.map_err(Self::map_err)? {
                let md5: [u8; 16] = md5::Md5::digest(&data).into();
                let size = data.len() as i64;
                return Ok(TakenBody { body: Body::Inline(data), size, md5, chunks: None });
            }
        }
        let stream = futures::stream::iter(head.into_iter().map(Ok)).chain(stream);
        let ingested = self.store.ingest(stream, Some(self.ingest_options())).await.map_err(Self::map_err)?;
        Ok(TakenBody {
            body: Body::Chunks(ingested.extents),
            size: ingested.size,
            md5: ingested.md5,
            chunks: Some(ingested.chunks),
        })
    }

    /// One version of an object and the bytes of `range` of it. If its
    /// chunks went away meanwhile (overwritten or deleted, then reclaimed),
    /// that version is gone: look the key up again.
    async fn read_object(&self, bucket: &str, key: &str, range: Option<&Range>) -> S3Result<ObjectRead> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let view = self
                .index
                .get_object_view(bucket, key)
                .await
                .map_err(Self::map_err)?
                .ok_or_else(|| s3_error!(NoSuchKey))?;
            let (start, length) = resolve_range(range, view.meta.size as u64)?;
            let span = start..start + length;
            let data = match &view.body {
                _ if length == 0 => Bytes::new(),
                Body::Inline(bytes) => Bytes::copy_from_slice(&bytes[span.start as usize..span.end as usize]),
                Body::Chunks(extents) => match self.store.read(extents, Some(span)).await {
                    Ok(data) => data,
                    Err(e) if pigeonhole_chunk_store::is_chunk_gone(&e) && attempt < GET_ATTEMPTS => continue,
                    Err(e) => return Err(Self::map_err(e)),
                },
            };
            return Ok(ObjectRead { view, start, length, data });
        }
    }

    /// The etag of a copied part: the source's if the part is all of a
    /// single-part source, else the MD5 of its bytes.
    async fn part_etag(&self, src: &SourceSnapshot, extents: &[Extent]) -> S3Result<String> {
        let length = extents.iter().map(|e| e.len).sum::<i64>();
        if length == src.size && !src.etag.contains('-') {
            return Ok(src.etag.clone());
        }
        let data = self.store.read(extents, None).await.map_err(Self::map_err)?;
        Ok(format!("{:x}", md5::Md5::digest(&data)))
    }
}

/// A PUT's body as it is to be stored.
struct TakenBody {
    body: Body,
    size: i64,
    md5: [u8; 16],
    /// Chunks to commit along (none for an inline body).
    chunks: Option<PendingChunks>,
}

/// What [`S3Gateway::read_object`] read.
struct ObjectRead {
    view: ObjectView,
    start: u64,
    length: u64,
    data: Bytes,
}

/// Read `stream` into `head` until it ends (the whole of it: `Some`) or
/// passes `max` bytes (`None`; `head` holds what was read).
async fn read_at_most<S>(stream: &mut S, max: u64, head: &mut Vec<Bytes>) -> anyhow::Result<Option<Vec<u8>>>
where
    S: futures::Stream<Item = anyhow::Result<Bytes>> + Unpin,
{
    let mut read = 0u64;
    while let Some(b) = stream.next().await {
        let b = b?;
        read += b.len() as u64;
        head.push(b);
        if read > max {
            return Ok(None);
        }
    }
    Ok(Some(head.concat()))
}

/// The first byte and the length a GET's `Range` asks of `total` bytes
/// (all of them without one).
fn resolve_range(range: Option<&Range>, total: u64) -> S3Result<(u64, u64)> {
    let (start, end_inclusive) = match range {
        None if total == 0 => return Ok((0, 0)),
        None => (0, total - 1),
        Some(Range::Int { first, last }) => {
            let last = last.unwrap_or(u64::MAX).min(total.saturating_sub(1));
            if *first >= total || *first > last {
                return Err(s3_error!(InvalidRange));
            }
            (*first, last)
        }
        Some(Range::Suffix { length }) => {
            if *length == 0 || total == 0 {
                return Err(s3_error!(InvalidRange));
            }
            (total.saturating_sub(*length), total - 1)
        }
    };
    Ok((start, end_inclusive - start + 1))
}

/// The bucket and key of a copy's source (an access point is not supported).
fn copy_source_of(source: CopySource) -> S3Result<(String, String)> {
    match source {
        CopySource::Bucket { bucket, key, .. } => Ok((bucket.to_string(), key.to_string())),
        _ => Err(s3_error!(NotImplemented)),
    }
}

/// A request's body as a stream (empty without one).
fn body_stream(body: Option<StreamingBlob>) -> impl futures::Stream<Item = anyhow::Result<Bytes>> + Unpin {
    match body {
        Some(body) => body.map(|r| r.map_err(|e| anyhow::anyhow!(e))).left_stream(),
        None => futures::stream::empty().right_stream(),
    }
}

/// The answer to a CopyObject that wrote `dst`.
fn copy_object_output(dst: &ObjectMeta) -> CopyObjectOutput {
    CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: Some(etag_hex(&dst.etag)),
            last_modified: Some(ts(&dst.mtime)),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The answer to an UploadPartCopy that wrote a part with `etag`.
fn copy_part_output(etag: &str) -> UploadPartCopyOutput {
    UploadPartCopyOutput {
        copy_part_result: Some(CopyPartResult {
            e_tag: Some(etag_hex(etag)),
            last_modified: Some(Timestamp::from(std::time::SystemTime::now())),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn ts(rfc3339: &str) -> Timestamp {
    Timestamp::from(std::time::SystemTime::from(parse_rfc3339(rfc3339)))
}

/// The [`Expect`] of a write's `If-Match` / `If-None-Match`
/// (docs/conditional-writes.md): an ETag or `*` in either, not both at once.
fn expect_of(if_match: Option<IfMatch>, if_none_match: Option<IfNoneMatch>) -> S3Result<Option<Expect>> {
    Ok(match (if_match, if_none_match) {
        (None, None) => None,
        (Some(ETagCondition::ETag(e)), None) => Some(Expect::ETag(e.value().to_string())),
        (Some(ETagCondition::Any), None) => Some(Expect::Present),
        (None, Some(ETagCondition::Any)) => Some(Expect::Absent),
        (None, Some(ETagCondition::ETag(e))) => Some(Expect::NotETag(e.value().to_string())),
        (Some(_), Some(_)) => return Err(s3_error!(InvalidArgument, "If-Match and If-None-Match together")),
    })
}

fn etag_hex(hex: &str) -> ETag {
    ETag::Strong(hex.to_string())
}

fn clamp_max_keys(requested: Option<i32>) -> (i64, i32) {
    let req = i64::from(requested.unwrap_or(1000));
    let clamped = req.clamp(1, 1000);
    (clamped, clamped as i32)
}

/// Parse Content-MD5 header: missing → None; empty/malformed → InvalidDigest; ok → digest.
fn parse_content_md5_header(expected: Option<&str>) -> S3Result<Option<[u8; 16]>> {
    let Some(exp) = expected else {
        return Ok(None);
    };
    // Empty header is invalid (AWS InvalidDigest), not "absent".
    if exp.is_empty() {
        return Err(s3_error!(
            InvalidDigest,
            "The Content-MD5 you specified is not valid."
        ));
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(exp)
        .map_err(|_| {
            s3_error!(
                InvalidDigest,
                "The Content-MD5 you specified is not valid."
            )
        })?;
    if decoded.len() != 16 {
        return Err(s3_error!(
            InvalidDigest,
            "The Content-MD5 you specified is not valid."
        ));
    }
    let mut dig = [0u8; 16];
    dig.copy_from_slice(&decoded);
    Ok(Some(dig))
}

fn verify_content_md5_digest(expected: [u8; 16], md5: &[u8; 16]) -> S3Result<()> {
    if expected != *md5 {
        return Err(s3_error!(
            BadDigest,
            "The Content-MD5 you specified did not match what we received."
        ));
    }
    Ok(())
}

fn percent_decode_component(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h = hex_nibble(bytes[i + 1]);
            let l = hex_nibble(bytes[i + 2]);
            if let (Some(h), Some(l)) = (h, l) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Parse `Key=Value&Key2=Value2` tagging header / validate TagSet.
fn parse_tagging_header(header: &str) -> S3Result<Vec<(String, String)>> {
    if header.is_empty() {
        return Ok(Vec::new());
    }
    let mut tags = Vec::new();
    for pair in header.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        tags.push((percent_decode_component(k), percent_decode_component(v)));
    }
    validate_tag_set(&tags)?;
    Ok(tags)
}

fn tags_from_tagging(tagging: &Tagging) -> S3Result<Vec<(String, String)>> {
    let mut tags = Vec::new();
    for t in &tagging.tag_set {
        let k = t.key.clone().unwrap_or_default();
        let v = t.value.clone().unwrap_or_default();
        tags.push((k, v));
    }
    validate_tag_set(&tags)?;
    Ok(tags)
}

fn validate_tag_set(tags: &[(String, String)]) -> S3Result<()> {
    if tags.len() > 10 {
        return Err(s3_error!(InvalidTag, "Object tags cannot exceed 10"));
    }
    let mut seen = std::collections::HashSet::new();
    for (k, v) in tags {
        if k.is_empty() || k.len() > 128 || v.len() > 256 {
            return Err(s3_error!(InvalidTag, "Invalid tag key or value"));
        }
        if !seen.insert(k.as_str()) {
            return Err(s3_error!(InvalidTag, "Duplicate tag key"));
        }
    }
    Ok(())
}

fn parse_copy_source_range(range: &str, total: u64) -> S3Result<(u64, u64)> {
    let s = range
        .strip_prefix("bytes=")
        .ok_or_else(|| s3_error!(InvalidArgument, "Invalid copy source range"))?;
    let (a, b) = s
        .split_once('-')
        .ok_or_else(|| s3_error!(InvalidArgument, "Invalid copy source range"))?;
    let first: u64 = a
        .parse()
        .map_err(|_| s3_error!(InvalidArgument, "Invalid copy source range"))?;
    let last: u64 = b
        .parse()
        .map_err(|_| s3_error!(InvalidArgument, "Invalid copy source range"))?;
    if total == 0 {
        return Err(s3_error!(InvalidArgument, "Invalid copy source range"));
    }
    if first > last || first >= total {
        return Err(s3_error!(InvalidArgument, "Invalid copy source range"));
    }
    let last = last.min(total - 1);
    Ok((first, last - first + 1))
}

#[async_trait]
impl S3 for S3Gateway {
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
                display_name: Some("pigeonhole".into()),
                id: Some("pigeonhole".into()),
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
            .create_bucket(&name)
            .await
            .map_err(Self::map_err)?;
        if !created && self.index.bucket_exists(&name).await.map_err(Self::map_err)? {
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
        self.require_bucket(&req.input.bucket).await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.require_bucket(&req.input.bucket).await?;
        // us-east-1 → null LocationConstraint (S3 convention).
        let location_constraint = if self.opts.region == "us-east-1" {
            None
        } else {
            Some(BucketLocationConstraint::from(self.opts.region.clone()))
        };
        Ok(S3Response::new(GetBucketLocationOutput {
            location_constraint,
        }))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket).await?;

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
        let ListPage { objects, prefixes: common, truncated, next } = self
            .index
            .list_objects(
                &input.bucket,
                prefix,
                input.delimiter.as_deref(),
                max_keys,
                input.marker.as_deref(),
            )
            .await
            .map_err(Self::map_err)?;

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

        Ok(S3Response::new(ListObjectsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            marker: input.marker,
            max_keys: Some(max_keys_out),
            is_truncated: Some(truncated),
            contents: contents.is_empty().not().then_some(contents),
            common_prefixes: common_prefixes.is_empty().not().then_some(common_prefixes),
            next_marker: next,
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        self.require_bucket(&input.bucket).await?;

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
        let start_after = input
            .continuation_token
            .as_deref()
            .or(input.start_after.as_deref());

        let ListPage { objects, prefixes: common, truncated, next } = self
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
            max_keys: Some(max_keys_out),
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

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        // Non-versioned store: expose current objects as a single version with
        // VersionId "null" so clients (s3-tests nuke_bucket) can empty buckets.
        let input = req.input;
        self.require_bucket(&input.bucket).await?;

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
        let ListPage { objects, prefixes: common, truncated, next } = self
            .index
            .list_objects(
                &input.bucket,
                prefix,
                input.delimiter.as_deref(),
                max_keys,
                input.key_marker.as_deref(),
            )
            .await
            .map_err(Self::map_err)?;

        let owner = Owner {
            display_name: Some("pigeonhole".into()),
            id: Some("pigeonhole".into()),
        };
        let versions = objects
            .into_iter()
            .map(|o| ObjectVersion {
                key: Some(o.key),
                version_id: Some("null".into()),
                is_latest: Some(true),
                e_tag: Some(etag_hex(&o.etag)),
                size: Some(o.size),
                last_modified: Some(ts(&o.mtime)),
                storage_class: Some(ObjectVersionStorageClass::from_static(
                    ObjectVersionStorageClass::STANDARD,
                )),
                owner: Some(owner.clone()),
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

        let (next_key_marker, next_version_id_marker) = if truncated {
            (
                next.clone(),
                next.as_ref().map(|_| "null".into()),
            )
        } else {
            (None, None)
        };

        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_marker: input.key_marker,
            version_id_marker: input.version_id_marker,
            max_keys: Some(max_keys_out),
            is_truncated: Some(truncated),
            versions: versions.is_empty().not().then_some(versions),
            common_prefixes: common_prefixes.is_empty().not().then_some(common_prefixes),
            next_key_marker,
            next_version_id_marker,
            ..Default::default()
        }))
    }


    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let trailing_headers = req.trailing_headers;
        let mut input = req.input;
        self.require_bucket(&input.bucket).await?;

        // Refuse a write bound to fail before reading its body; the command
        // decides again when it applies.
        let expect = expect_of(input.if_match.take(), input.if_none_match.take())?;
        self.index.check_expect(&input.bucket, &input.key, expect.as_ref()).map_err(Self::write_err)?;

        let expected_md5 = parse_content_md5_header(input.content_md5.as_deref())?;
        let tags = match input.tagging.as_deref() {
            Some(h) => parse_tagging_header(h)?,
            None => Vec::new(),
        };
        let mut expected = Checksum::default();
        copy_checksums!(input => expected);
        let checksums = IncomingChecksums::new(expected, &[input.checksum_algorithm.as_ref().map(|a| a.as_str())])?;

        let stream = checksums.wrap(body_stream(input.body.take()));
        let TakenBody { body, size, md5, chunks } = self.take_body(stream, input.content_length).await?;
        if let Some(exp) = expected_md5 {
            verify_content_md5_digest(exp, &md5)?;
        }
        let computed = checksums.verify(trailing_headers)?;

        let user_meta: Vec<(String, String)> = input.metadata.unwrap_or_default().into_iter().collect();
        let checksums_json = computed.as_ref().map_or_else(|| "{}".to_string(), checksums::to_json);
        let etag = Self::etag_of(&md5);
        let (extents, inline) = (body.extents().to_vec(), matches!(body, Body::Inline(_)));
        // The chunks (if any), the object and its tags: one entry.
        let put = self.index.prepare_put_object(
            &input.bucket,
            &input.key,
            &etag,
            size,
            input.content_type.as_deref(),
            body,
            &user_meta,
            &tags,
            &checksums_json,
            expect,
        );
        self.commit_new(chunks, &extents, put, Self::refused).await?;
        info!(bucket = %input.bucket, key = %input.key, size, extents = extents.len(), inline, "PutObject ok");

        let mut output = PutObjectOutput {
            e_tag: Some(etag_hex(&etag)),
            ..Default::default()
        };
        if let Some(c) = computed {
            copy_checksums!(c => output);
        }
        Ok(S3Response::new(output))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let ObjectRead { view, start, length, data } =
            self.read_object(&input.bucket, &input.key, input.range.as_ref()).await?;
        let meta = view.meta;
        let total = meta.size as u64;
        // A range is never empty: `resolve_range` refuses one.
        let content_range = input.range.as_ref().map(|_| format!("bytes {start}-{}/{total}", start + length - 1));
        let metadata = view.user_meta.is_empty().not().then(|| view.user_meta.into_iter().collect());
        let mut output = GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(data)),
            content_length: Some(length as i64),
            content_range,
            e_tag: Some(etag_hex(&meta.etag)),
            content_type: meta.content_type,
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            tag_count: Some(view.tag_count as i32),
            ..Default::default()
        };
        // The stored checksums are of the whole object.
        let checksum_enabled = input.checksum_mode.as_ref().is_some_and(|m| m.as_str() == ChecksumMode::ENABLED);
        if checksum_enabled && length == total {
            let stored = checksums::from_json(&view.checksums_json);
            copy_checksums!(stored => output);
        }
        Ok(S3Response::new(output))
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
        let tag_count = self
            .index
            .get_object_tags(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .len() as i32;
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
            e_tag: Some(etag_hex(&meta.etag)),
            content_length: Some(meta.size),
            content_type: meta.content_type,
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            tag_count: Some(tag_count),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let expect = expect_of(input.if_match, None)?;
        if let Some(ids) = self
            .index
            .delete_object(&input.bucket, &input.key, expect)
            .await
            .map_err(Self::write_err)?
        {
            self.release_later(ids);
        }
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for obj in input.delete.objects {
            let key = obj.key;
            match self.index.delete_object(&input.bucket, &key, None).await {
                Ok(Some(ids)) => {
                    self.release_later(ids);
                    deleted.push(DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    });
                }
                Ok(None) => {
                    deleted.push(DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    });
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
        let expect = expect_of(input.if_match, input.if_none_match)?;
        self.index.check_expect(&input.bucket, &input.key, expect.as_ref()).map_err(Self::write_err)?;
        let (src_bucket, src_key) = copy_source_of(input.copy_source)?;
        if self.index.get_object(&src_bucket, &src_key).await.map_err(Self::map_err)?.is_none() {
            return Err(s3_error!(NoSuchKey));
        }
        self.require_bucket(&input.bucket).await?;

        let copy_source_meta =
            input.metadata_directive.as_ref().map(|d| d.as_str()) != Some(MetadataDirective::REPLACE);
        let (content_type, user_meta): (_, Vec<(String, String)>) = if copy_source_meta {
            (None, vec![])
        } else {
            (input.content_type.as_deref(), input.metadata.unwrap_or_default().into_iter().collect())
        };

        let (src, extents) =
            self.pin_copy_source(&src_bucket, &src_key, |src| Ok(src.body.extents().to_vec())).await?;
        // An inline source too large for inlining now (it is off, or the
        // threshold was lowered) is stored at the destination as new chunks.
        let rechunk = match &src.body {
            Body::Inline(bytes) if self.opts.inline_max.is_none_or(|max| bytes.len() > max) => {
                Some(Bytes::copy_from_slice(bytes))
            }
            _ => None,
        };
        if let Some(data) = rechunk {
            let ingested = self.ingest_bytes(data).await?;
            let (put, dst) = self.index.prepare_copy_object(
                &src,
                Body::Chunks(ingested.extents.clone()),
                &input.bucket,
                &input.key,
                content_type,
                &user_meta,
                copy_source_meta,
                expect,
            );
            self.commit_new(Some(ingested.chunks), &ingested.extents, put, Self::refused).await?;
            return Ok(S3Response::new(copy_object_output(&dst)));
        }

        let copied = self
            .index
            .copy_object(&src, &input.bucket, &input.key, content_type, &user_meta, copy_source_meta, expect)
            .await;
        let dst = match copied {
            Ok((dst, old)) => {
                self.release_later(old);
                dst
            }
            Err(e) => {
                self.release_ids(&unique_chunk_ids(&extents)).await;
                return Err(Self::write_err(e));
            }
        };
        Ok(S3Response::new(copy_object_output(&dst)))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket).await?;
        let tags = match input.tagging.as_deref() {
            Some(h) => parse_tagging_header(h)?,
            None => Vec::new(),
        };
        let user_meta: Vec<(String, String)> = input
            .metadata
            .unwrap_or_default()
            .into_iter()
            .collect();
        let upload_id = uuid::Uuid::new_v4().to_string();
        self.index
            .create_multipart_upload(
                &upload_id,
                &input.bucket,
                &input.key,
                input.content_type.as_deref(),
                &user_meta,
                &tags,
                input.checksum_algorithm.as_ref().map(|a| a.as_str()),
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
        let trailing_headers = req.trailing_headers;
        let mut input = req.input;
        let upload_id = input.upload_id.clone();
        let part_number = i64::from(input.part_number);
        let expected_md5 = parse_content_md5_header(input.content_md5.as_deref())?;

        let upload = self
            .index
            .get_multipart_upload(&upload_id)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;

        let mut expected = Checksum::default();
        copy_checksums!(input => expected);
        let checksums = IncomingChecksums::new(
            expected,
            &[upload.checksum_algorithm.as_deref(), input.checksum_algorithm.as_ref().map(|a| a.as_str())],
        )?;

        let stream = checksums.wrap(body_stream(input.body.take()));
        let ingested = self.store.ingest(stream, Some(self.ingest_options())).await.map_err(Self::map_err)?;
        if let Some(exp) = expected_md5 {
            verify_content_md5_digest(exp, &ingested.md5)?;
        }
        let computed = checksums.verify(trailing_headers)?;

        let etag = Self::etag_of(&ingested.md5);
        let put = self.index.prepare_put_part(&upload_id, part_number, &etag, ingested.size, &ingested.extents);
        self.commit_new(Some(ingested.chunks), &ingested.extents, put, Self::part_refused).await?;

        let mut output = UploadPartOutput {
            e_tag: Some(etag_hex(&etag)),
            ..Default::default()
        };
        if let Some(c) = computed {
            copy_checksums!(c => output);
        }
        Ok(S3Response::new(output))
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;
        let upload = self
            .index
            .get_multipart_upload(&input.upload_id)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        if upload.bucket != input.bucket || upload.key != input.key {
            return Err(s3_error!(NoSuchUpload));
        }
        let (src_bucket, src_key) = copy_source_of(input.copy_source)?;
        let part_number = i64::from(input.part_number);
        // The first byte and the length of the part in a source of `size`.
        let span = |size: i64| match input.copy_source_range.as_deref() {
            None => Ok((0u64, size as u64)),
            Some(r) => parse_copy_source_range(r, size as u64),
        };

        // An inline source has no chunks to share: its bytes become the part.
        if let Some(src) = self.index.get_copy_source(&src_bucket, &src_key).await.map_err(Self::map_err)? {
            if let Body::Inline(bytes) = &src.body {
                let (start, length) = span(src.size)?;
                let data = Bytes::copy_from_slice(&bytes[start as usize..(start + length) as usize]);
                let ingested = self.ingest_bytes(data).await?;
                let etag = Self::etag_of(&ingested.md5);
                let put =
                    self.index.prepare_put_part(&input.upload_id, part_number, &etag, ingested.size, &ingested.extents);
                self.commit_new(Some(ingested.chunks), &ingested.extents, put, Self::part_refused).await?;
                return Ok(S3Response::new(copy_part_output(&etag)));
            }
        }

        let (src, extents) = self
            .pin_copy_source(&src_bucket, &src_key, |src| {
                let (start, length) = span(src.size)?;
                Ok(slice_extents(src.body.extents(), start, length))
            })
            .await?;
        let length = extents.iter().map(|e| e.len).sum::<i64>();
        let put = async {
            let etag = self.part_etag(&src, &extents).await?;
            self.index
                .put_multipart_part(&input.upload_id, part_number, &etag, length, &extents)
                .await
                .map_err(Self::map_err)
        };
        match put.await {
            Ok(old) => self.release_later(old),
            Err(e) => {
                self.release_ids(&unique_chunk_ids(&extents)).await;
                return Err(e);
            }
        }

        let part = self
            .index
            .get_multipart_part(&input.upload_id, part_number)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(InternalError, "part missing after copy"))?;
        Ok(S3Response::new(copy_part_output(&part.etag)))
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let input = req.input;
        if self
            .index
            .get_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .is_none()
        {
            return Err(s3_error!(NoSuchKey));
        }
        let tags = tags_from_tagging(&input.tagging)?;
        self.index
            .put_object_tags(&input.bucket, &input.key, &tags)
            .await
            .map_err(Self::map_err)?;
        Ok(S3Response::new(PutObjectTaggingOutput::default()))
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let input = req.input;
        if self
            .index
            .get_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .is_none()
        {
            return Err(s3_error!(NoSuchKey));
        }
        let tags = self
            .index
            .get_object_tags(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?;
        let tag_set = tags
            .into_iter()
            .map(|(k, v)| Tag {
                key: Some(k),
                value: Some(v),
            })
            .collect();
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            ..Default::default()
        }))
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let input = req.input;
        if self
            .index
            .get_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .is_none()
        {
            return Err(s3_error!(NoSuchKey));
        }
        self.index
            .delete_object_tags(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?;
        Ok(S3Response::new(DeleteObjectTaggingOutput::default()))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let upload = self
            .index
            .get_multipart_upload(&input.upload_id)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        if upload.bucket != input.bucket || upload.key != input.key {
            return Err(s3_error!(NoSuchUpload));
        }

        let (max_parts, max_parts_out) = clamp_max_keys(input.max_parts);
        let marker = input.part_number_marker.map(i64::from);
        let (parts, truncated, next) = self
            .index
            .list_parts(&input.upload_id, marker, max_parts)
            .await
            .map_err(Self::map_err)?;

        let parts = parts
            .into_iter()
            .map(|p| Part {
                part_number: Some(p.part_number as i32),
                e_tag: Some(etag_hex(&p.etag)),
                size: Some(p.size),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(upload.bucket),
            key: Some(upload.key),
            upload_id: Some(input.upload_id),
            part_number_marker: input.part_number_marker,
            max_parts: Some(max_parts_out),
            is_truncated: Some(truncated),
            next_part_number_marker: next.map(|n| n as i32),
            parts: parts.is_empty().not().then_some(parts),
            ..Default::default()
        }))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        self.require_bucket(&input.bucket).await?;
        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_uploads, max_uploads_out) = clamp_max_keys(input.max_uploads);
        let (uploads, truncated, next) = self
            .index
            .list_multipart_uploads(
                &input.bucket,
                prefix,
                input.key_marker.as_deref(),
                input.upload_id_marker.as_deref(),
                max_uploads,
            )
            .await
            .map_err(Self::map_err)?;

        let uploads = uploads
            .into_iter()
            .map(|u| MultipartUpload {
                key: Some(u.key),
                upload_id: Some(u.upload_id),
                initiated: Some(ts(&u.initiated_at)),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let (next_key, next_upload) = match next {
            Some((k, u)) => (Some(k), Some(u)),
            None => (None, None),
        };

        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: Some(max_uploads_out),
            is_truncated: Some(truncated),
            next_key_marker: next_key,
            next_upload_id_marker: next_upload,
            uploads: uploads.is_empty().not().then_some(uploads),
            ..Default::default()
        }))
    }


    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let expect = expect_of(input.if_match, input.if_none_match)?;
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

        let old = self
            .index
            .complete_multipart_upload(&upload, &part_numbers, &etag, total_size, expect)
            .await
            .map_err(Self::write_err)?;
        self.release_later(old);

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
            Some(ids) => {
                self.release_later(ids);
                Ok(S3Response::new(AbortMultipartUploadOutput::default()))
            }
        }
    }
}
