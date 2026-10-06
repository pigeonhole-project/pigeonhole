//! S3 API implementation on top of SQLite index + BlobStore (via s3s).

use s3gram_blob::{BlobStore, DeleteOutcome};
use s3gram_chunk::ChunkCodec;
use s3gram_core::{BackendId, BlobKey, Locator};
use s3gram_engine::config::Config;
use s3gram_engine::frame_cache::FrameCache;
use s3gram_engine::ingest::{
    decode_chunk_slice_async, ingest_stream_with_options, IngestOptions, UploadedChunk,
};
use s3gram_engine::read::read_chunk_range_cached;
use s3gram_index::{parse_rfc3339, DeleteBucketResult, Index, OrphanMsg};
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3, S3Request, S3Response, S3Result};
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

#[derive(Clone)]
pub struct S3gram {
    pub cfg: Config,
    pub index: Index,
    pub store: Arc<dyn BlobStore>,
    pub snapshot_gate: Arc<Mutex<()>>,
    /// L1 unpacked-frame cache (None when `[cache] enabled = false`).
    pub frame_cache: Option<Arc<FrameCache>>,
    /// Last exclusive end offset per object for sequential readahead detection.
    pub(crate) sequential_ends: Arc<Mutex<HashMap<(String, String), u64>>>,
}

impl S3gram {
    pub fn new(cfg: Config, index: Index, store: Arc<dyn BlobStore>) -> Self {
        let frame_cache = if cfg.cache.enabled {
            Some(Arc::new(FrameCache::new(
                cfg.cache.frame_memory_bytes,
                cfg.cache.readahead_frames,
            )))
        } else {
            None
        };
        Self {
            cfg,
            index,
            store,
            snapshot_gate: Arc::new(Mutex::new(())),
            frame_cache,
            sequential_ends: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn chat_id(&self) -> &str {
        &self.cfg.chat_id
    }

    fn ingest_options(&self) -> IngestOptions {
        let mut opts = IngestOptions::new(self.cfg.chunk_size, self.cfg.chunk_codec);
        opts.frame_size = self.cfg.frame_size;
        opts.memory_budget = self.cfg.ingest_budget.clone();
        opts
    }

    async fn cleanup_orphans(&self, orphans: Vec<OrphanMsg>) {
        let mut seen = std::collections::HashSet::new();
        for (_chat, message_id, file_id) in orphans {
            if !seen.insert(message_id) {
                continue;
            }
            self.invalidate_caches_for_file(&file_id).await;
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

    async fn invalidate_caches_for_file(&self, file_id: &str) {
        self.store.invalidate_blob(file_id).await;
        if let Some(fc) = &self.frame_cache {
            let key = blob_key_for_file(file_id);
            fc.invalidate_blob(&key).await;
        }
    }

    async fn warm_l1_frames(&self, chunks: &[UploadedChunk]) {
        let Some(fc) = &self.frame_cache else {
            return;
        };
        if !self.cfg.cache.write_through {
            return;
        }
        for c in chunks {
            if c.codec != ChunkCodec::Frames || c.frames.is_empty() {
                continue;
            }
            let Ok(stored) = self.store.get(&c.file_id).await else {
                continue;
            };
            let key = blob_key_for_file(&c.file_id);
            for fr in &c.frames {
                let soff = fr.stored_off as usize;
                let slen = fr.stored_len as usize;
                if soff + slen > stored.len() {
                    continue;
                }
                let slice = stored.slice(soff..soff + slen);
                let mut rec = fr.clone();
                rec.stored_off = 0;
                if let Ok(decoded) = s3gram_chunk::decode_frames_range(
                    slice.as_ref(),
                    &[rec],
                    0,
                    fr.logical_len as usize,
                ) {
                    fc.insert(key.clone(), fr.frame_no as u32, decoded).await;
                }
            }
        }
    }

    async fn note_sequential(&self, bucket: &str, key: &str, start: u64, end_excl: u64) -> bool {
        let mut map = self.sequential_ends.lock().await;
        let k = (bucket.to_string(), key.to_string());
        let readahead = match map.get(&k) {
            None => start == 0,
            Some(&prev) => start == prev || start == 0,
        };
        map.insert(k, end_excl);
        readahead
    }

    async fn queue_pending_deletes(&self, message_ids: Vec<i64>) {
        for message_id in message_ids {
            let _ = self
                .index
                .queue_tg_delete(self.chat_id(), message_id)
                .await;
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

fn verify_checksum_crc32(expected: Option<&str>, crc: u32) -> S3Result<()> {
    let Some(exp) = expected.filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let got = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
    if got != exp {
        return Err(s3_error!(BadDigest, "CRC32 checksum does not match"));
    }
    Ok(())
}

fn enable_expected_checksums(hasher: &mut s3s::checksum::ChecksumHasher, checksum: &Checksum) {
    if checksum.checksum_crc32.is_some() {
        hasher.crc32 = Some(Default::default());
    }
    if checksum.checksum_crc32c.is_some() {
        hasher.crc32c = Some(Default::default());
    }
    if checksum.checksum_sha1.is_some() {
        hasher.sha1 = Some(Default::default());
    }
    if checksum.checksum_sha256.is_some() {
        hasher.sha256 = Some(Default::default());
    }
    if checksum.checksum_crc64nvme.is_some() {
        hasher.crc64nvme = Some(Default::default());
    }
    if checksum.checksum_sha512.is_some() {
        hasher.sha512 = Some(Default::default());
    }
    if checksum.checksum_md5.is_some() {
        hasher.md5 = Some(Default::default());
    }
    if checksum.checksum_xxhash64.is_some() {
        hasher.xxhash64 = Some(Default::default());
    }
    if checksum.checksum_xxhash3.is_some() {
        hasher.xxhash3 = Some(Default::default());
    }
    if checksum.checksum_xxhash128.is_some() {
        hasher.xxhash128 = Some(Default::default());
    }
}

fn enable_checksum_algorithm(
    hasher: &mut s3s::checksum::ChecksumHasher,
    algorithm: &str,
) -> S3Result<()> {
    match algorithm {
        ChecksumAlgorithm::CRC32 => hasher.crc32 = Some(Default::default()),
        ChecksumAlgorithm::CRC32C => hasher.crc32c = Some(Default::default()),
        ChecksumAlgorithm::SHA1 => hasher.sha1 = Some(Default::default()),
        ChecksumAlgorithm::SHA256 => hasher.sha256 = Some(Default::default()),
        ChecksumAlgorithm::CRC64NVME => hasher.crc64nvme = Some(Default::default()),
        ChecksumAlgorithm::SHA512 => hasher.sha512 = Some(Default::default()),
        ChecksumAlgorithm::MD5 => hasher.md5 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH64 => hasher.xxhash64 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH3 => hasher.xxhash3 = Some(Default::default()),
        ChecksumAlgorithm::XXHASH128 => hasher.xxhash128 = Some(Default::default()),
        _ => return Err(s3_error!(NotImplemented, "Unsupported checksum algorithm")),
    }
    Ok(())
}

fn hasher_is_active(hasher: &s3s::checksum::ChecksumHasher) -> bool {
    hasher.crc32.is_some()
        || hasher.crc32c.is_some()
        || hasher.sha1.is_some()
        || hasher.sha256.is_some()
        || hasher.crc64nvme.is_some()
        || hasher.sha512.is_some()
        || hasher.md5.is_some()
        || hasher.xxhash64.is_some()
        || hasher.xxhash3.is_some()
        || hasher.xxhash128.is_some()
}

fn checksum_mismatch(actual: &Checksum, expected: &Checksum) -> Option<&'static str> {
    if expected.checksum_crc32.is_some() && actual.checksum_crc32 != expected.checksum_crc32 {
        return Some("checksum_crc32");
    }
    if expected.checksum_crc32c.is_some() && actual.checksum_crc32c != expected.checksum_crc32c {
        return Some("checksum_crc32c");
    }
    if expected.checksum_sha1.is_some() && actual.checksum_sha1 != expected.checksum_sha1 {
        return Some("checksum_sha1");
    }
    if expected.checksum_sha256.is_some() && actual.checksum_sha256 != expected.checksum_sha256 {
        return Some("checksum_sha256");
    }
    if expected.checksum_crc64nvme.is_some()
        && actual.checksum_crc64nvme != expected.checksum_crc64nvme
    {
        return Some("checksum_crc64nvme");
    }
    if expected.checksum_sha512.is_some() && actual.checksum_sha512 != expected.checksum_sha512 {
        return Some("checksum_sha512");
    }
    if expected.checksum_md5.is_some() && actual.checksum_md5 != expected.checksum_md5 {
        return Some("checksum_md5");
    }
    if expected.checksum_xxhash64.is_some()
        && actual.checksum_xxhash64 != expected.checksum_xxhash64
    {
        return Some("checksum_xxhash64");
    }
    if expected.checksum_xxhash3.is_some() && actual.checksum_xxhash3 != expected.checksum_xxhash3 {
        return Some("checksum_xxhash3");
    }
    if expected.checksum_xxhash128.is_some()
        && actual.checksum_xxhash128 != expected.checksum_xxhash128
    {
        return Some("checksum_xxhash128");
    }
    None
}

fn merge_trailer_checksums(expected: &mut Checksum, trailers: &http::HeaderMap) -> S3Result<()> {
    let take = |name: &str| -> S3Result<Option<String>> {
        match trailers.get(name) {
            Some(v) => Ok(Some(
                v.to_str()
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid trailer checksum"))?
                    .to_owned(),
            )),
            None => Ok(None),
        }
    };
    if let Some(v) = take("x-amz-checksum-crc32")? {
        expected.checksum_crc32 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-crc32c")? {
        expected.checksum_crc32c = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-sha1")? {
        expected.checksum_sha1 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-sha256")? {
        expected.checksum_sha256 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-crc64nvme")? {
        expected.checksum_crc64nvme = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-sha512")? {
        expected.checksum_sha512 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-md5")? {
        expected.checksum_md5 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-xxhash64")? {
        expected.checksum_xxhash64 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-xxhash3")? {
        expected.checksum_xxhash3 = Some(v);
    }
    if let Some(v) = take("x-amz-checksum-xxhash128")? {
        expected.checksum_xxhash128 = Some(v);
    }
    Ok(())
}

fn checksum_to_json(c: &Checksum) -> String {
    let mut map = serde_json::Map::new();
    let mut put = |k: &str, v: &Option<String>| {
        if let Some(val) = v {
            map.insert(k.to_string(), serde_json::Value::String(val.clone()));
        }
    };
    put("crc32", &c.checksum_crc32);
    put("crc32c", &c.checksum_crc32c);
    put("sha1", &c.checksum_sha1);
    put("sha256", &c.checksum_sha256);
    put("crc64nvme", &c.checksum_crc64nvme);
    put("sha512", &c.checksum_sha512);
    put("md5", &c.checksum_md5);
    put("xxhash64", &c.checksum_xxhash64);
    put("xxhash3", &c.checksum_xxhash3);
    put("xxhash128", &c.checksum_xxhash128);
    serde_json::Value::Object(map).to_string()
}

fn checksum_from_json(s: &str) -> Checksum {
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(s) else {
        return Checksum::default();
    };
    let get = |k: &str| -> Option<String> {
        map.get(k)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned())
    };
    Checksum {
        checksum_crc32: get("crc32"),
        checksum_crc32c: get("crc32c"),
        checksum_sha1: get("sha1"),
        checksum_sha256: get("sha256"),
        checksum_crc64nvme: get("crc64nvme"),
        checksum_sha512: get("sha512"),
        checksum_md5: get("md5"),
        checksum_xxhash64: get("xxhash64"),
        checksum_xxhash3: get("xxhash3"),
        checksum_xxhash128: get("xxhash128"),
        ..Default::default()
    }
}

fn apply_checksum_to_put(output: &mut PutObjectOutput, c: &Checksum) {
    output.checksum_crc32 = c.checksum_crc32.clone();
    output.checksum_crc32c = c.checksum_crc32c.clone();
    output.checksum_sha1 = c.checksum_sha1.clone();
    output.checksum_sha256 = c.checksum_sha256.clone();
    output.checksum_crc64nvme = c.checksum_crc64nvme.clone();
    output.checksum_sha512 = c.checksum_sha512.clone();
    output.checksum_md5 = c.checksum_md5.clone();
    output.checksum_xxhash64 = c.checksum_xxhash64.clone();
    output.checksum_xxhash3 = c.checksum_xxhash3.clone();
    output.checksum_xxhash128 = c.checksum_xxhash128.clone();
}

fn apply_checksum_to_get(output: &mut GetObjectOutput, c: &Checksum) {
    output.checksum_crc32 = c.checksum_crc32.clone();
    output.checksum_crc32c = c.checksum_crc32c.clone();
    output.checksum_sha1 = c.checksum_sha1.clone();
    output.checksum_sha256 = c.checksum_sha256.clone();
    output.checksum_crc64nvme = c.checksum_crc64nvme.clone();
    output.checksum_sha512 = c.checksum_sha512.clone();
    output.checksum_md5 = c.checksum_md5.clone();
    output.checksum_xxhash64 = c.checksum_xxhash64.clone();
    output.checksum_xxhash3 = c.checksum_xxhash3.clone();
    output.checksum_xxhash128 = c.checksum_xxhash128.clone();
}

fn apply_checksum_to_upload_part(output: &mut UploadPartOutput, c: &Checksum) {
    output.checksum_crc32 = c.checksum_crc32.clone();
    output.checksum_crc32c = c.checksum_crc32c.clone();
    output.checksum_sha1 = c.checksum_sha1.clone();
    output.checksum_sha256 = c.checksum_sha256.clone();
    output.checksum_crc64nvme = c.checksum_crc64nvme.clone();
    output.checksum_sha512 = c.checksum_sha512.clone();
    output.checksum_md5 = c.checksum_md5.clone();
    output.checksum_xxhash64 = c.checksum_xxhash64.clone();
    output.checksum_xxhash3 = c.checksum_xxhash3.clone();
    output.checksum_xxhash128 = c.checksum_xxhash128.clone();
}

struct ChunkSlice {
    file_id: String,
    codec: ChunkCodec,
    /// Full logical size of the Telegram chunk (decode bound).
    logical_size: usize,
    from: usize,
    to: usize,
}

fn plan_chunk_slices(
    chunks: &[s3gram_index::Chunk],
    mut start: u64,
    mut remaining: u64,
) -> Vec<ChunkSlice> {
    let mut plan = Vec::new();
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
        let local_start = start.saturating_sub(offset) as usize;
        let take = (chunk_size - local_start as u64).min(remaining) as usize;
        plan.push(ChunkSlice {
            file_id: chunk.file_id.clone(),
            codec: chunk.stored_codec(),
            logical_size: chunk.size.max(0) as usize,
            from: local_start,
            to: local_start + take,
        });
        remaining -= take as u64;
        start += take as u64;
        offset = chunk_end;
    }
    plan
}

/// If `[start, start+length)` covers whole Telegram chunks only, return them
/// renumbered as multipart part chunks. Misaligned ranges return `None`.
fn try_aligned_part_chunks(
    chunks: &[s3gram_index::Chunk],
    start: u64,
    length: u64,
) -> Option<Vec<UploadedChunk>> {
    if length == 0 {
        return Some(Vec::new());
    }
    let end = start.checked_add(length)?;
    let mut offset = 0u64;
    let mut out = Vec::new();
    let mut chunk_no: i64 = 0;
    for c in chunks {
        let csize = c.size as u64;
        let cend = offset + csize;
        if cend <= start {
            offset = cend;
            continue;
        }
        if offset >= end {
            break;
        }
        // Partial overlap with the requested range → not chunk-aligned.
        if offset < start || cend > end {
            return None;
        }
        out.push(UploadedChunk {
            part_no: chunk_no,
            file_id: c.file_id.clone(),
            message_id: c.message_id,
            logical_size: c.size,
            codec: c.stored_codec(),
            frames: Vec::new(),
            stored_crc32: None,
        });
        chunk_no += 1;
        offset = cend;
    }
    let covered: u64 = out.iter().map(|c| c.logical_size as u64).sum();
    if covered != length {
        return None;
    }
    Some(out)
}

fn blob_key_for_file(file_id: &str) -> BlobKey {
    let (backend, loc) = if file_id.starts_with("mem-") {
        (BackendId::memory(), Locator::memory(file_id, 0))
    } else {
        (BackendId::new("tg", "cached"), Locator::telegram(file_id, 0))
    };
    BlobKey::new(backend, loc)
}

fn stream_object_body(
    store: Arc<dyn BlobStore>,
    index: Index,
    chunks: Vec<s3gram_index::Chunk>,
    start: u64,
    length: u64,
    frame_cache: Option<Arc<FrameCache>>,
    readahead: bool,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static {
    let plan = plan_chunk_slices(&chunks, start, length);
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        for slice in plan {
            let frames = if slice.codec == ChunkCodec::Frames {
                match index.get_chunk_frames(&slice.file_id).await {
                    Ok(f) => f,
                    Err(e) => {
                        let _ = tx
                            .send(Err(std::io::Error::other(e.to_string())))
                            .await;
                        break;
                    }
                }
            } else {
                Vec::new()
            };
            let result = read_chunk_range_cached(
                store.clone(),
                &slice.file_id,
                slice.codec,
                &frames,
                slice.from,
                slice.to,
                slice.logical_size.max(1),
                frame_cache.clone(),
                readahead,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()));
            if tx.send(result).await.is_err() {
                break; // client disconnected
            }
        }
    });
    ReceiverStream::new(rx)
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

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        if !self
            .index
            .bucket_exists(&req.input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        // us-east-1 → null LocationConstraint (S3 convention).
        let location_constraint = if self.cfg.region == "us-east-1" {
            None
        } else {
            Some(BucketLocationConstraint::from(self.cfg.region.clone()))
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
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
        let (objects, common, truncated, next) = self
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
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
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
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let prefix = input.prefix.as_deref().unwrap_or("");
        let (max_keys, max_keys_out) = clamp_max_keys(input.max_keys);
        let (objects, common, truncated, next) = self
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
            display_name: Some("s3gram".into()),
            id: Some("s3gram".into()),
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
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }

        let expected_md5 = parse_content_md5_header(input.content_md5.as_deref())?;
        let tags = match input.tagging.as_deref() {
            Some(h) => parse_tagging_header(h)?,
            None => Vec::new(),
        };

        let mut expected_checksum = Checksum {
            checksum_crc32: input.checksum_crc32.clone(),
            checksum_crc32c: input.checksum_crc32c.clone(),
            checksum_sha1: input.checksum_sha1.clone(),
            checksum_sha256: input.checksum_sha256.clone(),
            checksum_crc64nvme: input.checksum_crc64nvme.clone(),
            checksum_sha512: input.checksum_sha512.clone(),
            checksum_md5: input.checksum_md5.clone(),
            checksum_xxhash64: input.checksum_xxhash64.clone(),
            checksum_xxhash3: input.checksum_xxhash3.clone(),
            checksum_xxhash128: input.checksum_xxhash128.clone(),
            ..Default::default()
        };

        let mut hasher = s3s::checksum::ChecksumHasher::default();
        enable_expected_checksums(&mut hasher, &expected_checksum);
        if let Some(alg) = input.checksum_algorithm.as_ref() {
            enable_checksum_algorithm(&mut hasher, alg.as_str())?;
        }
        let use_hasher = hasher_is_active(&hasher);

        let stream = match input.body.take() {
            Some(body) => body.map(|r| r.map_err(|e| anyhow::anyhow!(e))).left_stream(),
            None => futures::stream::empty().right_stream(),
        };

        let ingested = match ingest_stream_with_options(
            &self.store,
            stream,
            use_hasher.then_some(&mut hasher),
            self.ingest_options(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                self.queue_pending_deletes(e.pending_deletes).await;
                return Err(Self::map_err(e.source));
            }
        };

        if let Some(exp) = expected_md5 {
            verify_content_md5_digest(exp, &ingested.md5)?;
        }

        let computed = if use_hasher {
            hasher.finalize()
        } else {
            Checksum::default()
        };

        if let Some(trailers) = trailing_headers {
            if let Some(trailers) = trailers.take() {
                merge_trailer_checksums(&mut expected_checksum, &trailers)?;
            }
        }

        if let Some(field) = checksum_mismatch(&computed, &expected_checksum) {
            return Err(s3_error!(BadDigest, "{} mismatch", field));
        }
        // Fallback for legacy CRC32 header when hasher was not enabled.
        if !use_hasher {
            verify_checksum_crc32(input.checksum_crc32.as_deref(), ingested.crc32)?;
        }

        let content_type = input.content_type.as_deref();
        let user_meta: Vec<(String, String)> = input
            .metadata
            .unwrap_or_default()
            .into_iter()
            .collect();
        let checksums_json = if use_hasher {
            checksum_to_json(&computed)
        } else {
            "{}".to_string()
        };

        let orphans = self
            .index
            .put_object(
                &input.bucket,
                &input.key,
                &ingested.etag,
                ingested.size,
                content_type,
                &ingested.chunks,
                self.chat_id(),
                &user_meta,
                &checksums_json,
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;
        self.warm_l1_frames(&ingested.chunks).await;

        if !tags.is_empty() {
            self.index
                .put_object_tags(&input.bucket, &input.key, &tags)
                .await
                .map_err(Self::map_err)?;
        }

        info!(
            bucket = %input.bucket,
            key = %input.key,
            size = ingested.size,
            parts = ingested.chunks.len(),
            "PutObject ok"
        );

        let mut output = PutObjectOutput {
            e_tag: Some(etag_hex(&ingested.etag)),
            ..Default::default()
        };
        if use_hasher {
            apply_checksum_to_put(&mut output, &computed);
        }
        Ok(S3Response::new(output))
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
                let last = last
                    .unwrap_or(total.saturating_sub(1))
                    .min(total.saturating_sub(1));
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

        let body = if length == 0 {
            Some(StreamingBlob::from_bytes(Bytes::new()))
        } else {
            let end_excl = start.saturating_add(length);
            let readahead = self
                .note_sequential(&input.bucket, &input.key, start, end_excl)
                .await;
            let body_stream = stream_object_body(
                self.store.clone(),
                self.index.clone(),
                chunks,
                start,
                length,
                self.frame_cache.clone(),
                readahead,
            );
            Some(StreamingBlob::wrap(body_stream))
        };
        let content_range = input
            .range
            .as_ref()
            .map(|_| format!("bytes {start}-{end_inclusive}/{total}"));

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
        let tag_count = self
            .index
            .get_object_tags(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .len() as i32;

        let checksum_enabled = input
            .checksum_mode
            .as_ref()
            .is_some_and(|m| m.as_str() == ChecksumMode::ENABLED);
        let full_object = length == total;
        let stored_checksum = if checksum_enabled && full_object {
            self.index
                .get_object_checksums_json(&input.bucket, &input.key)
                .await
                .map_err(Self::map_err)?
                .map(|j| checksum_from_json(&j))
                .unwrap_or_default()
        } else {
            Checksum::default()
        };

        let mut output = GetObjectOutput {
            body,
            content_length: Some(length as i64),
            content_range,
            content_type: meta.content_type,
            e_tag: Some(etag_hex(&meta.etag)),
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            tag_count: (tag_count > 0).then_some(tag_count),
            ..Default::default()
        };
        if checksum_enabled && full_object {
            apply_checksum_to_get(&mut output, &stored_checksum);
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
        let tag_count = self
            .index
            .get_object_tags(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
            .len() as i32;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(meta.size),
            content_type: meta.content_type,
            e_tag: Some(etag_hex(&meta.etag)),
            last_modified: Some(ts(&meta.mtime)),
            metadata,
            tag_count: (tag_count > 0).then_some(tag_count),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        if let Some(orphans) = self
            .index
            .delete_object(&input.bucket, &input.key)
            .await
            .map_err(Self::map_err)?
        {
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
        let tags = match input.tagging.as_deref() {
            Some(h) => parse_tagging_header(h)?,
            None => Vec::new(),
        };
        let upload_id = uuid::Uuid::new_v4().to_string();
        let checksum_algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|a| a.as_str().to_owned());
        self.index
            .create_multipart_upload(
                &upload_id,
                &input.bucket,
                &input.key,
                input.content_type.as_deref(),
                &user_meta,
                &tags,
                checksum_algorithm.as_deref(),
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

        let mut expected_checksum = Checksum {
            checksum_crc32: input.checksum_crc32.clone(),
            checksum_crc32c: input.checksum_crc32c.clone(),
            checksum_sha1: input.checksum_sha1.clone(),
            checksum_sha256: input.checksum_sha256.clone(),
            checksum_crc64nvme: input.checksum_crc64nvme.clone(),
            checksum_sha512: input.checksum_sha512.clone(),
            checksum_md5: input.checksum_md5.clone(),
            checksum_xxhash64: input.checksum_xxhash64.clone(),
            checksum_xxhash3: input.checksum_xxhash3.clone(),
            checksum_xxhash128: input.checksum_xxhash128.clone(),
            ..Default::default()
        };

        let mut hasher = s3s::checksum::ChecksumHasher::default();
        enable_expected_checksums(&mut hasher, &expected_checksum);
        if let Some(alg) = upload.checksum_algorithm.as_deref() {
            enable_checksum_algorithm(&mut hasher, alg)?;
        }
        if let Some(alg) = input.checksum_algorithm.as_ref() {
            enable_checksum_algorithm(&mut hasher, alg.as_str())?;
        }
        let use_hasher = hasher_is_active(&hasher);

        let stream = match input.body.take() {
            Some(body) => body.map(|r| r.map_err(|e| anyhow::anyhow!(e))).left_stream(),
            None => futures::stream::empty().right_stream(),
        };
        let ingested = match ingest_stream_with_options(
            &self.store,
            stream,
            use_hasher.then_some(&mut hasher),
            self.ingest_options(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                self.queue_pending_deletes(e.pending_deletes).await;
                return Err(Self::map_err(e.source));
            }
        };
        if let Some(exp) = expected_md5 {
            verify_content_md5_digest(exp, &ingested.md5)?;
        }

        let computed = if use_hasher {
            hasher.finalize()
        } else {
            Checksum::default()
        };

        if let Some(trailers) = trailing_headers {
            if let Some(trailers) = trailers.take() {
                merge_trailer_checksums(&mut expected_checksum, &trailers)?;
            }
        }

        if let Some(field) = checksum_mismatch(&computed, &expected_checksum) {
            return Err(s3_error!(BadDigest, "{} mismatch", field));
        }
        if !use_hasher {
            verify_checksum_crc32(input.checksum_crc32.as_deref(), ingested.crc32)?;
        }

        let orphans = self
            .index
            .put_multipart_part(
                &upload_id,
                part_number,
                &ingested.etag,
                ingested.size,
                &ingested.chunks,
                self.chat_id(),
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

        let mut output = UploadPartOutput {
            e_tag: Some(etag_hex(&ingested.etag)),
            ..Default::default()
        };
        if use_hasher {
            apply_checksum_to_upload_part(&mut output, &computed);
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

        let (src_bucket, src_key) = match input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.to_string(), key.to_string()),
            _ => return Err(s3_error!(NotImplemented)),
        };
        let src = self
            .index
            .get_object(&src_bucket, &src_key)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let chunks = self
            .index
            .get_chunks(&src_bucket, &src_key)
            .await
            .map_err(Self::map_err)?;

        let total = src.size as u64;
        let (start, length) = match input.copy_source_range.as_deref() {
            None => (0u64, total),
            Some(r) => parse_copy_source_range(r, total)?,
        };

        let part_number = i64::from(input.part_number);

        // Chunk-aligned ranges: reuse Telegram file_ids (refcount++), like CopyObject.
        // Misaligned byte ranges still download + re-upload.
        let (etag, size, part_chunks, chat_for_blobs) =
            if let Some(aligned) = try_aligned_part_chunks(&chunks, start, length) {
                let etag = if start == 0
                    && length == total
                    && !src.etag.contains('-')
                {
                    // Single-shot PutObject etag is content-MD5; safe to reuse.
                    src.etag.clone()
                } else {
                    // Hash logical bytes via getFile only — no sendDocument.
                    use md5::Digest;
                    let mut md5 = md5::Md5::new();
                    for c in &aligned {
                        let data = self
                            .store
                            .get(&c.file_id)
                            .await
                            .map_err(Self::map_err)?;
                        let frames = if c.codec == ChunkCodec::Frames {
                            self.index
                                .get_chunk_frames(&c.file_id)
                                .await
                                .map_err(Self::map_err)?
                        } else {
                            Vec::new()
                        };
                        let logical = decode_chunk_slice_async(
                            data,
                            c.codec,
                            &frames,
                            0,
                            c.logical_size as usize,
                            c.logical_size.max(1) as usize,
                        )
                        .await
                        .map_err(Self::map_err)?;
                        md5.update(&logical);
                    }
                    format!("{:x}", md5.finalize())
                };
                let src_chat = self
                    .index
                    .bucket_chat_id(&src_bucket)
                    .await
                    .map_err(Self::map_err)?
                    .unwrap_or_else(|| self.chat_id().to_string());
                (etag, length as i64, aligned, src_chat)
            } else {
                let body_stream = stream_object_body(
                    self.store.clone(),
                    self.index.clone(),
                    chunks,
                    start,
                    length,
                    self.frame_cache.clone(),
                    true,
                )
                .map(|r| r.map_err(|e| anyhow::anyhow!(e)));
                let ingested = match ingest_stream_with_options(
                    &self.store,
                    body_stream,
                    None,
                    self.ingest_options(),
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        self.queue_pending_deletes(e.pending_deletes).await;
                        return Err(Self::map_err(e.source));
                    }
                };
                (
                    ingested.etag,
                    ingested.size,
                    ingested.chunks,
                    self.chat_id().to_string(),
                )
            };

        let orphans = self
            .index
            .put_multipart_part(
                &input.upload_id,
                part_number,
                &etag,
                size,
                &part_chunks,
                &chat_for_blobs,
            )
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

        let part = self
            .index
            .get_multipart_part(&input.upload_id, part_number)
            .await
            .map_err(Self::map_err)?
            .ok_or_else(|| s3_error!(InternalError, "part missing after copy"))?;

        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(etag_hex(&part.etag)),
                last_modified: Some(Timestamp::from(std::time::SystemTime::now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
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
        if !self
            .index
            .bucket_exists(&input.bucket)
            .await
            .map_err(Self::map_err)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
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

        let orphans = self
            .index
            .complete_multipart_upload(&upload, &part_numbers, &etag, total_size)
            .await
            .map_err(Self::map_err)?;
        self.cleanup_orphans(orphans).await;

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
