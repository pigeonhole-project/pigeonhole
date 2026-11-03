use crate::cas_index::{CasEntry, CasIndex};
use crate::commands::CasReply;
use crate::digest::{digest_hash_hex, hash_eq};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_chunk_store::{unique_chunk_ids, ChunkId, ChunkStore, PendingChunks};
use pigeonhole_meta::Proposal;
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Store and fetch CAS payloads through [`CasIndex`] + [`ChunkStore`].
#[derive(Clone)]
pub struct CasStore {
    pub cas: CasIndex,
    pub store: Arc<ChunkStore>,
}

impl CasStore {
    pub fn new(cas: CasIndex, store: Arc<ChunkStore>) -> Self {
        Self { cas, store }
    }

    pub async fn find_missing(&self, digests: &[crate::reapi::Digest]) -> Result<Vec<crate::reapi::Digest>> {
        let mut pairs = Vec::with_capacity(digests.len());
        for d in digests {
            pairs.push((digest_hash_hex(d)?, d.size_bytes));
        }
        let missing = self.cas.find_missing(&pairs).await?;
        Ok(missing.into_iter().map(|(hash, size)| crate::reapi::Digest { hash, size_bytes: size }).collect())
    }

    pub async fn get_bytes(&self, hash_hex: &str, size: i64) -> Result<Option<Bytes>> {
        let Some(stream) = self.read_range(hash_hex, size, 0, size).await? else {
            return Ok(None);
        };
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(Some(Bytes::from(out)))
    }

    /// Read `[offset, offset+limit)` of a CAS blob (limit <= 0 means through EOF).
    pub async fn read_range(
        &self,
        hash_hex: &str,
        size: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Option<pigeonhole_chunk_store::BoxByteStream>> {
        let Some(entry) = self.cas.get(hash_hex, size).await? else {
            return Ok(None);
        };
        let from = offset.max(0) as u64;
        if from as i64 > size {
            anyhow::bail!("read offset out of range");
        }
        let to = if limit > 0 { (from as i64 + limit).min(size) as u64 } else { size as u64 };
        self.cas.touch(hash_hex, size);
        if from == to {
            return Ok(Some(Box::pin(futures::stream::once(async { Ok(Bytes::new()) }))));
        }

        let store = self.store.clone();
        let extents = entry.extents;
        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1);
        tokio::spawn(async move {
            let result = store.read(&extents, Some(from..to)).await;
            let _ = tx.send(result).await;
        });
        Ok(Some(Box::pin(ReceiverStream::new(rx))))
    }

    /// True when a CAS row for `(hash, size)` exists and its chunks are live
    /// (the read is noted). A row pointing at reclaimed chunks is dropped so
    /// the caller re-ingests.
    ///
    /// The row owns exactly one ref, released by GC; a repeat write adds
    /// none. Liveness is a read of the chunk state: nothing goes to the log.
    async fn existing_is_live(&self, hash_hex: &str, size: i64) -> Result<bool> {
        let Some(entry) = self.cas.get(hash_hex, size).await? else {
            return Ok(false);
        };
        if self.store.db().are_live(&unique_chunk_ids(&entry.extents)) {
            self.cas.touch(hash_hex, size);
            return Ok(true);
        }
        self.cas.release(hash_hex, size).await?;
        Ok(false)
    }

    pub async fn put_bytes(&self, hash_hex: &str, size: i64, data: Bytes) -> Result<()> {
        let stream = futures::stream::iter(std::iter::once(Ok::<_, anyhow::Error>(data)));
        self.put_stream(hash_hex, size, stream).await
    }

    /// Store a body under the digest `(hash_hex, size)`: its bytes are hashed
    /// on their way to the chunk store, and nothing is committed unless they
    /// are the digest's ([`DigestMismatch`] then; the parts uploaded are
    /// orphans for the sweeper). A digest already stored keeps its entry:
    /// the body is only hashed, to answer whether it was the digest's.
    pub async fn put_stream<S>(&self, hash_hex: &str, size: i64, stream: S) -> Result<()>
    where
        S: futures::Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
    {
        let mut seen = Seen::default();
        let mut hashed = stream.inspect(|item| seen.add(item));
        if self.existing_is_live(hash_hex, size).await? {
            while let Some(item) = hashed.next().await {
                item?;
            }
            drop(hashed);
            return seen.check(hash_hex, size);
        }

        // The store's `[chunk]` and `[ingest]` settings.
        let mut opts = self.store.ingest_options();
        opts.size_hint = u64::try_from(size).ok();
        let mut ingested = self.store.ingest(&mut hashed, Some(opts)).await.context("CAS ingest")?;
        drop(hashed);
        // Dropped uncommitted on a mismatch: the parts are orphans.
        seen.check(hash_hex, size)?;

        // The chunks and the entry: one log entry.
        let new_ids = unique_chunk_ids(&ingested.extents);
        let entry = CasEntry { hash: hash_hex.to_string(), size, extents: ingested.extents.clone() };
        let chunks = std::mem::take(&mut ingested.chunks);
        let this = self.clone();
        // The commit and the release a duplicate needs: to the end even if
        // the request is dropped meanwhile.
        pigeonhole_chunk_store::to_the_end(async move { this.commit(entry, chunks, new_ids).await }).await?
    }

    /// Commit `entry` with its chunks; if another writer stored the digest
    /// first, release `new_ids` (ours).
    async fn commit(&self, entry: CasEntry, chunks: PendingChunks, new_ids: Vec<ChunkId>) -> Result<()> {
        let mut proposal = Proposal::new();
        let slot = proposal.push(self.cas.prepare_store(&entry));
        let mut outcomes = self.store.commit_along(chunks, self.cas.meta(), proposal).await?;
        let Ok(reply) = outcomes.take(slot)?;
        match reply {
            CasReply::Stored => Ok(()),
            // Another writer already stored this digest. Its row holds the one ref
            // that `CasIndex::release` drops on GC; drop ours and leave theirs as is
            // (an extra retain here would never be released).
            CasReply::Existing(_) => {
                let _ = self.store.release(&new_ids).await;
                Ok(())
            }
            other => anyhow::bail!("unexpected cas reply {other:?}"),
        }
    }
}

/// The SHA-256 and length of the bytes that went by.
#[derive(Default)]
struct Seen {
    hasher: Sha256,
    len: i64,
}

impl Seen {
    fn add(&mut self, item: &Result<Bytes>) {
        if let Ok(b) = item {
            self.hasher.update(b);
            self.len += b.len() as i64;
        }
    }

    fn check(self, hash_hex: &str, size: i64) -> Result<()> {
        let got = hex::encode(self.hasher.finalize());
        if self.len != size || !hash_eq(&got, hash_hex) {
            return Err(
                DigestMismatch { expected: format!("{hash_hex}/{size}"), got: format!("{got}/{}", self.len) }.into()
            );
        }
        Ok(())
    }
}

/// A body that is not the digest it was sent under.
#[derive(Debug)]
pub struct DigestMismatch {
    pub expected: String,
    pub got: String,
}

impl std::fmt::Display for DigestMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "digest mismatch: sent as {}, the bytes are {}", self.expected, self.got)
    }
}

impl std::error::Error for DigestMismatch {}

/// Whether `e` is a [`DigestMismatch`] (the client's fault, not ours).
pub fn is_digest_mismatch(e: &anyhow::Error) -> bool {
    e.downcast_ref::<DigestMismatch>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_chunk_store::{BlobDb, IngestOptions};
    use pigeonhole_codec::ChunkCodec;
    use pigeonhole_storage_memory::MemoryBlobStore;

    async fn open_cas() -> (CasStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cas_url = dir.path().join("cas.log");
        let blob_url = dir.path().join("blob.log");
        let cas = CasIndex::open(pigeonhole_testkit::tablet(&cas_url, CasIndex::register).await).await.unwrap();
        let db = BlobDb::open(pigeonhole_testkit::tablet(&blob_url, BlobDb::register).await).await.unwrap();
        let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
        opts.block_size = 64 * 1024;
        let store = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
        (CasStore::new(cas, store), dir)
    }

    #[tokio::test]
    async fn put_get_and_retain_duplicate() {
        let (cas, _dir) = open_cas().await;
        let payload = Bytes::from(vec![9u8; 10_000]);
        let hash = hex::encode(Sha256::digest(&payload));
        let size = payload.len() as i64;
        cas.put_bytes(&hash, size, payload.clone()).await.unwrap();
        let got = cas.get_bytes(&hash, size).await.unwrap().unwrap();
        assert_eq!(got, payload);

        // Duplicate write is a no-op: one logical object, one ref (released by GC).
        cas.put_bytes(&hash, size, payload.clone()).await.unwrap();
        let entry = cas.cas.get(&hash, size).await.unwrap().unwrap();
        assert!(!entry.extents.is_empty());
        let meta = cas.store.db().chunk_meta(entry.extents[0].chunk).await.unwrap().unwrap();
        assert_eq!(meta.2, 1, "duplicate write must not add a ref");
    }

    fn digest(data: &[u8]) -> (String, i64) {
        (hex::encode(Sha256::digest(data)), data.len() as i64)
    }

    /// Log positions of the CAS tablet and of the chunk tablet.
    fn positions(cas: &CasStore) -> (u64, u64) {
        (cas.cas.meta().applied_position(), cas.store.meta().applied_position())
    }

    #[tokio::test]
    async fn wrong_bytes_under_an_existing_digest_leave_its_entry_alone() {
        let (cas, _dir) = open_cas().await;
        let good = Bytes::from(vec![1u8; 5_000]);
        let (hash, size) = digest(&good);
        cas.put_bytes(&hash, size, good.clone()).await.unwrap();
        let entry = cas.cas.get(&hash, size).await.unwrap().unwrap();

        let bad = Bytes::from(vec![2u8; 5_000]);
        let err = cas.put_bytes(&hash, size, bad).await.unwrap_err();
        assert!(is_digest_mismatch(&err), "{err:#}");
        assert_eq!(cas.cas.get(&hash, size).await.unwrap().unwrap().extents, entry.extents);
        assert_eq!(cas.get_bytes(&hash, size).await.unwrap().unwrap(), good);
        let refs = cas.store.db().chunk_meta(entry.extents[0].chunk).await.unwrap().unwrap().2;
        assert_eq!(refs, 1);
    }

    #[tokio::test]
    async fn wrong_bytes_under_a_new_digest_commit_nothing() {
        let (cas, _dir) = open_cas().await;
        let (hash, size) = digest(&[3u8; 5_000]);
        let before = positions(&cas);
        let err = cas.put_bytes(&hash, size, Bytes::from(vec![4u8; 5_000])).await.unwrap_err();
        assert!(is_digest_mismatch(&err), "{err:#}");
        let short = cas.put_bytes(&hash, size, Bytes::from(vec![3u8; 4_000])).await.unwrap_err();
        assert!(is_digest_mismatch(&short), "{short:#}");
        assert!(cas.cas.get(&hash, size).await.unwrap().is_none());
        assert_eq!(cas.store.db().stats().live, 0, "no chunk committed");
        assert_eq!(positions(&cas), before, "nothing appended to either log");
    }

    /// Reads and a repeat write append nothing; their times reach the log
    /// as one entry when flushed, and a GC pass spares what was read.
    #[tokio::test]
    async fn reads_and_dedup_writes_append_nothing_until_flushed() {
        let (cas, _dir) = open_cas().await;
        let (a, b) = (Bytes::from(vec![5u8; 3_000]), Bytes::from(vec![6u8; 3_000]));
        let ((ha, sa), (hb, sb)) = (digest(&a), digest(&b));
        cas.put_bytes(&ha, sa, a.clone()).await.unwrap();
        cas.put_bytes(&hb, sb, b.clone()).await.unwrap();
        // Times are kept to the millisecond.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let cutoff = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let before = positions(&cas);
        for _ in 0..3 {
            assert_eq!(cas.get_bytes(&ha, sa).await.unwrap().unwrap(), a);
            cas.put_bytes(&ha, sa, a.clone()).await.unwrap();
        }
        assert_eq!(positions(&cas), before, "reads and repeat writes are not log entries");

        // Unflushed reads count: only b is stale.
        let released = cas.cas.drop_stale(cutoff).await.unwrap();
        assert_eq!(released.len(), 1);
        cas.store.release(&released).await.unwrap();
        assert!(cas.cas.get(&hb, sb).await.unwrap().is_none());
        assert!(cas.cas.get(&ha, sa).await.unwrap().is_some());

        let before = positions(&cas).0;
        cas.cas.flush_touches().await.unwrap();
        assert_eq!(cas.cas.meta().applied_position(), before + 1, "one entry for every read");
        cas.cas.flush_touches().await.unwrap();
        assert_eq!(cas.cas.meta().applied_position(), before + 1, "nothing to flush");
        assert!(cas.cas.drop_stale(cutoff).await.unwrap().is_empty(), "a was read after the cutoff");
    }

    #[tokio::test]
    async fn action_results_are_keyed_by_the_action_digest() {
        let (cas, _dir) = open_cas().await;
        let (action, size) = digest(b"an action");
        assert_eq!(cas.cas.get_action(&action, size), None);
        cas.cas.put_action(&action, size, b"result".to_vec()).await.unwrap();
        assert_eq!(cas.cas.get_action(&action, size).as_deref(), Some(&b"result"[..]));
        cas.cas.put_action(&action, size, b"newer".to_vec()).await.unwrap();
        assert_eq!(cas.cas.get_action(&action, size).as_deref(), Some(&b"newer"[..]));
        assert!(cas.cas.find_missing(&[(action.clone(), size)]).await.unwrap().len() == 1, "not a CAS blob");
    }
}
