//! Conformance suites for pigeonhole storage backends.

use anyhow::Result;
use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, BlobBackend, BlobId, Sweepable,
};

/// Shared suite for [`BlobBackend`] + [`Sweepable`].
///
/// Covers: put/get/range/delete, repeat delete, empty rejected, near-`max_blob_size`
/// blob, monotonic keys, candidates covering written keys.
pub async fn run_typed_conformance<B>(backend: &B) -> Result<()>
where
    B: BlobBackend + Sweepable,
{
    // empty rejected
    assert!(
        backend.put(Bytes::new(), &BlobId::random().name()).await.is_err(),
        "empty blob must be rejected"
    );

    let id = backend.put(Bytes::from_static(b"hello-typed-conformance"), &BlobId::random().name()).await?;
    let got = collect_stream(backend.get(&id, None).await?).await?;
    assert_eq!(got.as_ref(), b"hello-typed-conformance");

    let mid = collect_stream(backend.get(&id, Some(6..18)).await?).await?;
    assert_eq!(mid.as_ref(), b"typed-confor");

    // Near max (capped for test runtime; oversized still uses true max).
    let max = backend.limits().max_blob_size.min(64 * 1024);
    let big = Bytes::from(vec![0xABu8; max]);
    let big_id = backend.put(big.clone(), &BlobId::random().name()).await?;
    let big_got = collect_stream(backend.get(&big_id, None).await?).await?;
    assert_eq!(big_got, big);

    let over = Bytes::from(vec![0u8; backend.limits().max_blob_size + 1]);
    assert!(backend.put(over, &BlobId::random().name()).await.is_err());

    // Monotonic keys across successive puts.
    let mut ids = vec![id.clone(), big_id.clone()];
    for i in 0..4u8 {
        ids.push(backend.put(Bytes::from(vec![i; 32]), &BlobId::random().name()).await?);
    }
    for w in ids.windows(2) {
        assert!(
            B::key(&w[0]) < B::key(&w[1]),
            "keys must grow: {:?} then {:?}",
            B::key(&w[0]),
            B::key(&w[1])
        );
    }

    let max_key = B::key(ids.last().unwrap());
    let written: Vec<_> = ids.iter().map(B::key).collect();
    // Page candidates so backends with dense synthetic ranges (Telegram) still
    // reach high message ids within a reasonable limit per call.
    let mut found = Vec::new();
    let mut after = None;
    for _ in 0..256 {
        let batch = backend.candidates(after, max_key, 100).await?;
        if batch.is_empty() {
            break;
        }
        after = Some(*batch.last().unwrap());
        found.extend(batch);
        if written.iter().all(|k| found.contains(k)) {
            break;
        }
    }
    for k in &written {
        assert!(
            found.contains(k),
            "candidates missing key {k:?}; got {} keys up to {:?}",
            found.len(),
            found.last()
        );
    }

    // Delete + repeat delete (not-found = ok).
    let del_key = B::key(&id);
    backend.delete(&[del_key]).await?;
    assert!(backend.get(&id, None).await.is_err());
    backend.delete(&[del_key]).await?;

    // Cleanup remaining blobs (best-effort).
    let rest: Vec<_> = ids.iter().skip(1).map(B::key).collect();
    let _ = backend.delete(&rest).await;
    Ok(())
}
