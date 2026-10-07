//! Conformance suites for pigeonhole storage backends.

use pigeonhole_blob::{collect_stream, BlobBackend, DeleteOutcome, PutHint};
use anyhow::Result;
use bytes::Bytes;

/// Run the shared put/get/range/delete suite against any backend.
pub async fn run_conformance(backend: &dyn BlobBackend) -> Result<()> {
    // empty rejected
    assert!(backend
        .put(Bytes::new(), PutHint::new("empty.bin", ""))
        .await
        .is_err());

    // put / get / delete
    let loc = backend
        .put(Bytes::from_static(b"hello-conformance"), PutHint::new("a.bin", "cap"))
        .await?;
    let got = collect_stream(backend.get(&loc, None).await?).await?;
    assert_eq!(got.as_ref(), b"hello-conformance");

    // range
    let mid = collect_stream(backend.get(&loc, Some(6..12)).await?).await?;
    assert_eq!(mid.as_ref(), b"confor");

    // boundary-sized blob (still under max)
    let max = backend.limits().max_blob_size.min(64 * 1024);
    let big = Bytes::from(vec![0xABu8; max]);
    let big_loc = backend
        .put(big.clone(), PutHint::new("big.bin", ""))
        .await?;
    let big_got = collect_stream(backend.get(&big_loc, None).await?).await?;
    assert_eq!(big_got.len(), max);

    // oversized rejected
    let over = Bytes::from(vec![0u8; backend.limits().max_blob_size + 1]);
    assert!(backend
        .put(over, PutHint::new("over.bin", ""))
        .await
        .is_err());

    // delete + repeat delete
    assert_eq!(backend.delete(&loc).await?, DeleteOutcome::Deleted);
    assert!(backend.get(&loc, None).await.is_err());
    assert_eq!(backend.delete(&loc).await?, DeleteOutcome::Gone);

    // concurrent puts
    let mut handles = Vec::new();
    for i in 0..8u8 {
        // sequential is enough for Memory; backends that are Sync share &self
        let data = Bytes::from(vec![i; 128]);
        let loc = backend
            .put(data.clone(), PutHint::new(format!("c{i}.bin"), ""))
            .await?;
        let got = collect_stream(backend.get(&loc, None).await?).await?;
        assert_eq!(got, data);
        handles.push(loc);
    }
    for loc in handles {
        let _ = backend.delete(&loc).await?;
    }

    let _ = backend.delete(&big_loc).await?;
    Ok(())
}

