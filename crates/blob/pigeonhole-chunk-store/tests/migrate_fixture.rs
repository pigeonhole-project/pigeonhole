//! Stage G: migrate checked-in legacy s3gram index fixture into blob.db.

use pigeonhole_blob::InstanceKind;
use pigeonhole_chunk_store::{
    default_instance_for_migrate, migrate_index_to_blob_db, BlobDb, ChunkStore, IngestOptions,
};
use pigeonhole_codec::ChunkCodec;
use pigeonhole_storage_memory::MemoryBlobStore;
use std::path::PathBuf;

fn fixture_path() -> PathBuf {
    // workspace root / tests/fixtures/legacy_s3gram.db
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // pigeonhole-chunk-store
    p.pop(); // blob
    p.pop(); // crates
    p.push("tests/fixtures/legacy_s3gram.db");
    p
}

#[tokio::test]
async fn legacy_fixture_migrates_and_reads_metadata() {
    let fixture = fixture_path();
    assert!(
        fixture.is_file(),
        "missing fixture {}; generate via Stage G setup",
        fixture.display()
    );

    let dir = tempfile::tempdir().unwrap();
    let blob_url = format!("sqlite:{}?mode=rwc", dir.path().join("blob.db").display());
    let blob_db = BlobDb::connect(&blob_url).await.unwrap();
    let legacy_url = format!("sqlite:{}?mode=ro", fixture.display());

    let inst = default_instance_for_migrate(InstanceKind::Memory, "", "local").unwrap();
    let report = migrate_index_to_blob_db(&legacy_url, &blob_db, &inst, false)
        .await
        .unwrap();
    assert!(report.blobs >= 1, "expected migrated blobs: {report:?}");
    assert!(
        blob_db.get_root("s3/index").await.unwrap().is_some(),
        "fixture meta.snapshot_file_id should become s3/index root"
    );

    let mut opts = IngestOptions::new(64 * 1024, ChunkCodec::Raw);
    opts.block_size = 64 * 1024;
    let layer = ChunkStore::open(blob_db.clone(), MemoryBlobStore::new(), opts)
        .await
        .unwrap();
    let extents = layer.get_root("s3/index").await.unwrap().unwrap();
    assert!(!extents.is_empty());
    // Metadata is present; payload bytes live on Telegram and are absent in this fixture.
    let meta = blob_db.chunk_meta(extents[0].chunk).await.unwrap().unwrap();
    assert_eq!(meta.0, 11);
}
