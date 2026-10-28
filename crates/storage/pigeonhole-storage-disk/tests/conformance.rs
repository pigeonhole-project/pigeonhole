use pigeonhole_storage_disk::DiskBlobStore;
use pigeonhole_testkit::run_typed_conformance;

#[tokio::test]
async fn disk_typed_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let store = DiskBlobStore::open(dir.path()).unwrap();
    run_typed_conformance(&store).await.unwrap();
}
