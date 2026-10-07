use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_testkit::run_conformance;

#[tokio::test]
async fn memory_conformance() {
    let store = MemoryBlobStore::new();
    run_conformance(&store).await.unwrap();
}
