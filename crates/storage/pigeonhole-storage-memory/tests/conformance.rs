use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_testkit::run_typed_conformance;

#[tokio::test]
async fn memory_typed_conformance() {
    let store = MemoryBlobStore::new();
    run_typed_conformance(&store).await.unwrap();
}
