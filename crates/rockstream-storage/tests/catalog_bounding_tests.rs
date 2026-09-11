use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use rockstream_storage::catalog::log::MAX_REPLAY_BUFFER_BYTES;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use std::sync::Arc;

#[test]
fn test_max_mutations_per_transaction_bounded() {
    let mut mutations = Vec::new();
    for i in 0..1001 {
        mutations.push(CatalogMutation::DeleteTable(TableId(i)));
    }

    let res = CatalogTxn::new(1, 1, mutations);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1002]"),
        "Expected RS-1002 when mutations exceed 1000, got: {err}"
    );
}

#[tokio::test]
async fn test_replay_buffer_memory_bounded() {
    let store = Arc::new(InMemory::new());

    // Inject a gigantic log file that exceeds 64 MB
    let huge_bytes = vec![b'x'; MAX_REPLAY_BUFFER_BYTES + 1024];
    let path = ObjectPath::from("test/catalog/log/00000000000000000001_00000000000000000001.log");
    store.put(&path, huge_bytes.into()).await.unwrap();

    let res = DurableCatalogStore::recover(store, "test").await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1003]") && err.to_string().contains("replay buffer exceeded"),
        "Expected RS-1003 buffer exceeded error, got: {err}"
    );
}

#[tokio::test]
async fn test_inaccessible_catalog_storage_fails_fast() {
    use rockstream_storage::build_storage_backend_from_url;
    use rockstream_types::config::StorageUrl;
    use std::path::PathBuf;

    // Use a completely invalid/inaccessible path (e.g. invalid permissions or dev null child)
    let invalid_url = StorageUrl::File(PathBuf::from("/dev/null/impossible_dir/rockstream"));
    let res = build_storage_backend_from_url(&invalid_url);
    assert!(res.is_err(), "Inaccessible storage must fail fast");
}
