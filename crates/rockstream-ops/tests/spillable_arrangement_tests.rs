use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_ops::spill::SpillableArrangement;
use rockstream_storage::ShardDb;
use rockstream_types::metrics::{
    read_spill_faults_total, read_spilled_bytes, reset_all, METRICS_TEST_LOCK,
};

async fn open_test_db(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

#[tokio::test]
async fn test_spillable_arrangement_in_memory_basic() {
    let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
        SpillableArrangement::new(None, b"test:".to_vec(), 1000);

    arr.insert(b"key1".to_vec(), b"val1".to_vec()).unwrap();
    arr.insert(b"key2".to_vec(), b"val2".to_vec()).unwrap();

    assert_eq!(arr.get(&b"key1".to_vec()).unwrap(), Some(b"val1".to_vec()));
    assert_eq!(arr.get(&b"key2".to_vec()).unwrap(), Some(b"val2".to_vec()));
    assert_eq!(arr.get(&b"key3".to_vec()).unwrap(), None);

    assert_eq!(arr.in_memory_entry_count(), 2);
    assert_eq!(arr.spilled_entry_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spillable_arrangement_evicts_to_shard_db_and_faults_back() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let db = open_test_db("spill-test-1").await;
    // Set low memory limit (15 bytes) to force eviction when multiple entries are inserted.
    let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
        SpillableArrangement::new(Some(db.clone()), b"spill:".to_vec(), 15);

    // key1 (4) + val1 (4) = 8 bytes
    arr.insert(b"key1".to_vec(), b"val1".to_vec()).unwrap();
    // key2 (4) + val2 (4) = 8 bytes -> Total = 16 bytes > 15 limit -> cold entry (key1) spilled!
    arr.insert(b"key2".to_vec(), b"val2".to_vec()).unwrap();

    assert!(arr.spilled_entry_count() > 0 || read_spilled_bytes() > 0);

    // Fault back key1
    let val1 = arr.get(&b"key1".to_vec()).unwrap();
    assert_eq!(val1, Some(b"val1".to_vec()));
    assert!(read_spill_faults_total() > 0);

    // Check all values via scan_all
    let all = arr.scan_all().unwrap();
    assert_eq!(all.len(), 2);
}
