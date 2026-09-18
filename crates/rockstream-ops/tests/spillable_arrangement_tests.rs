#![allow(clippy::await_holding_lock)]

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

#[tokio::test(flavor = "multi_thread")]
async fn sole_oversized_entry_spills_and_scans_exactly() {
    let db = open_test_db("spill-test-sole-entry").await;
    let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
        SpillableArrangement::new(Some(db.clone()), b"spill:sole:".to_vec(), 7);

    arr.insert(b"key".to_vec(), b"value".to_vec()).unwrap();

    assert_eq!(arr.in_memory_entry_count(), 0);
    assert_eq!(arr.spilled_entry_count(), 1);
    assert_eq!(
        arr.scan_all().unwrap(),
        vec![(b"key".to_vec(), b"value".to_vec())]
    );
    assert_eq!(
        db.scan_prefix(b"spill:sole:").await.unwrap(),
        vec![(
            bytes::Bytes::from_static(b"spill:sole:key"),
            bytes::Bytes::from_static(b"value"),
        )]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_spillable_arrangement_zero_unbounded_key_metadata() {
    let db = open_test_db("spill-test-zero-metadata").await;
    // Limit memory to 200 bytes so that only a tiny fraction of 2,000 entries can stay in memory
    let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
        SpillableArrangement::new(Some(db.clone()), b"spill:bounded:".to_vec(), 200);

    let count = 2000;
    for i in 0..count {
        let key = format!("k{:06}", i).into_bytes();
        let val = format!("v{:06}", i).into_bytes();
        arr.insert(key, val).unwrap();
    }

    // In-memory count must be strictly bounded by memory limit (each entry ~14 bytes + overhead, so <= 15 entries)
    assert!(arr.in_memory_entry_count() <= 15);
    // Almost all entries should have spilled
    assert_eq!(
        arr.spilled_entry_count(),
        count - arr.in_memory_entry_count()
    );

    // Verify negative cache works: non-existent keys return None
    assert_eq!(arr.get(&b"nonexistent_key".to_vec()).unwrap(), None);

    // Verify all keys can still be retrieved (demand-loaded / faulted from disk)
    for i in [0, 500, 1000, 1500, 1999] {
        let key = format!("k{:06}", i).into_bytes();
        let expected_val = format!("v{:06}", i).into_bytes();
        assert_eq!(arr.get(&key).unwrap(), Some(expected_val));
    }

    // Memory usage remains bounded after lookups
    assert!(arr.in_memory_entry_count() <= 15);

    // Remove some keys and verify removal
    let del_key = format!("k{:06}", 500).into_bytes();
    assert_eq!(
        arr.remove(&del_key).unwrap(),
        Some(format!("v{:06}", 500).into_bytes())
    );
    assert_eq!(arr.get(&del_key).unwrap(), None);
}
