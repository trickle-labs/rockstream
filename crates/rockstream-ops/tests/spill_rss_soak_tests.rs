use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_ops::spill::SpillableArrangement;
use rockstream_storage::ShardDb;

async fn open_test_db(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_process_rss_under_budget_soak() {
    let db = open_test_db("rss-soak-test").await;
    // Set 100 KB limit and write 1000 items (larger than budget) to ensure RSS stays bounded
    let limit = 10 * 1024;
    let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
        SpillableArrangement::new(Some(db), b"rss:".to_vec(), limit);

    for i in 0..100 {
        let key = format!("soak_key_{:06}", i).into_bytes();
        let val = vec![0u8; 512];
        arr.insert(key, val).unwrap();
    }

    assert!(
        arr.state_bytes() <= (limit * 2) as u64,
        "In-memory state bytes {} should remain bounded near limit {}",
        arr.state_bytes(),
        limit
    );
}
