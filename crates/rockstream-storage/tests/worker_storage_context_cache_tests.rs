//! Worker Storage Context Cache & Budget Tests (v0.59.6).
//!
//! Asserts block and index cache reuse across multiple views/shards,
//! high hit ratio on shared arrangements, and deterministic LRU eviction under memory budget.

use rockstream_storage::storage_context::{BlockCacheKey, WorkerStorageContext};
use rockstream_types::ids::{ArrangementId, TenantId};

#[test]
fn test_shared_block_and_index_cache_reuse() {
    let ctx = WorkerStorageContext::new(1024 * 1024); // 1 MB budget
    let tenant = TenantId(1);
    let policy = [0u8; 32];
    let arr_id = ArrangementId(42);

    let key1 = BlockCacheKey::new(tenant, policy, arr_id, 100);
    let key2 = BlockCacheKey::new(tenant, policy, arr_id, 200);

    // Initial put
    ctx.put_block(key1.clone(), b"block_data_100".to_vec());
    ctx.put_index(key2.clone(), b"index_data_200".to_vec());

    // Multiple views accessing the exact same cached block
    for _ in 0..10 {
        let block = ctx.get_block(&key1).expect("cache hit");
        assert_eq!(block, b"block_data_100");
    }

    // Multiple shards accessing the same index block
    for _ in 0..10 {
        let index = ctx.get_index(&key2).expect("cache hit");
        assert_eq!(index, b"index_data_200");
    }

    let stats = ctx.stats();
    assert_eq!(stats.hits, 20);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.evictions, 0);
    assert_eq!(ctx.hit_ratio(), 1.0);
}

#[test]
fn test_storage_context_budget_eviction() {
    // Very small budget: 500 bytes (blocks budget = 350 bytes)
    let ctx = WorkerStorageContext::new(500);
    let tenant = TenantId(1);
    let policy = [0u8; 32];
    let arr_id = ArrangementId(1);

    // Insert 10 blocks of 50 bytes each (50 + overhead > 350 bytes)
    for i in 0..10 {
        let key = BlockCacheKey::new(tenant, policy, arr_id, i);
        let data = vec![i as u8; 50];
        ctx.put_block(key, data);
    }

    let stats = ctx.stats();
    assert!(stats.evictions > 0);
    assert!(stats.current_bytes <= stats.capacity_bytes);

    // Oldest block (block 0) should have been evicted
    let key0 = BlockCacheKey::new(tenant, policy, arr_id, 0);
    assert_eq!(ctx.get_block(&key0), None);

    // Most recent block (block 9) should still be in cache
    let key9 = BlockCacheKey::new(tenant, policy, arr_id, 9);
    assert!(ctx.get_block(&key9).is_some());
}
