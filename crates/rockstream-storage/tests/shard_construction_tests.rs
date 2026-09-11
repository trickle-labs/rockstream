//! Production Shard Construction & WorkerStorageContext Injection Tests (v0.62.1 Slice 2 / Phase 3a).

use object_store::memory::InMemory;
use rockstream_storage::shard_db::ShardDb;
use rockstream_storage::storage_context::{BlockCacheKey, WorkerStorageContext};
use rockstream_types::ids::{ArrangementId, TenantId};
use std::sync::Arc;

#[tokio::test]
async fn test_every_production_shard_receives_shared_storage_context() {
    let storage_context = Arc::new(WorkerStorageContext::new_with_worker_id(
        "worker-1",
        1024 * 1024,
    ));
    let store1 = Arc::new(InMemory::new());
    let store2 = Arc::new(InMemory::new());

    let shard1 = ShardDb::builder("shard-1", store1)
        .with_storage_context(storage_context.clone())
        .build()
        .await
        .expect("build shard 1");

    let shard2 = ShardDb::builder("shard-2", store2)
        .with_storage_context(storage_context.clone())
        .build()
        .await
        .expect("build shard 2");

    let ctx1 = shard1
        .storage_context()
        .expect("shard 1 has storage context");
    let ctx2 = shard2
        .storage_context()
        .expect("shard 2 has storage context");

    assert!(Arc::ptr_eq(ctx1, ctx2));
    assert!(Arc::ptr_eq(ctx1, &storage_context));
}

#[tokio::test]
async fn test_shared_cache_allocations_charged_once_across_shards() {
    let capacity = 100 * 1024; // 100 KiB
    let shared_ctx = Arc::new(WorkerStorageContext::new_with_worker_id(
        "worker-shared",
        capacity,
    ));
    let store1 = Arc::new(InMemory::new());
    let store2 = Arc::new(InMemory::new());

    let shard1 = ShardDb::builder("shard-a", store1)
        .with_storage_context(shared_ctx.clone())
        .build()
        .await
        .expect("build shard a");

    let shard2 = ShardDb::builder("shard-b", store2)
        .with_storage_context(shared_ctx.clone())
        .build()
        .await
        .expect("build shard b");

    // Initial stats on shared context
    let initial_stats = shared_ctx.stats();
    assert_eq!(initial_stats.capacity_bytes, capacity);
    assert_eq!(initial_stats.current_bytes, 0);

    // Shard A inserts block
    let tenant = TenantId(10);
    let policy = [1u8; 32];
    let arr1 = ArrangementId(101);
    let key1 = BlockCacheKey::new(tenant, policy, arr1, 1);
    shard1
        .storage_context()
        .unwrap()
        .put_block(key1.clone(), vec![0xAA; 512]);

    // Shard B accesses the exact same block through shared context
    let retrieved = shard2
        .storage_context()
        .unwrap()
        .get_block(&key1)
        .expect("cache hit across shards");
    assert_eq!(retrieved, vec![0xAA; 512]);

    let stats_after = shared_ctx.stats();
    assert_eq!(stats_after.hits, 1);
    assert!(stats_after.current_bytes > 512);
    assert!(stats_after.current_bytes <= capacity);
}

#[test]
fn test_cache_key_isolation_across_tenants_and_arrangements() {
    let ctx = WorkerStorageContext::new(512 * 1024);

    let t1 = TenantId(1);
    let t2 = TenantId(2);
    let policy1 = [0u8; 32];
    let mut policy2 = [0u8; 32];
    policy2[0] = 0xFF;

    let arr1 = ArrangementId(10);
    let arr2 = ArrangementId(20);
    let block_id = 42;

    let key_t1_a1 = BlockCacheKey::new(t1, policy1, arr1, block_id);
    let key_t2_a1 = BlockCacheKey::new(t2, policy1, arr1, block_id);
    let key_t1_p2 = BlockCacheKey::new(t1, policy2, arr1, block_id);
    let key_t1_a2 = BlockCacheKey::new(t1, policy1, arr2, block_id);

    // Insert for (t1, policy1, arr1)
    ctx.put_block(key_t1_a1.clone(), b"isolated_payload".to_vec());

    // 1. Same block_id but different tenant -> None
    assert_eq!(ctx.get_block(&key_t2_a1), None);

    // 2. Same block_id and tenant, but different security policy -> None
    assert_eq!(ctx.get_block(&key_t1_p2), None);

    // 3. Same block_id and tenant and policy, but different arrangement -> None
    assert_eq!(ctx.get_block(&key_t1_a2), None);

    // 4. Exact matching key -> Some
    assert_eq!(
        ctx.get_block(&key_t1_a1),
        Some(b"isolated_payload".to_vec())
    );
}
