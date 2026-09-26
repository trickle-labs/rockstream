//! Worker Storage Context Cache & Budget Tests (v0.59.6).
//!
//! Asserts block and index cache reuse across multiple views/shards,
//! high hit ratio on shared arrangements, and deterministic LRU eviction under memory budget.

use rockstream_storage::storage_context::{BlockCacheKey, DiskCacheStats, WorkerStorageContext};
use rockstream_types::ids::{ArrangementId, TenantId};
use rockstream_types::state_budget::{
    MemoryCategory, MemoryOwner, MemoryOwnerUsage, WorkerBudgetLedger,
};
use std::sync::Arc;

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

#[test]
fn disk_cache_status_reports_files_and_capacity_separately_from_ram() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_root = temp_dir.path().join("object-cache");
    std::fs::create_dir_all(cache_root.join("nested")).unwrap();
    std::fs::write(cache_root.join("first.sst"), b"abc").unwrap();
    std::fs::write(cache_root.join("nested/second.sst"), b"12345").unwrap();
    let ctx = WorkerStorageContext::new(1024).with_nvme_cache(&cache_root, 64);
    let key = BlockCacheKey::new(TenantId(1), [0; 32], ArrangementId(1), 1);
    ctx.put_block(key, b"ram-block".to_vec());

    assert_eq!(
        ctx.disk_cache_stats().unwrap(),
        Some(DiskCacheStats {
            used_bytes: 8,
            capacity_bytes: 64,
        })
    );
    assert_eq!(
        ctx.stats().current_bytes,
        b"ram-block".len() + std::mem::size_of::<BlockCacheKey>() + 16
    );
    assert_eq!(ctx.stats().capacity_bytes, 1024);
}

#[test]
fn worker_slate_db_cache_capacity_is_charged_once_to_distinct_categories() {
    let ledger = Arc::new(WorkerBudgetLedger::new(4096, 0));
    let context = Arc::new(
        WorkerStorageContext::new_with_worker_id_and_budget("worker-7", 1024, &ledger).unwrap(),
    );
    let _shared_by_shard = context.clone();
    let status = ledger.status();

    assert_eq!(status.allocated_bytes, 1024);
    assert_eq!(
        status.owners,
        vec![
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbBlockCache,
                owner: MemoryOwner::worker("worker-7"),
                bytes: 716,
            },
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbMetadataCache,
                owner: MemoryOwner::worker("worker-7"),
                bytes: 308,
            },
        ]
    );

    drop(context);
    assert_eq!(ledger.status().allocated_bytes, 1024);
    drop(_shared_by_shard);
    let released = ledger.status();
    assert_eq!(released.allocated_bytes, 0);
    assert_eq!(released.owners, Vec::<MemoryOwnerUsage>::new());
}

#[tokio::test]
async fn worker_slate_db_write_buffer_is_charged_until_last_db_handle_drops() {
    let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
    let context = Arc::new(
        WorkerStorageContext::new_with_worker_id_and_budget("worker-7", 512 * 1024 * 1024, &ledger)
            .unwrap(),
    );
    let db = rockstream_storage::ShardDb::builder(
        "worker-7/shard-1",
        Arc::new(object_store::memory::InMemory::new()),
    )
    .with_storage_context(context)
    .build()
    .await
    .unwrap();
    let shared_db = db.clone();
    let expected_bytes =
        rockstream_storage::storage_context::WORKER_SLATEDB_WRITE_BUFFER_CAPACITY_BYTES as u64;

    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        expected_bytes
    );
    assert_eq!(
        ledger
            .status()
            .owners
            .into_iter()
            .filter(|usage| usage.category == MemoryCategory::SlateDbWriteBuffers)
            .collect::<Vec<_>>(),
        vec![MemoryOwnerUsage {
            category: MemoryCategory::SlateDbWriteBuffers,
            owner: MemoryOwner::worker("worker-7"),
            bytes: expected_bytes,
        }]
    );

    drop(db);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        expected_bytes
    );
    drop(shared_db);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        0
    );
}

#[tokio::test]
async fn worker_slate_db_write_buffer_rejects_before_open_without_leaking() {
    let ledger = Arc::new(WorkerBudgetLedger::new(600 * 1024 * 1024, 0));
    let context = Arc::new(
        WorkerStorageContext::new_with_worker_id_and_budget("worker-7", 512 * 1024 * 1024, &ledger)
            .unwrap(),
    );
    let error = match rockstream_storage::ShardDb::builder(
        "worker-7/shard-1",
        Arc::new(object_store::memory::InMemory::new()),
    )
    .with_storage_context(context)
    .build()
    .await
    {
        Ok(_) => panic!("database opened without write-buffer memory headroom"),
        Err(error) => error,
    };

    assert_eq!(
        error.to_string(),
        "RS-5003: state budget exceeded for 'worker-budget-slatedb_write_buffers': current=536870912 bytes, requested=67108864 bytes, limit=629145600 bytes"
    );
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        0
    );
    assert_eq!(ledger.status().allocated_bytes, 0);
    assert!(ledger.status().owners.is_empty());
}
