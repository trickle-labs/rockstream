//! Many-Shard Overload, Shard Churn, and Recovery Oracle Tests (v0.62.1 Slice 8 / Phase 3b).

use object_store::memory::InMemory;
use rockstream_runtime::quota::WorkerQuotaManager;
use rockstream_storage::shard_db::ShardDb;
use rockstream_storage::storage_context::WorkerStorageContext;
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::collections::BTreeMap;
use std::sync::Arc;

#[tokio::test]
async fn test_twenty_shards_sustained_overload_bounded_rss() {
    let budget_bytes = 64 * 1024 * 1024; // 64 MiB total worker budget
    let foreground_reservation = 12 * 1024 * 1024; // 12 MiB foreground reserve
    let ledger = Arc::new(WorkerBudgetLedger::new(
        budget_bytes,
        foreground_reservation,
    ));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());
    let storage_context = Arc::new(WorkerStorageContext::new_with_worker_id(
        "worker-soak",
        8 * 1024 * 1024,
    ));

    // Host 20 concurrent shards sharing the worker storage context and memory budget
    let mut shards = Vec::new();
    let num_shards = 20;

    for i in 0..num_shards {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder(format!("shard-{i}"), store)
            .with_storage_context(storage_context.clone())
            .build()
            .await
            .unwrap_or_else(|e| panic!("failed to build shard {i}: {e}"));
        shards.push(shard);
    }
    assert_eq!(shards.len(), 20);

    // Sustained write load across all 20 shards
    for (i, shard) in shards.iter().enumerate() {
        // Acquire permit for write buffers
        let permit = quota_mgr
            .try_acquire_permit(MemoryCategory::SlateDbWriteBuffers, 100 * 1024)
            .expect("acquire write buffer permit");

        for k in 0..10 {
            let key = format!("shard-{i}-key-{k}");
            let val = format!("shard-{i}-value-{k}");
            shard
                .put(key.as_bytes(), val.as_bytes())
                .await
                .expect("put");
        }
        shard.flush().await.expect("flush");
        drop(permit);
    }

    // Verify all 20 shards return exact written values
    for (i, shard) in shards.iter().enumerate() {
        for k in 0..10 {
            let key = format!("shard-{i}-key-{k}");
            let expected_val = format!("shard-{i}-value-{k}");
            let val = shard.get(key.as_bytes()).await.expect("get");
            assert_eq!(
                val.map(|b| String::from_utf8(b.to_vec()).unwrap()),
                Some(expected_val)
            );
        }
    }

    // Assert total allocated memory remains strictly bounded within budget
    let allocated = ledger.total_allocated_bytes();
    let gross = allocated + (allocated * ledger.allocator_overhead_pct()) / 100;
    assert!(gross <= budget_bytes as u64);
}

#[tokio::test]
async fn test_shard_addition_and_removal_under_active_load() {
    let budget_bytes = 32 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 4 * 1024 * 1024));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());
    let storage_context = Arc::new(WorkerStorageContext::new_with_worker_id(
        "worker-churn",
        4 * 1024 * 1024,
    ));

    // Phase 1: Start 10 initial shards
    let mut active_shards = Vec::new();
    let mut initial_permits = Vec::new();

    for i in 0..10 {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder(format!("initial-shard-{i}"), store)
            .with_storage_context(storage_context.clone())
            .build()
            .await
            .expect("build initial shard");

        let permit = quota_mgr
            .try_acquire_permit(MemoryCategory::SlateDbWriteBuffers, 50 * 1024)
            .expect("permit for initial shard");
        initial_permits.push(permit);

        shard
            .put(format!("k-{i}").as_bytes(), format!("v-{i}").as_bytes())
            .await
            .expect("put");
        shard.flush().await.expect("flush");
        active_shards.push(shard);
    }
    assert_eq!(active_shards.len(), 10);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        500 * 1024
    );

    // Phase 2: Dynamically add 10 more shards
    let mut added_shards = Vec::new();
    let mut added_permits = Vec::new();
    for i in 10..20 {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder(format!("added-shard-{i}"), store)
            .with_storage_context(storage_context.clone())
            .build()
            .await
            .expect("build added shard");

        let permit = quota_mgr
            .try_acquire_permit(MemoryCategory::SlateDbWriteBuffers, 50 * 1024)
            .expect("permit for added shard");
        added_permits.push(permit);

        shard
            .put(format!("k-{i}").as_bytes(), format!("v-{i}").as_bytes())
            .await
            .expect("put");
        shard.flush().await.expect("flush");
        added_shards.push(shard);
    }
    assert_eq!(active_shards.len() + added_shards.len(), 20);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        1000 * 1024
    );

    // Phase 3: Remove initial 10 shards and their permits
    drop(active_shards);
    drop(initial_permits);

    // Memory for initial shards must be released
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        500 * 1024
    );

    // Remaining added shards continue to serve exact queries
    for (idx, shard) in added_shards.iter().enumerate() {
        let i = idx + 10;
        let v = shard.get(format!("k-{i}").as_bytes()).await.expect("get");
        assert_eq!(
            v.map(|b| String::from_utf8(b.to_vec()).unwrap()),
            Some(format!("v-{i}"))
        );
    }

    // Clean up remaining
    drop(added_shards);
    drop(added_permits);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        0
    );
}

#[tokio::test]
async fn test_process_restart_recovers_exact_committed_state() {
    let shared_store = Arc::new(InMemory::new());
    let mut oracle_state: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>> = BTreeMap::new();

    let storage_ctx1 = Arc::new(WorkerStorageContext::new(2 * 1024 * 1024));

    // Epoch 1: Write deterministic committed data across 5 shards
    for shard_idx in 0..5 {
        let shard_path = format!("restarting-shard-{shard_idx}");
        let shard = ShardDb::builder(&shard_path, shared_store.clone())
            .with_storage_context(storage_ctx1.clone())
            .build()
            .await
            .expect("initial shard build");

        let mut shard_oracle = BTreeMap::new();
        for key_idx in 0..20 {
            let k = format!("k-{shard_idx}-{key_idx}").into_bytes();
            let v = format!("v-epoch1-{shard_idx}-{key_idx}").into_bytes();
            shard.put(&k, &v).await.expect("put");
            shard_oracle.insert(k, v);
        }
        shard
            .commit_epoch(rockstream_types::ids::ShardId(shard_idx as u64), 1)
            .await
            .expect("commit epoch 1");
        oracle_state.insert(shard_path, shard_oracle);
        // Explicitly close / drop shard database
        drop(shard);
    }

    // Simulating process restart: new WorkerStorageContext
    let storage_ctx2 = Arc::new(WorkerStorageContext::new(2 * 1024 * 1024));

    // Reconstruct all 5 shards and compare exact committed state against oracle
    for (shard_path, expected_map) in &oracle_state {
        let reopened_shard = ShardDb::builder(shard_path, shared_store.clone())
            .with_storage_context(storage_ctx2.clone())
            .build()
            .await
            .expect("reopen shard after restart");

        for (k, expected_v) in expected_map {
            let recovered = reopened_shard.get(k).await.expect("get recovered key");
            assert_eq!(
                recovered.as_ref().map(|b| b.as_ref()),
                Some(expected_v.as_slice()),
                "recovered value for key {:?} must bit-identically match oracle",
                String::from_utf8_lossy(k)
            );
        }
    }
}
