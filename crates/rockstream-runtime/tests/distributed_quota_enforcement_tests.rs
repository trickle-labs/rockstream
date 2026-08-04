//! Integration tests for prospective worker-side quota enforcement and multi-worker isolation (v0.51.10).

use std::sync::Arc;
use std::time::Duration;
use rockstream_runtime::WorkerQuotaManager;
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::DistributedQuotaLedger;
use rockstream_types::view_lifecycle::ViewState;

/// Test 1: Prospective worker-side rejection for a hostile tenant allocating 10x MEMORY_LIMIT.
/// Proves that over-limit batches are rejected BEFORE batch memory allocation, producing `OverBudgetRejected`.
#[tokio::test]
async fn test_hostile_tenant_prospective_rejection_multi_worker() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let worker1_quota = WorkerQuotaManager::with_ledger(ledger.clone());
    let worker2_quota = WorkerQuotaManager::with_ledger(ledger.clone());

    let hostile_workload = WorkloadId(999);
    let memory_limit_bytes = 10_000u64;

    // Register hostile workload with 10k limit
    ledger.register_workload(hostile_workload, memory_limit_bytes, 8).unwrap();

    // Worker 1 allocates 8,000 bytes (within 10k limit)
    let _guard1 = worker1_quota
        .try_allocate_batch(hostile_workload, 8_000, 2)
        .expect("Worker 1 initial batch within limit must succeed");

    // Worker 2 attempts hostile allocation of 10x limit (100,000 bytes)
    let err = worker2_quota
        .try_allocate_batch(hostile_workload, 100_000, 2)
        .expect_err("Hostile allocation 10x limit must be rejected prospectively");

    // Handle prospective rejection and assert state transitions to OverBudgetRejected
    let state = worker2_quota.handle_prospective_rejection(hostile_workload, &err);
    assert_eq!(state, ViewState::OverBudgetRejected);
    assert_eq!(err.max_bytes, 10_000);
    assert_eq!(err.current_bytes, 8_000);
    assert_eq!(err.requested_bytes, 100_000);
    assert_eq!(ledger.total_rejections(), 1);
}

/// Test 2: Well-behaved tenant's freshness_slo is preserved under a noisy neighbor (hostile tenant).
/// Proves zero starvation for well-behaved tenant while hostile tenant undergoes prospective batch shedding.
#[tokio::test]
async fn test_well_behaved_tenant_slo_preserved_under_noisy_neighbor() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let worker = WorkerQuotaManager::with_ledger(ledger.clone());

    let well_behaved_workload = WorkloadId(1);
    let hostile_workload = WorkloadId(2);

    ledger.register_workload(well_behaved_workload, 50_000, 4).unwrap();
    ledger.register_workload(hostile_workload, 10_000, 4).unwrap();

    // Hostile tenant attempts continuous batch flooding (10x limit = 100,000 bytes)
    let mut hostile_rejections = 0;
    for _ in 0..50 {
        if worker.try_allocate_batch(hostile_workload, 100_000, 1).is_err() {
            hostile_rejections += 1;
        }
    }
    assert_eq!(hostile_rejections, 50, "All 50 hostile batch allocations must be prospectively rejected");

    // Well-behaved tenant executes workload within 1.0x limit (20,000 bytes out of 50,000)
    let start_time = std::time::Instant::now();
    let well_behaved_guard = worker
        .try_allocate_batch(well_behaved_workload, 20_000, 1)
        .expect("Well-behaved tenant allocation must succeed without starvation");
    let elapsed = start_time.elapsed();

    // Assert zero starvation (SLO latency target met, e.g. < 50ms)
    assert!(
        elapsed < Duration::from_millis(50),
        "Well-behaved tenant latency under noisy neighbor exceeded SLO: {:?}",
        elapsed
    );
    drop(well_behaved_guard);
}

/// Test 3: Cross-worker quota ledger coordination across multi-worker execution paths.
#[tokio::test]
async fn test_cross_worker_quota_coordination() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let workers: Vec<_> = (0..4)
        .map(|_| WorkerQuotaManager::with_ledger(ledger.clone()))
        .collect();

    let shared_workload = WorkloadId(100);
    ledger.register_workload(shared_workload, 40_000, 10).unwrap();

    // Each worker acquires 10,000 bytes
    let mut guards = Vec::new();
    for (i, w) in workers.iter().enumerate() {
        let guard = w
            .try_allocate_batch(shared_workload, 10_000, 1)
            .unwrap_or_else(|_| panic!("Worker {} allocation failed", i));
        guards.push(guard);
    }

    // Now total allocated = 40,000 bytes (at limit). Any additional allocation must be rejected.
    let extra_err = workers[0]
        .try_allocate_batch(shared_workload, 1, 1)
        .expect_err("Cross-worker quota boundary must be strictly enforced");
    assert_eq!(extra_err.current_bytes, 40_000);

    // Drop one guard (free 10,000 bytes)
    guards.pop();

    // Now allocation of 5,000 bytes succeeds
    let _new_guard = workers[3]
        .try_allocate_batch(shared_workload, 5_000, 1)
        .expect("Allocation after release must succeed across workers");
}
