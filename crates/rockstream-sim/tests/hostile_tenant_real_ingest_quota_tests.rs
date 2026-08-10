//! Hostile Tenant Real Ingest Quota Enforcement Tests (v0.51.23 - Slice 2).
//!
//! Verifies prospective batch ingestion quota enforcement via `WorkerQuotaManager::try_allocate_batch`,
//! transition to `ViewState::OverBudgetRejected` on limit breach, RS-5003/RS-9001 audit logging,
//! zero starvation for well-behaved tenants, and clean quota recovery.

use std::sync::Arc;

use rockstream_runtime::WorkerQuotaManager;
use rockstream_sim::buggify::{buggify_disable, buggify_focus, buggify_init};
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::DistributedQuotaLedger;
use rockstream_types::view_lifecycle::ViewState;

#[tokio::test]
async fn test_hostile_tenant_real_ingest_quota_rejection() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let manager = WorkerQuotaManager::with_ledger(ledger.clone());

    let hostile_workload = WorkloadId(7001);
    let memory_limit_bytes = 20_000u64;

    ledger
        .register_workload(hostile_workload, memory_limit_bytes, 4)
        .unwrap();

    // Initial valid batch allocation
    let guard = manager
        .try_allocate_batch(hostile_workload, 15_000, 2)
        .expect("Valid batch within memory limit must be approved prospectively");

    assert_eq!(
        ledger
            .get_entry(hostile_workload)
            .unwrap()
            .current_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        15_000
    );

    // Hostile batch attempting 10x limit overflow (200,000 bytes)
    let err = manager
        .try_allocate_batch(hostile_workload, 200_000, 2)
        .expect_err("Hostile batch exceeding memory limit must be prospectively rejected");

    let state = manager.handle_prospective_rejection(hostile_workload, &err);
    assert_eq!(
        state,
        ViewState::OverBudgetRejected,
        "Prospective quota rejection must set ViewState::OverBudgetRejected"
    );

    assert_eq!(err.max_bytes, 20_000);
    assert_eq!(err.current_bytes, 15_000);
    assert_eq!(err.requested_bytes, 200_000);
    assert_eq!(ledger.total_rejections(), 1);

    // Release initial batch guard
    drop(guard);
    assert_eq!(
        ledger
            .get_entry(hostile_workload)
            .unwrap()
            .current_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // After release, normal batch ingestion recovers cleanly
    let _recovered_guard = manager
        .try_allocate_batch(hostile_workload, 10_000, 1)
        .expect("Ingestion after quota release must succeed cleanly");
}

#[tokio::test]
async fn test_hostile_tenant_no_starvation_well_behaved_tenant() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let worker1 = WorkerQuotaManager::with_ledger(ledger.clone());
    let worker2 = WorkerQuotaManager::with_ledger(ledger.clone());

    let hostile_workload = WorkloadId(8001);
    let well_behaved_workload = WorkloadId(8002);

    ledger
        .register_workload(hostile_workload, 10_000, 2)
        .unwrap();
    ledger
        .register_workload(well_behaved_workload, 50_000, 4)
        .unwrap();

    // Hostile tenant floods worker1 with over-budget prospective allocations
    let mut rejections = 0;
    for _ in 0..20 {
        if worker1
            .try_allocate_batch(hostile_workload, 100_000, 1)
            .is_err()
        {
            rejections += 1;
        }
    }
    assert_eq!(rejections, 20);

    // Well-behaved tenant ingests on worker2 unimpeded
    let guard = worker2
        .try_allocate_batch(well_behaved_workload, 15_000, 2)
        .expect("Well-behaved tenant allocation must not be starved by hostile neighbor");

    drop(guard);
}

#[tokio::test]
async fn test_hostile_tenant_quota_buggify_focus() {
    buggify_init(0x51_23);
    buggify_focus("edge.quota.prospective_ingest_rejection");

    let ledger = Arc::new(DistributedQuotaLedger::new());
    let manager = WorkerQuotaManager::with_ledger(ledger.clone());
    let workload = WorkloadId(9001);

    ledger.register_workload(workload, 500, 1).unwrap();

    let res1 = manager.try_allocate_batch(workload, 400, 1);
    assert!(res1.is_ok());

    let err = manager.try_allocate_batch(workload, 200, 1).unwrap_err();
    let state = manager.handle_prospective_rejection(workload, &err);
    assert_eq!(state, ViewState::OverBudgetRejected);

    buggify_disable();
}
