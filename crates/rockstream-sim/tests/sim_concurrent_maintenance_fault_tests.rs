//! Simulated Concurrent Maintenance & Fencing Fault Injection Tests (v0.65.1 / Phase 3b).
//!
//! Validates:
//! 1. Concurrent view branch scheduling determinism under simulated task stalls and faults.
//! 2. Zero torn view dependency states when a branch task faults or is cancelled.
//! 3. Shard owner lease fencing preventing stale workers from enqueuing or staging commits.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rockstream_ops::branch_scheduler::{BranchScheduler, FnExecutor, ViewDependencyGraph};
use rockstream_ops::error::OpError;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::shard_actor::{ShardActorError, ShardActorRegistry};
use rockstream_sim::SimRuntime;
use rockstream_types::data_plane::RuntimeExchangeMessage;
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkloadId};

#[tokio::test]
async fn test_sim_concurrent_maintenance_and_fencing() {
    let rt = SimRuntime::new(0x51A1_0651);

    // ── 1. Simulated Fault Injection during Concurrent Branch Execution ─────
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v1", vec!["t".into()]);
    graph.add_view("v2", vec!["t".into()]);
    graph.add_view("v3", vec!["v1".into(), "v2".into()]);

    let scheduler = BranchScheduler::with_concurrency(4);

    let inject_fault = rt.random_u64().is_multiple_of(2);
    let fault_branch = if inject_fault { "v2" } else { "none" };

    let v1_completed = Arc::new(AtomicBool::new(false));
    let v2_completed = Arc::new(AtomicBool::new(false));
    let v3_completed = Arc::new(AtomicBool::new(false));

    let v1_done = v1_completed.clone();
    let v2_done = v2_completed.clone();
    let v3_done = v3_completed.clone();

    let executor = Arc::new(FnExecutor(
        move |view_name: &str, inputs: HashMap<String, ArrowZSet>| {
            let v1_done = v1_done.clone();
            let v2_done = v2_done.clone();
            let v3_done = v3_done.clone();
            let name = view_name.to_string();
            async move {
                if name == fault_branch {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    return Err(OpError::internal("simulated branch fault"));
                }
                if name == "v1" {
                    v1_done.store(true, Ordering::SeqCst);
                    Ok(inputs.get("t").cloned().unwrap())
                } else if name == "v2" {
                    v2_done.store(true, Ordering::SeqCst);
                    Ok(inputs.get("t").cloned().unwrap())
                } else if name == "v3" {
                    v3_done.store(true, Ordering::SeqCst);
                    Ok(inputs.get("v1").cloned().unwrap())
                } else {
                    Ok(ArrowZSet::from_ab_rows(&[(1, 10)], 1))
                }
            }
        },
    ));

    let mut source_deltas = HashMap::new();
    source_deltas.insert("t".into(), ArrowZSet::from_ab_rows(&[(1, 10)], 1));

    let result = scheduler
        .execute_epoch(&graph, source_deltas, executor)
        .await;

    if inject_fault {
        assert!(
            result.is_err(),
            "epoch must fail and cancel when simulated fault is injected in branch"
        );
        // Dependent view V3 must never have completed because V2 failed
        assert!(
            !v3_completed.load(Ordering::SeqCst),
            "v3 must not publish when upstream v2 faulted"
        );
    } else {
        assert!(result.is_ok(), "epoch must succeed when no faults injected");
        assert!(v1_completed.load(Ordering::SeqCst));
        assert!(v2_completed.load(Ordering::SeqCst));
        assert!(v3_completed.load(Ordering::SeqCst));
    }

    // ── 2. Simulated Stale-Owner Lease Fencing ──────────────────────────────
    let registry = ShardActorRegistry::new();
    let shard_id = ShardId(rt.random_u64());
    let initial_lease = LeaseToken(rt.random_u64().max(1));
    let renewed_lease = LeaseToken(initial_lease.0 + 1);

    registry.register(shard_id, initial_lease, Arc::new(|_| Box::pin(async {})));

    // New owner takes over lease
    registry.register(shard_id, renewed_lease, Arc::new(|_| Box::pin(async {})));

    // Stale message with old lease token must be fenced
    let stale_msg = RuntimeExchangeMessage {
        version: 1,
        request_id: "sim_stale_1".to_string(),
        workload_id: WorkloadId(1),
        shard_id,
        epoch: 1,
        operator_id: OperatorId(1),
        lease_token: initial_lease,
        source: "sim".to_string(),
        rows: vec![],
    };

    assert_eq!(
        registry.enqueue(stale_msg),
        Err(ShardActorError::StaleLease(shard_id)),
        "stale owner frame must be fenced"
    );

    // Current lease message succeeds
    let current_msg = RuntimeExchangeMessage {
        version: 1,
        request_id: "sim_current_1".to_string(),
        workload_id: WorkloadId(1),
        shard_id,
        epoch: 1,
        operator_id: OperatorId(1),
        lease_token: renewed_lease,
        source: "sim".to_string(),
        rows: vec![],
    };

    assert!(
        registry.enqueue(current_msg).is_ok(),
        "frame with current lease must succeed"
    );
}
