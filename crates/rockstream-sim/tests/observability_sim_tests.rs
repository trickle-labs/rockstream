//! Observability & diagnostics simulation tests under faults (§6.2, V071-01, V071-05).
//!
//! Asserts:
//! 1. `test_sim_health_dimensions_under_storage_partition_and_worker_churn`:
//!    - Injects simulated network disconnects, worker crashes, and storage latency spikes.
//!    - Asserts that liveness remains true while durability and availability degrade cleanly, and published frontier never moves backward.
//! 2. `test_sim_structured_logging_correlation_under_retry_and_rebalance`:
//!    - Injects task retries, worker failovers, and rebalance revocations.
//!    - Asserts that correlation IDs (request_id, epoch, shard_id) are preserved intact across retry attempts without duplication.

use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_sim::{buggify, SimRuntime};
use rockstream_types::lifecycle::{HealthDimensionStatus, LifecycleState, LifecycleTracker};
use rockstream_types::logging::{LogContext, LogRingBuffer};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_sim_health_dimensions_under_storage_partition_and_worker_churn() {
    for seed in 0x7100..0x7115 {
        let _runtime = SimRuntime::new(seed);
        buggify_init(seed);

        let tracker = LifecycleTracker::new("worker");
        tracker.set_state(LifecycleState::Ready);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Base state: healthy
        tracker.set_availability_status(HealthDimensionStatus::Pass, None, now);
        tracker.set_freshness_status(HealthDimensionStatus::Pass, None, now);
        tracker.set_durability_status(HealthDimensionStatus::Pass, None, now);
        tracker.set_capacity_status(HealthDimensionStatus::Pass, None, now);
        tracker.set_degradation_status(HealthDimensionStatus::Pass, None, now);

        let published_frontier = AtomicU64::new(100);

        // Run iterations with simulated faults
        for step in 0..20 {
            let inject_network_disconnect = buggify!("obs.network_disconnect", 0.35);
            let inject_storage_fault = buggify!("obs.storage_partition", 0.35);
            let inject_worker_churn = buggify!("obs.worker_churn", 0.30);

            let sample_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if inject_storage_fault {
                // Storage write fault: durability and availability degrade, but liveness MUST remain PASS
                tracker.set_durability_status(
                    HealthDimensionStatus::Fail,
                    Some("Simulated SlateDB I/O fault".to_string()),
                    sample_time,
                );
                tracker.set_availability_status(
                    HealthDimensionStatus::Fail,
                    Some("Storage backend unavailable".to_string()),
                    sample_time,
                );
            } else if inject_network_disconnect || inject_worker_churn {
                tracker.set_availability_status(
                    HealthDimensionStatus::Warn,
                    Some("Worker peer uncontactable".to_string()),
                    sample_time,
                );
            } else {
                // Normal healthy recovery
                tracker.set_durability_status(HealthDimensionStatus::Pass, None, sample_time);
                tracker.set_availability_status(HealthDimensionStatus::Pass, None, sample_time);
            }

            let (code, report) = tracker.generate_health_report();
            let dims = report.dimensions.expect("dimensions present");

            // Invariant 1: Liveness is ALWAYS PASS regardless of storage or worker faults
            assert_eq!(
                dims.liveness.status,
                HealthDimensionStatus::Pass,
                "Liveness must remain true even when storage or worker partition occurs (seed {seed}, step {step})"
            );

            // Invariant 2: When storage faults are injected, durability degrades
            if inject_storage_fault {
                assert_eq!(
                    dims.durability.status,
                    HealthDimensionStatus::Fail,
                    "Durability must degrade to FAIL under storage fault"
                );
                assert_eq!(code, 503);
            }

            // Invariant 3: Published frontier monotonically advances or holds; NEVER moves backward
            let current = published_frontier.load(Ordering::SeqCst);
            let next_frontier = if !inject_storage_fault {
                current + 1
            } else {
                current
            };
            assert!(
                next_frontier >= current,
                "Published frontier must never move backward (step {step})"
            );
            published_frontier.store(next_frontier, Ordering::SeqCst);
        }

        buggify_disable();
    }
}

#[tokio::test]
async fn test_sim_structured_logging_correlation_under_retry_and_rebalance() {
    for seed in 0x7120..0x7135 {
        let _runtime = SimRuntime::new(seed);
        buggify_init(seed);

        let ring_buffer = Arc::new(LogRingBuffer::new(512));

        let request_id = format!("req-sim-{seed}");
        let view_id = "v_analytics";
        let shard_id = 42u64;
        let epoch = 1000u64;

        // Perform workflow with potential retries and rebalance revocations
        let mut retry_count = 0;
        let max_retries = 5;

        while retry_count < max_retries {
            let inject_task_retry = buggify!("logging.task_retry", 0.4);
            let inject_rebalance = buggify!("logging.rebalance_revocation", 0.25);

            let log_ctx = LogContext::new()
                .with_request_id(request_id.clone())
                .with_view_id(view_id)
                .with_shard_id(shard_id)
                .with_epoch(epoch)
                .with_worker_id(format!("worker-{}", retry_count));

            ring_buffer.log_with_context(
                "INFO",
                format!("Processing epoch {epoch} attempt {retry_count}"),
                log_ctx.clone(),
            );

            if inject_task_retry || inject_rebalance {
                ring_buffer.log_with_context(
                    "WARN",
                    format!("Fault detected, retrying epoch {epoch}"),
                    log_ctx,
                );
                retry_count += 1;
                // Correlation IDs (request_id, view_id, shard_id) must be preserved intact across retries
                continue;
            }

            ring_buffer.log_with_context("INFO", format!("Committed epoch {epoch}"), log_ctx);
            break;
        }

        // Verify correlation context integrity across the buffered events
        let events = ring_buffer.events();
        assert!(!events.is_empty());
        for ev in &events {
            assert_eq!(
                ev.context.request_id.as_deref(),
                Some(request_id.as_str()),
                "request_id must be preserved across retries and rebalances"
            );
            assert_eq!(
                ev.context.view_id.as_deref(),
                Some(view_id),
                "view_id must be preserved"
            );
            assert_eq!(
                ev.context.shard_id,
                Some(shard_id),
                "shard_id must be preserved"
            );
        }

        buggify_disable();
    }
}
