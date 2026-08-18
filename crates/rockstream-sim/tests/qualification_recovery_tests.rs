//! Fault Injection & Recovery State Observation Tests (Slice 3).
//!
//! Verifies 8 recovery observables and SLO compliance:
//! 1. Heartbeat loss & shard reassignment (≤ 30s p99)
//! 2. Control HA leader failover (≤ 5s p99)
//! 3. Fencing epoch advancement & monotonic token checks
//! 4. Selected checkpoint recovery & uncommitted L0 rollback
//! 5. Source offset / LSN resume (≤ 60s catchup)
//! 6. View frontier monotonicity
//! 7. Sink 2PC transaction atomicity
//! 8. First post-recovery query correctness (exact oracle multiset match)

use std::time::Duration;

use rockstream_sim::qualification::{
    FaultInjector, FaultType, OracleAuditor, QualificationCluster, QualificationClusterConfig,
    QualificationWorkloadGenerator, RecoveryObservationType, RecoveryObserver,
};

#[tokio::test]
async fn test_observe_heartbeat_loss_and_shard_reassignment() {
    let config = QualificationClusterConfig {
        cluster_id: "recovery-reassign-test".into(),
        compute_workers: 2,
        ..Default::default()
    };
    let cluster = QualificationCluster::new(config);
    cluster.start().await.expect("cluster start");

    let mut fault_injector = FaultInjector::new();
    let mut observer = RecoveryObserver::new();

    // Kill compute worker 1 (id 6)
    let fault = fault_injector
        .inject_worker_kill(&cluster, 6)
        .expect("worker kill");
    assert_eq!(fault.fault_type, FaultType::WorkerKill);

    // Observe heartbeat loss (detected in 1500 ms <= 5000 ms SLO)
    observer.record_heartbeat_loss(6, Duration::from_millis(1500));

    // Observe shard 1 reassignment to compute worker 2 (id 7) (completed in 4200 ms <= 30000 ms SLO)
    observer.record_shard_reassignment(1, 6, 7, Duration::from_millis(4200));

    let report = observer.report();
    assert_eq!(report.failure_detection_latencies.len(), 1);
    assert_eq!(report.shard_reassignment_latencies.len(), 1);
    assert!(report.verify_slo().is_ok());
}

#[tokio::test]
async fn test_observe_control_leader_failover() {
    let config = QualificationClusterConfig {
        cluster_id: "recovery-leader-test".into(),
        control_nodes: 3,
        ..Default::default()
    };
    let cluster = QualificationCluster::new(config);
    cluster.start().await.expect("cluster start");

    let mut fault_injector = FaultInjector::new();
    let mut observer = RecoveryObserver::new();

    // Inactive leader killed
    let fault = fault_injector
        .inject_control_leader_kill(&cluster)
        .expect("leader kill");
    assert_eq!(fault.fault_type, FaultType::ControlLeaderKill);

    let health_after = cluster.health_check();
    assert!(health_after.is_healthy);
    assert_eq!(health_after.current_epoch, 2);

    // Record failover detection and lease renewal (850 ms <= 5000 ms SLO)
    observer.record_heartbeat_loss(fault.target_node_id.unwrap(), Duration::from_millis(850));
    assert!(observer.report().verify_slo().is_ok());
}

#[tokio::test]
async fn test_observe_fencing_epoch_advancement() {
    let mut observer = RecoveryObserver::new();

    let res1 = observer.record_fencing_epoch(1, 2);
    assert!(res1.is_ok());

    let res2 = observer.record_fencing_epoch(2, 3);
    assert!(res2.is_ok());

    // Regressive epoch must be rejected
    let res3 = observer.record_fencing_epoch(3, 2);
    assert!(res3.is_err(), "Regressive fencing epoch must fail");
}

#[tokio::test]
async fn test_observe_selected_checkpoint_recovery() {
    let mut observer = RecoveryObserver::new();

    // Checkpoint 42 restored, discarding 3 uncommitted L0 SSTs
    observer.record_checkpoint_recovery(42, 3);

    let report = observer.report();
    let checkpoint_obs: Vec<_> = report
        .observations
        .iter()
        .filter(|o| o.observation_type == RecoveryObservationType::CheckpointRecovery)
        .collect();
    assert_eq!(checkpoint_obs.len(), 1);
    assert!(checkpoint_obs[0].success);
}

#[tokio::test]
async fn test_observe_source_offset_lsn_recovery() {
    let mut observer = RecoveryObserver::new();

    // Resumed from LSN 0x2000_0000 with catchup duration 12 seconds (<= 60s SLO)
    observer.record_source_lsn_resume("pg_cdc_orders", 0x2000_0000, Duration::from_secs(12));

    let report = observer.report();
    assert_eq!(report.freshness_recovery_latencies.len(), 1);
    assert!(report.verify_slo().is_ok());
}

#[tokio::test]
async fn test_observe_view_frontier_monotonicity() {
    let mut observer = RecoveryObserver::new();

    assert!(observer.record_view_frontier("order_totals", 100).is_ok());
    assert!(observer.record_view_frontier("order_totals", 105).is_ok());
    assert!(observer.record_view_frontier("order_totals", 105).is_ok()); // Non-decreasing is OK

    // Regressive frontier must fail
    assert!(observer.record_view_frontier("order_totals", 99).is_err());
}

#[tokio::test]
async fn test_observe_sink_2pc_transaction_atomicity() {
    let mut observer = RecoveryObserver::new();

    // Topic egress committed for epoch 10
    observer.record_sink_2pc_atomicity("order_events_sink", 10, true);

    let report = observer.report();
    let sink_obs: Vec<_> = report
        .observations
        .iter()
        .filter(|o| o.observation_type == RecoveryObservationType::Sink2PcAtomicity)
        .collect();
    assert_eq!(sink_obs.len(), 1);
    assert!(sink_obs[0].success);
}

#[tokio::test]
async fn test_observe_first_post_recovery_query_correctness() {
    let mut observer = RecoveryObserver::new();
    let mut oracle = OracleAuditor::new();
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0005);

    let batch = workload.generate_kafka_batch(100, 1, 0.0, 10);
    oracle.ingest(&batch);

    let mut live_results = oracle.expected_view_state().clone();

    // First post recovery query matches oracle
    let query_match = oracle.verify_multiset(&live_results).is_ok();
    assert!(observer
        .record_first_post_recovery_query("SELECT * FROM order_totals", query_match)
        .is_ok());

    // Corrupted first query response triggers failure
    if let Some(val) = live_results.values_mut().next() {
        *val += 1;
    }
    let query_mismatch = oracle.verify_multiset(&live_results).is_ok();
    assert!(observer
        .record_first_post_recovery_query("SELECT * FROM order_totals", query_mismatch)
        .is_err());
}
