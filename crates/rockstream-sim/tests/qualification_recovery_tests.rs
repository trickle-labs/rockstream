//! Synthetic recovery-observer and in-memory topology bookkeeping tests.
//! Durations and query outcomes are supplied fixtures, not application proof.

use std::time::Duration;

use rockstream_sim::qualification::{
    FaultInjector, FaultType, OracleAuditor, QualificationCluster, QualificationClusterConfig,
    QualificationWorkloadGenerator, RecoveryObservationType, RecoveryObserver,
};

#[tokio::test]
async fn test_helper_observer_heartbeat_loss_and_shard_reassignment() {
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

    observer.record_heartbeat_loss(6, Duration::from_secs(6));
    assert_eq!(
        observer.report().verify_slo(),
        Err("RS-0001 SLO violation: failure detection latency 6s exceeds limit 5s".into())
    );

    let mut observer = RecoveryObserver::new();
    observer.record_shard_reassignment(1, 6, 7, Duration::from_secs(31));
    assert_eq!(
        observer.report().verify_slo(),
        Err("RS-0001 SLO violation: shard reassignment latency 31s exceeds limit 30s".into())
    );
}

#[tokio::test]
async fn test_helper_observer_control_leader_failover() {
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

    observer.record_heartbeat_loss(fault.target_node_id.unwrap(), Duration::from_secs(6));
    assert_eq!(
        observer.report().verify_slo(),
        Err("RS-0001 SLO violation: failure detection latency 6s exceeds limit 5s".into())
    );
}

#[tokio::test]
async fn test_helper_observer_fencing_epoch_advancement() {
    let mut observer = RecoveryObserver::new();

    let res1 = observer.record_fencing_epoch(1, 2);
    assert!(res1.is_ok());

    let res2 = observer.record_fencing_epoch(2, 3);
    assert!(res2.is_ok());

    // Regressive epoch must be rejected
    let res3 = observer.record_fencing_epoch(3, 2);
    assert_eq!(
        res3,
        Err("RS-4015 Fencing epoch regression: old=3, new=2".into())
    );
}

#[tokio::test]
async fn test_helper_observer_selected_checkpoint_recovery() {
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

    let mut corrupted = report.clone();
    corrupted.observations[0].success = false;
    assert_eq!(
        corrupted.verify_slo(),
        Err("RS-0001 Recovery observation CheckpointRecovery failed: Restored checkpoint 42, discarded 3 uncommitted L0 files".into())
    );
}

#[tokio::test]
async fn test_helper_observer_source_offset_lsn_recovery() {
    let mut observer = RecoveryObserver::new();

    // Resumed from LSN 0x2000_0000 with catchup duration 12 seconds (<= 60s SLO)
    observer.record_source_lsn_resume("pg_cdc_orders", 0x2000_0000, Duration::from_secs(12));

    let report = observer.report();
    assert_eq!(report.freshness_recovery_latencies.len(), 1);
    assert!(report.verify_slo().is_ok());

    observer.record_source_lsn_resume("pg_cdc_orders", 0x2000_0000, Duration::from_secs(61));
    assert_eq!(
        observer.report().verify_slo(),
        Err("RS-0001 SLO violation: freshness recovery latency 61s exceeds limit 60s".into())
    );
}

#[tokio::test]
async fn test_helper_observer_view_frontier_monotonicity() {
    let mut observer = RecoveryObserver::new();

    assert!(observer.record_view_frontier("order_totals", 100).is_ok());
    assert!(observer.record_view_frontier("order_totals", 105).is_ok());
    assert!(observer.record_view_frontier("order_totals", 105).is_ok()); // Non-decreasing is OK

    // Regressive frontier must fail
    assert_eq!(
        observer.record_view_frontier("order_totals", 99),
        Err("RS-2018 View frontier regressed from 105 to 99".into())
    );
}

#[tokio::test]
async fn test_helper_observer_sink_2pc_transaction_atomicity() {
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

    let mut corrupted = report.clone();
    corrupted.observations[0].success = false;
    assert_eq!(
        corrupted.verify_slo(),
        Err("RS-0001 Recovery observation Sink2PcAtomicity failed: Sink topic order_events_sink epoch 10 commit_status: true".into())
    );
}

#[tokio::test]
async fn test_helper_observer_first_post_recovery_query_correctness() {
    let mut observer = RecoveryObserver::new();
    let mut oracle = OracleAuditor::new();
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0005);

    let batch = workload.generate_kafka_batch(100, 1, 0.0, 10);
    oracle.ingest(&batch);

    let mut fixture_results = oracle.expected_view_state().clone();

    // First post recovery query matches oracle
    let query_match = oracle.verify_multiset(&fixture_results).is_ok();
    assert!(observer
        .record_first_post_recovery_query("SELECT * FROM order_totals", query_match)
        .is_ok());

    // Corrupted first query response triggers failure
    if let Some(val) = fixture_results.values_mut().next() {
        *val += 1;
    }
    let query_mismatch = oracle.verify_multiset(&fixture_results).is_ok();
    assert_eq!(
        observer.record_first_post_recovery_query("SELECT * FROM order_totals", query_mismatch),
        Err("RS-0001 First post-recovery query `SELECT * FROM order_totals` did not match oracle result".into())
    );
}
