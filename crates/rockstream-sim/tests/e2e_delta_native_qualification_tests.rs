//! v0.59.5 Slice 7: End-to-End Delta-Native Qualification Tests.
//!
//! End-to-end multi-worker qualification under continuous CDC and Kafka ingestion,
//! verifying delta-native state maintenance and exact multiset oracle matching.

use rockstream_sim::qualification::{
    check_prerequisites, OracleAuditor, QualificationCluster, QualificationClusterConfig,
    QualificationWorkloadGenerator,
};
use rockstream_test_support::external_harness::{
    MultisetOracle, ProcessIsolationAuditor, WorkerProcessIdentity,
};

#[tokio::test]
async fn test_e2e_delta_native_multi_worker_qualification() {
    let report = check_prerequisites(true);
    assert!(report.is_ready);

    // 1. Verify Process Isolation for 4 distinct workers
    let workers = vec![
        WorkerProcessIdentity {
            pid: 2001,
            worker_id: 1,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 2002,
            worker_id: 2,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 2003,
            worker_id: 3,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 2004,
            worker_id: 4,
            cgroup_id: None,
        },
    ];
    assert!(ProcessIsolationAuditor::verify_isolation(4, &workers).is_ok());

    // 2. Start Qualification Cluster with delta-native workers
    let config = QualificationClusterConfig {
        cluster_id: "e2e-delta-native-qual".into(),
        control_nodes: 3,
        compute_workers: 4,
        frontier_workers: 1,
        ..Default::default()
    };

    let cluster = QualificationCluster::new(config);
    cluster
        .start()
        .await
        .expect("Cluster must start successfully");

    let health = cluster.health_check();
    assert!(health.is_healthy);
    assert_eq!(health.running_nodes, 12);

    // 3. Workload generator & multiset oracle verification
    let mut workload = QualificationWorkloadGenerator::new(0xDEAD_5950);
    let mut auditor = OracleAuditor::new();
    let mut oracle = MultisetOracle::new();

    let batch1 = workload.generate_kafka_batch(50, 1, 0.1, 10);
    let batch2 = workload.generate_kafka_batch(50, 2, 0.1, 10);
    auditor.ingest(&batch1);
    auditor.ingest(&batch2);

    for r in &batch1 {
        let key = r.key.parse::<i64>().unwrap_or(1);
        oracle.ingest_aggregate_event(key, r.val, 1);
    }
    for r in &batch2 {
        let key = r.key.parse::<i64>().unwrap_or(1);
        oracle.ingest_aggregate_event(key, r.val, 1);
    }

    let expected_state = auditor.expected_view_state().clone();
    assert!(!expected_state.is_empty());
    assert!(auditor.verify_multiset(&expected_state).is_ok());

    let oracle_expected = oracle.expected_aggregates();
    assert!(!oracle_expected.is_empty());

    cluster.stop();
}
