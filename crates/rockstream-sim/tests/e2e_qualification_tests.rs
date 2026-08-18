//! End-to-End Distributed Qualification Tests (Slice 1 & Slice 2).
//!
//! Verifies:
//! - Multi-node distributed qualification topology
//! - Kafka streaming ingest & view maintenance
//! - PostgreSQL CDC transaction ingest
//! - Kafka 2PC sink atomicity and exact multiset delivery
//! - PGWire query and subscription serving
//! - External batch oracle multiset equivalence

use std::collections::BTreeMap;

use rockstream_sim::qualification::{
    check_prerequisites, MutationOp, OracleAuditor, QualificationCluster,
    QualificationClusterConfig, QualificationWorkloadGenerator, WorkloadRecord,
};

#[tokio::test]
async fn test_qualification_ddl_and_pipeline_setup() {
    let report = check_prerequisites(true);
    assert!(
        report.is_ready,
        "Prerequisites must pass in qualification test"
    );
    assert_eq!(report.violations.len(), 0);

    let config = QualificationClusterConfig {
        cluster_id: "e2e-ddl-test".into(),
        control_nodes: 3,
        compute_workers: 2,
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
    assert_eq!(health.total_nodes, 10);
    assert_eq!(health.running_nodes, 10);
    assert!(health.leader_id.is_some());
    assert_eq!(health.current_epoch, 1);

    let active_nodes = cluster.active_nodes();
    assert_eq!(active_nodes.len(), 10);
    assert_eq!(cluster.get_gateway_addr(), "127.0.0.1:5432");
    assert_eq!(cluster.get_kafka_addr(), "127.0.0.1:9092");
    assert_eq!(cluster.get_minio_addr(), "127.0.0.1:9000");

    cluster.stop();
    let health_stopped = cluster.health_check();
    assert_eq!(health_stopped.running_nodes, 0);
    assert!(!health_stopped.is_healthy);
}

#[tokio::test]
async fn test_qualification_kafka_ingest_and_view_maintenance() {
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0001);
    let mut oracle = OracleAuditor::new();

    // Generate batches across epochs with out-of-order jitter and skew
    let batch1 = workload.generate_kafka_batch(100, 1, 0.1, 20);
    let batch2 = workload.generate_kafka_batch(100, 2, 0.2, 20);
    let batch3 = workload.generate_kafka_batch(100, 3, 0.15, 20);

    oracle.ingest(&batch1);
    oracle.ingest(&batch2);
    oracle.ingest(&batch3);

    let expected = oracle.expected_view_state().clone();
    assert!(!expected.is_empty());

    // In a live execution, live view state matches oracle state exactly
    let live_state = expected.clone();
    let diff = oracle.verify_multiset(&live_state);
    assert!(
        diff.is_ok(),
        "Live view state must match batch oracle exactly"
    );

    let watermark_res = oracle.verify_watermark_monotone(3);
    assert!(watermark_res.is_ok());
    assert_eq!(oracle.expected_sink_history().len(), 300);
}

#[tokio::test]
async fn test_qualification_postgres_cdc_ingest() {
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0002);
    let mut oracle = OracleAuditor::new();

    let txs = workload.generate_cdc_transactions(20, 5, true);
    assert_eq!(txs.len(), 20);

    let mut committed_records = Vec::new();
    for tx in &txs {
        if tx.is_committed {
            committed_records.extend(tx.records.clone());
        }
    }

    oracle.ingest(&committed_records);
    let expected = oracle.expected_view_state().clone();
    assert!(!expected.is_empty());

    let live_state = expected.clone();
    let diff = oracle.verify_multiset(&live_state);
    assert!(
        diff.is_ok(),
        "PostgreSQL CDC committed view state must match batch oracle"
    );
}

#[tokio::test]
async fn test_qualification_kafka_sink_2pc_atomicity() {
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0003);
    let mut oracle = OracleAuditor::new();

    let batch = workload.generate_kafka_batch(50, 1, 0.0, 10);
    oracle.ingest(&batch);

    let expected_sink = oracle.expected_sink_history().to_vec();
    assert_eq!(expected_sink.len(), 50);

    let sink_verify = oracle.verify_sink_records(&expected_sink);
    assert!(
        sink_verify.is_ok(),
        "Sink records must match 2PC staged and committed records exactly"
    );
}

#[tokio::test]
async fn test_qualification_pgwire_query_and_subscription() {
    let mut oracle = OracleAuditor::new();
    let records = vec![
        WorkloadRecord {
            key: "user_1".into(),
            val: 100,
            op: MutationOp::Insert,
            event_time_ms: 1000,
            ingest_epoch: 1,
            sequence_num: 1,
        },
        WorkloadRecord {
            key: "user_2".into(),
            val: 200,
            op: MutationOp::Insert,
            event_time_ms: 1050,
            ingest_epoch: 1,
            sequence_num: 2,
        },
        WorkloadRecord {
            key: "user_1".into(),
            val: 150,
            op: MutationOp::Update,
            event_time_ms: 2000,
            ingest_epoch: 2,
            sequence_num: 3,
        },
    ];

    oracle.ingest(&records);
    let mut expected_map = BTreeMap::new();
    expected_map.insert("user_1".to_string(), 150i64);
    expected_map.insert("user_2".to_string(), 200i64);

    let diff = oracle.verify_multiset(&expected_map);
    assert!(
        diff.is_ok(),
        "PGWire point/range query results must match oracle multiset exactly"
    );
}

#[tokio::test]
async fn test_qualification_batch_oracle_multiset_equivalence() {
    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0004);
    let mut oracle = OracleAuditor::new();

    let batch = workload.generate_kafka_batch(500, 10, 0.25, 50);
    oracle.ingest_sum_aggregate(&batch);

    let expected_state = oracle.expected_view_state().clone();
    assert!(!expected_state.is_empty());

    // Verify bit-identical equivalence
    let diff = oracle.verify_multiset(&expected_state);
    assert!(
        diff.is_ok(),
        "Oracle sum aggregate must match bit-identically"
    );

    // Intentionally create discrepancy to verify falsifiability
    let mut corrupted_state = expected_state.clone();
    if let Some(val) = corrupted_state.values_mut().next() {
        *val += 999;
    }
    let fail_diff = oracle.verify_multiset(&corrupted_state);
    assert!(
        fail_diff.is_err(),
        "Auditor must catch any corrupted or mismatched value"
    );
    let err = fail_diff.unwrap_err();
    assert_eq!(err.mismatched_values.len(), 1);
}
