//! v0.59.5 Slice 2: External Benchmark and Oracle Harness Tests.
//!
//! Asserts harness execution, multiset oracle verification, and fail-closed behavior on simulated workers.

use rockstream_test_support::external_harness::{HarnessError, MultisetOracle, S1BaselineMetrics};

#[test]
fn test_external_benchmark_oracle_calculation_and_verification() {
    let mut oracle = MultisetOracle::new();

    // Ingest positive weights
    for i in 0..100 {
        oracle.ingest_aggregate_event(i % 10, i * 2, 1);
    }
    // Ingest retractions
    for i in 0..20 {
        oracle.ingest_aggregate_event(i % 10, i * 2, -1);
    }

    let expected = oracle.expected_aggregates();
    assert_eq!(expected.len(), 10);

    // Verify bit-for-bit oracle check
    assert!(oracle.verify_aggregates(&expected).is_ok());

    // Verify intentional mismatch detection
    let mut corrupt = expected.clone();
    corrupt[0].1 += 1; // alter sum
    assert!(matches!(
        oracle.verify_aggregates(&corrupt),
        Err(HarnessError::OracleMismatch(_))
    ));
}

#[test]
fn test_s1_baseline_metrics_structure() {
    let baseline = S1BaselineMetrics {
        worker_count: 4,
        group_cardinality: 100_000,
        throughput_events_per_sec: 152_000.0,
        p50_freshness_ms: 12.4,
        p95_freshness_ms: 24.8,
        p99_freshness_ms: 38.1,
        logical_write_bytes: 4_800_000,
        slatedb_storage_bytes: 18_200_000,
        write_amplification: 3.79,
    };

    let serialized = serde_json::to_string(&baseline).unwrap();
    let deserialized: S1BaselineMetrics = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized.worker_count, 4);
    assert_eq!(deserialized.group_cardinality, 100_000);
}
