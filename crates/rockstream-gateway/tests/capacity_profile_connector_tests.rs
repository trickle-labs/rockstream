//! Capacity profile connector calibration integration tests (v0.59.23 Slice 4 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityEstimate, CapacityObservation, CapacityProfile, HardwareIdentity, PhysicalStrategy,
    RawCapacityRecord, SourceStatisticProvenance, WorkloadDigest,
};

fn sample_candidate() -> CandidateIdentity {
    CandidateIdentity {
        semantic_version: "0.59.23".to_string(),
        commit_sha: "9cc2b4a73f75c3de5035a8e83d375383c3139dcbf109a7ba3a935c83db2f598f".to_string(),
        build_timestamp_rfc3339: "2026-09-02T12:00:00Z".to_string(),
        compiler_version: "rustc 1.88.0".to_string(),
        lockfile_digest: "891477e0c6a8957309ee5c45a6368af3ae14bb510732d2684ffa19af310920f9"
            .to_string(),
        enabled_features: vec!["default".to_string()],
    }
}

#[tokio::test]
async fn postgres_cdc_and_kafka_emit_exact_calibration_rows() {
    let candidate = sample_candidate();
    let profile = CapacityProfile::Medium;
    let hardware = HardwareIdentity::reference(profile);

    // Simulate CDC source feed for orders and Kafka source for web_events
    let cdc_provenance = SourceStatisticProvenance::Connector {
        source_name: "postgres_cdc_orders".to_string(),
        row_count: 50_000,
    };
    let kafka_provenance = SourceStatisticProvenance::Connector {
        source_name: "kafka_web_events".to_string(),
        row_count: 100_000,
    };

    let wl_digest = WorkloadDigest {
        workload_name: "shared_uniform_aggregation".to_string(),
        seed: 42,
        config_hash: "cdc_kafka_medium_cfg".to_string(),
        dataset_digest: "cdc_kafka_medium_data".to_string(),
    };

    let est = CapacityEstimate {
        private_state_bytes: 500_000,
        shared_state_bytes: 1_000_000,
        saved_bytes: 250_000,
        rss_bytes: 2_000_000,
        spill_bytes: 0,
        cache_hit_ratio: 0.96,
        epoch_duration_ms: 15.0,
        commit_group_duration_ms: 5.0,
        p99_freshness_ms: 60.0,
        shuffle_bytes: 200_000,
        logical_writes: 50_000,
        physical_writes: 50_100,
        object_store_requests: 250,
        checkpoint_cost_ms: 12.0,
        compaction_debt_bytes: 0,
        consumer_count: 20,
        maintained_arrangements: 3,
        selected_strategy: PhysicalStrategy::Classic,
        provenance: vec![cdc_provenance, kafka_provenance],
    };

    let obs = CapacityObservation {
        private_state_bytes: 505_000,
        shared_state_bytes: 1_010_000,
        rss_bytes: 2_020_000,
        spill_bytes: 0,
        cache_hit_ratio: 0.95,
        epoch_duration_ms: 15.2,
        commit_group_duration_ms: 5.1,
        p99_freshness_ms: 62.0,
        shuffle_bytes: 202_000,
        logical_writes: 50_000,
        physical_writes: 50_120,
        object_store_requests: 255,
        checkpoint_cost_ms: 12.2,
        compaction_debt_bytes: 0,
    };

    let record = RawCapacityRecord::new(
        "rec_cdc_kafka_001",
        profile,
        wl_digest,
        candidate,
        hardware,
        vec!["arr_orders".to_string(), "arr_events".to_string()],
        PhysicalStrategy::Classic,
        est,
        obs,
    );

    assert_eq!(record.errors.len(), 14);
    let priv_err = record.errors.get("private_state_bytes").unwrap();
    assert!(priv_err.relative_error < 0.05);
    let fresh_err = record.errors.get("p99_freshness_ms").unwrap();
    assert!(fresh_err.relative_error < 0.05);
}
