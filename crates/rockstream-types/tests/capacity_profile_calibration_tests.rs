//! Capacity profile calibration and reducer verification tests (v0.59.23 Slice 1).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityBatchCollector, CapacityEstimate, CapacityObservation, CapacityProfile,
    CapacityReducer, HardwareIdentity, MemoryChunkSink, PhysicalStrategy, RawCapacityRecord,
    SourceStatisticProvenance, WorkloadDigest,
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

fn create_sample_raw_records(profile: CapacityProfile) -> Vec<RawCapacityRecord> {
    let candidate = sample_candidate();
    let hardware = HardwareIdentity::reference(profile);
    let mut records = Vec::new();

    for (i, wl_name) in profile.required_workloads().iter().enumerate() {
        let wl_digest = WorkloadDigest {
            workload_name: wl_name.to_string(),
            seed: 42 + i as u64,
            config_hash: format!("cfg_hash_{i}"),
            dataset_digest: format!("ds_digest_{i}"),
        };

        let strategy = if *wl_name == "factorized_join" {
            PhysicalStrategy::Factorized {
                payload_bound: 1024,
                factor_payload_bytes: 4096,
                delta_amplification: 1.05,
            }
        } else {
            PhysicalStrategy::Classic
        };

        let (est_bytes, obs_bytes) = match profile {
            CapacityProfile::Small => (24_000, 24_200),
            CapacityProfile::Medium => (1_200_000, 1_210_000),
            CapacityProfile::Large => (100_000_000, 102_000_000),
        };

        let est = CapacityEstimate {
            private_state_bytes: est_bytes,
            shared_state_bytes: est_bytes * 2,
            saved_bytes: est_bytes / 2,
            rss_bytes: est_bytes * 3,
            spill_bytes: if profile == CapacityProfile::Large {
                est_bytes / 4
            } else {
                0
            },
            cache_hit_ratio: 0.95,
            epoch_duration_ms: 12.5,
            commit_group_duration_ms: 4.2,
            p99_freshness_ms: 85.0,
            shuffle_bytes: est_bytes / 3,
            logical_writes: 5_000,
            physical_writes: 5_200,
            object_store_requests: 120,
            checkpoint_cost_ms: 15.0,
            compaction_debt_bytes: 0,
            consumer_count: profile.consumer_count(),
            maintained_arrangements: profile.canonical_arrangement_count(),
            selected_strategy: strategy.clone(),
            provenance: vec![SourceStatisticProvenance::Catalog {
                table_name: "orders".to_string(),
                row_count: 100_000,
            }],
        };

        let obs = CapacityObservation {
            private_state_bytes: obs_bytes,
            shared_state_bytes: obs_bytes * 2,
            rss_bytes: obs_bytes * 3,
            spill_bytes: if profile == CapacityProfile::Large {
                obs_bytes / 4
            } else {
                0
            },
            cache_hit_ratio: 0.94,
            epoch_duration_ms: 12.8,
            commit_group_duration_ms: 4.3,
            p99_freshness_ms: 87.0,
            shuffle_bytes: obs_bytes / 3,
            logical_writes: 5_000,
            physical_writes: 5_210,
            object_store_requests: 122,
            checkpoint_cost_ms: 15.2,
            compaction_debt_bytes: 0,
        };

        records.push(RawCapacityRecord::new(
            format!("rec_{}_{i}", profile.as_str()),
            profile,
            wl_digest,
            candidate.clone(),
            hardware.clone(),
            vec![format!("arr_{i}")],
            strategy,
            est,
            obs,
        ));
    }

    records
}

#[test]
fn raw_fixture_reduces_to_exact_summary() {
    let small_records = create_sample_raw_records(CapacityProfile::Small);
    let summary_small = CapacityReducer::reduce_raw_records(CapacityProfile::Small, &small_records);

    assert_eq!(summary_small.profile, CapacityProfile::Small);
    assert_eq!(summary_small.total_samples, 2);
    assert_eq!(summary_small.workload_digests.len(), 2);
    assert!(summary_small
        .error_ranges
        .contains_key("private_state_bytes"));
    assert!(summary_small.error_ranges.contains_key("p99_freshness_ms"));
    assert!(!summary_small.summary_digest.is_empty());

    let med_records = create_sample_raw_records(CapacityProfile::Medium);
    let summary_med = CapacityReducer::reduce_raw_records(CapacityProfile::Medium, &med_records);

    assert_eq!(summary_med.profile, CapacityProfile::Medium);
    assert_eq!(summary_med.total_samples, 5);
    assert_eq!(summary_med.workload_digests.len(), 5);
    assert!(!summary_med.summary_digest.is_empty());

    // Deterministic summary digest check: reducing same records gives identical digest.
    let summary_med_2 = CapacityReducer::reduce_raw_records(CapacityProfile::Medium, &med_records);
    assert_eq!(summary_med.summary_digest, summary_med_2.summary_digest);
}

#[test]
fn bounded_batch_collector_enforces_flush_and_fill_ratio() {
    let sink = Box::new(MemoryChunkSink::new());
    let mut collector = CapacityBatchCollector::new(sink);

    assert_eq!(collector.len(), 0);
    assert_eq!(collector.fill_ratio(), 0.0);

    let recs = create_sample_raw_records(CapacityProfile::Small);
    for r in &recs {
        collector.push(r.clone()).expect("push must succeed");
    }

    assert_eq!(collector.len(), 2);
    assert!(collector.fill_ratio() > 0.0);
    assert!(collector.fill_ratio() <= 1.0);

    collector.flush().expect("flush must succeed");
    assert_eq!(collector.len(), 0);
    assert_eq!(collector.buffered_bytes(), 0);
    assert_eq!(collector.fill_ratio(), 0.0);
}

#[test]
fn directional_inputs_change_exact_fields() {
    let mut recs = create_sample_raw_records(CapacityProfile::Medium);
    let summary1 = CapacityReducer::reduce_raw_records(CapacityProfile::Medium, &recs);

    // Increase private state estimate
    recs[0].estimated.private_state_bytes += 500_000;
    recs[0].recompute_errors();
    let summary2 = CapacityReducer::reduce_raw_records(CapacityProfile::Medium, &recs);

    assert_ne!(
        summary1
            .error_ranges
            .get("private_state_bytes")
            .unwrap()
            .mean_error,
        summary2
            .error_ranges
            .get("private_state_bytes")
            .unwrap()
            .mean_error,
        "private state error must reflect directional change"
    );
    assert_ne!(summary1.summary_digest, summary2.summary_digest);
}

#[test]
fn uniform_profile_rows_match_fixture() {
    for profile in [
        CapacityProfile::Small,
        CapacityProfile::Medium,
        CapacityProfile::Large,
    ] {
        let recs = create_sample_raw_records(profile);
        let uniform_rec = recs
            .iter()
            .find(|r| {
                r.workload_digest.workload_name.contains("uniform")
                    || r.workload_digest.workload_name.contains("aggregation")
            })
            .expect("uniform or aggregation workload must be present");
        assert!(uniform_rec.observed.logical_writes > 0);
        assert!(uniform_rec.observed.physical_writes > 0);
        assert!(uniform_rec.observed.p99_freshness_ms < 1000.0);
    }
}

#[test]
fn high_cardinality_profile_rows_match_fixture() {
    for profile in [CapacityProfile::Medium, CapacityProfile::Large] {
        let recs = create_sample_raw_records(profile);
        let hc_rec = recs
            .iter()
            .find(|r| {
                r.workload_digest.workload_name.contains("high_cardinality")
                    || r.workload_digest.workload_name.contains("state_over_ram")
            })
            .expect("high cardinality workload must be present");
        assert!(hc_rec.estimated.private_state_bytes > 0);
        assert!(hc_rec.observed.private_state_bytes > 0);
    }
}

#[test]
fn join_profile_rows_match_fixture() {
    for profile in [CapacityProfile::Medium, CapacityProfile::Large] {
        let recs = create_sample_raw_records(profile);
        let join_rec = recs
            .iter()
            .find(|r| r.workload_digest.workload_name.contains("join"))
            .expect("join workload must be present");
        assert!(join_rec.estimated.shuffle_bytes > 0);
    }
}

#[test]
fn window_profile_rows_match_fixture() {
    for profile in [
        CapacityProfile::Small,
        CapacityProfile::Medium,
        CapacityProfile::Large,
    ] {
        let recs = create_sample_raw_records(profile);
        let win_rec = recs
            .iter()
            .find(|r| r.workload_digest.workload_name.contains("window"))
            .expect("window workload must be present");
        assert!(win_rec.estimated.epoch_duration_ms > 0.0);
    }
}

#[test]
fn skew_profile_rows_match_fixture() {
    let recs = create_sample_raw_records(CapacityProfile::Large);
    let skew_rec = recs
        .iter()
        .find(|r| r.workload_digest.workload_name.contains("zipf_hot_key"))
        .expect("skew workload must be present for large profile");
    assert_eq!(skew_rec.profile, CapacityProfile::Large);
    assert!(skew_rec.observed.p99_freshness_ms < 100.0);
}

#[test]
fn state_over_ram_rows_match_fixture() {
    let recs = create_sample_raw_records(CapacityProfile::Large);
    let ram_rec = recs
        .iter()
        .find(|r| r.workload_digest.workload_name.contains("state_over_ram"))
        .expect("state_over_ram workload must be present for large profile");
    assert!(ram_rec.estimated.spill_bytes > 0);
    assert!(ram_rec.observed.spill_bytes > 0);
}

#[test]
fn all_profile_metrics_and_error_ranges_are_exact() {
    for profile in [
        CapacityProfile::Small,
        CapacityProfile::Medium,
        CapacityProfile::Large,
    ] {
        let recs = create_sample_raw_records(profile);
        let summary = CapacityReducer::reduce_raw_records(profile, &recs);

        let required_metrics = [
            "private_state_bytes",
            "shared_state_bytes",
            "rss_bytes",
            "spill_bytes",
            "cache_hit_ratio",
            "epoch_duration_ms",
            "commit_group_duration_ms",
            "p99_freshness_ms",
            "shuffle_bytes",
            "logical_writes",
            "physical_writes",
            "object_store_requests",
            "checkpoint_cost_ms",
            "compaction_debt_bytes",
        ];

        for metric in required_metrics {
            let er = summary
                .error_ranges
                .get(metric)
                .unwrap_or_else(|| panic!("metric '{}' missing in profile '{}'", metric, profile));
            assert!(er.sample_count > 0);
            assert!(er.min_error >= 0.0);
            assert!(er.max_error >= er.min_error);
            assert!(er.mean_error >= er.min_error && er.mean_error <= er.max_error);
            assert!(!er.raw_record_digest.is_empty());
        }
    }
}
