//! Capacity guidance generation and determinism tests (v0.59.23 Slice 5 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityGuidance, CapacityProfile, HardwareIdentity, PhysicalStrategy, RawCapacityRecord,
    WorkloadDigest,
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

fn create_sample_raw_records() -> Vec<RawCapacityRecord> {
    let candidate = sample_candidate();
    let mut all_records = Vec::new();

    for profile in [
        CapacityProfile::Small,
        CapacityProfile::Medium,
        CapacityProfile::Large,
    ] {
        let hardware = HardwareIdentity::reference(profile);
        for (i, wl_name) in profile.required_workloads().iter().enumerate() {
            let wl_digest = WorkloadDigest {
                workload_name: wl_name.to_string(),
                seed: 200 + i as u64,
                config_hash: format!("cfg_{i}"),
                dataset_digest: format!("ds_{i}"),
            };
            let mut rec = RawCapacityRecord::new(
                format!("rec_{}_{i}", profile.as_str()),
                profile,
                wl_digest,
                candidate.clone(),
                hardware.clone(),
                vec![format!("arr_{i}")],
                PhysicalStrategy::Classic,
                rockstream_types::capacity::CapacityEstimate {
                    private_state_bytes: 10_000,
                    shared_state_bytes: 20_000,
                    saved_bytes: 5_000,
                    rss_bytes: 30_000,
                    spill_bytes: 0,
                    cache_hit_ratio: 0.95,
                    epoch_duration_ms: 10.0,
                    commit_group_duration_ms: 2.0,
                    p99_freshness_ms: 50.0,
                    shuffle_bytes: 1_000,
                    logical_writes: 100,
                    physical_writes: 105,
                    object_store_requests: 10,
                    checkpoint_cost_ms: 5.0,
                    compaction_debt_bytes: 0,
                    consumer_count: profile.consumer_count(),
                    maintained_arrangements: profile.canonical_arrangement_count(),
                    selected_strategy: PhysicalStrategy::Classic,
                    provenance: vec![],
                },
                rockstream_types::capacity::CapacityObservation {
                    private_state_bytes: 10_200,
                    shared_state_bytes: 20_100,
                    rss_bytes: 30_500,
                    spill_bytes: 0,
                    cache_hit_ratio: 0.94,
                    epoch_duration_ms: 10.2,
                    commit_group_duration_ms: 2.1,
                    p99_freshness_ms: 52.0,
                    shuffle_bytes: 1_000,
                    logical_writes: 100,
                    physical_writes: 106,
                    object_store_requests: 11,
                    checkpoint_cost_ms: 5.1,
                    compaction_debt_bytes: 0,
                },
            );
            rec.recompute_errors();
            all_records.push(rec);
        }
    }

    all_records
}

#[test]
fn raw_records_regenerate_exact_guidance() {
    let candidate = sample_candidate();
    let records = create_sample_raw_records();

    let (manifest1, markdown1) =
        CapacityGuidance::regenerate_from_raw_records(candidate.clone(), &records);
    let (manifest2, markdown2) =
        CapacityGuidance::regenerate_from_raw_records(candidate.clone(), &records);

    // Verification 1: Manifests are identical and seals match
    assert_eq!(manifest1, manifest2);
    assert_eq!(manifest1.manifest_seal, manifest2.manifest_seal);
    assert!(manifest1.verify_seal().is_ok());

    // Verification 2: Markdown guidance text is completely deterministic
    assert_eq!(markdown1, markdown2);
    assert!(markdown1.contains("# Capacity Guidance & Threshold Reference"));
    assert!(markdown1.contains("AMD EPYC 7763 64-Core Processor"));
    assert!(markdown1.contains("small"));
    assert!(markdown1.contains("medium"));
    assert!(markdown1.contains("large"));
}

#[test]
fn guidance_links_each_error_range_to_raw_record_digest() {
    let candidate = sample_candidate();
    let records = create_sample_raw_records();
    let (manifest, markdown) = CapacityGuidance::regenerate_from_raw_records(candidate, &records);

    for (profile, pt) in &manifest.profiles {
        for (metric, er) in &pt.summary.error_ranges {
            assert!(
                !er.raw_record_digest.is_empty(),
                "error range for {}/{} must link a raw record digest",
                profile,
                metric
            );
            assert!(
                markdown.contains(&er.raw_record_digest),
                "markdown guidance must contain raw record digest for {}/{}",
                profile,
                metric
            );
        }
    }
}
