//! Capacity release gate qualification tests (v0.59.23 Slice 6 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityGateRejection, CapacityGuidance, CapacityProfile, CapacityReleaseGate,
    HardwareIdentity, PhysicalStrategy, RawCapacityRecord, WorkloadDigest,
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

fn create_full_raw_records(candidate: &CandidateIdentity) -> Vec<RawCapacityRecord> {
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
                seed: 300 + i as u64,
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
fn complete_signed_profile_set_is_required() {
    let candidate = sample_candidate();
    let records = create_full_raw_records(&candidate);
    let (manifest, _) = CapacityGuidance::regenerate_from_raw_records(candidate.clone(), &records);

    let eval_res = CapacityReleaseGate::evaluate_candidate(&manifest, &candidate, &records);
    assert!(eval_res.is_ok(), "complete valid manifest must pass gate");
}

#[test]
fn release_gate_rejects_candidate_mismatch() {
    let candidate = sample_candidate();
    let mut other_candidate = candidate.clone();
    other_candidate.commit_sha =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string();

    let records = create_full_raw_records(&candidate);
    let (manifest, _) = CapacityGuidance::regenerate_from_raw_records(candidate, &records);

    let eval_res = CapacityReleaseGate::evaluate_candidate(&manifest, &other_candidate, &records);
    assert!(matches!(
        eval_res,
        Err(CapacityGateRejection::CandidateMismatch { .. })
    ));
}

#[test]
fn release_gate_rejects_underprovisioned_hardware() {
    let candidate = sample_candidate();
    let records = create_full_raw_records(&candidate);
    let (mut manifest, _) =
        CapacityGuidance::regenerate_from_raw_records(candidate.clone(), &records);

    if let Some(pt) = manifest.profiles.remove(&CapacityProfile::Large) {
        let mut bad_hw = pt.hardware.clone();
        bad_hw.physical_cores = 2; // Below required 32 cores
        let bad_pt = rockstream_types::capacity::ProfileThresholds::new(
            CapacityProfile::Large,
            bad_hw,
            pt.concurrency,
            pt.workload_digests,
            pt.raw_record_digests,
            pt.thresholds,
            pt.summary,
        );
        manifest.profiles.insert(CapacityProfile::Large, bad_pt);
    }
    // Reseal with bad hardware
    let bad_manifest = rockstream_types::capacity::CapacityThresholdManifest::seal(
        candidate.clone(),
        manifest.profiles,
    );

    let eval_res = CapacityReleaseGate::evaluate_candidate(&bad_manifest, &candidate, &records);
    assert!(matches!(
        eval_res,
        Err(CapacityGateRejection::HardwareMismatch { .. })
    ));
}

#[test]
fn release_gate_rejects_missing_workload() {
    let candidate = sample_candidate();
    let records = create_full_raw_records(&candidate);
    let (mut manifest, _) =
        CapacityGuidance::regenerate_from_raw_records(candidate.clone(), &records);

    if let Some(mut pt) = manifest.profiles.remove(&CapacityProfile::Medium) {
        pt.workload_digests
            .retain(|w| w.workload_name != "factorized_join");
        let bad_pt = rockstream_types::capacity::ProfileThresholds::new(
            CapacityProfile::Medium,
            pt.hardware,
            pt.concurrency,
            pt.workload_digests,
            pt.raw_record_digests,
            pt.thresholds,
            pt.summary,
        );
        manifest.profiles.insert(CapacityProfile::Medium, bad_pt);
    }
    let bad_manifest = rockstream_types::capacity::CapacityThresholdManifest::seal(
        candidate.clone(),
        manifest.profiles,
    );

    let eval_res = CapacityReleaseGate::evaluate_candidate(&bad_manifest, &candidate, &records);
    assert!(matches!(
        eval_res,
        Err(CapacityGateRejection::MissingWorkload { .. })
    ));
}
