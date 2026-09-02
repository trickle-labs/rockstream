//! Capacity threshold manifest integrity and immutability tests (v0.59.23 Slice 5 / Phase 3b).

use std::collections::BTreeMap;

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityProfile, CapacityReducer, CapacityThresholdManifest, HardwareIdentity,
    PhysicalStrategy, ProfileThresholds, RawCapacityRecord, ThresholdFloorCeiling, WorkloadDigest,
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

fn create_sample_manifest() -> (CapacityThresholdManifest, Vec<RawCapacityRecord>) {
    let candidate = sample_candidate();
    let mut profiles = BTreeMap::new();
    let mut all_records = Vec::new();

    for profile in [
        CapacityProfile::Small,
        CapacityProfile::Medium,
        CapacityProfile::Large,
    ] {
        let hardware = HardwareIdentity::reference(profile);
        let mut recs = Vec::new();
        for (i, wl_name) in profile.required_workloads().iter().enumerate() {
            let wl_digest = WorkloadDigest {
                workload_name: wl_name.to_string(),
                seed: 100 + i as u64,
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
            recs.push(rec);
        }

        let summary = CapacityReducer::reduce_raw_records(profile, &recs);
        let raw_digests = recs.iter().map(|r| r.compute_record_digest()).collect();
        let thresholds = ThresholdFloorCeiling::reference(profile);
        let pt = ProfileThresholds::new(
            profile,
            hardware,
            4,
            summary.workload_digests.clone(),
            raw_digests,
            thresholds,
            summary,
        );
        profiles.insert(profile, pt);
        all_records.extend(recs);
    }

    let manifest = CapacityThresholdManifest::seal(candidate, profiles);
    (manifest, all_records)
}

#[test]
fn sealed_manifest_rejects_any_mutation() {
    let (mut manifest, _) = create_sample_manifest();

    // 1. Initial sealed manifest must verify successfully
    assert!(manifest.verify_seal().is_ok());

    // 2. Tampering with candidate commit SHA invalidates the seal
    manifest.candidate.commit_sha =
        "0000000000000000000000000000000000000000000000000000000000000000".to_string();
    assert!(manifest.verify_seal().is_err());
}

#[test]
fn sealed_manifest_detects_tampered_profile() {
    let (mut manifest, _) = create_sample_manifest();

    // Tampering with hardware or thresholds inside a profile invalidates seal
    if let Some(pt) = manifest.profiles.get_mut(&CapacityProfile::Small) {
        pt.hardware.physical_cores = 128;
    }
    assert!(manifest.verify_seal().is_err());
}

#[test]
fn sealed_manifest_detects_tampered_digest() {
    let (mut manifest, _) = create_sample_manifest();

    // Tampering with the seal string directly fails verification
    manifest.manifest_seal = "bad_seal_value_0123456789".to_string();
    assert!(manifest.verify_seal().is_err());
}
