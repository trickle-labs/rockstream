//! Capacity threshold manifest and raw chunk durability tests (v0.59.23 Slice 5 / Phase 3b).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::CapacityThresholdStore;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::capacity::{
    CapacityProfile, CapacityReducer, CapacityThresholdManifest, HardwareIdentity,
    PhysicalStrategy, ProfileThresholds, RawCapacityRecord, ThresholdFloorCeiling, WorkloadDigest,
};
use support::{create_minio_bucket, docker_available, minio_object_store};

const MINIO_BUCKET: &str = "rockstream-capacity-manifest-durability-test";

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
                seed: 400 + i as u64,
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

#[tokio::test]
async fn lfs_reopen_preserves_exact_sealed_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let capacity_store = CapacityThresholdStore::new(store.clone());
    let (expected_manifest, records) = create_sample_manifest();

    // 1. Save manifest and raw chunk
    capacity_store
        .save_manifest(&expected_manifest)
        .await
        .expect("save manifest must succeed");

    let chunk_bytes = serde_json::to_vec(&records).unwrap();
    capacity_store
        .save_raw_chunk("chunk-001.json", &chunk_bytes)
        .await
        .expect("save raw chunk must succeed");

    // Reopen store from fresh handle over the same directory
    let reloaded_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let reloaded_capacity_store = CapacityThresholdStore::new(reloaded_store);

    // 2. Verify reloaded manifest matches exact sealed manifest
    let loaded_manifest = reloaded_capacity_store
        .load_manifest()
        .await
        .expect("load manifest must succeed")
        .expect("manifest must exist");

    assert_eq!(loaded_manifest, expected_manifest);
    assert_eq!(
        loaded_manifest.manifest_seal,
        expected_manifest.manifest_seal
    );
    assert!(loaded_manifest.verify_seal().is_ok());

    // 3. Verify reloaded chunks match
    let loaded_chunks = reloaded_capacity_store
        .load_raw_chunks()
        .await
        .expect("load chunks must succeed");
    assert_eq!(loaded_chunks.len(), 1);
    assert_eq!(loaded_chunks[0], chunk_bytes);

    // 4. Verify create-only semantics: attempting to overwrite must fail
    let overwrite_res = reloaded_capacity_store
        .save_manifest(&expected_manifest)
        .await;
    assert!(overwrite_res.is_err(), "cannot overwrite existing manifest");
}

#[tokio::test]
async fn minio_reopen_preserves_exact_sealed_manifest() {
    if !docker_available() {
        eprintln!("Docker not available; skipping MinIO capacity durability test");
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let minio_port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(minio_port, MINIO_BUCKET).await;

    let store = minio_object_store(minio_port, MINIO_BUCKET);
    let capacity_store = CapacityThresholdStore::new(store.clone());
    let (expected_manifest, records) = create_sample_manifest();

    // 1. Save manifest and raw chunk
    capacity_store
        .save_manifest(&expected_manifest)
        .await
        .expect("save manifest to MinIO must succeed");

    let chunk_bytes = serde_json::to_vec(&records).unwrap();
    capacity_store
        .save_raw_chunk("chunk-minio-001.json", &chunk_bytes)
        .await
        .expect("save raw chunk to MinIO must succeed");

    // Reopen store from fresh MinIO client
    let reloaded_store = minio_object_store(minio_port, MINIO_BUCKET);
    let reloaded_capacity_store = CapacityThresholdStore::new(reloaded_store);

    let loaded_manifest = reloaded_capacity_store
        .load_manifest()
        .await
        .expect("load manifest from MinIO must succeed")
        .expect("manifest must exist in MinIO");

    assert_eq!(loaded_manifest, expected_manifest);
    assert_eq!(
        loaded_manifest.manifest_seal,
        expected_manifest.manifest_seal
    );
    assert!(loaded_manifest.verify_seal().is_ok());
}

#[tokio::test]
async fn cleanup_never_uses_range_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let capacity_store = CapacityThresholdStore::new(store.clone());

    // Save temporary chunks
    for i in 0..5 {
        capacity_store
            .save_raw_chunk(&format!("chunk-{i}.json"), b"data")
            .await
            .unwrap();
    }

    let deleted = capacity_store
        .cleanup_scan_and_delete("chunks")
        .await
        .expect("scan-and-delete cleanup must succeed");
    assert_eq!(deleted, 5);

    let remaining_chunks = capacity_store.load_raw_chunks().await.unwrap();
    assert_eq!(remaining_chunks.len(), 0);
}
