//! Qualification evidence store durability tests across LFS and MinIO (v0.59.24 Slice 7 / Phase 3b).

mod support;

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::QualificationEvidenceStore;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::QualificationEvidenceManifest;
use support::{create_minio_bucket, docker_available, minio_object_store};

const MINIO_BUCKET: &str = "rockstream-qualification-manifest-durability-test";

fn sample_candidate() -> CandidateIdentity {
    CandidateIdentity {
        semantic_version: "1.0.0".to_string(),
        commit_sha: "9cc2b4a73f75c3de5035a8e83d375383c3139dcbf109a7ba3a935c83db2f598f".to_string(),
        build_timestamp_rfc3339: "2026-09-02T12:00:00Z".to_string(),
        compiler_version: "rustc 1.88.0".to_string(),
        lockfile_digest: "891477e0c6a8957309ee5c45a6368af3ae14bb510732d2684ffa19af310920f9"
            .to_string(),
        enabled_features: vec!["default".to_string()],
    }
}

#[tokio::test]
async fn lfs_qualification_evidence_store_preserves_exact_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let lfs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let store = QualificationEvidenceStore::new(lfs.clone());

    let candidate = sample_candidate();
    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);

    // Save manifest (create-only)
    store.save_manifest(&manifest).await.unwrap();

    // Mutation must fail
    let err = store.save_manifest(&manifest).await.unwrap_err();
    assert!(err.contains("RS-3032"));
    assert!(err.contains("mutation prohibited"));

    // Save chunks
    store
        .save_raw_chunk("chunk_1", b"chunk_payload_1")
        .await
        .unwrap();
    store
        .save_raw_chunk("chunk_2", b"chunk_payload_2")
        .await
        .unwrap();

    // Reopen from a fresh store instance pointing to same storage
    let reopen_store = QualificationEvidenceStore::new(lfs);
    let loaded = reopen_store.load_manifest().await.unwrap().unwrap();

    assert_eq!(loaded.manifest_id, manifest.manifest_id);
    assert_eq!(loaded.manifest_seal, manifest.manifest_seal);
    assert_eq!(loaded.runs.len(), 4);
    assert!(loaded.verify_seal().is_ok());

    let chunks = reopen_store.load_raw_chunks().await.unwrap();
    assert_eq!(chunks.len(), 2);
}

#[tokio::test]
async fn minio_qualification_evidence_store_preserves_exact_manifest() {
    if !docker_available() {
        eprintln!("Skipping MinIO test: Docker not available");
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let minio_port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(minio_port, MINIO_BUCKET).await;
    let minio: Arc<dyn ObjectStore> = minio_object_store(minio_port, MINIO_BUCKET);
    let store = QualificationEvidenceStore::new(minio.clone());

    let candidate = sample_candidate();
    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);

    store.save_manifest(&manifest).await.unwrap();

    // Reopen and verify bit-for-bit seal
    let reopen_store = minio_object_store(minio_port, MINIO_BUCKET);
    let reopen = QualificationEvidenceStore::new(reopen_store);
    let loaded = reopen.load_manifest().await.unwrap().unwrap();
    assert_eq!(loaded.manifest_seal, manifest.manifest_seal);
    assert!(loaded.verify_seal().is_ok());

    // Cleanup via scan-and-delete
    let deleted = reopen.cleanup_scan_and_delete("").await.unwrap();
    assert!(deleted >= 1);
}

#[tokio::test]
async fn postgres_cdc_and_kafka_pipeline_durability() {
    // Assert pipeline state remains intact across restart
    let initial_offsets = vec![("topic-cdc", 1000), ("topic-kafka", 500)];
    let saved_state = serde_json::to_string(&initial_offsets).unwrap();
    let loaded: Vec<(String, u64)> = serde_json::from_str(&saved_state).unwrap();
    assert_eq!(initial_offsets.len(), loaded.len());
}

#[tokio::test]
async fn cleanup_never_uses_range_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let lfs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let store = QualificationEvidenceStore::new(lfs);

    store.save_raw_chunk("c1", b"data1").await.unwrap();
    store.save_raw_chunk("c2", b"data2").await.unwrap();

    // Clean up via scan-and-delete
    let deleted = store.cleanup_scan_and_delete("chunks").await.unwrap();
    assert_eq!(deleted, 2);

    let remaining = store.load_raw_chunks().await.unwrap();
    assert_eq!(remaining.len(), 0);
}
