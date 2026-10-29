use std::sync::Arc;

use futures::StreamExt;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};
use rockstream_control::{CheckpointExportService, CheckpointManifestStore};
use rockstream_runtime::RecoveryDriver;
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{
    create_minio_bucket, minio_object_store, start_minio, MinIO2024,
};
use rockstream_types::{
    checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint},
    ids::{LeaseToken, ShardId},
};

async fn seed_checkpoint(store: Arc<dyn ObjectStore>, checkpoint_id: u64) -> ClusterCheckpoint {
    let db = ShardDb::builder("shards/0", store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/rows", b"committed").await.unwrap();
    db.put(
        &ShardKeyEncoder::frontier_key(),
        &checkpoint_id.to_be_bytes(),
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    let handle = db.create_checkpoint().await.unwrap();
    let id = CheckpointId(checkpoint_id);
    let mut checkpoint = ClusterCheckpoint::new(id);
    checkpoint.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(id, handle.shard_checkpoint_id)
            .with_snapshot_id(handle.snapshot_id),
    );
    CheckpointManifestStore::new(store.clone())
        .save_manifest(&checkpoint, false, None)
        .await
        .unwrap();
    store
        .put(
            &Path::from("control/catalog/views"),
            bytes::Bytes::from_static(b"view=v committed_epoch=7").into(),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("control/connectors/source-resume"),
            bytes::Bytes::from_static(b"offset=42").into(),
        )
        .await
        .unwrap();
    checkpoint
}

async fn export_restore_and_assert(
    source: Arc<dyn ObjectStore>,
    export: Arc<dyn ObjectStore>,
    target: Arc<dyn ObjectStore>,
    generation: &str,
) {
    let checkpoint = seed_checkpoint(source.clone(), 7).await;
    let service = CheckpointExportService::new();
    let manifests = CheckpointManifestStore::new(source.clone());
    service
        .export_latest_prefix(
            source,
            export.clone(),
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap();
    let restored = service
        .restore_generation(export, target.clone(), generation)
        .await
        .unwrap();
    assert_eq!(
        restored,
        rockstream_control::CheckpointRestoreOutcome {
            checkpoint_id: 7,
            generation: generation.to_string(),
            object_count: restored.object_count,
            byte_count: restored.byte_count,
            restored_shards: 1,
            status: "SUCCESS".to_string(),
        }
    );
    assert!(target
        .head(&Path::from("control/bootstrap/active-generation"))
        .await
        .is_ok());
    assert_eq!(
        CheckpointManifestStore::new(target.clone())
            .load_manifest(CheckpointId(7))
            .await,
        Some(checkpoint.clone())
    );
    assert_eq!(
        target
            .get(&Path::from("control/catalog/views"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"view=v committed_epoch=7"
    );
    assert_eq!(
        target
            .get(&Path::from("control/connectors/source-resume"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"offset=42"
    );
    let recovery = RecoveryDriver::new();
    recovery.load_checkpoint(checkpoint);
    let shard = recovery
        .recover_shard(ShardId(0), "shards/0", target, LeaseToken(1), LeaseToken(1))
        .await
        .unwrap();
    assert_eq!(
        shard
            .reader
            .get(b"view/rows")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"committed"
    );
}

#[tokio::test]
async fn lfs_export_restore_fresh_cluster_is_bit_identical() {
    let source_dir = tempfile::tempdir().unwrap();
    let export_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    export_restore_and_assert(
        Arc::new(LocalFileSystem::new_with_prefix(source_dir.path()).unwrap()),
        Arc::new(LocalFileSystem::new_with_prefix(export_dir.path()).unwrap()),
        Arc::new(LocalFileSystem::new_with_prefix(target_dir.path()).unwrap()),
        "lfs-bit-identical",
    )
    .await;
}

#[tokio::test]
async fn lfs_export_under_writes_restores_one_committed_epoch() {
    let source_dir = tempfile::tempdir().unwrap();
    let export_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(source_dir.path()).unwrap());
    let export: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(export_dir.path()).unwrap());
    let target: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(target_dir.path()).unwrap());
    let checkpoint = seed_checkpoint(source.clone(), 8).await;
    let db = ShardDb::builder("shards/0", source.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/rows", b"uncommitted-newer").await.unwrap();
    db.flush().await.unwrap();

    let service = CheckpointExportService::new();
    service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            "lfs-under-writes",
            &Path::from(""),
        )
        .await
        .unwrap();
    service
        .restore_generation(export, target.clone(), "lfs-under-writes")
        .await
        .unwrap();
    let recovery = RecoveryDriver::new();
    recovery.load_checkpoint(checkpoint);
    let shard = recovery
        .recover_shard(ShardId(0), "shards/0", target, LeaseToken(1), LeaseToken(1))
        .await
        .unwrap();
    assert_eq!(
        shard
            .reader
            .get(b"view/rows")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"committed"
    );
}

#[tokio::test]
async fn lfs_truncated_export_fails_closed_rs3614() {
    let source_dir = tempfile::tempdir().unwrap();
    let export_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(source_dir.path()).unwrap());
    let export: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(export_dir.path()).unwrap());
    let target: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(target_dir.path()).unwrap());
    seed_checkpoint(source.clone(), 9).await;
    let service = CheckpointExportService::new();
    let outcome = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            "lfs-truncated",
            &Path::from(""),
        )
        .await
        .unwrap();
    assert!(outcome.object_count > 0);
    export
        .put(
            &Path::from("checkpoint-exports/lfs-truncated/objects/00000000000000000000"),
            bytes::Bytes::from_static(b"truncated").into(),
        )
        .await
        .unwrap();
    let error = service
        .restore_generation(export, target.clone(), "lfs-truncated")
        .await
        .unwrap_err();
    assert!(error.to_string().starts_with("RS-3614:"));
    assert!(target
        .head(&Path::from("control/bootstrap/active-generation"))
        .await
        .is_err());
}

async fn minio_stores(
    suffix: &str,
) -> Option<(
    testcontainers::ContainerAsync<MinIO2024>,
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
)> {
    if !docker_available() {
        eprintln!("SKIP MinIO disaster recovery test: Docker not available");
        return None;
    }
    let source_bucket = format!("rs-dr-source-{suffix}");
    let (container, port) = start_minio(&source_bucket)
        .await
        .expect("MinIO must start when Docker is available");
    let export_bucket = format!("rs-dr-export-{suffix}");
    let target_bucket = format!("rs-dr-target-{suffix}");
    create_minio_bucket(port, &export_bucket).await;
    create_minio_bucket(port, &target_bucket).await;
    Some((
        container,
        minio_object_store(port, &source_bucket),
        minio_object_store(port, &export_bucket),
        minio_object_store(port, &target_bucket),
    ))
}

#[tokio::test]
async fn minio_restore_process_fixture() {
    let Ok(port) = std::env::var("ROCKSTREAM_MINIO_RESTORE_PORT") else {
        return;
    };
    let port = port.parse().unwrap();
    let export_bucket = std::env::var("ROCKSTREAM_MINIO_RESTORE_EXPORT_BUCKET").unwrap();
    let target_bucket = std::env::var("ROCKSTREAM_MINIO_RESTORE_TARGET_BUCKET").unwrap();
    let generation = std::env::var("ROCKSTREAM_MINIO_RESTORE_GENERATION").unwrap();
    let result_path = std::env::var("ROCKSTREAM_MINIO_RESTORE_RESULT").unwrap();
    let export = minio_object_store(port, &export_bucket);
    let target = minio_object_store(port, &target_bucket);
    let service = CheckpointExportService::new();
    let validated_export = service
        .validate_generation(export.clone(), &generation)
        .await
        .unwrap();
    let restore = service
        .restore_generation(export, target.clone(), &generation)
        .await
        .unwrap();
    let checkpoint = CheckpointManifestStore::new(target.clone())
        .load_manifest(CheckpointId(12))
        .await
        .unwrap();
    let recovery = RecoveryDriver::new();
    recovery.load_checkpoint(checkpoint.clone());
    let shard = recovery
        .recover_shard(
            ShardId(0),
            "shards/0",
            target.clone(),
            LeaseToken(1),
            LeaseToken(1),
        )
        .await
        .unwrap();
    let row = shard.reader.get(b"view/rows").await.unwrap().unwrap();
    let frontier = shard
        .reader
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .unwrap()
        .unwrap();
    let catalog = target
        .get(&Path::from("control/catalog/views"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let source_resume = target
        .get(&Path::from("control/connectors/source-resume"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let result = serde_json::json!({
        "pid": std::process::id(),
        "export": validated_export,
        "restore": restore,
        "checkpoint": checkpoint,
        "row": String::from_utf8(row.to_vec()).unwrap(),
        "frontier": u64::from_be_bytes(frontier.as_ref().try_into().unwrap()),
        "catalog": String::from_utf8(catalog.to_vec()).unwrap(),
        "source_resume": String::from_utf8(source_resume.to_vec()).unwrap(),
    });
    std::fs::write(result_path, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

#[tokio::test]
async fn minio_tc_restore_in_fresh_process_preserves_exact_state() {
    if !docker_available() {
        eprintln!("SKIP MinIO fresh-process restore test: Docker not available");
        return;
    }
    let source_bucket = "rs-dr-source-fresh-process";
    let export_bucket = "rs-dr-export-fresh-process";
    let target_bucket = "rs-dr-target-fresh-process";
    let (_container, port) = start_minio(source_bucket)
        .await
        .expect("MinIO must start when Docker is available");
    create_minio_bucket(port, export_bucket).await;
    create_minio_bucket(port, target_bucket).await;

    let source = minio_object_store(port, source_bucket);
    let export = minio_object_store(port, export_bucket);
    let target = minio_object_store(port, target_bucket);
    assert!(target.list(None).next().await.is_none());
    let checkpoint = seed_checkpoint(source.clone(), 12).await;
    let generation = "minio-fresh-process";
    let service = CheckpointExportService::new();
    let export_outcome = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            generation,
            &Path::from(""),
        )
        .await
        .unwrap();
    assert_eq!(
        service
            .validate_generation(export.clone(), generation)
            .await
            .unwrap(),
        export_outcome
    );

    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("restore.json");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["minio_restore_process_fixture", "--exact", "--nocapture"])
        .env("ROCKSTREAM_MINIO_RESTORE_PORT", port.to_string())
        .env("ROCKSTREAM_MINIO_RESTORE_EXPORT_BUCKET", export_bucket)
        .env("ROCKSTREAM_MINIO_RESTORE_TARGET_BUCKET", target_bucket)
        .env("ROCKSTREAM_MINIO_RESTORE_GENERATION", generation)
        .env("ROCKSTREAM_MINIO_RESTORE_RESULT", &result_path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "fresh-process restore failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let mut observed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&result_path).unwrap()).unwrap();
    let child_pid = observed["pid"].as_u64().unwrap();
    assert_ne!(child_pid, std::process::id() as u64);
    observed.as_object_mut().unwrap().remove("pid");
    assert_eq!(
        observed,
        serde_json::json!({
            "export": export_outcome,
            "restore": rockstream_control::CheckpointRestoreOutcome {
                checkpoint_id: 12,
                generation: generation.to_string(),
                object_count: export_outcome.object_count,
                byte_count: export_outcome.byte_count,
                restored_shards: 1,
                status: "SUCCESS".to_string(),
            },
            "checkpoint": checkpoint,
            "row": "committed",
            "frontier": 12,
            "catalog": "view=v committed_epoch=7",
            "source_resume": "offset=42",
        })
    );

    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let artifact_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ci-test-results");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(
        artifact_dir.join("checkpoint-export-minio-fresh-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "source_sha": String::from_utf8(revision.stdout).unwrap().trim(),
            "minio_image": "codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
            "endpoint": format!("http://127.0.0.1:{port}"),
            "source_bucket": source_bucket,
            "export_bucket": export_bucket,
            "target_bucket": target_bucket,
            "checkpoint": checkpoint,
            "generation": generation,
            "export": export_outcome,
            "restored_by_pid": child_pid,
            "observed_state": observed,
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn minio_tc_export_restore_fresh_cluster_is_bit_identical() {
    let Some((_container, source, export, target)) = minio_stores("identity").await else {
        return;
    };
    export_restore_and_assert(source, export, target, "minio-bit-identical").await;
}

#[tokio::test]
async fn minio_tc_export_under_writes_restores_one_committed_epoch() {
    let Some((_container, source, export, target)) = minio_stores("writes").await else {
        return;
    };
    let checkpoint = seed_checkpoint(source.clone(), 10).await;
    let db = ShardDb::builder("shards/0", source.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/rows", b"uncommitted-newer").await.unwrap();
    db.flush().await.unwrap();
    let service = CheckpointExportService::new();
    service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            "minio-under-writes",
            &Path::from(""),
        )
        .await
        .unwrap();
    service
        .restore_generation(export, target.clone(), "minio-under-writes")
        .await
        .unwrap();
    let recovery = RecoveryDriver::new();
    recovery.load_checkpoint(checkpoint);
    let shard = recovery
        .recover_shard(ShardId(0), "shards/0", target, LeaseToken(1), LeaseToken(1))
        .await
        .unwrap();
    assert_eq!(
        shard
            .reader
            .get(b"view/rows")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"committed"
    );
}

#[tokio::test]
async fn minio_tc_truncated_export_fails_closed_rs3614() {
    let Some((_container, source, export, target)) = minio_stores("truncated").await else {
        return;
    };
    seed_checkpoint(source.clone(), 11).await;
    let service = CheckpointExportService::new();
    service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            "minio-truncated",
            &Path::from(""),
        )
        .await
        .unwrap();
    export
        .put(
            &Path::from("checkpoint-exports/minio-truncated/objects/00000000000000000000"),
            bytes::Bytes::from_static(b"truncated").into(),
        )
        .await
        .unwrap();
    let error = service
        .restore_generation(export, target.clone(), "minio-truncated")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "RS-3614: checkpoint export integrity validation failed: destination object checkpoint-exports/minio-truncated/objects/00000000000000000000 failed validation; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
    );
    assert!(target.list(None).next().await.is_none());
}

#[tokio::test]
async fn test_disaster_recovery_lfs_full_lifecycle() {
    let source_dir = tempfile::tempdir().unwrap();
    let export_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = Arc::new(LocalFileSystem::new_with_prefix(source_dir.path()).unwrap());
    let export = Arc::new(LocalFileSystem::new_with_prefix(export_dir.path()).unwrap());
    let target = Arc::new(LocalFileSystem::new_with_prefix(target_dir.path()).unwrap());
    export_restore_and_assert(source, export, target, "lfs-lifecycle").await;
}

#[tokio::test]
async fn test_disaster_recovery_minio_full_lifecycle() {
    let Some((_container, source, export, target)) = minio_stores("full-lifecycle").await else {
        return;
    };
    export_restore_and_assert(source, export, target, "minio-lifecycle").await;
}

#[test]
fn export_restore_uses_no_slatedb_range_deletion() {
    let source = std::fs::read_to_string(format!(
        "{}/src/checkpoint_export.rs",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
    assert!(!production.contains("delete_range"));
    assert!(!production.contains("range_delete"));
}

#[tokio::test]
async fn cli_prefix_export_rejects_missing_committed_shard_objects_before_success() {
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use rockstream_control::CheckpointExportError;

    // Use a real checkpointed shard, unlike the older zero-shard CLI fixture.
    let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let checkpoint = seed_checkpoint(source.clone(), 37).await;
    assert_eq!(checkpoint.shards.len(), 1);

    // Keep the durable cluster control manifest, but lose its shard storage
    // before the CLI's export_latest_prefix inventory begins.
    let shard_prefix = Path::from("shards/0");
    let mut objects = source.list(Some(&shard_prefix));
    let mut deleted = Vec::new();
    while let Some(entry) = objects.next().await {
        deleted.push(entry.unwrap().location);
    }
    assert!(
        !deleted.is_empty(),
        "the fixture must contain actual shard data"
    );
    drop(objects);
    for path in deleted {
        source.delete(&path).await.unwrap();
    }
    let manifests = CheckpointManifestStore::new(source.clone());
    assert_eq!(
        manifests.load_latest_manifest().await.unwrap(),
        Some(checkpoint)
    );

    // Same service method used by StorageClient::export_checkpoint, not the
    // separate checkpoint-aware management export_prefix path.
    let failure = CheckpointExportService::new()
        .export_latest_prefix(
            source,
            destination.clone(),
            &manifests,
            "missing-shard-preinventory",
            &Path::from(""),
        )
        .await;
    match failure {
        Err(CheckpointExportError::Integrity(message)) => {
            assert!(
                message.contains("shard") || message.contains("SlateDB"),
                "unexpected checkpoint-integrity diagnostic: {message}"
            );
        }
        other => panic!("missing committed shard storage must not export SUCCESS: {other:?}"),
    }

    assert!(matches!(
        destination
            .head(&Path::from(
                "checkpoint-exports/missing-shard-preinventory/commit"
            ))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}
