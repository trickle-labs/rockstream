mod support;

use std::sync::Arc;

use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use rockstream_control::{CheckpointExportService, CheckpointManifestStore};
use rockstream_runtime::RecoveryDriver;
use rockstream_storage::ShardDb;
use rockstream_types::{
    checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint},
    ids::{LeaseToken, ShardId},
};
use support::{create_minio_bucket, docker_available, minio_object_store};

async fn seed_checkpoint(store: Arc<dyn ObjectStore>, checkpoint_id: u64) -> ClusterCheckpoint {
    let db = ShardDb::builder("shards/0", store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/rows", b"committed").await.unwrap();
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
async fn lfs_truncated_export_fails_closed_rs5035() {
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
    assert!(error.to_string().starts_with("RS-5035:"));
    assert!(target
        .head(&Path::from("control/bootstrap/active-generation"))
        .await
        .is_err());
}

async fn minio_stores(
    suffix: &str,
) -> Option<(
    testcontainers::ContainerAsync<testcontainers_modules::minio::MinIO>,
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
)> {
    if !docker_available() {
        eprintln!("SKIP MinIO disaster recovery test: Docker not available");
        return None;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let source_bucket = format!("rs-dr-source-{suffix}");
    let export_bucket = format!("rs-dr-export-{suffix}");
    let target_bucket = format!("rs-dr-target-{suffix}");
    create_minio_bucket(port, &source_bucket).await;
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
async fn minio_tc_truncated_export_fails_closed_rs5035() {
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
    assert!(error.to_string().starts_with("RS-5035:"));
    assert!(target
        .head(&Path::from("control/bootstrap/active-generation"))
        .await
        .is_err());
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
