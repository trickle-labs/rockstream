use std::{
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use futures::{stream::BoxStream, StreamExt};
use object_store::{
    local::LocalFileSystem, path::Path, CopyOptions, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rockstream_control::{CheckpointExportService, CheckpointManifestStore};
use rockstream_runtime::RecoveryDriver;
#[cfg(feature = "simulation")]
use rockstream_sim::buggify::{buggify_disable, buggify_focus, buggify_init};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{
    create_minio_bucket, minio_object_store, start_minio, MinIO2024,
};
use rockstream_types::{
    checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint},
    ids::{LeaseToken, ShardId},
};

#[derive(Debug)]
struct FailOnNthPutStore {
    inner: Arc<dyn ObjectStore>,
    fail_at: usize,
    attempts: AtomicUsize,
}

impl FailOnNthPutStore {
    fn new(inner: Arc<dyn ObjectStore>, fail_at: usize) -> Self {
        Self {
            inner,
            fail_at,
            attempts: AtomicUsize::new(0),
        }
    }

    fn put_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "FailOnNthPutStore",
            source: std::io::Error::other("injected remote PUT failure").into(),
        }
    }
}

impl Display for FailOnNthPutStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "FailOnNthPutStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailOnNthPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == self.fail_at {
            return Err(Self::put_error());
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn seed_checkpoint(store: Arc<dyn ObjectStore>, checkpoint_id: u64) -> ClusterCheckpoint {
    let db = ShardDb::builder("shards/0", store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/rows", b"committed").await.unwrap();
    db.put(b"view/rows/1", b"before-overwrite").await.unwrap();
    db.put(b"view/rows/2", b"deleted").await.unwrap();
    db.put(b"view/rows/1", b"committed").await.unwrap();
    db.delete(b"view/rows/2").await.unwrap();
    db.put(b"view/rows/3", b"committed-second-row")
        .await
        .unwrap();
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
    restore_and_assert(export, target, generation, checkpoint).await;
}

async fn restore_and_assert(
    export: Arc<dyn ObjectStore>,
    target: Arc<dyn ObjectStore>,
    generation: &str,
    checkpoint: ClusterCheckpoint,
) -> serde_json::Value {
    let service = CheckpointExportService::new();
    let validated_export = service
        .validate_generation(export.clone(), generation)
        .await
        .unwrap();
    let restored = service
        .restore_generation(export, target.clone(), generation)
        .await
        .unwrap();
    assert_eq!(
        restored,
        rockstream_control::CheckpointRestoreOutcome {
            checkpoint_id: checkpoint.checkpoint_id.0,
            generation: generation.to_string(),
            object_count: validated_export.object_count,
            byte_count: validated_export.byte_count,
            restored_shards: checkpoint.shards.len(),
            status: "SUCCESS".to_string(),
        }
    );
    let active_generation = target
        .get(&Path::from("control/bootstrap/active-generation"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let active_generation: serde_json::Value = serde_json::from_slice(&active_generation).unwrap();
    assert_eq!(
        active_generation,
        serde_json::json!({
            "checkpoint_id": checkpoint.checkpoint_id,
            "generation": generation,
        })
    );
    let restored_manifest = CheckpointManifestStore::new(target.clone())
        .load_manifest(checkpoint.checkpoint_id)
        .await;
    assert_eq!(restored_manifest, Some(checkpoint.clone()));
    let catalog = target
        .get(&Path::from("control/catalog/views"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(catalog.as_ref(), b"view=v committed_epoch=7");
    let source_resume = target
        .get(&Path::from("control/connectors/source-resume"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(source_resume.as_ref(), b"offset=42");
    let recovery = RecoveryDriver::new();
    recovery.load_checkpoint(checkpoint.clone());
    let shard = recovery
        .recover_shard(ShardId(0), "shards/0", target, LeaseToken(1), LeaseToken(1))
        .await
        .unwrap();
    let row = shard.reader.get(b"view/rows").await.unwrap().unwrap();
    assert_eq!(row.as_ref(), b"committed");
    let overwritten_row = shard.reader.get(b"view/rows/1").await.unwrap().unwrap();
    assert_eq!(overwritten_row.as_ref(), b"committed");
    let deleted_row = shard.reader.get(b"view/rows/2").await.unwrap();
    assert!(deleted_row.is_none());
    let second_row = shard.reader.get(b"view/rows/3").await.unwrap().unwrap();
    assert_eq!(second_row.as_ref(), b"committed-second-row");
    let frontier_key = ShardKeyEncoder::frontier_key();
    let frontier = shard.reader.get(&frontier_key).await.unwrap().unwrap();
    let frontier = u64::from_be_bytes(frontier.as_ref().try_into().unwrap());
    let observed_state = serde_json::json!({
        "checkpoint_manifest": restored_manifest,
        "active_generation": active_generation,
        "frontier": frontier,
        "catalog": String::from_utf8(catalog.to_vec()).unwrap(),
        "source_resume": String::from_utf8(source_resume.to_vec()).unwrap(),
        "rows": {
            "view/rows": String::from_utf8(row.to_vec()).unwrap(),
            "view/rows/1": String::from_utf8(overwritten_row.to_vec()).unwrap(),
            "view/rows/2": deleted_row.map(|bytes| String::from_utf8(bytes.to_vec()).unwrap()),
            "view/rows/3": String::from_utf8(second_row.to_vec()).unwrap(),
        },
    });
    let expected_state = serde_json::json!({
        "checkpoint_manifest": checkpoint.clone(),
        "active_generation": {
            "checkpoint_id": checkpoint.checkpoint_id,
            "generation": generation,
        },
        "frontier": checkpoint.checkpoint_id.0,
        "catalog": "view=v committed_epoch=7",
        "source_resume": "offset=42",
        "rows": {
            "view/rows": "committed",
            "view/rows/1": "committed",
            "view/rows/2": null,
            "view/rows/3": "committed-second-row",
        },
    });
    assert_eq!(observed_state, expected_state);
    serde_json::json!({
        "active_generation": active_generation,
        "expected_state": expected_state,
        "observed_state": observed_state,
    })
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

async fn object_inventory(store: &Arc<dyn ObjectStore>) -> Vec<serde_json::Value> {
    let mut listing = store.list(None);
    let mut inventory = Vec::new();
    while let Some(entry) = listing.next().await {
        let entry = entry.expect("MinIO inventory listing must succeed");
        inventory.push(serde_json::json!({
            "path": entry.location.to_string(),
            "size": entry.size,
            "etag": entry.e_tag,
        }));
    }
    inventory.sort_by_key(|entry| entry["path"].as_str().unwrap().to_string());
    inventory
}

async fn export_inventory_records(
    store: &Arc<dyn ObjectStore>,
    generation: &str,
) -> Vec<serde_json::Value> {
    let prefix = Path::from(format!("checkpoint-exports/{generation}/inventory"));
    let mut listing = store.list(Some(&prefix));
    let mut records = Vec::new();
    while let Some(entry) = listing.next().await {
        let entry = entry.expect("MinIO export inventory listing must succeed");
        let bytes = store
            .get(&entry.location)
            .await
            .expect("MinIO export inventory record must be readable")
            .bytes()
            .await
            .expect("MinIO export inventory record bytes must be readable");
        records.push(serde_json::json!({
            "record_path": entry.location.to_string(),
            "record": serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        }));
    }
    records.sort_by_key(|record| record["record_path"].as_str().unwrap().to_string());
    records
}

fn source_revision() -> String {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn write_export_evidence(name: &str, evidence: serde_json::Value) {
    let artifact_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ci-test-results");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(
        artifact_dir.join(name),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
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
    let row_1 = shard.reader.get(b"view/rows/1").await.unwrap().unwrap();
    let row_2 = shard.reader.get(b"view/rows/2").await.unwrap();
    let row_3 = shard.reader.get(b"view/rows/3").await.unwrap().unwrap();
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
        "rows": {
            "1": String::from_utf8(row_1.to_vec()).unwrap(),
            "2": row_2.map(|value| String::from_utf8(value.to_vec()).unwrap()),
            "3": String::from_utf8(row_3.to_vec()).unwrap(),
        },
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
    let source_inventory = object_inventory(&source).await;
    let generation = "minio-fresh-process";
    let service = CheckpointExportService::new();
    let export_outcome = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source.clone()),
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

    let export_records = export_inventory_records(&export, generation).await;
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
            "rows": {
                "1": "committed",
                "2": null,
                "3": "committed-second-row",
            },
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
            "source_inventory": source_inventory,
            "export_inventory_records": export_records,
            "export_object_inventory": object_inventory(&export).await,
            "restored_by_pid": child_pid,
            "observed_state": observed,
            "target_inventory": object_inventory(&target).await,
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
async fn minio_tc_failed_export_put_has_no_success_and_retry_restores_exact_state() {
    let Some((_container, source, export, target)) = minio_stores("put-failure").await else {
        return;
    };
    let checkpoint = seed_checkpoint(source.clone(), 14).await;
    let manifests = CheckpointManifestStore::new(source.clone());
    let source_before = object_inventory(&source).await;
    let target_before = object_inventory(&target).await;
    let generation = "minio-put-failure";
    let injected = Arc::new(FailOnNthPutStore::new(export.clone(), 2));
    let faulting_destination: Arc<dyn ObjectStore> = injected.clone();
    let service = CheckpointExportService::new();
    let error = service
        .export_latest_prefix(
            source.clone(),
            faulting_destination,
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap_err();
    let expected = format!(
        "RS-3614: checkpoint export object-store operation failed: {}; next_steps: verify source and destination access, then retry the same generation",
        FailOnNthPutStore::put_error()
    );
    assert_eq!(error.to_string(), expected);
    assert_eq!(injected.attempts.load(Ordering::SeqCst), 2);

    let source_after_failure = object_inventory(&source).await;
    let export_after_failure = object_inventory(&export).await;
    let target_after_failure = object_inventory(&target).await;
    assert_eq!(source_after_failure, source_before);
    assert_eq!(
        export_after_failure
            .iter()
            .map(|entry| entry["path"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["checkpoint-exports/minio-put-failure/generation"]
    );
    assert_eq!(target_after_failure, target_before);
    assert!(matches!(
        export
            .head(&Path::from("checkpoint-exports/minio-put-failure/commit"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let retry = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap();
    assert_eq!(retry.checkpoint_id, 14);
    assert_eq!(retry.generation, generation);
    assert_eq!(retry.status, "SUCCESS");
    let restored_state = restore_and_assert(
        export.clone(),
        target.clone(),
        generation,
        checkpoint.clone(),
    )
    .await;

    write_export_evidence(
        "checkpoint-export-minio-put-failure.json",
        serde_json::json!({
            "source_sha": source_revision(),
            "minio_image": "codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
            "source_bucket": "rs-dr-source-put-failure",
            "export_bucket": "rs-dr-export-put-failure",
            "target_bucket": "rs-dr-target-put-failure",
            "checkpoint": checkpoint,
            "generation": generation,
            "fault": {
                "operation": "destination ObjectStore PUT during object copy",
                "attempt": 2,
                "error": error.to_string(),
            },
            "source_inventory_before": source_before,
            "source_inventory_after_failure": source_after_failure,
            "export_inventory_after_failure": export_after_failure,
            "target_inventory_before": target_before,
            "target_inventory_after_failure": target_after_failure,
            "terminal_success_marker_present_after_failure": false,
            "retry_outcome": retry,
            "retry_inventory_records": export_inventory_records(&export, generation).await,
            "export_inventory_after_retry": object_inventory(&export).await,
            "source_inventory_after_retry": object_inventory(&source).await,
            "restored_state": restored_state,
            "source_inventory_after_retry": object_inventory(&source).await,
            "export_inventory_after_retry": object_inventory(&export).await,
            "target_inventory_after_retry": object_inventory(&target).await,
            "terminal_success_marker_present_after_retry": true,
        }),
    );
}

#[cfg(feature = "simulation")]
#[tokio::test]
async fn minio_tc_interrupted_export_has_no_success_and_retry_restores_exact_state() {
    let Some((_container, source, export, target)) = minio_stores("interrupted").await else {
        return;
    };
    let checkpoint = seed_checkpoint(source.clone(), 15).await;
    let manifests = CheckpointManifestStore::new(source.clone());
    let source_before = object_inventory(&source).await;
    let target_before = object_inventory(&target).await;
    let generation = "minio-interrupted";
    let service = CheckpointExportService::new();

    buggify_init(0x5601_0015);
    buggify_focus("dr.export.before_terminal_marker");
    let error = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap_err();
    buggify_disable();
    assert_eq!(
        error.to_string(),
        "RS-3614: checkpoint export was interrupted at dr.export.before_terminal_marker; next_steps: retry the same generation so validated objects can be resumed"
    );

    let source_after_failure = object_inventory(&source).await;
    let export_after_failure = object_inventory(&export).await;
    let target_after_failure = object_inventory(&target).await;
    assert_eq!(source_after_failure, source_before);
    assert!(!export_after_failure
        .iter()
        .any(|entry| entry["path"] == "checkpoint-exports/minio-interrupted/commit"));
    assert!(!export_after_failure.is_empty());
    assert_eq!(target_after_failure, target_before);
    assert!(matches!(
        export
            .head(&Path::from("checkpoint-exports/minio-interrupted/commit"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let retry = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap();
    assert_eq!(retry.checkpoint_id, 15);
    assert_eq!(retry.generation, generation);
    assert_eq!(retry.status, "SUCCESS");
    let restored_state = restore_and_assert(
        export.clone(),
        target.clone(),
        generation,
        checkpoint.clone(),
    )
    .await;

    write_export_evidence(
        "checkpoint-export-minio-interrupted.json",
        serde_json::json!({
            "source_sha": source_revision(),
            "minio_image": "codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
            "source_bucket": "rs-dr-source-interrupted",
            "export_bucket": "rs-dr-export-interrupted",
            "target_bucket": "rs-dr-target-interrupted",
            "checkpoint": checkpoint,
            "generation": generation,
            "fault": "dr.export.before_terminal_marker",
            "error": error.to_string(),
            "source_inventory_before": source_before,
            "source_inventory_after_failure": source_after_failure,
            "export_inventory_after_failure": export_after_failure,
            "target_inventory_before": target_before,
            "target_inventory_after_failure": target_after_failure,
            "terminal_success_marker_present_after_failure": false,
            "retry_outcome": retry,
            "retry_inventory_records": export_inventory_records(&export, generation).await,
            "export_inventory_after_retry": object_inventory(&export).await,
            "source_inventory_after_retry": object_inventory(&source).await,
            "restored_state": restored_state,
            "source_inventory_after_retry": object_inventory(&source).await,
            "export_inventory_after_retry": object_inventory(&export).await,
            "target_inventory_after_retry": object_inventory(&target).await,
            "terminal_success_marker_present_after_retry": true,
        }),
    );
}

#[tokio::test]
async fn minio_tc_truncated_export_fails_closed_rs3614() {
    let Some((_container, source, export, target)) = minio_stores("truncated").await else {
        return;
    };
    let checkpoint = seed_checkpoint(source.clone(), 11).await;
    let source_inventory = object_inventory(&source).await;
    let service = CheckpointExportService::new();
    let export_outcome = service
        .export_latest_prefix(
            source.clone(),
            export.clone(),
            &CheckpointManifestStore::new(source),
            "minio-truncated",
            &Path::from(""),
        )
        .await
        .unwrap();
    let expected_record = export_inventory_records(&export, "minio-truncated")
        .await
        .into_iter()
        .map(|record| record["record"].clone())
        .find(|record| {
            record["destination"]
                == "checkpoint-exports/minio-truncated/objects/00000000000000000000"
        })
        .unwrap();
    let corrupted_path = Path::from(expected_record["destination"].as_str().unwrap());
    export
        .put(
            &corrupted_path,
            bytes::Bytes::from_static(b"truncated").into(),
        )
        .await
        .unwrap();
    let error = service
        .restore_generation(export.clone(), target.clone(), "minio-truncated")
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "RS-3614: checkpoint export integrity validation failed: destination object checkpoint-exports/minio-truncated/objects/00000000000000000000 failed validation; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
    );
    assert!(target.list(None).next().await.is_none());
    write_export_evidence(
        "checkpoint-export-minio-truncated.json",
        serde_json::json!({
            "source_sha": source_revision(),
            "minio_image": "codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
            "source_bucket": "rs-dr-source-truncated",
            "export_bucket": "rs-dr-export-truncated",
            "target_bucket": "rs-dr-target-truncated",
            "checkpoint": checkpoint,
            "generation": "minio-truncated",
            "export_outcome": export_outcome,
            "source_inventory": source_inventory,
            "export_inventory_records": export_inventory_records(&export, "minio-truncated").await,
            "corruption": {
                "path": corrupted_path.to_string(),
                "expected_record": expected_record,
                "written_bytes": "truncated",
            },
            "export_inventory_after_corruption": object_inventory(&export).await,
            "target_inventory_after_failure": object_inventory(&target).await,
            "failure": error.to_string(),
            "target_unchanged": true,
        }),
    );
}

#[tokio::test]
async fn minio_tc_export_rejects_missing_committed_shard_objects_before_success() {
    let Some((_container, source, export, target)) = minio_stores("missing-shard").await else {
        return;
    };
    let checkpoint = seed_checkpoint(source.clone(), 13).await;
    let manifests = CheckpointManifestStore::new(source.clone());
    let shard_prefix = Path::from("shards/0");
    let mut listing = source.list(Some(&shard_prefix));
    let mut paths = Vec::new();
    let mut inventory_before = Vec::new();
    while let Some(entry) = listing.next().await {
        let entry = entry.expect("MinIO shard inventory must list successfully");
        paths.push(entry.location.clone());
        inventory_before.push(serde_json::json!({
            "path": entry.location.to_string(),
            "size": entry.size,
            "etag": entry.e_tag,
        }));
    }
    drop(listing);
    assert!(
        !paths.is_empty(),
        "checkpoint must reference MinIO shard objects"
    );
    for path in paths {
        source.delete(&path).await.unwrap();
    }
    assert_eq!(
        manifests.load_latest_manifest().await.unwrap(),
        Some(checkpoint.clone())
    );
    assert!(source.list(Some(&shard_prefix)).next().await.is_none());

    let generation = "minio-missing-shard";
    let error = CheckpointExportService::new()
        .export_latest_prefix(
            source,
            export.clone(),
            &manifests,
            generation,
            &Path::from(""),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "RS-3614: checkpoint export integrity validation failed: SlateDB database for shard `shards/0` is missing; next_steps: discard the incomplete generation and retry from the same committed checkpoint"
    );
    let marker = Path::from("checkpoint-exports/minio-missing-shard/commit");
    assert!(matches!(
        export.head(&marker).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(export.list(None).next().await.is_none());
    assert!(target.list(None).next().await.is_none());

    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let artifact_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ci-test-results");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(
        artifact_dir.join("checkpoint-export-minio-missing-object.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "source_sha": String::from_utf8(revision.stdout).unwrap().trim(),
            "minio_image": "codemowers/minio:RELEASE.2025-09-07T16-13-09Z",
            "source_bucket": "rs-dr-source-missing-shard",
            "export_bucket": "rs-dr-export-missing-shard",
            "target_bucket": "rs-dr-target-missing-shard",
            "checkpoint": checkpoint,
            "generation": generation,
            "fault": "deleted all committed shard objects before export",
            "source_inventory_before": inventory_before,
            "source_inventory_after": [],
            "export_inventory_after": [],
            "target_inventory_after": [],
            "failure": error.to_string(),
            "terminal_success_marker_present": false,
        }))
        .unwrap(),
    )
    .unwrap();
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
