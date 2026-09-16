use std::sync::Arc;

use object_store::{prefix::PrefixStore, ObjectStore};
use rockstream_control::{
    CheckpointManifestStore, ControlService, ManagementOperationStore, ManagementService,
    OperationKind, OperationStatus, OperationUpdate, ShardManager, TopologyCatalog,
};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CreateBackupRequest, GetOperationRequest,
    Operation,
};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::config::NodeConfig;
use rockstream_types::ids::{ShardId, WorkerId};
use tokio::net::TcpListener;

async fn management_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn local_store(path: &std::path::Path) -> Arc<dyn ObjectStore> {
    Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path).unwrap())
}

async fn await_succeeded(
    client: &mut ManagementServiceClient<tonic::transport::Channel>,
    operation_id: &str,
) -> Operation {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let operation = client
                .get_operation(GetOperationRequest {
                    protocol_version: 1,
                    operation_id: operation_id.to_owned(),
                })
                .await
                .unwrap()
                .into_inner()
                .operation
                .unwrap();
            if operation.state == "succeeded" {
                return operation;
            }
            assert_ne!(
                operation.state, "failed",
                "backup operation failed: {operation:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("backup should finish")
}

#[tokio::test]
async fn create_backup_snapshots_real_worker_shard_and_is_idempotent() {
    let storage = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let source_store = local_store(storage.path());
    let management_addr = management_addr().await;
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let control = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_single_worker_local_backup_source_store(source_store.clone(), WorkerId(17))
        .with_management(
            management_addr.clone(),
            source_store.clone(),
            NodeConfig::default(),
        );
    let control = control.start("127.0.0.1:0").await.unwrap();
    let control_url = control.addr.to_string();
    let (worker, worker_task) =
        rockstream_runtime::start_worker_client(17, &control_url, storage.path())
            .await
            .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while worker.worker_id().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    worker.request_shard(ShardId(9)).await.unwrap();
    let db = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let Some(db) = worker.get_shard_db(ShardId(9)) {
                break db;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    db.put(b"backup-test-key", b"durable-value").await.unwrap();
    db.flush().await.unwrap();
    assert_eq!(
        db.get(b"backup-test-key").await.unwrap().unwrap().as_ref(),
        b"durable-value"
    );

    let mut client = ManagementServiceClient::connect(format!("http://{management_addr}"))
        .await
        .unwrap();
    let backup_response = client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: destination.path().display().to_string(),
            idempotency_key: "backup-exact-key".to_owned(),
        })
        .await;
    assert!(backup_response.is_ok());
    let accepted = backup_response.unwrap().into_inner();
    assert_eq!(accepted.protocol_version, 1);
    let accepted_operation = accepted.operation.unwrap();
    assert_eq!(accepted_operation.kind, "create_backup");
    assert_eq!(accepted_operation.source_version, "operation-record:2");
    assert!(!accepted_operation.operation_id.is_empty());
    let completed = await_succeeded(&mut client, &accepted_operation.operation_id).await;
    assert_eq!(completed.operation_id, accepted_operation.operation_id);
    assert_eq!(completed.kind, "create_backup");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.started_at, accepted_operation.started_at);
    assert!(completed
        .updated_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.error_code, "");
    assert!(completed.next_steps.is_empty());
    assert_eq!(completed.source_version, "operation-record:2");
    let replay = client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: destination.path().display().to_string(),
            idempotency_key: "backup-exact-key".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(replay, completed);

    let manifest = CheckpointManifestStore::new(source_store.clone())
        .load_latest_manifest()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        manifest.shards.keys().copied().collect::<Vec<_>>(),
        vec![ShardId(9)]
    );
    assert_eq!(
        manifest.shards[&ShardId(9)].checkpoint_id,
        manifest.checkpoint_id
    );
    assert!(manifest.shards[&ShardId(9)].snapshot_id.is_some());
    let destination_store = local_store(destination.path());
    let outcome = rockstream_control::checkpoint_export::CheckpointExportService::new()
        .validate_generation(
            destination_store,
            &format!("management-{}", completed.operation_id),
        )
        .await
        .unwrap();
    assert_eq!(outcome.checkpoint_id, manifest.checkpoint_id.0);
    assert_eq!(
        outcome.generation,
        format!("management-{}", completed.operation_id)
    );
    assert_eq!(outcome.status, "SUCCESS");
    assert!(outcome.object_count > 0);
    assert!(outcome.byte_count > 0);
    assert_eq!(outcome.inventory_digest.len(), 64);
    let restored_dir = tempfile::tempdir().unwrap();
    let restored_store = local_store(restored_dir.path());
    let restore = rockstream_control::checkpoint_export::CheckpointExportService::new()
        .restore_generation(
            local_store(destination.path()),
            restored_store.clone(),
            &outcome.generation,
        )
        .await
        .unwrap();
    assert_eq!(restore.checkpoint_id, manifest.checkpoint_id.0);
    assert_eq!(restore.generation, outcome.generation);
    assert_eq!(restore.restored_shards, 1);
    assert_eq!(restore.status, "SUCCESS");
    let snapshot_id = manifest.shards[&ShardId(9)].snapshot_id.as_deref().unwrap();
    let source_shard_store: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(source_store.clone(), "shards/9"));
    let source_snapshot_reader = rockstream_storage::ShardReader::open_with_snapshot_id(
        "db",
        source_shard_store,
        snapshot_id,
    )
    .await
    .unwrap();
    assert_eq!(
        source_snapshot_reader
            .get(b"backup-test-key")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"durable-value"
    );
    let restored_shard_store: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(restored_store.clone(), "shards/9"));
    let snapshot_reader = rockstream_storage::ShardReader::open_with_snapshot_id(
        "db",
        restored_shard_store.clone(),
        snapshot_id,
    )
    .await
    .unwrap();
    assert_eq!(
        snapshot_reader
            .get(b"backup-test-key")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"durable-value"
    );
    worker_task.abort();
    control.shutdown();
}

#[tokio::test]
async fn create_backup_reconciles_persisted_exporting_record_from_manifest() {
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let source = local_store(source_dir.path());
    let shard_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(source.clone(), "shards/3"));
    let db = rockstream_storage::ShardDb::builder("db", shard_store)
        .build()
        .await
        .unwrap();
    db.put(b"restart-key", b"restart-value").await.unwrap();
    let handle = db.create_checkpoint().await.unwrap();
    db.close().await.unwrap();
    let checkpoint_id = CheckpointId(44);
    let mut manifest = ClusterCheckpoint::new(checkpoint_id);
    manifest.record_shard(
        ShardId(3),
        PerShardCheckpoint::new(checkpoint_id, handle.shard_checkpoint_id)
            .with_snapshot_id(handle.snapshot_id),
    );
    CheckpointManifestStore::new(source.clone())
        .save_manifest(&manifest, false, None)
        .await
        .unwrap();

    let operations = ManagementOperationStore::new(source.clone());
    let destination_text = destination.path().display().to_string();
    let accepted = operations
        .accept_idempotent(
            "restart-backup-key",
            1,
            &serde_json::json!({ "destination": destination_text }),
            "restart-backup-operation",
            OperationKind::CreateBackup,
            10,
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), OperationStatus::Pending);
    operations
        .transition(
            accepted.operation_id(),
            OperationUpdate {
                status: OperationStatus::Running,
                updated_at_ms: 11,
                progress: Some(70),
                phase: Some("backup_exporting:44".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();

    let management_addr = management_addr().await;
    let service = ControlService::new(TopologyCatalog::new())
        .with_single_worker_local_backup_source_store(source.clone(), WorkerId(1))
        .with_management(
            management_addr.clone(),
            source.clone(),
            NodeConfig::default(),
        );
    let running = service.start("127.0.0.1:0").await.unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{management_addr}"))
        .await
        .unwrap();
    let operation = await_succeeded(&mut client, "restart-backup-operation").await;
    assert_eq!(operation.operation_id, "restart-backup-operation");
    assert_eq!(operation.kind, "create_backup");
    assert_eq!(operation.state, "succeeded");
    assert_eq!(operation.progress, "100%");
    assert_eq!(operation.phase, "completed");
    assert_eq!(operation.error_code, "");
    assert!(operation.next_steps.is_empty());

    let destination_store = local_store(destination.path());
    let outcome = rockstream_control::checkpoint_export::CheckpointExportService::new()
        .validate_generation(destination_store, "management-restart-backup-operation")
        .await
        .unwrap();
    assert_eq!(outcome.checkpoint_id, 44);
    assert_eq!(outcome.generation, "management-restart-backup-operation");
    assert_eq!(outcome.status, "SUCCESS");
    assert!(outcome.object_count > 0);
    assert!(outcome.byte_count > 0);
    assert_eq!(outcome.inventory_digest.len(), 64);
    running.shutdown();
}

#[tokio::test]
async fn create_backup_is_unavailable_without_a_real_source_store() {
    let operations = ManagementOperationStore::new(Arc::new(object_store::memory::InMemory::new()));
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations.clone(),
        NodeConfig::default(),
    );
    let running = service.start("127.0.0.1:0").await.unwrap();
    let mut client = ManagementServiceClient::connect(format!("http://{}", running.addr))
        .await
        .unwrap();
    let error = client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: "file:///tmp/backup".to_owned(),
            idempotency_key: "no-source-key".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "CreateBackup executor is not attached");
    assert!(operations.nonterminal().await.unwrap().is_empty());
    running.shutdown();
}
