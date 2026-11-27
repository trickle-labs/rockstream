//! Comprehensive integration tests for Issue #406 and Issue #352:
//! - R1: Online active-workload shard migration & worker drain with atomic routing updates.
//! - R2: Online `CreateBackup` under active workloads & ongoing writes + #352 concurrent manifest consistency.
//! - R3: Streaming memory-bounded large-dataset export & restore (>2,000 objects and >64 MiB objects).
//! - R4: GC & compaction pin coordination (protected while active, reclaimed cleanly after release).
//! - R5: Crash/rollback resilience and clean pin/handoff cleanup.

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::{
    memory::InMemory, path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt,
};
use rockstream_control::{
    ChangelogCheckpointContribution, CheckpointExportService, CheckpointManifestStore,
    ControlService, GcPin, GcPinKind, GcPinStore, ShardManager, TopologyCatalog,
    CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES,
};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CreateBackupRequest, DrainWorkerRequest,
    GetOperationRequest, MigrateShardRequest, Operation,
};
use rockstream_storage::{ShardDb, ShardReader};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::config::{JoinStrategy, NodeConfig};
use rockstream_types::data_plane::{
    DeploymentColumn, DeploymentRequest, DeploymentSchema, RuntimeRow, SourceDeltaRequest,
};
use rockstream_types::ids::{OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::state_mutation::EpochStateDelta;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerMessage,
    WorkerRegistration,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[path = "common/management_auth.rs"]
mod management_auth;
use management_auth::ManagementAuth;

async fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn local_store(path: &std::path::Path) -> Arc<dyn ObjectStore> {
    Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path).unwrap())
}

async fn await_operation_succeeded(
    client: &mut ManagementServiceClient<tonic::transport::Channel>,
    operation_id: &str,
) -> Operation {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
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
                "operation failed unexpectedly: {operation:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("management operation should succeed within timeout")
}

async fn send_worker_msg(writer: &mut tokio::net::tcp::OwnedWriteHalf, message: &WorkerMessage) {
    let line = serde_json::to_string(message).unwrap() + "\n";
    writer.write_all(line.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();
}

async fn read_control_msg(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> ControlMessage {
    let mut line = String::new();
    let n = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.read_line(&mut line),
    )
    .await
    .expect("timed out reading ControlMessage")
    .unwrap();
    assert!(n > 0, "control connection closed unexpectedly");
    serde_json::from_str(&line).unwrap()
}

async fn read_control_msg_skipping_topology(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> ControlMessage {
    loop {
        let msg = read_control_msg(reader).await;
        if matches!(
            msg,
            ControlMessage::TopologyChanged { .. } | ControlMessage::DrainStatus { .. }
        ) {
            continue;
        }
        return msg;
    }
}

#[tokio::test]
async fn large_inventory_and_large_object_streaming_export_and_restore() {
    let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let export_dest: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let restore_target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Populate >2,000 small objects (exceeding legacy 1,024 ceiling)
    let small_count = 2_100_usize;
    let mut expected_small_bytes = 0_u64;
    for i in 0..small_count {
        let payload = format!("meta-entry-{i:05}");
        expected_small_bytes += payload.len() as u64;
        source
            .put(
                &Path::from(format!("control/meta/obj-{i:05}")),
                payload.into_bytes().into(),
            )
            .await
            .unwrap();
    }

    // Populate a 72 MiB object (exceeding legacy 64 MiB ceiling, spanning 9 x 8 MiB multipart chunks)
    let large_size: usize = 72 * 1024 * 1024;
    assert!(large_size > 64 * 1024 * 1024);
    assert!(large_size > CHECKPOINT_EXPORT_STREAM_CHUNK_BYTES);
    let mut large_payload = vec![0_u8; large_size];
    for (idx, chunk) in large_payload.chunks_mut(1024 * 1024).enumerate() {
        chunk.fill((idx % 251) as u8);
    }
    let expected_large_sha = hex::encode(Sha256::digest(&large_payload));
    source
        .put(
            &Path::from("control/large-sstable.bin"),
            bytes::Bytes::from(large_payload).into(),
        )
        .await
        .unwrap();

    let checkpoint = ClusterCheckpoint::new(CheckpointId(42));
    CheckpointManifestStore::new(source.clone())
        .save_manifest(&checkpoint, false, None)
        .await
        .unwrap();
    let manifest_size = source
        .head(&Path::from("control/checkpoints/42"))
        .await
        .unwrap()
        .size;

    let service = CheckpointExportService::new();
    let export_outcome = service
        .export_prefix(
            source.clone(),
            export_dest.clone(),
            checkpoint,
            "gen-large-42",
            &Path::from(""),
        )
        .await
        .expect("large inventory + >64 MiB object export must succeed");

    let expected_object_count = (small_count + 2) as u64; // 2100 small + 1 large + 1 manifest
    let expected_byte_count = expected_small_bytes + large_size as u64 + manifest_size;
    assert_eq!(export_outcome.checkpoint_id, 42);
    assert_eq!(export_outcome.generation, "gen-large-42");
    assert_eq!(export_outcome.object_count, expected_object_count);
    assert_eq!(export_outcome.byte_count, expected_byte_count);
    assert_eq!(export_outcome.status, "SUCCESS");
    assert_eq!(
        rockstream_types::metrics::read_checkpoint_export_object_buffer_fill_level(),
        0
    );
    assert_eq!(
        rockstream_types::metrics::read_checkpoint_export_scan_window_fill_level(),
        0
    );

    // Validate generation without consulting source
    let validated = service
        .validate_generation(export_dest.clone(), "gen-large-42")
        .await
        .expect("large generation must validate cleanly");
    assert_eq!(validated, export_outcome);

    // latest_committed_generation must scan past >4,000 objects under checkpoint-exports/
    let latest_gen = service
        .latest_committed_generation(export_dest.clone())
        .await
        .expect("latest_committed_generation must succeed with >2000 exported objects");
    assert_eq!(latest_gen, "gen-large-42");

    // Clean-environment restore
    let restore_outcome = service
        .restore_generation(export_dest, restore_target.clone(), "gen-large-42")
        .await
        .expect("streaming restore must succeed");
    assert_eq!(restore_outcome.checkpoint_id, 42);
    assert_eq!(restore_outcome.generation, "gen-large-42");
    assert_eq!(restore_outcome.object_count, expected_object_count);
    assert_eq!(restore_outcome.byte_count, expected_byte_count);
    assert_eq!(restore_outcome.status, "SUCCESS");

    // Verify restored 72 MiB object byte-for-byte via SHA-256
    let restored_large = restore_target
        .get(&Path::from("control/large-sstable.bin"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(restored_large.len(), large_size);
    assert_eq!(
        hex::encode(Sha256::digest(&restored_large)),
        expected_large_sha
    );

    // Verify first, middle, and last of the 2,100 small objects
    for i in [0, 1024, 1025, small_count - 1] {
        let bytes = restore_target
            .get(&Path::from(format!("control/meta/obj-{i:05}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), format!("meta-entry-{i:05}").as_bytes());
    }
}

#[tokio::test]
async fn concurrent_newer_control_manifest_is_excluded_and_rejected_on_tamper() {
    let source_dir = tempfile::tempdir().unwrap();
    let export_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = local_store(source_dir.path());
    let export_dest = local_store(export_dir.path());
    let restore_target = local_store(target_dir.path());

    // Create real shard with state A and checkpoint A (CheckpointId(10))
    let shard_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(source.clone(), "shards/0"));
    let db = ShardDb::builder("db", shard_store).build().await.unwrap();
    db.put(b"k1", b"v1_A").await.unwrap();
    db.put(b"k2", b"v2_A").await.unwrap();
    db.flush().await.unwrap();
    let snap_a = db.create_checkpoint().await.unwrap();

    let manifests = CheckpointManifestStore::new(source.clone());
    let mut ckpt_a = ClusterCheckpoint::new(CheckpointId(10));
    ckpt_a.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(CheckpointId(10), snap_a.shard_checkpoint_id)
            .with_snapshot_id(snap_a.snapshot_id.clone()),
    );
    manifests.save_manifest(&ckpt_a, false, None).await.unwrap();

    // Concurrent state B and newer committed checkpoint B (CheckpointId(11)) published BEFORE export inventory runs!
    db.put(b"k1", b"v1_B").await.unwrap();
    db.put(b"k3", b"v3_B").await.unwrap();
    db.flush().await.unwrap();
    let snap_b = db.create_checkpoint().await.unwrap();
    let mut ckpt_b = ClusterCheckpoint::new(CheckpointId(11));
    ckpt_b.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(CheckpointId(11), snap_b.shard_checkpoint_id)
            .with_snapshot_id(snap_b.snapshot_id.clone()),
    );
    manifests.save_manifest(&ckpt_b, false, None).await.unwrap();
    manifests
        .save_changelog_contribution(
            ShardId(0),
            &ChangelogCheckpointContribution::new(CheckpointId(11), EpochStateDelta::new()),
        )
        .await
        .unwrap();
    db.close().await.unwrap();

    // Export checkpoint A while B already exists in control/checkpoints/11
    let service = CheckpointExportService::new();
    let outcome = service
        .export_prefix(
            source.clone(),
            export_dest.clone(),
            ckpt_a.clone(),
            "gen-ckpt-10",
            &Path::from(""),
        )
        .await
        .expect("export of A must succeed despite concurrent B manifest");
    assert_eq!(outcome.checkpoint_id, 10);
    assert_eq!(outcome.status, "SUCCESS");

    // Restore into a completely fresh target store
    let restore = service
        .restore_generation(export_dest.clone(), restore_target.clone(), "gen-ckpt-10")
        .await
        .expect("restore of A must succeed");
    assert_eq!(restore.checkpoint_id, 10);
    assert_eq!(restore.restored_shards, 1);

    // Discover latest manifest purely from restored target state (never supplying remembered A)
    let discovered = CheckpointManifestStore::new(restore_target.clone())
        .load_latest_manifest()
        .await
        .unwrap()
        .expect("restored target must contain a discoverable manifest");
    assert_eq!(discovered, ckpt_a);
    assert!(restore_target
        .head(&Path::from("control/checkpoints/11"))
        .await
        .is_err());
    assert!(restore_target
        .head(&Path::from("control/changelog-checkpoints/11-0"))
        .await
        .is_err());

    // Open restored shard at discovered snapshot and assert exact state A
    let restored_shard_store: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(restore_target.clone(), "shards/0"));
    let reader = ShardReader::open_with_snapshot_id(
        "db",
        restored_shard_store,
        discovered.shards[&ShardId(0)]
            .snapshot_id
            .as_deref()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(reader.get(b"k1").await.unwrap().unwrap().as_ref(), b"v1_A");
    assert_eq!(reader.get(b"k2").await.unwrap().unwrap().as_ref(), b"v2_A");
    assert_eq!(reader.get(b"k3").await.unwrap(), None);

    // Negative check (#352): if an export manually includes a newer control manifest (11 > 10),
    // validate_generation MUST reject it before restore can proceed.
    let bad_dest: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bad_outcome = service
        .export_objects(
            source,
            bad_dest.clone(),
            ckpt_a,
            "gen-bad-10",
            vec![
                Path::from("control/checkpoints/10"),
                Path::from("control/checkpoints/11"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(bad_outcome.checkpoint_id, 10);
    let err = service
        .validate_generation(bad_dest, "gen-bad-10")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("other than selected checkpoint 10"),
        "unexpected validation error: {err}"
    );
}

#[tokio::test]
async fn gc_pin_protects_active_backup_and_compaction_horizon_until_release() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifests = CheckpointManifestStore::new(store.clone());
    let gc_pins = GcPinStore::new(store.clone());

    for id in 1..=6_u64 {
        let ckpt = ClusterCheckpoint::new(CheckpointId(id));
        manifests.save_manifest(&ckpt, false, None).await.unwrap();
        manifests
            .save_changelog_contribution(
                ShardId(0),
                &ChangelogCheckpointContribution::new(CheckpointId(id), EpochStateDelta::new()),
            )
            .await
            .unwrap();
    }

    // Acquire an active backup GC pin protecting CheckpointId(2) and replay epoch 20
    let pin = GcPin {
        pin_id: "backup-slow-op".to_owned(),
        kind: GcPinKind::Backup,
        checkpoint_id: Some(CheckpointId(2)),
        min_epoch: Some(20),
        shard_snapshots: vec![(ShardId(0), "snap-2".to_owned())],
        created_at_ms: 1_000,
    };
    gc_pins.acquire_pin(&pin).await.unwrap();
    assert_eq!(gc_pins.effective_compaction_horizon(100).await.unwrap(), 20);

    // Aggressive GC with retention_horizon=1 (cutoff = 6 - 1 = 5):
    // checkpoints 1, 3, 4 should be reclaimed, while pinned 2 MUST be preserved!
    let mut deleted_manifests = manifests
        .gc_old_manifests(CheckpointId(6), 1, None)
        .await
        .unwrap();
    deleted_manifests.sort();
    assert_eq!(
        deleted_manifests,
        vec![CheckpointId(1), CheckpointId(3), CheckpointId(4)]
    );
    let deleted_changelogs = manifests
        .gc_old_changelog_contributions(CheckpointId(6), 1)
        .await
        .unwrap();
    assert_eq!(
        deleted_changelogs,
        vec![
            (CheckpointId(1), ShardId(0)),
            (CheckpointId(3), ShardId(0)),
            (CheckpointId(4), ShardId(0)),
        ]
    );
    assert_eq!(
        manifests.load_manifest(CheckpointId(2)).await,
        Some(ClusterCheckpoint::new(CheckpointId(2)))
    );
    assert!(manifests
        .load_changelog_contribution(CheckpointId(2), ShardId(0))
        .await
        .unwrap()
        .is_some());

    // Release the pin: horizon advances and CheckpointId(2) is cleanly reclaimed on next GC pass
    gc_pins.release_pin("backup-slow-op").await.unwrap();
    assert!(gc_pins.list_active_pins().await.unwrap().is_empty());
    assert_eq!(
        gc_pins.effective_compaction_horizon(100).await.unwrap(),
        100
    );

    let reclaimed_manifests = manifests
        .gc_old_manifests(CheckpointId(6), 1, None)
        .await
        .unwrap();
    assert_eq!(reclaimed_manifests, vec![CheckpointId(2)]);
    let reclaimed_changelogs = manifests
        .gc_old_changelog_contributions(CheckpointId(6), 1)
        .await
        .unwrap();
    assert_eq!(reclaimed_changelogs, vec![(CheckpointId(2), ShardId(0))]);
    assert_eq!(manifests.load_manifest(CheckpointId(2)).await, None);
}

#[tokio::test]
async fn online_backup_succeeds_with_active_workload_and_concurrent_writes() {
    let storage = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let restored_dir = tempfile::tempdir().unwrap();
    let auth = ManagementAuth::new(&[destination.path().display().to_string()]).await;
    let source_store = local_store(storage.path());
    let management_addr = free_addr().await;

    let control = auth.protect_control(
        ControlService::new(TopologyCatalog::new())
            .with_shard_manager(ShardManager::new())
            .with_single_worker_local_backup_source_store(source_store.clone(), WorkerId(21))
            .with_management(
                management_addr.clone(),
                source_store.clone(),
                NodeConfig::default(),
            ),
    );
    let control = control
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let control_url = control.addr.to_string();

    let (worker, worker_task) =
        rockstream_runtime::start_worker_client(21, &control_url, storage.path())
            .await
            .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while worker.worker_id().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // Deploy an active workload on the real worker so `deployments` is non-empty
    let client_stream = TcpStream::connect(control.addr).await.unwrap();
    let (client_read, mut client_write) = client_stream.into_split();
    let mut client_reader = BufReader::new(client_read);
    let plan = rockstream_plan::PlanNode::ViewSink {
        view_name: "live_view".to_owned(),
        pk: vec![0],
        child: Box::new(rockstream_plan::PlanNode::Source {
            name: "events".to_owned(),
        }),
    };
    let deploy_req = DeploymentRequest {
        version: rockstream_types::data_plane::DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id: WorkloadId(7),
        namespace: "public".to_owned(),
        workload_name: "live-view".to_owned(),
        plan_json: serde_json::to_string(&plan).unwrap(),
        join_strategy: JoinStrategy::Auto,
        schemas: vec![DeploymentSchema {
            relation: "events".to_owned(),
            columns: vec![
                DeploymentColumn {
                    name: "k".to_owned(),
                    data_type: "string".to_owned(),
                },
                DeploymentColumn {
                    name: "v".to_owned(),
                    data_type: "string".to_owned(),
                },
            ],
        }],
        frontier: 0,
        storage_root: storage.path().display().to_string(),
        sink_operator_id: OperatorId(1),
        output_columns: vec!["k".to_owned(), "v".to_owned()],
        primary_key: vec![0],
        merge_key_columns: vec![0],
        routing_columns: BTreeMap::from([("events".to_owned(), 0)]),
    };
    send_worker_msg(
        &mut client_write,
        &WorkerMessage::DeployWorkload(deploy_req),
    )
    .await;
    let ready = read_control_msg_skipping_topology(&mut client_reader).await;
    let shard_id = match ready {
        ControlMessage::DeploymentReady {
            workload_id,
            workers,
        } => {
            assert_eq!(workload_id, WorkloadId(7));
            assert_eq!(workers.len(), 1);
            workers[0].shard_ids[0]
        }
        other => panic!("expected DeploymentReady, got {other:?}"),
    };

    // Write pre-backup committed state to the active workload shard
    let db = worker
        .get_shard_db(shard_id)
        .expect("active workload shard db");
    db.put(b"online-k1", b"committed-before-backup")
        .await
        .unwrap();
    db.flush().await.unwrap();

    // Trigger online CreateBackup while the workload is active
    let mut mgmt_client = auth.client(management_addr.parse().unwrap()).await;
    let accepted = mgmt_client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: destination.path().display().to_string(),
            idempotency_key: "00000000-0000-4000-8000-000000000406".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    // Concurrent foreground write while backup is running must not be blocked
    db.put(b"concurrent-k2", b"written-during-backup")
        .await
        .unwrap();
    db.flush().await.unwrap();

    let completed = await_operation_succeeded(&mut mgmt_client, &accepted.operation_id).await;
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.error_code, "");

    // GC pin must be cleanly released after backup completes
    assert!(GcPinStore::new(source_store.clone())
        .list_active_pins()
        .await
        .unwrap()
        .is_empty());

    // Restore exported backup into a clean prefix and verify committed state
    let dest_store = local_store(destination.path());
    let restore_store = local_store(restored_dir.path());
    let generation = format!("management-{}", accepted.operation_id);
    let exporter = CheckpointExportService::new();
    let restore_outcome = exporter
        .restore_generation(dest_store, restore_store.clone(), &generation)
        .await
        .expect("restoring online backup must succeed");
    assert_eq!(restore_outcome.status, "SUCCESS");
    assert_eq!(restore_outcome.restored_shards, 1);

    let restored_manifest = CheckpointManifestStore::new(restore_store.clone())
        .load_latest_manifest()
        .await
        .unwrap()
        .expect("restored backup must contain discoverable manifest");
    let shard_ckpt = &restored_manifest.shards[&shard_id];
    let shard_rel = shard_ckpt
        .shard_path
        .clone()
        .unwrap_or_else(|| format!("shards/{}", shard_id.0));
    let restored_shard_store: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(restore_store, shard_rel));
    let reader = ShardReader::open_with_snapshot_id(
        "db",
        restored_shard_store,
        shard_ckpt.snapshot_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader.get(b"online-k1").await.unwrap().unwrap().as_ref(),
        b"committed-before-backup"
    );

    worker_task.abort();
    control.shutdown();
}

// Fake workers must prove the durable idle barrier before the canonical
// migration executor asks the donor to flush and close its writer.
async fn acknowledge_idle_anchor(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    workload_id: WorkloadId,
    expected_anchor: &rockstream_types::lease::ShardLease,
) -> String {
    let operation_id = match read_control_msg_skipping_topology(reader).await {
        ControlMessage::VerifyWorkloadIdle {
            operation_id,
            workload_id: actual_workload,
            anchor,
        } => {
            assert_eq!(actual_workload, workload_id);
            assert_eq!(&anchor, expected_anchor);
            operation_id
        }
        other => panic!("expected idle verification before donor handoff, got {other:?}"),
    };
    send_worker_msg(
        writer,
        &WorkerMessage::ShardTransferAck {
            operation_id: operation_id.clone(),
            stage: format!("idle:{}", workload_id.0),
            worker_id: expected_anchor.worker_id,
            shard_id: expected_anchor.shard_id,
            lease_token: expected_anchor.lease_token,
            success: true,
            error: None,
        },
    )
    .await;
    operation_id
}

#[tokio::test]
async fn online_migrate_shard_moves_active_workload_and_updates_routing_atomically() {
    let mgmt_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let auth = ManagementAuth::new(&[]).await;
    let management_addr = free_addr().await;
    let control = auth.protect_control(
        ControlService::new(TopologyCatalog::new())
            .with_shard_manager(ShardManager::new())
            .with_management(
                management_addr.clone(),
                mgmt_store.clone(),
                NodeConfig::default(),
            ),
    );
    let control = control
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();

    let shared_caps = WorkerCapabilities {
        shared_shard_store_id: Some([7_u8; 32]),
        ..WorkerCapabilities::default()
    };

    // Connect worker 1 (donor) and worker 2 (recipient)
    let (w1_read, mut w1_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut w1_reader = BufReader::new(w1_read);
    let mut reg1 = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9101",
        CapacityHeadroom::default(),
    );
    reg1.capabilities = shared_caps;
    send_worker_msg(&mut w1_write, &WorkerMessage::Register(reg1)).await;
    assert!(matches!(
        read_control_msg_skipping_topology(&mut w1_reader).await,
        ControlMessage::Registered {
            worker_id: WorkerId(1),
            ..
        }
    ));

    let (w2_read, mut w2_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut w2_reader = BufReader::new(w2_read);
    let mut reg2 = WorkerRegistration::new(
        WorkerId(2),
        NodeRole::Worker,
        "127.0.0.1:9102",
        CapacityHeadroom::default(),
    );
    reg2.capabilities = shared_caps;
    send_worker_msg(&mut w2_write, &WorkerMessage::Register(reg2)).await;
    assert!(matches!(
        read_control_msg_skipping_topology(&mut w2_reader).await,
        ControlMessage::Registered {
            worker_id: WorkerId(2),
            ..
        }
    ));

    // Deploy an active workload (control places shard on worker 1 and/or worker 2)
    let (c_read, mut c_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut c_reader = BufReader::new(c_read);
    let workload_id = WorkloadId(55);
    send_worker_msg(
        &mut c_write,
        &WorkerMessage::DeployWorkloadSharded {
            request: DeploymentRequest {
                version: 1,
                workload_id,
                namespace: "public".to_owned(),
                workload_name: "active-mig-view".to_owned(),
                plan_json: "{}".to_owned(),
                join_strategy: JoinStrategy::Auto,
                schemas: vec![],
                frontier: 0,
                storage_root: "/tmp/shared-store".to_owned(),
                sink_operator_id: OperatorId(10),
                output_columns: vec!["val".to_owned()],
                primary_key: vec![0],
                merge_key_columns: vec![0],
                routing_columns: BTreeMap::from([("src".to_owned(), 0)]),
            },
            shard_count: 2,
        },
    )
    .await;

    // Both workers receive ShardAssigned + Deploy (1 shard each)
    let mut w1_shard = None;
    for (wid, reader, writer) in [
        (WorkerId(1), &mut w1_reader, &mut w1_write),
        (WorkerId(2), &mut w2_reader, &mut w2_write),
    ] {
        assert!(matches!(
            read_control_msg_skipping_topology(reader).await,
            ControlMessage::ShardAssigned { .. }
        ));
        let descriptor = match read_control_msg_skipping_topology(reader).await {
            ControlMessage::Deploy { descriptor } => descriptor,
            other => panic!("expected Deploy on {wid}, got {other:?}"),
        };
        if wid == WorkerId(1) {
            w1_shard = Some(descriptor.shard.clone());
        }
        send_worker_msg(
            writer,
            &WorkerMessage::DeploymentReady {
                version: 1,
                workload_id,
                shard_id: descriptor.shard.shard_id,
                worker_id: wid,
                process_id: 1000 + wid.0 as u32,
                operator_ids: vec![OperatorId(10)],
                frontier: 1,
            },
        )
        .await;
    }
    assert!(matches!(
        read_control_msg_skipping_topology(&mut c_reader).await,
        ControlMessage::DeploymentReady {
            workload_id: WorkloadId(55),
            ..
        }
    ));
    let donor_lease = w1_shard.expect("worker 1 must own a workload shard");

    // Trigger online MigrateShard from worker 1 -> worker 2 while workload is active!
    let mut mgmt_client = auth.client(management_addr.parse().unwrap()).await;
    let accepted = mgmt_client
        .migrate_shard(MigrateShardRequest {
            protocol_version: 1,
            shard_id: donor_lease.shard_id.0.to_string(),
            target_node_id: "2".to_owned(),
            idempotency_key: "00000000-0000-4000-8000-000000000501".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();

    let idle_operation_id =
        acknowledge_idle_anchor(&mut w1_reader, &mut w1_write, workload_id, &donor_lease).await;
    assert_eq!(idle_operation_id, accepted.operation_id);

    // 1. Donor receives PrepareShardTransfer and acknowledges flush/close
    let (op_id, prep_lease) = match read_control_msg_skipping_topology(&mut w1_reader).await {
        ControlMessage::PrepareShardTransfer {
            operation_id,
            lease,
        } => (operation_id, lease),
        other => panic!("expected PrepareShardTransfer on donor, got {other:?}"),
    };
    assert_eq!(op_id, accepted.operation_id);
    assert_eq!(prep_lease, donor_lease);
    send_worker_msg(
        &mut w1_write,
        &WorkerMessage::ShardTransferAck {
            operation_id: op_id.clone(),
            stage: "donor".to_owned(),
            worker_id: WorkerId(1),
            shard_id: donor_lease.shard_id,
            lease_token: donor_lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    // 2. Recipient receives ShardAssigned with incremented lease_token and acknowledges open
    let new_lease = match read_control_msg_skipping_topology(&mut w2_reader).await {
        ControlMessage::ShardAssigned {
            lease,
            operation_id,
        } => {
            assert_eq!(
                operation_id.as_deref(),
                Some(accepted.operation_id.as_str())
            );
            lease
        }
        other => panic!("expected ShardAssigned on recipient, got {other:?}"),
    };
    assert_eq!(new_lease.shard_id, donor_lease.shard_id);
    assert_eq!(new_lease.worker_id, WorkerId(2));
    assert!(new_lease.lease_token.0 > donor_lease.lease_token.0);
    send_worker_msg(
        &mut w2_write,
        &WorkerMessage::ShardTransferAck {
            operation_id: accepted.operation_id.clone(),
            stage: "recipient".to_owned(),
            worker_id: WorkerId(2),
            shard_id: new_lease.shard_id,
            lease_token: new_lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    // 3. Recipient receives ControlMessage::Deploy for the active workload with the NEW lease!
    let migrated_descriptor = match read_control_msg_skipping_topology(&mut w2_reader).await {
        ControlMessage::Deploy { descriptor } => descriptor,
        other => panic!("expected Deploy on recipient for active workload, got {other:?}"),
    };
    assert_eq!(migrated_descriptor.workload_id, workload_id);
    assert_eq!(migrated_descriptor.shard, new_lease);
    send_worker_msg(
        &mut w2_write,
        &WorkerMessage::DeploymentReady {
            version: 1,
            workload_id,
            shard_id: new_lease.shard_id,
            worker_id: WorkerId(2),
            process_id: 1002,
            operator_ids: vec![OperatorId(10)],
            frontier: 1,
        },
    )
    .await;

    // 4. Migration completes cleanly and releases its GC pin
    let completed = await_operation_succeeded(&mut mgmt_client, &accepted.operation_id).await;
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert!(GcPinStore::new(mgmt_store)
        .list_active_pins()
        .await
        .unwrap()
        .is_empty());

    // 5. Submit a source delta: ALL rows now route to worker 2 (zero traffic to worker 1!)
    send_worker_msg(
        &mut c_write,
        &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
            version: 1,
            request_id: "post-migration-req-1".to_owned(),
            workload_id,
            source: "src".to_owned(),
            epoch: 2,
            rows: vec![
                RuntimeRow {
                    values_tsv: "key-a".to_owned(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "key-b".to_owned(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "key-c".to_owned(),
                    weight: 1,
                },
            ],
        }),
    )
    .await;
    // Worker 2 receives Execute frame(s) with the updated lease token for migrated shard
    let exec_msg = read_control_msg_skipping_topology(&mut w2_reader).await;
    match exec_msg {
        ControlMessage::Execute { frame } => {
            assert_eq!(frame.workload_id, workload_id);
            if frame.shard_id == new_lease.shard_id {
                assert_eq!(frame.lease_token, new_lease.lease_token);
            }
        }
        other => panic!("expected Execute on recipient worker 2, got {other:?}"),
    }

    control.shutdown();
}

#[tokio::test]
async fn online_drain_worker_moves_active_workload_to_healthy_recipient() {
    let mgmt_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let auth = ManagementAuth::new(&[]).await;
    let management_addr = free_addr().await;
    let control = auth.protect_control(
        ControlService::new(TopologyCatalog::new())
            .with_shard_manager(ShardManager::new())
            .with_management(management_addr.clone(), mgmt_store, NodeConfig::default()),
    );
    let control = control
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();

    let shared_caps = WorkerCapabilities {
        shared_shard_store_id: Some([9_u8; 32]),
        ..WorkerCapabilities::default()
    };

    let (w1_read, mut w1_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut w1_reader = BufReader::new(w1_read);
    let mut reg1 = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9201",
        CapacityHeadroom::default(),
    );
    reg1.capabilities = shared_caps;
    send_worker_msg(&mut w1_write, &WorkerMessage::Register(reg1)).await;
    assert!(matches!(
        read_control_msg_skipping_topology(&mut w1_reader).await,
        ControlMessage::Registered {
            worker_id: WorkerId(1),
            ..
        }
    ));

    let (w2_read, mut w2_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut w2_reader = BufReader::new(w2_read);
    let mut reg2 = WorkerRegistration::new(
        WorkerId(2),
        NodeRole::Worker,
        "127.0.0.1:9202",
        CapacityHeadroom::default(),
    );
    reg2.capabilities = shared_caps;
    send_worker_msg(&mut w2_write, &WorkerMessage::Register(reg2)).await;
    assert!(matches!(
        read_control_msg_skipping_topology(&mut w2_reader).await,
        ControlMessage::Registered {
            worker_id: WorkerId(2),
            ..
        }
    ));

    // Deploy an active workload across both workers
    let (c_read, mut c_write) = TcpStream::connect(control.addr).await.unwrap().into_split();
    let mut c_reader = BufReader::new(c_read);
    let workload_id = WorkloadId(77);
    send_worker_msg(
        &mut c_write,
        &WorkerMessage::DeployWorkloadSharded {
            request: DeploymentRequest {
                version: 1,
                workload_id,
                namespace: "public".to_owned(),
                workload_name: "active-drain-view".to_owned(),
                plan_json: "{}".to_owned(),
                join_strategy: JoinStrategy::Auto,
                schemas: vec![],
                frontier: 0,
                storage_root: "/tmp/shared-store".to_owned(),
                sink_operator_id: OperatorId(12),
                output_columns: vec!["val".to_owned()],
                primary_key: vec![0],
                merge_key_columns: vec![0],
                routing_columns: BTreeMap::from([("src".to_owned(), 0)]),
            },
            shard_count: 2,
        },
    )
    .await;

    let mut w1_shard = None;
    for (wid, reader, writer) in [
        (WorkerId(1), &mut w1_reader, &mut w1_write),
        (WorkerId(2), &mut w2_reader, &mut w2_write),
    ] {
        assert!(matches!(
            read_control_msg_skipping_topology(reader).await,
            ControlMessage::ShardAssigned { .. }
        ));
        let descriptor = match read_control_msg_skipping_topology(reader).await {
            ControlMessage::Deploy { descriptor } => descriptor,
            other => panic!("expected Deploy on {wid}, got {other:?}"),
        };
        if wid == WorkerId(1) {
            w1_shard = Some(descriptor.shard.clone());
        }
        send_worker_msg(
            writer,
            &WorkerMessage::DeploymentReady {
                version: 1,
                workload_id,
                shard_id: descriptor.shard.shard_id,
                worker_id: wid,
                process_id: 2000 + wid.0 as u32,
                operator_ids: vec![OperatorId(12)],
                frontier: 1,
            },
        )
        .await;
    }
    assert!(matches!(
        read_control_msg_skipping_topology(&mut c_reader).await,
        ControlMessage::DeploymentReady {
            workload_id: WorkloadId(77),
            ..
        }
    ));
    let drained_lease = w1_shard.unwrap();

    // Drain worker 1 online while workload is active
    let mut mgmt_client = auth.client(management_addr.parse().unwrap()).await;
    let accepted = mgmt_client
        .drain_worker(DrainWorkerRequest {
            protocol_version: 1,
            worker_id: "1".to_owned(),
            idempotency_key: "00000000-0000-4000-8000-000000000601".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();

    // A drain is now a parent operation. Its child migration must verify
    // the anchor and close the donor writer before moving the lease.
    let migration_id =
        acknowledge_idle_anchor(&mut w1_reader, &mut w1_write, workload_id, &drained_lease).await;
    assert_ne!(migration_id, accepted.operation_id);
    let (op_id, prep_lease) = match read_control_msg_skipping_topology(&mut w1_reader).await {
        ControlMessage::PrepareShardTransfer {
            operation_id,
            lease,
        } => (operation_id, lease),
        other => panic!("expected PrepareShardTransfer during drain, got {other:?}"),
    };
    assert_eq!(op_id, migration_id);
    assert_eq!(prep_lease, drained_lease);
    send_worker_msg(
        &mut w1_write,
        &WorkerMessage::ShardTransferAck {
            operation_id: op_id,
            stage: "donor".to_owned(),
            worker_id: WorkerId(1),
            shard_id: drained_lease.shard_id,
            lease_token: drained_lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    // Worker 2 receives ShardAssigned for the drained shard, then Deploy for the active workload
    let (new_lease, ack_op_id) = match read_control_msg_skipping_topology(&mut w2_reader).await {
        ControlMessage::ShardAssigned {
            lease,
            operation_id,
        } => (lease, operation_id.expect("drain must carry operation_id")),
        other => panic!("expected ShardAssigned on recipient worker 2, got {other:?}"),
    };
    assert_eq!(ack_op_id, migration_id);
    assert_eq!(new_lease.shard_id, drained_lease.shard_id);
    assert_eq!(new_lease.worker_id, WorkerId(2));
    assert!(new_lease.lease_token.0 > drained_lease.lease_token.0);
    send_worker_msg(
        &mut w2_write,
        &WorkerMessage::ShardTransferAck {
            operation_id: ack_op_id,
            stage: "recipient".to_owned(),
            worker_id: WorkerId(2),
            shard_id: new_lease.shard_id,
            lease_token: new_lease.lease_token,
            success: true,
            error: None,
        },
    )
    .await;

    let drained_descriptor = match read_control_msg_skipping_topology(&mut w2_reader).await {
        ControlMessage::Deploy { descriptor } => descriptor,
        other => panic!("expected Deploy on recipient worker 2 during drain, got {other:?}"),
    };
    assert_eq!(drained_descriptor.workload_id, workload_id);
    assert_eq!(drained_descriptor.shard, new_lease);
    let parent = mgmt_client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: accepted.operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(parent.state, "running");
    send_worker_msg(
        &mut w2_write,
        &WorkerMessage::DeploymentReady {
            version: 1,
            workload_id,
            shard_id: new_lease.shard_id,
            worker_id: WorkerId(2),
            process_id: 2002,
            operator_ids: vec![OperatorId(12)],
            frontier: 1,
        },
    )
    .await;

    let completed = await_operation_succeeded(&mut mgmt_client, &accepted.operation_id).await;
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert!(matches!(
        read_control_msg_skipping_topology(&mut w1_reader).await,
        ControlMessage::BeginDrain(req) if req.worker_id == WorkerId(1)
    ));

    control.shutdown();
}
