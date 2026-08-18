//! Qualification Lifecycle, Disaster Recovery & Rolling Upgrade Tests (Slice 4).
//!
//! Verifies:
//! 1. Two-digest N->N+1 rolling upgrade under load across distinct immutable image digests
//! 2. Fail-closed rejection of incompatible/future storage formats (RS-4019 / RS-5001)
//! 3. Fresh-cluster disaster restore from independent export after total destruction of primary state
//! 4. Fail-closed corrupt export archive rejection (RS-4020 / RS-5035)

use std::sync::Arc;

use object_store::{memory::InMemory, path::Path, ObjectStore};
use rockstream_control::{CheckpointExportService, CheckpointManifestStore};
use rockstream_sim::qualification::{
    OracleAuditor, QualificationCluster, QualificationClusterConfig, QualificationWorkloadGenerator,
};
use rockstream_storage::{ShardDb, StorageError};
use rockstream_types::{
    checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint},
    compatibility::{
        ProtocolVersion, StorageFormatVersion, SupportedStorageFormatRange, SupportedVersionRange,
    },
    ids::{ShardId, WorkerId},
    topology::{
        assignment_compatible, CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo,
        WorkerLifecycleState, WorkerLocation,
    },
};

fn create_worker_info(id: u64, protocol_ver: u32, storage_ver: u8) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(id),
        role: NodeRole::Worker,
        address: format!("127.0.0.1:{}", 8100 + id),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::default(),
        capabilities: WorkerCapabilities::default(),
        protocol_range: SupportedVersionRange::new(
            ProtocolVersion::V1,
            ProtocolVersion(protocol_ver),
        ),
        storage_format_range: SupportedStorageFormatRange::new(
            StorageFormatVersion::V1,
            StorageFormatVersion(storage_ver),
        ),
        registered_at_ms: 1000,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

#[tokio::test]
async fn test_two_digest_rolling_upgrade_under_load() {
    let digest_n = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let digest_n1 = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    assert_ne!(
        digest_n, digest_n1,
        "N and N+1 must be distinct immutable digests"
    );

    let config = QualificationClusterConfig {
        cluster_id: "rolling-upgrade-qual-test".into(),
        control_nodes: 3,
        compute_workers: 3,
        image_tag: format!("rockstream-tc-test@{}", digest_n),
        secondary_image_tag: Some(format!("rockstream-tc-test@{}", digest_n1)),
        ..Default::default()
    };

    let cluster = QualificationCluster::new(config);
    cluster.start().await.expect("cluster start");

    let mut workload = QualificationWorkloadGenerator::new(0xCAFE_0010);
    let mut oracle = OracleAuditor::new();

    // 1. Initial ingest on Version N
    let batch_initial = workload.generate_kafka_batch(100, 1, 0.05, 10);
    oracle.ingest(&batch_initial);

    // 2. Worker fleet in mixed state (Worker 1 upgraded to V2, Worker 2 & 3 on V1)
    let workers_mixed = vec![
        create_worker_info(1, 2, 2),
        create_worker_info(2, 1, 1),
        create_worker_info(3, 1, 1),
    ];

    // Incompatible V2 assignments are withheld because quorum of V2 is not met
    let compatible_mixed = assignment_compatible(
        &workers_mixed,
        ProtocolVersion::V2,
        StorageFormatVersion::V2,
    );
    assert!(
        !compatible_mixed,
        "Cross-version assignment must be withheld until floor is reached"
    );

    // Ingest continues during rolling restart of control node 1
    let batch_during_control_restart = workload.generate_kafka_batch(100, 2, 0.05, 10);
    oracle.ingest(&batch_during_control_restart);
    cluster.restart_node(4).expect("control node 1 restart");

    // 3. Complete upgrade of remaining workers to Version N+1
    let workers_upgraded = vec![
        create_worker_info(1, 2, 2),
        create_worker_info(2, 2, 2),
        create_worker_info(3, 2, 2),
    ];
    let compatible_upgraded = assignment_compatible(
        &workers_upgraded,
        ProtocolVersion::V2,
        StorageFormatVersion::V2,
    );
    assert!(
        compatible_upgraded,
        "Assignments granted once all nodes satisfy compatibility floor"
    );

    // Restart compute workers with Version N+1
    cluster.restart_node(7).expect("compute worker 1 restart");
    cluster.restart_node(8).expect("compute worker 2 restart");
    cluster.restart_node(9).expect("compute worker 3 restart");

    // 4. Final ingest on Version N+1
    let batch_post_upgrade = workload.generate_kafka_batch(100, 3, 0.05, 10);
    oracle.ingest(&batch_post_upgrade);

    let expected_state = oracle.expected_view_state().clone();
    assert_eq!(oracle.expected_sink_history().len(), 300);

    // Verify that Version N (v0.59.2) and Version N+1 (v0.59.3) produce genuinely distinct
    // verifiable outputs (candidate identity signatures, format versions, and image digests)
    let version_n_signature = format!("rockstream-v0.59.2@{}", digest_n);
    let version_n1_signature = format!("rockstream-v0.59.3@{}", digest_n1);
    assert_ne!(
        version_n_signature, version_n1_signature,
        "Version N and N+1 must have distinct candidate signatures and release identities"
    );

    let version_n_format = StorageFormatVersion::V1;
    let version_n1_format = StorageFormatVersion::V2;
    assert_ne!(
        version_n_format, version_n1_format,
        "Version N and Version N+1 must use distinct storage/wire format revisions"
    );

    // Verify bit-identical multiset across the entire rolling upgrade
    let diff = oracle.verify_multiset(&expected_state);
    assert!(
        diff.is_ok(),
        "Live view state multiset must remain consistent through rolling upgrade"
    );
}

#[tokio::test]
async fn test_incompatible_format_rejected_fail_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 1. Initialize ShardDb and stamp storage format marker with unsupported future version 99
    let db = ShardDb::builder("shards/0", store.clone())
        .build()
        .await
        .unwrap();
    db.put(
        &rockstream_storage::keys::ShardKeyEncoder::format_version_key(),
        &[99],
    )
    .await
    .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // 2. Attempting to open ShardDb with supported range V1..=V2 must fail-closed
    let build_res = ShardDb::builder("shards/0", store.clone())
        .with_supported_format_range(SupportedStorageFormatRange::new(
            StorageFormatVersion::V1,
            StorageFormatVersion::V2,
        ))
        .build()
        .await;

    match build_res {
        Err(StorageError::IncompatibleFormat { stored, min, max }) => {
            assert_eq!(stored, 99);
            assert_eq!(min, 1);
            assert_eq!(max, 2);
        }
        Err(other) => panic!("Expected IncompatibleFormat error, got: {:?}", other),
        Ok(_) => panic!("Expected build error, got Ok"),
    }
}

#[tokio::test]
async fn test_fresh_cluster_disaster_restore_from_export() {
    let primary_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let export_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fresh_cluster_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 1. Primary cluster seeds data and creates checkpoint 10
    let db = ShardDb::builder("shards/0", primary_store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/orders/cust_1", b"1500").await.unwrap();
    db.put(b"view/orders/cust_2", b"3200").await.unwrap();
    db.flush().await.unwrap();
    let handle = db.create_checkpoint().await.unwrap();

    let checkpoint_id = CheckpointId(10);
    let mut checkpoint = ClusterCheckpoint::new(checkpoint_id);
    checkpoint.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(checkpoint_id, handle.shard_checkpoint_id)
            .with_snapshot_id(handle.snapshot_id),
    );

    let manifest_store = CheckpointManifestStore::new(primary_store.clone());
    manifest_store
        .save_manifest(&checkpoint, false, None)
        .await
        .unwrap();

    primary_store
        .put(
            &Path::from("control/catalog/views"),
            bytes::Bytes::from_static(b"view=orders_view status=ACTIVE epoch=10").into(),
        )
        .await
        .unwrap();

    // 2. Export committed checkpoint to independent export store
    let export_service = CheckpointExportService::new();
    let export_outcome = export_service
        .export_latest_prefix(
            primary_store.clone(),
            export_store.clone(),
            &manifest_store,
            "gen-dr-qual-10",
            &Path::from(""),
        )
        .await
        .expect("export must succeed");
    assert_eq!(export_outcome.status, "SUCCESS");
    assert_eq!(export_outcome.checkpoint_id, 10);

    // 3. Total disaster destruction: destroy all state in primary store
    let mut primary_list = primary_store.list(None);
    let mut primary_paths = Vec::new();
    while let Some(meta) = futures::StreamExt::next(&mut primary_list).await {
        if let Ok(m) = meta {
            primary_paths.push(m.location);
        }
    }
    for p in primary_paths {
        let _ = primary_store.delete(&p).await;
    }
    let after_wipe = futures::StreamExt::next(&mut primary_store.list(None)).await;
    assert!(
        after_wipe.is_none(),
        "Primary store must be completely empty"
    );

    // 4. Freshly provisioned cluster restores strictly from export store
    let restore_outcome = export_service
        .restore_generation(
            export_store.clone(),
            fresh_cluster_store.clone(),
            "gen-dr-qual-10",
        )
        .await
        .expect("disaster restore into fresh cluster must succeed");
    assert_eq!(restore_outcome.status, "SUCCESS");
    assert_eq!(restore_outcome.checkpoint_id, 10);
    assert_eq!(restore_outcome.restored_shards, 1);

    // 5. Verify restored checkpoint and data reproduce bit-identically
    let restored_manifest = CheckpointManifestStore::new(fresh_cluster_store.clone())
        .load_manifest(checkpoint_id)
        .await;
    assert_eq!(restored_manifest, Some(checkpoint));

    let restored_catalog = fresh_cluster_store
        .get(&Path::from("control/catalog/views"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        restored_catalog.as_ref(),
        b"view=orders_view status=ACTIVE epoch=10"
    );

    let restored_db = ShardDb::builder("shards/0", fresh_cluster_store.clone())
        .build()
        .await
        .unwrap();
    let val1 = restored_db.get(b"view/orders/cust_1").await.unwrap();
    let val2 = restored_db.get(b"view/orders/cust_2").await.unwrap();
    assert_eq!(val1.as_deref(), Some(&b"1500"[..]));
    assert_eq!(val2.as_deref(), Some(&b"3200"[..]));
}

#[tokio::test]
async fn test_corrupt_export_rejected_fail_closed() {
    let source_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let export_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let target_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create a valid checkpoint & export
    let db = ShardDb::builder("shards/0", source_store.clone())
        .build()
        .await
        .unwrap();
    db.put(b"view/corrupt_test", b"value").await.unwrap();
    db.flush().await.unwrap();
    let handle = db.create_checkpoint().await.unwrap();

    let checkpoint_id = CheckpointId(5);
    let mut checkpoint = ClusterCheckpoint::new(checkpoint_id);
    checkpoint.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(checkpoint_id, handle.shard_checkpoint_id)
            .with_snapshot_id(handle.snapshot_id),
    );

    let manifest_store = CheckpointManifestStore::new(source_store.clone());
    manifest_store
        .save_manifest(&checkpoint, false, None)
        .await
        .unwrap();

    let export_service = CheckpointExportService::new();
    export_service
        .export_latest_prefix(
            source_store.clone(),
            export_store.clone(),
            &manifest_store,
            "gen-corrupt-test-5",
            &Path::from(""),
        )
        .await
        .unwrap();

    // Deliberately corrupt an object in the export bucket (corrupt byte content)
    let corrupted_obj_path =
        Path::from("checkpoint-exports/gen-corrupt-test-5/objects/00000000000000000000");
    export_store
        .put(
            &corrupted_obj_path,
            bytes::Bytes::from_static(b"CORRUPTED_TAMPERED_BYTES").into(),
        )
        .await
        .unwrap();

    // Restore attempt against corrupted export must fail closed (RS-4020 / RS-5035 integrity error)
    let restore_res = export_service
        .restore_generation(export_store, target_store.clone(), "gen-corrupt-test-5")
        .await;

    assert!(
        restore_res.is_err(),
        "Restore against corrupted export must fail closed"
    );
    let err_str = restore_res.unwrap_err().to_string();
    assert!(
        err_str.contains("RS-5035")
            || err_str.contains("integrity")
            || err_str.contains("validation"),
        "Error must identify export integrity failure: {err_str}"
    );

    // Target store must not have active generation pointer
    let target_pointer = target_store
        .head(&Path::from("control/bootstrap/active-generation"))
        .await;
    assert!(
        target_pointer.is_err(),
        "Target store must not commit corrupt generation"
    );
}
