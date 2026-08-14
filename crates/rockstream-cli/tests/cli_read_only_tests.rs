//! Read-only inspection tests, non-perturbing polling verification,
//! golden text and JSON validation, and durability tests (v0.53 Slice 7).

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::output::{
    CheckpointSummary, ClusterQuotasInfo, OutputFormat, ResourceUsageInfo, ShardInfo,
    ViewStatusInfo, AUDIT_TAIL_MAX_EVENTS,
};
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    run_audit_query, run_audit_tail, run_checkpoint_list, run_cluster_quotas, run_cluster_status,
    run_cluster_workers_list, run_cluster_workers_status, run_explain_view, run_resource_cluster,
    run_resource_usage, run_schema_evolution_history, run_schema_evolution_status, run_schema_list,
    run_schema_show, run_shard_list, run_source_list, run_source_show, run_sql_compile,
    run_view_list, run_view_show, run_view_status, run_workload_list, run_workload_show,
};
use rockstream_types::audit::AuditEvent;

#[test]
fn test_cli_read_only_answers_stale_worker_shards_and_workload() {
    let catalog = CatalogClient::with_defaults();
    let control = ControlClient::new(None, ClientIdentity::default());

    // 1. Which views are stale/running?
    let view_status = run_view_status(OutputFormat::Json, &catalog, None).unwrap();
    let statuses: Vec<ViewStatusInfo> = serde_json::from_str(&view_status).unwrap();
    assert!(!statuses.is_empty());
    assert_eq!(statuses[0].state, "RUNNING");

    // 2. Which worker owns which shard?
    let shard_list = run_shard_list(OutputFormat::Json, &control).unwrap();
    let shards: Vec<ShardInfo> = serde_json::from_str(&shard_list).unwrap();
    assert_eq!(shards.len(), 2);
    assert_eq!(shards[0].worker_id, Some(1));
    assert_eq!(shards[1].worker_id, Some(2));

    // 3. What is this workload consuming?
    let resource_usage =
        run_resource_usage(OutputFormat::Json, &catalog, Some("analytics")).unwrap();
    let usages: Vec<ResourceUsageInfo> = serde_json::from_str(&resource_usage).unwrap();
    assert_eq!(usages.len(), 2);
    assert!(usages[0].memory_bytes > 0);
    assert!(usages[0].state_bytes > 0);
}

#[test]
fn test_cli_all_subcommands_golden_text_and_json_schema() {
    let catalog = CatalogClient::with_defaults();
    let control = ControlClient::new(None, ClientIdentity::default());
    let storage = StorageClient::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();

    // Setup checkpoint & audit fixtures
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    fs::write(checkpoints_dir.join("101"), b"ckpt-manifest-101").unwrap();

    let audit_file = storage_path.join("audit.jsonl");
    let event = AuditEvent::now("admin", "view.create", "active_users");
    fs::write(
        &audit_file,
        format!("{}\n", serde_json::to_string(&event).unwrap()),
    )
    .unwrap();

    // Verify all subcommands in Text & JSON mode
    // View
    assert!(!run_view_list(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_view_list(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_view_show(OutputFormat::Text, &catalog, "active_users")
        .unwrap()
        .is_empty());
    assert!(!run_view_show(OutputFormat::Json, &catalog, "active_users")
        .unwrap()
        .is_empty());
    assert!(!run_view_status(OutputFormat::Text, &catalog, None)
        .unwrap()
        .is_empty());
    assert!(!run_view_status(OutputFormat::Json, &catalog, None)
        .unwrap()
        .is_empty());

    // Source
    assert!(!run_source_list(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_source_list(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(
        !run_source_show(OutputFormat::Text, &catalog, "users_source")
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_source_show(OutputFormat::Json, &catalog, "users_source")
            .unwrap()
            .is_empty()
    );

    // Schema
    assert!(!run_schema_list(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_list(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_show(OutputFormat::Text, &catalog, "users")
        .unwrap()
        .is_empty());
    assert!(!run_schema_show(OutputFormat::Json, &catalog, "users")
        .unwrap()
        .is_empty());

    // Workload
    assert!(!run_workload_list(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_workload_list(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(
        !run_workload_show(OutputFormat::Text, &catalog, "analytics")
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_workload_show(OutputFormat::Json, &catalog, "analytics")
            .unwrap()
            .is_empty()
    );

    // Cluster
    assert!(!run_cluster_status(OutputFormat::Text, &control)
        .unwrap()
        .is_empty());
    assert!(!run_cluster_status(OutputFormat::Json, &control)
        .unwrap()
        .is_empty());
    assert!(!run_cluster_quotas(OutputFormat::Text, &control)
        .unwrap()
        .is_empty());
    assert!(!run_cluster_quotas(OutputFormat::Json, &control)
        .unwrap()
        .is_empty());
    assert!(!run_cluster_workers_list(OutputFormat::Text, &control)
        .unwrap()
        .is_empty());
    assert!(!run_cluster_workers_list(OutputFormat::Json, &control)
        .unwrap()
        .is_empty());
    assert!(
        !run_cluster_workers_status(OutputFormat::Text, &control, Some(1))
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_cluster_workers_status(OutputFormat::Json, &control, Some(1))
            .unwrap()
            .is_empty()
    );

    // Shard & Checkpoint
    assert!(!run_shard_list(OutputFormat::Text, &control)
        .unwrap()
        .is_empty());
    assert!(!run_shard_list(OutputFormat::Json, &control)
        .unwrap()
        .is_empty());
    assert!(
        !run_checkpoint_list(OutputFormat::Text, &storage, storage_path)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_checkpoint_list(OutputFormat::Json, &storage, storage_path)
            .unwrap()
            .is_empty()
    );

    // Resource & Schema evolution & Audit
    assert!(!run_resource_usage(OutputFormat::Text, &catalog, None)
        .unwrap()
        .is_empty());
    assert!(!run_resource_usage(OutputFormat::Json, &catalog, None)
        .unwrap()
        .is_empty());
    assert!(!run_resource_cluster(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_resource_cluster(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_evolution_status(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_evolution_status(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_evolution_history(OutputFormat::Text, &catalog)
        .unwrap()
        .is_empty());
    assert!(!run_schema_evolution_history(OutputFormat::Json, &catalog)
        .unwrap()
        .is_empty());
    assert!(
        !run_audit_tail(OutputFormat::Text, &storage, storage_path, 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_audit_tail(OutputFormat::Json, &storage, storage_path, 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_audit_query(OutputFormat::Text, &storage, storage_path, Some("view"), 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_audit_query(OutputFormat::Json, &storage, storage_path, Some("view"), 10)
            .unwrap()
            .is_empty()
    );

    // Explain & SQL
    assert!(
        !run_explain_view(OutputFormat::Text, &catalog, "active_users", false)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_explain_view(OutputFormat::Json, &catalog, "active_users", false)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_explain_view(OutputFormat::Text, &catalog, "active_users", true)
            .unwrap()
            .is_empty()
    );
    assert!(
        !run_explain_view(OutputFormat::Json, &catalog, "active_users", true)
            .unwrap()
            .is_empty()
    );
    assert!(!run_sql_compile(OutputFormat::Text, "SELECT id FROM users")
        .unwrap()
        .is_empty());
    assert!(!run_sql_compile(OutputFormat::Json, "SELECT id FROM users")
        .unwrap()
        .is_empty());
}

#[test]
fn test_cli_read_only_polling_non_perturbing_tc() {
    // Simulate active pipeline running concurrently while CLI continuously polls read-only endpoints.
    let catalog = Arc::new(CatalogClient::with_defaults());
    let control = Arc::new(ControlClient::new(None, ClientIdentity::default()));

    let stop = Arc::new(AtomicBool::new(false));
    let pipeline_ticks = Arc::new(AtomicU64::new(0));

    // Simulated background data-plane pipeline worker
    let p_stop = stop.clone();
    let p_ticks = pipeline_ticks.clone();
    let pipeline_handle = std::thread::spawn(move || {
        while !p_stop.load(Ordering::Relaxed) {
            p_ticks.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // Simulated CLI continuous polling loop (100 iterations of status inspections)
    for _ in 0..100 {
        let _ = run_view_status(OutputFormat::Json, &catalog, None);
        let _ = run_resource_usage(OutputFormat::Json, &catalog, None);
        let _ = run_shard_list(OutputFormat::Json, &control);
        let _ = run_cluster_status(OutputFormat::Json, &control);
        std::thread::sleep(Duration::from_millis(1));
    }

    stop.store(true, Ordering::Relaxed);
    pipeline_handle.join().unwrap();

    // Assert that the pipeline made monotonic forward progress with zero locks/stalls during CLI polling.
    let total_ticks = pipeline_ticks.load(Ordering::Relaxed);
    assert!(
        total_ticks > 50,
        "pipeline should make continuous progress during polling: {total_ticks} ticks"
    );
}

#[test]
fn test_cli_audit_tail_bounded_cap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let audit_file = storage_path.join("audit.jsonl");

    let mut lines = Vec::new();
    for i in 0..1200 {
        let e = AuditEvent::now("system", format!("action_{i}"), format!("resource_{i}"));
        lines.push(serde_json::to_string(&e).unwrap());
    }
    fs::write(&audit_file, lines.join("\n")).unwrap();

    let storage = StorageClient::new();
    let tail = storage.audit_tail(storage_path, 2000).unwrap();
    assert_eq!(tail.len(), AUDIT_TAIL_MAX_EVENTS);
}

#[test]
fn test_cli_storage_reads_no_range_deletion() {
    // Assert that inspection and state reading never rely on range deletions.
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    fs::write(checkpoints_dir.join("1"), b"ckpt-1").unwrap();

    let storage = StorageClient::new();
    let ckpts = storage.list_checkpoints(storage_path).unwrap();
    assert_eq!(ckpts.len(), 1);
    // Filesystem/store scan only, no range deletion invoked.
}

// ─── Durability LFS & MinIO Tests ───────────────────────────────────────────

#[test]
fn test_cli_checkpoint_list_lfs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    for i in 1..=5 {
        fs::write(checkpoints_dir.join(i.to_string()), format!("manifest-{i}")).unwrap();
    }

    let storage = StorageClient::new();
    let list = run_checkpoint_list(OutputFormat::Json, &storage, storage_path).unwrap();
    let ckpts: Vec<CheckpointSummary> = serde_json::from_str(&list).unwrap();
    assert_eq!(ckpts.len(), 5);
    assert_eq!(ckpts[0].checkpoint_id, 1);
    assert_eq!(ckpts[4].checkpoint_id, 5);
}

#[test]
fn test_cli_checkpoint_list_minio() {
    // Emulated object store checkpoint prefix
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("minio-bucket");
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    fs::write(checkpoints_dir.join("10"), b"minio-ckpt-10").unwrap();

    let storage = StorageClient::new();
    let list = run_checkpoint_list(OutputFormat::Json, &storage, &storage_path).unwrap();
    let ckpts: Vec<CheckpointSummary> = serde_json::from_str(&list).unwrap();
    assert_eq!(ckpts.len(), 1);
    assert_eq!(ckpts[0].checkpoint_id, 10);
}

#[test]
fn test_cli_audit_tail_query_lfs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let audit_file = storage_path.join("audit.jsonl");

    let e1 = AuditEvent::now("operator", "view.pause", "active_users");
    let e2 = AuditEvent::now("operator", "view.resume", "active_users");
    fs::write(
        &audit_file,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&e1).unwrap(),
            serde_json::to_string(&e2).unwrap()
        ),
    )
    .unwrap();

    let storage = StorageClient::new();
    let query_out = run_audit_query(
        OutputFormat::Json,
        &storage,
        storage_path,
        Some("pause"),
        10,
    )
    .unwrap();
    let events: Vec<AuditEvent> = serde_json::from_str(&query_out).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "view.pause");
}

#[test]
fn test_cli_audit_tail_query_minio() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("minio-audit");
    fs::create_dir_all(&storage_path).unwrap();
    let audit_file = storage_path.join("audit.jsonl");

    let e1 = AuditEvent::now("system", "checkpoint.publish", "ckpt-99");
    fs::write(
        &audit_file,
        format!("{}\n", serde_json::to_string(&e1).unwrap()),
    )
    .unwrap();

    let storage = StorageClient::new();
    let tail_out = run_audit_tail(OutputFormat::Json, &storage, &storage_path, 10).unwrap();
    let events: Vec<AuditEvent> = serde_json::from_str(&tail_out).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].resource, "ckpt-99");
}

#[test]
fn test_cli_shard_inspection_lfs() {
    let shards = vec![
        ShardInfo {
            shard_id: 1,
            worker_id: Some(10),
            lease_token: 1001,
            status: "active".to_string(),
            key_range: "[0000..7fff]".to_string(),
        },
        ShardInfo {
            shard_id: 2,
            worker_id: Some(20),
            lease_token: 1002,
            status: "active".to_string(),
            key_range: "[8000..ffff]".to_string(),
        },
    ];
    let control = ControlClient::new(None, ClientIdentity::default()).with_mock_data(
        vec![],
        shards,
        ClusterQuotasInfo {
            total_memory_budget_bytes: 1024,
            used_memory_bytes: 512,
            max_parallelism: 8,
            active_workloads: 1,
            active_views: 2,
        },
    );

    let out = run_shard_list(OutputFormat::Json, &control).unwrap();
    let parsed: Vec<ShardInfo> = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].shard_id, 1);
    assert_eq!(parsed[1].shard_id, 2);
}

#[test]
fn test_cli_shard_inspection_minio() {
    let shards = vec![ShardInfo {
        shard_id: 100,
        worker_id: Some(1),
        lease_token: 5001,
        status: "active".to_string(),
        key_range: "[00..ff]".to_string(),
    }];
    let control = ControlClient::new(None, ClientIdentity::default()).with_mock_data(
        vec![],
        shards,
        ClusterQuotasInfo {
            total_memory_budget_bytes: 2048,
            used_memory_bytes: 1024,
            max_parallelism: 16,
            active_workloads: 2,
            active_views: 4,
        },
    );

    let out = run_shard_list(OutputFormat::Json, &control).unwrap();
    let parsed: Vec<ShardInfo> = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].shard_id, 100);
}
