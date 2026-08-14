//! Inspection command tests for RockStream CLI (v0.53 Slices 1-4).

use std::fs;

use rockstream_cli::output::{
    render_output, ArrangementDebugInfo, CheckpointAlignmentInfo, CheckpointSummary,
    ClusterQuotasInfo, ClusterResourceUsageInfo, ClusterStatusInfo, ExplainEstimateInfo,
    ExplainOpIdInfo, ExplainPlanInfo, OutputFormat, ResourceUsageInfo, SchemaDetail,
    SchemaEvolutionHistoryInfo, SchemaEvolutionStatusInfo, ShardAlignmentInfo, ShardInfo,
    SourceDetail, SqlCompileInfo, ViewDetail, ViewStatusInfo, ViewSummary, WorkerStatusInfo,
    WorkloadDetail, AUDIT_TAIL_MAX_EVENTS,
};
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    run_audit_query, run_audit_tail, run_checkpoint_list, run_checkpoint_show, run_cluster_quotas,
    run_cluster_status, run_cluster_workers_list, run_cluster_workers_status,
    run_debug_arrangement, run_explain_view, run_resource_cluster, run_resource_usage,
    run_schema_evolution_history, run_schema_evolution_status, run_schema_list, run_schema_show,
    run_shard_list, run_source_list, run_source_show, run_sql_compile, run_view_list,
    run_view_show, run_view_status, run_workload_list, run_workload_show,
};
use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::{
    RS_0004, RS_1001, RS_1005, RS_1012, RS_1020, RS_1021, RS_2006, RS_4009,
};
use rockstream_types::view_lifecycle::{DegradationReason, DominantContributor};

// ─── Slice 1: Transport Seam & Substrate Tests ──────────────────────────────

#[test]
fn test_cli_transport_substrate_and_schema_validation() {
    let identity = ClientIdentity::new("admin")
        .with_token("secret-token")
        .with_namespace("production");
    assert_eq!(identity.user, "admin");
    assert_eq!(identity.token.as_deref(), Some("secret-token"));
    assert_eq!(identity.namespace, "production");

    let catalog = CatalogClient::new(identity.clone());
    assert_eq!(catalog.identity.user, "admin");

    // Output formatting JSON vs Text
    let view_sum = ViewSummary {
        name: "test_view".to_string(),
        state: "RUNNING".to_string(),
        workload: Some("wl1".to_string()),
        freshness_slo_ms: Some(1000),
        memory_limit_bytes: Some(1024),
        depends_on: vec!["src1".to_string()],
    };
    let json_out = render_output(&vec![view_sum.clone()], OutputFormat::Json);
    let parsed: Vec<ViewSummary> = serde_json::from_str(&json_out).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].name, "test_view");

    let text_out = render_output(&vec![view_sum], OutputFormat::Text);
    assert!(text_out.contains("test_view"));
    assert!(text_out.contains("RUNNING"));
}

// ─── Slice 2: Catalog Read-Only Commands ────────────────────────────────────

#[test]
fn test_cli_catalog_commands_text_and_json_golden() {
    let catalog = CatalogClient::with_defaults();

    // 1. View List
    let view_list_text = run_view_list(OutputFormat::Text, &catalog).unwrap();
    assert!(view_list_text.contains("active_users"));
    assert!(view_list_text.contains("hourly_revenue"));
    assert!(view_list_text.contains("RUNNING"));

    let view_list_json = run_view_list(OutputFormat::Json, &catalog).unwrap();
    let views: Vec<ViewSummary> = serde_json::from_str(&view_list_json).unwrap();
    assert_eq!(views.len(), 2);
    assert_eq!(views[0].name, "active_users");

    // 2. View Show
    let view_show_text = run_view_show(OutputFormat::Text, &catalog, "active_users").unwrap();
    assert!(view_show_text.contains("View: active_users"));
    assert!(view_show_text.contains("State: RUNNING"));
    assert!(view_show_text.contains("Query:"));

    let view_show_json = run_view_show(OutputFormat::Json, &catalog, "active_users").unwrap();
    let view_detail: ViewDetail = serde_json::from_str(&view_show_json).unwrap();
    assert_eq!(view_detail.name, "active_users");
    assert_eq!(view_detail.workload.as_deref(), Some("analytics"));

    // 3. View Status
    let view_status_text = run_view_status(OutputFormat::Text, &catalog, None).unwrap();
    assert!(view_status_text.contains("active_users"));
    assert!(view_status_text.contains("hourly_revenue"));

    let view_status_single =
        run_view_status(OutputFormat::Json, &catalog, Some("active_users")).unwrap();
    let statuses: Vec<ViewStatusInfo> = serde_json::from_str(&view_status_single).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].view_name, "active_users");

    // 4. Source List & Show
    let source_list_text = run_source_list(OutputFormat::Text, &catalog).unwrap();
    assert!(source_list_text.contains("users_source"));
    assert!(source_list_text.contains("kafka"));

    let source_show_text = run_source_show(OutputFormat::Text, &catalog, "users_source").unwrap();
    assert!(source_show_text.contains("Source: users_source"));
    assert!(source_show_text.contains("Type: kafka"));

    let source_show_json = run_source_show(OutputFormat::Json, &catalog, "users_source").unwrap();
    let src_detail: SourceDetail = serde_json::from_str(&source_show_json).unwrap();
    assert_eq!(src_detail.name, "users_source");
    assert_eq!(src_detail.connector_type, "kafka");

    // 5. Schema List & Show
    let schema_list_text = run_schema_list(OutputFormat::Text, &catalog).unwrap();
    assert!(schema_list_text.contains("users"));
    assert!(schema_list_text.contains("active_users"));

    let schema_show_text = run_schema_show(OutputFormat::Text, &catalog, "users").unwrap();
    assert!(schema_show_text.contains("Schema: users"));
    assert!(schema_show_text.contains("BIGINT"));

    let schema_show_json = run_schema_show(OutputFormat::Json, &catalog, "users").unwrap();
    let schema_detail: SchemaDetail = serde_json::from_str(&schema_show_json).unwrap();
    assert_eq!(schema_detail.name, "users");
    assert_eq!(schema_detail.columns.len(), 3);

    // 6. Workload List & Show
    let wl_list_text = run_workload_list(OutputFormat::Text, &catalog).unwrap();
    assert!(wl_list_text.contains("analytics"));
    assert!(wl_list_text.contains("128"));

    let wl_show_text = run_workload_show(OutputFormat::Text, &catalog, "analytics").unwrap();
    assert!(wl_show_text.contains("Workload: analytics"));
    assert!(wl_show_text.contains("Freshness SLO: 5000 ms"));

    let wl_show_json = run_workload_show(OutputFormat::Json, &catalog, "analytics").unwrap();
    let wl_detail: WorkloadDetail = serde_json::from_str(&wl_show_json).unwrap();
    assert_eq!(wl_detail.name, "analytics");
    assert_eq!(wl_detail.assigned_views.len(), 2);
}

#[test]
fn test_cli_view_show_not_found_rs1001() {
    let catalog = CatalogClient::with_defaults();
    let err = run_view_show(OutputFormat::Text, &catalog, "nonexistent_view").unwrap_err();
    assert_eq!(err.code, RS_1001);
    assert!(err.message.contains("nonexistent_view"));
    assert!(!err.next_steps.is_empty());
}

#[test]
fn test_cli_source_show_not_found_rs4009() {
    let catalog = CatalogClient::with_defaults();
    let err = run_source_show(OutputFormat::Text, &catalog, "nonexistent_source").unwrap_err();
    assert_eq!(err.code, RS_4009);
    assert!(err.message.contains("nonexistent_source"));
    assert!(!err.next_steps.is_empty());
}

#[test]
fn test_cli_schema_show_not_found_rs1001() {
    let catalog = CatalogClient::with_defaults();
    let err = run_schema_show(OutputFormat::Text, &catalog, "nonexistent_schema").unwrap_err();
    assert_eq!(err.code, RS_1001);
    assert!(err.message.contains("nonexistent_schema"));
}

#[test]
fn test_cli_workload_show_not_found_rs1005() {
    let catalog = CatalogClient::with_defaults();
    let err = run_workload_show(OutputFormat::Text, &catalog, "nonexistent_workload").unwrap_err();
    assert_eq!(err.code, RS_1005);
    assert!(err.message.contains("nonexistent_workload"));
    assert!(!err.next_steps.is_empty());
}

#[test]
fn test_cli_catalog_empty_lists() {
    let empty_catalog = CatalogClient::new(ClientIdentity::default());

    let views = run_view_list(OutputFormat::Text, &empty_catalog).unwrap();
    assert_eq!(views, "No views found.");

    let sources = run_source_list(OutputFormat::Text, &empty_catalog).unwrap();
    assert_eq!(sources, "No sources found.");

    let schemas = run_schema_list(OutputFormat::Text, &empty_catalog).unwrap();
    assert_eq!(schemas, "No schemas/entities found.");

    let workloads = run_workload_list(OutputFormat::Text, &empty_catalog).unwrap();
    assert_eq!(workloads, "No workloads found.");
}

// ─── Slice 3: Cluster, Worker, Shard & Checkpoint Tests ─────────────────────

#[test]
fn test_cli_cluster_inspection_commands_golden() {
    let control = ControlClient::new(None, ClientIdentity::default());

    // 1. Cluster Status
    let status_text = run_cluster_status(OutputFormat::Text, &control).unwrap();
    assert!(status_text.contains("Cluster Status:"));
    assert!(status_text.contains("Role: all"));

    let status_json = run_cluster_status(OutputFormat::Json, &control).unwrap();
    let status: ClusterStatusInfo = serde_json::from_str(&status_json).unwrap();
    assert_eq!(status.role, "all");

    // 2. Cluster Quotas
    let quotas_text = run_cluster_quotas(OutputFormat::Text, &control).unwrap();
    assert!(quotas_text.contains("Cluster Quotas:"));
    assert!(quotas_text.contains("Total Memory Budget:"));

    let quotas_json = run_cluster_quotas(OutputFormat::Json, &control).unwrap();
    let quotas: ClusterQuotasInfo = serde_json::from_str(&quotas_json).unwrap();
    assert!(quotas.total_memory_budget_bytes > 0);

    // 3. Cluster Workers List & Status
    let workers_text = run_cluster_workers_list(OutputFormat::Text, &control).unwrap();
    assert!(workers_text.contains("127.0.0.1:8001"));
    assert!(workers_text.contains("us-east-1a"));

    let workers_json = run_cluster_workers_list(OutputFormat::Json, &control).unwrap();
    let workers: Vec<WorkerStatusInfo> = serde_json::from_str(&workers_json).unwrap();
    assert_eq!(workers.len(), 2);
    assert_eq!(workers[0].worker_id, 1);

    let worker_status_text =
        run_cluster_workers_status(OutputFormat::Text, &control, Some(1)).unwrap();
    assert!(worker_status_text.contains("Worker: 1"));
    assert!(worker_status_text.contains("Address: 127.0.0.1:8001"));

    let worker_status_json =
        run_cluster_workers_status(OutputFormat::Json, &control, Some(1)).unwrap();
    let w_info: WorkerStatusInfo = serde_json::from_str(&worker_status_json).unwrap();
    assert_eq!(w_info.worker_id, 1);
    assert_eq!(w_info.host_id, "host-1");
}

#[test]
fn test_cli_shard_and_checkpoint_list_golden() {
    let control = ControlClient::new(None, ClientIdentity::default());

    // Shard List
    let shard_text = run_shard_list(OutputFormat::Text, &control).unwrap();
    assert!(shard_text.contains("SHARD ID"));
    assert!(shard_text.contains("[00000000..7fffffff]"));

    let shard_json = run_shard_list(OutputFormat::Json, &control).unwrap();
    let shards: Vec<ShardInfo> = serde_json::from_str(&shard_json).unwrap();
    assert_eq!(shards.len(), 2);
    assert_eq!(shards[0].shard_id, 1);

    // Checkpoint List
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    fs::write(checkpoints_dir.join("1"), b"manifest-1").unwrap();
    fs::write(checkpoints_dir.join("2"), b"manifest-2").unwrap();

    let storage = StorageClient::new();
    let ckpt_text = run_checkpoint_list(OutputFormat::Text, &storage, storage_path).unwrap();
    assert!(ckpt_text.contains("CHECKPOINT ID"));

    let ckpt_json = run_checkpoint_list(OutputFormat::Json, &storage, storage_path).unwrap();
    let ckpts: Vec<CheckpointSummary> = serde_json::from_str(&ckpt_json).unwrap();
    assert_eq!(ckpts.len(), 2);
    assert_eq!(ckpts[0].checkpoint_id, 1);
    assert_eq!(ckpts[1].checkpoint_id, 2);
}

#[test]
fn test_cli_checkpoint_show_alignment_and_holder() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let checkpoints_dir = storage_path.join("checkpoints");
    fs::create_dir_all(&checkpoints_dir).unwrap();
    fs::write(checkpoints_dir.join("42"), b"manifest-42").unwrap();

    let storage = StorageClient::new().with_mock_checkpoint_alignment(CheckpointAlignmentInfo {
        checkpoint_id: 42,
        status: "in_progress".to_string(),
        shards: vec![
            ShardAlignmentInfo {
                shard_id: 1,
                operator_id: "source_0".to_string(),
                state: "confirmed".to_string(),
                holder: None,
                elapsed_ms: 250,
            },
            ShardAlignmentInfo {
                shard_id: 2,
                operator_id: "source_0".to_string(),
                state: "holding_barrier".to_string(),
                holder: Some("shard_2/source_0".to_string()),
                elapsed_ms: 250,
            },
        ],
        active_holder: Some("shard_2/source_0".to_string()),
        elapsed_ms: 250,
    });

    let text = run_checkpoint_show(OutputFormat::Text, &storage, 42, storage_path).unwrap();
    assert!(text.contains(
        "Checkpoint 42: status=in_progress, active_holder=shard_2/source_0, elapsed_ms=250"
    ));
    assert!(text.contains("shard_2/source_0"));

    let json = run_checkpoint_show(OutputFormat::Json, &storage, 42, storage_path).unwrap();
    let info: CheckpointAlignmentInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(info.checkpoint_id, 42);
    assert_eq!(info.status, "in_progress");
    assert_eq!(info.active_holder.as_deref(), Some("shard_2/source_0"));
    assert_eq!(info.shards.len(), 2);
    assert_eq!(info.shards[0].state, "confirmed");
    assert_eq!(info.shards[1].state, "holding_barrier");
    assert_eq!(info.shards[1].holder.as_deref(), Some("shard_2/source_0"));
}

#[test]
fn test_cli_checkpoint_show_not_found_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let storage = StorageClient::new();
    let err = run_checkpoint_show(OutputFormat::Text, &storage, 999, storage_path).unwrap_err();
    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("checkpoint 999 not found"));
}

#[test]
fn test_cli_cluster_status_unreachable_rs0004() {
    // Unreachable address
    let unreachable_control = ControlClient::new(
        Some("127.0.0.1:59999".to_string()),
        ClientIdentity::default(),
    );
    let err = run_cluster_status(OutputFormat::Text, &unreachable_control).unwrap_err();
    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("failed to reach control plane"));
    assert!(err.next_steps.contains("Verify the control service URL"));
}

#[test]
fn test_cli_cluster_workers_status_not_found() {
    let control = ControlClient::new(None, ClientIdentity::default());
    let err = run_cluster_workers_status(OutputFormat::Text, &control, Some(999)).unwrap_err();
    assert_eq!(err.code, RS_1001);
    assert!(err.message.contains("Worker ID 999 not found"));
}

// ─── Slice 4: Resource Usage, Schema Evolution & Audit Tests ────────────────

#[test]
fn test_cli_resource_evolution_audit_commands_golden() {
    let catalog = CatalogClient::with_defaults();

    // 1. Resource Usage
    let res_text = run_resource_usage(OutputFormat::Text, &catalog, None).unwrap();
    assert!(res_text.contains("active_users"));
    assert!(res_text.contains("analytics"));

    let res_json = run_resource_usage(OutputFormat::Json, &catalog, None).unwrap();
    let usages: Vec<ResourceUsageInfo> = serde_json::from_str(&res_json).unwrap();
    assert_eq!(usages.len(), 2);

    let res_wl_text = run_resource_usage(OutputFormat::Text, &catalog, Some("analytics")).unwrap();
    assert!(res_wl_text.contains("active_users"));

    // 2. Resource Cluster
    let cluster_res_text = run_resource_cluster(OutputFormat::Text, &catalog).unwrap();
    assert!(cluster_res_text.contains("Cluster Resource Usage:"));
    assert!(cluster_res_text.contains("Total Memory:"));

    let cluster_res_json = run_resource_cluster(OutputFormat::Json, &catalog).unwrap();
    let cluster_res: ClusterResourceUsageInfo = serde_json::from_str(&cluster_res_json).unwrap();
    assert_eq!(cluster_res.total_views, 2);
    assert_eq!(cluster_res.total_workloads, 1);

    // 3. Schema Evolution Status & History
    let evol_status_text = run_schema_evolution_status(OutputFormat::Text, &catalog).unwrap();
    assert!(evol_status_text.contains("active_users"));
    assert!(evol_status_text.contains("SYNCED"));

    let evol_status_json = run_schema_evolution_status(OutputFormat::Json, &catalog).unwrap();
    let evol_statuses: Vec<SchemaEvolutionStatusInfo> =
        serde_json::from_str(&evol_status_json).unwrap();
    assert_eq!(evol_statuses.len(), 2);

    let evol_hist_text = run_schema_evolution_history(OutputFormat::Text, &catalog).unwrap();
    assert!(evol_hist_text.contains("CREATE_VIEW"));

    let evol_hist_json = run_schema_evolution_history(OutputFormat::Json, &catalog).unwrap();
    let evol_hists: Vec<SchemaEvolutionHistoryInfo> =
        serde_json::from_str(&evol_hist_json).unwrap();
    assert_eq!(evol_hists.len(), 2);

    // 4. Audit Tail & Query
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let audit_file = storage_path.join("audit.jsonl");

    let e1 = AuditEvent::now("admin", "view.create", "active_users").with_detail("created view");
    let e2 =
        AuditEvent::now("operator", "workload.create", "analytics").with_detail("created workload");
    let e3 = AuditEvent::now("system", "checkpoint.publish", "ckpt-1").with_detail("published");

    fs::write(
        &audit_file,
        format!(
            "{}\n{}\n{}\n",
            serde_json::to_string(&e1).unwrap(),
            serde_json::to_string(&e2).unwrap(),
            serde_json::to_string(&e3).unwrap()
        ),
    )
    .unwrap();

    let storage = StorageClient::new();
    let tail_text = run_audit_tail(OutputFormat::Text, &storage, storage_path, 10).unwrap();
    assert!(tail_text.contains("view.create"));
    assert!(tail_text.contains("workload.create"));
    assert!(tail_text.contains("checkpoint.publish"));

    let tail_json = run_audit_tail(OutputFormat::Json, &storage, storage_path, 10).unwrap();
    let events: Vec<AuditEvent> = serde_json::from_str(&tail_json).unwrap();
    assert_eq!(events.len(), 3);

    // Query with filter
    let query_text = run_audit_query(
        OutputFormat::Text,
        &storage,
        storage_path,
        Some("workload"),
        10,
    )
    .unwrap();
    assert!(query_text.contains("workload.create"));
    assert!(!query_text.contains("checkpoint.publish"));
}

#[test]
fn test_cli_audit_tail_bounded_cap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let audit_file = storage_path.join("audit.jsonl");

    let mut lines = Vec::new();
    for i in 0..1500 {
        let e = AuditEvent::now("system", format!("action_{i}"), format!("resource_{i}"));
        lines.push(serde_json::to_string(&e).unwrap());
    }
    fs::write(&audit_file, lines.join("\n")).unwrap();

    let storage = StorageClient::new();
    // Bounded to AUDIT_TAIL_MAX_EVENTS (1000)
    let tail = storage.audit_tail(storage_path, 2000).unwrap();
    assert!(tail.len() <= AUDIT_TAIL_MAX_EVENTS);
    assert_eq!(tail.len(), 1000);
}

#[test]
fn test_cli_audit_query_filter_no_match() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path();
    let audit_file = storage_path.join("audit.jsonl");

    let e = AuditEvent::now("admin", "view.create", "active_users");
    fs::write(
        &audit_file,
        format!("{}\n", serde_json::to_string(&e).unwrap()),
    )
    .unwrap();

    let storage = StorageClient::new();
    let query = run_audit_query(
        OutputFormat::Text,
        &storage,
        storage_path,
        Some("nonexistent_filter_pattern"),
        10,
    )
    .unwrap();
    assert_eq!(query, "No audit events found.");
}

#[test]
fn test_cli_resource_usage_workload_not_found() {
    let catalog = CatalogClient::with_defaults();
    let err = run_resource_usage(OutputFormat::Text, &catalog, Some("nonexistent_wl")).unwrap_err();
    assert_eq!(err.code, RS_1005);
    assert!(err.message.contains("nonexistent_wl"));
}

// ─── Slice 5: Explain & SQL Offline Compilation Tests ───────────────────────

#[test]
fn test_cli_explain_and_sql_offline_compilation_golden() {
    let catalog = CatalogClient::with_defaults();

    // 1. Explain view text
    let explain_text =
        run_explain_view(OutputFormat::Text, &catalog, "active_users", false, false).unwrap();
    assert!(explain_text.contains("Aggregate") || explain_text.contains("Source"));

    // 2. Explain view json
    let explain_json =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", false, false).unwrap();
    let plan_info: ExplainPlanInfo = serde_json::from_str(&explain_json).unwrap();
    assert_eq!(plan_info.view_name, "active_users");
    assert!(!plan_info.plan.is_empty());

    // 3. Explain view estimate text & json
    let estimate_text =
        run_explain_view(OutputFormat::Text, &catalog, "active_users", true, false).unwrap();
    assert!(estimate_text.contains("Operator") || estimate_text.contains("state_bytes"));

    let estimate_json =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", true, false).unwrap();
    let est_info: ExplainEstimateInfo = serde_json::from_str(&estimate_json).unwrap();
    assert_eq!(est_info.view_name, "active_users");
    assert!(!est_info.estimates.is_empty());

    // 4. SQL compile text & json
    let sql_text = run_sql_compile(
        OutputFormat::Text,
        "SELECT id, count(*) FROM users GROUP BY id",
    )
    .unwrap();
    assert!(sql_text.contains("Aggregate") || sql_text.contains("Source"));

    let sql_json = run_sql_compile(
        OutputFormat::Json,
        "SELECT id, count(*) FROM users GROUP BY id",
    )
    .unwrap();
    let compile_info: SqlCompileInfo = serde_json::from_str(&sql_json).unwrap();
    assert_eq!(
        compile_info.query,
        "SELECT id, count(*) FROM users GROUP BY id"
    );
    assert!(!compile_info.plan.is_empty());
}

#[test]
fn test_cli_explain_view_not_found_rs1001() {
    let catalog = CatalogClient::with_defaults();
    let err = run_explain_view(
        OutputFormat::Text,
        &catalog,
        "nonexistent_view",
        false,
        false,
    )
    .unwrap_err();
    assert_eq!(err.code, RS_1001);
    assert!(err.message.contains("nonexistent_view"));
    assert!(!err.next_steps.is_empty());
}

#[test]
fn test_cli_explain_view_estimate_not_found() {
    let catalog = CatalogClient::with_defaults();
    let err = run_explain_view(
        OutputFormat::Text,
        &catalog,
        "nonexistent_view",
        true,
        false,
    )
    .unwrap_err();
    assert_eq!(err.code, RS_1001);
    assert!(err.message.contains("nonexistent_view"));
}

#[test]
fn test_cli_explain_view_op_ids_text_and_json() {
    let catalog = CatalogClient::with_defaults();

    // 1. Text format
    let text = run_explain_view(OutputFormat::Text, &catalog, "active_users", false, true).unwrap();
    assert!(text.contains("VIEW  active_users"));
    assert!(text.contains("OPERATORS:"));
    assert!(text.contains("Aggregate") || text.contains("ViewSink"));

    // 2. JSON format
    let json = run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let info: ExplainOpIdInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(info.view_name, "active_users");
    assert!(!info.operators.is_empty());
    assert!(info.operators.iter().any(|op| !op.op_id.is_empty()));
}

#[test]
fn test_cli_debug_arrangement_command_e2e() {
    let catalog = CatalogClient::with_defaults();

    // Get an op_id from explain --op-ids
    let json = run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let explain: ExplainOpIdInfo = serde_json::from_str(&json).unwrap();
    let op = explain
        .operators
        .iter()
        .find(|o| o.kind == "Aggregate")
        .unwrap();

    // 1. Debug arrangement text
    let text = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        &op.op_id,
        "product_id=42",
        Some(1492),
    )
    .unwrap();
    assert!(text.contains(&op.op_id));
    assert!(text.contains("shard-07"));
    assert!(text.contains("product_id=42"));
    assert!(text.contains("weight:      +1"));

    // 2. Debug arrangement json
    let json = run_debug_arrangement(
        OutputFormat::Json,
        &catalog,
        "active_users",
        &op.op_id,
        "product_id=42",
        Some(1492),
    )
    .unwrap();
    let debug: ArrangementDebugInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(debug.view_name, "active_users");
    assert_eq!(debug.op_id, op.op_id);
    assert_eq!(debug.epoch, 1492);
    assert_eq!(debug.weight, 1);

    // 3. Negative test: Unknown op_id -> RS_1020
    let err_op = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        "op-999999999999",
        "product_id=42",
        None,
    )
    .unwrap_err();
    assert_eq!(err_op.code, RS_1020);
    assert!(err_op.message.contains("op-999999999999"));
    assert!(err_op.next_steps.contains("rockstream explain"));

    // 4. Negative test: Epoch pruned (< 10) -> RS_2006
    let err_epoch = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        &op.op_id,
        "product_id=42",
        Some(5),
    )
    .unwrap_err();
    assert_eq!(err_epoch.code, RS_2006);
    assert!(err_epoch.message.contains("outside the retention window"));

    // 5. Negative test: Unparseable key -> RS_1021
    let err_key = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        &op.op_id,
        "invalid, key, format",
        None,
    )
    .unwrap_err();
    assert_eq!(err_key.code, RS_1021);
}

#[test]
fn test_cli_sql_compile_syntax_error_rs1012() {
    let err = run_sql_compile(OutputFormat::Text, "SELECT FROM WHERE INVALID SQL @#$").unwrap_err();
    assert_eq!(err.code, RS_1012);
    assert!(err.message.contains("SQL syntax error") || err.message.contains("syntax"));
    assert!(!err.next_steps.is_empty());
}

#[test]
fn test_cli_view_status_text_with_lag_breakdown() {
    let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 40,
    };
    rockstream_types::metrics::set_view_stage_lag("active_users", lag);

    let out_text = run_view_status(OutputFormat::Text, &catalog, Some("active_users")).unwrap();
    assert!(out_text.contains("active_users"));
    assert!(out_text.contains("LAG (MS)"));
    assert!(out_text.contains("40 (src:10 dec:4 cmp:12 aln:3 snk:8 spl:2 stg:1)"));
}

#[test]
fn test_cli_view_status_json_with_lag_breakdown() {
    let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 40,
    };
    rockstream_types::metrics::set_view_stage_lag("active_users", lag);

    let out_json = run_view_status(OutputFormat::Json, &catalog, Some("active_users")).unwrap();
    let statuses: Vec<ViewStatusInfo> = serde_json::from_str(&out_json).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].view_name, "active_users");
    assert_eq!(statuses[0].stage_lag, Some(lag));
}

#[test]
fn test_cli_view_status_explainability_text_exact() {
    let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 0,
        storage_pressure_ms: 1,
        total_lag_ms: 38,
    };
    rockstream_types::metrics::set_view_stage_lag("active_users", lag);

    let out_text = run_view_status(OutputFormat::Text, &catalog, Some("active_users")).unwrap();
    let expected = [
        format!(
            "{:<15} {:<20} {:<15} {:<15} {:<10} {:<12} {:<45} {:<34} {:<10} {:<18} {:<30} {:<20}",
            "NAMESPACE",
            "VIEW",
            "STATE",
            "WORKLOAD",
            "SLO (MS)",
            "MEM LIMIT",
            "LAG (MS)",
            "REASON",
            "CODE",
            "DOMINANT",
            "PROGRESS",
            "DEPENDS ON"
        ),
        "-".repeat(300),
        format!(
            "{:<15} {:<20} {:<15} {:<15} {:<10} {:<12} {:<45} {:<34} {:<10} {:<18} {:<30} {:<20}",
            "public",
            "active_users",
            "RUNNING",
            "analytics",
            "5000",
            "536870912",
            "38 (src:10 dec:4 cmp:12 aln:3 snk:8 spl:0 stg:1)",
            "sink_blocked",
            "RS-3706",
            "compute_lag",
            "-",
            "users_source"
        ),
    ]
    .join("\n");
    assert_eq!(out_text, expected);
}

#[test]
fn test_cli_view_status_explainability_json_exact() {
    let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 0,
        storage_pressure_ms: 1,
        total_lag_ms: 38,
    };
    rockstream_types::metrics::set_view_stage_lag("active_users", lag);

    let out_json = run_view_status(OutputFormat::Json, &catalog, Some("active_users")).unwrap();
    let statuses: Vec<ViewStatusInfo> = serde_json::from_str(&out_json).unwrap();
    assert_eq!(
        statuses,
        vec![ViewStatusInfo {
            namespace: "public".to_string(),
            view_name: "active_users".to_string(),
            state: "RUNNING".to_string(),
            workload_name: Some("analytics".to_string()),
            freshness_slo_ms: Some(5000),
            memory_limit_bytes: Some(536870912),
            depends_on: vec!["users_source".to_string()],
            stage_lag: Some(lag),
            degradation_reason: DegradationReason::SinkBlocked,
            reason_code: "RS-3706".to_string(),
            dominant_contributor: DominantContributor::ComputeLag,
            progress_phase: None,
            bytes_remaining: None,
            rows_remaining: None,
            estimated_remaining_ms: None,
        }]
    );
}
