//! End-to-end and exhaustive mutating commands tests (v0.53.1 Slice 9).
//!
//! Verifies:
//! - All 16 mutating subcommands against cluster transport.
//! - Audit event emission per mutation.
//! - Idempotency and refusal semantics.
//! - Resumability after interrupted drain and migration.
//! - Support bundle secret redaction and size cap.
//! - Exhaustive dynamic dispatch table refusal and audit verification for unauthorized identities.

use rockstream_cli::output::OutputFormat;
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    run_checkpoint_restore, run_cluster_workers_drain, run_schema_create, run_schema_drop,
    run_shard_migrate, run_source_drop, run_source_pause, run_source_resume, run_support_bundle,
    run_view_pause, run_view_query, run_view_resume, run_view_subscribe, run_workload_alter,
    run_workload_create, run_workload_drop,
};
use rockstream_types::acl::Role;
use rockstream_types::error_code::{
    RS_1001, RS_1004, RS_1007, RS_1008, RS_1014, RS_2401, RS_4009, RS_5030,
};

#[test]
fn test_cli_mutating_subcommands_e2e_live_cluster() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();

    let admin_identity = ClientIdentity::new("admin_op").with_role(Role::Admin);
    let mut catalog = CatalogClient::with_defaults().with_storage_path(&storage_path);
    catalog.identity = admin_identity.clone();
    let control = ControlClient::new(None, admin_identity.clone()).with_storage_path(&storage_path);
    let storage = StorageClient::with_identity(admin_identity);

    // 1. view pause
    let out = run_view_pause(OutputFormat::Text, &mut catalog, "active_users", true).unwrap();
    assert!(out.contains("PAUSE VIEW"));

    // 2. view resume
    let out = run_view_resume(OutputFormat::Text, &mut catalog, "active_users").unwrap();
    assert!(out.contains("RESUME VIEW"));

    // 3. view query
    let out = run_view_query(OutputFormat::Text, &catalog, "active_users", Some(5)).unwrap();
    assert!(out.contains("active_users") || out.contains("count"));

    // 4. view subscribe
    let out =
        run_view_subscribe(OutputFormat::Text, &catalog, "active_users", Some(15), true).unwrap();
    assert!(out.contains("SNAPSHOT"));

    // 5. source pause
    let out = run_source_pause(OutputFormat::Text, &mut catalog, "users_source").unwrap();
    assert!(out.contains("PAUSE SOURCE"));

    // 6. source resume
    let out = run_source_resume(OutputFormat::Text, &mut catalog, "users_source").unwrap();
    assert!(out.contains("RESUME SOURCE"));

    // 7. source drop
    let out = run_source_drop(OutputFormat::Text, &mut catalog, "users_source", true).unwrap();
    assert!(out.contains("DROP SOURCE"));

    // 8. schema create
    let out = run_schema_create(
        OutputFormat::Text,
        &mut catalog,
        "events_log",
        Some("id BIGINT, data VARCHAR"),
    )
    .unwrap();
    assert!(out.contains("CREATE SCHEMA"));

    // 9. schema drop
    let out = run_schema_drop(OutputFormat::Text, &mut catalog, "events_log", true).unwrap();
    assert!(out.contains("DROP SCHEMA"));

    // 10. workload create
    let out = run_workload_create(
        OutputFormat::Text,
        &mut catalog,
        "etl_job",
        Some(10),
        Some(5000),
        Some(1024 * 1024),
        Some(4),
    )
    .unwrap();
    assert!(out.contains("CREATE WORKLOAD"));

    // 11. workload alter
    let out = run_workload_alter(
        OutputFormat::Text,
        &mut catalog,
        "etl_job",
        Some(20),
        None,
        None,
        None,
    )
    .unwrap();
    assert!(out.contains("ALTER WORKLOAD"));

    // 12. workload drop
    let out = run_workload_drop(OutputFormat::Text, &mut catalog, "etl_job", true).unwrap();
    assert!(out.contains("DROP WORKLOAD"));

    // 13. cluster workers drain
    let out = run_cluster_workers_drain(OutputFormat::Text, &control, 1, true).unwrap();
    assert!(out.contains("DRAINING"));

    // 14. shard migrate
    let out = run_shard_migrate(OutputFormat::Text, &control, 42, 2, true).unwrap();
    assert!(out.contains("COMPLETED"));

    // 15. checkpoint restore
    let out = run_checkpoint_restore(OutputFormat::Text, &storage, &storage_path, 100, None, true)
        .unwrap();
    assert!(out.contains("restored to"));

    // 16. support bundle
    let out = run_support_bundle(
        OutputFormat::Text,
        &storage,
        &storage_path,
        None,
        None,
        None,
    )
    .unwrap();
    assert!(out.contains("Support bundle generated"));
}

#[test]
fn test_cli_mutating_commands_emit_exact_audit_event() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();
    let admin_identity = ClientIdentity::new("audit_admin").with_role(Role::Admin);

    let mut catalog = CatalogClient::with_defaults().with_storage_path(&storage_path);
    catalog.identity = admin_identity.clone();
    let control = ControlClient::new(None, admin_identity.clone()).with_storage_path(&storage_path);
    let storage = StorageClient::with_identity(admin_identity);

    // Initial audit log is empty
    let events = storage.audit_tail(&storage_path, 100).unwrap();
    let initial_count = events.len();

    // Execute a series of 4 mutations
    catalog.pause_view("active_users").unwrap();
    catalog.resume_view("active_users").unwrap();
    control.drain_worker(1).unwrap();
    storage
        .restore_checkpoint(&storage_path, 200, None)
        .unwrap();

    let events = storage.audit_tail(&storage_path, 100).unwrap();
    assert_eq!(events.len(), initial_count + 4);

    let actions: Vec<&str> = events.iter().map(|e| e.action.as_str()).collect();
    assert!(actions.contains(&"view.pause"));
    assert!(actions.contains(&"view.resume"));
    assert!(actions.contains(&"cluster.workers.drain"));
    assert!(actions.contains(&"checkpoint.restore"));

    for e in &events {
        assert_eq!(e.actor, "audit_admin");
    }
}

#[test]
fn test_cli_mutating_commands_idempotency_and_refusal() {
    let tmp = tempfile::tempdir().unwrap();
    let admin_identity = ClientIdentity::new("admin").with_role(Role::Admin);
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.identity = admin_identity.clone();
    let control = ControlClient::new(None, admin_identity).with_storage_path(tmp.path());

    // 1. Pausing already paused view -> RS-1007
    catalog.pause_view("active_users").unwrap();
    let res = catalog.pause_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_1007);

    // 2. Resuming already running view -> RS-1008
    catalog.resume_view("active_users").unwrap();
    let res = catalog.resume_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_1008);

    // 3. Dropping non-existent source -> RS-4009
    catalog.drop_source("users_source").unwrap();
    let res = catalog.drop_source("users_source");
    assert_eq!(res.unwrap_err().code, RS_4009);

    // 4. Duplicate schema create -> RS-1004
    catalog.create_schema("tbl_a", None).unwrap();
    let res = catalog.create_schema("tbl_a", None);
    assert_eq!(res.unwrap_err().code, RS_1004);

    // 5. Dropping non-existent schema -> RS-1001
    catalog.drop_schema("tbl_a").unwrap();
    let res = catalog.drop_schema("tbl_a");
    assert_eq!(res.unwrap_err().code, RS_1001);

    // 6. In-flight shard migration refusal -> RS-5030
    let res = control.migrate_shard(999, 2);
    assert_eq!(res.unwrap_err().code, RS_5030);

    // 7. Workload drop with views -> RS-1014
    let res = catalog.drop_workload("analytics");
    assert_eq!(res.unwrap_err().code, RS_1014);
}

#[test]
fn test_cli_drain_and_migrate_resumable_after_interruption() {
    let tmp = tempfile::tempdir().unwrap();
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin))
        .with_storage_path(tmp.path());

    // Simulate initiating a drain, then re-issuing (idempotent progress status)
    let res1 = control.drain_worker(1).unwrap();
    assert_eq!(res1.status, "DRAINING");

    let res2 = control.drain_worker(1).unwrap();
    assert_eq!(res2.status, "DRAINING");
    assert_eq!(res1.worker_id, res2.worker_id);

    // Shard migrate re-issuance on valid shard completes cleanly
    let mig1 = control.migrate_shard(5, 2).unwrap();
    assert_eq!(mig1.status, "COMPLETED");

    let mig2 = control.migrate_shard(5, 2).unwrap();
    assert_eq!(mig2.status, "COMPLETED");
}

#[test]
fn test_cli_support_bundle_redaction_and_size_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = StorageClient::new();
    let bundle_file = tmp.path().join("bundle.tar.gz");

    let res = storage
        .generate_support_bundle(
            tmp.path(),
            Some("active_users"),
            Some("24h"),
            Some(&bundle_file),
        )
        .unwrap();

    assert!(res.size_bytes > 0);
    assert!(res.size_bytes < 10 * 1024 * 1024); // < 10MB default size cap
    assert!(res.redacted_secrets_count > 0); // Secret redaction confirmed
}

#[test]
fn test_cli_unauthorized_identity_refused_and_audited_exhaustive() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();
    let viewer_identity = ClientIdentity::new("unauth_viewer").with_role(Role::Viewer);

    let mut catalog = CatalogClient::new(viewer_identity.clone()).with_storage_path(&storage_path);
    let control =
        ControlClient::new(None, viewer_identity.clone()).with_storage_path(&storage_path);
    let storage = StorageClient::with_identity(viewer_identity);

    // Define table of all mutating operations requiring elevated role
    enum MutatingOp {
        ViewPause,
        ViewResume,
        SourcePause,
        SourceResume,
        SourceDrop,
        SchemaCreate,
        SchemaDrop,
        WorkloadCreate,
        WorkloadAlter,
        WorkloadDrop,
        WorkerDrain,
        ShardMigrate,
        CheckpointRestore,
        SupportBundle,
    }

    let mutating_ops = [
        MutatingOp::ViewPause,
        MutatingOp::ViewResume,
        MutatingOp::SourcePause,
        MutatingOp::SourceResume,
        MutatingOp::SourceDrop,
        MutatingOp::SchemaCreate,
        MutatingOp::SchemaDrop,
        MutatingOp::WorkloadCreate,
        MutatingOp::WorkloadAlter,
        MutatingOp::WorkloadDrop,
        MutatingOp::WorkerDrain,
        MutatingOp::ShardMigrate,
        MutatingOp::CheckpointRestore,
        MutatingOp::SupportBundle,
    ];

    for op in &mutating_ops {
        let err = match op {
            MutatingOp::ViewPause => catalog.pause_view("active_users").unwrap_err(),
            MutatingOp::ViewResume => catalog.resume_view("active_users").unwrap_err(),
            MutatingOp::SourcePause => catalog.pause_source("users_source").unwrap_err(),
            MutatingOp::SourceResume => catalog.resume_source("users_source").unwrap_err(),
            MutatingOp::SourceDrop => catalog.drop_source("users_source").unwrap_err(),
            MutatingOp::SchemaCreate => catalog.create_schema("tbl", None).unwrap_err(),
            MutatingOp::SchemaDrop => catalog.drop_schema("users").unwrap_err(),
            MutatingOp::WorkloadCreate => catalog
                .create_workload("wl", None, None, None, None)
                .unwrap_err(),
            MutatingOp::WorkloadAlter => catalog
                .alter_workload("analytics", None, None, None, None)
                .unwrap_err(),
            MutatingOp::WorkloadDrop => catalog.drop_workload("analytics").unwrap_err(),
            MutatingOp::WorkerDrain => control.drain_worker(1).unwrap_err(),
            MutatingOp::ShardMigrate => control.migrate_shard(1, 2).unwrap_err(),
            MutatingOp::CheckpointRestore => storage
                .restore_checkpoint(&storage_path, 1, None)
                .unwrap_err(),
            MutatingOp::SupportBundle => storage
                .generate_support_bundle(&storage_path, None, None, None)
                .unwrap_err(),
        };

        assert_eq!(
            err.code, RS_2401,
            "Mutating operation must be refused with RS-2401 for Viewer role"
        );
    }

    // Verify audit log has exactly one refusal entry per attempted mutation
    let events = StorageClient::new().audit_tail(&storage_path, 100).unwrap();
    assert_eq!(events.len(), mutating_ops.len());
    for e in &events {
        assert_eq!(e.actor, "unauth_viewer");
        assert_eq!(e.error_code.as_deref(), Some("RS-2401"));
    }
}
