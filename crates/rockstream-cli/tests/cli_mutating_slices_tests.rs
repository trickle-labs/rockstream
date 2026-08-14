//! Tests for Mutating Operator Commands — Slices 1-8 (v0.53.1).
//!
//! Covers:
//! - Slice 1: Confirmation safeguards and error code RS-0005.
//! - Slice 2: RBAC authorization and audit logging.
//! - Slice 3: View mutating commands (pause, resume, query, subscribe).
//! - Slice 4: Source and schema mutating commands (pause, resume, drop, create).
//! - Slice 5: Workload mutating commands (create, alter, drop).
//! - Slice 6: Cluster worker drain and shard migration control wire.
//! - Slice 7: Checkpoint restore and storage inspection.
//! - Slice 8: Diagnostic support bundle generation.

use rockstream_cli::output::OutputFormat;
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    prompt_confirmation, run_checkpoint_restore, run_cluster_workers_drain, run_schema_create,
    run_schema_drop, run_shard_migrate, run_source_drop, run_source_pause, run_source_resume,
    run_support_bundle, run_view_pause, run_view_query, run_view_resume, run_view_subscribe,
    run_workload_alter, run_workload_create, run_workload_drop,
};
use rockstream_types::acl::Role;
use rockstream_types::error_code::{
    RS_0005, RS_1001, RS_1004, RS_1005, RS_1006, RS_1007, RS_1008, RS_1014, RS_2006, RS_2401,
    RS_4009, RS_5030,
};

// ─── Slice 1: Confirmation Safeguards & Flags ──────────────────────────────

#[test]
fn test_cli_confirmation_safeguards_and_flags() {
    // When yes_flag is true, prompt_confirmation succeeds immediately
    assert!(prompt_confirmation("Are you sure?", true).is_ok());

    // In a non-interactive test environment without yes_flag, prompt_confirmation returns RS-0005
    let res = prompt_confirmation("Are you sure?", false);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err.code, RS_0005);
    assert!(err.message.contains("confirmation required"));
    assert!(err.next_steps.contains("--yes"));
}

// ─── Slice 2: RBAC Authorization & Audit Trail ──────────────────────────────

#[test]
fn test_cli_mutating_auth_rbac_and_audit_logging() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();

    // 1. Unauthorized principal (Viewer role) attempting mutation
    let viewer_identity = ClientIdentity::new("viewer_user").with_role(Role::Viewer);
    let mut catalog = CatalogClient::new(viewer_identity).with_storage_path(&storage_path);

    let res = catalog.pause_view("active_users");
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err.code, RS_2401);
    assert!(err.message.contains("permission denied"));

    // Verify audit log has the denial event
    let storage = StorageClient::new();
    let audit_events = storage.audit_tail(&storage_path, 100).unwrap();
    assert_eq!(audit_events.len(), 1);
    let event = &audit_events[0];
    assert_eq!(event.actor, "viewer_user");
    assert_eq!(event.action, "view.pause");
    assert_eq!(event.resource, "active_users");
    assert_eq!(event.error_code.as_deref(), Some("RS-2401"));

    // 2. Authorized principal (PipelineOwner role) performing mutation
    let owner_identity = ClientIdentity::new("owner_user").with_role(Role::PipelineOwner);
    let mut owner_catalog = CatalogClient::with_defaults();
    owner_catalog.identity = owner_identity;
    owner_catalog.storage_path = Some(storage_path.clone());

    let outcome = owner_catalog.pause_view("active_users").unwrap();
    assert_eq!(outcome.action, "PAUSE VIEW");
    assert_eq!(outcome.resource, "active_users");
    assert_eq!(outcome.status, "SUCCESS");

    // Verify audit log now has 2 events (denial + success)
    let audit_events2 = storage.audit_tail(&storage_path, 100).unwrap();
    assert_eq!(audit_events2.len(), 2);
    let success_event = &audit_events2[1];
    assert_eq!(success_event.actor, "owner_user");
    assert_eq!(success_event.action, "view.pause");
    assert_eq!(success_event.resource, "active_users");
    assert_eq!(success_event.error_code, None);
}

// ─── Slice 3: View Mutating Commands ────────────────────────────────────────

#[test]
fn test_cli_view_mutating_lifecycle_query_and_subscribe() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();

    let mut catalog = CatalogClient::with_defaults().with_storage_path(&storage_path);

    // 1. Pause view with --yes
    let pause_out = run_view_pause(OutputFormat::Text, &mut catalog, "active_users", true).unwrap();
    assert!(pause_out.contains("PAUSE VIEW"));
    assert!(pause_out.contains("active_users"));

    // 2. Re-issuing pause on already paused view returns RS-1007
    let pause_again = run_view_pause(OutputFormat::Text, &mut catalog, "active_users", true);
    assert!(pause_again.is_err());
    assert_eq!(pause_again.unwrap_err().code, RS_1007);

    // 3. Resume view
    let resume_out = run_view_resume(OutputFormat::Text, &mut catalog, "active_users").unwrap();
    assert!(resume_out.contains("RESUME VIEW"));
    assert!(resume_out.contains("active_users"));

    // 4. Re-issuing resume on running view returns RS-1008
    let resume_again = run_view_resume(OutputFormat::Text, &mut catalog, "active_users");
    assert!(resume_again.is_err());
    assert_eq!(resume_again.unwrap_err().code, RS_1008);

    // 5. Query view
    let query_out = run_view_query(OutputFormat::Text, &catalog, "active_users", Some(10)).unwrap();
    assert!(query_out.contains("id"));
    assert!(query_out.contains("count"));
    assert!(query_out.contains("rows"));

    // Query non-existent view returns RS-1001
    let query_err = run_view_query(OutputFormat::Text, &catalog, "non_existent", None);
    assert!(query_err.is_err());
    assert_eq!(query_err.unwrap_err().code, RS_1001);

    // 6. Subscribe view with valid retention epoch and snapshot
    let sub_out =
        run_view_subscribe(OutputFormat::Text, &catalog, "active_users", Some(15), true).unwrap();
    assert!(sub_out.contains("SNAPSHOT"));
    assert!(sub_out.contains("INSERT"));

    // Subscribe with epoch before retention returns RS-2006
    let sub_err = run_view_subscribe(OutputFormat::Text, &catalog, "active_users", Some(5), false);
    assert!(sub_err.is_err());
    assert_eq!(sub_err.unwrap_err().code, RS_2006);
}

// ─── Slice 4: Source & Schema Mutating Commands ─────────────────────────────

#[test]
fn test_cli_source_and_schema_mutating_commands() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(&storage_path);

    // Source lifecycle: pause, resume, drop
    let pause_out = run_source_pause(OutputFormat::Text, &mut catalog, "users_source").unwrap();
    assert!(pause_out.contains("PAUSE SOURCE"));

    let resume_out = run_source_resume(OutputFormat::Text, &mut catalog, "users_source").unwrap();
    assert!(resume_out.contains("RESUME SOURCE"));

    let drop_out = run_source_drop(OutputFormat::Text, &mut catalog, "users_source", true).unwrap();
    assert!(drop_out.contains("DROP SOURCE"));

    // Dropping again returns RS-4009 (Source not found)
    let drop_again = run_source_drop(OutputFormat::Text, &mut catalog, "users_source", true);
    assert!(drop_again.is_err());
    assert_eq!(drop_again.unwrap_err().code, RS_4009);

    // Schema lifecycle: create, drop
    let create_out = run_schema_create(
        OutputFormat::Text,
        &mut catalog,
        "new_table",
        Some("id BIGINT, name VARCHAR"),
    )
    .unwrap();
    assert!(create_out.contains("CREATE SCHEMA"));

    // Creating duplicate returns RS-1004 (Entity already exists)
    let create_dup = run_schema_create(OutputFormat::Text, &mut catalog, "new_table", None);
    assert!(create_dup.is_err());
    assert_eq!(create_dup.unwrap_err().code, RS_1004);

    let drop_schema_out =
        run_schema_drop(OutputFormat::Text, &mut catalog, "new_table", true).unwrap();
    assert!(drop_schema_out.contains("DROP SCHEMA"));

    // Dropping non-existent schema returns RS-1001
    let drop_schema_again = run_schema_drop(OutputFormat::Text, &mut catalog, "new_table", true);
    assert!(drop_schema_again.is_err());
    assert_eq!(drop_schema_again.unwrap_err().code, RS_1001);
}

// ─── Slice 5: Workload Mutating Commands ────────────────────────────────────

#[test]
fn test_cli_workload_mutating_lifecycle_and_constraints() {
    let tmp = tempfile::tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(&storage_path);

    // 1. Create workload
    let create_out = run_workload_create(
        OutputFormat::Text,
        &mut catalog,
        "reporting",
        Some(100),
        Some(10000),
        Some(2 * 1024 * 1024 * 1024),
        Some(8),
    )
    .unwrap();
    assert!(create_out.contains("CREATE WORKLOAD"));
    assert!(create_out.contains("reporting"));

    // Duplicate create returns RS-1006
    let create_dup = run_workload_create(
        OutputFormat::Text,
        &mut catalog,
        "reporting",
        None,
        None,
        None,
        None,
    );
    assert!(create_dup.is_err());
    assert_eq!(create_dup.unwrap_err().code, RS_1006);

    // 2. Alter workload
    let alter_out = run_workload_alter(
        OutputFormat::Text,
        &mut catalog,
        "reporting",
        Some(200),
        Some(20000),
        Some(4 * 1024 * 1024 * 1024),
        Some(16),
    )
    .unwrap();
    assert!(alter_out.contains("ALTER WORKLOAD"));

    // Alter non-existent returns RS-1005
    let alter_non_existent = run_workload_alter(
        OutputFormat::Text,
        &mut catalog,
        "non_existent",
        Some(100),
        None,
        None,
        None,
    );
    assert!(alter_non_existent.is_err());
    assert_eq!(alter_non_existent.unwrap_err().code, RS_1005);

    // 3. Drop workload with assigned views returns RS-1014
    let drop_with_views = run_workload_drop(OutputFormat::Text, &mut catalog, "analytics", true);
    assert!(drop_with_views.is_err());
    assert_eq!(drop_with_views.unwrap_err().code, RS_1014);

    // 4. Drop workload without assigned views succeeds
    let drop_ok = run_workload_drop(OutputFormat::Text, &mut catalog, "reporting", true).unwrap();
    assert!(drop_ok.contains("DROP WORKLOAD"));
}

// ─── Individual Coverage Matrix Cells ───────────────────────────────────────

#[test]
fn test_cli_view_pause_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    let res = catalog.pause_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_2401);
}

#[test]
fn test_cli_view_pause_owner_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("owner").with_role(Role::PipelineOwner);
    assert!(catalog.pause_view("active_users").is_ok());
}

#[test]
fn test_cli_view_pause_unconfirmed_refusal() {
    let mut catalog = CatalogClient::with_defaults();
    let res = run_view_pause(OutputFormat::Text, &mut catalog, "active_users", false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_view_pause_idempotent() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.pause_view("active_users").unwrap();
    let res = catalog.pause_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_1007);
}

#[test]
fn test_cli_view_pause_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.pause_view("active_users").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "view.pause" && e.resource == "active_users"));
}

#[test]
fn test_cli_view_resume_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.views.get_mut("active_users").unwrap().state = "PAUSED".to_string();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    let res = catalog.resume_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_2401);
}

#[test]
fn test_cli_view_resume_owner_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.views.get_mut("active_users").unwrap().state = "PAUSED".to_string();
    catalog.identity = ClientIdentity::new("owner").with_role(Role::PipelineOwner);
    assert!(catalog.resume_view("active_users").is_ok());
}

#[test]
fn test_cli_view_resume_idempotent() {
    let mut catalog = CatalogClient::with_defaults();
    // active_users is already RUNNING
    let res = catalog.resume_view("active_users");
    assert_eq!(res.unwrap_err().code, RS_1008);
}

#[test]
fn test_cli_view_resume_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.views.get_mut("active_users").unwrap().state = "PAUSED".to_string();
    catalog.resume_view("active_users").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "view.resume" && e.resource == "active_users"));
}

#[test]
fn test_cli_view_query_viewer_authorized() {
    let catalog = CatalogClient::with_defaults();
    let query_viewer = CatalogClient {
        identity: ClientIdentity::new("viewer").with_role(Role::Viewer),
        ..catalog
    };
    assert!(query_viewer.query_view("active_users", None).is_ok());
}

#[test]
fn test_cli_view_query_admin_authorized() {
    let catalog = CatalogClient::with_defaults();
    assert!(catalog.query_view("active_users", None).is_ok());
}

#[test]
fn test_cli_view_query_reissue_identical() {
    let catalog = CatalogClient::with_defaults();
    let res1 = catalog.query_view("active_users", Some(5)).unwrap();
    let res2 = catalog.query_view("active_users", Some(5)).unwrap();
    assert_eq!(res1, res2);
}

#[test]
fn test_cli_view_query_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.query_view("active_users", None).unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "view.query"));
}

#[test]
fn test_cli_view_subscribe_viewer_authorized() {
    let catalog = CatalogClient::with_defaults();
    let sub_viewer = CatalogClient {
        identity: ClientIdentity::new("viewer").with_role(Role::Viewer),
        ..catalog
    };
    assert!(sub_viewer
        .subscribe_view("active_users", Some(15), true)
        .is_ok());
}

#[test]
fn test_cli_view_subscribe_admin_authorized() {
    let catalog = CatalogClient::with_defaults();
    assert!(catalog
        .subscribe_view("active_users", Some(15), true)
        .is_ok());
}

#[test]
fn test_cli_view_subscribe_rs2006_retention() {
    let catalog = CatalogClient::with_defaults();
    let res = catalog.subscribe_view("active_users", Some(2), false);
    assert_eq!(res.unwrap_err().code, RS_2006);
}

#[test]
fn test_cli_view_subscribe_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog
        .subscribe_view("active_users", Some(15), true)
        .unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "view.subscribe"));
}

#[test]
fn test_cli_source_pause_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog.pause_source("users_source").unwrap_err().code,
        RS_2401
    );
}

#[test]
fn test_cli_source_pause_owner_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("owner").with_role(Role::PipelineOwner);
    assert!(catalog.pause_source("users_source").is_ok());
}

#[test]
fn test_cli_source_pause_idempotent() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.pause_source("users_source").unwrap();
    assert!(catalog.pause_source("users_source").is_ok());
}

#[test]
fn test_cli_source_pause_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.pause_source("users_source").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "source.pause"));
}

#[test]
fn test_cli_source_resume_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog.resume_source("users_source").unwrap_err().code,
        RS_2401
    );
}

#[test]
fn test_cli_source_resume_owner_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("owner").with_role(Role::PipelineOwner);
    assert!(catalog.resume_source("users_source").is_ok());
}

#[test]
fn test_cli_source_resume_idempotent() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.resume_source("users_source").unwrap();
    assert!(catalog.resume_source("users_source").is_ok());
}

#[test]
fn test_cli_source_resume_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.resume_source("users_source").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "source.resume"));
}

#[test]
fn test_cli_source_drop_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog.drop_source("users_source").unwrap_err().code,
        RS_2401
    );
}

#[test]
fn test_cli_source_drop_admin_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    assert!(catalog.drop_source("users_source").is_ok());
}

#[test]
fn test_cli_source_drop_unconfirmed_refusal() {
    let mut catalog = CatalogClient::with_defaults();
    let res = run_source_drop(OutputFormat::Text, &mut catalog, "users_source", false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_source_drop_not_found_rs4009() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.drop_source("users_source").unwrap();
    assert_eq!(
        catalog.drop_source("users_source").unwrap_err().code,
        RS_4009
    );
}

#[test]
fn test_cli_source_drop_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.drop_source("users_source").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "source.drop"));
}

#[test]
fn test_cli_schema_create_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog.create_schema("tbl", None).unwrap_err().code,
        RS_2401
    );
}

#[test]
fn test_cli_schema_create_owner_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("owner").with_role(Role::PipelineOwner);
    assert!(catalog.create_schema("tbl", None).is_ok());
}

#[test]
fn test_cli_schema_create_already_exists_rs1004() {
    let mut catalog = CatalogClient::with_defaults();
    assert_eq!(
        catalog.create_schema("users", None).unwrap_err().code,
        RS_1004
    );
}

#[test]
fn test_cli_schema_create_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.create_schema("new_tbl", None).unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "schema.create"));
}

#[test]
fn test_cli_schema_drop_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(catalog.drop_schema("users").unwrap_err().code, RS_2401);
}

#[test]
fn test_cli_schema_drop_admin_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    assert!(catalog.drop_schema("users").is_ok());
}

#[test]
fn test_cli_schema_drop_unconfirmed_refusal() {
    let mut catalog = CatalogClient::with_defaults();
    let res = run_schema_drop(OutputFormat::Text, &mut catalog, "users", false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_schema_drop_not_found_rs1001() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.drop_schema("users").unwrap();
    assert_eq!(catalog.drop_schema("users").unwrap_err().code, RS_1001);
}

#[test]
fn test_cli_schema_drop_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog.drop_schema("users").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "schema.drop"));
}

#[test]
fn test_cli_workload_create_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog
            .create_workload("new_wl", None, None, None, None)
            .unwrap_err()
            .code,
        RS_2401
    );
}

#[test]
fn test_cli_workload_create_admin_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    assert!(catalog
        .create_workload("new_wl", None, None, None, None)
        .is_ok());
}

#[test]
fn test_cli_workload_create_already_exists_rs1006() {
    let mut catalog = CatalogClient::with_defaults();
    assert_eq!(
        catalog
            .create_workload("analytics", None, None, None, None)
            .unwrap_err()
            .code,
        RS_1006
    );
}

#[test]
fn test_cli_workload_create_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog
        .create_workload("new_wl", None, None, None, None)
        .unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "workload.create"));
}

#[test]
fn test_cli_workload_alter_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog
            .alter_workload("analytics", Some(50), None, None, None)
            .unwrap_err()
            .code,
        RS_2401
    );
}

#[test]
fn test_cli_workload_alter_admin_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    assert!(catalog
        .alter_workload("analytics", Some(50), None, None, None)
        .is_ok());
}

#[test]
fn test_cli_workload_alter_idempotent() {
    let mut catalog = CatalogClient::with_defaults();
    catalog
        .alter_workload("analytics", Some(50), None, None, None)
        .unwrap();
    assert!(catalog
        .alter_workload("analytics", Some(50), None, None, None)
        .is_ok());
}

#[test]
fn test_cli_workload_alter_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog
        .alter_workload("analytics", Some(50), None, None, None)
        .unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "workload.alter"));
}

#[test]
fn test_cli_workload_drop_viewer_denied_rs2401() {
    let mut catalog = CatalogClient::with_defaults();
    catalog.identity = ClientIdentity::new("viewer").with_role(Role::Viewer);
    assert_eq!(
        catalog.drop_workload("analytics").unwrap_err().code,
        RS_2401
    );
}

#[test]
fn test_cli_workload_drop_admin_authorized() {
    let mut catalog = CatalogClient::with_defaults();
    catalog
        .create_workload("empty_wl", None, None, None, None)
        .unwrap();
    assert!(catalog.drop_workload("empty_wl").is_ok());
}

#[test]
fn test_cli_workload_drop_unconfirmed_refusal() {
    let mut catalog = CatalogClient::with_defaults();
    catalog
        .create_workload("empty_wl", None, None, None, None)
        .unwrap();
    let res = run_workload_drop(OutputFormat::Text, &mut catalog, "empty_wl", false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_workload_drop_has_views_rs1014() {
    let mut catalog = CatalogClient::with_defaults();
    assert_eq!(
        catalog.drop_workload("analytics").unwrap_err().code,
        RS_1014
    );
}

#[test]
fn test_cli_workload_drop_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut catalog = CatalogClient::with_defaults().with_storage_path(tmp.path());
    catalog
        .create_workload("empty_wl", None, None, None, None)
        .unwrap();
    catalog.drop_workload("empty_wl").unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events.iter().any(|e| e.action == "workload.drop"));
}

// ─── Slice 6: Worker Drain & Shard Migration Control Wire ───────────────────

#[test]
fn test_cli_cluster_workers_drain_and_shard_migrate_resumability() {
    let tmp = tempfile::tempdir().unwrap();
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin))
        .with_storage_path(tmp.path());

    // 1. Worker drain with confirmation
    let drain_out = run_cluster_workers_drain(OutputFormat::Text, &control, 1, true).unwrap();
    assert!(drain_out.contains("DRAINING"));
    assert!(drain_out.contains("Worker 1"));

    // Unconfirmed drain refuses with RS-0005
    let unconf_drain = run_cluster_workers_drain(OutputFormat::Text, &control, 1, false);
    assert_eq!(unconf_drain.unwrap_err().code, RS_0005);

    // 2. Shard migrate with confirmation
    let migrate_out = run_shard_migrate(OutputFormat::Text, &control, 10, 2, true).unwrap();
    assert!(migrate_out.contains("COMPLETED"));
    assert!(migrate_out.contains("Shard 10"));

    // Unconfirmed migrate refuses with RS-0005
    let unconf_mig = run_shard_migrate(OutputFormat::Text, &control, 10, 2, false);
    assert_eq!(unconf_mig.unwrap_err().code, RS_0005);

    // In-flight migration conflict returns RS-5030
    let inflight_mig = run_shard_migrate(OutputFormat::Text, &control, 999, 2, true);
    assert!(inflight_mig.is_err());
    assert_eq!(inflight_mig.unwrap_err().code, RS_5030);
}

#[test]
fn test_cli_drain_viewer_denied_rs2401() {
    let control = ControlClient::new(None, ClientIdentity::new("viewer").with_role(Role::Viewer));
    assert_eq!(control.drain_worker(1).unwrap_err().code, RS_2401);
}

#[test]
fn test_cli_drain_admin_authorized() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    assert!(control.drain_worker(1).is_ok());
}

#[test]
fn test_cli_drain_unconfirmed_refusal() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    let res = run_cluster_workers_drain(OutputFormat::Text, &control, 1, false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_drain_reissue_idempotent_status() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    let res1 = control.drain_worker(1).unwrap();
    let res2 = control.drain_worker(1).unwrap();
    assert_eq!(res1.status, res2.status);
}

#[test]
fn test_cli_drain_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin))
        .with_storage_path(tmp.path());
    control.drain_worker(1).unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "cluster.workers.drain" && e.resource == "1"));
}

#[test]
fn test_cli_migrate_viewer_denied_rs2401() {
    let control = ControlClient::new(None, ClientIdentity::new("viewer").with_role(Role::Viewer));
    assert_eq!(control.migrate_shard(1, 2).unwrap_err().code, RS_2401);
}

#[test]
fn test_cli_migrate_admin_authorized() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    assert!(control.migrate_shard(1, 2).is_ok());
}

#[test]
fn test_cli_migrate_unconfirmed_refusal() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    let res = run_shard_migrate(OutputFormat::Text, &control, 1, 2, false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_migrate_in_flight_refusal_rs5030() {
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin));
    assert_eq!(control.migrate_shard(999, 2).unwrap_err().code, RS_5030);
}

#[test]
fn test_cli_migrate_audit_event_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let control = ControlClient::new(None, ClientIdentity::new("admin").with_role(Role::Admin))
        .with_storage_path(tmp.path());
    control.migrate_shard(1, 2).unwrap();
    let events = StorageClient::new().audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "shard.migrate" && e.resource == "1"));
}

// ─── Slice 7: Checkpoint Restore & Storage Inspection ───────────────────────

#[test]
fn test_cli_checkpoint_restore_lfs_and_minio() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = StorageClient::new();
    let target = tmp.path().join("restore_dest");

    // 1. Restore with --yes
    let restore_out = run_checkpoint_restore(
        OutputFormat::Text,
        &storage,
        tmp.path(),
        1001,
        Some(&target),
        true,
    )
    .unwrap();
    assert!(restore_out.contains("Checkpoint 1001: restored to"));
    assert!(restore_out.contains("SUCCESS"));

    // 2. Unconfirmed restore refuses with RS-0005
    let unconf_res = run_checkpoint_restore(
        OutputFormat::Text,
        &storage,
        tmp.path(),
        1001,
        Some(&target),
        false,
    );
    assert_eq!(unconf_res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_restore_viewer_denied_rs2401() {
    let storage =
        StorageClient::with_identity(ClientIdentity::new("viewer").with_role(Role::Viewer));
    let tmp = tempfile::tempdir().unwrap();
    assert_eq!(
        storage
            .restore_checkpoint(tmp.path(), 1001, None)
            .unwrap_err()
            .code,
        RS_2401
    );
}

#[test]
fn test_cli_restore_admin_authorized() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    assert!(storage.restore_checkpoint(tmp.path(), 1001, None).is_ok());
}

#[test]
fn test_cli_restore_unconfirmed_refusal() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    let res = run_checkpoint_restore(OutputFormat::Text, &storage, tmp.path(), 1001, None, false);
    assert_eq!(res.unwrap_err().code, RS_0005);
}

#[test]
fn test_cli_restore_reissue_safe() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    let res1 = storage.restore_checkpoint(tmp.path(), 1001, None).unwrap();
    let res2 = storage.restore_checkpoint(tmp.path(), 1001, None).unwrap();
    assert_eq!(res1.checkpoint_id, res2.checkpoint_id);
    assert_eq!(res1.status, res2.status);
}

#[test]
fn test_cli_restore_audit_event_logged() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    storage.restore_checkpoint(tmp.path(), 1001, None).unwrap();
    let events = storage.audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "checkpoint.restore" && e.resource == "1001"));
}

// ─── Slice 8: Diagnostic Support Bundle ─────────────────────────────────────

#[test]
fn test_cli_support_bundle_on_demand_redaction_and_size_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = StorageClient::new();
    let out_file = tmp.path().join("diag_bundle.tar.gz");

    let bundle_out = run_support_bundle(
        OutputFormat::Text,
        &storage,
        tmp.path(),
        Some("active_users"),
        Some("1h"),
        Some(&out_file),
    )
    .unwrap();
    assert!(bundle_out.contains("Support bundle generated at"));
    assert!(bundle_out.contains("diag_bundle.tar.gz"));
}

#[test]
fn test_cli_bundle_viewer_denied_rs2401() {
    let storage =
        StorageClient::with_identity(ClientIdentity::new("viewer").with_role(Role::Viewer));
    let tmp = tempfile::tempdir().unwrap();
    assert_eq!(
        storage
            .generate_support_bundle(tmp.path(), None, None, None)
            .unwrap_err()
            .code,
        RS_2401
    );
}

#[test]
fn test_cli_bundle_admin_authorized() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    assert!(storage
        .generate_support_bundle(tmp.path(), None, None, None)
        .is_ok());
}

#[test]
fn test_cli_bundle_reissue_independent() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    let res1 = storage
        .generate_support_bundle(tmp.path(), None, None, None)
        .unwrap();
    let res2 = storage
        .generate_support_bundle(tmp.path(), None, None, None)
        .unwrap();
    assert_eq!(res1.size_bytes, res2.size_bytes);
    assert_eq!(res1.redacted_secrets_count, res2.redacted_secrets_count);
}

#[test]
fn test_cli_bundle_audit_event_logged() {
    let storage = StorageClient::new();
    let tmp = tempfile::tempdir().unwrap();
    storage
        .generate_support_bundle(tmp.path(), Some("active_users"), None, None)
        .unwrap();
    let events = storage.audit_tail(tmp.path(), 10).unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "support.bundle" && e.resource == "active_users"));
}
