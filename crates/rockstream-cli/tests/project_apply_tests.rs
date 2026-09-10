//! Project apply and idempotency integration tests (Slice 4).

use rockstream_cli::init::{scaffold_project, InitOptions};
use rockstream_cli::project::{load_manifest, run_project_apply, AppliedState};
use rockstream_cli::{connect_client, execute_query, start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_0002, RS_0004, RS_2001};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::fs;
use tempfile::TempDir;

fn test_gateway_opts(dir: &TempDir) -> StartOptions {
    StartOptions {
        storage: dir.path().to_path_buf(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some("127.0.0.1:0".to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
        shutdown_timeout_secs: None,
    }
}

#[test]
fn test_manifest_validation_missing_schema_file_fails_with_rs0004() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("missing_schema_proj");

    let opts = InitOptions {
        name: "missing_schema_proj".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&opts).expect("scaffold");

    // Delete schema.sql
    fs::remove_file(proj_dir.join("schema.sql")).expect("remove schema.sql");

    let err = load_manifest(&proj_dir).expect_err("should fail for missing schema file");
    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("apply schema file"));
    assert!(err.message.contains("does not exist"));
}

#[test]
fn test_manifest_validation_unsupported_version_fails_with_rs0002() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("version_proj");

    let opts = InitOptions {
        name: "version_proj".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&opts).expect("scaffold");

    // Rewrite project.toml with version = 99
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("version = 1", "version = 99");
    fs::write(&manifest_path, modified).expect("write");

    let err = load_manifest(&proj_dir).expect_err("should fail for unsupported version");
    assert_eq!(err.code, RS_0002);
    assert!(err
        .message
        .contains("unsupported project manifest version '99'"));
}

#[test]
fn test_manifest_validation_unsupported_seed_format_fails_with_rs0002() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("format_proj");

    let opts = InitOptions {
        name: "format_proj".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&opts).expect("scaffold");

    // Rewrite project.toml with format = "json"
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("format = \"csv\"", "format = \"json\"");
    fs::write(&manifest_path, modified).expect("write");

    let err = load_manifest(&proj_dir).expect_err("should fail for unsupported format");
    assert_eq!(err.code, RS_0002);
    assert!(err.message.contains("unsupported seed format 'json'"));
}

#[tokio::test]
async fn test_apply_unreachable_node_fails_with_rs0004() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("unreachable_proj");

    let opts = InitOptions {
        name: "unreachable_proj".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&opts).expect("scaffold");

    let err = run_project_apply(&proj_dir, "127.0.0.1:59997", 2)
        .await
        .expect_err("should fail for unreachable endpoint");

    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("cannot reach RockStream gateway"));
}

#[tokio::test]
async fn test_clean_apply_and_repeat_apply_idempotency() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales");
    let init_opts = InitOptions {
        name: "sales".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold sales project");

    // 1. Initial clean apply
    let out1 = run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("clean apply should succeed");

    assert!(out1.contains("schema 'schema.sql' (applied)"));
    assert!(out1.contains("seed table 'orders' from 'data/seed.csv' (ingested)"));

    // Verify metadata saved
    let state = AppliedState::load(&proj_dir);
    assert_eq!(state.project_name, "sales");
    assert_eq!(state.steps.len(), 2);
    assert_eq!(state.steps[0].step_type, "apply");
    assert_eq!(state.steps[0].identifier, "schema.sql");
    assert_eq!(state.steps[1].step_type, "seed");
    assert_eq!(state.steps[1].identifier, "orders:data/seed.csv");

    // Verify view contents over pgwire
    let (client, _c_handle) = connect_client(&endpoint, 30).await.expect("connect");
    let view_res1 = execute_query(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await
    .expect("query view");

    assert_eq!(view_res1.rows.len(), 2);
    assert_eq!(
        view_res1.rows[0],
        vec![Some("100".to_string()), Some("120".to_string())]
    );
    assert_eq!(
        view_res1.rows[1],
        vec![Some("200".to_string()), Some("40".to_string())]
    );

    // 2. Repeat apply — must skip already applied steps and NOT duplicate rows
    let out2 = run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("repeat apply should succeed");

    assert!(out2.contains("schema 'schema.sql' (already applied, skipped)"));
    assert!(out2.contains("seed table 'orders' from 'data/seed.csv' (already applied, skipped)"));

    // Verify view contents remain exactly the same!
    let view_res2 = execute_query(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await
    .expect("query view after repeat apply");

    assert_eq!(view_res2.rows.len(), 2);
    assert_eq!(
        view_res2.rows[0],
        vec![Some("100".to_string()), Some("120".to_string())]
    );
    assert_eq!(
        view_res2.rows[1],
        vec![Some("200".to_string()), Some("40".to_string())]
    );
}

#[tokio::test]
async fn test_mid_apply_failure_stops_and_records_partial_metadata() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("partial_sales");
    let init_opts = InitOptions {
        name: "partial_sales".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    // Corrupt the seed data with an invalid row (wrong number of columns)
    let bad_seed = "id,store_id,amount\n1,100\n";
    fs::write(proj_dir.join("data/seed.csv"), bad_seed).expect("write corrupt seed");

    // Apply should execute schema.sql successfully, then fail on seed ingestion
    let err = run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect_err("should fail on invalid seed row");

    assert_eq!(err.code, RS_2001);
    assert!(err.message.contains("CSV row mismatch in seed data"));

    // Metadata should have recorded schema.sql, but NOT seed!
    let state = AppliedState::load(&proj_dir);
    assert_eq!(state.steps.len(), 1);
    assert_eq!(state.steps[0].step_type, "apply");
    assert_eq!(state.steps[0].identifier, "schema.sql");

    // Now fix the seed file
    let fixed_seed = "id,store_id,amount\n1,100,50\n2,100,70\n3,200,40\n";
    fs::write(proj_dir.join("data/seed.csv"), fixed_seed).expect("write fixed seed");

    // Resumed apply: should skip schema.sql and complete seed ingestion!
    let out_resumed = run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("resumed apply should succeed");

    assert!(out_resumed.contains("schema 'schema.sql' (already applied, skipped)"));
    assert!(out_resumed.contains("seed table 'orders' from 'data/seed.csv' (ingested)"));

    // Metadata now has both!
    let state_final = AppliedState::load(&proj_dir);
    assert_eq!(state_final.steps.len(), 2);
    assert_eq!(state_final.steps[1].step_type, "seed");

    // Verify view totals are correct
    let (client, _c_handle) = connect_client(&endpoint, 30).await.expect("connect");
    let view_res = execute_query(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await
    .expect("query view");

    assert_eq!(view_res.rows.len(), 2);
    assert_eq!(
        view_res.rows[0],
        vec![Some("100".to_string()), Some("120".to_string())]
    );
    assert_eq!(
        view_res.rows[1],
        vec![Some("200".to_string()), Some("40".to_string())]
    );
}
