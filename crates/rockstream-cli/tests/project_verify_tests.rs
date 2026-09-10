//! Project verify structured comparison engine tests (Slice 5).

use rockstream_cli::init::{scaffold_project, InitOptions};
use rockstream_cli::project::{run_project_apply, run_project_verify};
use rockstream_cli::{start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_0004, RS_2005};
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

#[tokio::test]
async fn verify_happy_path_exact_match() {
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

    // Apply first
    run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply should succeed");

    // Verify
    let result = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect("verify should succeed");

    assert!(result.contains("PASSED verification 'sales_by_store' (2 rows)"));
}

#[tokio::test]
async fn verify_fails_on_wrong_value() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales_wrong_val");
    let init_opts = InitOptions {
        name: "sales_wrong_val".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply");

    // Modify expected value to 100|999
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("100|120", "100|999");
    fs::write(&manifest_path, modified).expect("write");

    let err = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect_err("verify must fail on wrong value");

    assert!(err.code == RS_2005 || err.code == rockstream_types::error_code::RS_1004);
    assert!(err.message.contains("FAILED verification 'sales_by_store'"));
    assert!(err
        .message
        .contains("row 1 mismatch: expected 100|999, got 100|120"));
    assert!(err.message.contains("Expected:"));
    assert!(err.message.contains("Actual:"));
}

#[tokio::test]
async fn verify_fails_on_missing_row() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales_missing_row");
    let init_opts = InitOptions {
        name: "sales_missing_row".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply");

    // Add extra expected row in project.toml
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("200|40", "200|40\n300|10");
    fs::write(&manifest_path, modified).expect("write");

    let err = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect_err("verify must fail on missing row");

    assert!(err.code == RS_2005 || err.code == rockstream_types::error_code::RS_1004);
    assert!(err.message.contains("expected 3 rows, got 2"));
}

#[tokio::test]
async fn verify_fails_on_extra_row() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales_extra_row");
    let init_opts = InitOptions {
        name: "sales_extra_row".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply");

    // Remove expected row 200|40 so actual has an extra row
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("\n200|40", "");
    fs::write(&manifest_path, modified).expect("write");

    let err = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect_err("verify must fail on extra row");

    assert!(err.code == RS_2005 || err.code == rockstream_types::error_code::RS_1004);
    assert!(err.message.contains("expected 1 rows, got 2"));
}

#[tokio::test]
async fn verify_fails_on_duplicate_row() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales_dup");
    let init_opts = InitOptions {
        name: "sales_dup".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply");

    // Change verify query to return duplicated rows from orders table
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content
        .replace(
            "SELECT store_id, total_amount\nFROM sales_by_store\nORDER BY store_id;",
            "SELECT store_id, 100 FROM orders WHERE store_id = 100 ORDER BY store_id;",
        )
        .replace("100|120\n200|40", "100|100");
    fs::write(&manifest_path, modified).expect("write");

    let err = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect_err("verify must fail on duplicate rows");

    assert!(err.code == RS_2005 || err.code == rockstream_types::error_code::RS_1004);
    assert!(err.message.contains("duplicate row"));
}

#[tokio::test]
async fn verify_fails_on_missing_view() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("sales_missing_view");
    let init_opts = InitOptions {
        name: "sales_missing_view".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    // Do NOT run apply, so view does not exist
    let err = run_project_verify(&proj_dir, &endpoint, 30)
        .await
        .expect_err("verify must fail when view does not exist");

    assert!(err.code == RS_2005 || err.code == rockstream_types::error_code::RS_1004);
}

#[tokio::test]
async fn verify_fails_nonzero_on_unreachable_node() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("sales_unreach");
    let init_opts = InitOptions {
        name: "sales_unreach".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    let err = run_project_verify(&proj_dir, "127.0.0.1:59998", 2)
        .await
        .expect_err("verify must fail when gateway is unreachable");

    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("cannot reach RockStream gateway"));
}

#[tokio::test]
async fn verify_fails_on_missing_query_file() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("sales_missing_qf");
    let init_opts = InitOptions {
        name: "sales_missing_qf".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    // Reference nonexistent file in [[verify]]
    let manifest_path = proj_dir.join("project.toml");
    let content = fs::read_to_string(&manifest_path).expect("read");
    let modified = content.replace("query = \"\"\"\nSELECT store_id, total_amount\nFROM sales_by_store\nORDER BY store_id;\n\"\"\"", "file = \"queries/nonexistent.sql\"");
    fs::write(&manifest_path, modified).expect("write");

    let err = run_project_verify(&proj_dir, "127.0.0.1:5432", 2)
        .await
        .expect_err("verify must fail for nonexistent query file");

    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("verification query file"));
}
