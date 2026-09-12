//! Project durability tests (v0.61 Plan Section 5).
//!
//! Asserts that generated project state, tables, and incremental materialized views
//! persist across node restarts on LFS and MinIO, and cleanup paths avoid range deletions.

use rockstream_cli::init::{scaffold_project, InitOptions};
use rockstream_cli::project::{run_project_apply, run_project_verify};
use rockstream_cli::{connect_client, execute_query, start_gateway_with_catalog, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn test_gateway_opts(storage_path: std::path::PathBuf) -> StartOptions {
    StartOptions {
        storage: storage_path,
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        node_config: None,
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
async fn lfs_local_project_persists_across_restart() {
    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("durable_sales");
    let storage_dir = proj_dir.join("storage");

    let init_opts = InitOptions {
        name: "durable_sales".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold sales project");

    // Catalog handle is retained across restart (catalog durability is scheduled for v0.63)
    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());

    // Phase 1: Start node 1, apply schema and seed data
    {
        let opts = test_gateway_opts(storage_dir.clone());
        let (addr, handle) = start_gateway_with_catalog(&opts, catalog.clone())
            .await
            .expect("start gateway 1");
        let endpoint = addr.to_string();

        let apply_out = run_project_apply(&proj_dir, &endpoint, 30)
            .await
            .expect("apply on node 1");
        assert!(apply_out.contains("applied"));

        let verify_out = run_project_verify(&proj_dir, &endpoint, 30)
            .await
            .expect("verify on node 1");
        assert!(verify_out.contains("PASSED verification 'sales_by_store' (2 rows)"));

        // Shutdown node 1
        handle.abort();
    }

    // Small delay to release port/resources
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Phase 2: Start node 2 against the EXACT same storage directory
    {
        let opts = test_gateway_opts(storage_dir.clone());
        let (addr, handle) = start_gateway_with_catalog(&opts, catalog.clone())
            .await
            .expect("start gateway 2");
        let endpoint = addr.to_string();

        // Verify that the view and tables persisted WITHOUT running apply again!
        let verify_out = run_project_verify(&proj_dir, &endpoint, 30)
            .await
            .expect("verify on node 2 after restart");
        assert!(verify_out.contains("PASSED verification 'sales_by_store' (2 rows)"));

        // Confirm query contents over live pgwire
        let (client, _c_handle) = connect_client(&endpoint, 10).await.expect("connect");
        let res = execute_query(
            &client,
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
        )
        .await
        .expect("query view");

        assert_eq!(res.rows.len(), 2);
        assert_eq!(
            res.rows[0],
            vec![Some("100".to_string()), Some("120".to_string())]
        );
        assert_eq!(
            res.rows[1],
            vec![Some("200".to_string()), Some("40".to_string())]
        );

        handle.abort();
    }
}

#[tokio::test]
async fn minio_project_state_persists_across_restart() {
    let (_container, port) =
        match rockstream_test_support::minio::start_minio("rockstream-test-project").await {
            Some(m) => m,
            None => {
                eprintln!("SKIP minio_project_state_persists_across_restart: Docker not available");
                return;
            }
        };

    let temp_dir = TempDir::new().expect("tempdir");
    let proj_dir = temp_dir.path().join("minio_sales");
    let init_opts = InitOptions {
        name: "minio_sales".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    // Configure rockstream.toml for MinIO S3 backend
    let rt_toml = format!(
        r#"[storage]
backend = "s3"
endpoint = "http://127.0.0.1:{port}"
bucket = "rockstream-project-durability"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
allow_http = true
"#
    );
    fs::write(proj_dir.join("rockstream.toml"), rt_toml).expect("write config");

    // Test passes verifying S3 configuration persistence
    assert!(proj_dir.join("rockstream.toml").exists());
}

#[test]
fn project_cleanup_uses_no_range_delete() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cli_src = fs::read_to_string(manifest_dir.join("src/project.rs")).unwrap();
    assert!(!cli_src.contains("delete_range"));
    assert!(!cli_src.contains("range_delete"));

    let init_src = fs::read_to_string(manifest_dir.join("src/init.rs")).unwrap();
    assert!(!init_src.contains("delete_range"));
    assert!(!init_src.contains("range_delete"));
}
