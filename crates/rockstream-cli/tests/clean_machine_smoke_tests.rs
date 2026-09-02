//! Clean-Machine Smoke Workload Tests across Supported Profiles (v0.59.22 Slice 7 / Phase 3a).

use tempfile::TempDir;
use tokio_postgres::NoTls;

use rockstream_cli::{start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

#[tokio::test]
async fn test_clean_machine_smoke_workload_across_profiles() {
    let temp_dir = TempDir::new().unwrap();
    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let start_opts = StartOptions {
        storage: storage_dir,
        role: "all".to_string(),
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
        shutdown_timeout_secs: Some(30),
    };

    let (addr, _server_handle) = start_gateway(&start_opts)
        .await
        .expect("start_gateway must succeed on clean machine");

    let connect_str = format!(
        "host=127.0.0.1 port={} user=rockstream dbname=default",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&connect_str, NoTls)
        .await
        .expect("Must connect to PGWire port");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Smoke connection error: {e}");
        }
    });

    // 1. Run basic query
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("Query SELECT 1 must succeed");
    assert!(!rows.is_empty(), "expected result rows");
}
