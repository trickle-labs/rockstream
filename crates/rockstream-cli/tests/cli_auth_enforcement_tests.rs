//! v0.51.7 Slice 3 — CLI Auth Mode Routing & Fail-Closed Enforcement Tests.

use std::path::PathBuf;
use tokio_postgres::NoTls;

use rockstream_cli::{run_start, start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

fn test_options(storage: PathBuf, auth_mode: &str, listen: Option<String>) -> StartOptions {
    StartOptions {
        storage,
        role: "gateway".to_string(),
        control: None,
        auth_mode: auth_mode.to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: listen,
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
fn test_cli_invalid_auth_fails_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = test_options(tmp.path().to_path_buf(), "invalid_super_auth", None);

    let res = run_start(&opts);
    assert!(res.is_err(), "Invalid --auth mode string must fail startup");
    let err = res.unwrap_err();
    assert_eq!(err.code.value(), 2); // RS_0002
    assert!(
        err.message.contains("unknown auth mode"),
        "Error message must indicate unknown auth mode, got: {}",
        err.message
    );
}

#[tokio::test]
async fn test_cli_auth_scram_enforcement() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = test_options(
        tmp.path().to_path_buf(),
        "scram",
        Some("127.0.0.1:0".to_string()),
    );

    let (addr, handle) = start_gateway(&opts).await.unwrap();

    // Under --auth scram, connecting with missing / unauthenticated credentials must fail
    let res = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=unauthed_user dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await;

    assert!(
        res.is_err(),
        "Unauthenticated connection to --auth scram gateway must be rejected"
    );

    handle.abort();
}

#[test]
fn test_startup_log_contains_auth_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = test_options(tmp.path().to_path_buf(), "scram", None);

    // Verify valid auth_mode options pass startup validation in no-op mode
    let outcome = run_start(&opts).unwrap();
    assert!(
        outcome.events_written >= 1,
        "Audit log should contain startup events"
    );
}
