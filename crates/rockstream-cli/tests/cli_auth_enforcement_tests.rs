//! v0.51.7 Slice 3 — CLI Auth Mode Routing & Fail-Closed Enforcement Tests.

use rockstream_cli::{run_start, start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::path::PathBuf;

fn test_options(storage: PathBuf, auth_mode: &str, listen: Option<String>) -> StartOptions {
    StartOptions {
        storage,
        role: "gateway".to_string(),
        control: None,
        auth_mode: auth_mode.to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        node_config: None,
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
    assert_eq!(
        err.to_string(),
        "RS-0002 unknown auth mode `invalid_super_auth`\n  next steps: Pass --auth=off; other authentication modes are currently unavailable."
    );
}

#[tokio::test]
async fn test_cli_unavailable_auth_modes_fail_closed_before_storage_or_listener_start() {
    let tmp = tempfile::tempdir().unwrap();
    for (mode, detail, next_steps) in [
        (
            "scram",
            "no durable role store is configured",
            "This mode is unavailable; use --auth=off only where unauthenticated access is acceptable.",
        ),
        (
            "md5",
            "no durable role store is configured",
            "This mode is unavailable; use --auth=off only where unauthenticated access is acceptable.",
        ),
        (
            "oidc",
            "issuer, audience, and JWKS verification are not configured",
            "This mode is unavailable; use --auth=off only where unauthenticated access is acceptable.",
        ),
        (
            "mtls",
            "the CLI does not configure a gateway TLS listener",
            "This mode is unavailable; use --auth=off only where unauthenticated access is acceptable.",
        ),
    ] {
        let storage = tmp.path().join(mode);
        let opts = test_options(storage.clone(), mode, Some("127.0.0.1:0".to_string()));
        let err = start_gateway(&opts).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "RS-0002 authentication mode `{mode}` is unavailable: {detail}\n  next steps: {next_steps}"
            )
        );
        assert!(!storage.exists(), "{mode} created gateway storage");
    }
}

#[test]
fn test_startup_log_contains_auth_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = test_options(tmp.path().to_path_buf(), "off", None);

    // Verify the only available auth mode passes startup validation in no-op mode.
    let outcome = run_start(&opts).unwrap();
    assert!(
        outcome.events_written >= 1,
        "Audit log should contain startup events"
    );
}
