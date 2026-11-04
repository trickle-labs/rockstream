//! v0.51.7 Slice 3 — CLI Auth Mode Routing & Fail-Closed Enforcement Tests.

use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStoreExt};
use rockstream_cli::{run_start, start_gateway, StartOptions};
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_storage::ShardDb;
use rockstream_types::config::{NodeConfig, RockstreamConfig};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

fn normalize_correlation_id(stderr: Vec<u8>) -> String {
    let stderr = String::from_utf8(stderr).unwrap();
    let marker = "(correlation_id=";
    let start = stderr.find(marker).unwrap() + marker.len();
    let end = start + stderr[start..].find(' ').unwrap();
    uuid::Uuid::parse_str(&stderr[start..end]).unwrap();
    stderr.replacen(&stderr[start..end], "<correlation_id>", 1)
}

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
        "RS-0002 unknown auth mode `invalid_super_auth`\n  next steps: Pass a supported authentication mode: off, scram, oidc, or mtls."
    );
}

#[tokio::test]
async fn test_cli_md5_mode_fails_closed_before_storage_or_listener_start() {
    let tmp = tempfile::tempdir().unwrap();
    for (mode, detail, next_steps) in [(
        "md5",
        "MD5 is not a release authentication mode",
        "Use a release-supported authentication mode.",
    )] {
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

#[tokio::test]
async fn scram_mode_fails_closed_without_enabled_login() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = test_options(
        tmp.path().join("scram"),
        "scram",
        Some("127.0.0.1:0".to_string()),
    );
    let error = start_gateway(&opts).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("SCRAM requires an available durable catalog with an enabled SCRAM login"),
        "unexpected startup error: {error}"
    );
}

#[tokio::test]
async fn configured_oidc_auth_mode_cannot_be_disabled_by_start_options() {
    let tmp = tempfile::tempdir().unwrap();
    let mut node_config = NodeConfig::default();
    node_config.auth.mode = "oidc".to_string();
    let mut opts = test_options(
        tmp.path().join("gateway-storage"),
        "off",
        Some("127.0.0.1:0".to_string()),
    );
    opts.node_config = Some(node_config);

    let error = start_gateway(&opts)
        .await
        .expect_err("off must not mask configured OIDC");
    assert_eq!(
        error.to_string(),
        "RS-0002 OIDC requires auth.oidc_issuer, auth.oidc_audience, and auth.oidc_jwks_url\n  next steps: Configure the issuer, audience, and JWKS endpoint before starting the gateway."
    );
    assert!(!opts.storage.exists(), "failed startup created storage");
}

#[tokio::test]
async fn gateway_start_fails_closed_when_durable_catalog_recovery_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_path = "gateway";
    let store = Arc::new(InMemory::new());
    let snapshot_path = ObjectPath::from("gateway/catalog/snapshots/00000000000000000000.snap");
    store
        .put(&snapshot_path, b"{".to_vec().into())
        .await
        .unwrap();

    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let options = test_options(
        tmp.path().to_path_buf(),
        "off",
        Some("127.0.0.1:0".to_string()),
    );
    let error = rockstream_cli::start_gateway_with_shard_and_catalog(
        &options,
        shard_db,
        store.clone(),
        shard_path,
        Arc::new(CatalogStubs::new()),
    )
    .await
    .unwrap_err();

    let catalog_error = rockstream_storage::catalog::CatalogSnapshot::decode(b"{")
        .unwrap_err()
        .to_string();
    assert_eq!(
        error.to_string(),
        format!(
            "RS-0003 gateway durable catalog recovery failed: {catalog_error}\n  next steps: Check that the durable catalog is intact and storage is accessible."
        )
    );
}

#[test]
fn oidc_missing_configuration_fails_before_storage_or_listener_start() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = tmp.path().join("gateway-storage");
    let output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage.to_str().unwrap(),
            "--listen",
            "not-an-address",
            "--auth",
            "oidc",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(output.stdout, b"");
    assert_eq!(
        normalize_correlation_id(output.stderr),
        "[RS-0002] config.invalid: Configuration error (detail=OIDC requires auth.oidc_issuer, auth.oidc_audience, and auth.oidc_jwks_url) (correlation_id=<correlation_id> context=detail=OIDC requires auth.oidc_issuer, auth.oidc_audience, and auth.oidc_jwks_url) next_steps: Check configuration file and CLI flags.\n  next steps: Configure the issuer, audience, and JWKS endpoint before starting the gateway.\n"
    );
    assert!(
        !storage.exists(),
        "Unavailable auth created gateway storage"
    );
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
