//! v0.59.4 Slice 3 — Config Resolver & Origin Precedence Tests (CFG-02)

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use tempfile::tempdir;

use rockstream_types::config_resolver::{CliConfigOverrides, ConfigOrigin, ConfigResolver};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_precedence_defaults_only() {
    let _guard = ENV_LOCK.lock().unwrap();
    let overrides = CliConfigOverrides::default();
    let resolved = ConfigResolver::resolve(None, &overrides).expect("Resolution succeeds");
    assert_eq!(resolved.config.cluster.min_epoch_ms, 10);
    assert_eq!(resolved.config.worker.execution_threads, 1);
    assert_eq!(
        resolved.origins.get("cluster.min_epoch_ms"),
        Some(&ConfigOrigin::Default)
    );
    assert_eq!(
        resolved.origins.get("worker.execution_threads"),
        Some(&ConfigOrigin::Default)
    );
}

#[test]
fn worker_execution_threads_uses_single_underscore_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("ROCKSTREAM_WORKER_EXECUTION_THREADS", "4");
    let resolved = ConfigResolver::resolve(None, &CliConfigOverrides::default()).unwrap();
    std::env::remove_var("ROCKSTREAM_WORKER_EXECUTION_THREADS");

    assert_eq!(resolved.config.worker.execution_threads, 4);
    assert_eq!(
        resolved.origins.get("worker.execution_threads"),
        Some(&ConfigOrigin::Environment(
            "ROCKSTREAM_WORKER_EXECUTION_THREADS".to_string()
        ))
    );
}

#[test]
fn test_precedence_file_over_default() {
    let _guard = ENV_LOCK.lock().unwrap();
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"
[cluster]
min_epoch_ms = 25
"#
    )
    .unwrap();

    let overrides = CliConfigOverrides::default();
    let resolved =
        ConfigResolver::resolve(Some(&config_file), &overrides).expect("Resolution succeeds");
    assert_eq!(resolved.config.cluster.min_epoch_ms, 25);
    assert_eq!(
        resolved.origins.get("cluster.min_epoch_ms"),
        Some(&ConfigOrigin::File(config_file))
    );
}

#[test]
fn test_precedence_env_over_file() {
    let _guard = ENV_LOCK.lock().unwrap();
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"
[cluster]
min_epoch_ms = 25
"#
    )
    .unwrap();

    std::env::set_var("ROCKSTREAM__CLUSTER__MIN_EPOCH_MS", "50");

    let overrides = CliConfigOverrides::default();
    let resolved =
        ConfigResolver::resolve(Some(&config_file), &overrides).expect("Resolution succeeds");

    std::env::remove_var("ROCKSTREAM__CLUSTER__MIN_EPOCH_MS");

    assert_eq!(resolved.config.cluster.min_epoch_ms, 50);
    assert_eq!(
        resolved.origins.get("cluster.min_epoch_ms"),
        Some(&ConfigOrigin::Environment(
            "ROCKSTREAM__CLUSTER__MIN_EPOCH_MS".to_string()
        ))
    );
}

#[test]
fn test_precedence_cli_over_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("ROCKSTREAM__EXCHANGE__EXCHANGE_SPILL_THRESHOLD_MB", "256");

    let overrides = CliConfigOverrides {
        exchange_spill_threshold_mb: Some(512),
        ..Default::default()
    };
    let resolved = ConfigResolver::resolve(None, &overrides).expect("Resolution succeeds");

    std::env::remove_var("ROCKSTREAM__EXCHANGE__EXCHANGE_SPILL_THRESHOLD_MB");

    assert_eq!(resolved.config.exchange.exchange_spill_threshold_mb, 512);
    assert_eq!(
        resolved.origins.get("exchange.exchange_spill_threshold_mb"),
        Some(&ConfigOrigin::Cli(
            "--exchange-spill-threshold-mb".to_string()
        ))
    );
}

#[test]
fn test_config_path_resolution_order() {
    let _guard = ENV_LOCK.lock().unwrap();
    let tmp = tempdir().unwrap();
    let custom_file = tmp.path().join("custom.toml");
    let mut f = File::create(&custom_file).unwrap();
    writeln!(
        f,
        r#"
[cluster]
min_epoch_ms = 42
"#
    )
    .unwrap();

    let overrides = CliConfigOverrides::default();
    let resolved =
        ConfigResolver::resolve(Some(&custom_file), &overrides).expect("Resolution succeeds");
    assert_eq!(resolved.config.cluster.min_epoch_ms, 42);
}

#[test]
fn test_secret_redaction_in_effective_config() {
    let _guard = ENV_LOCK.lock().unwrap();
    let overrides = CliConfigOverrides {
        tls_key_path: Some(PathBuf::from("/etc/rockstream/private_key.pem")),
        ..Default::default()
    };

    let resolved = ConfigResolver::resolve(None, &overrides).expect("Resolution succeeds");
    let redacted = resolved.redacted_config();
    assert_eq!(
        redacted.gateway.tls_key_path,
        Some(PathBuf::from("[REDACTED]"))
    );
}
