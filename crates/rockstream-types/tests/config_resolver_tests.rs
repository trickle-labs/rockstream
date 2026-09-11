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

#[test]
fn test_node_config_schema_defaults() {
    use rockstream_types::config::{GatewayTlsConfig, NodeConfig};

    let node_cfg = NodeConfig::default();
    assert_eq!(node_cfg.version, 1);
    assert_eq!(node_cfg.node.role, "all");
    assert_eq!(node_cfg.node.host_id, None);
    assert_eq!(node_cfg.node.availability_zone, None);

    assert_eq!(node_cfg.gateway.listen_addr, "127.0.0.1:5432");
    assert_eq!(node_cfg.gateway.max_connections, 1024);
    assert_eq!(node_cfg.gateway.query_timeout_secs, 60);
    assert_eq!(node_cfg.gateway.tls, GatewayTlsConfig::default());

    assert_eq!(
        node_cfg.control.listen_addr,
        Some("127.0.0.1:9200".to_string())
    );
    assert_eq!(node_cfg.control.url, None);
    assert_eq!(node_cfg.control.shared_storage, None);
    assert_eq!(node_cfg.control.raft, None);

    assert_eq!(node_cfg.worker.worker_id, None);
    assert_eq!(node_cfg.worker.execution_threads, 1);
    assert_eq!(node_cfg.worker.segment_cache_bytes, 536870912);
    assert_eq!(node_cfg.worker.max_rows_per_quantum, 1000);
    assert!(node_cfg.worker.capabilities.is_empty());

    assert_eq!(node_cfg.storage.url.to_string(), "file://./data");
    assert_eq!(node_cfg.storage.temp_dir, None);
    assert_eq!(node_cfg.storage.spill_dir, None);

    assert_eq!(node_cfg.metrics.listen_addr, "127.0.0.1:9090");
    assert!(node_cfg.metrics.enabled);
    assert_eq!(node_cfg.metrics.scrape_interval_secs, 15);

    assert_eq!(node_cfg.auth.mode, "off");
    assert_eq!(node_cfg.auth.secret_path, None);
    assert_eq!(node_cfg.auth.admin_user, None);

    assert_eq!(node_cfg.logging.level, "info");
    assert_eq!(node_cfg.logging.format, "text");

    assert_eq!(node_cfg.runtime.shutdown_timeout_secs, 30);
    assert_eq!(node_cfg.runtime.min_epoch_ms, 10);
    assert_eq!(node_cfg.runtime.checkpoint_retention_count, 128);
    assert_eq!(node_cfg.runtime.state_budget_gb, 10);
}

#[test]
fn test_legacy_config_backward_compatibility_mapping() {
    use rockstream_types::config::{NodeConfig, RockstreamConfig};

    let legacy_toml = r#"
[cluster]
min_epoch_ms = 45
shutdown_timeout_secs = 60
checkpoint_retention_count = 256
state_budget_gb = 20

[worker]
execution_threads = 8
segment_cache_bytes = 1073741824
max_rows_per_quantum = 2000
"#;

    let node_cfg =
        NodeConfig::load_from_str(legacy_toml).expect("legacy config parsed into NodeConfig");
    assert_eq!(node_cfg.runtime.min_epoch_ms, 45);
    assert_eq!(node_cfg.runtime.shutdown_timeout_secs, 60);
    assert_eq!(node_cfg.runtime.checkpoint_retention_count, 256);
    assert_eq!(node_cfg.runtime.state_budget_gb, 20);
    assert_eq!(node_cfg.worker.execution_threads, 8);
    assert_eq!(node_cfg.worker.segment_cache_bytes, 1073741824);
    assert_eq!(node_cfg.worker.max_rows_per_quantum, 2000);

    // Test From conversions both ways
    let legacy_parsed: RockstreamConfig = toml::from_str(legacy_toml).expect("parse legacy");
    let converted_node = NodeConfig::from(&legacy_parsed);
    assert_eq!(converted_node.runtime.min_epoch_ms, 45);
    assert_eq!(converted_node.worker.execution_threads, 8);

    let converted_back = RockstreamConfig::from(&converted_node);
    assert_eq!(converted_back.cluster.min_epoch_ms, 45);
    assert_eq!(converted_back.worker.execution_threads, 8);
}

#[test]
fn test_four_tier_precedence_resolution() {
    let _guard = ENV_LOCK.lock().unwrap();

    // Tier 1: Defaults only
    let empty_overrides = CliConfigOverrides::default();
    let resolved_default =
        ConfigResolver::resolve(None, &empty_overrides).expect("resolve default");
    assert_eq!(
        resolved_default.node_config.gateway.listen_addr,
        "127.0.0.1:5432"
    );
    assert_eq!(
        resolved_default.origins.get("gateway.listen_addr"),
        Some(&ConfigOrigin::Default)
    );

    // Tier 2: File overrides default
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"version = 1
[node]
role = "all"

[gateway]
listen_addr = "0.0.0.0:5432"
"#
    )
    .unwrap();

    let resolved_file =
        ConfigResolver::resolve(Some(&config_file), &empty_overrides).expect("resolve file");
    assert_eq!(
        resolved_file.node_config.gateway.listen_addr,
        "0.0.0.0:5432"
    );
    assert_eq!(
        resolved_file.origins.get("gateway.listen_addr"),
        Some(&ConfigOrigin::File(config_file.clone()))
    );

    // Tier 3: Environment overrides file
    std::env::set_var("ROCKSTREAM__GATEWAY__LISTEN_ADDR", "10.0.0.1:5432");
    let resolved_env =
        ConfigResolver::resolve(Some(&config_file), &empty_overrides).expect("resolve env");
    assert_eq!(
        resolved_env.node_config.gateway.listen_addr,
        "10.0.0.1:5432"
    );
    assert_eq!(
        resolved_env.origins.get("gateway.listen_addr"),
        Some(&ConfigOrigin::Environment(
            "ROCKSTREAM__GATEWAY__LISTEN_ADDR".to_string()
        ))
    );

    // Tier 4: CLI overrides environment
    let cli_overrides = CliConfigOverrides {
        listen_addr: Some("192.168.1.1:5432".to_string()),
        ..Default::default()
    };
    let resolved_cli =
        ConfigResolver::resolve(Some(&config_file), &cli_overrides).expect("resolve cli");
    std::env::remove_var("ROCKSTREAM__GATEWAY__LISTEN_ADDR");

    assert_eq!(
        resolved_cli.node_config.gateway.listen_addr,
        "192.168.1.1:5432"
    );
    assert_eq!(
        resolved_cli.origins.get("gateway.listen_addr"),
        Some(&ConfigOrigin::Cli("--listen".to_string()))
    );
}

#[test]
fn test_effective_config_origin_annotations() {
    let _guard = ENV_LOCK.lock().unwrap();
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"version = 1
[node]
role = "worker"

[gateway]
listen_addr = "0.0.0.0:5432"

[worker]
execution_threads = 4
"#
    )
    .unwrap();

    std::env::set_var("ROCKSTREAM__AUTH__MODE", "scram");

    let overrides = CliConfigOverrides {
        worker_id: Some(101),
        log_level: Some("debug".to_string()),
        ..Default::default()
    };

    let resolved =
        ConfigResolver::resolve(Some(&config_file), &overrides).expect("resolve succeeds");
    std::env::remove_var("ROCKSTREAM__AUTH__MODE");

    let annotated_toml = resolved.to_toml_text(true);

    // Check File origin annotations
    assert!(
        annotated_toml.contains(&format!("# origin: file({})", config_file.display())),
        "expected file origin in:\n{annotated_toml}"
    );
    assert!(annotated_toml.contains("role = \"worker\""));
    assert!(annotated_toml.contains("execution_threads = 4"));

    // Check Env origin annotations
    assert!(
        annotated_toml.contains("# origin: env(ROCKSTREAM__AUTH__MODE)"),
        "expected env origin in:\n{annotated_toml}"
    );
    assert!(annotated_toml.contains("mode = \"scram\""));

    // Check CLI origin annotations
    assert!(
        annotated_toml.contains("# origin: cli(--worker-id)"),
        "expected cli origin for worker_id in:\n{annotated_toml}"
    );
    assert!(
        annotated_toml.contains("# origin: cli(--log-level)"),
        "expected cli origin for log_level in:\n{annotated_toml}"
    );

    // Check Default origin annotations
    assert!(
        annotated_toml.contains("# origin: default"),
        "expected default origin in:\n{annotated_toml}"
    );
}

#[test]
fn test_secret_redaction_in_print_effective() {
    let _guard = ENV_LOCK.lock().unwrap();
    let overrides = CliConfigOverrides {
        auth_secret_path: Some(PathBuf::from("/etc/rockstream/auth_secrets.key")),
        tls_key_path: Some(PathBuf::from("/etc/rockstream/private_gateway.pem")),
        ..Default::default()
    };

    let resolved = ConfigResolver::resolve(None, &overrides).expect("resolve succeeds");

    // Output without origins
    let text_no_origins = resolved.to_toml_text(false);
    assert!(
        text_no_origins.contains("[REDACTED]"),
        "expected [REDACTED] in plain effective output:\n{text_no_origins}"
    );
    assert!(
        !text_no_origins.contains("/etc/rockstream/auth_secrets.key"),
        "secret leaked in plain output!"
    );
    assert!(
        !text_no_origins.contains("/etc/rockstream/private_gateway.pem"),
        "private key leaked in plain output!"
    );

    // Output with origins
    let text_with_origins = resolved.to_toml_text(true);
    assert!(
        text_with_origins.contains("[REDACTED]"),
        "expected [REDACTED] in annotated output:\n{text_with_origins}"
    );
    assert!(
        !text_with_origins.contains("/etc/rockstream/auth_secrets.key"),
        "secret leaked in origin-annotated output!"
    );
    assert!(
        !text_with_origins.contains("/etc/rockstream/private_gateway.pem"),
        "private key leaked in origin-annotated output!"
    );
    assert!(text_with_origins.contains("# origin: cli(--auth-secret-path)"));
    assert!(text_with_origins.contains("# origin: cli(--tls-key-path)"));
}

#[test]
fn test_mixed_precedence_with_complete_origins() {
    let _guard = ENV_LOCK.lock().unwrap();
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"version = 1
[node]
role = "control"

[metrics]
listen_addr = "127.0.0.1:9191"
"#
    )
    .unwrap();

    std::env::set_var("ROCKSTREAM__LOGGING__LEVEL", "warn");
    let overrides = CliConfigOverrides {
        worker_threads: Some(16),
        ..Default::default()
    };

    let resolved =
        ConfigResolver::resolve(Some(&config_file), &overrides).expect("resolve succeeds");
    std::env::remove_var("ROCKSTREAM__LOGGING__LEVEL");

    // Default source
    assert_eq!(
        resolved.origins.get("node.availability_zone"),
        Some(&ConfigOrigin::Default)
    );
    // File source
    assert_eq!(
        resolved.origins.get("node.role"),
        Some(&ConfigOrigin::File(config_file.clone()))
    );
    assert_eq!(
        resolved.origins.get("metrics.listen_addr"),
        Some(&ConfigOrigin::File(config_file.clone()))
    );
    // Env source
    assert_eq!(
        resolved.origins.get("logging.level"),
        Some(&ConfigOrigin::Environment(
            "ROCKSTREAM__LOGGING__LEVEL".to_string()
        ))
    );
    // CLI source
    assert_eq!(
        resolved.origins.get("worker.execution_threads"),
        Some(&ConfigOrigin::Cli("--worker-threads".to_string()))
    );
}
