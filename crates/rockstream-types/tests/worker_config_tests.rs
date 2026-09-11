//! NodeConfig Worker Memory Budget & Concurrency Configuration Tests (v0.62.1 Slice 1 / Phase 3a).

use rockstream_types::config::NodeConfig;
use rockstream_types::config_resolver::{CliConfigOverrides, ConfigOrigin, ConfigResolver};
use rockstream_types::config_validation::validate_node_config;
use std::path::PathBuf;

#[test]
fn test_worker_budget_defaults_and_schema() {
    let cfg = NodeConfig::default();
    assert_eq!(cfg.worker.memory_budget_bytes, 2_147_483_648);
    assert_eq!(cfg.worker.foreground_reservation_bytes, 429_496_729);
    assert_eq!(cfg.worker.max_compaction_concurrency, 2);
    assert_eq!(cfg.worker.max_backfill_concurrency, 1);
    assert_eq!(cfg.worker.max_migration_concurrency, 1);
    assert_eq!(cfg.worker.disk_cache_dir, None);
    assert_eq!(cfg.worker.disk_cache_bytes, 17_179_869_184);

    // Serialization and deserialization roundtrip
    let toml_str = toml::to_string(&cfg).expect("serialize NodeConfig to toml");
    let deserialized: NodeConfig =
        toml::from_str(&toml_str).expect("deserialize NodeConfig from toml");
    assert_eq!(cfg.worker, deserialized.worker);
}

#[test]
fn test_worker_budget_precedence_and_origins() {
    // 1. Resolve with default origins
    let resolved =
        ConfigResolver::resolve(None, &CliConfigOverrides::default()).expect("resolve defaults");
    assert_eq!(
        resolved.origins.get("worker.memory_budget_bytes"),
        Some(&ConfigOrigin::Default)
    );
    assert_eq!(
        resolved.origins.get("worker.foreground_reservation_bytes"),
        Some(&ConfigOrigin::Default)
    );
    assert_eq!(
        resolved.origins.get("worker.max_compaction_concurrency"),
        Some(&ConfigOrigin::Default)
    );

    // 2. Resolve with CLI overrides
    let cli_overrides = CliConfigOverrides {
        worker_memory_budget_bytes: Some(4 * 1024 * 1024 * 1024),
        worker_foreground_reservation_bytes: Some(512 * 1024 * 1024),
        worker_max_compaction_concurrency: Some(4),
        worker_disk_cache_dir: Some(PathBuf::from("/tmp/cache")),
        ..Default::default()
    };

    let resolved_cli = ConfigResolver::resolve(None, &cli_overrides).expect("resolve with cli");
    assert_eq!(
        resolved_cli.node_config.worker.memory_budget_bytes,
        4 * 1024 * 1024 * 1024
    );
    assert_eq!(
        resolved_cli.node_config.worker.foreground_reservation_bytes,
        512 * 1024 * 1024
    );
    assert_eq!(
        resolved_cli.node_config.worker.max_compaction_concurrency,
        4
    );
    assert_eq!(
        resolved_cli.node_config.worker.disk_cache_dir,
        Some(PathBuf::from("/tmp/cache"))
    );
    assert_eq!(
        resolved_cli.origins.get("worker.memory_budget_bytes"),
        Some(&ConfigOrigin::Cli("--worker-memory-budget".to_string()))
    );
}

#[test]
fn test_invalid_worker_budget_validation_fails_before_bind() {
    // Case 1: foreground_reservation_bytes >= memory_budget_bytes
    let mut cfg = NodeConfig::default();
    cfg.worker.memory_budget_bytes = 100 * 1024 * 1024;
    cfg.worker.foreground_reservation_bytes = 100 * 1024 * 1024;
    let report = validate_node_config(&cfg);
    assert!(!report.valid);
    let diag = report
        .diagnostics
        .iter()
        .find(|d| d.path == "worker.foreground_reservation_bytes")
        .expect("diagnostic for foreground_reservation_bytes");
    assert_eq!(diag.code, "RS-0002");

    // Case 2: memory_budget_bytes < 64 MiB
    let mut cfg2 = NodeConfig::default();
    cfg2.worker.memory_budget_bytes = 32 * 1024 * 1024; // 32 MiB < 64 MiB
    cfg2.worker.foreground_reservation_bytes = 4 * 1024 * 1024;
    let report2 = validate_node_config(&cfg2);
    assert!(!report2.valid);
    let diag2 = report2
        .diagnostics
        .iter()
        .find(|d| d.path == "worker.memory_budget_bytes")
        .expect("diagnostic for memory_budget_bytes");
    assert_eq!(diag2.code, "RS-0002");

    // Case 3: max_compaction_concurrency == 0
    let mut cfg3 = NodeConfig::default();
    cfg3.worker.max_compaction_concurrency = 0;
    let report3 = validate_node_config(&cfg3);
    assert!(!report3.valid);
    let diag3 = report3
        .diagnostics
        .iter()
        .find(|d| d.path == "worker.max_compaction_concurrency")
        .expect("diagnostic for max_compaction_concurrency");
    assert_eq!(diag3.code, "RS-0002");
}
