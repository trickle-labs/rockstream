//! v0.59.4 Slice 2 — Multi-Layer Semantic Config Validation Tests (CFG-01)

use std::fs::File;
use tempfile::tempdir;

use rockstream_types::config_validation::validate_config_str;

#[test]
fn test_syntax_errors() {
    let invalid_toml = r#"
[cluster
min_epoch_ms = 10
"#;
    let report = validate_config_str(invalid_toml, false);
    assert!(!report.valid);
    assert_eq!(report.diagnostics.len(), 1);
    assert_eq!(report.diagnostics[0].path, "syntax");
    assert_eq!(report.diagnostics[0].code, "RS-0002");
    assert!(report.diagnostics[0].line.is_some());
}

#[test]
fn test_unknown_key_typo_suggestions() {
    let toml_str = r#"
[cluster]
min_epoch_msec = 10
"#;
    let report = validate_config_str(toml_str, false);
    assert!(!report.valid);
    let diag = report
        .diagnostics
        .iter()
        .find(|d| d.path == "cluster.min_epoch_msec")
        .expect("diagnostic for unknown key min_epoch_msec");

    assert_eq!(diag.code, "RS-0002");
    assert_eq!(
        diag.suggestion.as_deref(),
        Some("Did you mean `min_epoch_ms`?")
    );
}

#[test]
fn test_cluster_shutdown_timeout_secs_is_known_key() {
    let toml_str = r#"
[cluster]
shutdown_timeout_secs = 45
"#;
    let report = validate_config_str(toml_str, false);
    assert_eq!(
        report,
        rockstream_types::config_validation::ConfigValidationReport::success()
    );
}

#[test]
fn test_deprecated_tiering_keys() {
    let toml_str = r#"
[storage.tiering]
cold_sst_backend = "s3"
"#;
    let report = validate_config_str(toml_str, false);
    assert!(!report.valid);
    let diag = report
        .diagnostics
        .iter()
        .find(|d| d.path == "storage.tiering.cold_sst_backend")
        .expect("diagnostic for deprecated tiering key");

    assert_eq!(diag.code, "RS-4017");
    assert!(diag.message.contains("Storage tiering is removed"));
}

#[test]
fn test_cluster_semantic_rules() {
    // min_epoch_ms = 0
    let toml_zero_epoch = r#"
[cluster]
min_epoch_ms = 0
"#;
    let report = validate_config_str(toml_zero_epoch, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.min_epoch_ms" && d.code == "RS-0002"));

    // checkpoint_retention_count = 0
    let toml_zero_ckpt = r#"
[cluster]
checkpoint_retention_count = 0
"#;
    let report = validate_config_str(toml_zero_ckpt, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.checkpoint_retention_count"));

    // state_budget_gb = 0
    let toml_zero_budget = r#"
[cluster]
state_budget_gb = 0
"#;
    let report = validate_config_str(toml_zero_budget, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.state_budget_gb"));

    // index_prefer_selectivity_threshold out of bounds (1.5)
    let toml_invalid_selectivity = r#"
[cluster]
index_prefer_selectivity_threshold = 1.5
"#;
    let report = validate_config_str(toml_invalid_selectivity, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.index_prefer_selectivity_threshold"));
}

#[test]
fn test_autotuner_semantic_rules() {
    // min > default > max violation (min=8, default=4, max=2)
    let toml_bad_autotuner = r#"
[cluster.autotuner]
min_parallelism = 8
default_parallelism = 4
max_parallelism = 2
"#;
    let report = validate_config_str(toml_bad_autotuner, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.autotuner.parallelism"));
}

#[test]
fn test_skew_semantic_rules() {
    let toml_bad_skew = r#"
[cluster.skew_split]
hot_key_factor = 0.5
max_skew_buckets = 0
"#;
    let report = validate_config_str(toml_bad_skew, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.skew_split.hot_key_factor"));
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "cluster.skew_split.max_skew_buckets"));
}

#[test]
fn test_worker_semantic_rules() {
    let toml_bad_worker = r#"
[worker]
segment_cache_bytes = 0
max_rows_per_quantum = 0
execution_threads = 0
"#;
    let report = validate_config_str(toml_bad_worker, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "worker.segment_cache_bytes"));
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "worker.max_rows_per_quantum"));
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "worker.execution_threads"));
}

#[test]
fn test_exchange_semantic_rules() {
    // direct threshold (20MB) > spill threshold (10MB)
    let toml_bad_exchange = r#"
[exchange]
exchange_direct_threshold_bytes = 20971520
exchange_spill_threshold_mb = 10
"#;
    let report = validate_config_str(toml_bad_exchange, false);
    assert!(!report.valid);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.path == "exchange.thresholds"));
}

#[test]
fn test_tls_semantic_rules() {
    // Gateway TLS pair requirement (cert set, key missing)
    let toml_gw_tls = r#"
[gateway]
tls_cert_path = "/path/to/cert.pem"
"#;
    let report = validate_config_str(toml_gw_tls, false);
    assert!(!report.valid);
    assert!(report.diagnostics.iter().any(|d| d.path == "gateway.tls"));

    // Internal TLS triplet requirement (cert + key set, CA missing)
    let toml_internal_tls = r#"
[internal_tls]
cert_path = "/path/to/cert.pem"
key_path = "/path/to/key.pem"
"#;
    let report = validate_config_str(toml_internal_tls, false);
    assert!(!report.valid);
    assert!(report.diagnostics.iter().any(|d| d.path == "internal_tls"));
}

#[test]
fn test_check_files_flag() {
    let tmp = tempdir().unwrap();
    let cert_file = tmp.path().join("cert.pem");
    let key_file = tmp.path().join("key.pem");
    File::create(&cert_file).unwrap();
    File::create(&key_file).unwrap();

    let non_existent = tmp.path().join("missing.pem");

    // Existing files pass check
    let toml_valid_files = format!(
        r#"
[gateway]
tls_cert_path = "{}"
tls_key_path = "{}"
"#,
        cert_file.display(),
        key_file.display()
    );
    let report_valid = validate_config_str(&toml_valid_files, true);
    assert!(report_valid.valid);

    // Non-existent file fails with check_files = true
    let toml_invalid_files = format!(
        r#"
[gateway]
tls_cert_path = "{}"
tls_key_path = "{}"
"#,
        cert_file.display(),
        non_existent.display()
    );
    let report_invalid = validate_config_str(&toml_invalid_files, true);
    assert!(!report_invalid.valid);
    assert!(report_invalid
        .diagnostics
        .iter()
        .any(|d| d.path == "gateway.tls_key_path" && d.code == "RS-0002"));
}
