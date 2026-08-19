//! v0.59.4 Slice 2 & 3 — CLI Config Commands Integration Tests (CFG-01, CFG-02)

use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

use rockstream_cli::output::OutputFormat;
use rockstream_cli::{run_config_print_effective, run_config_validate};
use rockstream_types::config_resolver::{CliConfigOverrides, ResolvedConfig};
use rockstream_types::config_validation::ConfigValidationReport;

#[test]
fn test_config_validate_json_schema() {
    let tmp = tempdir().unwrap();
    let valid_file = tmp.path().join("valid.toml");
    let mut f = File::create(&valid_file).unwrap();
    writeln!(
        f,
        r#"
[cluster]
min_epoch_ms = 10
checkpoint_retention_count = 5
state_budget_gb = 16
"#
    )
    .unwrap();

    let out_json = run_config_validate(OutputFormat::Json, Some(&valid_file), true, false)
        .expect("validation succeeds for valid file");

    let report: ConfigValidationReport =
        serde_json::from_str(&out_json).expect("valid json schema");
    assert!(report.valid);
    assert!(report.diagnostics.is_empty());

    let invalid_file = tmp.path().join("invalid.toml");
    let mut f2 = File::create(&invalid_file).unwrap();
    writeln!(
        f2,
        r#"
[cluster]
min_epoch_msec = 10
"#
    )
    .unwrap();

    let err = run_config_validate(OutputFormat::Json, Some(&invalid_file), true, false)
        .expect_err("validation fails on unknown key");
    assert_eq!(err.code.to_string(), "RS-0002");
    let report: ConfigValidationReport =
        serde_json::from_str(&err.next_steps).expect("valid json report in err.next_steps");
    assert!(!report.valid);
    assert!(!report.diagnostics.is_empty());
}

#[test]
fn test_config_print_effective_json_and_toml() {
    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"
[cluster]
min_epoch_ms = 30
"#
    )
    .unwrap();

    let overrides = CliConfigOverrides {
        exchange_spill_threshold_mb: Some(512),
        ..Default::default()
    };

    // Text format without origins
    let text_out =
        run_config_print_effective(OutputFormat::Text, Some(&config_file), false, &overrides)
            .expect("print-effective text succeeds");
    assert!(text_out.contains("min_epoch_ms = 30"));
    assert!(text_out.contains("exchange_spill_threshold_mb = 512"));

    // Text format with origins
    let text_with_origins =
        run_config_print_effective(OutputFormat::Text, Some(&config_file), true, &overrides)
            .expect("print-effective with origins succeeds");
    assert!(text_with_origins.contains("# origin: file("));
    assert!(text_with_origins.contains("# origin: cli("));

    // JSON format
    let json_out =
        run_config_print_effective(OutputFormat::Json, Some(&config_file), false, &overrides)
            .expect("print-effective json succeeds");
    let resolved: ResolvedConfig = serde_json::from_str(&json_out).expect("valid JSON schema");
    assert_eq!(resolved.config.cluster.min_epoch_ms, 30);
    assert_eq!(resolved.config.exchange.exchange_spill_threshold_mb, 512);
}

#[test]
fn test_start_and_print_effective_consistency() {
    let overrides = CliConfigOverrides {
        min_epoch_ms: Some(40),
        state_budget_gb: Some(32),
        ..Default::default()
    };

    let resolved = rockstream_types::config_resolver::ConfigResolver::resolve(None, &overrides)
        .expect("Resolution succeeds");

    assert_eq!(resolved.config.cluster.min_epoch_ms, 40);
    assert_eq!(resolved.config.cluster.state_budget_gb, 32);
}
