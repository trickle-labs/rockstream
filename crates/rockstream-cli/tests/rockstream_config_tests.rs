//! v0.51.25 Slice S5 — Exchange Network Policy & Config TOML Tests

use rockstream_types::config::{ExchangeConfig, RockstreamConfig};

#[test]
fn test_exchange_config_toml_defaults() {
    let toml_str = r#"
[exchange]
connect_timeout_ms = 500
rpc_timeout_ms = 15000
max_retries = 5
backoff_jitter_ms = 200
frame_channel_capacity = 128
"#;

    let parsed: RockstreamConfig = toml::from_str(toml_str).expect("failed to parse TOML");
    assert_eq!(parsed.exchange.connect_timeout_ms, 500);
    assert_eq!(parsed.exchange.rpc_timeout_ms, 15000);
    assert_eq!(parsed.exchange.max_retries, 5);
    assert_eq!(parsed.exchange.backoff_jitter_ms, 200);
    assert_eq!(parsed.exchange.frame_channel_capacity, 128);

    let default_parsed: RockstreamConfig = toml::from_str("").expect("failed to parse empty TOML");
    assert_eq!(default_parsed.exchange, ExchangeConfig::default());
    assert_eq!(default_parsed.exchange.connect_timeout_ms, 250);
    assert_eq!(default_parsed.exchange.rpc_timeout_ms, 10000);
    assert_eq!(default_parsed.exchange.max_retries, 3);
    assert_eq!(default_parsed.exchange.backoff_jitter_ms, 100);
    assert_eq!(default_parsed.exchange.frame_channel_capacity, 64);
}

#[test]
fn test_cli_config_print_effective_command() {
    use rockstream_cli::output::OutputFormat;
    use rockstream_cli::run_config_print_effective;
    use rockstream_types::config_resolver::CliConfigOverrides;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    let tmp = tempdir().unwrap();
    let config_file = tmp.path().join("rockstream.toml");
    let mut f = File::create(&config_file).unwrap();
    writeln!(
        f,
        r#"version = 1
[node]
role = "all"
host_id = "node-1"

[gateway]
listen_addr = "127.0.0.1:5432"

[auth]
mode = "scram"
secret_path = "/etc/rockstream/secrets.key"
"#
    )
    .unwrap();

    let overrides = CliConfigOverrides {
        listen_addr: Some("0.0.0.0:5433".to_string()),
        worker_threads: Some(8),
        ..Default::default()
    };

    // 1. Text format without origins: must contain resolved fields, redacted secrets, and valid TOML
    let text_out =
        run_config_print_effective(OutputFormat::Text, Some(&config_file), false, &overrides)
            .expect("print-effective text succeeds");
    assert!(text_out.contains("version = 1"));
    assert!(text_out.contains(r#"role = "all""#));
    assert!(text_out.contains(r#"host_id = "node-1""#));
    assert!(text_out.contains(r#"listen_addr = "0.0.0.0:5433""#));
    assert!(text_out.contains("execution_threads = 8"));
    assert!(text_out.contains("[REDACTED]"));
    assert!(!text_out.contains("/etc/rockstream/secrets.key"));

    // 2. Text format with origins: must contain origins for each tier
    let origins_out =
        run_config_print_effective(OutputFormat::Text, Some(&config_file), true, &overrides)
            .expect("print-effective origins succeeds");
    assert!(origins_out.contains(&format!("# origin: file({})", config_file.display())));
    assert!(origins_out.contains("# origin: cli(--listen)"));
    assert!(origins_out.contains("# origin: cli(--worker-threads)"));
    assert!(origins_out.contains("# origin: default"));
    assert!(origins_out.contains("[REDACTED]"));
    assert!(!origins_out.contains("/etc/rockstream/secrets.key"));

    // 3. JSON format: verify valid schema
    let json_out =
        run_config_print_effective(OutputFormat::Json, Some(&config_file), false, &overrides)
            .expect("print-effective json succeeds");
    let parsed: serde_json::Value = serde_json::from_str(&json_out).expect("valid JSON schema");
    assert_eq!(parsed["node_config"]["version"], 1);
    assert_eq!(parsed["node_config"]["node"]["role"], "all");
    assert_eq!(parsed["node_config"]["node"]["host_id"], "node-1");
    assert_eq!(
        parsed["node_config"]["gateway"]["listen_addr"],
        "0.0.0.0:5433"
    );
    assert_eq!(parsed["node_config"]["worker"]["execution_threads"], 8);
}
