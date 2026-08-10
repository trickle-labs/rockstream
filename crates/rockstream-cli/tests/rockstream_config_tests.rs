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
