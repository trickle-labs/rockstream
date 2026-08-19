//! Authoritative configuration resolver with four-tier binding precedence and origin tracking.
//!
//! Precedence order:
//! 1. Compiled Defaults (`RockstreamConfig::default()`)
//! 2. Config File (`--config`/`--file`, `ROCKSTREAM_CONFIG`, `./rockstream.toml`)
//! 3. Environment Variables (`ROCKSTREAM__<SECTION>__<KEY>`)
//! 4. CLI Flag Overrides (`CliConfigOverrides`)

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::RockstreamConfig;
use crate::config_validation::{validate_config_str, ConfigDiagnostic};

/// Source origin of a configuration value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", content = "value")]
pub enum ConfigOrigin {
    Default,
    File(PathBuf),
    Environment(String),
    Cli(String),
}

impl std::fmt::Display for ConfigOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "default"),
            Self::File(p) => write!(f, "file({})", p.display()),
            Self::Environment(v) => write!(f, "env({v})"),
            Self::Cli(flag) => write!(f, "cli({flag})"),
        }
    }
}

/// CLI override flags matching start and config print-effective commands.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CliConfigOverrides {
    pub min_epoch_ms: Option<u64>,
    pub checkpoint_retention_count: Option<u32>,
    pub state_budget_gb: Option<u64>,
    pub exchange_direct_threshold_bytes: Option<usize>,
    pub exchange_spill_threshold_mb: Option<u64>,
    pub exchange_domain_size: Option<usize>,
    pub exchange_force_durable: Option<bool>,
    pub same_host_shm_segment_bytes: Option<usize>,
    pub same_host_shm_segments_per_peer: Option<usize>,
    pub max_exchange_compression_states: Option<usize>,
    pub webhook_listen_addr: Option<String>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub tls_ca_cert_path: Option<PathBuf>,
    pub internal_tls_cert_path: Option<PathBuf>,
    pub internal_tls_key_path: Option<PathBuf>,
    pub internal_tls_ca_cert_path: Option<PathBuf>,
}

/// A resolved configuration with tracked source origins for each parameter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedConfig {
    pub config: RockstreamConfig,
    pub origins: BTreeMap<String, ConfigOrigin>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub diagnostics: Vec<ConfigDiagnostic>,
}

impl ResolvedConfig {
    /// Format the resolved configuration as TOML, with optional source origin comments.
    pub fn to_toml_text(&self, show_origins: bool) -> String {
        let toml_str = toml::to_string_pretty(&self.config).unwrap_or_default();
        if !show_origins {
            return toml_str;
        }

        let mut lines = Vec::new();
        let mut current_section = String::new();

        for line in toml_str.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                current_section = trimmed[1..trimmed.len() - 1].to_string();
                lines.push(line.to_string());
                continue;
            }

            if let Some((k, _)) = line.split_once('=') {
                let key = k.trim();
                let full_key = if current_section.is_empty() {
                    key.to_string()
                } else {
                    format!("{current_section}.{key}")
                };

                if let Some(origin) = self.origins.get(&full_key) {
                    lines.push(format!("{line}  # origin: {origin}"));
                } else {
                    lines.push(line.to_string());
                }
            } else {
                lines.push(line.to_string());
            }
        }
        lines.join("\n")
    }

    /// Return a copy of `config` with sensitive values redacted.
    pub fn redacted_config(&self) -> RockstreamConfig {
        let mut cfg = self.config.clone();
        if let Some(ref mut key_path) = cfg.gateway.tls_key_path {
            let path_str = key_path.to_string_lossy();
            if path_str.contains("secret") || path_str.contains("private") {
                *key_path = PathBuf::from("[REDACTED]");
            }
        }
        if let Some(ref mut key_path) = cfg.internal_tls.key_path {
            let path_str = key_path.to_string_lossy();
            if path_str.contains("secret") || path_str.contains("private") {
                *key_path = PathBuf::from("[REDACTED]");
            }
        }
        cfg
    }
}

/// The authoritative configuration resolver.
pub struct ConfigResolver;

impl ConfigResolver {
    /// Resolve the authoritative configuration according to the 4-tier precedence:
    /// Defaults → File → Environment Variables → CLI Overrides.
    pub fn resolve(
        file: Option<&Path>,
        cli_overrides: &CliConfigOverrides,
    ) -> Result<ResolvedConfig, String> {
        let mut config = RockstreamConfig::default();
        let mut origins = BTreeMap::new();
        let mut diagnostics = Vec::new();

        // 1. Mark defaults
        init_default_origins(&mut origins);

        // 2. Resolve Config File
        let resolved_file_path = file
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("ROCKSTREAM_CONFIG").map(PathBuf::from))
            .or_else(|| {
                let default_path = PathBuf::from("rockstream.toml");
                if default_path.exists() {
                    Some(default_path)
                } else {
                    None
                }
            });

        if let Some(file_path) = resolved_file_path {
            if !file_path.exists() {
                if file.is_some() {
                    return Err(format!(
                        "RS-0002: Config file does not exist: {}",
                        file_path.display()
                    ));
                }
            } else {
                let contents = std::fs::read_to_string(&file_path).map_err(|e| {
                    format!("Failed to read config file {}: {e}", file_path.display())
                })?;

                let report = validate_config_str(&contents, false);
                diagnostics.extend(report.diagnostics);

                let parsed_config = RockstreamConfig::load_from_str(&contents).map_err(|e| {
                    format!("Failed to parse config file {}: {e}", file_path.display())
                })?;

                // Merge file values into config and update origins
                if let Ok(toml_val) = toml::from_str::<toml::Value>(&contents) {
                    merge_toml_table_origins(&toml_val, "", &file_path, &mut origins);
                }
                config = parsed_config;
            }
        }

        // 3. Environment Variables (ROCKSTREAM__<SECTION>__<KEY>)
        apply_env_vars(&mut config, &mut origins);

        // 4. CLI Overrides
        apply_cli_overrides(&mut config, cli_overrides, &mut origins);

        // Validate final semantic bounds
        crate::config_validation::validate_semantic_bounds(&config, false, &mut diagnostics);
        diagnostics.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.message.cmp(&b.message)));

        Ok(ResolvedConfig {
            config,
            origins,
            diagnostics,
        })
    }
}

fn init_default_origins(origins: &mut BTreeMap<String, ConfigOrigin>) {
    origins.insert(
        "recursion_max_iterations".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("cluster.min_epoch_ms".to_string(), ConfigOrigin::Default);
    origins.insert(
        "cluster.checkpoint_retention_count".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("cluster.state_budget_gb".to_string(), ConfigOrigin::Default);
    origins.insert(
        "cluster.index_prefer_selectivity_threshold".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.index_max_lag_ms".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.autotuner.enabled".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.autotuner.min_parallelism".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.autotuner.default_parallelism".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.autotuner.max_parallelism".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.skew_split.enabled".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.skew_split.hot_key_factor".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "cluster.skew_split.max_skew_buckets".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "worker.segment_cache_bytes".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "worker.max_rows_per_quantum".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "connector.dlq_warn_threshold".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "connector.dlq_retention_days".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.exchange_direct_threshold_bytes".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.exchange_spill_threshold_mb".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.exchange_domain_size".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.exchange_force_durable".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.same_host_shm_segment_bytes".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.same_host_shm_segments_per_peer".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.max_exchange_compression_states".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.connect_timeout_ms".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("exchange.rpc_timeout_ms".to_string(), ConfigOrigin::Default);
    origins.insert("exchange.max_retries".to_string(), ConfigOrigin::Default);
    origins.insert(
        "exchange.backoff_jitter_ms".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert(
        "exchange.frame_channel_capacity".to_string(),
        ConfigOrigin::Default,
    );
}

fn merge_toml_table_origins(
    val: &toml::Value,
    section: &str,
    file_path: &Path,
    origins: &mut BTreeMap<String, ConfigOrigin>,
) {
    if let toml::Value::Table(tbl) = val {
        for (k, v) in tbl {
            let full_key = if section.is_empty() {
                k.clone()
            } else {
                format!("{section}.{k}")
            };
            if let toml::Value::Table(_) = v {
                merge_toml_table_origins(v, &full_key, file_path, origins);
            } else {
                origins.insert(full_key, ConfigOrigin::File(file_path.to_path_buf()));
            }
        }
    }
}

fn apply_env_vars(config: &mut RockstreamConfig, origins: &mut BTreeMap<String, ConfigOrigin>) {
    for (k, v) in std::env::vars() {
        if !k.starts_with("ROCKSTREAM__") {
            continue;
        }
        let stripped = &k["ROCKSTREAM__".len()..];
        let parts: Vec<&str> = stripped.split("__").collect();

        match parts.as_slice() {
            ["CLUSTER", "MIN_EPOCH_MS"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.cluster.min_epoch_ms = val;
                    origins.insert(
                        "cluster.min_epoch_ms".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "CHECKPOINT_RETENTION_COUNT"] => {
                if let Ok(val) = v.parse::<u32>() {
                    config.cluster.checkpoint_retention_count = val;
                    origins.insert(
                        "cluster.checkpoint_retention_count".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "STATE_BUDGET_GB"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.cluster.state_budget_gb = val;
                    origins.insert(
                        "cluster.state_budget_gb".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["EXCHANGE", "EXCHANGE_DIRECT_THRESHOLD_BYTES"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.exchange.exchange_direct_threshold_bytes = val;
                    origins.insert(
                        "exchange.exchange_direct_threshold_bytes".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["EXCHANGE", "EXCHANGE_SPILL_THRESHOLD_MB"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.exchange.exchange_spill_threshold_mb = val;
                    origins.insert(
                        "exchange.exchange_spill_threshold_mb".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["EXCHANGE", "EXCHANGE_FORCE_DURABLE"] => {
                if let Ok(val) = v.parse::<bool>() {
                    config.exchange.exchange_force_durable = val;
                    origins.insert(
                        "exchange.exchange_force_durable".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["WORKER", "SEGMENT_CACHE_BYTES"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.worker.segment_cache_bytes = val;
                    origins.insert(
                        "worker.segment_cache_bytes".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["WORKER", "MAX_ROWS_PER_QUANTUM"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.worker.max_rows_per_quantum = val;
                    origins.insert(
                        "worker.max_rows_per_quantum".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["RECURSION_MAX_ITERATIONS"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.recursion_max_iterations = val;
                    origins.insert(
                        "recursion_max_iterations".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            _ => {}
        }
    }
}

fn apply_cli_overrides(
    config: &mut RockstreamConfig,
    cli: &CliConfigOverrides,
    origins: &mut BTreeMap<String, ConfigOrigin>,
) {
    if let Some(val) = cli.min_epoch_ms {
        config.cluster.min_epoch_ms = val;
        origins.insert(
            "cluster.min_epoch_ms".to_string(),
            ConfigOrigin::Cli("--min-epoch-ms".to_string()),
        );
    }
    if let Some(val) = cli.checkpoint_retention_count {
        config.cluster.checkpoint_retention_count = val;
        origins.insert(
            "cluster.checkpoint_retention_count".to_string(),
            ConfigOrigin::Cli("--checkpoint-retention-count".to_string()),
        );
    }
    if let Some(val) = cli.state_budget_gb {
        config.cluster.state_budget_gb = val;
        origins.insert(
            "cluster.state_budget_gb".to_string(),
            ConfigOrigin::Cli("--state-budget-gb".to_string()),
        );
    }
    if let Some(val) = cli.exchange_direct_threshold_bytes {
        config.exchange.exchange_direct_threshold_bytes = val;
        origins.insert(
            "exchange.exchange_direct_threshold_bytes".to_string(),
            ConfigOrigin::Cli("--exchange-direct-threshold-bytes".to_string()),
        );
    }
    if let Some(val) = cli.exchange_spill_threshold_mb {
        config.exchange.exchange_spill_threshold_mb = val;
        origins.insert(
            "exchange.exchange_spill_threshold_mb".to_string(),
            ConfigOrigin::Cli("--exchange-spill-threshold-mb".to_string()),
        );
    }
    if let Some(val) = cli.exchange_domain_size {
        config.exchange.exchange_domain_size = val;
        origins.insert(
            "exchange.exchange_domain_size".to_string(),
            ConfigOrigin::Cli("--exchange-domain-size".to_string()),
        );
    }
    if let Some(val) = cli.exchange_force_durable {
        if val {
            config.exchange.exchange_force_durable = true;
            origins.insert(
                "exchange.exchange_force_durable".to_string(),
                ConfigOrigin::Cli("--exchange-force-durable".to_string()),
            );
        }
    }
    if let Some(val) = cli.same_host_shm_segment_bytes {
        config.exchange.same_host_shm_segment_bytes = val;
        origins.insert(
            "exchange.same_host_shm_segment_bytes".to_string(),
            ConfigOrigin::Cli("--same-host-shm-segment-bytes".to_string()),
        );
    }
    if let Some(val) = cli.same_host_shm_segments_per_peer {
        config.exchange.same_host_shm_segments_per_peer = val;
        origins.insert(
            "exchange.same_host_shm_segments_per_peer".to_string(),
            ConfigOrigin::Cli("--same-host-shm-segments-per-peer".to_string()),
        );
    }
    if let Some(val) = cli.max_exchange_compression_states {
        config.exchange.max_exchange_compression_states = val;
        origins.insert(
            "exchange.max_exchange_compression_states".to_string(),
            ConfigOrigin::Cli("--max-exchange-compression-states".to_string()),
        );
    }
    if let Some(ref val) = cli.webhook_listen_addr {
        config.gateway.webhook_listen_addr = Some(val.clone());
        origins.insert(
            "gateway.webhook_listen_addr".to_string(),
            ConfigOrigin::Cli("--webhook-listen".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_cert_path {
        config.gateway.tls_cert_path = Some(val.clone());
        origins.insert(
            "gateway.tls_cert_path".to_string(),
            ConfigOrigin::Cli("--tls-cert-path".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_key_path {
        config.gateway.tls_key_path = Some(val.clone());
        origins.insert(
            "gateway.tls_key_path".to_string(),
            ConfigOrigin::Cli("--tls-key-path".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_ca_cert_path {
        config.gateway.tls_ca_cert_path = Some(val.clone());
        origins.insert(
            "gateway.tls_ca_cert_path".to_string(),
            ConfigOrigin::Cli("--tls-ca-cert-path".to_string()),
        );
    }
    if let Some(ref val) = cli.internal_tls_cert_path {
        config.internal_tls.cert_path = Some(val.clone());
        origins.insert(
            "internal_tls.cert_path".to_string(),
            ConfigOrigin::Cli("--internal-tls-cert-path".to_string()),
        );
    }
    if let Some(ref val) = cli.internal_tls_key_path {
        config.internal_tls.key_path = Some(val.clone());
        origins.insert(
            "internal_tls.key_path".to_string(),
            ConfigOrigin::Cli("--internal-tls-key-path".to_string()),
        );
    }
    if let Some(ref val) = cli.internal_tls_ca_cert_path {
        config.internal_tls.ca_cert_path = Some(val.clone());
        origins.insert(
            "internal_tls.ca_cert_path".to_string(),
            ConfigOrigin::Cli("--internal-tls-ca-cert-path".to_string()),
        );
    }
}
