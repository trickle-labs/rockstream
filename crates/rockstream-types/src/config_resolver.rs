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

use crate::config::{NodeConfig, RockstreamConfig, StorageUrl};
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
    pub role: Option<String>,
    pub host_id: Option<String>,
    pub availability_zone: Option<String>,
    pub listen_addr: Option<String>,
    pub gateway_max_connections: Option<usize>,
    pub gateway_query_timeout_secs: Option<u64>,
    pub control_bind: Option<String>,
    pub control_url: Option<String>,
    pub control_shared_storage: Option<String>,
    pub worker_id: Option<u64>,
    pub worker_threads: Option<usize>,
    pub worker_cache_bytes: Option<usize>,
    pub worker_quantum: Option<usize>,
    pub storage_url: Option<String>,
    pub storage_temp_dir: Option<PathBuf>,
    pub storage_spill_dir: Option<PathBuf>,
    pub metrics_addr: Option<String>,
    pub metrics_enabled: Option<bool>,
    pub auth_mode: Option<String>,
    pub auth_secret_path: Option<PathBuf>,
    pub log_level: Option<String>,
    pub log_format: Option<String>,

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
    pub shutdown_timeout_secs: Option<u64>,
}

/// A resolved configuration with tracked source origins for each parameter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedConfig {
    pub config: RockstreamConfig,
    #[serde(default)]
    pub node_config: NodeConfig,
    pub origins: BTreeMap<String, ConfigOrigin>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub diagnostics: Vec<ConfigDiagnostic>,
}

impl ResolvedConfig {
    /// Format the resolved configuration as TOML, with optional source origin comments.
    pub fn to_toml_text(&self, show_origins: bool) -> String {
        let redacted = self.redacted_node_config();
        let mut toml_str = toml::to_string_pretty(&redacted).unwrap_or_default();
        if self.config.exchange != crate::config::ExchangeConfig::default() {
            if let Ok(ex_str) = toml::to_string_pretty(&self.config.exchange) {
                toml_str.push_str("\n[exchange]\n");
                toml_str.push_str(&ex_str);
            }
        }
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

    /// Return a copy of `node_config` with sensitive values redacted.
    pub fn redacted_node_config(&self) -> NodeConfig {
        let mut node = self.node_config.clone();
        if let Some(ref mut key_path) = node.gateway.tls.key_path {
            let path_str = key_path.to_string_lossy();
            if path_str.contains("secret") || path_str.contains("private") {
                *key_path = PathBuf::from("[REDACTED]");
            }
        }
        if let Some(ref mut secret_path) = node.auth.secret_path {
            *secret_path = PathBuf::from("[REDACTED]");
        }
        node
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
        let mut node_config = NodeConfig::default();
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

                if let Ok(mut parsed_node) = NodeConfig::load_from_str(&contents) {
                    if let Some(parent) = file_path.parent() {
                        parsed_node.storage.url = parsed_node.storage.url.resolve(parent);
                    }
                    config = RockstreamConfig::from(&parsed_node);
                    node_config = parsed_node;
                } else if let Ok(parsed_legacy) = RockstreamConfig::load_from_str(&contents) {
                    node_config = NodeConfig::from(&parsed_legacy);
                    config = parsed_legacy;
                }

                // Merge file values into config and update origins
                if let Ok(toml_val) = toml::from_str::<toml::Value>(&contents) {
                    merge_toml_table_origins(&toml_val, "", &file_path, &mut origins);
                    sync_canonical_and_legacy_origins(&mut origins);
                }
            }
        }

        // 3. Environment Variables (ROCKSTREAM__<SECTION>__<KEY>)
        apply_env_vars(&mut config, &mut node_config, &mut origins);

        // 4. CLI Overrides
        apply_cli_overrides(&mut config, &mut node_config, cli_overrides, &mut origins);

        // Validate final semantic bounds
        crate::config_validation::validate_node_config_semantic_bounds(
            &node_config,
            false,
            &mut diagnostics,
        );
        crate::config_validation::validate_semantic_bounds(&config, false, &mut diagnostics);
        diagnostics.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.message.cmp(&b.message)));

        Ok(ResolvedConfig {
            config,
            node_config,
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
        "cluster.shutdown_timeout_secs".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("execution.join_strategy".to_string(), ConfigOrigin::Default);
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
        "worker.execution_threads".to_string(),
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

    // Canonical NodeConfig defaults
    origins.insert("node.role".to_string(), ConfigOrigin::Default);
    origins.insert("node.host_id".to_string(), ConfigOrigin::Default);
    origins.insert("node.availability_zone".to_string(), ConfigOrigin::Default);
    origins.insert("gateway.listen_addr".to_string(), ConfigOrigin::Default);
    origins.insert("gateway.max_connections".to_string(), ConfigOrigin::Default);
    origins.insert(
        "gateway.query_timeout_secs".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("control.listen_addr".to_string(), ConfigOrigin::Default);
    origins.insert("control.url".to_string(), ConfigOrigin::Default);
    origins.insert("control.shared_storage".to_string(), ConfigOrigin::Default);
    origins.insert("worker.worker_id".to_string(), ConfigOrigin::Default);
    origins.insert("storage.url".to_string(), ConfigOrigin::Default);
    origins.insert("metrics.listen_addr".to_string(), ConfigOrigin::Default);
    origins.insert("metrics.enabled".to_string(), ConfigOrigin::Default);
    origins.insert("auth.mode".to_string(), ConfigOrigin::Default);
    origins.insert("logging.level".to_string(), ConfigOrigin::Default);
    origins.insert("logging.format".to_string(), ConfigOrigin::Default);
    origins.insert(
        "runtime.shutdown_timeout_secs".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("runtime.min_epoch_ms".to_string(), ConfigOrigin::Default);
    origins.insert(
        "runtime.checkpoint_retention_count".to_string(),
        ConfigOrigin::Default,
    );
    origins.insert("runtime.state_budget_gb".to_string(), ConfigOrigin::Default);
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

fn sync_canonical_and_legacy_origins(origins: &mut BTreeMap<String, ConfigOrigin>) {
    let mappings = [
        ("cluster.min_epoch_ms", "runtime.min_epoch_ms"),
        (
            "cluster.shutdown_timeout_secs",
            "runtime.shutdown_timeout_secs",
        ),
        (
            "cluster.checkpoint_retention_count",
            "runtime.checkpoint_retention_count",
        ),
        ("cluster.state_budget_gb", "runtime.state_budget_gb"),
        ("gateway.listen", "gateway.listen_addr"),
        ("metrics.listen", "metrics.listen_addr"),
    ];
    for (legacy, canonical) in mappings {
        if let Some(orig) = origins.get(legacy).cloned() {
            if orig != ConfigOrigin::Default {
                origins.insert(canonical.to_string(), orig);
            }
        }
        if let Some(orig) = origins.get(canonical).cloned() {
            if orig != ConfigOrigin::Default {
                origins.insert(legacy.to_string(), orig);
            }
        }
    }
}

fn apply_env_vars(
    config: &mut RockstreamConfig,
    node_config: &mut NodeConfig,
    origins: &mut BTreeMap<String, ConfigOrigin>,
) {
    if let Ok(value) = std::env::var("ROCKSTREAM_WORKER_EXECUTION_THREADS") {
        if let Ok(value) = value.parse::<usize>() {
            config.worker.execution_threads = value;
            node_config.worker.execution_threads = value;
            origins.insert(
                "worker.execution_threads".to_string(),
                ConfigOrigin::Environment("ROCKSTREAM_WORKER_EXECUTION_THREADS".to_string()),
            );
        }
    }
    for (k, v) in std::env::vars() {
        if !k.starts_with("ROCKSTREAM__") {
            continue;
        }
        let stripped = &k["ROCKSTREAM__".len()..];
        let parts: Vec<&str> = stripped.split("__").collect();

        match parts.as_slice() {
            ["NODE", "ROLE"] => {
                node_config.node.role = v.clone();
                origins.insert("node.role".to_string(), ConfigOrigin::Environment(k));
            }
            ["NODE", "HOST_ID"] => {
                node_config.node.host_id = Some(v.clone());
                origins.insert("node.host_id".to_string(), ConfigOrigin::Environment(k));
            }
            ["NODE", "AVAILABILITY_ZONE"] => {
                node_config.node.availability_zone = Some(v.clone());
                origins.insert(
                    "node.availability_zone".to_string(),
                    ConfigOrigin::Environment(k),
                );
            }
            ["GATEWAY", "LISTEN_ADDR"] => {
                node_config.gateway.listen_addr = v.clone();
                origins.insert(
                    "gateway.listen_addr".to_string(),
                    ConfigOrigin::Environment(k),
                );
            }
            ["CONTROL", "LISTEN_ADDR"] => {
                node_config.control.listen_addr = Some(v.clone());
                origins.insert(
                    "control.listen_addr".to_string(),
                    ConfigOrigin::Environment(k),
                );
            }
            ["CONTROL", "URL"] => {
                node_config.control.url = Some(v.clone());
                origins.insert("control.url".to_string(), ConfigOrigin::Environment(k));
            }
            ["WORKER", "WORKER_ID"] => {
                if let Ok(id) = v.parse::<u64>() {
                    node_config.worker.worker_id = Some(id);
                    origins.insert("worker.worker_id".to_string(), ConfigOrigin::Environment(k));
                }
            }
            ["STORAGE", "URL"] => {
                if let Ok(u) = StorageUrl::parse(&v) {
                    node_config.storage.url = u;
                    origins.insert("storage.url".to_string(), ConfigOrigin::Environment(k));
                }
            }
            ["METRICS", "LISTEN_ADDR"] => {
                node_config.metrics.listen_addr = v.clone();
                origins.insert(
                    "metrics.listen_addr".to_string(),
                    ConfigOrigin::Environment(k),
                );
            }
            ["AUTH", "MODE"] => {
                node_config.auth.mode = v.clone();
                origins.insert("auth.mode".to_string(), ConfigOrigin::Environment(k));
            }
            ["LOGGING", "LEVEL"] => {
                node_config.logging.level = v.clone();
                origins.insert("logging.level".to_string(), ConfigOrigin::Environment(k));
            }
            ["LOGGING", "FORMAT"] => {
                node_config.logging.format = v.clone();
                origins.insert("logging.format".to_string(), ConfigOrigin::Environment(k));
            }
            ["RUNTIME", "SHUTDOWN_TIMEOUT_SECS"] => {
                if let Ok(val) = v.parse::<u64>() {
                    node_config.runtime.shutdown_timeout_secs = val;
                    config.cluster.shutdown_timeout_secs = val;
                    origins.insert(
                        "runtime.shutdown_timeout_secs".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "cluster.shutdown_timeout_secs".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["RUNTIME", "MIN_EPOCH_MS"] => {
                if let Ok(val) = v.parse::<u64>() {
                    node_config.runtime.min_epoch_ms = val;
                    config.cluster.min_epoch_ms = val;
                    origins.insert(
                        "runtime.min_epoch_ms".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "cluster.min_epoch_ms".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["RUNTIME", "CHECKPOINT_RETENTION_COUNT"] => {
                if let Ok(val) = v.parse::<u32>() {
                    node_config.runtime.checkpoint_retention_count = val;
                    config.cluster.checkpoint_retention_count = val;
                    origins.insert(
                        "runtime.checkpoint_retention_count".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "cluster.checkpoint_retention_count".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["RUNTIME", "STATE_BUDGET_GB"] => {
                if let Ok(val) = v.parse::<u64>() {
                    node_config.runtime.state_budget_gb = val;
                    config.cluster.state_budget_gb = val;
                    origins.insert(
                        "runtime.state_budget_gb".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "cluster.state_budget_gb".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "MIN_EPOCH_MS"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.cluster.min_epoch_ms = val;
                    node_config.runtime.min_epoch_ms = val;
                    origins.insert(
                        "cluster.min_epoch_ms".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "runtime.min_epoch_ms".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "CHECKPOINT_RETENTION_COUNT"] => {
                if let Ok(val) = v.parse::<u32>() {
                    config.cluster.checkpoint_retention_count = val;
                    node_config.runtime.checkpoint_retention_count = val;
                    origins.insert(
                        "cluster.checkpoint_retention_count".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "runtime.checkpoint_retention_count".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "STATE_BUDGET_GB"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.cluster.state_budget_gb = val;
                    node_config.runtime.state_budget_gb = val;
                    origins.insert(
                        "cluster.state_budget_gb".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "runtime.state_budget_gb".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["CLUSTER", "SHUTDOWN_TIMEOUT_SECS"] => {
                if let Ok(val) = v.parse::<u64>() {
                    config.cluster.shutdown_timeout_secs = val;
                    node_config.runtime.shutdown_timeout_secs = val;
                    origins.insert(
                        "cluster.shutdown_timeout_secs".to_string(),
                        ConfigOrigin::Environment(k.clone()),
                    );
                    origins.insert(
                        "runtime.shutdown_timeout_secs".to_string(),
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
                    node_config.worker.segment_cache_bytes = val;
                    origins.insert(
                        "worker.segment_cache_bytes".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["WORKER", "MAX_ROWS_PER_QUANTUM"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.worker.max_rows_per_quantum = val;
                    node_config.worker.max_rows_per_quantum = val;
                    origins.insert(
                        "worker.max_rows_per_quantum".to_string(),
                        ConfigOrigin::Environment(k),
                    );
                }
            }
            ["WORKER", "EXECUTION_THREADS"] => {
                if let Ok(val) = v.parse::<usize>() {
                    config.worker.execution_threads = val;
                    node_config.worker.execution_threads = val;
                    origins.insert(
                        "worker.execution_threads".to_string(),
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
            ["EXECUTION", "JOIN_STRATEGY"] => {
                if let Ok(val) = toml::from_str::<crate::config::JoinStrategy>(&format!("\"{v}\""))
                {
                    config.execution.join_strategy = val;
                    origins.insert(
                        "execution.join_strategy".to_string(),
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
    node_config: &mut NodeConfig,
    cli: &CliConfigOverrides,
    origins: &mut BTreeMap<String, ConfigOrigin>,
) {
    if let Some(ref val) = cli.role {
        node_config.node.role = val.clone();
        origins.insert(
            "node.role".to_string(),
            ConfigOrigin::Cli("--role".to_string()),
        );
    }
    if let Some(ref val) = cli.host_id {
        node_config.node.host_id = Some(val.clone());
        origins.insert(
            "node.host_id".to_string(),
            ConfigOrigin::Cli("--host-id".to_string()),
        );
    }
    if let Some(ref val) = cli.availability_zone {
        node_config.node.availability_zone = Some(val.clone());
        origins.insert(
            "node.availability_zone".to_string(),
            ConfigOrigin::Cli("--availability-zone".to_string()),
        );
    }
    if let Some(ref val) = cli.listen_addr {
        node_config.gateway.listen_addr = val.clone();
        origins.insert(
            "gateway.listen_addr".to_string(),
            ConfigOrigin::Cli("--listen".to_string()),
        );
    }
    if let Some(val) = cli.gateway_max_connections {
        node_config.gateway.max_connections = val;
        origins.insert(
            "gateway.max_connections".to_string(),
            ConfigOrigin::Cli("--gateway-max-connections".to_string()),
        );
    }
    if let Some(val) = cli.gateway_query_timeout_secs {
        node_config.gateway.query_timeout_secs = val;
        origins.insert(
            "gateway.query_timeout_secs".to_string(),
            ConfigOrigin::Cli("--gateway-query-timeout-secs".to_string()),
        );
    }
    if let Some(ref val) = cli.control_bind {
        node_config.control.listen_addr = Some(val.clone());
        origins.insert(
            "control.listen_addr".to_string(),
            ConfigOrigin::Cli("--control-bind".to_string()),
        );
    }
    if let Some(ref val) = cli.control_url {
        node_config.control.url = Some(val.clone());
        origins.insert(
            "control.url".to_string(),
            ConfigOrigin::Cli("--control".to_string()),
        );
    }
    if let Some(ref val) = cli.control_shared_storage {
        node_config.control.shared_storage = Some(val.clone());
        origins.insert(
            "control.shared_storage".to_string(),
            ConfigOrigin::Cli("--control-shared-storage".to_string()),
        );
    }
    if let Some(val) = cli.worker_id {
        node_config.worker.worker_id = Some(val);
        origins.insert(
            "worker.worker_id".to_string(),
            ConfigOrigin::Cli("--worker-id".to_string()),
        );
    }
    if let Some(val) = cli.worker_threads {
        node_config.worker.execution_threads = val;
        config.worker.execution_threads = val;
        origins.insert(
            "worker.execution_threads".to_string(),
            ConfigOrigin::Cli("--worker-threads".to_string()),
        );
    }
    if let Some(val) = cli.worker_cache_bytes {
        node_config.worker.segment_cache_bytes = val;
        config.worker.segment_cache_bytes = val;
        origins.insert(
            "worker.segment_cache_bytes".to_string(),
            ConfigOrigin::Cli("--worker-cache-bytes".to_string()),
        );
    }
    if let Some(val) = cli.worker_quantum {
        node_config.worker.max_rows_per_quantum = val;
        config.worker.max_rows_per_quantum = val;
        origins.insert(
            "worker.max_rows_per_quantum".to_string(),
            ConfigOrigin::Cli("--worker-quantum".to_string()),
        );
    }
    if let Some(ref val) = cli.storage_url {
        if let Ok(u) = StorageUrl::parse(val) {
            node_config.storage.url = u;
            origins.insert(
                "storage.url".to_string(),
                ConfigOrigin::Cli("--storage".to_string()),
            );
        }
    }
    if let Some(ref val) = cli.storage_temp_dir {
        node_config.storage.temp_dir = Some(val.clone());
        origins.insert(
            "storage.temp_dir".to_string(),
            ConfigOrigin::Cli("--storage-temp-dir".to_string()),
        );
    }
    if let Some(ref val) = cli.storage_spill_dir {
        node_config.storage.spill_dir = Some(val.clone());
        origins.insert(
            "storage.spill_dir".to_string(),
            ConfigOrigin::Cli("--storage-spill-dir".to_string()),
        );
    }
    if let Some(ref val) = cli.metrics_addr {
        node_config.metrics.listen_addr = val.clone();
        origins.insert(
            "metrics.listen_addr".to_string(),
            ConfigOrigin::Cli("--metrics-addr".to_string()),
        );
    }
    if let Some(val) = cli.metrics_enabled {
        node_config.metrics.enabled = val;
        origins.insert(
            "metrics.enabled".to_string(),
            ConfigOrigin::Cli("--metrics-enabled".to_string()),
        );
    }
    if let Some(ref val) = cli.auth_mode {
        node_config.auth.mode = val.clone();
        origins.insert(
            "auth.mode".to_string(),
            ConfigOrigin::Cli("--auth".to_string()),
        );
    }
    if let Some(ref val) = cli.auth_secret_path {
        node_config.auth.secret_path = Some(val.clone());
        origins.insert(
            "auth.secret_path".to_string(),
            ConfigOrigin::Cli("--auth-secret-path".to_string()),
        );
    }
    if let Some(ref val) = cli.log_level {
        node_config.logging.level = val.clone();
        origins.insert(
            "logging.level".to_string(),
            ConfigOrigin::Cli("--log-level".to_string()),
        );
    }
    if let Some(ref val) = cli.log_format {
        node_config.logging.format = val.clone();
        origins.insert(
            "logging.format".to_string(),
            ConfigOrigin::Cli("--log-format".to_string()),
        );
    }
    if let Some(val) = cli.min_epoch_ms {
        config.cluster.min_epoch_ms = val;
        node_config.runtime.min_epoch_ms = val;
        origins.insert(
            "cluster.min_epoch_ms".to_string(),
            ConfigOrigin::Cli("--min-epoch-ms".to_string()),
        );
        origins.insert(
            "runtime.min_epoch_ms".to_string(),
            ConfigOrigin::Cli("--min-epoch-ms".to_string()),
        );
    }
    if let Some(val) = cli.checkpoint_retention_count {
        config.cluster.checkpoint_retention_count = val;
        node_config.runtime.checkpoint_retention_count = val;
        origins.insert(
            "cluster.checkpoint_retention_count".to_string(),
            ConfigOrigin::Cli("--checkpoint-retention-count".to_string()),
        );
        origins.insert(
            "runtime.checkpoint_retention_count".to_string(),
            ConfigOrigin::Cli("--checkpoint-retention-count".to_string()),
        );
    }
    if let Some(val) = cli.state_budget_gb {
        config.cluster.state_budget_gb = val;
        node_config.runtime.state_budget_gb = val;
        origins.insert(
            "cluster.state_budget_gb".to_string(),
            ConfigOrigin::Cli("--state-budget-gb".to_string()),
        );
        origins.insert(
            "runtime.state_budget_gb".to_string(),
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
        node_config.gateway.webhook_listen_addr = Some(val.clone());
        origins.insert(
            "gateway.webhook_listen_addr".to_string(),
            ConfigOrigin::Cli("--webhook-listen".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_cert_path {
        config.gateway.tls_cert_path = Some(val.clone());
        node_config.gateway.tls.cert_path = Some(val.clone());
        origins.insert(
            "gateway.tls_cert_path".to_string(),
            ConfigOrigin::Cli("--tls-cert-path".to_string()),
        );
        origins.insert(
            "gateway.tls.cert_path".to_string(),
            ConfigOrigin::Cli("--tls-cert-path".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_key_path {
        config.gateway.tls_key_path = Some(val.clone());
        node_config.gateway.tls.key_path = Some(val.clone());
        origins.insert(
            "gateway.tls_key_path".to_string(),
            ConfigOrigin::Cli("--tls-key-path".to_string()),
        );
        origins.insert(
            "gateway.tls.key_path".to_string(),
            ConfigOrigin::Cli("--tls-key-path".to_string()),
        );
    }
    if let Some(ref val) = cli.tls_ca_cert_path {
        config.gateway.tls_ca_cert_path = Some(val.clone());
        node_config.gateway.tls.ca_cert_path = Some(val.clone());
        origins.insert(
            "gateway.tls_ca_cert_path".to_string(),
            ConfigOrigin::Cli("--tls-ca-cert-path".to_string()),
        );
        origins.insert(
            "gateway.tls.ca_cert_path".to_string(),
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
    if let Some(val) = cli.shutdown_timeout_secs {
        config.cluster.shutdown_timeout_secs = val;
        node_config.runtime.shutdown_timeout_secs = val;
        origins.insert(
            "cluster.shutdown_timeout_secs".to_string(),
            ConfigOrigin::Cli("--shutdown-timeout-secs".to_string()),
        );
        origins.insert(
            "runtime.shutdown_timeout_secs".to_string(),
            ConfigOrigin::Cli("--shutdown-timeout-secs".to_string()),
        );
    }
}
