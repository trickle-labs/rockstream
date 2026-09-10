//! Multi-layer semantic configuration validation and unknown-key reporting for RockStream.
//!
//! Enforces:
//! - Layer 1: TOML syntax validation with exact line/column reporting.
//! - Layer 2: Unknown and deprecated key detection with dotted path reporting and typo suggestions.
//! - Layer 3: Semantic bounds validation across all configuration sections.
//! - Layer 4: Referenced file accessibility validation (`check_files`).

use serde::{Deserialize, Serialize};

use crate::config::RockstreamConfig;

/// Severity level for a configuration diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDiagnosticSeverity {
    Warning,
    Error,
}

impl std::fmt::Display for ConfigDiagnosticSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warning => write!(f, "WARN"),
            Self::Error => write!(f, "ERROR"),
        }
    }
}

/// A diagnostic emitted during configuration validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiagnostic {
    pub path: String,
    pub severity: ConfigDiagnosticSeverity,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<usize>,
}

impl std::fmt::Display for ConfigDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let loc = match (self.line, self.column) {
            (Some(l), Some(c)) => format!(" [line {l}, col {c}]"),
            (Some(l), None) => format!(" [line {l}]"),
            _ => String::new(),
        };
        let sugg = match &self.suggestion {
            Some(s) => format!(" (suggestion: {s})"),
            None => String::new(),
        };
        write!(
            f,
            "[{}] {} ({}){loc}: {}{sugg}",
            self.severity, self.path, self.code, self.message
        )
    }
}

/// Report produced by validating a Rockstream configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidationReport {
    pub valid: bool,
    pub diagnostics: Vec<ConfigDiagnostic>,
}

impl ConfigValidationReport {
    pub fn success() -> Self {
        Self {
            valid: true,
            diagnostics: Vec::new(),
        }
    }

    pub fn to_text(&self) -> String {
        if self.diagnostics.is_empty() {
            return "Configuration is valid.".to_string();
        }
        let mut lines = Vec::new();
        for d in &self.diagnostics {
            lines.push(d.to_string());
        }
        lines.join("\n")
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();
    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }
    let mut v0: Vec<usize> = (0..=b_len).collect();
    let mut v1: Vec<usize> = vec![0; b_len + 1];
    for (i, ca) in a.chars().enumerate() {
        v1[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            v1[j + 1] = std::cmp::min(std::cmp::min(v1[j] + 1, v0[j + 1] + 1), v0[j] + cost);
        }
        v0.copy_from_slice(&v1);
    }
    v0[b_len]
}

fn find_closest_match<'a>(target: &str, candidates: &'a [&'a str]) -> Option<&'a str> {
    let mut best = None;
    let mut best_dist = usize::MAX;
    for &cand in candidates {
        let dist = levenshtein(target, cand);
        let base_cand = cand.split('_').next().unwrap_or(cand);
        let base_dist = levenshtein(target, base_cand);
        let effective_dist = dist.min(base_dist + 1);

        if effective_dist < best_dist && effective_dist <= 3 {
            best_dist = effective_dist;
            best = Some(cand);
        }
    }
    best
}

// Known keys registry
const KNOWN_TOP_LEVEL_TABLES: &[&str] = &[
    "cluster",
    "worker",
    "connector",
    "exchange",
    "storage",
    "execution",
    "pricing",
    "gateway",
    "internal_tls",
    "autotuner",
    "skew_split",
    "scatter_pruning",
];

const KNOWN_TOP_LEVEL_SCALARS: &[&str] = &["recursion_max_iterations"];

const KNOWN_CLUSTER_KEYS: &[&str] = &[
    "min_epoch_ms",
    "checkpoint_retention_count",
    "state_budget_gb",
    "index_prefer_selectivity_threshold",
    "index_max_lag_ms",
    "shutdown_timeout_secs",
    "autotuner",
    "skew_split",
    "scatter_pruning",
];

const KNOWN_AUTOTUNER_KEYS: &[&str] = &[
    "enabled",
    "hysteresis_scale_up_windows",
    "hysteresis_scale_down_windows",
    "default_parallelism",
    "min_parallelism",
    "max_parallelism",
    "direct_compression_cpu_budget_ms",
    "compression_disable_hysteresis_windows",
    "compression_reenable_hysteresis_windows",
];

const KNOWN_SKEW_SPLIT_KEYS: &[&str] = &["enabled", "hot_key_factor", "max_skew_buckets"];

const KNOWN_SCATTER_PRUNING_KEYS: &[&str] = &[
    "shard_bloom_budget_bytes",
    "shard_stats_max_age_checkpoints",
];

const KNOWN_WORKER_KEYS: &[&str] = &[
    "segment_cache_bytes",
    "max_rows_per_quantum",
    "execution_threads",
];

const KNOWN_CONNECTOR_KEYS: &[&str] = &["dlq_warn_threshold", "dlq_retention_days"];

const KNOWN_EXCHANGE_KEYS: &[&str] = &[
    "exchange_direct_threshold_bytes",
    "exchange_spill_threshold_mb",
    "exchange_domain_size",
    "exchange_force_durable",
    "same_host_shm_segment_bytes",
    "same_host_shm_segments_per_peer",
    "max_exchange_compression_states",
    "connect_timeout_ms",
    "rpc_timeout_ms",
    "max_retries",
    "backoff_jitter_ms",
    "frame_channel_capacity",
];

const KNOWN_PRICING_KEYS: &[&str] = &[
    "object_store_request_per_1k",
    "object_store_standard_gb_month",
    "object_store_standard_ia_gb_month",
    "object_store_egress_gb",
    "compute_on_demand_core_hour",
    "compute_spot_core_hour",
    "compute_spot_mix",
];

const KNOWN_GATEWAY_KEYS: &[&str] = &[
    "webhook_listen_addr",
    "tls_cert_path",
    "tls_key_path",
    "tls_ca_cert_path",
];

const KNOWN_INTERNAL_TLS_KEYS: &[&str] = &[
    "cert_path",
    "key_path",
    "ca_cert_path",
    "client_auth_required",
    "reload_enabled",
];

const KNOWN_STORAGE_KEYS: &[&str] = &["tiering"];

const KNOWN_EXECUTION_KEYS: &[&str] = &["join_strategy"];

const DEPRECATED_TIERING_KEYS: &[&str] = &[
    "shard_meta_backend",
    "cold_sst_backend",
    "cold_sst_age_threshold",
];

const KNOWN_NODE_CONFIG_ROOT_KEYS: &[&str] = &[
    "version", "node", "gateway", "control", "worker", "storage", "metrics", "auth", "logging",
    "runtime",
];

const KNOWN_NODE_SECTION_KEYS: &[&str] = &["role", "host_id", "availability_zone"];
const KNOWN_GATEWAY_SECTION_KEYS: &[&str] = &[
    "listen_addr",
    "max_connections",
    "query_timeout_secs",
    "tls",
    "webhook_listen_addr",
];
const KNOWN_GATEWAY_TLS_KEYS: &[&str] = &["cert_path", "key_path", "ca_cert_path"];
const KNOWN_CONTROL_SECTION_KEYS: &[&str] = &["listen_addr", "url", "shared_storage", "raft"];
const KNOWN_CONTROL_RAFT_KEYS: &[&str] = &["peers", "node_id", "bind", "bootstrap"];
const KNOWN_WORKER_SECTION_KEYS: &[&str] = &[
    "worker_id",
    "execution_threads",
    "segment_cache_bytes",
    "max_rows_per_quantum",
    "capabilities",
];
const KNOWN_STORAGE_SECTION_KEYS: &[&str] = &["url", "temp_dir", "spill_dir", "tiering"];
const KNOWN_METRICS_SECTION_KEYS: &[&str] = &["listen_addr", "enabled", "scrape_interval_secs"];
const KNOWN_AUTH_SECTION_KEYS: &[&str] = &["mode", "secret_path", "admin_user"];
const KNOWN_LOGGING_SECTION_KEYS: &[&str] = &["level", "format"];
const KNOWN_RUNTIME_SECTION_KEYS: &[&str] = &[
    "shutdown_timeout_secs",
    "min_epoch_ms",
    "checkpoint_retention_count",
    "state_budget_gb",
];

fn find_line_col_for_key(toml_str: &str, key: &str) -> (Option<usize>, Option<usize>) {
    for (line_idx, line) in toml_str.lines().enumerate() {
        if let Some(col_idx) = line.find(key) {
            return (Some(line_idx + 1), Some(col_idx + 1));
        }
    }
    (None, None)
}

fn validate_keys_in_table(
    table: &toml::Table,
    section_path: &str,
    known_keys: &[&str],
    toml_str: &str,
    diagnostics: &mut Vec<ConfigDiagnostic>,
) {
    for (k, v) in table {
        let full_path = if section_path.is_empty() {
            k.clone()
        } else {
            format!("{section_path}.{k}")
        };

        if !known_keys.contains(&k.as_str()) {
            let suggestion =
                find_closest_match(k, known_keys).map(|c| format!("Did you mean `{c}`?"));
            let (line, column) = find_line_col_for_key(toml_str, k);
            diagnostics.push(ConfigDiagnostic {
                path: full_path.clone(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0002".to_string(),
                message: format!(
                    "Unknown key `{k}` in section `{}`",
                    if section_path.is_empty() {
                        "root"
                    } else {
                        section_path
                    }
                ),
                suggestion,
                line,
                column,
            });
        } else if let toml::Value::Table(sub) = v {
            match full_path.as_str() {
                "cluster" => validate_keys_in_table(
                    sub,
                    "cluster",
                    KNOWN_CLUSTER_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "cluster.autotuner" | "autotuner" => validate_keys_in_table(
                    sub,
                    &full_path,
                    KNOWN_AUTOTUNER_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "cluster.skew_split" | "skew_split" => validate_keys_in_table(
                    sub,
                    &full_path,
                    KNOWN_SKEW_SPLIT_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "cluster.scatter_pruning" | "scatter_pruning" => validate_keys_in_table(
                    sub,
                    &full_path,
                    KNOWN_SCATTER_PRUNING_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "worker" => {
                    validate_keys_in_table(sub, "worker", KNOWN_WORKER_KEYS, toml_str, diagnostics)
                }
                "connector" => validate_keys_in_table(
                    sub,
                    "connector",
                    KNOWN_CONNECTOR_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "exchange" => validate_keys_in_table(
                    sub,
                    "exchange",
                    KNOWN_EXCHANGE_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "pricing" => validate_keys_in_table(
                    sub,
                    "pricing",
                    KNOWN_PRICING_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "gateway" => validate_keys_in_table(
                    sub,
                    "gateway",
                    KNOWN_GATEWAY_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "internal_tls" => validate_keys_in_table(
                    sub,
                    "internal_tls",
                    KNOWN_INTERNAL_TLS_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "storage" => validate_keys_in_table(
                    sub,
                    "storage",
                    KNOWN_STORAGE_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "execution" => validate_keys_in_table(
                    sub,
                    "execution",
                    KNOWN_EXECUTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "storage.tiering" => {
                    for (tk, _) in sub {
                        let t_path = format!("storage.tiering.{tk}");
                        let (line, column) = find_line_col_for_key(toml_str, tk);
                        if DEPRECATED_TIERING_KEYS.contains(&tk.as_str()) {
                            diagnostics.push(ConfigDiagnostic {
                                path: t_path,
                                severity: ConfigDiagnosticSeverity::Error,
                                code: "RS-4017".to_string(),
                                message: "Storage tiering is removed. Refer to migration docs."
                                    .to_string(),
                                suggestion: None,
                                line,
                                column,
                            });
                        } else {
                            diagnostics.push(ConfigDiagnostic {
                                path: t_path,
                                severity: ConfigDiagnosticSeverity::Error,
                                code: "RS-0002".to_string(),
                                message: format!("Unknown key `{tk}` in storage.tiering"),
                                suggestion: None,
                                line,
                                column,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn validate_node_config_keys(
    table: &toml::Table,
    section_path: &str,
    known_keys: &[&str],
    toml_str: &str,
    diagnostics: &mut Vec<ConfigDiagnostic>,
) {
    for (k, v) in table {
        let full_path = if section_path.is_empty() {
            k.clone()
        } else {
            format!("{section_path}.{k}")
        };

        if !known_keys.contains(&k.as_str()) {
            let suggestion =
                find_closest_match(k, known_keys).map(|c| format!("Did you mean `{c}`?"));
            let (line, column) = find_line_col_for_key(toml_str, k);
            diagnostics.push(ConfigDiagnostic {
                path: full_path.clone(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0002".to_string(),
                message: format!(
                    "Unknown key `{k}` in section `{}`",
                    if section_path.is_empty() {
                        "root"
                    } else {
                        section_path
                    }
                ),
                suggestion,
                line,
                column,
            });
        } else if let toml::Value::Table(sub) = v {
            match full_path.as_str() {
                "node" => validate_node_config_keys(
                    sub,
                    "node",
                    KNOWN_NODE_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "gateway" => validate_node_config_keys(
                    sub,
                    "gateway",
                    KNOWN_GATEWAY_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "gateway.tls" => validate_node_config_keys(
                    sub,
                    "gateway.tls",
                    KNOWN_GATEWAY_TLS_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "control" => validate_node_config_keys(
                    sub,
                    "control",
                    KNOWN_CONTROL_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "control.raft" => validate_node_config_keys(
                    sub,
                    "control.raft",
                    KNOWN_CONTROL_RAFT_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "worker" => validate_node_config_keys(
                    sub,
                    "worker",
                    KNOWN_WORKER_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "storage" => validate_node_config_keys(
                    sub,
                    "storage",
                    KNOWN_STORAGE_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "metrics" => validate_node_config_keys(
                    sub,
                    "metrics",
                    KNOWN_METRICS_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "auth" => validate_node_config_keys(
                    sub,
                    "auth",
                    KNOWN_AUTH_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "logging" => validate_node_config_keys(
                    sub,
                    "logging",
                    KNOWN_LOGGING_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                "runtime" => validate_node_config_keys(
                    sub,
                    "runtime",
                    KNOWN_RUNTIME_SECTION_KEYS,
                    toml_str,
                    diagnostics,
                ),
                _ => {}
            }
        }
    }
}

/// Validate configuration string through all validation layers.
pub fn validate_config_str(toml_str: &str, check_files: bool) -> ConfigValidationReport {
    let mut diagnostics = Vec::new();

    // Layer 1: TOML Syntax
    let parsed_val: toml::Value = match toml::from_str(toml_str) {
        Ok(v) => v,
        Err(e) => {
            let (line, col) = match e.span() {
                Some(span) => {
                    let mut line_num = 1;
                    let mut col_num = 1;
                    for (idx, ch) in toml_str.chars().enumerate() {
                        if idx >= span.start {
                            break;
                        }
                        if ch == '\n' {
                            line_num += 1;
                            col_num = 1;
                        } else {
                            col_num += 1;
                        }
                    }
                    (Some(line_num), Some(col_num))
                }
                None => (None, None),
            };
            diagnostics.push(ConfigDiagnostic {
                path: "syntax".to_string(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0002".to_string(),
                message: format!("TOML syntax error: {e}"),
                suggestion: None,
                line,
                column: col,
            });
            return ConfigValidationReport {
                valid: false,
                diagnostics,
            };
        }
    };

    let root_table = match parsed_val {
        toml::Value::Table(tbl) => tbl,
        _ => {
            diagnostics.push(ConfigDiagnostic {
                path: "root".to_string(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0002".to_string(),
                message: "Configuration root must be a table".to_string(),
                suggestion: None,
                line: None,
                column: None,
            });
            return ConfigValidationReport {
                valid: false,
                diagnostics,
            };
        }
    };

    let has_version = root_table.contains_key("version");
    let has_node_sections = root_table.contains_key("node")
        || root_table.contains_key("control")
        || root_table.contains_key("runtime")
        || (root_table.contains_key("storage")
            && root_table
                .get("storage")
                .and_then(|v| v.as_table())
                .map(|t| t.contains_key("url"))
                .unwrap_or(false));

    let is_node_config = has_version || has_node_sections;

    if is_node_config {
        // Layer 2: Version validation
        if let Some(v) = root_table.get("version") {
            let ver_val = match v {
                toml::Value::Integer(i) => *i,
                _ => -1,
            };
            if ver_val != 1 {
                let (line, column) = find_line_col_for_key(toml_str, "version");
                diagnostics.push(ConfigDiagnostic {
                    path: "version".to_string(),
                    severity: ConfigDiagnosticSeverity::Error,
                    code: "RS-0002".to_string(),
                    message: format!(
                        "unsupported configuration version {ver_val} (expected version = 1)"
                    ),
                    suggestion: Some("Set `version = 1`".to_string()),
                    line,
                    column,
                });
            }
        }

        // Layer 2: Unknown keys in NodeConfig
        validate_node_config_keys(
            &root_table,
            "",
            KNOWN_NODE_CONFIG_ROOT_KEYS,
            toml_str,
            &mut diagnostics,
        );

        // Layer 3: Semantic Bounds Validation
        match crate::config::NodeConfig::load_from_str(toml_str) {
            Ok(node_cfg) => {
                validate_node_config_semantic_bounds(&node_cfg, check_files, &mut diagnostics);
            }
            Err(e) => {
                if diagnostics.is_empty() {
                    diagnostics.push(ConfigDiagnostic {
                        path: "config".to_string(),
                        severity: ConfigDiagnosticSeverity::Error,
                        code: "RS-0002".to_string(),
                        message: format!("Failed to parse config into NodeConfig: {e}"),
                        suggestion: None,
                        line: None,
                        column: None,
                    });
                }
            }
        }
    } else {
        // Legacy RockstreamConfig validation
        let mut top_known = Vec::new();
        top_known.extend_from_slice(KNOWN_TOP_LEVEL_TABLES);
        top_known.extend_from_slice(KNOWN_TOP_LEVEL_SCALARS);
        validate_keys_in_table(&root_table, "", &top_known, toml_str, &mut diagnostics);

        match toml::from_str::<RockstreamConfig>(toml_str) {
            Ok(config) => {
                validate_semantic_bounds(&config, check_files, &mut diagnostics);
            }
            Err(e) => {
                if diagnostics.is_empty() {
                    diagnostics.push(ConfigDiagnostic {
                        path: "config".to_string(),
                        severity: ConfigDiagnosticSeverity::Error,
                        code: "RS-0002".to_string(),
                        message: format!("Failed to parse config into RockstreamConfig: {e}"),
                        suggestion: None,
                        line: None,
                        column: None,
                    });
                }
            }
        }
    }

    // Deterministic sort by path, then message
    diagnostics.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.message.cmp(&b.message)));

    let valid = diagnostics
        .iter()
        .all(|d| d.severity != ConfigDiagnosticSeverity::Error);
    ConfigValidationReport { valid, diagnostics }
}

/// Validate semantic bounds on an already-parsed `RockstreamConfig`.
pub fn validate_semantic_bounds(
    config: &RockstreamConfig,
    check_files: bool,
    diagnostics: &mut Vec<ConfigDiagnostic>,
) {
    // Cluster bounds
    if config.cluster.min_epoch_ms == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.min_epoch_ms".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "cluster.min_epoch_ms must be greater than 0".to_string(),
            suggestion: Some("Set min_epoch_ms >= 1 (default 10)".to_string()),
            line: None,
            column: None,
        });
    }

    if config.cluster.checkpoint_retention_count == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.checkpoint_retention_count".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "cluster.checkpoint_retention_count must be greater than 0".to_string(),
            suggestion: Some("Set checkpoint_retention_count >= 1 (default 5 or 128)".to_string()),
            line: None,
            column: None,
        });
    }

    if config.cluster.state_budget_gb == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.state_budget_gb".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "cluster.state_budget_gb must be greater than 0".to_string(),
            suggestion: Some("Set state_budget_gb >= 1".to_string()),
            line: None,
            column: None,
        });
    }

    if config.cluster.index_prefer_selectivity_threshold <= 0.0
        || config.cluster.index_prefer_selectivity_threshold > 1.0
    {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.index_prefer_selectivity_threshold".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "Selectivity threshold must be in (0.0, 1.0]".to_string(),
            suggestion: Some(
                "Set index_prefer_selectivity_threshold between 0.01 and 1.0".to_string(),
            ),
            line: None,
            column: None,
        });
    }

    // Autotuner bounds
    let auto = &config.cluster.autotuner;
    if auto.min_parallelism == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.autotuner.min_parallelism".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "autotuner.min_parallelism must be greater than 0".to_string(),
            suggestion: Some("Set min_parallelism >= 1".to_string()),
            line: None,
            column: None,
        });
    }
    if auto.min_parallelism > auto.default_parallelism
        || auto.default_parallelism > auto.max_parallelism
    {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.autotuner.parallelism".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message:
                "Constraint min_parallelism <= default_parallelism <= max_parallelism violated"
                    .to_string(),
            suggestion: Some("Adjust min, default, and max parallelism values".to_string()),
            line: None,
            column: None,
        });
    }

    // Skew split bounds
    let skew = &config.cluster.skew_split;
    if skew.hot_key_factor <= 1.0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.skew_split.hot_key_factor".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "skew_split.hot_key_factor must be greater than 1.0".to_string(),
            suggestion: Some("Set hot_key_factor > 1.0 (default 20.0)".to_string()),
            line: None,
            column: None,
        });
    }
    if skew.max_skew_buckets == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "cluster.skew_split.max_skew_buckets".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "skew_split.max_skew_buckets must be greater than 0".to_string(),
            suggestion: Some("Set max_skew_buckets >= 1 (default 16)".to_string()),
            line: None,
            column: None,
        });
    }

    // Worker bounds
    if config.worker.segment_cache_bytes == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.segment_cache_bytes".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.segment_cache_bytes must be greater than 0".to_string(),
            suggestion: Some("Set segment_cache_bytes > 0 (e.g. 67108864 for 64MB)".to_string()),
            line: None,
            column: None,
        });
    }
    if config.worker.max_rows_per_quantum == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.max_rows_per_quantum".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.max_rows_per_quantum must be greater than 0".to_string(),
            suggestion: Some("Set max_rows_per_quantum > 0 (default 8192 or 1000)".to_string()),
            line: None,
            column: None,
        });
    }
    if config.worker.execution_threads == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.execution_threads".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.execution_threads must be greater than 0".to_string(),
            suggestion: Some("Set execution_threads >= 1 (default 1)".to_string()),
            line: None,
            column: None,
        });
    }

    // Exchange bounds
    let spill_threshold_bytes = config.exchange.exchange_spill_threshold_mb as usize * 1024 * 1024;
    if config.exchange.exchange_direct_threshold_bytes > spill_threshold_bytes {
        diagnostics.push(ConfigDiagnostic {
            path: "exchange.thresholds".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "exchange_direct_threshold_bytes cannot exceed exchange_spill_threshold_mb"
                .to_string(),
            suggestion: Some("Set direct_threshold <= spill_threshold".to_string()),
            line: None,
            column: None,
        });
    }

    // Gateway TLS pair check
    let gw_cert = config.gateway.tls_cert_path.is_some();
    let gw_key = config.gateway.tls_key_path.is_some();
    if gw_cert != gw_key {
        diagnostics.push(ConfigDiagnostic {
            path: "gateway.tls".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "Gateway TLS requires both `tls_cert_path` and `tls_key_path`".to_string(),
            suggestion: Some(
                "Provide both certificate and private key paths or neither".to_string(),
            ),
            line: None,
            column: None,
        });
    }

    // Internal TLS triplet check
    let int_cert = config.internal_tls.cert_path.is_some();
    let int_key = config.internal_tls.key_path.is_some();
    let int_ca = config.internal_tls.ca_cert_path.is_some();
    if (int_cert || int_key || int_ca) && !(int_cert && int_key && int_ca) {
        diagnostics.push(ConfigDiagnostic {
            path: "internal_tls".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "Internal TLS requires `cert_path`, `key_path`, and `ca_cert_path`"
                .to_string(),
            suggestion: Some("Provide cert, key, and CA certificate paths together".to_string()),
            line: None,
            column: None,
        });
    }

    // Storage tiering check
    if config.storage.tiering.shard_meta_backend.is_some()
        || config.storage.tiering.cold_sst_backend.is_some()
        || config.storage.tiering.cold_sst_age_threshold.is_some()
    {
        diagnostics.push(ConfigDiagnostic {
            path: "storage.tiering".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-4017".to_string(),
            message: "Storage tiering is removed. Refer to migration docs.".to_string(),
            suggestion: None,
            line: None,
            column: None,
        });
    }

    // Layer 4: Check file paths if requested
    if check_files {
        let mut check_file = |path_opt: &Option<std::path::PathBuf>, path_name: &str| {
            if let Some(p) = path_opt {
                if !p.exists() {
                    diagnostics.push(ConfigDiagnostic {
                        path: path_name.to_string(),
                        severity: ConfigDiagnosticSeverity::Error,
                        code: "RS-0002".to_string(),
                        message: format!("Referenced file does not exist: {}", p.display()),
                        suggestion: Some("Check file path and existence".to_string()),
                        line: None,
                        column: None,
                    });
                }
            }
        };

        check_file(&config.gateway.tls_cert_path, "gateway.tls_cert_path");
        check_file(&config.gateway.tls_key_path, "gateway.tls_key_path");
        check_file(&config.gateway.tls_ca_cert_path, "gateway.tls_ca_cert_path");
        check_file(&config.internal_tls.cert_path, "internal_tls.cert_path");
        check_file(&config.internal_tls.key_path, "internal_tls.key_path");
        check_file(
            &config.internal_tls.ca_cert_path,
            "internal_tls.ca_cert_path",
        );
    }
}

/// Validate semantic bounds on a `NodeConfig`.
pub fn validate_node_config_semantic_bounds(
    node_cfg: &crate::config::NodeConfig,
    check_files: bool,
    diagnostics: &mut Vec<ConfigDiagnostic>,
) {
    if node_cfg.version != 1 {
        diagnostics.push(ConfigDiagnostic {
            path: "version".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!(
                "unsupported configuration version {} (expected version = 1)",
                node_cfg.version
            ),
            suggestion: Some("Set `version = 1`".to_string()),
            line: None,
            column: None,
        });
    }

    let role_norm = node_cfg.node.role.trim().to_lowercase();
    let valid_roles = ["all", "control", "worker", "gateway", "metrics", "frontier"];
    if !valid_roles.contains(&role_norm.as_str()) {
        diagnostics.push(ConfigDiagnostic {
            path: "node.role".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!(
                "unknown role `{}`; supported: all, control, worker, gateway, metrics",
                node_cfg.node.role
            ),
            suggestion: Some("Set `role = \"all\"` or one of the supported roles".to_string()),
            line: None,
            column: None,
        });
    }

    if (role_norm == "worker" || role_norm == "frontier") && node_cfg.control.url.is_none() {
        diagnostics.push(ConfigDiagnostic {
            path: "node.role".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!("role `{}` requires `control.url`", node_cfg.node.role),
            suggestion: Some(
                "Set `control.url` in [control] section or pass --control=<url>".to_string(),
            ),
            line: None,
            column: None,
        });
    }

    // gateway listen_addr
    if node_cfg
        .gateway
        .listen_addr
        .parse::<std::net::SocketAddr>()
        .is_err()
    {
        diagnostics.push(ConfigDiagnostic {
            path: "gateway.listen_addr".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!("invalid socket address `{}`", node_cfg.gateway.listen_addr),
            suggestion: Some("Specify a valid IP:port like `127.0.0.1:5432`".to_string()),
            line: None,
            column: None,
        });
    }

    // control listen_addr
    if let Some(ref addr) = node_cfg.control.listen_addr {
        if addr.parse::<std::net::SocketAddr>().is_err() {
            diagnostics.push(ConfigDiagnostic {
                path: "control.listen_addr".to_string(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0002".to_string(),
                message: format!("invalid socket address `{}`", addr),
                suggestion: Some("Specify a valid IP:port like `127.0.0.1:9200`".to_string()),
                line: None,
                column: None,
            });
        }
    }

    // metrics listen_addr
    if node_cfg
        .metrics
        .listen_addr
        .parse::<std::net::SocketAddr>()
        .is_err()
    {
        diagnostics.push(ConfigDiagnostic {
            path: "metrics.listen_addr".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!("invalid socket address `{}`", node_cfg.metrics.listen_addr),
            suggestion: Some("Specify a valid IP:port like `127.0.0.1:9090`".to_string()),
            line: None,
            column: None,
        });
    }

    // auth mode
    let auth_mode_norm = node_cfg.auth.mode.trim().to_lowercase();
    let valid_auth = ["off", "scram", "md5", "oidc", "mtls"];
    if !valid_auth.contains(&auth_mode_norm.as_str()) {
        diagnostics.push(ConfigDiagnostic {
            path: "auth.mode".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: format!(
                "unknown auth mode `{}`; supported: off, scram, md5, oidc, mtls",
                node_cfg.auth.mode
            ),
            suggestion: Some("Set auth mode to off, scram, md5, oidc, or mtls".to_string()),
            line: None,
            column: None,
        });
    }

    // storage bounds
    if check_files {
        if let Err(e) = node_cfg.storage.url.verify_accessible() {
            diagnostics.push(ConfigDiagnostic {
                path: "storage.url".to_string(),
                severity: ConfigDiagnosticSeverity::Error,
                code: "RS-0003".to_string(),
                message: e,
                suggestion: Some("Ensure storage directory exists and is writable".to_string()),
                line: None,
                column: None,
            });
        }
    }

    // worker bounds
    if node_cfg.worker.execution_threads == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.execution_threads".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.execution_threads must be greater than 0".to_string(),
            suggestion: Some("Set execution_threads >= 1".to_string()),
            line: None,
            column: None,
        });
    }

    if node_cfg.worker.segment_cache_bytes == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.segment_cache_bytes".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.segment_cache_bytes must be greater than 0".to_string(),
            suggestion: Some("Set segment_cache_bytes > 0".to_string()),
            line: None,
            column: None,
        });
    }

    if node_cfg.worker.max_rows_per_quantum == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "worker.max_rows_per_quantum".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "worker.max_rows_per_quantum must be greater than 0".to_string(),
            suggestion: Some("Set max_rows_per_quantum > 0".to_string()),
            line: None,
            column: None,
        });
    }

    // runtime bounds
    if node_cfg.runtime.shutdown_timeout_secs == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "runtime.shutdown_timeout_secs".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "runtime.shutdown_timeout_secs must be greater than 0".to_string(),
            suggestion: Some("Set shutdown_timeout_secs >= 1".to_string()),
            line: None,
            column: None,
        });
    }

    if node_cfg.runtime.min_epoch_ms == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "runtime.min_epoch_ms".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "runtime.min_epoch_ms must be greater than 0".to_string(),
            suggestion: Some("Set min_epoch_ms >= 1 (default 10)".to_string()),
            line: None,
            column: None,
        });
    }

    if node_cfg.runtime.checkpoint_retention_count == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "runtime.checkpoint_retention_count".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "runtime.checkpoint_retention_count must be greater than 0".to_string(),
            suggestion: Some("Set checkpoint_retention_count >= 1 (default 128)".to_string()),
            line: None,
            column: None,
        });
    }

    if node_cfg.runtime.state_budget_gb == 0 {
        diagnostics.push(ConfigDiagnostic {
            path: "runtime.state_budget_gb".to_string(),
            severity: ConfigDiagnosticSeverity::Error,
            code: "RS-0002".to_string(),
            message: "runtime.state_budget_gb must be greater than 0".to_string(),
            suggestion: Some("Set state_budget_gb >= 1".to_string()),
            line: None,
            column: None,
        });
    }

    if check_files {
        let mut check_file = |opt: &Option<std::path::PathBuf>, path: &str| {
            if let Some(ref p) = opt {
                if !p.exists() {
                    diagnostics.push(ConfigDiagnostic {
                        path: path.to_string(),
                        severity: ConfigDiagnosticSeverity::Error,
                        code: "RS-0002".to_string(),
                        message: format!("Referenced file does not exist: {}", p.display()),
                        suggestion: Some("Check file path and existence".to_string()),
                        line: None,
                        column: None,
                    });
                }
            }
        };

        check_file(&node_cfg.gateway.tls.cert_path, "gateway.tls.cert_path");
        check_file(&node_cfg.gateway.tls.key_path, "gateway.tls.key_path");
        check_file(
            &node_cfg.gateway.tls.ca_cert_path,
            "gateway.tls.ca_cert_path",
        );
        check_file(&node_cfg.auth.secret_path, "auth.secret_path");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_keys_are_validated_exactly() {
        assert_eq!(
            validate_config_str("[execution]\njoin_strategy = \"factorized\"\n", false),
            ConfigValidationReport::success()
        );
        assert_eq!(
            validate_config_str("[execution]\njoin_stratgey = \"factorized\"\n", false),
            ConfigValidationReport {
                valid: false,
                diagnostics: vec![ConfigDiagnostic {
                    path: "execution.join_stratgey".to_string(),
                    severity: ConfigDiagnosticSeverity::Error,
                    code: "RS-0002".to_string(),
                    message: "Unknown key `join_stratgey` in section `execution`".to_string(),
                    suggestion: Some("Did you mean `join_strategy`?".to_string()),
                    line: None,
                    column: None,
                }],
            }
        );
    }
}
