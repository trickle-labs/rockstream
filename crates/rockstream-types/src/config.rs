//! Configuration types for RockStream (v0.49).

use serde::{Deserialize, Serialize};

use crate::cost::PricingConfig;
use crate::tiering::StorageTieringConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AutotunerConfig {
    pub enabled: bool,
    pub hysteresis_scale_up_windows: usize,
    pub hysteresis_scale_down_windows: usize,
    pub default_parallelism: usize,
    pub min_parallelism: usize,
    pub max_parallelism: usize,
    pub direct_compression_cpu_budget_ms: u64,
    pub compression_disable_hysteresis_windows: usize,
    pub compression_reenable_hysteresis_windows: usize,
}

impl Default for AutotunerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hysteresis_scale_up_windows: 3,
            hysteresis_scale_down_windows: 12, // 4x K
            default_parallelism: 4,
            min_parallelism: 1,
            max_parallelism: 32,
            direct_compression_cpu_budget_ms: 5,
            compression_disable_hysteresis_windows: 2,
            compression_reenable_hysteresis_windows: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SkewSplitConfig {
    pub enabled: bool,
    pub hot_key_factor: f64,
    pub max_skew_buckets: u16,
}

impl Default for SkewSplitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hot_key_factor: 20.0,
            max_skew_buckets: 16,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ScatterPruningConfig {
    pub shard_bloom_budget_bytes: usize,
    pub shard_stats_max_age_checkpoints: u64,
}

impl Default for ScatterPruningConfig {
    fn default() -> Self {
        Self {
            shard_bloom_budget_bytes: 65_536,
            shard_stats_max_age_checkpoints: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TunerOverrides {
    pub parallelism: Option<usize>,
    pub epoch_size_ms: Option<u64>,
    pub memory_limit_mb: Option<u64>,
    pub skew_buckets: Option<u16>,
}

fn default_selectivity_threshold() -> f64 {
    0.01
}

fn default_max_lag_ms() -> u64 {
    1000
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ClusterConfig {
    pub min_epoch_ms: u64,
    pub checkpoint_retention_count: u32,
    pub state_budget_gb: u64,
    #[serde(default)]
    pub autotuner: AutotunerConfig,
    #[serde(default)]
    pub skew_split: SkewSplitConfig,
    #[serde(default)]
    pub scatter_pruning: ScatterPruningConfig,
    #[serde(default = "default_selectivity_threshold")]
    pub index_prefer_selectivity_threshold: f64,
    #[serde(default = "default_max_lag_ms")]
    pub index_max_lag_ms: u64,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            min_epoch_ms: 10,
            checkpoint_retention_count: 5,
            state_budget_gb: 16,
            autotuner: AutotunerConfig::default(),
            skew_split: SkewSplitConfig::default(),
            scatter_pruning: ScatterPruningConfig::default(),
            index_prefer_selectivity_threshold: 0.01,
            index_max_lag_ms: 1000,
            shutdown_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct WorkerConfig {
    pub segment_cache_bytes: usize,
    pub max_rows_per_quantum: usize,
    pub execution_threads: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            segment_cache_bytes: 64 * 1024 * 1024,
            max_rows_per_quantum: 8192,
            execution_threads: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectorConfig {
    pub dlq_warn_threshold: u32,
    pub dlq_retention_days: u32,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            dlq_warn_threshold: 100,
            dlq_retention_days: 7,
        }
    }
}

fn default_connect_timeout_ms() -> u64 {
    250
}

fn default_rpc_timeout_ms() -> u64 {
    10000
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_jitter_ms() -> u64 {
    100
}

fn default_frame_channel_capacity() -> usize {
    64
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ExchangeConfig {
    pub exchange_direct_threshold_bytes: usize,
    pub exchange_spill_threshold_mb: u64,
    pub exchange_domain_size: usize,
    pub exchange_force_durable: bool,
    pub same_host_shm_segment_bytes: usize,
    pub same_host_shm_segments_per_peer: usize,
    pub max_exchange_compression_states: usize,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_backoff_jitter_ms")]
    pub backoff_jitter_ms: u64,
    #[serde(default = "default_frame_channel_capacity")]
    pub frame_channel_capacity: usize,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            exchange_direct_threshold_bytes: 64 * 1024,
            exchange_spill_threshold_mb: 256,
            exchange_domain_size: 64,
            exchange_force_durable: false,
            same_host_shm_segment_bytes: 8 * 1024 * 1024,
            same_host_shm_segments_per_peer: 8,
            max_exchange_compression_states: 1024,
            connect_timeout_ms: 250,
            rpc_timeout_ms: 10000,
            max_retries: 3,
            backoff_jitter_ms: 100,
            frame_channel_capacity: 64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StorageConfig {
    #[serde(default)]
    pub tiering: StorageTieringConfig,
}

/// Join implementation selected when a view is compiled.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JoinStrategy {
    #[default]
    Auto,
    Classic,
    Factorized,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct ExecutionConfig {
    pub join_strategy: JoinStrategy,
}

/// v0.51.5: gateway-facing (client SQL-port) TLS termination configuration.
/// Distinct from any *internal* control<->worker/worker<->worker mTLS.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GatewayConfig {
    /// Optional independent listener for authenticated `POST /webhook/<source>`
    /// ingestion.  It must not share the pgwire port.
    #[serde(default)]
    pub webhook_listen_addr: Option<String>,
    /// Path to the PEM-encoded server certificate (chain) presented during
    /// the TLS handshake. `None` (the default) means TLS is not configured
    /// and the gateway keeps its pre-v0.51.5 plaintext-refusal `SSLRequest`
    /// behavior.
    #[serde(default)]
    pub tls_cert_path: Option<std::path::PathBuf>,
    /// Path to the PEM-encoded private key matching `tls_cert_path`.
    #[serde(default)]
    pub tls_key_path: Option<std::path::PathBuf>,
    /// Path to the PEM-encoded CA certificate used to validate client
    /// certificates for `--auth=mtls`. Required (fails fast at startup if
    /// missing) whenever `--auth=mtls` is set.
    #[serde(default)]
    pub tls_ca_cert_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RockstreamConfig {
    #[serde(default = "default_recursion_max_iterations")]
    pub recursion_max_iterations: usize,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    #[serde(default)]
    pub connector: ConnectorConfig,
    #[serde(default)]
    pub exchange: ExchangeConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub pricing: Option<PricingConfig>,
    #[serde(default)]
    pub gateway: GatewayConfig,
    #[serde(default)]
    pub internal_tls: crate::identity::InternalTlsConfig,
}

const fn default_recursion_max_iterations() -> usize {
    1024
}

impl RockstreamConfig {
    pub fn load_from_str(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    pub fn to_string(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn validate(&self, check_files: bool) -> crate::config_validation::ConfigValidationReport {
        let mut diagnostics = Vec::new();
        crate::config_validation::validate_semantic_bounds(self, check_files, &mut diagnostics);
        let valid = diagnostics
            .iter()
            .all(|d| d.severity != crate::config_validation::ConfigDiagnosticSeverity::Error);
        crate::config_validation::ConfigValidationReport { valid, diagnostics }
    }
}

impl Default for RockstreamConfig {
    fn default() -> Self {
        Self {
            recursion_max_iterations: default_recursion_max_iterations(),
            cluster: ClusterConfig {
                min_epoch_ms: 10,
                checkpoint_retention_count: 128,
                state_budget_gb: 10,
                autotuner: AutotunerConfig::default(),
                skew_split: SkewSplitConfig::default(),
                scatter_pruning: ScatterPruningConfig::default(),
                index_prefer_selectivity_threshold: 0.01,
                index_max_lag_ms: 1000,
                shutdown_timeout_secs: 30,
            },
            worker: WorkerConfig {
                segment_cache_bytes: 536870912, // 512 MB
                max_rows_per_quantum: 1000,
                execution_threads: 1,
            },
            connector: ConnectorConfig {
                dlq_warn_threshold: 100,
                dlq_retention_days: 7,
            },
            exchange: ExchangeConfig::default(),
            storage: StorageConfig::default(),
            execution: ExecutionConfig::default(),
            pricing: None,
            gateway: GatewayConfig::default(),
            internal_tls: crate::identity::InternalTlsConfig::default(),
        }
    }
}

// ============================================================================
// v0.62: StorageUrl & NodeConfig (Unified Configuration Architecture)
// ============================================================================

/// Explicit storage URL representation supporting `file://` and `s3://` schemes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StorageUrl {
    File(std::path::PathBuf),
    S3 { bucket: String, prefix: String },
}

impl StorageUrl {
    pub fn parse(s: &str) -> Result<Self, String> {
        let trimmed = s.trim();
        if let Some(rest) = trimmed.strip_prefix("file://") {
            let path_str = if rest.is_empty() { "." } else { rest };
            Ok(Self::File(std::path::PathBuf::from(path_str)))
        } else if let Some(rest) = trimmed.strip_prefix("s3://") {
            let (bucket, prefix) = match rest.split_once('/') {
                Some((b, p)) => (b, p),
                None => (rest, ""),
            };
            if bucket.is_empty() {
                return Err("RS-0002: S3 storage URL missing bucket name".to_string());
            }
            Ok(Self::S3 {
                bucket: bucket.to_string(),
                prefix: prefix.trim_matches('/').to_string(),
            })
        } else if let Some((scheme, _)) = trimmed.split_once("://") {
            Err(format!(
                "RS-0002: unsupported storage scheme `{scheme}`; supported schemes are file:// and s3://"
            ))
        } else {
            Ok(Self::File(std::path::PathBuf::from(trimmed)))
        }
    }

    pub fn scheme(&self) -> &str {
        match self {
            Self::File(_) => "file",
            Self::S3 { .. } => "s3",
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }

    pub fn is_s3(&self) -> bool {
        matches!(self, Self::S3 { .. })
    }

    pub fn as_file_path(&self) -> Option<&std::path::Path> {
        match self {
            Self::File(p) => Some(p.as_path()),
            Self::S3 { .. } => None,
        }
    }

    pub fn s3_bucket(&self) -> Option<&str> {
        match self {
            Self::S3 { bucket, .. } => Some(bucket.as_str()),
            Self::File(_) => None,
        }
    }

    pub fn s3_prefix(&self) -> Option<&str> {
        match self {
            Self::S3 { prefix, .. } => Some(prefix.as_str()),
            Self::File(_) => None,
        }
    }

    pub fn resolve(&self, base_dir: &std::path::Path) -> Self {
        match self {
            Self::File(p) => {
                if p.is_relative() {
                    Self::File(base_dir.join(p))
                } else {
                    self.clone()
                }
            }
            Self::S3 { .. } => self.clone(),
        }
    }

    pub fn resolve_relative(&self, base_dir: &std::path::Path) -> Self {
        self.resolve(base_dir)
    }

    pub fn verify_accessible(&self) -> Result<(), String> {
        match self {
            Self::File(p) => {
                std::fs::create_dir_all(p).map_err(|e| {
                    format!(
                        "RS-0003: storage directory inaccessible `{}`: {e}",
                        p.display()
                    )
                })?;
                Ok(())
            }
            Self::S3 { bucket, .. } => {
                if bucket.is_empty() {
                    return Err("RS-0003: S3 storage bucket cannot be empty".to_string());
                }
                Ok(())
            }
        }
    }
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(p) => {
                let p_str = p.to_string_lossy();
                if p_str.starts_with('/') || p_str.starts_with('.') {
                    write!(f, "file://{p_str}")
                } else {
                    write!(f, "file://./{p_str}")
                }
            }
            Self::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    write!(f, "s3://{bucket}")
                } else {
                    write!(f, "s3://{bucket}/{prefix}")
                }
            }
        }
    }
}

impl std::str::FromStr for StorageUrl {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Default for StorageUrl {
    fn default() -> Self {
        Self::File(std::path::PathBuf::from("./data"))
    }
}

impl Serialize for StorageUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StorageUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeSection {
    #[serde(default = "default_node_role")]
    pub role: String,
    #[serde(default)]
    pub host_id: Option<String>,
    #[serde(default)]
    pub availability_zone: Option<String>,
}

fn default_node_role() -> String {
    "all".to_string()
}

impl Default for NodeSection {
    fn default() -> Self {
        Self {
            role: default_node_role(),
            host_id: None,
            availability_zone: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct GatewayTlsConfig {
    #[serde(default)]
    pub cert_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub key_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub ca_cert_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewaySection {
    #[serde(default = "default_gateway_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_gateway_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_gateway_query_timeout_secs")]
    pub query_timeout_secs: u64,
    #[serde(default)]
    pub tls: GatewayTlsConfig,
    #[serde(default)]
    pub webhook_listen_addr: Option<String>,
}

fn default_gateway_listen_addr() -> String {
    "127.0.0.1:5432".to_string()
}

fn default_gateway_max_connections() -> usize {
    1024
}

fn default_gateway_query_timeout_secs() -> u64 {
    60
}

impl Default for GatewaySection {
    fn default() -> Self {
        Self {
            listen_addr: default_gateway_listen_addr(),
            max_connections: default_gateway_max_connections(),
            query_timeout_secs: default_gateway_query_timeout_secs(),
            tls: GatewayTlsConfig::default(),
            webhook_listen_addr: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RaftSection {
    #[serde(default)]
    pub peers: Vec<String>,
    #[serde(default)]
    pub node_id: Option<u64>,
    #[serde(default)]
    pub bind: Option<String>,
    #[serde(default)]
    pub bootstrap: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControlSection {
    #[serde(default = "default_control_listen_addr")]
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub shared_storage: Option<String>,
    #[serde(default)]
    pub raft: Option<RaftSection>,
}

fn default_control_listen_addr() -> Option<String> {
    Some("127.0.0.1:9200".to_string())
}

impl Default for ControlSection {
    fn default() -> Self {
        Self {
            listen_addr: default_control_listen_addr(),
            url: None,
            shared_storage: None,
            raft: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerSection {
    #[serde(default)]
    pub worker_id: Option<u64>,
    #[serde(default = "default_worker_execution_threads")]
    pub execution_threads: usize,
    #[serde(default = "default_worker_segment_cache_bytes")]
    pub segment_cache_bytes: usize,
    #[serde(default = "default_worker_max_rows_per_quantum")]
    pub max_rows_per_quantum: usize,
    #[serde(default = "default_worker_memory_budget_bytes")]
    pub memory_budget_bytes: usize,
    #[serde(default = "default_worker_foreground_reservation_bytes")]
    pub foreground_reservation_bytes: usize,
    #[serde(default = "default_worker_max_compaction_concurrency")]
    pub max_compaction_concurrency: usize,
    #[serde(default = "default_worker_max_backfill_concurrency")]
    pub max_backfill_concurrency: usize,
    #[serde(default = "default_worker_max_migration_concurrency")]
    pub max_migration_concurrency: usize,
    #[serde(default)]
    pub disk_cache_dir: Option<std::path::PathBuf>,
    #[serde(default = "default_worker_disk_cache_bytes")]
    pub disk_cache_bytes: usize,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

fn default_worker_execution_threads() -> usize {
    1
}

fn default_worker_segment_cache_bytes() -> usize {
    536870912
}

fn default_worker_max_rows_per_quantum() -> usize {
    1000
}

fn default_worker_memory_budget_bytes() -> usize {
    2_147_483_648
}

fn default_worker_foreground_reservation_bytes() -> usize {
    429_496_729
}

fn default_worker_max_compaction_concurrency() -> usize {
    2
}

fn default_worker_max_backfill_concurrency() -> usize {
    1
}

fn default_worker_max_migration_concurrency() -> usize {
    1
}

fn default_worker_disk_cache_bytes() -> usize {
    17_179_869_184
}

impl Default for WorkerSection {
    fn default() -> Self {
        Self {
            worker_id: None,
            execution_threads: default_worker_execution_threads(),
            segment_cache_bytes: default_worker_segment_cache_bytes(),
            max_rows_per_quantum: default_worker_max_rows_per_quantum(),
            memory_budget_bytes: default_worker_memory_budget_bytes(),
            foreground_reservation_bytes: default_worker_foreground_reservation_bytes(),
            max_compaction_concurrency: default_worker_max_compaction_concurrency(),
            max_backfill_concurrency: default_worker_max_backfill_concurrency(),
            max_migration_concurrency: default_worker_max_migration_concurrency(),
            disk_cache_dir: None,
            disk_cache_bytes: default_worker_disk_cache_bytes(),
            capabilities: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StorageSection {
    #[serde(default)]
    pub url: StorageUrl,
    #[serde(default)]
    pub temp_dir: Option<std::path::PathBuf>,
    #[serde(default)]
    pub spill_dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsSection {
    #[serde(default = "default_metrics_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_scrape_interval_secs")]
    pub scrape_interval_secs: u64,
}

fn default_metrics_listen_addr() -> String {
    "127.0.0.1:9090".to_string()
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_scrape_interval_secs() -> u64 {
    15
}

impl Default for MetricsSection {
    fn default() -> Self {
        Self {
            listen_addr: default_metrics_listen_addr(),
            enabled: default_metrics_enabled(),
            scrape_interval_secs: default_metrics_scrape_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthSection {
    #[serde(default = "default_auth_mode")]
    pub mode: String,
    #[serde(default)]
    pub secret_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub admin_user: Option<String>,
}

fn default_auth_mode() -> String {
    "off".to_string()
}

impl Default for AuthSection {
    fn default() -> Self {
        Self {
            mode: default_auth_mode(),
            secret_path: None,
            admin_user: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoggingSection {
    #[serde(default = "default_logging_level")]
    pub level: String,
    #[serde(default = "default_logging_format")]
    pub format: String,
}

fn default_logging_level() -> String {
    "info".to_string()
}

fn default_logging_format() -> String {
    "text".to_string()
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            level: default_logging_level(),
            format: default_logging_format(),
        }
    }
}

fn default_min_epoch_ms() -> u64 {
    10
}

fn default_checkpoint_retention_count() -> u32 {
    128
}

fn default_state_budget_gb() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSection {
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_min_epoch_ms")]
    pub min_epoch_ms: u64,
    #[serde(default = "default_checkpoint_retention_count")]
    pub checkpoint_retention_count: u32,
    #[serde(default = "default_state_budget_gb")]
    pub state_budget_gb: u64,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            min_epoch_ms: default_min_epoch_ms(),
            checkpoint_retention_count: default_checkpoint_retention_count(),
            state_budget_gb: default_state_budget_gb(),
        }
    }
}

/// The unified, authoritative RockStream node configuration (v0.62).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    #[serde(default)]
    pub node: NodeSection,
    #[serde(default)]
    pub gateway: GatewaySection,
    #[serde(default)]
    pub control: ControlSection,
    #[serde(default)]
    pub worker: WorkerSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub metrics: MetricsSection,
    #[serde(default)]
    pub auth: AuthSection,
    #[serde(default)]
    pub logging: LoggingSection,
    #[serde(default)]
    pub runtime: RuntimeSection,
}

fn default_config_version() -> u32 {
    1
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            node: NodeSection::default(),
            gateway: GatewaySection::default(),
            control: ControlSection::default(),
            worker: WorkerSection::default(),
            storage: StorageSection::default(),
            metrics: MetricsSection::default(),
            auth: AuthSection::default(),
            logging: LoggingSection::default(),
            runtime: RuntimeSection::default(),
        }
    }
}

impl From<&RockstreamConfig> for NodeConfig {
    fn from(cfg: &RockstreamConfig) -> Self {
        Self {
            version: 1,
            node: NodeSection {
                role: "all".to_string(),
                host_id: None,
                availability_zone: None,
            },
            gateway: GatewaySection {
                listen_addr: "127.0.0.1:5432".to_string(),
                max_connections: 1024,
                query_timeout_secs: 60,
                tls: GatewayTlsConfig {
                    cert_path: cfg.gateway.tls_cert_path.clone(),
                    key_path: cfg.gateway.tls_key_path.clone(),
                    ca_cert_path: cfg.gateway.tls_ca_cert_path.clone(),
                },
                webhook_listen_addr: cfg.gateway.webhook_listen_addr.clone(),
            },
            control: ControlSection::default(),
            worker: WorkerSection {
                worker_id: None,
                execution_threads: cfg.worker.execution_threads,
                segment_cache_bytes: cfg.worker.segment_cache_bytes,
                max_rows_per_quantum: cfg.worker.max_rows_per_quantum,
                capabilities: Vec::new(),
                ..WorkerSection::default()
            },
            storage: StorageSection::default(),
            metrics: MetricsSection::default(),
            auth: AuthSection::default(),
            logging: LoggingSection::default(),
            runtime: RuntimeSection {
                shutdown_timeout_secs: cfg.cluster.shutdown_timeout_secs,
                min_epoch_ms: cfg.cluster.min_epoch_ms,
                checkpoint_retention_count: cfg.cluster.checkpoint_retention_count,
                state_budget_gb: cfg.cluster.state_budget_gb,
            },
        }
    }
}

impl From<RockstreamConfig> for NodeConfig {
    fn from(cfg: RockstreamConfig) -> Self {
        Self::from(&cfg)
    }
}

impl From<&NodeConfig> for RockstreamConfig {
    fn from(node: &NodeConfig) -> Self {
        let mut cfg = RockstreamConfig::default();
        cfg.cluster.shutdown_timeout_secs = node.runtime.shutdown_timeout_secs;
        cfg.cluster.min_epoch_ms = node.runtime.min_epoch_ms;
        cfg.cluster.checkpoint_retention_count = node.runtime.checkpoint_retention_count;
        cfg.cluster.state_budget_gb = node.runtime.state_budget_gb;
        cfg.worker.execution_threads = node.worker.execution_threads;
        cfg.worker.segment_cache_bytes = node.worker.segment_cache_bytes;
        cfg.worker.max_rows_per_quantum = node.worker.max_rows_per_quantum;
        cfg.gateway.tls_cert_path = node.gateway.tls.cert_path.clone();
        cfg.gateway.tls_key_path = node.gateway.tls.key_path.clone();
        cfg.gateway.tls_ca_cert_path = node.gateway.tls.ca_cert_path.clone();
        cfg.gateway.webhook_listen_addr = node.gateway.webhook_listen_addr.clone();
        cfg
    }
}

impl From<NodeConfig> for RockstreamConfig {
    fn from(node: NodeConfig) -> Self {
        Self::from(&node)
    }
}

impl NodeConfig {
    pub fn load_from_str(s: &str) -> Result<Self, String> {
        if s.contains("version")
            || s.contains("[node]")
            || s.contains("[gateway]")
            || s.contains("[control]")
            || s.contains("[storage]")
            || s.contains("[runtime]")
        {
            if let Ok(cfg) = toml::from_str::<NodeConfig>(s) {
                return Ok(cfg);
            }
        }
        match toml::from_str::<RockstreamConfig>(s) {
            Ok(legacy) => Ok(NodeConfig::from(&legacy)),
            Err(_) => toml::from_str::<NodeConfig>(s).map_err(|err| err.to_string()),
        }
    }

    pub fn to_string(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn validate(&self, check_files: bool) -> crate::config_validation::ConfigValidationReport {
        let mut diagnostics = Vec::new();
        crate::config_validation::validate_node_config_semantic_bounds(
            self,
            check_files,
            &mut diagnostics,
        );
        let valid = diagnostics
            .iter()
            .all(|d| d.severity != crate::config_validation::ConfigDiagnosticSeverity::Error);
        crate::config_validation::ConfigValidationReport { valid, diagnostics }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_roundtrip() {
        let default_cfg = RockstreamConfig::default();
        let serialized = default_cfg.to_string().unwrap();
        let deserialized = RockstreamConfig::load_from_str(&serialized).unwrap();
        assert_eq!(default_cfg, deserialized);
    }

    #[test]
    fn config_exchange_defaults_roundtrip() {
        let cfg = RockstreamConfig::default();
        assert_eq!(cfg.exchange, ExchangeConfig::default());
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(roundtrip.exchange, ExchangeConfig::default());
    }

    #[test]
    fn config_scatter_pruning_defaults_roundtrip() {
        let cfg = RockstreamConfig::default();
        assert_eq!(cfg.cluster.scatter_pruning, ScatterPruningConfig::default());
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(
            roundtrip.cluster.scatter_pruning.shard_bloom_budget_bytes,
            65_536
        );
        assert_eq!(
            roundtrip
                .cluster
                .scatter_pruning
                .shard_stats_max_age_checkpoints,
            5
        );
    }

    #[test]
    fn config_gateway_tls_defaults_roundtrip() {
        let cfg = RockstreamConfig::default();
        assert_eq!(cfg.gateway, GatewayConfig::default());
        assert_eq!(cfg.gateway.tls_cert_path, None);
        assert_eq!(cfg.gateway.tls_key_path, None);
        assert_eq!(cfg.gateway.tls_ca_cert_path, None);
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(roundtrip.gateway, GatewayConfig::default());
    }

    #[test]
    fn config_gateway_tls_paths_roundtrip() {
        let mut cfg = RockstreamConfig::default();
        cfg.gateway.tls_cert_path = Some(std::path::PathBuf::from("/etc/rockstream/tls/cert.pem"));
        cfg.gateway.tls_key_path = Some(std::path::PathBuf::from("/etc/rockstream/tls/key.pem"));
        cfg.gateway.tls_ca_cert_path = Some(std::path::PathBuf::from("/etc/rockstream/tls/ca.pem"));
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(roundtrip.gateway, cfg.gateway);
    }

    #[test]
    fn pricing_and_tiering_blocks_parse() {
        let cfg = RockstreamConfig::load_from_str(
            r#"
[cluster]
min_epoch_ms = 10
checkpoint_retention_count = 128
state_budget_gb = 10

[worker]
segment_cache_bytes = 536870912
max_rows_per_quantum = 1000

[connector]
dlq_warn_threshold = 100
dlq_retention_days = 7

[storage.tiering]
shard_meta_backend = "s3express"
cold_sst_backend = "standard-ia"
cold_sst_age_threshold = 3600

[pricing]
object_store_request_per_1k = 0.005
object_store_standard_gb_month = 0.023
object_store_standard_ia_gb_month = 0.0125
object_store_egress_gb = 0.09
compute_on_demand_core_hour = 0.20
compute_spot_core_hour = 0.06
compute_spot_mix = 0.75
"#,
        )
        .unwrap();

        assert_eq!(
            cfg.storage.tiering.shard_meta_backend.as_deref(),
            Some("s3express")
        );
        assert_eq!(
            cfg.storage.tiering.cold_sst_backend.as_deref(),
            Some("standard-ia")
        );
        assert_eq!(cfg.storage.tiering.cold_sst_age_threshold, Some(3600));
        assert!(cfg.pricing.is_some());
    }

    #[test]
    fn config_internal_tls_defaults_roundtrip() {
        let cfg = RockstreamConfig::default();
        assert_eq!(
            cfg.internal_tls,
            crate::identity::InternalTlsConfig::default()
        );
        assert_eq!(cfg.internal_tls.cert_path, None);
        assert_eq!(cfg.internal_tls.key_path, None);
        assert_eq!(cfg.internal_tls.ca_cert_path, None);
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(
            roundtrip.internal_tls,
            crate::identity::InternalTlsConfig::default()
        );
    }

    #[test]
    fn config_internal_tls_paths_roundtrip() {
        let mut cfg = RockstreamConfig::default();
        cfg.internal_tls.cert_path = Some(std::path::PathBuf::from(
            "/etc/rockstream/tls/internal-cert.pem",
        ));
        cfg.internal_tls.key_path = Some(std::path::PathBuf::from(
            "/etc/rockstream/tls/internal-key.pem",
        ));
        cfg.internal_tls.ca_cert_path = Some(std::path::PathBuf::from(
            "/etc/rockstream/tls/cluster-ca.pem",
        ));
        let roundtrip = RockstreamConfig::load_from_str(&cfg.to_string().unwrap()).unwrap();
        assert_eq!(roundtrip.internal_tls, cfg.internal_tls);
    }
}
