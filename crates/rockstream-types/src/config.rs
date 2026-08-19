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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct WorkerConfig {
    pub segment_cache_bytes: usize,
    pub max_rows_per_quantum: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            segment_cache_bytes: 64 * 1024 * 1024,
            max_rows_per_quantum: 8192,
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
            },
            worker: WorkerConfig {
                segment_cache_bytes: 536870912, // 512 MB
                max_rows_per_quantum: 1000,
            },
            connector: ConnectorConfig {
                dlq_warn_threshold: 100,
                dlq_retention_days: 7,
            },
            exchange: ExchangeConfig::default(),
            storage: StorageConfig::default(),
            pricing: None,
            gateway: GatewayConfig::default(),
            internal_tls: crate::identity::InternalTlsConfig::default(),
        }
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
