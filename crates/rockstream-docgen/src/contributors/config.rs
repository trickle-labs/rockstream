//! Configuration surface contributor (DOC-001).

use crate::manifest::{ConfigOptionDescriptor, ConfigSurface};

pub struct ConfigContributor;

impl ConfigContributor {
    /// Extract runtime and system configuration descriptors.
    pub fn extract() -> ConfigSurface {
        let mut options = vec![
            ConfigOptionDescriptor {
                key: "autotuner.compression_disable_hysteresis_windows".to_string(),
                data_type: "usize".to_string(),
                default_value: "2".to_string(),
                description: "Windows of high CPU before disabling direct compression".to_string(),
                deprecated: false,
                env_var: Some(
                    "ROCKSTREAM_AUTOTUNER_COMPRESSION_DISABLE_HYSTERESIS_WINDOWS".to_string(),
                ),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.compression_reenable_hysteresis_windows".to_string(),
                data_type: "usize".to_string(),
                default_value: "4".to_string(),
                description: "Windows of low CPU before re-enabling direct compression".to_string(),
                deprecated: false,
                env_var: Some(
                    "ROCKSTREAM_AUTOTUNER_COMPRESSION_REENABLE_HYSTERESIS_WINDOWS".to_string(),
                ),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.default_parallelism".to_string(),
                data_type: "usize".to_string(),
                default_value: "4".to_string(),
                description: "Default operator execution parallelism".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_DEFAULT_PARALLELISM".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.direct_compression_cpu_budget_ms".to_string(),
                data_type: "u64".to_string(),
                default_value: "5".to_string(),
                description: "CPU time budget in milliseconds allocated for direct compression"
                    .to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_DIRECT_COMPRESSION_CPU_BUDGET_MS".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.enabled".to_string(),
                data_type: "bool".to_string(),
                default_value: "true".to_string(),
                description: "Enable dynamic pipeline autotuning".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_ENABLED".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.hysteresis_scale_down_windows".to_string(),
                data_type: "usize".to_string(),
                default_value: "12".to_string(),
                description: "Number of consecutive underloaded windows before scale-down"
                    .to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_HYSTERESIS_SCALE_DOWN_WINDOWS".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.hysteresis_scale_up_windows".to_string(),
                data_type: "usize".to_string(),
                default_value: "3".to_string(),
                description: "Number of consecutive overloaded windows before scale-up".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_HYSTERESIS_SCALE_UP_WINDOWS".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.max_parallelism".to_string(),
                data_type: "usize".to_string(),
                default_value: "32".to_string(),
                description: "Maximum allowed operator parallelism".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_MAX_PARALLELISM".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "autotuner.min_parallelism".to_string(),
                data_type: "usize".to_string(),
                default_value: "1".to_string(),
                description: "Minimum allowed operator parallelism".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_AUTOTUNER_MIN_PARALLELISM".to_string()),
                source_origin: "rockstream-types::config::AutotunerConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "scatter_pruning.shard_bloom_budget_bytes".to_string(),
                data_type: "usize".to_string(),
                default_value: "65536".to_string(),
                description: "Memory budget in bytes allocated for shard bloom filters".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_SCATTER_PRUNING_SHARD_BLOOM_BUDGET_BYTES".to_string()),
                source_origin: "rockstream-types::config::ScatterPruningConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "scatter_pruning.shard_stats_max_age_checkpoints".to_string(),
                data_type: "u64".to_string(),
                default_value: "5".to_string(),
                description: "Maximum checkpoint age for shard key statistics".to_string(),
                deprecated: false,
                env_var: Some(
                    "ROCKSTREAM_SCATTER_PRUNING_SHARD_STATS_MAX_AGE_CHECKPOINTS".to_string(),
                ),
                source_origin: "rockstream-types::config::ScatterPruningConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "server.listen_addr".to_string(),
                data_type: "String".to_string(),
                default_value: "127.0.0.1:5432".to_string(),
                description: "PostgreSQL wire protocol server listen address".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_LISTEN_ADDR".to_string()),
                source_origin: "rockstream-runtime::server".to_string(),
            },
            ConfigOptionDescriptor {
                key: "server.max_connections".to_string(),
                data_type: "usize".to_string(),
                default_value: "1024".to_string(),
                description: "Maximum concurrent pgwire client connections".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_MAX_CONNECTIONS".to_string()),
                source_origin: "rockstream-runtime::server".to_string(),
            },
            ConfigOptionDescriptor {
                key: "skew_split.enabled".to_string(),
                data_type: "bool".to_string(),
                default_value: "true".to_string(),
                description: "Enable automatic hot-key skew splitting".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_SKEW_SPLIT_ENABLED".to_string()),
                source_origin: "rockstream-types::config::SkewSplitConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "skew_split.hot_key_factor".to_string(),
                data_type: "f64".to_string(),
                default_value: "20.0".to_string(),
                description: "Skew threshold multiplier relative to mean key frequency".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_SKEW_SPLIT_HOT_KEY_FACTOR".to_string()),
                source_origin: "rockstream-types::config::SkewSplitConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "skew_split.max_skew_buckets".to_string(),
                data_type: "u16".to_string(),
                default_value: "16".to_string(),
                description: "Maximum number of sub-buckets per skewed key".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_SKEW_SPLIT_MAX_SKEW_BUCKETS".to_string()),
                source_origin: "rockstream-types::config::SkewSplitConfig".to_string(),
            },
            ConfigOptionDescriptor {
                key: "storage.base_dir".to_string(),
                data_type: "String".to_string(),
                default_value: "./data".to_string(),
                description: "Local filesystem storage directory root".to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_STORAGE_DIR".to_string()),
                source_origin: "rockstream-storage".to_string(),
            },
            ConfigOptionDescriptor {
                key: "storage.object_store_url".to_string(),
                data_type: "Option<String>".to_string(),
                default_value: "None".to_string(),
                description: "Object store bucket URL (e.g. s3://bucket or minio://bucket)"
                    .to_string(),
                deprecated: false,
                env_var: Some("ROCKSTREAM_OBJECT_STORE_URL".to_string()),
                source_origin: "rockstream-storage".to_string(),
            },
        ];

        options.sort_by(|a, b| a.key.cmp(&b.key));
        ConfigSurface { options }
    }
}
