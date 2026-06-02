//! Configuration types for RockStream (v0.49).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub min_epoch_ms: u64,
    pub checkpoint_retention_count: u32,
    pub state_budget_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerConfig {
    pub segment_cache_bytes: usize,
    pub max_rows_per_quantum: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorConfig {
    pub dlq_warn_threshold: u32,
    pub dlq_retention_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RockstreamConfig {
    pub cluster: ClusterConfig,
    pub worker: WorkerConfig,
    pub connector: ConnectorConfig,
}

impl RockstreamConfig {
    pub fn load_from_str(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    pub fn to_string(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

impl Default for RockstreamConfig {
    fn default() -> Self {
        Self {
            cluster: ClusterConfig {
                min_epoch_ms: 10,
                checkpoint_retention_count: 128,
                state_budget_gb: 10,
            },
            worker: WorkerConfig {
                segment_cache_bytes: 536870912, // 512 MB
                max_rows_per_quantum: 1000,
            },
            connector: ConnectorConfig {
                dlq_warn_threshold: 100,
                dlq_retention_days: 7,
            },
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
}
