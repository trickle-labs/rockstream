use std::sync::{LazyLock, RwLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PricingConfig {
    pub object_store_request_per_1k: f64,
    pub object_store_standard_gb_month: f64,
    #[serde(default)]
    pub object_store_standard_ia_gb_month: Option<f64>,
    pub object_store_egress_gb: f64,
    pub compute_on_demand_core_hour: f64,
    #[serde(default)]
    pub compute_spot_core_hour: Option<f64>,
    #[serde(default)]
    pub compute_spot_mix: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostEstimateInput {
    pub state_bytes: u64,
    pub memory_bytes: u64,
    pub memory_limit_bytes: Option<u64>,
    pub shard_count: u32,
    pub object_store_request_units_per_hour: f64,
    pub object_store_storage_bytes: u64,
    pub object_store_egress_bytes_per_hour: u64,
    pub worker_cores: Option<f64>,
    pub cold_storage_fraction: f64,
}

impl Default for CostEstimateInput {
    fn default() -> Self {
        Self {
            state_bytes: 0,
            memory_bytes: 0,
            memory_limit_bytes: None,
            shard_count: 1,
            object_store_request_units_per_hour: 0.0,
            object_store_storage_bytes: 0,
            object_store_egress_bytes_per_hour: 0,
            worker_cores: None,
            cold_storage_fraction: 0.0,
        }
    }
}

static ACTIVE_PRICING: LazyLock<RwLock<Option<PricingConfig>>> =
    LazyLock::new(|| RwLock::new(None));

pub fn set_active_pricing_config(pricing: Option<PricingConfig>) {
    *ACTIVE_PRICING.write().expect("pricing lock poisoned") = pricing;
}

pub fn active_pricing_config() -> Option<PricingConfig> {
    ACTIVE_PRICING
        .read()
        .expect("pricing lock poisoned")
        .clone()
}

pub fn estimate_cost_per_hour(
    pricing: Option<&PricingConfig>,
    input: &CostEstimateInput,
) -> Option<f64> {
    let pricing = pricing?;
    let total_storage_bytes = input.object_store_storage_bytes.max(input.state_bytes);
    let storage_gb = total_storage_bytes as f64 / 1_000_000_000.0;
    let cold_fraction = input.cold_storage_fraction.clamp(0.0, 1.0);
    let hot_fraction = 1.0 - cold_fraction;

    let standard_storage_hourly = pricing.object_store_standard_gb_month / (30.0 * 24.0);
    let standard_ia_storage_hourly = pricing
        .object_store_standard_ia_gb_month
        .unwrap_or(pricing.object_store_standard_gb_month)
        / (30.0 * 24.0);
    let storage_cost = storage_gb
        * ((hot_fraction * standard_storage_hourly) + (cold_fraction * standard_ia_storage_hourly));

    let request_cost =
        (input.object_store_request_units_per_hour / 1_000.0) * pricing.object_store_request_per_1k;
    let egress_gb = input.object_store_egress_bytes_per_hour as f64 / 1_000_000_000.0;
    let egress_cost = egress_gb * pricing.object_store_egress_gb;

    let derived_memory_bytes = input.memory_limit_bytes.unwrap_or(input.memory_bytes);
    let derived_memory_cores = (derived_memory_bytes as f64 / (8.0 * 1024.0 * 1024.0 * 1024.0))
        .ceil()
        .max(1.0);
    let shard_cores = input.shard_count.max(1) as f64;
    let worker_cores = input
        .worker_cores
        .unwrap_or_else(|| derived_memory_cores.max(shard_cores));

    let spot_mix = pricing.compute_spot_mix.unwrap_or(0.0).clamp(0.0, 1.0);
    let spot_core_hour = pricing
        .compute_spot_core_hour
        .unwrap_or(pricing.compute_on_demand_core_hour);
    let compute_cost = worker_cores
        * ((1.0 - spot_mix) * pricing.compute_on_demand_core_hour + spot_mix * spot_core_hour);

    Some(storage_cost + request_cost + egress_cost + compute_cost)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RockstreamConfig;

    fn sample_pricing_toml() -> &'static str {
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

[pricing]
object_store_request_per_1k = 0.005
object_store_standard_gb_month = 0.023
object_store_standard_ia_gb_month = 0.0125
object_store_egress_gb = 0.09
compute_on_demand_core_hour = 0.20
compute_spot_core_hour = 0.06
compute_spot_mix = 0.75
"#
    }

    #[test]
    fn estimated_cost_is_none_without_pricing_block() {
        let cfg = RockstreamConfig::default();
        assert!(cfg.pricing.is_none());
        let input = CostEstimateInput {
            memory_limit_bytes: Some(2 * 1024 * 1024 * 1024),
            ..Default::default()
        };
        assert_eq!(estimate_cost_per_hour(cfg.pricing.as_ref(), &input), None);
    }

    #[test]
    fn estimated_cost_changes_with_memory_limit_and_shard_count() {
        let cfg = RockstreamConfig::load_from_str(sample_pricing_toml()).unwrap();
        let pricing = cfg.pricing.as_ref();
        assert!(pricing.is_some());

        let cases = [
            CostEstimateInput {
                state_bytes: 8 * 1024 * 1024 * 1024,
                memory_bytes: 512 * 1024 * 1024,
                memory_limit_bytes: Some(2 * 1024 * 1024 * 1024),
                shard_count: 1,
                object_store_request_units_per_hour: 1_000.0,
                object_store_storage_bytes: 8 * 1024 * 1024 * 1024,
                object_store_egress_bytes_per_hour: 0,
                worker_cores: None,
                cold_storage_fraction: 0.0,
            },
            CostEstimateInput {
                state_bytes: 8 * 1024 * 1024 * 1024,
                memory_bytes: 512 * 1024 * 1024 * 1024,
                memory_limit_bytes: Some(8 * 1024 * 1024 * 1024),
                shard_count: 4,
                object_store_request_units_per_hour: 1_000.0,
                object_store_storage_bytes: 8 * 1024 * 1024 * 1024,
                object_store_egress_bytes_per_hour: 0,
                worker_cores: None,
                cold_storage_fraction: 0.0,
            },
        ];

        let low = estimate_cost_per_hour(pricing, &cases[0]).unwrap();
        let high = estimate_cost_per_hour(pricing, &cases[1]).unwrap();
        assert!(low > 0.0);
        assert!(high > 0.0);
        assert_ne!(low, high);
        assert!(high > low);
    }
}
