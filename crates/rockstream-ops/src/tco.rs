use rockstream_types::cost::{estimate_cost_per_hour, CostEstimateInput, PricingConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct TcoScenario {
    pub name: &'static str,
    pub input: CostEstimateInput,
}

pub fn workload_profile() -> TcoScenario {
    TcoScenario {
        name: "shared-workload",
        input: CostEstimateInput {
            state_bytes: 2_000_000_000_000,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            memory_limit_bytes: Some(16 * 1024 * 1024 * 1024),
            shard_count: 8,
            object_store_request_units_per_hour: 2_000_000.0,
            object_store_storage_bytes: 2_000_000_000_000,
            object_store_egress_bytes_per_hour: 25_000_000_000,
            worker_cores: Some(16.0),
            cold_storage_fraction: 0.0,
        },
    }
}

pub fn baseline_pricing() -> PricingConfig {
    PricingConfig {
        object_store_request_per_1k: 0.005,
        object_store_standard_gb_month: 0.023,
        object_store_standard_ia_gb_month: Some(0.023),
        object_store_egress_gb: 0.09,
        compute_on_demand_core_hour: 0.48,
        compute_spot_core_hour: Some(0.48),
        compute_spot_mix: Some(0.0),
    }
}

pub fn optimized_pricing() -> PricingConfig {
    PricingConfig {
        object_store_request_per_1k: 0.005,
        object_store_standard_gb_month: 0.023,
        object_store_standard_ia_gb_month: Some(0.0125),
        object_store_egress_gb: 0.09,
        compute_on_demand_core_hour: 0.48,
        compute_spot_core_hour: Some(0.06),
        compute_spot_mix: Some(0.80),
    }
}

pub fn scenario_cost(pricing: &PricingConfig, scenario: &TcoScenario) -> f64 {
    estimate_cost_per_hour(Some(pricing), &scenario.input).unwrap_or(0.0)
}

pub fn tiered_profile() -> TcoScenario {
    let mut input = workload_profile().input;
    input.memory_bytes = 4 * 1024 * 1024 * 1024;
    input.memory_limit_bytes = Some(8 * 1024 * 1024 * 1024);
    input.shard_count = 4;
    input.object_store_request_units_per_hour = 1_200_000.0;
    input.object_store_egress_bytes_per_hour = 8_000_000_000;
    input.worker_cores = Some(4.0);
    input.cold_storage_fraction = 0.90;
    TcoScenario {
        name: "tiered-workload",
        input,
    }
}

pub fn reduction_ratio() -> f64 {
    let baseline = scenario_cost(&baseline_pricing(), &workload_profile());
    let optimized = scenario_cost(&optimized_pricing(), &tiered_profile());
    if baseline == 0.0 {
        0.0
    } else {
        1.0 - (optimized / baseline)
    }
}
