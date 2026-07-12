use rockstream_ops::tco::{
    baseline_pricing, optimized_pricing, reduction_ratio, scenario_cost, tiered_profile,
    workload_profile,
};

#[test]
fn optimized_tco_is_more_than_fifty_percent_lower() {
    let baseline = scenario_cost(&baseline_pricing(), &workload_profile());
    let optimized = scenario_cost(&optimized_pricing(), &tiered_profile());
    assert!(baseline > 0.0);
    assert!(optimized > 0.0);
    assert!(optimized < baseline * 0.5);
    assert!(reduction_ratio() > 0.5);
}
