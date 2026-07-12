use criterion::{criterion_group, criterion_main, Criterion};
use rockstream_ops::tco::{
    baseline_pricing, optimized_pricing, scenario_cost, tiered_profile, workload_profile,
};

fn bench_tco(c: &mut Criterion) {
    let baseline_pricing = baseline_pricing();
    let optimized_pricing = optimized_pricing();
    let baseline = workload_profile();
    let optimized = tiered_profile();
    c.bench_function("tco_cost_baseline", |b| {
        b.iter(|| scenario_cost(&baseline_pricing, &baseline))
    });
    c.bench_function("tco_cost_optimized", |b| {
        b.iter(|| scenario_cost(&optimized_pricing, &optimized))
    });
}

criterion_group!(benches, bench_tco);
criterion_main!(benches);
