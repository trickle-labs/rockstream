//! Frontier-aggregation performance regression benchmark suite (v0.45.4, S5).
//!
//! Answers DESIGN.md §16.6's named open risk: "Frontier-aggregator throughput
//! with thousands of shards × hundreds of operators. The aggregator must be
//! CPU- and memory-bounded, never blocking." Benches steady-state `ingest`
//! throughput (monotonic epoch advances only, matching real traffic) plus
//! `cluster_frontier`/`fill_level` read cost, against a pre-registered
//! simulated fleet of shards sized well below `MAX_REGISTERED_SHARDS`. No new
//! coordination protocol is introduced — this measures the existing,
//! already-tested `FrontierAggregator` under load.

use std::path::PathBuf;

use criterion::{BatchSize, Criterion, Throughput};
use rockstream_control::frontier::FrontierAggregator;
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::ShardId;

/// Simulated fleet size: "thousands of shards" (well under
/// `MAX_REGISTERED_SHARDS` = 100_000) combined with "hundreds of operators"
/// by giving each simulated shard hundreds of distinct epoch-advance events
/// during setup, mirroring hundreds of operators each driving frontier
/// advances on their shard.
const SIMULATED_SHARDS: u64 = 5_000;
const SIMULATED_OPERATORS_PER_SHARD: u64 = 200;

/// Build an aggregator with `SIMULATED_SHARDS` already registered, each
/// having advanced through `SIMULATED_OPERATORS_PER_SHARD` monotonic epoch
/// steps — i.e. steady-state, not cold-start.
fn build_warm_fleet() -> FrontierAggregator {
    let agg = FrontierAggregator::new();
    for shard in 0..SIMULATED_SHARDS {
        for epoch in 1..=SIMULATED_OPERATORS_PER_SHARD {
            agg.ingest(ShardFrontierReport {
                shard_id: ShardId(shard),
                epoch,
            })
            .expect("registry has capacity for the simulated fleet");
        }
    }
    agg
}

fn bench_ingest_steady_state(c: &mut Criterion) {
    let agg = build_warm_fleet();
    let mut next_epoch = SIMULATED_OPERATORS_PER_SHARD;
    let mut shard_cursor: u64 = 0;

    let mut group = c.benchmark_group("frontier_ingest");
    group.throughput(Throughput::Elements(1));
    group.bench_function("steady_state_monotonic_advance", |b| {
        b.iter(|| {
            // Monotonic epoch advances only, matching real traffic — no
            // retreats/no-ops, as the plan requires.
            next_epoch += 1;
            let shard = ShardId(shard_cursor % SIMULATED_SHARDS);
            shard_cursor += 1;
            agg.ingest(ShardFrontierReport {
                shard_id: shard,
                epoch: next_epoch,
            })
            .unwrap();
        });
    });
    group.finish();
}

fn bench_cluster_frontier_read(c: &mut Criterion) {
    let agg = build_warm_fleet();

    let mut group = c.benchmark_group("frontier_cluster_frontier_read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("read_under_load", |b| {
        b.iter(|| agg.cluster_frontier());
    });
    group.finish();
}

fn bench_fill_level_read(c: &mut Criterion) {
    let agg = build_warm_fleet();

    let mut group = c.benchmark_group("frontier_fill_level_read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("read_under_load", |b| {
        b.iter_batched(|| (), |()| agg.fill_level(), BatchSize::SmallInput);
    });
    group.finish();
}

fn default_criterion_dir() -> PathBuf {
    rockstream_ops::bench_regression::default_criterion_dir(env!("CARGO_MANIFEST_DIR"))
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_ingest_steady_state(&mut criterion);
    bench_cluster_frontier_read(&mut criterion);
    bench_fill_level_read(&mut criterion);
    criterion.final_summary();

    let summary = rockstream_ops::bench_regression::collect_criterion_summary(
        &default_criterion_dir(),
        &[
            "frontier_ingest",
            "frontier_cluster_frontier_read",
            "frontier_fill_level_read",
        ],
    );
    println!(
        "[bench_summary:control] {}",
        serde_json::to_string(&summary).unwrap()
    );
}
