//! Performance regression benchmark suite for filters, aggregates, and joins (v0.14).
//!
//! ## Delta propagation cost benchmarks
//!
//! `bench_delta_propagation_rates` measures IVM throughput and delta
//! amplification factor at three realistic change rates:
//!
//! - **0.1%** — typical OLTP tick (100 rows changed out of 100 000)
//! - **1.0%** — moderate churn (1 000 rows changed)
//! - **10%**  — high-churn scenario (10 000 rows changed)
//!
//! Delta amplification = output_rows / input_rows.  For aggregate operators
//! this is ≤ 2 × groups (one retraction + one insertion per changed group);
//! for filter it is ≤ 1.  Tracking this over time guards against regressions
//! where a small input delta causes unexpectedly large output fan-out.

use criterion::{BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};
use rockstream_types::ids::OperatorId;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn make_filter_op() -> FilterOp {
    let predicate = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column(1)), // v
        right: Box::new(Expr::Literal(500i64.to_be_bytes().to_vec())),
    };
    FilterOp::new(predicate)
}

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_performance");
    let op = make_filter_op();

    let batch_size = 10_000usize;
    group.throughput(Throughput::Elements(batch_size as u64));
    group.bench_with_input(
        BenchmarkId::new("filter_in_memory", batch_size),
        &batch_size,
        |b, &n| {
            let rows: Vec<(i64, i64, i64)> =
                (0..n as i64).map(|i| (i % 5, i * 7 % 1000, 1)).collect();
            let batch = make_kv_batch(&rows);
            b.iter(|| {
                let _ = op.process_delta(batch.clone()).unwrap();
            });
        },
    );
    group.finish();
}

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate_performance");

    let batch_size = 10_000usize;
    group.throughput(Throughput::Elements(batch_size as u64));
    group.bench_with_input(
        BenchmarkId::new("aggregate_in_memory", batch_size),
        &batch_size,
        |b, &n| {
            let op = AggregateOp::new(OperatorId(10));
            let rows: Vec<(i64, i64, i64)> =
                (0..n as i64).map(|i| (i % 5, i * 7 % 1000, 1)).collect();
            let batch = make_kv_batch(&rows);
            b.iter(|| {
                let _ = op.process_delta(batch.clone()).unwrap();
            });
        },
    );
    group.finish();
}

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_performance");

    let batch_size = 10_000usize;
    group.throughput(Throughput::Elements(batch_size as u64));
    group.bench_with_input(
        BenchmarkId::new("join_in_memory", batch_size),
        &batch_size,
        |b, &n| {
            let op = JoinOp::with_schema(OperatorId(20), vec![0], vec![0], 2, 2);
            let left_rows: Vec<(i64, i64, i64)> =
                (0..n as i64).map(|i| (i % 10, i * 3 % 100, 1)).collect();
            let right_rows: Vec<(i64, i64, i64)> =
                (0..n as i64).map(|i| (i % 10, i * 5 % 100, 1)).collect();
            let left_batch = make_kv_batch(&left_rows);
            let right_batch = make_kv_batch(&right_rows);
            b.iter(|| {
                let _ = op
                    .process_epoch(left_batch.clone(), right_batch.clone())
                    .unwrap();
            });
        },
    );
    group.finish();
}

// ─── Delta propagation cost at 0.1% / 1% / 10% change rates ─────────────────

/// Dataset size used for delta propagation cost benchmarks.
const DATASET_SIZE: usize = 100_000;

/// Number of distinct group keys for the aggregate operator.
const NUM_GROUPS: usize = 100;

/// Build the initial aggregate state by loading DATASET_SIZE rows.
///
/// Returns a warm `AggregateOp` whose state reflects the full dataset so
/// that subsequent delta benchmarks measure *incremental* update cost only.
fn warm_aggregate_op(dataset_size: usize) -> AggregateOp {
    let op = AggregateOp::new(OperatorId(30));
    let rows: Vec<(i64, i64, i64)> = (0..dataset_size as i64)
        .map(|i| (i % NUM_GROUPS as i64, i * 7 % 10_000, 1))
        .collect();
    let batch = make_kv_batch(&rows);
    op.process_delta(batch).expect("warm aggregate");
    op
}

/// Build a delta batch that retracts `delta_size` rows and inserts `delta_size`
/// replacement rows — simulating a real UPDATE workload at a given change rate.
fn make_update_delta(dataset_size: usize, delta_size: usize) -> ArrowZSet {
    // Retractions: pick rows spread evenly across the dataset.
    let step = dataset_size / delta_size;
    let mut rows: Vec<(i64, i64, i64)> = Vec::with_capacity(delta_size * 2);
    for i in 0..delta_size {
        let base = (i * step) as i64;
        // Retract old value.
        rows.push((base % NUM_GROUPS as i64, base * 7 % 10_000, -1));
        // Insert new (updated) value.
        rows.push((base % NUM_GROUPS as i64, (base * 7 + 1) % 10_000, 1));
    }
    make_kv_batch(&rows)
}

/// Benchmark delta propagation cost at three change rates.
///
/// Reports throughput in rows/sec and prints the delta amplification factor
/// (output_rows / input_rows) for each run so regressions are visible.
fn bench_delta_propagation_rates(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_propagation_cost");

    // Change rates: (label, delta_size as fraction of DATASET_SIZE).
    let rates: &[(&str, usize)] = &[
        ("0.1pct", DATASET_SIZE / 1000), // 100 rows
        ("1pct", DATASET_SIZE / 100),    // 1 000 rows
        ("10pct", DATASET_SIZE / 10),    // 10 000 rows
    ];

    for &(label, delta_size) in rates {
        // Pre-build delta outside the timed loop so only operator cost is measured.
        let delta = make_update_delta(DATASET_SIZE, delta_size);
        let input_rows = delta.num_rows();

        group.throughput(Throughput::Elements(input_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("aggregate_update", label),
            &delta,
            |b, d| {
                b.iter_batched(
                    || warm_aggregate_op(DATASET_SIZE),
                    |op| {
                        let out = op.process_delta(d.clone()).unwrap();
                        // Return output size so the compiler cannot optimise away the call.
                        out.num_rows()
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );

        // Measure amplification once (outside criterion timing) to print it.
        {
            let op = warm_aggregate_op(DATASET_SIZE);
            let out = op.process_delta(delta.clone()).unwrap();
            let amplification = out.num_rows() as f64 / input_rows as f64;
            eprintln!(
                "[delta_propagation] rate={label} input={input_rows} output={} amp={amplification:.2}",
                out.num_rows()
            );
        }
    }

    group.finish();
}

/// Benchmark delta propagation cost through a filter operator at three rates.
///
/// For a filter, delta amplification is always ≤ 1 (rows can only be dropped).
/// This establishes the minimum-cost baseline for the IVM pipeline.
fn bench_filter_delta_rates(c: &mut Criterion) {
    let op = make_filter_op();
    let mut group = c.benchmark_group("filter_delta_cost");

    let rates: &[(&str, usize)] = &[
        ("0.1pct", DATASET_SIZE / 1000),
        ("1pct", DATASET_SIZE / 100),
        ("10pct", DATASET_SIZE / 10),
    ];

    for &(label, delta_size) in rates {
        let delta = make_update_delta(DATASET_SIZE, delta_size);
        group.throughput(Throughput::Elements(delta.num_rows() as u64));
        group.bench_with_input(BenchmarkId::new("filter_update", label), &delta, |b, d| {
            b.iter(|| op.process_delta(d.clone()).unwrap());
        });
    }

    group.finish();
}

// Custom main (instead of `criterion_main!`) so we can read back the mean
// point estimates criterion just wrote to `target/criterion/**/new/
// estimates.json` and print a single tagged `[bench_summary:ops]` JSON line
// once all groups finish — closing the "descriptive only" gap flagged in
// DESIGN.md/roadmap for this suite. This is a small wrapper around what
// `criterion_main!` expands to; it does not change any criterion API usage.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_filter(&mut criterion);
    bench_aggregate(&mut criterion);
    bench_join(&mut criterion);
    bench_delta_propagation_rates(&mut criterion);
    bench_filter_delta_rates(&mut criterion);
    criterion.final_summary();

    let criterion_dir =
        rockstream_ops::bench_regression::default_criterion_dir(env!("CARGO_MANIFEST_DIR"));
    let summary = rockstream_ops::bench_regression::collect_criterion_summary(
        &criterion_dir,
        &[
            "filter_performance",
            "aggregate_performance",
            "join_performance",
            "delta_propagation_cost",
            "filter_delta_cost",
        ],
    );
    println!(
        "[bench_summary:ops] {}",
        serde_json::to_string(&summary).unwrap()
    );
}
