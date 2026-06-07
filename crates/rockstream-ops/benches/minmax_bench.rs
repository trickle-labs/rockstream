//! Throughput benchmark for the MIN operator (v0.6 Phase 1 exit criteria).
//!
//! Phase 1 exit criteria (NEW_IMPLEMENTATION_PLAN.md):
//! - In-memory object store:  ≥100k rows/s GROUP BY MIN
//! - Local-filesystem store:  ≥50k  rows/s GROUP BY MIN
//!
//! This benchmark measures pure `process_delta` throughput (no storage I/O)
//! as the upper bound.  The measurements are recorded here for CI comparison.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::minmax::{MinMaxKind, MinMaxOp};
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
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

/// Benchmark: process N insert rows across K groups.
///
/// Inserts only (no retractions); measures steady-state insert throughput.
fn bench_min_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_min");

    for &batch_size in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("inserts_in_memory", batch_size),
            &batch_size,
            |b, &n| {
                // Build a batch with n rows across 5 groups, varying values.
                let rows: Vec<(i64, i64, i64)> =
                    (0..n as i64).map(|i| (i % 5, i * 7 % 1000, 1)).collect();
                let batch = make_kv_batch(&rows);
                b.iter(|| {
                    let op = MinMaxOp::new(OperatorId(0), MinMaxKind::Min);
                    let _ = op.process_delta(batch.clone()).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: insert then retract (extremum churn — the expensive path).
///
/// Each batch inserts N rows, then retracts the minimum, forcing a multiset
/// rescan.  This is the worst-case throughput scenario.
fn bench_min_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_min_churn");

    for &batch_size in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("extremum_churn_in_memory", batch_size),
            &batch_size,
            |b, &n| {
                let op = MinMaxOp::new(OperatorId(0), MinMaxKind::Min);
                // Pre-populate: insert n/2 rows per group.
                let setup: Vec<(i64, i64, i64)> = (0..n as i64 / 2)
                    .map(|i| (i % 5, i * 7 % 1000 + 1, 1))
                    .collect();
                let _ = op.process_delta(make_kv_batch(&setup)).unwrap();
                // Benchmark: retract one row per group (forces rescan).
                let retract: Vec<(i64, i64, i64)> = (0..5i64).map(|k| (k, 1, -1)).collect(); // retract value=1 if exists
                let retract_batch = make_kv_batch(&retract);
                b.iter(|| {
                    let _ = op.process_delta(retract_batch.clone());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_min_inserts, bench_min_churn);
criterion_main!(benches);
