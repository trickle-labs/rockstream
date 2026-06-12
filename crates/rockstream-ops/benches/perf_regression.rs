//! Performance regression benchmark suite for filters, aggregates, and joins (v0.14).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

criterion_group!(benches, bench_filter, bench_aggregate, bench_join);
criterion_main!(benches);
