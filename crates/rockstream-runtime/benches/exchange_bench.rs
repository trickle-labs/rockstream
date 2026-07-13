//! Exchange/shuffle performance regression benchmark suite (v0.45.4, S4).
//!
//! Benches:
//! - `serialize_zset`/`deserialize_zset` round-trip on a representative
//!   shuffle-batch-sized `ArrowZSet`.
//! - `FlowController` credit-flow throughput — a tight async loop of
//!   `acquire_credit` immediately answered by a paired `handle_ack`,
//!   measuring achieved acquire/release pairs per second (the "credit-flow
//!   throughput" metric named by DESIGN.md/roadmap).

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, Throughput};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::proto::ShuffleAck;
use rockstream_runtime::exchange::serialization::{deserialize_zset, serialize_zset};
use tokio::runtime::Runtime;

/// Representative shuffle-batch row count (mirrors `perf_regression.rs`'s
/// `make_kv_batch` sizing conventions).
const SHUFFLE_BATCH_ROWS: usize = 10_000;

fn make_kv_batch(rows: usize) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = (0..rows as i64).map(|i| i % 5).collect();
    let v_vals: Vec<i64> = (0..rows as i64).map(|i| i * 7 % 1000).collect();
    let weights: Vec<i64> = vec![1; rows];
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

fn bench_serialize_zset(c: &mut Criterion) {
    let zset = make_kv_batch(SHUFFLE_BATCH_ROWS);

    let mut group = c.benchmark_group("exchange_serialize_zset");
    group.throughput(criterion::Throughput::Elements(SHUFFLE_BATCH_ROWS as u64));
    group.bench_function("serialize", |b| {
        b.iter(|| serialize_zset(&zset).unwrap());
    });
    group.finish();
}

fn bench_deserialize_zset(c: &mut Criterion) {
    let zset = make_kv_batch(SHUFFLE_BATCH_ROWS);
    let schema = zset.data.schema();
    let payload = serialize_zset(&zset).unwrap();

    let mut group = c.benchmark_group("exchange_deserialize_zset");
    group.throughput(Throughput::Elements(SHUFFLE_BATCH_ROWS as u64));
    group.bench_function("deserialize", |b| {
        b.iter(|| deserialize_zset(&payload, schema.clone()).unwrap());
    });
    group.finish();
}

fn bench_flow_control_credit_throughput(c: &mut Criterion) {
    let rt = Runtime::new().expect("build tokio runtime");

    let mut group = c.benchmark_group("exchange_flow_control");
    group.throughput(Throughput::Elements(1));
    group.bench_function("acquire_ack_pair", |b| {
        b.to_async(&rt).iter_batched(
            FlowController::new,
            |fc| async move {
                fc.acquire_credit(0, 0, 0).await;
                fc.handle_ack(&ShuffleAck {
                    exchange_id: 0,
                    src_shard: 0,
                    target_shard: 0,
                    epoch: 0,
                    seq: 0,
                    credit_grant: 1,
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn default_criterion_dir() -> PathBuf {
    rockstream_ops::bench_regression::default_criterion_dir(env!("CARGO_MANIFEST_DIR"))
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_serialize_zset(&mut criterion);
    bench_deserialize_zset(&mut criterion);
    bench_flow_control_credit_throughput(&mut criterion);
    criterion.final_summary();

    let summary = rockstream_ops::bench_regression::collect_criterion_summary(
        &default_criterion_dir(),
        &[
            "exchange_serialize_zset",
            "exchange_deserialize_zset",
            "exchange_flow_control",
        ],
    );
    println!(
        "[bench_summary:runtime] {}",
        serde_json::to_string(&summary).unwrap()
    );
}
