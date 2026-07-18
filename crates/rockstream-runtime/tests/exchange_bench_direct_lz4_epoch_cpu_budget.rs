use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::compression_tuner::CompressionTuner;
use rockstream_runtime::exchange::serialization::{frame_payload_bytes, serialize_zset};
use rockstream_types::config::{AutotunerConfig, ExchangeConfig};
use rockstream_types::exchange::ShuffleCompression;
use rockstream_types::ids::ExchangeId;

fn make_wide_shuffle_batch(rows: usize) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals = vec![17_i64; rows];
    let v_vals: Vec<i64> = (0..rows as i64).map(|i| i % 8).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, vec![1; rows])
}

fn measure_direct_lz4_epoch_cpu_ms(payload: &[u8], epochs: usize) -> u64 {
    let started = std::time::Instant::now();
    for _ in 0..epochs {
        let framed = frame_payload_bytes(payload, ShuffleCompression::Lz4, true).unwrap();
        assert!(framed.len() < payload.len());
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    (elapsed_ms / epochs as f64).ceil() as u64
}

#[test]
fn exchange_bench_direct_lz4_epoch_cpu_budget() {
    let batch = make_wide_shuffle_batch(8_192);
    let raw = serialize_zset(&batch).unwrap();
    let autotuner = AutotunerConfig {
        direct_compression_cpu_budget_ms: 20,
        ..AutotunerConfig::default()
    };
    let measured_ms = measure_direct_lz4_epoch_cpu_ms(&raw, 8);
    assert!(
        measured_ms <= autotuner.direct_compression_cpu_budget_ms,
        "expected direct LZ4 epoch CPU {}ms to stay within {}ms budget",
        measured_ms,
        autotuner.direct_compression_cpu_budget_ms
    );

    let tuner = CompressionTuner::new(ExchangeConfig::default(), autotuner.clone());
    assert_eq!(
        tuner.decide(ExchangeId(49), ShuffleCompression::Lz4, measured_ms),
        ShuffleCompression::Lz4
    );
    assert_eq!(
        tuner.decide(ExchangeId(49), ShuffleCompression::Lz4, measured_ms),
        ShuffleCompression::Lz4
    );
}
