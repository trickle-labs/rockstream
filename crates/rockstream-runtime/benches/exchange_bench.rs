//! Exchange/shuffle performance regression benchmark suite (v0.45.4 + v0.51).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, Throughput};
use object_store::memory::InMemory;
use parking_lot::RwLock;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::compression_tuner::CompressionTuner;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::persistence::{persist_inbox, persist_outbox};
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleAck;
use rockstream_runtime::exchange::serialization::{
    deserialize_zset, frame_payload_bytes, serialize_zset,
};
use rockstream_runtime::exchange::service::ExchangeRegistry;
use rockstream_runtime::exchange::service::ShuffleServer;
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::config::{AutotunerConfig, ExchangeConfig, WorkerConfig};
use rockstream_types::exchange::ShuffleCompression;
use rockstream_types::ids::{ExchangeId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Representative shuffle-batch row count (mirrors `perf_regression.rs`'s
/// sizing conventions).
const SHUFFLE_BATCH_ROWS: usize = 10_000;
const COMPLEX_DAG_ROWS: usize = 4_096;

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

fn worker_info(worker_id: u64, address: String, host_id: &str) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address,
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new(host_id, "az-1"),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: false,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

async fn shard_db(name: &str) -> ShardDb {
    ShardDb::builder(name, Arc::new(InMemory::new()))
        .build()
        .await
        .unwrap()
}

struct BenchServer {
    addr: std::net::SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

async fn spawn_server(registry: ExchangeRegistry) -> BenchServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let server = ShuffleServer::new(registry);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                    server,
                ),
            )
            .serve(addr)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    BenchServer { addr, handle }
}

struct ComplexDagBench {
    source_db: ShardDb,
    target_db: ShardDb,
    target_rx: mpsc::Receiver<ArrowZSet>,
    source_mux: WorkerStreamMultiplexer,
    payload: Vec<u8>,
    row_count: u32,
    legacy_wal_on: bool,
    next_seq: u64,
    server: tokio::task::JoinHandle<()>,
}

impl ComplexDagBench {
    async fn new(legacy_wal_on: bool) -> Self {
        let batch = make_wide_shuffle_batch(COMPLEX_DAG_ROWS);
        let payload = serialize_zset(&batch).unwrap().to_vec();

        let source_db = shard_db("bench-complex-source").await;
        let target_db = shard_db("bench-complex-target").await;
        let registry = ExchangeRegistry::new();
        let (target_tx, target_rx) = mpsc::channel(16);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for exchange_id in [100_u64, 101, 102] {
            registry.register(exchange_id, 1, target_tx.clone(), schema.clone());
        }

        let target_server = spawn_server(registry).await;

        let source_peers = Arc::new(RwLock::new(HashMap::new()));
        source_peers
            .write()
            .insert(WorkerId(702), target_server.addr.to_string());
        let source_pool = ShuffleClientPool::new(source_peers);
        source_pool.set_local_worker_info(worker_info(701, "127.0.0.1:9701".to_string(), "host-a"));
        source_pool.upsert_peer_info(worker_info(702, target_server.addr.to_string(), "host-b"));
        let source_mux = WorkerStreamMultiplexer::new(
            source_pool,
            FlowController::with_row_budget(COMPLEX_DAG_ROWS as u32),
        )
        .with_src_worker(WorkerId(701))
        .with_exchange_config(ExchangeConfig {
            exchange_direct_threshold_bytes: usize::MAX,
            ..ExchangeConfig::default()
        })
        .with_worker_config(WorkerConfig {
            segment_cache_bytes: 0,
            max_rows_per_quantum: COMPLEX_DAG_ROWS,
        });

        Self {
            source_db,
            target_db,
            target_rx,
            source_mux,
            payload,
            row_count: batch.num_rows() as u32,
            legacy_wal_on,
            next_seq: 1,
            server: target_server.handle,
        }
    }

    async fn run_once(&mut self) {
        for exchange_id in [100_u64, 101, 102] {
            let seq = self.next_seq;
            self.next_seq += 1;
            if self.legacy_wal_on {
                persist_outbox(&self.source_db, exchange_id, 0, 1, seq, &self.payload)
                    .await
                    .unwrap();
            }
            self.source_mux
                .send_frame(
                    WorkerId(702),
                    rockstream_runtime::exchange::proto::ShuffleFrame {
                        exchange_id,
                        src_shard: 0,
                        target_shard: 1,
                        epoch: 1,
                        seq,
                        payload: self.payload.clone(),
                        row_count: self.row_count,
                    },
                )
                .await
                .unwrap();
            let received = self.target_rx.recv().await.unwrap();
            if self.legacy_wal_on {
                persist_inbox(&self.target_db, exchange_id, 0, 1, seq, &self.payload)
                    .await
                    .unwrap();
            }
            assert_eq!(serialize_zset(&received).unwrap(), self.payload);
        }
    }
}

impl Drop for ComplexDagBench {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn bench_serialize_zset(c: &mut Criterion) {
    let zset = make_kv_batch(SHUFFLE_BATCH_ROWS);

    let mut group = c.benchmark_group("exchange_serialize_zset");
    group.throughput(Throughput::Elements(SHUFFLE_BATCH_ROWS as u64));
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
                fc.acquire_credit(0, 0, 0, 1).await.unwrap();
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

fn bench_transport_codec_claims(c: &mut Criterion) {
    let batch = make_wide_shuffle_batch(SHUFFLE_BATCH_ROWS * 2);
    let raw = serialize_zset(&batch).unwrap();

    let mut group = c.benchmark_group("exchange_transport_claims");
    group.throughput(Throughput::Bytes(raw.len() as u64));
    group.bench_function("direct_raw_baseline", |b| {
        b.iter(|| serialize_zset(&batch).unwrap());
    });
    group.bench_function("direct_lz4_codec_v1", |b| {
        b.iter(|| frame_payload_bytes(&raw, ShuffleCompression::Lz4, true).unwrap());
    });
    group.bench_function("durable_zstd_codec_v1", |b| {
        b.iter(|| frame_payload_bytes(&raw, ShuffleCompression::Zstd, true).unwrap());
    });
    group.finish();
}

fn measure_direct_lz4_epoch_cpu_ms(payload: &[u8], epochs: usize) -> u64 {
    let started = std::time::Instant::now();
    for _ in 0..epochs {
        let framed = frame_payload_bytes(payload, ShuffleCompression::Lz4, true).unwrap();
        criterion::black_box(framed);
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    (elapsed_ms / epochs as f64).ceil() as u64
}

fn bench_direct_lz4_epoch_cpu_budget(c: &mut Criterion) {
    let batch = make_wide_shuffle_batch(SHUFFLE_BATCH_ROWS * 2);
    let raw = serialize_zset(&batch).unwrap();
    let exchange = ExchangeConfig::default();
    let autotuner = AutotunerConfig::default();

    let mut group = c.benchmark_group("exchange_bench_direct_lz4_epoch_cpu_budget");
    group.throughput(Throughput::Bytes(raw.len() as u64));
    group.bench_function("direct_lz4_epoch_cpu_ms", |b| {
        b.iter(|| {
            let cpu_ms = measure_direct_lz4_epoch_cpu_ms(&raw, 8);
            let tuner = CompressionTuner::new(exchange.clone(), autotuner.clone());
            criterion::black_box(tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, cpu_ms))
        });
    });
    group.finish();
}

fn bench_complex_dag_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_complex_dag_hot_path");
    group.sample_size(10);
    group.throughput(Throughput::Elements((COMPLEX_DAG_ROWS * 3) as u64));
    for (id, legacy_wal_on) in [
        ("legacy_wal_on", true),
        ("wal_elided_quantum_coupled", false),
    ] {
        group.bench_function(id, |b| {
            let rt = Runtime::new().expect("build tokio runtime");
            b.iter(|| {
                rt.block_on(async {
                    let mut dag = ComplexDagBench::new(legacy_wal_on).await;
                    dag.run_once().await;
                });
            });
        });
    }
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
    bench_transport_codec_claims(&mut criterion);
    bench_direct_lz4_epoch_cpu_budget(&mut criterion);
    bench_complex_dag_hot_path(&mut criterion);
    criterion.final_summary();

    let summary = rockstream_ops::bench_regression::collect_criterion_summary(
        &default_criterion_dir(),
        &[
            "exchange_serialize_zset",
            "exchange_deserialize_zset",
            "exchange_flow_control",
            "exchange_transport_claims",
            "exchange_bench_direct_lz4_epoch_cpu_budget",
            "exchange_complex_dag_hot_path",
        ],
    );
    println!(
        "[bench_summary:runtime] {}",
        serde_json::to_string(&summary).unwrap()
    );
}
