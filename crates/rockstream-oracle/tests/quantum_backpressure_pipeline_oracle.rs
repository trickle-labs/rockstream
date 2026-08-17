//! v0.51 pipeline-level oracles for row-budget/quantum-coupled exchange
//! delivery (Slice 4/5) and durable-fallback delivery under fast-path failure.
//!
//! Both oracles drive the same Filter -> Join -> Aggregate 2-shard DAG used by
//! `fast_path_wal_elision_pipeline_oracle.rs`, but over a *real* gRPC
//! `WorkerStreamMultiplexer` <-> `ShuffleServer` round trip so that
//! `FlowController` row-budget acquire/release and durable object-store
//! fallback routing are genuinely exercised, not just modeled locally.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::serialize_zset;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};
use tokio::sync::mpsc;

const EXCHANGE_ID: u64 = 900;
const VALUE_THRESHOLD: i64 = 3;
const KEY_SPACE: i64 = 12;
const CATEGORY_MOD: i64 = 4;

fn lit(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

fn value_ge_threshold() -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Ge,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(VALUE_THRESHOLD)),
    }
}

fn static_dim_table() -> ArrowZSet {
    let rows: Vec<(i64, i64)> = (0..KEY_SPACE).map(|k| (k, k % CATEGORY_MOD)).collect();
    ArrowZSet::from_ab_rows(&rows, 1)
}

fn project_to_category_value(join_out: &ArrowZSet) -> ArrowZSet {
    if join_out.is_empty() {
        return ArrowZSet::from_ab_weighted(&[]);
    }
    let value_col = join_out
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let category_col = join_out
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rows: Vec<(i64, i64, i64)> = (0..join_out.num_rows())
        .map(|i| {
            (
                category_col.value(i),
                value_col.value(i),
                join_out.weights[i],
            )
        })
        .collect();
    ArrowZSet::from_ab_weighted(&rows)
}

fn accumulate_full_rows(zset: &ArrowZSet, acc: &mut BTreeMap<Vec<i64>, i64>) {
    if zset.is_empty() {
        return;
    }
    let ncols = zset.data.num_columns();
    for i in 0..zset.num_rows() {
        let key: Vec<i64> = (0..ncols)
            .map(|c| {
                let column = zset.data.column(c);
                if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                    values.value(i)
                } else if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
                    values.value(i).to_bits() as i64
                } else {
                    panic!("aggregate oracle received unsupported output type")
                }
            })
            .collect();
        let entry = acc.entry(key.clone()).or_insert(0);
        *entry += zset.weights[i];
        if *entry == 0 {
            acc.remove(&key);
        }
    }
}

/// One randomized CRUD epoch on relation R (key -> value). Returns the delta as
/// a Z-set of (key, value, weight) rows and updates the live model in place.
fn random_epoch_delta(rng: &mut StdRng, live: &mut BTreeMap<i64, i64>) -> ArrowZSet {
    let choice = rng.gen_range(0..3);
    let mut rows: Vec<(i64, i64, i64)> = Vec::new();
    match choice {
        0 => {
            let key = rng.gen_range(0..KEY_SPACE);
            let value = rng.gen_range(0..8);
            if let Some(old) = live.get(&key).copied() {
                rows.push((key, old, -1));
            }
            rows.push((key, value, 1));
            live.insert(key, value);
        }
        1 => {
            if let Some((&key, &old)) = live.iter().next() {
                let value = rng.gen_range(0..8);
                rows.push((key, old, -1));
                rows.push((key, value, 1));
                live.insert(key, value);
            }
        }
        _ => {
            if let Some((&key, &old)) = live.iter().next() {
                rows.push((key, old, -1));
                live.remove(&key);
            }
        }
    }
    ArrowZSet::from_ab_weighted(&rows)
}

fn agg_input_schema() -> Arc<arrow::datatypes::Schema> {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
    ]))
}

/// Batch reference: run the full Filter -> Join -> Aggregate pipeline once over
/// the net input relation, with no exchange and no restart.
fn batch_recompute(net_input: &ArrowZSet) -> BTreeMap<Vec<i64>, i64> {
    let filter = FilterOp::new(value_ge_threshold());
    let join = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    let aggregate = AggregateOp::new(OperatorId(2));

    join.process_right_delta(static_dim_table()).unwrap();
    join.commit_epoch().unwrap();

    let filtered = filter.apply(net_input.clone()).unwrap();
    let join_out = join.process_left_delta(filtered).unwrap();
    join.commit_epoch().unwrap();
    let projected = project_to_category_value(&join_out);
    let agg_out = aggregate.process_delta(projected).unwrap();

    let mut acc = BTreeMap::new();
    accumulate_full_rows(&agg_out, &mut acc);
    acc
}

fn join_left_only(join: &JoinOp, left_delta: ArrowZSet) -> ArrowZSet {
    let out = join.process_left_delta(left_delta).unwrap();
    let correction = join.commit_epoch().unwrap();
    debug_assert_eq!(correction.num_rows(), 0);
    out
}

/// Split a projected epoch delta into row-count-bounded chunks (<= `max_rows`
/// each), preserving row order. This models the sender-side rechunking that
/// keeps every shuffle frame within `worker.max_rows_per_quantum`.
fn rechunk(zset: &ArrowZSet, max_rows: usize) -> Vec<ArrowZSet> {
    if zset.is_empty() {
        return Vec::new();
    }
    let ncols = zset.data.num_columns();
    let cols: Vec<&Int64Array> = (0..ncols)
        .map(|c| {
            zset.data
                .column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        })
        .collect();
    let mut chunks = Vec::new();
    let mut rows = Vec::new();
    for i in 0..zset.num_rows() {
        let mut row: Vec<i64> = cols.iter().map(|c| c.value(i)).collect();
        row.push(zset.weights[i]);
        rows.push((row[0], row[1], row[2]));
        if rows.len() == max_rows {
            chunks.push(ArrowZSet::from_ab_weighted(&rows));
            rows = Vec::new();
        }
    }
    if !rows.is_empty() {
        chunks.push(ArrowZSet::from_ab_weighted(&rows));
    }
    chunks
}

async fn open_db(name: &str, store: Arc<dyn ObjectStore>) -> ShardDb {
    ShardDb::builder(name, store).build().await.unwrap()
}

fn make_receiver_registry(target_db: ShardDb) -> (ExchangeRegistry, mpsc::Receiver<ArrowZSet>) {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), WorkerId(2), LeaseToken(1)),
            db: Some(target_db),
        },
    );
    let registry = ExchangeRegistry::with_shards(Arc::new(parking_lot::RwLock::new(shards)));
    let (tx, rx) = mpsc::channel(1024);
    registry.register(EXCHANGE_ID, 1, tx, agg_input_schema());
    (registry, rx)
}

fn make_sender_shards(src_db: ShardDb) -> Arc<parking_lot::RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), WorkerId(1), LeaseToken(1)),
            db: Some(src_db),
        },
    );
    Arc::new(parking_lot::RwLock::new(shards))
}

fn worker_info(worker_id: u64, address: &str, host_id: &str, az: &str, shm: bool) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address: address.to_string(),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new(host_id, az),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: shm,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

/// A row-budget/quantum-coupled real gRPC exchange DAG: every epoch's join
/// output is rechunked into `MAX_ROWS_PER_QUANTUM`-bounded frames before being
/// sent over `WorkerStreamMultiplexer::send_frame`, which blocks on
/// `FlowController::acquire_credit` when the row budget is exhausted and only
/// proceeds once the receiver's `ShuffleAck` releases row credits. Different
/// chunking / suspension points must not change the final result.
#[tokio::test]
async fn quantum_coupled_exchange_rechunking_incremental_equals_batch() {
    const EPOCHS: u64 = 40;
    const MAX_ROWS_PER_QUANTUM: usize = 3;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let src_db = open_db("quantum-src", store.clone()).await;
    let target_db = open_db("quantum-dst", store.clone()).await;
    let target_handle = target_db.clone();
    let (registry, mut inlet_rx) = make_receiver_registry(target_db);

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let server = ShuffleServer::new(registry);
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(902), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    // Distinct hosts, no SHM -> forces the direct-gRPC path, where row-budget
    // credit acquire/release is actually wired in (Slice 4).
    pool.set_local_worker_info(worker_info(901, "127.0.0.1:9901", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(902, &addr.to_string(), "host-b", "az-1", false));
    let flow_controller = FlowController::with_row_budget(MAX_ROWS_PER_QUANTUM as u32);
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        flow_controller.clone(),
        make_sender_shards(src_db.clone()),
    )
    .with_src_worker(WorkerId(901));

    let filter = FilterOp::new(value_ge_threshold());
    let join = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    join.process_right_delta(static_dim_table()).unwrap();
    join.commit_epoch().unwrap();
    let aggregate = AggregateOp::new(OperatorId(2));

    let mut rng = StdRng::seed_from_u64(0xA11CE);
    let mut live: BTreeMap<i64, i64> = BTreeMap::new();
    let mut net_input: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut incremental_agg: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    let mut seq = 0u64;

    for epoch in 1..=EPOCHS {
        let delta = random_epoch_delta(&mut rng, &mut live);
        delta.accumulate_ab(&mut net_input);

        let filtered = filter.apply(delta).unwrap();
        let join_out = join_left_only(&join, filtered);
        let projected = project_to_category_value(&join_out);

        // Rechunk the epoch's payload into row-budget-bounded frames and send
        // each one; a chunk larger than the previous chunk's in-flight rows
        // will suspend on `acquire_credit` until the prior frame's ack lands.
        for chunk in rechunk(&projected, MAX_ROWS_PER_QUANTUM) {
            let row_count = chunk.num_rows() as u32;
            assert!(
                row_count as usize <= MAX_ROWS_PER_QUANTUM,
                "rechunk must respect max_rows_per_quantum"
            );
            seq += 1;
            let payload = serialize_zset(&chunk).unwrap();
            multiplexer
                .send_frame(
                    WorkerId(902),
                    ShuffleFrame {
                        exchange_id: EXCHANGE_ID,
                        src_shard: 0,
                        target_shard: 1,
                        epoch,
                        seq,
                        payload: payload.into(),
                        row_count,
                    },
                )
                .await
                .unwrap();
        }

        // Drain whatever arrived so far into the aggregate operator.
        while let Ok(received) = inlet_rx.try_recv() {
            let agg_out = aggregate.process_delta(received).unwrap();
            accumulate_full_rows(&agg_out, &mut incremental_agg);
        }
    }

    // Final drain in case any deliveries are still in flight.
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            if let Some(received) = inlet_rx.recv().await {
                let agg_out = aggregate.process_delta(received).unwrap();
                accumulate_full_rows(&agg_out, &mut incremental_agg);
            }
        }
    })
    .await
    .ok();

    let _ = tx_close.send(());
    server_handle.abort();
    let _ = target_handle;

    let net_rows: Vec<(i64, i64, i64)> = net_input.iter().map(|(&(a, b), &w)| (a, b, w)).collect();
    let net_zset = ArrowZSet::from_ab_weighted(&net_rows);
    let batch_agg = batch_recompute(&net_zset);

    assert_eq!(
        incremental_agg, batch_agg,
        "quantum-coupled rechunked delivery must equal batch recomputation regardless of chunking/suspension points"
    );
    assert!(!batch_agg.is_empty(), "expected non-empty aggregate output");
}

/// With the fast (direct-gRPC / same-host) paths unavailable — modeling an
/// induced connectivity failure by leaving the peer unresolvable — every epoch
/// routes through the durable object-store fallback. Fallback delivery plus a
/// single `catch_up_durable()` replay must still produce exactly the same
/// final rows as batch recomputation.
#[tokio::test]
async fn durable_fallback_after_fast_path_failure_incremental_equals_batch() {
    const EPOCHS: u64 = 40;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable_dir = tempfile::tempdir().unwrap();
    let durable_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(durable_dir.path()).unwrap());

    let src_db = open_db("durable-fallback-src", store.clone()).await;
    let target_db = open_db("durable-fallback-dst", store.clone()).await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());

    // No peer address is registered for worker 912, so gRPC connectivity is
    // impossible -> every send_frame falls back to the durable object store.
    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(911, "127.0.0.1:9911", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(912, "127.0.0.1:9912", "host-b", "az-2", false));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(src_db.clone()),
    )
    .with_object_store(durable_store.clone())
    .with_src_worker(WorkerId(911));

    let filter = FilterOp::new(value_ge_threshold());
    let join = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    join.process_right_delta(static_dim_table()).unwrap();
    join.commit_epoch().unwrap();
    let aggregate = AggregateOp::new(OperatorId(2));

    let mut rng = StdRng::seed_from_u64(0xFEED5);
    let mut live: BTreeMap<i64, i64> = BTreeMap::new();
    let mut net_input: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut incremental_agg: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    let mut sent_epochs: Vec<u64> = Vec::new();

    for epoch in 1..=EPOCHS {
        let delta = random_epoch_delta(&mut rng, &mut live);
        delta.accumulate_ab(&mut net_input);

        let filtered = filter.apply(delta).unwrap();
        let join_out = join_left_only(&join, filtered);
        let projected = project_to_category_value(&join_out);
        if projected.is_empty() {
            // Nothing to shuffle this epoch; an empty Z-set has no schema to
            // serialize/deserialize, so skip sending a frame for it.
            continue;
        }
        let row_count = projected.num_rows() as u32;
        let payload = serialize_zset(&projected).unwrap();

        multiplexer
            .send_frame(
                WorkerId(912),
                ShuffleFrame {
                    exchange_id: EXCHANGE_ID,
                    src_shard: 0,
                    target_shard: 1,
                    epoch,
                    seq: epoch,
                    payload: payload.into(),
                    row_count,
                },
            )
            .await
            .unwrap();
        // The fallback path never opens a direct gRPC stream.
        assert_eq!(multiplexer.connection_count(), 0);
        sent_epochs.push(epoch);
    }

    // Nothing is delivered to the inlet until the durable object(s) are caught
    // up; the fast path never ran.
    tokio::select! {
        maybe = inlet_rx.recv() => panic!("unexpected direct delivery without catch-up: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }

    // catch_up_durable() catches up exactly one epoch's durable object per
    // call (the durable path is keyed by exchange/epoch/src/target), so replay
    // every epoch that was actually shuffled.
    for &epoch in &sent_epochs {
        multiplexer
            .catch_up_durable(
                EXCHANGE_ID,
                epoch,
                WorkerId(911),
                WorkerId(912),
                &registry,
                durable_store.as_ref(),
            )
            .await
            .unwrap();
    }

    while let Ok(received) = inlet_rx.try_recv() {
        let agg_out = aggregate.process_delta(received).unwrap();
        accumulate_full_rows(&agg_out, &mut incremental_agg);
    }

    // A repeat catch-up must not re-deliver (idempotent durable replay).
    for &epoch in &sent_epochs {
        multiplexer
            .catch_up_durable(
                EXCHANGE_ID,
                epoch,
                WorkerId(911),
                WorkerId(912),
                &registry,
                durable_store.as_ref(),
            )
            .await
            .unwrap();
    }
    tokio::select! {
        maybe = inlet_rx.recv() => panic!("unexpected duplicate durable re-delivery: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }

    let net_rows: Vec<(i64, i64, i64)> = net_input.iter().map(|(&(a, b), &w)| (a, b, w)).collect();
    let net_zset = ArrowZSet::from_ab_weighted(&net_rows);
    let batch_agg = batch_recompute(&net_zset);

    assert_eq!(
        incremental_agg, batch_agg,
        "durable fallback delivery under induced fast-path failure must equal batch recomputation"
    );
    assert!(!batch_agg.is_empty(), "expected non-empty aggregate output");
}
