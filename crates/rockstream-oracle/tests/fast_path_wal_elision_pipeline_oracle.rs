//! v0.51 pipeline-level oracle for fast-path shuffle WAL elision.
//!
//! A Filter -> Join -> Aggregate 2-shard DAG is driven incrementally through
//! the real same-worker loopback exchange fast path (which, under v0.51 Slices
//! 1-3, no longer persists `shuffle_outbox/` / `shuffle_inbox/` on success and
//! dedups replays via the committed frontier). After a random sequence of
//! INSERT / UPDATE / DELETE epochs and a restart boundary, the incrementally
//! materialized aggregate must equal a from-scratch batch recomputation of the
//! same net input.
//!
//! Shard layout:
//!   * Shard 0 (source worker): Filter -> Join(⋈ static dim table S).
//!   * Exchange: join output projected to (category, value) is shuffled to the
//!     aggregate shard over the loopback fast path (WAL elided).
//!   * Shard 1 (aggregate worker): group-by-category SUM/COUNT aggregate.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::Int64Array;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::loopback::LoopbackRouter;
use rockstream_runtime::exchange::service::ExchangeRegistry;
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use tokio::sync::mpsc;

const EXCHANGE_ID: u64 = 700;
const VALUE_THRESHOLD: i64 = 3;
const KEY_SPACE: i64 = 12;
const CATEGORY_MOD: i64 = 4;

fn lit(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

/// Filter predicate: keep rows whose value column (col 1) is >= threshold.
fn value_ge_threshold() -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Ge,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(VALUE_THRESHOLD)),
    }
}

/// Static dimension table S: key -> category (category = key % CATEGORY_MOD).
fn static_dim_table() -> ArrowZSet {
    let rows: Vec<(i64, i64)> = (0..KEY_SPACE).map(|k| (k, k % CATEGORY_MOD)).collect();
    ArrowZSet::from_ab_rows(&rows, 1)
}

/// Project a join output batch [l_key, l_value, r_key, r_category] down to the
/// aggregate input schema (a = category, b = value), preserving weights.
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

/// Accumulate all rows of an aggregate-output ZSet (4 Int64 columns) into a
/// net weight map keyed by the full row. Zero-net rows are dropped, so the
/// resulting map is the materialized aggregate relation.
fn accumulate_full_rows(zset: &ArrowZSet, acc: &mut BTreeMap<Vec<i64>, i64>) {
    if zset.is_empty() {
        return;
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
    for i in 0..zset.num_rows() {
        let key: Vec<i64> = cols.iter().map(|c| c.value(i)).collect();
        let entry = acc.entry(key.clone()).or_insert(0);
        *entry += zset.weights[i];
        if *entry == 0 {
            acc.remove(&key);
        }
    }
}

/// One randomized CRUD epoch on relation R (key -> value). Returns the delta as
/// a Z-set of (key, value, weight) rows and updates the live model in place.
fn random_epoch_delta(rng: &mut StdRng, live: &mut HashMap<i64, i64>) -> ArrowZSet {
    let choice = rng.gen_range(0..3);
    let mut rows: Vec<(i64, i64, i64)> = Vec::new();
    match choice {
        // INSERT (or overwrite-as-insert of a fresh key).
        0 => {
            let key = rng.gen_range(0..KEY_SPACE);
            let value = rng.gen_range(0..8);
            if let Some(old) = live.get(&key).copied() {
                // Key already live: model an UPDATE (retract old, insert new).
                rows.push((key, old, -1));
            }
            rows.push((key, value, 1));
            live.insert(key, value);
        }
        // UPDATE an existing key (no-op if none live).
        1 => {
            if let Some((&key, &old)) = live.iter().next() {
                let value = rng.gen_range(0..8);
                rows.push((key, old, -1));
                rows.push((key, value, 1));
                live.insert(key, value);
            }
        }
        // DELETE an existing key (no-op if none live).
        _ => {
            if let Some((&key, &old)) = live.iter().next() {
                rows.push((key, old, -1));
                live.remove(&key);
            }
        }
    }
    ArrowZSet::from_ab_weighted(&rows)
}

async fn open_db(name: &str, store: Arc<dyn ObjectStore>) -> ShardDb {
    ShardDb::builder(name, store).build().await.unwrap()
}

fn agg_input_schema() -> Arc<arrow::datatypes::Schema> {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
    ]))
}

fn build_shard_map(
    src_db: ShardDb,
    target_db: ShardDb,
) -> Arc<RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), WorkerId(1), LeaseToken(1)),
            db: Some(src_db),
        },
    );
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), WorkerId(2), LeaseToken(1)),
            db: Some(target_db),
        },
    );
    Arc::new(RwLock::new(shards))
}

/// Batch reference: run the full Filter -> Join -> Aggregate pipeline once over
/// the net input relation, with no exchange and no restart.
fn batch_recompute(net_input: &ArrowZSet) -> BTreeMap<Vec<i64>, i64> {
    let filter = FilterOp::new(value_ge_threshold());
    let join = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    let aggregate = AggregateOp::new(OperatorId(2));

    // Load the static dimension table into the join's right arrangement.
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

#[tokio::test]
async fn exchange_fast_path_wal_elision_incremental_equals_batch_for_join_agg_dag() {
    const EPOCHS: u64 = 40;
    const RESTART_AT: u64 = 21;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut src_db = open_db("oracle-src", store.clone()).await;
    let mut target_db = open_db("oracle-dst", store.clone()).await;

    // Shard 0 operators: Filter -> Join(⋈ S).
    let filter = FilterOp::new(value_ge_threshold());
    let join = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    join.process_right_delta(static_dim_table()).unwrap();
    join.commit_epoch().unwrap();

    // Shard 1 operator: aggregate, fed from the exchange inlet.
    let aggregate = AggregateOp::new(OperatorId(2));

    // Exchange registry + inlet on the aggregate shard. These in-memory objects
    // are NOT restarted; only the durable shard DBs (frontier state) restart.
    let registry =
        ExchangeRegistry::with_shards(build_shard_map(src_db.clone(), target_db.clone()));
    let (inlet_tx, mut inlet_rx) = mpsc::channel::<ArrowZSet>(1024);
    registry.register(EXCHANGE_ID, 1, inlet_tx, agg_input_schema());

    let mut shard_map = build_shard_map(src_db.clone(), target_db.clone());
    let mut router = LoopbackRouter::new(registry.clone(), shard_map.clone());

    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let mut live: HashMap<i64, i64> = HashMap::new();
    let mut net_input: BTreeMap<(i64, i64), i64> = BTreeMap::new();

    let mut incremental_agg: BTreeMap<Vec<i64>, i64> = BTreeMap::new();

    for epoch in 1..=EPOCHS {
        let delta = random_epoch_delta(&mut rng, &mut live);
        delta.accumulate_ab(&mut net_input);

        drive_epoch(
            &filter,
            &join,
            &router,
            &aggregate,
            &mut inlet_rx,
            &mut incremental_agg,
            epoch,
            delta.clone(),
        )
        .await;

        if epoch == RESTART_AT {
            // The aggregate shard checkpoints this epoch: its committed frontier
            // is durably advanced. No fast-path shuffle WAL exists.
            target_db.commit_epoch(ShardId(1), epoch).await.unwrap();
            assert_eq!(
                target_db.scan_prefix(&[0x04]).await.unwrap().len(),
                0,
                "receiver must not persist shuffle_inbox on the loopback fast path"
            );
            assert_eq!(
                src_db.scan_prefix(&[0x05]).await.unwrap().len(),
                0,
                "sender must not persist shuffle_outbox on the loopback fast path"
            );

            // --- Restart boundary: reopen the durable shard DBs. ---
            src_db.close().await.unwrap();
            target_db.close().await.unwrap();
            src_db = open_db("oracle-src", store.clone()).await;
            target_db = open_db("oracle-dst", store.clone()).await;
            shard_map = build_shard_map(src_db.clone(), target_db.clone());
            router = LoopbackRouter::new(registry.clone(), shard_map.clone());

            // Source replays the just-committed epoch. The restored frontier
            // (== RESTART_AT) must dedup it so the aggregate is NOT double-fed.
            let replay_filtered = filter.apply(delta.clone()).unwrap();
            let replay_join = join_left_only(&join, replay_filtered);
            let replay_proj = project_to_category_value(&replay_join);
            router
                .route_loopback(EXCHANGE_ID, 0, 1, epoch, epoch, &replay_proj)
                .await
                .unwrap();
            // Drain: nothing should have been delivered (deduped by frontier).
            let mut redelivered = 0usize;
            while let Ok(z) = inlet_rx.try_recv() {
                redelivered += z.num_rows();
            }
            assert_eq!(
                redelivered, 0,
                "frontier dedup must suppress replay of an already-committed epoch"
            );
        }
    }

    // Final consistency: no fast-path shuffle WAL was ever persisted.
    assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
    assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);

    // Batch recomputation over the net input.
    let net_rows: Vec<(i64, i64, i64)> = net_input.iter().map(|(&(a, b), &w)| (a, b, w)).collect();
    let net_zset = ArrowZSet::from_ab_weighted(&net_rows);
    let batch_agg = batch_recompute(&net_zset);

    assert_eq!(
        incremental_agg, batch_agg,
        "incremental output through the WAL-elided fast path must equal batch recomputation"
    );
    // Sanity: the pipeline actually produced aggregate groups.
    assert!(!batch_agg.is_empty(), "expected non-empty aggregate output");
}

/// Run Filter -> Join(left delta) -> project -> exchange -> Aggregate for one
/// epoch and fold the aggregate output into the running materialization.
#[allow(clippy::too_many_arguments)]
async fn drive_epoch(
    filter: &FilterOp,
    join: &JoinOp,
    router: &LoopbackRouter,
    aggregate: &AggregateOp,
    inlet_rx: &mut mpsc::Receiver<ArrowZSet>,
    incremental_agg: &mut BTreeMap<Vec<i64>, i64>,
    epoch: u64,
    delta: ArrowZSet,
) {
    let filtered = filter.apply(delta).unwrap();
    let join_out = join_left_only(join, filtered);
    let projected = project_to_category_value(&join_out);
    router
        .route_loopback(EXCHANGE_ID, 0, 1, epoch, epoch, &projected)
        .await
        .unwrap();
    while let Ok(received) = inlet_rx.try_recv() {
        let agg_out = aggregate.process_delta(received).unwrap();
        accumulate_full_rows(&agg_out, incremental_agg);
    }
}

/// Process a left delta against the (static) right arrangement and commit the
/// (empty) correction, returning the join output for this delta.
fn join_left_only(join: &JoinOp, left_delta: ArrowZSet) -> ArrowZSet {
    let out = join.process_left_delta(left_delta).unwrap();
    // Commit clears staged left rows; right staged is empty so ΔL⋈ΔR = ∅.
    let correction = join.commit_epoch().unwrap();
    debug_assert_eq!(correction.num_rows(), 0);
    out
}
