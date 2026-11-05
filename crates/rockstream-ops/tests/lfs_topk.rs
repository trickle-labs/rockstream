//! LFS durability tests for `TopKOp` (v0.12 — IVM-9).
//!
//! ## Tests
//!
//! 1. `lfs_topk_state_persists` — arrangement (buffer) survives ShardDb close/reopen.
//!
//! 2. `lfs_topk_crash_replay` — WAL replay; output bit-identical to non-crash path.
//!
//! 3. `lfs_topk_no_range_delete` — WriteBatch has no DeleteRange (structural guarantee).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::topk::{
    append_topk_state, load_topk_state, persist_topk_state, TopKOp, TOPK_BUFFER_LIMIT,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn schema_kv() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false), // rank_col = 0
        Field::new("id", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let id: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let data = RecordBatch::try_new(
        schema_kv(),
        vec![
            Arc::new(Int64Array::from(v)) as ArrayRef,
            Arc::new(Int64Array::from(id)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn accumulate_vals(state: &mut HashMap<i64, i64>, zset: &ArrowZSet) {
    if zset.is_empty() {
        return;
    }
    let col = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..zset.num_rows() {
        *state.entry(col.value(i)).or_insert(0) += zset.weights[i];
    }
}

fn live_vals(state: &HashMap<i64, i64>) -> Vec<i64> {
    let mut vals: Vec<i64> = state
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&v, _)| v)
        .collect();
    vals.sort_by(|a, b| b.cmp(a));
    vals
}

async fn open_shard(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

fn weighted_rows(zset: &ArrowZSet) -> BTreeMap<(i64, i64), i64> {
    let values = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ids = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = BTreeMap::new();
    for (i, weight) in zset.weights.iter().enumerate() {
        *rows.entry((values.value(i), ids.value(i))).or_insert(0) += weight;
    }
    rows.retain(|_, weight| *weight != 0);
    rows
}

#[tokio::test]
async fn compiled_topk_restore_discards_uncommitted_delta() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let id = OperatorId(481);
    let op = Arc::new(TopKOp::new(schema_kv(), 2, 0, vec![]));
    let pipeline = rockstream_ops::live_exec::StatefulPipeline::new()
        .push(rockstream_ops::live_exec::Stage::TopK(op.clone(), id));
    pipeline
        .process(make_input(&[(1, 1, 1), (2, 2, 1), (3, 3, 1)]))
        .unwrap();
    persist_topk_state(&db, &op, id).await.unwrap();
    pipeline.process(make_input(&[(4, 4, 1)])).unwrap();
    db.set_fail_scans(true);
    assert!(pipeline.restore(&db).await.is_err());
    db.set_fail_scans(false);
    pipeline.restore(&db).await.unwrap();
    let output = pipeline.process(make_input(&[(4, 4, 1)])).unwrap();
    assert_eq!(
        weighted_rows(&output),
        BTreeMap::from([((2, 2), -1), ((4, 4), 1)])
    );
    let records = db
        .scan_prefix(&rockstream_storage::ShardKeyEncoder::topk_op_prefix(id.0))
        .await
        .unwrap();
    db.put(&records[0].0, b"invalid").await.unwrap();
    assert!(pipeline.restore(&db).await.is_err());
    let output = pipeline.process(make_input(&[(5, 5, 1)])).unwrap();
    assert_eq!(
        weighted_rows(&output),
        BTreeMap::from([((3, 3), -1), ((5, 5), 1)])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn spilled_topk_restores_checkpoint_after_abandoned_update() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let id = OperatorId(482);
    let op = Arc::new(TopKOp::new(schema_kv(), 2, 0, vec![1]).with_db(db.clone()));
    let mut initial: Vec<_> = (1..=100_001).map(|rank| (rank, 0, 1)).collect();
    initial.extend([(4, 1, 1), (5, 1, 1), (6, 1, 1)]);
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&initial), 1).unwrap()),
        BTreeMap::from([
            ((100_001, 0), 1),
            ((100_000, 0), 1),
            ((5, 1), 1),
            ((6, 1), 1)
        ])
    );
    let other = TopKOp::new(schema_kv(), 1, 0, vec![1]).with_db(db.clone());
    let other_input: Vec<_> = (1..=100_001).map(|rank| (rank, 9, 1)).collect();
    assert_eq!(
        weighted_rows(&other.process_epoch(make_input(&other_input), 1).unwrap()),
        BTreeMap::from([((100_001, 9), 1)])
    );
    let other_spill = db
        .scan_prefix(b"topk_spill:")
        .await
        .unwrap()
        .into_iter()
        .find(|(_, value)| value[24..32] == 9i64.to_be_bytes())
        .unwrap();
    persist_topk_state(&db, &op, id).await.unwrap();
    assert_eq!(
        weighted_rows(
            &op.process_epoch(make_input(&[(100_001, 0, -1)]), 2)
                .unwrap()
        ),
        BTreeMap::from([((100_001, 0), -1), ((99_999, 0), 1)])
    );
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(0, 0, 1)]), 2).unwrap()),
        BTreeMap::new()
    );
    db.set_fail_scans(true);
    assert!(
        matches!(op.restore_in_place(&db, id).await, Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::Unsupported(message), .. }) if message == "injected storage scan failure")
    );
    db.set_fail_scans(false);
    db.set_fail_writes(true);
    assert!(
        matches!(op.restore_in_place(&db, id).await, Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::Unsupported(message), .. }) if message == "injected storage write failure")
    );
    db.set_fail_writes(false);
    let before_cancel = db.scan_prefix(b"topk_spill:").await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    db.pause_after_next_put_with_prefix(b"topk_spill:v2:".to_vec(), entered.clone(), release);
    let restoring = {
        let op = op.clone();
        let db = db.clone();
        tokio::spawn(async move { op.restore_in_place(&db, id).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let added = db
        .scan_prefix(b"topk_spill:")
        .await
        .unwrap()
        .into_iter()
        .filter(|record| !before_cancel.contains(record))
        .map(|(_, value)| value.to_vec())
        .collect::<Vec<_>>();
    let expected_spill = [1i64, 1, 1, 0]
        .into_iter()
        .flat_map(i64::to_be_bytes)
        .collect::<Vec<_>>();
    assert_eq!(added, vec![expected_spill]);
    assert!(
        matches!(op.restore_in_place(&db, id).await, Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::KeyEncoding(message), .. }) if message == "topk recovery already in progress")
    );
    assert!(
        matches!(op.process_epoch(make_input(&[(100_001, 0, 1)]), 3), Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::KeyEncoding(message), .. }) if message == "topk recovery in progress")
    );
    let mut snapshot = WriteBatch::new();
    assert!(
        matches!(append_topk_state(&op, id, &mut snapshot), Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::KeyEncoding(message), .. }) if message == "topk recovery in progress")
    );
    assert!(snapshot.is_empty());
    restoring.abort();
    assert!(matches!(restoring.await, Err(error) if error.is_cancelled()));
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_001, 0, 1)]), 3).unwrap()),
        BTreeMap::from([((100_001, 0), 1), ((99_999, 0), -1)])
    );
    assert_eq!(
        weighted_rows(
            &op.process_epoch(make_input(&[(100_001, 0, -1)]), 4)
                .unwrap()
        ),
        BTreeMap::from([((100_001, 0), -1), ((99_999, 0), 1)])
    );
    op.restore_in_place(&db, id).await.unwrap();
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(7, 1, 1)]), 3).unwrap()),
        BTreeMap::from([((5, 1), -1), ((7, 1), 1)])
    );
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_001, 0, 1)]), 3).unwrap()),
        BTreeMap::from([((100_001, 0), 1), ((100_000, 0), -1)])
    );
    db.set_fail_writes(true);
    assert!(
        matches!(op.restore_in_place(&db, id).await, Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::Unsupported(message), .. }) if message == "injected storage write failure")
    );
    db.set_fail_writes(false);
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_002, 0, 1)]), 4).unwrap()),
        BTreeMap::from([((100_001, 0), -1), ((100_002, 0), 1)])
    );
    op.restore_in_place(&db, id).await.unwrap();
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_001, 0, 1)]), 5).unwrap()),
        BTreeMap::from([((100_001, 0), 1), ((100_000, 0), -1)])
    );
    assert_eq!(db.get(&other_spill.0).await.unwrap(), Some(other_spill.1));
    drop(other);
    drop(op);
    Arc::try_unwrap(db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
    let reopened = open_shard(&dir).await;
    let fresh = TopKOp::new(schema_kv(), 2, 0, vec![1]).with_db(reopened.clone());
    fresh.restore_in_place(&reopened, id).await.unwrap();
    assert_eq!(
        weighted_rows(&fresh.process_epoch(make_input(&[(7, 1, 1)]), 6).unwrap()),
        BTreeMap::from([((5, 1), -1), ((7, 1), 1)])
    );
    assert_eq!(
        weighted_rows(
            &fresh
                .process_epoch(make_input(&[(100_001, 0, 1)]), 6)
                .unwrap()
        ),
        BTreeMap::from([((100_001, 0), 1), ((100_000, 0), -1)])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn spilled_topk_restores_candidates_beyond_buffer_limit() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let id = OperatorId(483);
    let op = TopKOp::new(schema_kv(), TOPK_BUFFER_LIMIT + 1, 0, vec![1]).with_db(db.clone());
    let input: Vec<_> = (1..=100_002).map(|rank| (rank, 0, 1)).collect();
    let expected: BTreeMap<_, _> = (2..=100_002).map(|rank| ((rank, 0), 1)).collect();
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&input), 1).unwrap()),
        expected
    );
    assert_eq!(
        weighted_rows(
            &op.process_epoch(make_input(&[(100_003, 0, -2)]), 1)
                .unwrap()
        ),
        BTreeMap::new()
    );
    persist_topk_state(&db, &op, id).await.unwrap();
    op.restore_in_place(&db, id).await.unwrap();
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_003, 0, 3)]), 2).unwrap()),
        BTreeMap::from([((2, 0), -1), ((100_003, 0), 1)])
    );
    assert_eq!(op.fill_level(), TOPK_BUFFER_LIMIT);
}

#[tokio::test(flavor = "multi_thread")]
async fn spilled_topk_restores_ties_and_multiplicity() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let id = OperatorId(484);
    let op = TopKOp::new(schema_kv(), TOPK_BUFFER_LIMIT + 1, 0, vec![]).with_db(db.clone());
    let input: Vec<_> = (1..=100_002)
        .map(|key| (5, key, if key == 1 { 2 } else { 1 }))
        .collect();
    let mut expected: BTreeMap<_, _> = (2..=100_000).map(|key| ((5, key), 1)).collect();
    expected.insert((5, 1), 2);
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&input), 1).unwrap()),
        expected
    );
    persist_topk_state(&db, &op, id).await.unwrap();
    op.restore_in_place(&db, id).await.unwrap();
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(5, 0, 1)]), 2).unwrap()),
        BTreeMap::from([((5, 0), 1), ((5, 100_000), -1)])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn topk_debt_turning_positive_respects_buffer_limit() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let memory = TopKOp::new(schema_kv(), 1, 0, vec![]);
    let spilled = TopKOp::new(schema_kv(), 1, 0, vec![]).with_db(db.clone());
    let initial: Vec<_> = (1..=100_000).map(|rank| (rank, 0, 1)).collect();
    for op in [&memory, &spilled] {
        assert_eq!(
            weighted_rows(&op.process_epoch(make_input(&initial), 1).unwrap()),
            BTreeMap::from([((100_000, 0), 1)])
        );
        assert_eq!(
            weighted_rows(
                &op.process_epoch(make_input(&[(100_001, 0, -1)]), 2)
                    .unwrap()
            ),
            BTreeMap::new()
        );
    }
    assert!(matches!(
        memory.process_epoch(make_input(&[(100_001, 0, 2)]), 3),
        Err(rockstream_ops::OpError::TopKBufferOverflow {
            limit: TOPK_BUFFER_LIMIT,
            ..
        })
    ));
    assert_eq!(
        weighted_rows(
            &memory
                .process_epoch(make_input(&[(100_001, 0, 1)]), 4)
                .unwrap()
        ),
        BTreeMap::new()
    );
    assert_eq!(memory.fill_level(), TOPK_BUFFER_LIMIT);
    db.set_fail_writes(true);
    assert!(
        matches!(spilled.process_epoch(make_input(&[(100_001, 0, 2)]), 3), Err(rockstream_ops::OpError::Storage { source: rockstream_storage::StorageError::Unsupported(message), .. }) if message == "injected storage write failure")
    );
    db.set_fail_writes(false);
    assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), vec![]);
    assert_eq!(
        weighted_rows(
            &spilled
                .process_epoch(make_input(&[(100_001, 0, 2)]), 3)
                .unwrap()
        ),
        BTreeMap::from([((100_000, 0), -1), ((100_001, 0), 1)])
    );
    assert_eq!(spilled.fill_level(), TOPK_BUFFER_LIMIT);
    let values = db
        .scan_prefix(b"topk_spill:")
        .await
        .unwrap()
        .into_iter()
        .map(|(_, value)| value.to_vec())
        .collect::<Vec<_>>();
    let expected = [100_001i64, 1, 100_001, 0]
        .into_iter()
        .flat_map(i64::to_be_bytes)
        .collect::<Vec<_>>();
    assert_eq!(values, vec![expected]);
}

// #113 R1-R5: real spill, exact weighted deltas/bags, and shared-storage isolation.
#[tokio::test(flavor = "multi_thread")]
async fn lfs_topk_spilled_multiplicity_and_operator_isolation() {
    assert_eq!(TOPK_BUFFER_LIMIT, 100_000);
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let op = TopKOp::new(schema_kv(), 2, 0, vec![1]).with_db(db.clone());
    let other = TopKOp::new(schema_kv(), 1, 0, vec![1]).with_db(db.clone());
    let initial: Vec<_> = (1..=100_000).map(|rank| (rank, 0, 1)).collect();
    let mut bag = weighted_rows(&op.process_epoch(make_input(&initial), 1).unwrap());
    assert_eq!(bag, BTreeMap::from([((100_000, 0), 1), ((99_999, 0), 1)]));
    assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), vec![]);

    for (epoch, weight, expected_delta, expected_bag) in [
        (
            2,
            1,
            vec![((100_001, 0), 1), ((99_999, 0), -1)],
            vec![((100_001, 0), 1), ((100_000, 0), 1)],
        ),
        (
            3,
            1,
            vec![((100_001, 0), 1), ((100_000, 0), -1)],
            vec![((100_001, 0), 2)],
        ),
        (
            4,
            -1,
            vec![((100_001, 0), -1), ((100_000, 0), 1)],
            vec![((100_001, 0), 1), ((100_000, 0), 1)],
        ),
        (
            5,
            -1,
            vec![((100_001, 0), -1), ((99_999, 0), 1)],
            vec![((100_000, 0), 1), ((99_999, 0), 1)],
        ),
    ] {
        let delta = weighted_rows(
            &op.process_epoch(make_input(&[(100_001, 0, weight)]), epoch)
                .unwrap(),
        );
        assert_eq!(
            delta,
            expected_delta.into_iter().collect(),
            "epoch {epoch} delta"
        );
        for (row, weight) in delta {
            *bag.entry(row).or_insert(0) += weight;
        }
        bag.retain(|_, weight| *weight != 0);
        assert_eq!(bag, expected_bag.into_iter().collect(), "epoch {epoch} bag");

        if epoch == 2 {
            let id = OperatorId(489);
            persist_topk_state(&db, &op, id).await.unwrap();
            let bounded = TopKOp::new(schema_kv(), 2, 0, vec![1]);
            bounded.process_epoch(make_input(&[(10, 0, 1)]), 1).unwrap();
            assert!(matches!(
                bounded.restore_in_place(&db, id).await,
                Err(rockstream_ops::OpError::TopKBufferOverflow {
                    limit: TOPK_BUFFER_LIMIT,
                    ..
                })
            ));
            let output = bounded.process_epoch(make_input(&[(20, 0, 1)]), 2).unwrap();
            assert_eq!(weighted_rows(&output), BTreeMap::from([((20, 0), 1)]));
        }

        let spill_before = db.scan_prefix(b"topk_spill:").await.unwrap();
        let mut expected_spill_value = Vec::new();
        for value in [100_001_i64, if epoch == 3 { 2 } else { 1 }, 100_001, 0] {
            expected_spill_value.extend_from_slice(&value.to_be_bytes());
        }
        if epoch < 5 {
            assert_eq!(
                spill_before
                    .iter()
                    .map(|(_, value)| value.to_vec())
                    .collect::<Vec<_>>(),
                vec![expected_spill_value]
            );
        } else {
            assert_eq!(spill_before, vec![]);
        }
        let other_delta = weighted_rows(
            &other
                .process_epoch(make_input(&[(-1, 0, 1)]), epoch)
                .unwrap(),
        );
        assert_eq!(
            other_delta,
            if epoch == 2 {
                BTreeMap::from([((-1, 0), 1)])
            } else {
                BTreeMap::new()
            }
        );
        assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), spill_before);
    }

    drop(op);
    drop(other);
    Arc::try_unwrap(db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn lfs_topk_shared_spill_updates_and_retraction_debt() {
    let dir = TempDir::new().unwrap();
    let db = open_shard(&dir).await;
    let op = TopKOp::new(schema_kv(), 2, 0, vec![1]).with_db(db.clone());
    let other = TopKOp::new(schema_kv(), 1, 0, vec![1]).with_db(db.clone());
    let initial: Vec<_> = (1..=TOPK_BUFFER_LIMIT as i64)
        .map(|rank| (rank, 0, 1))
        .collect();
    let input = make_input(&initial);
    assert_eq!(
        weighted_rows(&op.process_epoch(input.clone(), 1).unwrap()),
        BTreeMap::from([((100_000, 0), 1), ((99_999, 0), 1)])
    );
    assert_eq!(
        weighted_rows(&other.process_epoch(input, 1).unwrap()),
        BTreeMap::from([((100_000, 0), 1)])
    );
    assert_eq!(
        weighted_rows(&op.process_epoch(make_input(&[(100_001, 0, 2)]), 2).unwrap()),
        BTreeMap::from([((100_001, 0), 2), ((100_000, 0), -1), ((99_999, 0), -1)])
    );
    let first_spill = db.scan_prefix(b"topk_spill:").await.unwrap();
    assert_eq!(
        weighted_rows(
            &other
                .process_epoch(make_input(&[(100_001, 0, 1)]), 2)
                .unwrap()
        ),
        BTreeMap::from([((100_001, 0), 1), ((100_000, 0), -1)])
    );
    assert_eq!(
        db.get(&first_spill[0].0).await.unwrap(),
        Some(first_spill[0].1.clone())
    );
    assert_eq!(
        weighted_rows(
            &other
                .process_epoch(make_input(&[(100_001, 0, -1)]), 3)
                .unwrap()
        ),
        BTreeMap::from([((100_001, 0), -1), ((100_000, 0), 1)])
    );
    assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), first_spill);

    // Free a buffer slot; updates must still find the existing spilled weight.
    assert_eq!(
        weighted_rows(
            &op.process_epoch(make_input(&[(100_000, 0, -1)]), 3)
                .unwrap()
        ),
        BTreeMap::new()
    );
    for (epoch, weight, expected_delta, stored_weight) in [
        (
            4,
            -3,
            vec![((100_001, 0), -2), ((99_999, 0), 1), ((99_998, 0), 1)],
            Some(-1_i64),
        ),
        (5, 1, vec![], None),
        (6, 1, vec![((100_001, 0), 1), ((99_998, 0), -1)], None),
    ] {
        assert_eq!(
            weighted_rows(
                &op.process_epoch(make_input(&[(100_001, 0, weight)]), epoch)
                    .unwrap()
            ),
            expected_delta.into_iter().collect(),
            "epoch {epoch}"
        );
        let expected_spill = stored_weight
            .map(|weight| {
                let mut value = Vec::new();
                for number in [100_001, weight, 100_001, 0] {
                    value.extend_from_slice(&number.to_be_bytes());
                }
                vec![value]
            })
            .unwrap_or_default();
        assert_eq!(
            db.scan_prefix(b"topk_spill:")
                .await
                .unwrap()
                .into_iter()
                .map(|(_, value)| value.to_vec())
                .collect::<Vec<_>>(),
            expected_spill
        );
    }
    drop(op);
    drop(other);
    Arc::try_unwrap(db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
}

// ─── Test 1: State persists across close/reopen ────────────────────────────

#[tokio::test]
async fn lfs_topk_state_persists() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(20);
    let k = 3usize;
    let mut net_state: HashMap<i64, i64> = Default::default();

    // ── Epoch 1 & 2: insert rows, persist, close ─────────────────────────
    {
        let db = open_shard(&dir).await;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);

        let out1 = op
            .process_epoch(
                make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]),
                1,
            )
            .unwrap();
        accumulate_vals(&mut net_state, &out1);
        assert_eq!(live_vals(&net_state), vec![10, 8, 6]);

        let out2 = op.process_epoch(make_input(&[(9, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut net_state, &out2);
        assert_eq!(live_vals(&net_state), vec![10, 9, 8]);

        assert_eq!(op.fill_level(), 5, "5 entries in buffer after epoch 2");

        persist_topk_state(&db, &op, op_id).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Epoch 3: reopen, load, verify top-K is correct ───────────────────
    {
        let db = open_shard(&dir).await;
        let op = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
            .await
            .unwrap();

        assert_eq!(op.fill_level(), 5, "fill level restored after reopen");

        // Epoch 3: delete rank-1 (v=10). Next best (v=9 already in top-3), v=6 refills.
        let out3 = op.process_epoch(make_input(&[(10, 1, -1)]), 3).unwrap();
        accumulate_vals(&mut net_state, &out3);
        assert_eq!(live_vals(&net_state), vec![9, 8, 6]);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }
}

// ─── Test 2: WAL crash replay ─────────────────────────────────────────────

#[tokio::test]
async fn lfs_topk_crash_replay() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(21);
    let k = 3usize;

    // ── Epoch 1: write to WAL, drop without flush ─────────────────────────
    {
        let db = open_shard(&dir).await;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);
        op.process_epoch(
            make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]),
            1,
        )
        .unwrap();
        assert_eq!(op.fill_level(), 4);
        persist_topk_state(&db, &op, op_id).await.unwrap();
        // Simulate crash: drop without flush.
        drop(Arc::try_unwrap(db).ok().expect("single owner"));
    }

    // ── Epoch 2: reopen (WAL replay), add one row ─────────────────────────
    {
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(ShardDb::builder("shard", store).build().await.unwrap());

        let op = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
            .await
            .unwrap();

        assert_eq!(op.fill_level(), 4, "4 entries recovered from WAL");

        let mut crash_state: HashMap<i64, i64> = Default::default();
        // Reconstruct epoch 1 output.
        let init_op = TopKOp::new(schema_kv(), k, 0, vec![]);
        let init_out = init_op
            .process_epoch(
                make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]),
                1,
            )
            .unwrap();
        accumulate_vals(&mut crash_state, &init_out);
        // Epoch 2: insert v=7, outranks v=6.
        let out2 = op.process_epoch(make_input(&[(7, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut crash_state, &out2);
        let crash_live = live_vals(&crash_state);

        // Non-crash path.
        let fresh_op = TopKOp::new(schema_kv(), k, 0, vec![]);
        let mut fresh_state: HashMap<i64, i64> = Default::default();
        let f1 = fresh_op
            .process_epoch(
                make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1)]),
                1,
            )
            .unwrap();
        accumulate_vals(&mut fresh_state, &f1);
        let f2 = fresh_op.process_epoch(make_input(&[(7, 5, 1)]), 2).unwrap();
        accumulate_vals(&mut fresh_state, &f2);
        let fresh_live = live_vals(&fresh_state);

        assert_eq!(
            crash_live, fresh_live,
            "crash-replay bit-identical to non-crash path"
        );
        assert_eq!(crash_live, vec![10, 8, 7]);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }
}

// ─── Test 3: No range delete ──────────────────────────────────────────────

#[tokio::test]
async fn lfs_topk_no_range_delete() {
    // WriteBatch::delete_range does not exist — structural guarantee.
    let mut batch = WriteBatch::new();
    batch.put(b"key1", b"value1");
    batch.delete(b"key2");
    let _ = batch;
}

#[path = "common/topk_recovery.rs"]
mod topk_recovery;

#[tokio::test(flavor = "multi_thread")]
async fn lfs_topk_spilled_state_recovers_after_close() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    topk_recovery::spilled_recovery(store, "topk-recovery").await;
}
