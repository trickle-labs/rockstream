//! Issue #402 Verification 2 & R5:
//! ADR 0004 Arithmetic Safety (`i64::MAX` overflow, `i64::MIN` negation/underflow,
//! non-reassociable compaction barriers), Rollback Safety, Group Deletion +
//! Compaction + Restart, `ShardReader` Snapshot Reads, and `WeightAdd/v1` Merge
//! Persistence on both `LocalFileSystem` and MinIO/S3 backends.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_ops::aggregate::{
    append_agg_state, persist_agg_state, AggregateOp, AggregatePersistenceMode,
};
use rockstream_ops::distinct::{
    load_distinct_state, persist_distinct_state, DistinctOp, DistinctPersistenceMode,
};
use rockstream_ops::{ArrowZSet, OpError, Operator};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{MergeOperatorRegistry, ShardDb, ShardReader, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::sum_count::SUM_COUNT_VERSION;
use rockstream_types::laws::SUM_COUNT_ID;
use rockstream_types::state_mutation::StateMutation;

const MINIO_BUCKET: &str = "rockstream-test-402-adr0004";

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, rows.iter().map(|r| r.2).collect())
}

fn agg_rows(zset: &ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let ks = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = zset
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avgs = zset
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..zset.num_rows())
        .map(|i| {
            (
                ks.value(i),
                sums.value(i),
                counts.value(i),
                avgs.value(i),
                zset.weights[i],
            )
        })
        .collect()
}

fn state_key(op_id: OperatorId, k: i64) -> Vec<u8> {
    ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &k.to_be_bytes())
}

#[tokio::test]
async fn adr0004_overflow_negation_rollback_deletion_and_compaction_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = ShardDb::builder("adr0004-lfs", store.clone())
        .build()
        .await
        .unwrap();
    let op_id = OperatorId(40_201);
    let op = AggregateOp::new(op_id);

    // 1. Seed group 1 near i64::MAX, group 2 for deletion, group 3 for normal merge.
    let e1 = op
        .process_delta_with_result(make_kv_batch(&[
            (1, i64::MAX - 5, 1),
            (2, 50, 1),
            (3, 100, 1),
        ]))
        .unwrap();
    assert_eq!(e1.state_mutations.len(), 3);
    assert!(e1
        .state_mutations
        .iter()
        .all(|m| matches!(m, StateMutation::Put { .. })));
    persist_agg_state(&db, &op).await.unwrap();
    db.flush().await.unwrap();

    // 2. Overflow: i64::MAX - 5 + 6 overflows i64 -> must fail closed with AggregateOverflow (RS-1002)
    // and leave all in-memory & persisted state completely intact!
    let overflow_err = op
        .process_delta_with_result(make_kv_batch(&[(1, 6, 1), (3, 1, 1)]))
        .unwrap_err();
    assert!(
        matches!(
            overflow_err,
            OpError::AggregateOverflow { group_key: 1, .. }
        ),
        "expected AggregateOverflow for key 1, got {overflow_err:?}"
    );
    assert_eq!(
        overflow_err.to_string(),
        "[RS-1016] Aggregate sum overflow for group key 1: next_steps: reduce value magnitudes or switch to a wider numeric type"
    );
    assert_eq!(op.live_groups(), 3);

    // 3. i64::MIN negation / retraction overflow: retracting i64::MIN (-1 * i64::MIN = +2^63)
    // must fail closed without wrapping!
    let neg_err = op
        .process_delta_with_result(make_kv_batch(&[(3, i64::MIN, -1)]))
        .unwrap_err();
    assert!(
        matches!(neg_err, OpError::AggregateOverflow { group_key: 3, .. }),
        "expected AggregateOverflow on i64::MIN negation, got {neg_err:?}"
    );
    assert_eq!(
        neg_err.to_string(),
        "[RS-1016] Aggregate sum overflow for group key 3: next_steps: reduce value magnitudes or switch to a wider numeric type"
    );

    // 4. Non-reassociable boundary update near i64::MAX:
    // Group 1 has (i64::MAX - 5, 1). Updating by (+4, +1) -> (i64::MAX - 1, 2) IS reassociable,
    // wait: |i64::MAX - 5| + |4| = i64::MAX - 1 <= i64::MAX -> admitted as Merge!
    // Updating by (-10, +1) from (i64::MAX - 1, 2): |i64::MAX - 1| + |-10| > i64::MAX ->
    // ADR 0004 admits_reassociation is FALSE -> MUST fall back to tagged Put compaction barrier!
    let near_max_ok = op
        .process_delta_with_result(make_kv_batch(&[(1, 4, 1)]))
        .unwrap();
    assert_eq!(
        near_max_ok.state_mutations,
        vec![StateMutation::Merge {
            key: state_key(op_id, 1),
            law: SUM_COUNT_ID,
            law_version: SUM_COUNT_VERSION,
            operand: Bytes::from(MergeOperatorRegistry::encode_sum_count(4, 1)),
        }]
    );
    persist_agg_state(&db, &op).await.unwrap();

    let non_reassoc = op
        .process_delta_with_result(make_kv_batch(&[(1, -10, 1)]))
        .unwrap();
    assert_eq!(
        non_reassoc.state_mutations,
        vec![StateMutation::Put {
            key: state_key(op_id, 1),
            value: Bytes::from(MergeOperatorRegistry::encode_sum_count(i64::MAX - 11, 3)),
        }],
        "non-reassociable update near i64::MAX must fall back to tagged Put compaction barrier"
    );
    persist_agg_state(&db, &op).await.unwrap();

    // 5. Normal update emits Merge; group deletion emits Delete.
    let e_merge_del = op
        .process_delta_with_result(make_kv_batch(&[(3, 25, 1), (2, 50, -1)]))
        .unwrap();
    assert_eq!(
        agg_rows(&e_merge_del.output_delta),
        vec![
            (3, 100, 1, 100.0, -1),
            (3, 125, 2, 62.5, 1),
            (2, 50, 1, 50.0, -1),
        ]
    );
    assert_eq!(
        e_merge_del.state_mutations,
        vec![
            StateMutation::Delete {
                key: state_key(op_id, 2),
            },
            StateMutation::Merge {
                key: state_key(op_id, 3),
                law: SUM_COUNT_ID,
                law_version: SUM_COUNT_VERSION,
                operand: Bytes::from(MergeOperatorRegistry::encode_sum_count(25, 1)),
            },
        ]
    );
    persist_agg_state(&db, &op).await.unwrap();
    db.flush().await.unwrap();

    // 6. Mid-epoch rollback safety: process uncommitted batch, then roll back without persisting!
    let _uncommitted = op
        .process_delta_with_result(make_kv_batch(&[(3, 999, 1), (4, 444, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 3); // keys 1, 3, 4
    op.rollback_uncommitted();
    assert_eq!(op.live_groups(), 2); // back to keys 1, 3!
    let mut wb_after_rollback = WriteBatch::new();
    append_agg_state(&db, &op, &mut wb_after_rollback)
        .await
        .unwrap();
    assert!(
        wb_after_rollback.is_empty(),
        "rollback_uncommitted must clear all dirty mutations"
    );

    // 7. Verify ShardReader resolves WriteBatch::merge operands directly from storage!
    let reader = ShardReader::open("adr0004-lfs", store.clone())
        .await
        .unwrap();
    assert_eq!(
        reader.get(&state_key(op_id, 1)).await.unwrap(),
        Some(Bytes::from(MergeOperatorRegistry::encode_sum_count(
            i64::MAX - 11,
            3
        )))
    );
    assert_eq!(reader.get(&state_key(op_id, 2)).await.unwrap(), None);
    assert_eq!(
        reader.get(&state_key(op_id, 3)).await.unwrap(),
        Some(Bytes::from(MergeOperatorRegistry::encode_sum_count(125, 2)))
    );
    drop(reader);

    // 8. Close, reopen, and restore: verify zero resurrected keys and exact post-restart continuation!
    db.close().await.unwrap();
    let reopened = ShardDb::builder("adr0004-lfs", store.clone())
        .build()
        .await
        .unwrap();
    let restored = AggregateOp::new(op_id);
    restored.restore_in_place(&reopened).await.unwrap();
    assert_eq!(restored.live_groups(), 2);

    // Next update to key 3 confirms exact restored state (125, 2) -> (135, 3).
    let post_restart = restored
        .process_delta_with_result(make_kv_batch(&[(3, 10, 1)]))
        .unwrap();
    assert_eq!(
        agg_rows(&post_restart.output_delta),
        vec![(3, 125, 2, 62.5, -1), (3, 135, 3, 45.0, 1)]
    );
    // Deleted key 2 must not resurrect; recreating it emits fresh insert (no retraction).
    let recreate_k2 = restored
        .process_delta_with_result(make_kv_batch(&[(2, 7, 1)]))
        .unwrap();
    assert_eq!(agg_rows(&recreate_k2.output_delta), vec![(2, 7, 1, 7.0, 1)]);
    assert_eq!(
        recreate_k2.state_mutations,
        vec![StateMutation::Put {
            key: state_key(op_id, 2),
            value: Bytes::from(MergeOperatorRegistry::encode_sum_count(7, 1)),
        }]
    );

    // 9. Verify PutDelete fallback mode for non-admitted aggregates.
    let fallback_id = OperatorId(40_202);
    let fallback_op =
        AggregateOp::new(fallback_id).with_persistence_mode(AggregatePersistenceMode::PutDelete);
    let fb1 = fallback_op
        .process_delta_with_result(make_kv_batch(&[(10, 5, 1)]))
        .unwrap();
    assert!(matches!(fb1.state_mutations[0], StateMutation::Put { .. }));
    persist_agg_state(&reopened, &fallback_op).await.unwrap();
    let fb2 = fallback_op
        .process_delta_with_result(make_kv_batch(&[(10, 7, 1)]))
        .unwrap();
    assert!(
        matches!(fb2.state_mutations[0], StateMutation::Put { .. }),
        "PutDelete fallback mode must emit Put rather than Merge"
    );
    persist_agg_state(&reopened, &fallback_op).await.unwrap();

    // 10. Verify DistinctOp ADR 0004 WeightAdd/v1 merge persistence, overflow, deletion, & restart.
    let distinct_id = OperatorId(40_203);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let distinct = DistinctOp::with_weight_merge(schema.clone());
    assert_eq!(
        distinct.persistence_mode(),
        DistinctPersistenceMode::WeightMerge
    );
    let d1 = distinct
        .process_delta(make_kv_batch(&[(1, 10, 1), (2, 20, 2)]))
        .unwrap();
    assert_eq!(d1.num_rows(), 2);
    persist_distinct_state(&reopened, &distinct, distinct_id)
        .await
        .unwrap();

    // Update existing row weight via WeightAdd/v1 Merge, delete row (2, 20).
    let d2 = distinct
        .process_delta(make_kv_batch(&[(1, 10, 3), (2, 20, -2)]))
        .unwrap();
    assert_eq!(d2.num_rows(), 1);
    assert_eq!(d2.weights, vec![-1]); // row (2, 20) retracted
    persist_distinct_state(&reopened, &distinct, distinct_id)
        .await
        .unwrap();

    // Overflow on distinct weight fails closed with RS-1002.
    let d_ovf = distinct
        .process_delta(make_kv_batch(&[(1, 10, i64::MAX)]))
        .unwrap_err();
    assert!(
        d_ovf.to_string().contains("RS-1002"),
        "expected RS-1002 on distinct weight overflow, got: {d_ovf}"
    );

    reopened.flush().await.unwrap();
    let restored_distinct = load_distinct_state(&reopened, schema, distinct_id)
        .await
        .unwrap();
    assert_eq!(restored_distinct.fill_level(), 1);
    assert_eq!(
        restored_distinct.persistence_mode(),
        DistinctPersistenceMode::WeightMerge
    );
    // Retracting weight 4 from (1, 10) must now cross 4 -> 0 and emit -1!
    let d3 = restored_distinct
        .process_delta(make_kv_batch(&[(1, 10, -4)]))
        .unwrap();
    assert_eq!(d3.num_rows(), 1);
    assert_eq!(d3.weights, vec![-1]);
    assert_eq!(restored_distinct.fill_level(), 0);

    reopened.close().await.unwrap();
}

#[tokio::test]
async fn adr0004_merge_deletion_and_restart_minio() {
    let Some((_container, port)) = rockstream_test_support::minio::start_minio(MINIO_BUCKET).await
    else {
        eprintln!("skipping MinIO test: Docker/MinIO unavailable");
        return;
    };
    let store: Arc<dyn ObjectStore> = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));
    let path = "adr0004-minio-shard";
    let op_id = OperatorId(40_299);

    {
        let db = Arc::new(ShardDb::builder(path, store.clone()).build().await.unwrap());
        let op = AggregateOp::new(op_id);
        let _ = op
            .process_delta_with_result(make_kv_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]))
            .unwrap();
        persist_agg_state(&db, &op).await.unwrap();
        db.flush().await.unwrap();

        // Issue Merge updates on keys 1 & 3 and Delete on key 2.
        let e2 = op
            .process_delta_with_result(make_kv_batch(&[(1, 15, 1), (2, 20, -1), (3, -5, 1)]))
            .unwrap();
        assert!(matches!(e2.state_mutations[0], StateMutation::Merge { .. }));
        assert!(matches!(
            e2.state_mutations[1],
            StateMutation::Delete { .. }
        ));
        assert!(matches!(e2.state_mutations[2], StateMutation::Merge { .. }));
        persist_agg_state(&db, &op).await.unwrap();
        db.flush().await.unwrap();

        // BlindMerge update directly on cold evicted key 1 with 0 cold-key reads.
        let blind = AggregateOp::new(op_id)
            .with_db(db.clone())
            .with_persistence_mode(AggregatePersistenceMode::BlindMerge);
        let _ = blind
            .process_delta_with_result(make_kv_batch(&[(1, 5, 1)]))
            .unwrap();
        assert_eq!(blind.cold_key_reads(), 0);
        persist_agg_state(&db, &blind).await.unwrap();
        db.flush().await.unwrap();
        drop(blind);
        Arc::try_unwrap(db)
            .ok()
            .expect("unique db")
            .close()
            .await
            .unwrap();
    }

    // Read via ShardReader against MinIO S3 to prove merge resolution on reader path.
    let reader = ShardReader::open(path, store.clone()).await.unwrap();
    assert_eq!(
        reader.get(&state_key(op_id, 1)).await.unwrap(),
        Some(Bytes::from(MergeOperatorRegistry::encode_sum_count(30, 3)))
    );
    assert_eq!(reader.get(&state_key(op_id, 2)).await.unwrap(), None);
    assert_eq!(
        reader.get(&state_key(op_id, 3)).await.unwrap(),
        Some(Bytes::from(MergeOperatorRegistry::encode_sum_count(25, 2)))
    );
    drop(reader);

    // Reopen ShardDb on MinIO and restore AggregateOp.
    let db2 = ShardDb::builder(path, store).build().await.unwrap();
    let restored = AggregateOp::new(op_id);
    restored.restore_in_place(&db2).await.unwrap();
    assert_eq!(restored.live_groups(), 2);
    let next = restored
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    assert_eq!(
        agg_rows(&next.output_delta),
        vec![(1, 30, 3, 10.0, -1), (1, 40, 4, 10.0, 1)]
    );
    db2.close().await.unwrap();
}
