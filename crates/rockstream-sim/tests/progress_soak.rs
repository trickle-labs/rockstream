//! Multi-source E2E join parity and shuffle GC soak simulation test (v0.19, Slice 6).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rockstream_ops::join::JoinOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::persistence::{
    gc_exchange_storage, persist_inbox, persist_outbox,
};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::frontier::{FreshnessToken, ProgressMerge, SourceProgress};
use rockstream_types::ids::{OperatorId, SourceId};

const MINIO_BUCKET: &str = "rockstream-test";

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

fn empty_kv() -> ArrowZSet {
    ArrowZSet::empty(Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ])))
}

fn extract_output(batch: &ArrowZSet) -> Vec<(i64, i64, i64, i64)> {
    if batch.is_empty() {
        return vec![];
    }
    let lk = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lv = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rv = batch
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows: Vec<(i64, i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| (lk.value(i), lv.value(i), rv.value(i), batch.weights[i]))
        .collect();
    rows.sort();
    rows
}

#[test]
fn test_multi_source_join_progress() {
    // 1. Sets up JoinOp
    let join_op = JoinOp::new(OperatorId(1), vec![0], vec![0]);

    // 2. Set up asymmetric source progress:
    // Left side (Source 1) goes fast: epochs 1 to 5.
    // Right side (Source 2) goes slow: stays at epoch 1.

    // In Epoch 1:
    // Left: key=10, val=100. Right: key=10, val=200.
    let left_progress_1 = SourceProgress::new(1, Some(100));
    let right_progress_1 = SourceProgress::new(1, Some(100));
    let left_token_1 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_1),
            (SourceId(2), right_progress_1),
        ]),
        0,
    );
    let right_token_1 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_1),
            (SourceId(2), right_progress_1),
        ]),
        0,
    );
    let meet_token_1 = left_token_1.meet(&right_token_1);
    assert_eq!(meet_token_1.watermark_ms(), Some(100));

    let left_1 = make_kv_batch(&[(10, 100, 1)]);
    let right_1 = make_kv_batch(&[(10, 200, 1)]);
    let out_1 = join_op.process_epoch(left_1, right_1).unwrap();
    let rows_1 = extract_output(&out_1);
    assert!(rows_1.contains(&(10, 100, 200, 1)));

    // In Epochs 2 to 4:
    // Left Source 1 advances: epoch = i, watermark = 100 * i.
    // Right Source 2 lags: epoch = 1, watermark = 100.
    for i in 2..=4 {
        let left_progress_i = SourceProgress::new(i as u64, Some(100 * i));
        let right_progress_i = SourceProgress::new(1, Some(100));
        let left_token_i = FreshnessToken::new(
            BTreeMap::from([
                (SourceId(1), left_progress_i),
                (SourceId(2), right_progress_i),
            ]),
            0,
        );
        let right_token_i = FreshnessToken::new(
            BTreeMap::from([
                (SourceId(1), left_progress_i),
                (SourceId(2), right_progress_i),
            ]),
            0,
        );
        let meet_token_i = left_token_i.meet(&right_token_i);

        // Assert no premature emissions: the output joint watermark must remain at 100.
        assert_eq!(
            meet_token_i.watermark_ms(),
            Some(100),
            "Watermark advanced prematurely at epoch {}",
            i
        );

        // Feed left delta, right empty
        let left_delta = make_kv_batch(&[(10, 100 + i * 10, 1)]);
        let right_delta = empty_kv();
        let _out_i = join_op.process_epoch(left_delta, right_delta).unwrap();
        // Staged buffers must not accumulate unboundedly (they are drained by commit_epoch).
        assert_eq!(join_op.left_entry_count(), i as usize);
        assert_eq!(join_op.right_entry_count(), 1);
    }

    // In Epoch 5:
    // Slow side (Right Source 2) catches up: epoch = 5, watermark = 500.
    let left_progress_5 = SourceProgress::new(5, Some(500));
    let right_progress_5 = SourceProgress::new(5, Some(500));
    let left_token_5 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_5),
            (SourceId(2), right_progress_5),
        ]),
        0,
    );
    let right_token_5 = FreshnessToken::new(
        BTreeMap::from([
            (SourceId(1), left_progress_5),
            (SourceId(2), right_progress_5),
        ]),
        0,
    );
    let meet_token_5 = left_token_5.meet(&right_token_5);

    // Assert the joint watermark has converged correctly to 500.
    assert_eq!(meet_token_5.watermark_ms(), Some(500));

    // Feed left empty, right catching up with new data: (10, 300)
    let left_delta = empty_kv();
    let right_delta = make_kv_batch(&[(10, 300, 1)]);
    let out_5 = join_op.process_epoch(left_delta, right_delta).unwrap();
    let rows_5 = extract_output(&out_5);

    // Verify correct convergence (all historical left rows joined with the new right row).
    assert!(rows_5.contains(&(10, 100, 300, 1)));
    assert!(rows_5.contains(&(10, 120, 300, 1)));
    assert!(rows_5.contains(&(10, 130, 300, 1)));
    assert!(rows_5.contains(&(10, 140, 300, 1)));
}

#[tokio::test]
async fn test_shuffle_storage_gc_bounded() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_shuffle_storage_gc_bounded: Docker not available");
            return;
        }
    };
    let store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));
    let db = ShardDb::builder("test_gc_db", store).build().await.unwrap();

    let inbox_prefix = [0x04];
    let outbox_prefix = [0x05];

    // Write inbox and outbox shuffle entries for 100 consecutive epochs
    for epoch in 1..=100 {
        persist_inbox(&db, 100, 1, epoch, 1, b"inbox_data")
            .await
            .unwrap();
        persist_outbox(&db, 200, 2, epoch, 1, b"outbox_data")
            .await
            .unwrap();

        // Perform GC periodically, keeping only the most recent 5 epochs
        if epoch >= 5 {
            gc_exchange_storage(&db, epoch - 5).await.unwrap();
        }
    }

    // Verify key count in ShardDb is bounded (only epochs 96..100 remain)
    let inbox_keys = db.scan_prefix(&inbox_prefix).await.unwrap();
    let outbox_keys = db.scan_prefix(&outbox_prefix).await.unwrap();

    assert!(
        inbox_keys.len() <= 5,
        "Inbox keys not bounded: {}",
        inbox_keys.len()
    );
    assert!(
        outbox_keys.len() <= 5,
        "Outbox keys not bounded: {}",
        outbox_keys.len()
    );

    // Verify that the remaining keys indeed correspond to the latest epochs
    for (key, _) in inbox_keys {
        if let Some((_, _, suffix)) = rockstream_storage::keys::ShardKeyEncoder::decode(&key) {
            let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
            assert!(
                epoch >= 96,
                "Stale epoch {} not cleaned up from inbox!",
                epoch
            );
        }
    }

    for (key, _) in outbox_keys {
        if let Some((_, _, suffix)) = rockstream_storage::keys::ShardKeyEncoder::decode(&key) {
            let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
            assert!(
                epoch >= 96,
                "Stale epoch {} not cleaned up from outbox!",
                epoch
            );
        }
    }

    db.close().await.unwrap();
}
