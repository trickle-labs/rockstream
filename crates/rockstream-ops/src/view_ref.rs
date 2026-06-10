//! Physical ViewRef Operator (v0.13 — Slice 3).
//!
//! `ViewRefOp` tails an upstream view's CDC delta updates from `ShardDb`
//! under the `ViewOutput` prefix, epoch-by-epoch.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rockstream_storage::{ShardDb, ShardPrefix};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// Named buffer limit to prevent unbounded memory accumulation.
pub const VIEW_REF_SCAN_LIMIT_BYTES: usize = 64 * 1024 * 1024; // 64MB cap

/// Physical ViewRef Operator.
pub struct ViewRefOp {
    db: Arc<ShardDb>,
    upstream_op_id: OperatorId,
    schema: SchemaRef,
    epoch: AtomicU64,
}

impl ViewRefOp {
    /// Create and initialize the ViewRef operator.
    pub fn new(
        db: Arc<ShardDb>,
        upstream_op_id: OperatorId,
        schema: SchemaRef,
        resume_epoch: u64,
    ) -> Self {
        ViewRefOp {
            db,
            upstream_op_id,
            schema,
            epoch: AtomicU64::new(resume_epoch),
        }
    }

    /// Set the current epoch position to resume tailing from.
    pub fn resume_from(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::SeqCst);
    }
}

impl Operator for ViewRefOp {
    fn name(&self) -> &str {
        "ViewRefOp"
    }

    fn process_delta(&self, _delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let e = self.epoch.load(Ordering::SeqCst);
        let prefix = {
            let mut p = Vec::with_capacity(1 + 8 + 8);
            p.push(ShardPrefix::ViewOutput.as_byte());
            p.extend_from_slice(&self.upstream_op_id.0.to_be_bytes());
            p.extend_from_slice(&e.to_be_bytes());
            p
        };

        // Scan this epoch's cdc delta entries synchronously by spawning a helper thread.
        let (entries, _truncated) = {
            let db = self.db.clone();
            let prefix = prefix.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    db.scan_prefix_bounded(&prefix, VIEW_REF_SCAN_LIMIT_BYTES)
                        .await
                })
            })
            .join()
            .unwrap()
            .map_err(OpError::storage)?
        };

        let num_cols = self.schema.fields().len();
        let expected_len = (num_cols + 1) * 8;

        // Parse key-value entries into rows and sort them by row_index.
        let mut sorted_entries = Vec::new();
        for (key, value) in entries {
            if key.len() < 25 {
                continue;
            }
            if value.len() < expected_len {
                continue;
            }
            // Key format: [prefix:1][op_id:8][epoch:8][row_index:8]
            let row_idx = u64::from_be_bytes(key[17..25].try_into().unwrap());
            sorted_entries.push((row_idx, value));
        }

        // Deterministic sorting of rows within the epoch
        sorted_entries.sort_by_key(|(row_idx, _)| *row_idx);

        if sorted_entries.is_empty() {
            // No CDC updates for this epoch; advance and return empty.
            self.epoch.store(e + 1, Ordering::SeqCst);
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut cols = vec![Vec::with_capacity(sorted_entries.len()); num_cols];
        let mut weights = Vec::with_capacity(sorted_entries.len());

        for (_, value) in sorted_entries {
            for c in 0..num_cols {
                let v = i64::from_be_bytes(value[c * 8..(c + 1) * 8].try_into().unwrap());
                cols[c].push(v);
            }
            let w = i64::from_be_bytes(value[num_cols * 8..(num_cols + 1) * 8].try_into().unwrap());
            weights.push(w);
        }

        let arrow_cols: Vec<ArrayRef> = cols
            .into_iter()
            .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
            .collect();

        let data = RecordBatch::try_new(self.schema.clone(), arrow_cols).map_err(OpError::arrow)?;

        // Advance epoch counter
        self.epoch.store(e + 1, Ordering::SeqCst);

        Ok(ArrowZSet::new(data, weights))
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::ViewSinkOp;
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn schema_kv() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    async fn open_shard_db(dir: &TempDir) -> Arc<ShardDb> {
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
    }

    #[tokio::test]
    async fn test_view_ref_op_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_shard_db(&dir).await;
        let op_id = OperatorId(42);

        // Write some delta updates using ViewSinkOp
        let sink = ViewSinkOp::new(db.clone(), op_id);
        let schema = schema_kv();

        // Epoch 0: add (1, 10) and (2, 20)
        let batch0 = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
        sink.write_epoch(&batch0, 0).await.unwrap();

        // Epoch 1: retract (1, 10)
        let batch1_combined = ArrowZSet::from_ab_rows(&[(1, 10)], -1);
        sink.write_epoch(&batch1_combined, 1).await.unwrap();

        // Epoch 2: add (3, 30)
        let batch2_combined = ArrowZSet::from_ab_rows(&[(3, 30)], 1);
        sink.write_epoch(&batch2_combined, 2).await.unwrap();

        db.flush().await.unwrap();

        // Initialize ViewRefOp starting from epoch 0
        let view_ref_op = ViewRefOp::new(db.clone(), op_id, schema.clone(), 0);

        // Process epoch 0
        let out0 = view_ref_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out0.num_rows(), 2);
        let k0 = out0
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let w0 = out0.weights[0];
        assert_eq!((k0, w0), (1, 1));
        let k1 = out0
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        let w1 = out0.weights[1];
        assert_eq!((k1, w1), (2, 1));

        // Process epoch 1
        let out1 = view_ref_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out1.num_rows(), 1);
        let k_ret = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let w_ret = out1.weights[0];
        assert_eq!((k_ret, w_ret), (1, -1));

        // Process epoch 2
        let out2 = view_ref_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out2.num_rows(), 1);
        let k_add = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let w_add = out2.weights[0];
        assert_eq!((k_add, w_add), (3, 1));

        // Process epoch 3 (empty)
        let out3 = view_ref_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert!(out3.is_empty());
    }

    #[tokio::test]
    async fn oracle_view_ref_matches_batch() {
        let dir = TempDir::new().unwrap();
        let db = open_shard_db(&dir).await;
        let op_id = OperatorId(301);
        let schema = schema_kv();
        let sink = ViewSinkOp::new(db.clone(), op_id);

        let mut expected_epochs = Vec::new();

        let mut rng = 54321u64;
        let mut next_random = move || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng
        };

        for epoch in 0..10 {
            let mut key_vals = Vec::new();
            let mut weights = Vec::new();
            for _ in 0..15 {
                let k = (next_random() % 10) as i64;
                let v = (next_random() % 10) as i64;
                let w = if next_random() % 2 == 0 { 1 } else { -1 };
                key_vals.push((k, v));
                weights.push(w);
            }

            let k_array = Arc::new(Int64Array::from(
                key_vals.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            )) as ArrayRef;
            let v_array = Arc::new(Int64Array::from(
                key_vals.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )) as ArrayRef;
            let data = RecordBatch::try_new(schema.clone(), vec![k_array, v_array]).unwrap();
            let zset = ArrowZSet::new(data, weights.clone());

            sink.write_epoch(&zset, epoch).await.unwrap();

            let mut epoch_expected = Vec::new();
            for (idx, (k, v)) in key_vals.iter().enumerate() {
                epoch_expected.push((idx as u64, *k, *v, weights[idx]));
            }
            expected_epochs.push(epoch_expected);
        }

        db.flush().await.unwrap();

        let view_ref_op = ViewRefOp::new(db.clone(), op_id, schema.clone(), 0);

        for epoch in 0..10 {
            let out = view_ref_op
                .process_delta(ArrowZSet::empty(schema.clone()))
                .unwrap();
            let expected = &expected_epochs[epoch];
            assert_eq!(out.num_rows(), expected.len());

            let k_col = out
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v_col = out
                .data
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for idx in 0..out.num_rows() {
                let (_, exp_k, exp_v, exp_w) = expected[idx];
                assert_eq!(k_col.value(idx), exp_k);
                assert_eq!(v_col.value(idx), exp_v);
                assert_eq!(out.weights[idx], exp_w);
            }
        }

        let out = view_ref_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert!(out.is_empty());
    }
}
