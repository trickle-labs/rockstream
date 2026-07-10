//! Physical Snapshot Operator (v0.13 — Slice 2).
//!
//! `SnapshotOp` scans an existing view/table's materialized outputs in `ShardDb`
//! under the `ViewOutput` prefix, consolidates the row weights, and delivers
//! them as positive-weight Z-set entries in chunked bootstrap epochs.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rockstream_storage::{ShardDb, ShardPrefix};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// Named buffer limit to prevent unbounded memory accumulation.
pub const SNAPSHOT_BUFFER_LIMIT: usize = 1_000_000;

/// Physical Snapshot Operator.
pub struct SnapshotOp {
    batch_size: usize,
    schema: SchemaRef,
    rows: Mutex<Vec<Vec<i64>>>,
    cursor: AtomicUsize,
    is_complete: AtomicBool,
}

impl SnapshotOp {
    /// Load and initialize the snapshot operator by scanning shard storage.
    pub async fn load_and_initialize(
        db: Arc<ShardDb>,
        source_id: OperatorId,
        batch_size: usize,
        schema: SchemaRef,
        resume_offset: usize,
    ) -> Result<Self, OpError> {
        let prefix = {
            let mut p = Vec::with_capacity(9);
            p.push(ShardPrefix::ViewOutput.as_byte());
            p.extend_from_slice(&source_id.0.to_be_bytes());
            p
        };

        // Scan all raw view output entries (64MB memory cap)
        let (entries, _truncated) = db
            .scan_prefix_bounded(&prefix, 64 * 1024 * 1024)
            .await
            .map_err(OpError::storage)?;

        let num_cols = schema.fields().len();
        let mut map: std::collections::HashMap<Vec<i64>, i64> = std::collections::HashMap::new();

        for (key, value) in entries {
            if key.len() < 25 {
                continue;
            }
            if value.len() < (num_cols + 1) * 8 {
                continue;
            }
            let mut cols = Vec::with_capacity(num_cols);
            for c in 0..num_cols {
                let v = i64::from_be_bytes(value[c * 8..(c + 1) * 8].try_into().unwrap());
                cols.push(v);
            }
            let w = i64::from_be_bytes(value[num_cols * 8..(num_cols + 1) * 8].try_into().unwrap());
            *map.entry(cols).or_insert(0) += w;
        }

        // Keep only positive-weight Z-set rows
        let mut consolidated: Vec<Vec<i64>> = map
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .map(|(r, _)| r)
            .collect();

        // Enforce buffer limit
        if consolidated.len() > SNAPSHOT_BUFFER_LIMIT {
            return Err(OpError::storage(
                rockstream_storage::StorageError::Unsupported(format!(
                    "Snapshot size {} exceeds SNAPSHOT_BUFFER_LIMIT ({})",
                    consolidated.len(),
                    SNAPSHOT_BUFFER_LIMIT
                )),
            ));
        }

        // Deterministic sorting of rows
        consolidated.sort();

        let is_complete = resume_offset >= consolidated.len();

        Ok(SnapshotOp {
            batch_size,
            schema,
            rows: Mutex::new(consolidated),
            cursor: AtomicUsize::new(resume_offset),
            is_complete: AtomicBool::new(is_complete),
        })
    }

    /// Set a new cursor position to resume from.
    pub fn resume_from(&self, offset: usize) {
        self.cursor.store(offset, Ordering::SeqCst);
        let total = self.rows.lock().unwrap().len();
        self.is_complete.store(offset >= total, Ordering::SeqCst);
    }
}

impl Operator for SnapshotOp {
    fn name(&self) -> &str {
        "SnapshotOp"
    }

    fn process_delta(&self, _delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let cursor = self.cursor.load(Ordering::SeqCst);
        let rows_guard = self.rows.lock().unwrap();
        let total = rows_guard.len();

        if cursor >= total {
            self.is_complete.store(true, Ordering::SeqCst);
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let end = (cursor + self.batch_size).min(total);
        let chunk = &rows_guard[cursor..end];
        let chunk_len = chunk.len();

        let mut cols = vec![Vec::with_capacity(chunk_len); self.schema.fields().len()];
        for row in chunk {
            for (c, &v) in row.iter().enumerate() {
                cols[c].push(v);
            }
        }

        let arrow_cols: Vec<ArrayRef> = cols
            .into_iter()
            .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
            .collect();

        let data = RecordBatch::try_new(self.schema.clone(), arrow_cols).map_err(OpError::arrow)?;
        let weights = vec![1i64; chunk_len];

        self.cursor.store(end, Ordering::SeqCst);
        if end >= total {
            self.is_complete.store(true, Ordering::SeqCst);
        }

        Ok(ArrowZSet::new(data, weights))
    }

    fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::SeqCst)
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
    async fn test_snapshot_op_basic() {
        let dir = TempDir::new().unwrap();
        let db = open_shard_db(&dir).await;
        let op_id = OperatorId(42);

        // Write some rows using ViewSinkOp
        let sink = ViewSinkOp::new(db.clone(), op_id);

        let schema = schema_kv();
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (1, 10)], 1); // 1 is weight
        sink.write_epoch(&batch, 0).await.unwrap();

        // Write a deletion for one row in next epoch
        let batch2 = ArrowZSet::from_ab_rows(&[(1, 10)], -1); // retract (1,10)
        sink.write_epoch(&batch2, 1).await.unwrap();

        db.flush().await.unwrap();

        // Consolidated state should be:
        // (1, 10) -> weight 1 + 1 - 1 = 1
        // (2, 20) -> weight 1

        // Initialize SnapshotOp with batch_size = 1
        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 1, schema.clone(), 0)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        // Process first delta (empty input)
        let out1 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out1.num_rows(), 1);
        let k1 = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let v1 = out1
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((k1, v1), (1, 10)); // sorted order
        assert!(!snap_op.is_complete());

        // Process second delta
        let out2 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out2.num_rows(), 1);
        let k2 = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let v2 = out2
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((k2, v2), (2, 20));
        assert!(snap_op.is_complete());

        // Next call returns empty
        let out3 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert!(out3.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_op_resume() {
        let dir = TempDir::new().unwrap();
        let db = open_shard_db(&dir).await;
        let op_id = OperatorId(99);

        let sink = ViewSinkOp::new(db.clone(), op_id);
        let schema = schema_kv();
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30)], 1);
        sink.write_epoch(&batch, 0).await.unwrap();
        db.flush().await.unwrap();

        // Load with resume_offset = 1
        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 1)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        let out = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out.num_rows(), 2); // should emit (2,20) and (3,30)
        let k0 = out
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let k1 = out
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        assert_eq!(k0, 2);
        assert_eq!(k1, 3);
        assert!(snap_op.is_complete());
    }

    #[tokio::test]
    async fn oracle_snapshot_matches_batch() {
        let dir = TempDir::new().unwrap();
        let db = open_shard_db(&dir).await;
        let op_id = OperatorId(201);
        let schema = schema_kv();
        let sink = ViewSinkOp::new(db.clone(), op_id);

        let mut expected: std::collections::HashMap<(i64, i64), i64> =
            std::collections::HashMap::new();

        let mut rng = 12345u64;
        let mut next_random = move || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng
        };

        for epoch in 0..10 {
            let mut key_vals = Vec::new();
            let mut weights = Vec::new();
            for _ in 0..20 {
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

            for (idx, (k, v)) in key_vals.iter().enumerate() {
                *expected.entry((*k, *v)).or_insert(0) += weights[idx];
            }
        }

        db.flush().await.unwrap();

        let mut expected_positive: Vec<(i64, i64)> = expected
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .map(|(k, _)| k)
            .collect();
        expected_positive.sort();

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 5, schema.clone(), 0)
            .await
            .unwrap();

        let mut actual = Vec::new();
        while !snap_op.is_complete() {
            let out = snap_op
                .process_delta(ArrowZSet::empty(schema.clone()))
                .unwrap();
            if out.is_empty() {
                break;
            }
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
            for i in 0..out.num_rows() {
                actual.push((k_col.value(i), v_col.value(i)));
            }
        }

        assert_eq!(actual, expected_positive);
    }
}
