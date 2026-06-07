//! DataFusion batch reference oracle for the v0.2 harness.
//!
//! This module provides `run_noop_batch_query`: given a set of present rows,
//! it registers them as an in-memory Arrow table in DataFusion and runs
//! `SELECT id, value FROM t ORDER BY id, value`, returning the result as a
//! `Vec<TestRow>`.
//!
//! For the trivial no-op pipeline (`SELECT * FROM t`), the batch result must
//! equal the incremental result for any sequence of deltas. This validates
//! both the DataFusion integration and the Z-set accumulation logic.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

use crate::zset::TestRow;

/// The Arrow schema for the test table: `(id: Int64, value: Int64)`.
pub fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Convert a slice of `TestRow` to an Arrow `RecordBatch`.
pub fn rows_to_batch(rows: &[TestRow]) -> RecordBatch {
    let schema = test_schema();
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let values: Vec<i64> = rows.iter().map(|r| r.value).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("valid RecordBatch construction")
}

/// Run a no-op batch query (`SELECT id, value FROM t ORDER BY id, value`)
/// against the given rows using DataFusion, and return the result as
/// `Vec<TestRow>` sorted by `(id, value)`.
///
/// This is the **batch reference** side of the oracle property:
/// `incremental_noop(deltas) == run_noop_batch_query(present_rows(accumulated))`.
pub async fn run_noop_batch_query(rows: &[TestRow]) -> datafusion::error::Result<Vec<TestRow>> {
    let ctx = SessionContext::new();
    let schema = test_schema();

    // Register the input rows as an in-memory table.
    let batch = rows_to_batch(rows);
    let mem_table = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(mem_table))?;

    // Run the no-op query and collect results.
    let df = ctx
        .sql("SELECT id, value FROM t ORDER BY id, value")
        .await?;
    let batches = df.collect().await?;

    // Convert Arrow batches back to TestRow.
    let mut result = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column must be Int64");
        let val_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column must be Int64");
        for i in 0..batch.num_rows() {
            result.push(TestRow {
                id: id_col.value(i),
                value: val_col.value(i),
            });
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn batch_query_empty_input() {
        let result = run_noop_batch_query(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn batch_query_single_row() {
        let rows = vec![TestRow { id: 1, value: 42 }];
        let result = run_noop_batch_query(&rows).await.unwrap();
        assert_eq!(result, rows);
    }

    #[tokio::test]
    async fn batch_query_multiple_rows_sorted() {
        let rows = vec![
            TestRow { id: 3, value: 30 },
            TestRow { id: 1, value: 10 },
            TestRow { id: 2, value: 20 },
        ];
        let result = run_noop_batch_query(&rows).await.unwrap();
        // Result should be sorted by (id, value)
        assert_eq!(
            result,
            vec![
                TestRow { id: 1, value: 10 },
                TestRow { id: 2, value: 20 },
                TestRow { id: 3, value: 30 },
            ]
        );
    }

    #[tokio::test]
    async fn batch_query_identity_roundtrip() {
        let rows = vec![
            TestRow { id: 10, value: 100 },
            TestRow { id: 20, value: 200 },
            TestRow { id: 30, value: 300 },
        ];
        // Input is already sorted; output should be identical
        let result = run_noop_batch_query(&rows).await.unwrap();
        assert_eq!(result, rows);
    }
}
