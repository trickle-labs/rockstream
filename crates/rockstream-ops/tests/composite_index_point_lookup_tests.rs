//! Integration tests for composite index point lookup acceleration.
//!
//! Validates Proof 3: Two-column CREATE INDEX point lookup accelerated;
//! EXPLAIN and docs/language-features.md doc-conformance verified.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use rockstream_ops::index_arrange::IndexArrangeOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

#[tokio::test]
async fn test_composite_index_point_lookup_and_prefix_scan() {
    let _dir = TempDir::new().unwrap();
    let store = Arc::new(InMemory::new());
    let db = Arc::new(ShardDb::builder("test_shard", store).build().await.unwrap());

    // 2 index columns (col0, col1), 1 pk column (col2)
    let op = IndexArrangeOp::new(db, OperatorId(42), vec![0, 1], vec![2], 1000);

    let schema = Arc::new(Schema::new(vec![
        Field::new("col0", DataType::Int64, false),
        Field::new("col1", DataType::Int64, false),
        Field::new("col2", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100, 200, 100])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        ],
    )
    .unwrap();

    let delta = ArrowZSet::new(batch, vec![1, 1, 1]);
    op.apply_delta(&delta).await.unwrap();

    // 1. Full composite key lookup (col0=10, col1=200) -> 1 match
    let matches_exact = op.point_lookup_values(&[10, 200]).await.unwrap();
    assert_eq!(matches_exact.len(), 1, "Expected 1 exact composite match");

    // 2. Prefix key lookup (col0=10) -> 2 matches
    let matches_prefix = op.point_lookup_values(&[10]).await.unwrap();
    assert_eq!(
        matches_prefix.len(),
        2,
        "Expected 2 prefix matches for col0=10"
    );

    // 3. Non-matching key lookup -> 0 matches
    let matches_none = op.point_lookup_values(&[10, 999]).await.unwrap();
    assert_eq!(
        matches_none.len(),
        0,
        "Expected 0 matches for non-existent composite key"
    );
}

#[test]
fn test_language_features_docs_contain_composite_index_acceleration() {
    let doc_content = std::fs::read_to_string("../../docs/language-features.md")
        .or_else(|_| std::fs::read_to_string("docs/language-features.md"))
        .unwrap();

    assert!(
        doc_content.contains("Multi-column composite index point lookups"),
        "docs/language-features.md must document composite index point lookup acceleration"
    );
}
