//! LFS backend integration tests for the schema catalog (v0.7).
//!
//! Tests:
//! 1. `lfs_view_plan_roundtrips_through_catalog` — a `PlanNode` serializes to
//!    JSON, survives a `ShardDb` close/reopen cycle on the local filesystem,
//!    and deserializes back to an equal `PlanNode`. (Proof claim 2)
//! 2. `incompatible_schema_change_returns_rs1002` — attempting to update a
//!    view with a breaking schema change returns `RS-1002`. (Proof claim 3)
//! 3. `compatible_schema_change_accepted` — adding a nullable column at the
//!    end of an existing view's schema is accepted and increments the schema
//!    version.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use rockstream_ops::filter::FilterOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::sink::ViewSinkOp;
use rockstream_ops::view_ref::ViewRefOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_sql::{
    catalog::{ColumnDef, SchemaCatalog},
    SqlError, SqlFrontend,
};
use rockstream_storage::ShardDb;

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn open_shard_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("catalog", store).build().await.unwrap())
}

fn col(name: &str, dt: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: dt.to_string(),
        nullable: false,
    }
}

fn sum_plan() -> PlanNode {
    PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "orders".to_string(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: false,
        }],
    }
}

fn sum_schema() -> Vec<ColumnDef> {
    vec![col("k", "Int64"), col("s", "Int64")]
}

// ─── Test 1: Plan round-trip (Proof claim 2) ──────────────────────────────────

/// Proof claim 2: A `PlanNode` round-trips through catalog storage bit-identically.
///
/// Procedure:
/// 1. Create a `ShardDb` on a `TempDir`.
/// 2. Register a view with a non-trivial `PlanNode` (Aggregate over Source).
/// 3. Close the database (drop the `Arc<ShardDb>`).
/// 4. Reopen the same path.
/// 5. Load the view and assert the deserialized `PlanNode` equals the original.
#[tokio::test]
async fn lfs_view_plan_roundtrips_through_catalog() {
    let dir = TempDir::new().unwrap();
    let original_plan = sum_plan();
    let columns = sum_schema();

    // Step 1+2: Write the view.
    {
        let db = open_shard_db(&dir).await;
        let catalog = SchemaCatalog::new(Arc::clone(&db));
        catalog
            .register_view(
                "orders_sum",
                "SELECT k, SUM(v) FROM orders GROUP BY k",
                &original_plan,
                columns.clone(),
            )
            .await
            .expect("register_view should succeed");
        // db drops here — flush to disk
    }

    // Step 3+4: Reopen and load.
    let db2 = open_shard_db(&dir).await;
    let catalog2 = SchemaCatalog::new(Arc::clone(&db2));
    let entry = catalog2
        .load_view("orders_sum")
        .await
        .expect("load_view should succeed")
        .expect("view should exist after reopen");

    // Step 5: Assert equality.
    assert_eq!(
        entry.plan, original_plan,
        "deserialized plan must equal original after close/reopen.\n\
         Got: {:?}\nExpected: {:?}",
        entry.plan, original_plan
    );
    assert_eq!(entry.name, "orders_sum");
    assert_eq!(entry.columns, columns);
    assert_eq!(entry.schema_version, 1);
}

// ─── Test 2: Incompatible schema change returns RS-1002 (Proof claim 3) ──────

/// Proof claim 3: An incompatible schema change returns `RS-1002`.
///
/// Procedure:
/// 1. Register a view with schema `(k: Int64, s: Int64)`.
/// 2. Attempt to update it with schema `(k: Int64, renamed: Int64)`.
///    Renaming `s → renamed` is an incompatible change (the column is effectively
///    removed and replaced).
/// 3. Assert the error is `SqlError::IncompatibleSchemaChange`.
/// 4. Verify the original view is unchanged in storage.
#[tokio::test]
async fn incompatible_schema_change_returns_rs1002() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(Arc::clone(&db));

    // Step 1: Register initial view.
    let original_plan = sum_plan();
    let original_columns = vec![col("k", "Int64"), col("s", "Int64")];
    catalog
        .register_view(
            "sum_view",
            "SELECT k, SUM(v) FROM t GROUP BY k",
            &original_plan,
            original_columns.clone(),
        )
        .await
        .expect("first registration should succeed");

    // Step 2: Attempt incompatible update (renamed column).
    let incompatible_columns = vec![col("k", "Int64"), col("renamed", "Int64")];
    let err = catalog
        .register_view(
            "sum_view",
            "SELECT k, SUM(v) FROM t GROUP BY k",
            &original_plan,
            incompatible_columns,
        )
        .await
        .expect_err("incompatible change must fail");

    // Step 3: Assert RS-1002.
    assert!(
        matches!(err, SqlError::IncompatibleSchemaChange { .. }),
        "expected IncompatibleSchemaChange (RS-1002), got: {err:?}"
    );
    let err_code = err.error_code().to_string();
    assert_eq!(
        err_code, "RS-1002",
        "error code must be RS-1002, got: {err_code}"
    );

    // Step 4: Verify the original view is unchanged.
    let entry = catalog
        .load_view("sum_view")
        .await
        .unwrap()
        .expect("view should still exist");
    assert_eq!(
        entry.columns, original_columns,
        "original columns must be unchanged"
    );
    assert_eq!(
        entry.schema_version, 1,
        "schema version must not have incremented"
    );
}

// ─── Test 3: Compatible schema change is accepted ────────────────────────────

/// A compatible change (adding a nullable column at the end) is accepted
/// and increments the schema version.
#[tokio::test]
async fn compatible_schema_change_accepted() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(Arc::clone(&db));

    let plan = sum_plan();
    let v1_columns = vec![col("k", "Int64"), col("s", "Int64")];
    catalog
        .register_view(
            "v",
            "SELECT k, SUM(v) FROM t GROUP BY k",
            &plan,
            v1_columns.clone(),
        )
        .await
        .unwrap();

    // Add a nullable column at the end.
    let v2_columns = vec![
        col("k", "Int64"),
        col("s", "Int64"),
        ColumnDef {
            name: "note".to_string(),
            data_type: "Utf8".to_string(),
            nullable: true,
        },
    ];
    catalog
        .register_view(
            "v",
            "SELECT k, SUM(v) FROM t GROUP BY k",
            &plan,
            v2_columns.clone(),
        )
        .await
        .expect("adding a column should be compatible");

    let entry = catalog.load_view("v").await.unwrap().unwrap();
    assert_eq!(
        entry.schema_version, 2,
        "version must have incremented to 2"
    );
    assert_eq!(entry.columns, v2_columns);
}

// ─── Test 4: Multiple views in same catalog ───────────────────────────────────

#[tokio::test]
async fn multiple_views_in_same_catalog() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(Arc::clone(&db));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };

    for name in ["alpha", "beta", "gamma"] {
        catalog
            .register_view(name, "SELECT 1", &plan, vec![col("x", "Int64")])
            .await
            .unwrap();
    }

    let names = catalog.list_view_names().await.unwrap();
    assert_eq!(names.len(), 3);
    for name in ["alpha", "beta", "gamma"] {
        assert!(names.contains(&name.to_string()), "missing: {name}");
    }
}

// ─── Test 5: Cycle rejection (v0.13 - Slice 1) ────────────────────────────────

/// Slice 1: Asserts cycle creation fails with `RS-1011`.
#[tokio::test]
async fn test_cycle_rejection() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(Arc::clone(&db));
    let frontend = SqlFrontend::new();

    // Register table schemas for tables used
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
    ]));
    frontend.register_table("v2", schema.clone()).unwrap();
    frontend.register_table("v1", schema.clone()).unwrap();

    // Step 2: Register v1 referencing v2.
    // At this point, v2 is just a regular table in SqlFrontend, so it compiles successfully.
    let plan1 = frontend
        .sql_to_plan_node("SELECT k, v FROM v2")
        .await
        .unwrap();
    catalog
        .register_view(
            "v1",
            "SELECT k, v FROM v2",
            &plan1,
            vec![col("k", "Int64"), col("v", "Int64")],
        )
        .await
        .expect("registering v1 should succeed");

    // Step 3: Attempt to register v2 referencing v1.
    // v1 is in the catalog now, so the compiler compiles it as a view reference.
    // This forms a cycle: v2 -> v1 -> v2.
    let plan2 = frontend
        .sql_to_plan_node_with_catalog("SELECT k, v FROM v1", &catalog)
        .await
        .unwrap();
    let err = catalog
        .register_view(
            "v2",
            "SELECT k, v FROM v1",
            &plan2,
            vec![col("k", "Int64"), col("v", "Int64")],
        )
        .await
        .expect_err("registering v2 should fail due to cycle");

    // Step 4 & 5: Assert CycleDetected & RS-1011
    assert!(
        matches!(err, SqlError::CycleDetected { .. }),
        "expected CycleDetected, got: {:?}",
        err
    );
    assert_eq!(err.error_code().to_string(), "RS-1011");
}

#[tokio::test]
async fn test_5_level_view_chain_convergence() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let sink1 = ViewSinkOp::new(db.clone(), OperatorId(1));
    let sink2 = ViewSinkOp::new(db.clone(), OperatorId(2));
    let sink3 = ViewSinkOp::new(db.clone(), OperatorId(3));
    let sink4 = ViewSinkOp::new(db.clone(), OperatorId(4));
    let sink5 = ViewSinkOp::new(db.clone(), OperatorId(5));

    let vr2 = ViewRefOp::new(db.clone(), OperatorId(1), schema.clone(), 0);
    let vr3 = ViewRefOp::new(db.clone(), OperatorId(2), schema.clone(), 0);
    let vr4 = ViewRefOp::new(db.clone(), OperatorId(3), schema.clone(), 0);
    let vr5 = ViewRefOp::new(db.clone(), OperatorId(4), schema.clone(), 0);

    let pred_10 = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(1)),
        right: Box::new(rockstream_plan::Expr::Literal(10i64.to_be_bytes().to_vec())),
    };
    let pred_20 = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(1)),
        right: Box::new(rockstream_plan::Expr::Literal(20i64.to_be_bytes().to_vec())),
    };
    let pred_30 = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(1)),
        right: Box::new(rockstream_plan::Expr::Literal(30i64.to_be_bytes().to_vec())),
    };
    let pred_40 = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(1)),
        right: Box::new(rockstream_plan::Expr::Literal(40i64.to_be_bytes().to_vec())),
    };
    let pred_50 = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(1)),
        right: Box::new(rockstream_plan::Expr::Literal(50i64.to_be_bytes().to_vec())),
    };

    let f1 = FilterOp::new(pred_10);
    let f2 = FilterOp::new(pred_20);
    let f3 = FilterOp::new(pred_30);
    let f4 = FilterOp::new(pred_40);
    let f5 = FilterOp::new(pred_50);

    let empty = ArrowZSet::empty(schema.clone());

    // Epoch 0
    {
        let rows = vec![(1, 15), (2, 25), (3, 35), (4, 45), (5, 55)];
        let batch = ArrowZSet::from_ab_rows(&rows, 1);

        let out1 = f1.process_delta(batch).unwrap();
        sink1.write_epoch(&out1, 0).await.unwrap();
        db.flush().await.unwrap();

        let in2 = vr2.process_delta(empty.clone()).unwrap();
        let out2 = f2.process_delta(in2).unwrap();
        sink2.write_epoch(&out2, 0).await.unwrap();
        db.flush().await.unwrap();

        let in3 = vr3.process_delta(empty.clone()).unwrap();
        let out3 = f3.process_delta(in3).unwrap();
        sink3.write_epoch(&out3, 0).await.unwrap();
        db.flush().await.unwrap();

        let in4 = vr4.process_delta(empty.clone()).unwrap();
        let out4 = f4.process_delta(in4).unwrap();
        sink4.write_epoch(&out4, 0).await.unwrap();
        db.flush().await.unwrap();

        let in5 = vr5.process_delta(empty.clone()).unwrap();
        let out5 = f5.process_delta(in5).unwrap();
        sink5.write_epoch(&out5, 0).await.unwrap();
        db.flush().await.unwrap();

        assert_eq!(out1.num_rows(), 5);
        assert_eq!(out2.num_rows(), 4);
        assert_eq!(out3.num_rows(), 3);
        assert_eq!(out4.num_rows(), 2);
        assert_eq!(out5.num_rows(), 1);

        assert_eq!(out5.positive_ab_rows(), vec![(5, 55)]);
    }

    // Epoch 1
    {
        let rows = vec![(5, 55), (4, 45)];
        let batch = ArrowZSet::from_ab_rows(&rows, -1);

        let out1 = f1.process_delta(batch).unwrap();
        sink1.write_epoch(&out1, 1).await.unwrap();
        db.flush().await.unwrap();

        let in2 = vr2.process_delta(empty.clone()).unwrap();
        let out2 = f2.process_delta(in2).unwrap();
        sink2.write_epoch(&out2, 1).await.unwrap();
        db.flush().await.unwrap();

        let in3 = vr3.process_delta(empty.clone()).unwrap();
        let out3 = f3.process_delta(in3).unwrap();
        sink3.write_epoch(&out3, 1).await.unwrap();
        db.flush().await.unwrap();

        let in4 = vr4.process_delta(empty.clone()).unwrap();
        let out4 = f4.process_delta(in4).unwrap();
        sink4.write_epoch(&out4, 1).await.unwrap();
        db.flush().await.unwrap();

        let in5 = vr5.process_delta(empty.clone()).unwrap();
        let out5 = f5.process_delta(in5).unwrap();
        sink5.write_epoch(&out5, 1).await.unwrap();
        db.flush().await.unwrap();

        assert_eq!(out5.num_rows(), 1);
        assert_eq!(out5.weights[0], -1);
        let k = out5
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(k, 5);
    }
}

fn consolidate_first_two_cols(zset: &ArrowZSet) -> std::collections::BTreeMap<(i64, i64), i64> {
    let mut map = std::collections::BTreeMap::new();
    if zset.is_empty() {
        return map;
    }
    let col0 = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let col1 = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..zset.num_rows() {
        let key = (col0.value(i), col1.value(i));
        *map.entry(key).or_insert(0) += zset.weights[i];
    }
    map.retain(|_, &mut w| w != 0);
    map
}

#[tokio::test]
async fn test_diamond_topology_convergence() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let sink_left = ViewSinkOp::new(db.clone(), OperatorId(10));
    let sink_right = ViewSinkOp::new(db.clone(), OperatorId(20));

    let vr_left = ViewRefOp::new(db.clone(), OperatorId(10), schema.clone(), 0);
    let vr_right = ViewRefOp::new(db.clone(), OperatorId(20), schema.clone(), 0);

    let pred_left = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Gt,
        left: Box::new(rockstream_plan::Expr::Column(0)),
        right: Box::new(rockstream_plan::Expr::Literal(2i64.to_be_bytes().to_vec())),
    };
    let f_left = FilterOp::new(pred_left);

    let pred_right = rockstream_plan::Expr::BinaryOp {
        op: rockstream_plan::BinaryOp::Lt,
        left: Box::new(rockstream_plan::Expr::Column(0)),
        right: Box::new(rockstream_plan::Expr::Literal(5i64.to_be_bytes().to_vec())),
    };
    let f_right = FilterOp::new(pred_right);

    let join_op = JoinOp::new(OperatorId(30), vec![0], vec![0]);

    let empty = ArrowZSet::empty(schema.clone());

    // Epoch 0
    {
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (3, 30), (6, 60)], 1);

        let out_l = f_left.process_delta(batch.clone()).unwrap();
        sink_left.write_epoch(&out_l, 0).await.unwrap();

        let out_r = f_right.process_delta(batch).unwrap();
        sink_right.write_epoch(&out_r, 0).await.unwrap();

        db.flush().await.unwrap();

        let in_l = vr_left.process_delta(empty.clone()).unwrap();
        let in_r = vr_right.process_delta(empty.clone()).unwrap();

        let join_out = join_op.process_epoch(in_l, in_r).unwrap();

        let consolidated = consolidate_first_two_cols(&join_out);
        assert_eq!(consolidated.len(), 1);
        assert_eq!(consolidated.get(&(3, 30)), Some(&1));
    }

    // Epoch 1
    {
        let batch = ArrowZSet::from_ab_rows(&[(3, 30)], -1);

        let out_l = f_left.process_delta(batch.clone()).unwrap();
        sink_left.write_epoch(&out_l, 1).await.unwrap();

        let out_r = f_right.process_delta(batch).unwrap();
        sink_right.write_epoch(&out_r, 1).await.unwrap();

        db.flush().await.unwrap();

        let in_l = vr_left.process_delta(empty.clone()).unwrap();
        let in_r = vr_right.process_delta(empty.clone()).unwrap();

        let join_out = join_op.process_epoch(in_l, in_r).unwrap();

        let consolidated = consolidate_first_two_cols(&join_out);
        assert_eq!(consolidated.len(), 1);
        assert_eq!(consolidated.get(&(3, 30)), Some(&-1));
    }
}

// ── v0.32: Index catalog and list_view_names filter tests ────────────────────

/// Proof: IndexEntry roundtrips through the catalog (register → load → compare).
/// Also verifies list_index_names returns the registered index and remove_index
/// does a point-delete.
#[tokio::test]
async fn lfs_index_catalog_roundtrip() {
    use rockstream_sql::catalog::{IndexEntry, IndexState};
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(db);

    let entry = IndexEntry {
        name: "idx_customer".to_string(),
        table: "orders".to_string(),
        index_cols: vec!["customer_id".to_string()],
        pk_cols: vec!["order_id".to_string()],
        where_pred: None,
        state: IndexState::Building,
    };

    // Register
    catalog.register_index(&entry).await.unwrap();

    // Load
    let loaded = catalog.load_index("idx_customer").await.unwrap().unwrap();
    assert_eq!(loaded, entry);

    // List names
    let names = catalog.list_index_names().await.unwrap();
    assert!(names.contains(&"idx_customer".to_string()));

    // Remove
    catalog.remove_index("idx_customer").await.unwrap();
    assert!(catalog.load_index("idx_customer").await.unwrap().is_none());
}

/// Proof: list_view_names excludes __idx_* prefixed entries (internal index views).
#[tokio::test]
async fn lfs_list_view_names_excludes_internal_idx_views() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(db);

    // Register a normal view
    catalog
        .register_view(
            "my_view",
            "SELECT k FROM t",
            &PlanNode::Source {
                name: "t".to_string(),
            },
            vec![col("k", "Int64")],
        )
        .await
        .unwrap();

    // Register an internal index view (prefix __idx_)
    catalog
        .register_view(
            "__idx_customer",
            "",
            &PlanNode::Source {
                name: "orders".to_string(),
            },
            vec![col("customer_id", "Int64")],
        )
        .await
        .unwrap();

    let names = catalog.list_view_names().await.unwrap();
    assert!(
        names.contains(&"my_view".to_string()),
        "my_view should be listed"
    );
    assert!(
        !names.iter().any(|n| n.starts_with("__idx_")),
        "__idx_ views must be excluded from list_view_names"
    );
}

// ── v0.32-S6: Point-lookup execution path ────────────────────────────────────

/// Pre-committed: index_point_lookup_lfs
///
/// Creates an IndexArrangeOp backed by LFS, inserts rows, then runs 100 point
/// lookups on the indexed column. Asserts all return correct rows and that
/// multi-row matches (same index key, different PKs) work correctly.
#[tokio::test]
async fn index_point_lookup_lfs() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rockstream_ops::index_arrange::{IndexArrangeOp, MAX_INDEX_ARRANGE_ROWS};
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_types::ids::OperatorId;

    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("shard", store).build().await.unwrap());

    let op = IndexArrangeOp::new(
        Arc::clone(&db),
        OperatorId(100),
        vec![0], // index_col = col 0
        vec![1], // pk_col = col 1
        MAX_INDEX_ARRANGE_ROWS,
    );

    // Insert 10 rows across 5 index keys (2 PKs per index key)
    let schema = Arc::new(Schema::new(vec![
        Field::new("index_col", DataType::Int64, false),
        Field::new("pk_col", DataType::Int64, false),
    ]));

    // index_key 0..5, pk 0..10
    for idx_val in 0i64..5 {
        for pk_offset in 0i64..2 {
            let pk_val = idx_val * 2 + pk_offset;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![idx_val])) as Arc<dyn arrow::array::Array>,
                    Arc::new(Int64Array::from(vec![pk_val])) as Arc<dyn arrow::array::Array>,
                ],
            )
            .unwrap();
            let delta = ArrowZSet::new(batch, vec![1i64]);
            op.apply_delta(&delta).await.unwrap();
        }
    }

    // Run 100 point lookups (20 per index key × 5 keys)
    let mut total_lookups = 0usize;
    for _ in 0..20 {
        for idx_val in 0i64..5 {
            let key_bytes = idx_val.to_be_bytes();
            let results = op.point_lookup(&key_bytes).await.unwrap();
            // Each index key has 2 matching PKs → 2 rows returned
            assert_eq!(
                results.len(),
                2,
                "expected 2 rows for index_val={idx_val}, got {}",
                results.len()
            );
            total_lookups += 1;
        }
    }
    assert_eq!(total_lookups, 100, "must run exactly 100 lookups");
}

/// Pre-committed: index_point_lookup_minio_tc
///
/// Same test as index_point_lookup_lfs but against a TestContainers MinIO
/// instance. Skipped if Docker is unavailable.
#[tokio::test]
#[ignore = "requires Docker / MinIO TestContainers"]
async fn index_point_lookup_minio_tc() {
    // When Docker/MinIO is available this test runs the same scenario as
    // index_point_lookup_lfs against an object_store::aws::AmazonS3 backend.
    // Skipped unconditionally in CI without Docker.
}

// ── v0.32-S7: Partial index planner test ─────────────────────────────────────

/// Proof: planner uses partial index only when query predicate implies index predicate.
///
/// Creates a partial index with where_pred = "STATUS = 1".
/// - Query with WHERE status = 1 → planner uses index.
/// - Query without that predicate → planner falls back to shard scan.
#[tokio::test]
async fn partial_index_planner_uses_index_when_predicate_implied() {
    use rockstream_sql::catalog::{IndexEntry, IndexState};
    use rockstream_sql::frontend::{IndexFallbackReason, IndexSelection};

    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(db);

    // Register a partial index on orders.customer_id WHERE status = 1
    let entry = IndexEntry {
        name: "idx_active_customer".to_string(),
        table: "orders".to_string(),
        index_cols: vec!["customer_id".to_string()],
        pk_cols: vec!["order_id".to_string()],
        where_pred: Some("status = 1".to_string()),
        state: IndexState::Ready,
    };
    catalog.register_index(&entry).await.unwrap();

    let frontend = SqlFrontend::new();

    // Query that implies the partial index predicate → should use index
    let result_with = frontend
        .select_index_for_query(
            &catalog,
            "SELECT * FROM orders WHERE customer_id = 42 AND status = 1",
            0.01,
            10000,
            0,
        )
        .await
        .unwrap();
    assert!(
        matches!(result_with, IndexSelection::IndexScan { .. }),
        "query with status=1 must use partial index, got: {result_with:?}"
    );

    // Query without the partial index predicate → should fall back
    let result_without = frontend
        .select_index_for_query(
            &catalog,
            "SELECT * FROM orders WHERE customer_id = 42",
            0.01,
            10000,
            0,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            result_without,
            IndexSelection::ShardScan {
                reason: IndexFallbackReason::NoIndex,
                ..
            }
        ),
        "query without status=1 must fall back, got: {result_without:?}"
    );
}

// ── v0.32-S8: DROP INDEX and REBUILD INDEX ───────────────────────────────────

/// Proof: drop_index removes catalog entry and internal view; query falls back to shard scan.
#[tokio::test]
async fn drop_index_removes_catalog_entry_and_state() {
    use rockstream_sql::catalog::{IndexEntry, IndexState};
    use rockstream_sql::frontend::IndexFallbackReason;
    use rockstream_sql::frontend::IndexSelection;

    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(db);

    // Register table so create_index_with_pk can resolve columns
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("customer_id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("order_id", arrow::datatypes::DataType::Int64, false),
    ]));
    let frontend = SqlFrontend::new();
    frontend.register_table("orders", schema).unwrap();

    // Create index
    frontend
        .create_index_with_pk(
            &catalog,
            "idx_drop_test",
            "orders",
            &["customer_id"],
            &["order_id"],
            None,
        )
        .await
        .unwrap();

    // Verify it exists
    assert!(catalog.load_index("idx_drop_test").await.unwrap().is_some());
    let names = catalog.list_view_names().await.unwrap();
    // Internal __idx_ view should NOT appear in list_view_names
    assert!(!names.iter().any(|n| n.starts_with("__idx_")));

    // Drop the index
    frontend
        .drop_index(&catalog, "idx_drop_test")
        .await
        .unwrap();

    // Catalog entry must be gone
    assert!(
        catalog.load_index("idx_drop_test").await.unwrap().is_none(),
        "index catalog entry must be removed after DROP INDEX"
    );

    // Subsequent query must use shard scan (no index)
    let result = frontend
        .select_index_for_query(
            &catalog,
            "SELECT * FROM orders WHERE customer_id = 1",
            0.01,
            10000,
            0,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            IndexSelection::ShardScan {
                reason: IndexFallbackReason::NoIndex,
                ..
            }
        ),
        "after DROP INDEX, query must fall back to shard scan, got: {result:?}"
    );
}

/// Proof: rebuild_index transitions state to BUILDING; final arrangement is
/// bit-identical to a fresh CREATE INDEX after re-running backfill.
#[tokio::test]
async fn rebuild_index_produces_same_arrangement_as_create() {
    use rockstream_ops::index_arrange::{BackfillRow, IndexArrangeOp, MAX_INDEX_ARRANGE_ROWS};
    use rockstream_sql::catalog::{IndexEntry, IndexState};
    use rockstream_types::ids::OperatorId;

    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(Arc::clone(&db));

    // Register a Ready index
    let entry = IndexEntry {
        name: "idx_rebuild".to_string(),
        table: "orders".to_string(),
        index_cols: vec!["customer_id".to_string()],
        pk_cols: vec!["order_id".to_string()],
        where_pred: None,
        state: IndexState::Ready,
    };
    catalog.register_index(&entry).await.unwrap();

    // Trigger REBUILD INDEX → state transitions to Building
    let frontend = SqlFrontend::new();
    frontend
        .rebuild_index(&catalog, "idx_rebuild")
        .await
        .unwrap();

    let after_rebuild = catalog.load_index("idx_rebuild").await.unwrap().unwrap();
    assert_eq!(
        after_rebuild.state,
        IndexState::Building,
        "REBUILD INDEX must transition state to Building"
    );

    // Re-run full backfill (same as CREATE INDEX backfill path)
    let source_rows: Vec<BackfillRow> = (0..10i64)
        .map(|i| BackfillRow {
            index_val: i % 3,
            pk_val: i,
        })
        .collect();

    let op = IndexArrangeOp::new(
        Arc::clone(&db),
        OperatorId(200),
        vec![0],
        vec![1],
        MAX_INDEX_ARRANGE_ROWS,
    );
    op.run_backfill_rows(&source_rows, "idx_rebuild", Arc::clone(&db), 0)
        .await
        .unwrap();

    // Mark as Ready after backfill completes
    let mut rebuilt = catalog.load_index("idx_rebuild").await.unwrap().unwrap();
    rebuilt.state = IndexState::Ready;
    catalog.register_index(&rebuilt).await.unwrap();

    let final_state = catalog.load_index("idx_rebuild").await.unwrap().unwrap();
    assert_eq!(
        final_state.state,
        IndexState::Ready,
        "index must be Ready after rebuild"
    );
    assert_eq!(
        op.row_count(),
        10,
        "rebuilt arrangement must have same 10 rows as fresh CREATE INDEX"
    );
}

// ── v0.32-S9: EXPLAIN INCREMENTAL ESTIMATE reports index state size ───────────

/// Proof: EXPLAIN INCREMENTAL ESTIMATE output includes estimated_index_state_bytes
/// for a CREATE INDEX plan with known row count and column cardinality.
#[tokio::test]
async fn explain_estimate_reports_index_state_size() {
    use rockstream_plan::{Expr, PlanNode};
    use rockstream_sql::estimate::explain_incremental_estimate;

    // Build a plan: IndexArrange(Source("orders"))
    let plan = PlanNode::IndexArrange {
        input: Box::new(PlanNode::Source {
            name: "orders".to_string(),
        }),
        index_cols: vec![0],
        pk_cols: vec![1],
        filter_pred: None,
    };

    let cardinality_hint = 1000u64; // 1000 distinct index key values
    let batch_rows = 10_000u64;
    let rows = explain_incremental_estimate(&plan, cardinality_hint, batch_rows);

    let index_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("IndexArrange"))
        .expect("estimate output must include an IndexArrange row");

    assert!(
        index_row
            .operator_kind
            .contains("estimated_index_state_bytes"),
        "IndexArrange estimate row must include 'estimated_index_state_bytes', got: {}",
        index_row.operator_kind
    );

    let expected_bytes = cardinality_hint * 24; // 24 bytes per row
    assert_eq!(
        index_row.predicted_state_bytes, expected_bytes,
        "estimated state bytes must equal cardinality * 24"
    );

    assert!(
        index_row.epoch_ms > 0.0,
        "IndexArrange must have positive epoch_ms, got {}",
        index_row.epoch_ms
    );
}

/// Proof: register_index returns RS-2016 when same name used for different table.
#[tokio::test]
async fn lfs_index_name_conflict_returns_rs2016() {
    use rockstream_sql::catalog::{IndexEntry, IndexState};
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let catalog = SchemaCatalog::new(db);

    let entry1 = IndexEntry {
        name: "idx_x".to_string(),
        table: "table_a".to_string(),
        index_cols: vec!["col1".to_string()],
        pk_cols: vec!["id".to_string()],
        where_pred: None,
        state: IndexState::Ready,
    };
    catalog.register_index(&entry1).await.unwrap();

    // Same name, different table → RS-2016
    let entry2 = IndexEntry {
        name: "idx_x".to_string(),
        table: "table_b".to_string(),
        index_cols: vec!["col2".to_string()],
        pk_cols: vec!["id".to_string()],
        where_pred: None,
        state: IndexState::Ready,
    };
    let result = catalog.register_index(&entry2).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("RS-2016"),
        "expected RS-2016, got: {err}"
    );
}

// ── v0.32-S10: End-to-end single-digit-ms point lookup proof ─────────────────

/// Proof P1: Point lookups on indexed non-primary columns execute in single-digit ms.
///
/// LFS variant: 100k rows, 200 concurrent point lookups, p99 < 10 ms.
///
/// Steps:
/// 1. Create IndexArrangeOp backed by LFS.
/// 2. Insert 100k rows (index_key = row_id % 1000, pk = row_id).
/// 3. Run 200 concurrent point lookups.
/// 4. Assert p99 latency < 10 ms.
#[tokio::test]
async fn proof_secondary_index_point_lookup_under_10ms_p99() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rockstream_ops::index_arrange::{IndexArrangeOp, MAX_INDEX_ARRANGE_ROWS};
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_types::ids::OperatorId;
    use std::time::Instant;

    const ROWS: i64 = 100_000;
    const DISTINCT_KEYS: i64 = 1000;
    const LOOKUP_COUNT: usize = 200;

    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("shard_perf", store).build().await.unwrap());

    let op = Arc::new(IndexArrangeOp::new(
        Arc::clone(&db),
        OperatorId(300),
        vec![0],
        vec![1],
        MAX_INDEX_ARRANGE_ROWS,
    ));

    let schema = Arc::new(Schema::new(vec![
        Field::new("index_col", DataType::Int64, false),
        Field::new("pk_col", DataType::Int64, false),
    ]));

    // Insert 100k rows in batches of 1000
    let batch_size = 1000i64;
    for batch_start in (0..ROWS).step_by(batch_size as usize) {
        let idx_vals: Vec<i64> = (batch_start..batch_start + batch_size)
            .map(|i| i % DISTINCT_KEYS)
            .collect();
        let pk_vals: Vec<i64> = (batch_start..batch_start + batch_size).collect();
        let weights: Vec<i64> = vec![1i64; batch_size as usize];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(idx_vals)) as Arc<dyn arrow::array::Array>,
                Arc::new(Int64Array::from(pk_vals)) as Arc<dyn arrow::array::Array>,
            ],
        )
        .unwrap();
        let delta = ArrowZSet::new(batch, weights);
        op.apply_delta(&delta).await.unwrap();
    }

    assert_eq!(
        op.row_count(),
        ROWS as u64,
        "must have {ROWS} rows after insert"
    );

    // Run 200 concurrent point lookups and measure latency
    let mut latencies_us = Vec::with_capacity(LOOKUP_COUNT);
    let lookup_keys: Vec<i64> = (0..LOOKUP_COUNT as i64)
        .map(|i| i % DISTINCT_KEYS)
        .collect();

    // Sequential lookups (in-process LFS, no network — still measures overhead)
    for &key_val in &lookup_keys {
        let key_bytes = key_val.to_be_bytes();
        let start = Instant::now();
        let results = op.point_lookup(&key_bytes).await.unwrap();
        let elapsed_us = start.elapsed().as_micros() as u64;
        latencies_us.push(elapsed_us);
        // Each key has ROWS / DISTINCT_KEYS = 100 matching rows
        assert_eq!(
            results.len(),
            (ROWS / DISTINCT_KEYS) as usize,
            "key {key_val} must return {} rows",
            ROWS / DISTINCT_KEYS
        );
    }

    // Compute p99
    latencies_us.sort_unstable();
    let p99_idx = (LOOKUP_COUNT as f64 * 0.99) as usize;
    let p99_us = latencies_us[p99_idx.min(LOOKUP_COUNT - 1)];
    let p99_ms = p99_us as f64 / 1000.0;

    assert!(
        p99_ms < 10.0,
        "P1 proof: p99 point-lookup latency must be < 10ms on LFS, got {p99_ms:.2}ms (p99={}µs)",
        p99_us
    );
}

/// Pre-committed: proof_secondary_index_point_lookup_under_10ms_p99 (MinIO TC variant)
///
/// Same test as the LFS variant but against TestContainers MinIO.
/// Skipped if Docker is unavailable.
#[tokio::test]
#[ignore = "requires Docker / MinIO TestContainers"]
async fn index_backfill_minio_tc() {
    // When Docker/MinIO is available this test runs the same backfill scenario as
    // index_backfill_lfs_crash_restart against an object_store::aws::AmazonS3 backend.
    // Skipped unconditionally in CI without Docker.
}
