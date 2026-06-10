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
