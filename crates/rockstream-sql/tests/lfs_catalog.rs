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

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_sql::{
    catalog::{ColumnDef, SchemaCatalog},
    SqlError,
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
