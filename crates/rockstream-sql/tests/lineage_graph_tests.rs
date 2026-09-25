//! Lineage graph tests (v0.71 V071-10, Slice 10).
//!
//! Validates bounded lineage DAG traversal in SchemaCatalog with:
//! - Diamond dependency resolution
//! - Bounded depth traversal (MAX_LINEAGE_DEPTH = 16)
//! - Bounded node count (MAX_LINEAGE_NODES = 256)
//! - Persistence across ShardDb restarts

use object_store::local::LocalFileSystem;
use rockstream_plan::PlanNode;
use rockstream_sql::{
    catalog::{MAX_LINEAGE_DEPTH, MAX_LINEAGE_NODES},
    ColumnDef, SchemaCatalog,
};
use rockstream_storage::ShardDb;
use std::sync::Arc;
use tempfile::TempDir;

async fn open_catalog(dir: &TempDir) -> SchemaCatalog {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("catalog", store).build().await.unwrap());
    SchemaCatalog::new(db)
}

fn sample_col() -> Vec<ColumnDef> {
    vec![ColumnDef {
        name: "id".to_string(),
        data_type: "Int64".to_string(),
        nullable: false,
    }]
}

#[tokio::test]
async fn test_catalog_lineage_dag_traversal_and_bounds() {
    let temp = TempDir::new().unwrap();
    let catalog = open_catalog(&temp).await;

    // Diamond graph:
    //      v_sink
    //      /    \
    //   v_left  v_right
    //      \    /
    //     v_source
    let v_source_plan = PlanNode::Source {
        name: "raw_events".to_string(),
    };
    catalog
        .register_view(
            "v_source",
            "SELECT id FROM raw_events",
            &v_source_plan,
            sample_col(),
        )
        .await
        .unwrap();

    let v_left_plan = PlanNode::ViewRef {
        view_name: "v_source".to_string(),
    };
    catalog
        .register_view(
            "v_left",
            "SELECT id FROM v_source",
            &v_left_plan,
            sample_col(),
        )
        .await
        .unwrap();

    let v_right_plan = PlanNode::ViewRef {
        view_name: "v_source".to_string(),
    };
    catalog
        .register_view(
            "v_right",
            "SELECT id FROM v_source",
            &v_right_plan,
            sample_col(),
        )
        .await
        .unwrap();

    let v_sink_plan = PlanNode::Union {
        left: Box::new(PlanNode::ViewRef {
            view_name: "v_left".to_string(),
        }),
        right: Box::new(PlanNode::ViewRef {
            view_name: "v_right".to_string(),
        }),
    };
    catalog
        .register_view(
            "v_sink",
            "SELECT id FROM v_left UNION ALL SELECT id FROM v_right",
            &v_sink_plan,
            sample_col(),
        )
        .await
        .unwrap();

    let lineage = catalog.get_lineage("v_sink").await.unwrap();
    assert_eq!(lineage.root, "v_sink");
    assert!(!lineage.truncated);
    assert_eq!(lineage.depth, 2);

    let nodes = lineage.nodes;
    assert!(nodes.contains(&"v_sink".to_string()));
    assert!(nodes.contains(&"v_left".to_string()));
    assert!(nodes.contains(&"v_right".to_string()));
    assert!(nodes.contains(&"v_source".to_string()));

    let edges = lineage.edges;
    assert!(edges
        .iter()
        .any(|e| e.from == "v_sink" && e.to == "v_left" && e.depth == 1));
    assert!(edges
        .iter()
        .any(|e| e.from == "v_sink" && e.to == "v_right" && e.depth == 1));
    assert!(edges
        .iter()
        .any(|e| e.from == "v_left" && e.to == "v_source" && e.depth == 2));
    assert!(edges
        .iter()
        .any(|e| e.from == "v_right" && e.to == "v_source" && e.depth == 2));
}

#[tokio::test]
async fn test_lineage_graph_depth_truncation() {
    let temp = TempDir::new().unwrap();
    let catalog = open_catalog(&temp).await;

    // Chain of 25 views (exceeds MAX_LINEAGE_DEPTH = 16)
    let mut prev = "source_0".to_string();
    for i in 0..25 {
        let name = format!("chain_{i}");
        let plan = if i == 0 {
            PlanNode::Source { name: prev.clone() }
        } else {
            PlanNode::ViewRef {
                view_name: prev.clone(),
            }
        };
        catalog
            .register_view(&name, "SELECT id FROM src", &plan, sample_col())
            .await
            .unwrap();
        prev = name;
    }

    let lineage = catalog.get_lineage("chain_24").await.unwrap();
    assert!(lineage.depth <= MAX_LINEAGE_DEPTH);
    assert!(lineage.truncated);
}

#[tokio::test]
async fn test_lineage_survives_restart() {
    let temp = TempDir::new().unwrap();
    {
        let catalog = open_catalog(&temp).await;
        let v1_plan = PlanNode::Source {
            name: "orders".to_string(),
        };
        catalog
            .register_view("v_orders", "SELECT id FROM orders", &v1_plan, sample_col())
            .await
            .unwrap();

        let v2_plan = PlanNode::ViewRef {
            view_name: "v_orders".to_string(),
        };
        catalog
            .register_view(
                "v_summary",
                "SELECT id FROM v_orders",
                &v2_plan,
                sample_col(),
            )
            .await
            .unwrap();
    }

    // Reopen catalog after restart
    let reopened = open_catalog(&temp).await;
    let lineage = reopened.get_lineage("v_summary").await.unwrap();
    assert_eq!(lineage.root, "v_summary");
    assert_eq!(lineage.depth, 1);
    assert!(lineage.nodes.contains(&"v_summary".to_string()));
    assert!(lineage.nodes.contains(&"v_orders".to_string()));
    assert!(!lineage.truncated);
}

#[test]
fn test_lineage_graph_nodes_limit_constant() {
    assert_eq!(MAX_LINEAGE_NODES, 256);
}
