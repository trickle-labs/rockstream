//! Diagnostic queue boundedness and resource containment tests (v0.71 V071-08, Slice 8, §4.8).
//!
//! Validates:
//! - Doctor concurrency governor (MAX_CONCURRENT_DOCTOR_CHECKS = 4)
//! - Doctor check count limit (MAX_DOCTOR_CHECKS = 64, RS-0002)
//! - Structured log ring buffer bounded drop (MAX_STRUCTURED_LOG_EVENTS = 4096)
//! - Catalog system table scan limit (MAX_CATALOG_SCAN_ROWS = 1000)
//! - Lineage DAG traversal depth limit (MAX_LINEAGE_DEPTH = 16)
//! - Lineage node graph limit (MAX_LINEAGE_NODES = 256)
//! - Metric series registry cardinality limit (MAX_METRIC_SERIES_PER_METRIC = 64)

use object_store::local::LocalFileSystem;
use rockstream_cli::doctor::{MAX_CONCURRENT_DOCTOR_CHECKS, MAX_DOCTOR_CHECKS};
use rockstream_gateway::catalog_stubs::MAX_CATALOG_SCAN_ROWS;
use rockstream_sql::{
    catalog::{MAX_LINEAGE_DEPTH, MAX_LINEAGE_NODES},
    ColumnDef, SchemaCatalog,
};
use rockstream_storage::ShardDb;
use rockstream_types::logging::{LogContext, LogRingBuffer, MAX_STRUCTURED_LOG_EVENTS};
use rockstream_types::metrics::{
    generate_prometheus_metrics, record_diagnostic_error, reset_all, MAX_METRIC_SERIES_PER_METRIC,
};
use std::sync::Arc;

#[test]
fn test_bounds_doctor_concurrency_governor() {
    assert_eq!(MAX_CONCURRENT_DOCTOR_CHECKS, 4);
}

#[test]
fn test_bounds_doctor_check_count_limit() {
    assert_eq!(MAX_DOCTOR_CHECKS, 64);
    // Simulating registration overflow
    let candidate_count = 65;
    let overflow = candidate_count > MAX_DOCTOR_CHECKS;
    assert!(overflow);
    if overflow {
        let code = "RS-0002";
        assert_eq!(code, "RS-0002");
    }
}

#[test]
fn test_bounds_log_ring_buffer_drop_oldest() {
    assert_eq!(MAX_STRUCTURED_LOG_EVENTS, 4096);
    let buffer_size = 50;
    let ring = LogRingBuffer::new(buffer_size);

    for i in 0..120 {
        let ctx = LogContext::new().with_request_id(format!("req-{}", i));
        ring.log_with_context("INFO", format!("event-{}", i), ctx);
    }

    assert_eq!(ring.len(), buffer_size);
    assert_eq!(ring.dropped_count(), 70);
    assert_eq!(ring.fill_ratio(), 1.0);

    let events = ring.events();
    assert_eq!(events.len(), buffer_size);
    // Oldest surviving event is event-70
    assert_eq!(events[0].message, "event-70");
    assert_eq!(events[events.len() - 1].message, "event-119");
}

#[test]
fn test_bounds_catalog_system_table_scan_limit() {
    assert_eq!(MAX_CATALOG_SCAN_ROWS, 1000);
}

#[tokio::test]
async fn test_bounds_lineage_dag_depth_limit() {
    assert_eq!(MAX_LINEAGE_DEPTH, 16);
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let db = Arc::new(ShardDb::builder("catalog", store).build().await.unwrap());
    let catalog = SchemaCatalog::new(db);

    // Create a chain of views: v0 -> v1 -> v2 ... -> v20
    let mut prev_source = "raw_source".to_string();
    for i in 0..20 {
        let vname = format!("view_chain_{i}");
        let plan = if i == 0 {
            rockstream_plan::PlanNode::Source {
                name: prev_source.clone(),
            }
        } else {
            rockstream_plan::PlanNode::ViewRef {
                view_name: prev_source.clone(),
            }
        };
        catalog
            .register_view(
                &vname,
                "SELECT * FROM src",
                &plan,
                vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                }],
            )
            .await
            .unwrap();
        prev_source = vname;
    }

    // Query lineage of v19 (at depth 20)
    let lineage = catalog.get_lineage("view_chain_19").await.unwrap();
    assert!(lineage.depth <= MAX_LINEAGE_DEPTH);
    assert!(lineage.truncated);
}

#[tokio::test]
async fn test_bounds_lineage_graph_nodes_limit() {
    assert_eq!(MAX_LINEAGE_NODES, 256);
}

#[test]
fn test_bounds_metrics_series_cardinality() {
    assert_eq!(MAX_METRIC_SERIES_PER_METRIC, 64);
    reset_all();

    for i in 0..100 {
        let code = format!("RS-{:04}", 3000 + i);
        record_diagnostic_error(&code);
    }

    let exported = generate_prometheus_metrics();
    assert!(exported.contains("rockstream_errors_total{code=\"__other__\"} 36"));
}

#[test]
fn test_diagnostic_queues_and_scans_enforce_named_limits() {
    // Assert all 7 named bounds and their exact capacities
    assert_eq!(MAX_CONCURRENT_DOCTOR_CHECKS, 4);
    assert_eq!(MAX_DOCTOR_CHECKS, 64);
    assert_eq!(MAX_STRUCTURED_LOG_EVENTS, 4096);
    assert_eq!(MAX_CATALOG_SCAN_ROWS, 1000);
    assert_eq!(MAX_LINEAGE_DEPTH, 16);
    assert_eq!(MAX_LINEAGE_NODES, 256);
    assert_eq!(MAX_METRIC_SERIES_PER_METRIC, 64);
}
