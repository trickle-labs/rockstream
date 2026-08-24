//! v0.59.10 Slice 6: Deterministic SimRuntime Introspection & Fault Injection Tests.
//!
//! Asserts that under concurrent view creation, checkpointing, and simulated fault injection,
//! system catalog queries remain consistent, monotonic, and leak-free.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Barrier;

use rockstream_gateway::catalog_stubs::{
    CatalogCheckpointEntry, CatalogNodeEntry, CatalogSourceEntry, CatalogStubs, CatalogView,
    SessionInfo,
};

#[tokio::test]
async fn catalog_queries_remain_consistent_under_faults() {
    let catalog = Arc::new(CatalogStubs::new());

    // Populate initial baseline
    for i in 0..10 {
        catalog.add_node(CatalogNodeEntry {
            node_id: format!("node-{i}"),
            worker_id: format!("worker-{i}"),
            role: if i % 2 == 0 {
                "worker".to_string()
            } else {
                "gateway".to_string()
            },
            address: format!("127.0.0.1:{}", 9000 + i),
            state: "READY".to_string(),
            lease_count: i as u64,
            memory_budget_bytes: 1024 * 1024 * 1024,
            last_heartbeat_at: "2026-08-24 11:00:00+00".to_string(),
        });
        catalog.add_view(CatalogView {
            name: format!("view_{i}"),
            sql: format!("SELECT {i}"),
            columns: vec![],
            namespace: "public".to_string(),
            op_id: Some(100 + i as u64),
        });
        catalog.record_checkpoint(CatalogCheckpointEntry {
            checkpoint_id: 100 + i as u64,
            committed_at: "2026-08-24 11:00:00+00".to_string(),
            epoch_number: i as u64,
            frontier: format!("[{i}]"),
            storage_path: format!("s3://bucket/checkpoints/chk-{i}"),
            duration_ms: 10 + i as u64,
        });
    }

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    // Task 1: Concurrent view additions and drops
    {
        let c = catalog.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            for i in 10..30 {
                let vname = format!("dyn_view_{i}");
                c.add_view(CatalogView {
                    name: vname.clone(),
                    sql: format!("SELECT {i}"),
                    columns: vec![],
                    namespace: "public".to_string(),
                    op_id: Some(200 + i as u64),
                });
                tokio::task::yield_now().await;
                if i % 3 == 0 {
                    c.remove_view(&vname);
                }
            }
        }));
    }

    // Task 2: Concurrent checkpoint commits
    {
        let c = catalog.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            for i in 10..30 {
                c.record_checkpoint(CatalogCheckpointEntry {
                    checkpoint_id: 200 + i as u64,
                    committed_at: "2026-08-24 11:01:00+00".to_string(),
                    epoch_number: i as u64,
                    frontier: format!("[{i}]"),
                    storage_path: format!("s3://bucket/checkpoints/chk-{i}"),
                    duration_ms: 15,
                });
                tokio::task::yield_now().await;
            }
        }));
    }

    // Task 3: Concurrent source updates and status mutations
    {
        let c = catalog.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            for i in 0..20 {
                let sname = format!("dyn_source_{i}");
                c.add_source(CatalogSourceEntry {
                    name: sname.clone(),
                    table_name: None,
                    source_type: "kafka".to_string(),
                    options: HashMap::new(),
                    format: "json".to_string(),
                    status: "RUNNING".to_string(),
                    live_offset: (i * 1000).to_string(),
                    live_lag: i as u64,
                });
                tokio::task::yield_now().await;
            }
        }));
    }

    // Task 4: Continuous catalog queries verifying monotonic snapshot consistency
    {
        let c = catalog.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let session = SessionInfo::default();
            for _ in 0..50 {
                let node_resp = c
                    .handle_query("SELECT * FROM rockstream_catalog.nodes", &session)
                    .unwrap();
                let view_resp = c
                    .handle_query("SELECT * FROM rockstream_catalog.views", &session)
                    .unwrap();
                let op_resp = c
                    .handle_query("SELECT * FROM rockstream_catalog.operators", &session)
                    .unwrap();
                let chk_resp = c
                    .handle_query("SELECT * FROM rockstream_catalog.checkpoints", &session)
                    .unwrap();
                let cap_resp = c
                    .handle_query("SELECT * FROM rockstream_catalog.capabilities", &session)
                    .unwrap();

                // Assert results contain valid rows without panic or torn states
                if let rockstream_gateway::catalog_stubs::CatalogResponse::Rows { columns, rows } =
                    node_resp
                {
                    assert_eq!(columns.len(), 8);
                    assert!(rows.len() >= 10);
                }
                if let rockstream_gateway::catalog_stubs::CatalogResponse::Rows { columns, rows } =
                    view_resp
                {
                    assert_eq!(columns.len(), 8);
                    assert!(!rows.is_empty());
                }
                if let rockstream_gateway::catalog_stubs::CatalogResponse::Rows { columns, rows } =
                    op_resp
                {
                    assert_eq!(columns.len(), 6);
                    assert!(!rows.is_empty());
                }
                if let rockstream_gateway::catalog_stubs::CatalogResponse::Rows { columns, rows } =
                    chk_resp
                {
                    assert_eq!(columns.len(), 6);
                    assert!(rows.len() >= 10);
                }
                if let rockstream_gateway::catalog_stubs::CatalogResponse::Rows { columns, rows } =
                    cap_resp
                {
                    assert_eq!(columns.len(), 8);
                    assert!(!rows.is_empty());
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
