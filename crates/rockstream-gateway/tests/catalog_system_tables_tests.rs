//! PGWire SQL tests for `rockstream_catalog` virtual system tables (v0.59.10 CAT-01).

use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::{
        CatalogCheckpointEntry, CatalogNamespaceEntry, CatalogNodeEntry, CatalogOperationEntry,
        CatalogShardEntry, CatalogSourceEntry, CatalogStubs, CatalogTableEntry, CatalogView,
        CatalogWorkloadEntry, MAX_ARRANGEMENTS_SCAN_ROWS, MAX_CAPABILITIES_SCAN_ROWS,
        MAX_CATALOG_TABLES_SCAN_ROWS, MAX_CATALOG_WORKLOADS_SCAN_ROWS, MAX_CHECKPOINTS_SCAN_ROWS,
        MAX_NAMESPACES_SCAN_ROWS, MAX_NODES_SCAN_ROWS, MAX_OPERATIONS_SCAN_ROWS,
        MAX_OPERATORS_SCAN_ROWS, MAX_SHARDS_SCAN_ROWS, MAX_SOURCES_SCAN_ROWS, MAX_VIEWS_SCAN_ROWS,
    },
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway(catalog: CatalogStubs) -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .expect("query failed")
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(str::to_string));
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_catalog_nodes_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_node(CatalogNodeEntry {
        node_id: "node-1".to_string(),
        worker_id: "worker-1".to_string(),
        role: "worker".to_string(),
        address: "10.0.0.1:9090".to_string(),
        state: "READY".to_string(),
        lease_count: 4,
        memory_budget_bytes: 1024 * 1024 * 1024,
        last_heartbeat_at: "2026-08-24 10:00:00+00".to_string(),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.nodes;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 8);
    assert_eq!(row[0].as_deref(), Some("node-1"));
    assert_eq!(row[1].as_deref(), Some("worker-1"));
    assert_eq!(row[2].as_deref(), Some("worker"));
    assert_eq!(row[3].as_deref(), Some("10.0.0.1:9090"));
    assert_eq!(row[4].as_deref(), Some("READY"));
    assert_eq!(row[5].as_deref(), Some("4"));
    assert_eq!(row[6].as_deref(), Some("1073741824"));
    assert_eq!(row[7].as_deref(), Some("2026-08-24 10:00:00+00"));
}

#[tokio::test]
async fn test_catalog_sources_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_source(CatalogSourceEntry {
        name: "events_src".to_string(),
        table_name: None,
        source_type: "kafka".to_string(),
        options: HashMap::new(),
        format: "json".to_string(),
        status: "RUNNING".to_string(),
        live_offset: "1048576".to_string(),
        live_lag: 42,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.sources;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 8);
    assert_eq!(row[0].as_deref(), Some("events_src"));
    assert_eq!(row[1].as_deref(), Some("kafka"));
    assert_eq!(row[2].as_deref(), Some("json"));
    assert_eq!(row[3].as_deref(), Some("RUNNING"));
    assert_eq!(row[4].as_deref(), Some("1048576"));
    assert_eq!(row[5].as_deref(), Some("42"));
    assert_eq!(row[6].as_deref(), Some("0")); // buffer_fill
    assert_eq!(row[7].as_deref(), Some("0")); // schema_version
}

#[tokio::test]
async fn test_catalog_views_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT user_id FROM users WHERE active = true".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(101),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.views;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 8);
    assert_eq!(row[0].as_deref(), Some("public"));
    assert_eq!(row[1].as_deref(), Some("active_users"));
    assert_eq!(row[2].as_deref(), Some("Running"));
    assert_eq!(row[3].as_deref(), Some("default"));
    assert_eq!(row[4].as_deref(), Some("DEFAULT"));
    assert!(row[5].is_some(), "arrangement_id must be populated");
    assert_eq!(row[6].as_deref(), Some("0")); // shared_state_bytes
    assert_eq!(row[7].as_deref(), Some("[1]")); // frontier
}

#[tokio::test]
async fn test_catalog_operators_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT user_id FROM users WHERE active = true".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(42),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.operators;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 6);
    assert_eq!(row[0].as_deref(), Some("op-000000000000002a"));
    assert_eq!(row[1].as_deref(), Some("active_users"));
    assert_eq!(row[2].as_deref(), Some("ViewSinkOp"));
    assert_eq!(row[3].as_deref(), Some("ML-001"));
    assert_eq!(row[4].as_deref(), Some("0")); // dirty_key_count
    assert_eq!(row[5].as_deref(), Some("0")); // logical_write_bytes
}

#[tokio::test]
async fn test_catalog_arrangements_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "v1".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.arrangements;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 6);
    assert!(row[0].is_some(), "arrangement_id must be present");
    assert_eq!(row[1].as_deref(), Some("1")); // consumer_count
    assert_eq!(row[2].as_deref(), Some("0")); // shared_state_bytes
    assert_eq!(row[3].as_deref(), Some("0")); // bytes_saved
    assert_eq!(row[4].as_deref(), Some("[1]")); // compaction_frontier
    assert_eq!(row[5].as_deref(), Some("hash(id)")); // partitioning
}

#[tokio::test]
async fn test_catalog_checkpoints_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 1001,
        committed_at: "2026-08-24 10:30:00+00".to_string(),
        epoch_number: 55,
        frontier: "[55]".to_string(),
        storage_path: "s3://prod-data/checkpoints/chk-1001".to_string(),
        duration_ms: 125,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.checkpoints;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 6);
    assert_eq!(row[0].as_deref(), Some("1001"));
    assert_eq!(row[1].as_deref(), Some("2026-08-24 10:30:00+00"));
    assert_eq!(row[2].as_deref(), Some("55"));
    assert_eq!(row[3].as_deref(), Some("[55]"));
    assert_eq!(
        row[4].as_deref(),
        Some("s3://prod-data/checkpoints/chk-1001")
    );
    assert_eq!(row[5].as_deref(), Some("125"));
}

#[tokio::test]
async fn test_catalog_capabilities_query() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(
        &client,
        "SELECT id, kind, name, tier FROM rockstream_catalog.capabilities WHERE tier = 'Core';",
    )
    .await;
    assert!(!rows.is_empty(), "Core capabilities must be returned");
    for r in &rows {
        assert_eq!(r.len(), 4);
        assert_eq!(r[3].as_deref(), Some("Core"));
    }
}

#[tokio::test]
async fn test_catalog_secrets_redacted() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    catalog.add_node(CatalogNodeEntry {
        node_id: "node-sec".to_string(),
        worker_id: "worker-sec".to_string(),
        role: "worker".to_string(),
        address: "https://user:supersecretpassword@10.0.0.5:9090".to_string(),
        state: "READY".to_string(),
        lease_count: 1,
        memory_budget_bytes: 1024,
        last_heartbeat_at: "2026-08-24 10:00:00+00".to_string(),
    });

    catalog.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 2002,
        committed_at: "2026-08-24 10:35:00+00".to_string(),
        epoch_number: 60,
        frontier: "[60]".to_string(),
        storage_path: "s3://access_key=AKIA123&secret_key=SECRET999@bucket/chk".to_string(),
        duration_ms: 50,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let node_rows = simple_rows(
        &client,
        "SELECT address FROM rockstream_catalog.nodes WHERE node_id = 'node-sec';",
    )
    .await;
    assert_eq!(node_rows.len(), 1);
    assert!(!node_rows[0][0]
        .as_ref()
        .unwrap()
        .contains("supersecretpassword"));
    assert!(node_rows[0][0].as_ref().unwrap().contains("***"));

    let chk_rows = simple_rows(
        &client,
        "SELECT storage_path FROM rockstream_catalog.checkpoints WHERE checkpoint_id = 2002;",
    )
    .await;
    assert_eq!(chk_rows.len(), 1);
    assert!(!chk_rows[0][0].as_ref().unwrap().contains("SECRET999"));
    assert!(chk_rows[0][0].as_ref().unwrap().contains("***"));
}

#[tokio::test]
async fn test_catalog_enforced_scan_bounds() {
    assert_eq!(MAX_NODES_SCAN_ROWS, 1000);
    assert_eq!(MAX_SOURCES_SCAN_ROWS, 1000);
    assert_eq!(MAX_VIEWS_SCAN_ROWS, 5000);
    assert_eq!(MAX_OPERATORS_SCAN_ROWS, 10000);
    assert_eq!(MAX_ARRANGEMENTS_SCAN_ROWS, 5000);
    assert_eq!(MAX_CHECKPOINTS_SCAN_ROWS, 1000);
    assert_eq!(MAX_CAPABILITIES_SCAN_ROWS, 1000);

    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    for i in 0..1100 {
        catalog.add_node(CatalogNodeEntry {
            node_id: format!("node-{i}"),
            worker_id: format!("worker-{i}"),
            role: "worker".to_string(),
            address: "127.0.0.1:8080".to_string(),
            state: "READY".to_string(),
            lease_count: 1,
            memory_budget_bytes: 1024,
            last_heartbeat_at: "2026-08-24 10:00:00+00".to_string(),
        });
    }

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SELECT * FROM rockstream_catalog.nodes;").await;
    assert_eq!(
        rows.len(),
        MAX_NODES_SCAN_ROWS,
        "Scan rows must be bounded at max 1000"
    );
}

#[tokio::test]
async fn test_canonical_rockstream_catalog_system_tables() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_shard(CatalogShardEntry {
        shard_id: 42,
        view_id: "analytics_orders".to_string(),
        worker_id: "worker-prod-1".to_string(),
        state: "ACTIVE".to_string(),
        frontier: 10500,
    });

    catalog.add_operation(CatalogOperationEntry {
        operation_id: "op-backfill-99".to_string(),
        kind: "backfill".to_string(),
        target: "analytics_orders".to_string(),
        phase: "STREAMING".to_string(),
        progress_pct: 85,
    });

    catalog.add_namespace(CatalogNamespaceEntry {
        name: "tenant_analytics".to_string(),
        owner: "admin_user".to_string(),
        created_at: "2026-09-01 12:00:00+00".to_string(),
    });

    catalog.add_catalog_table_entry(CatalogTableEntry {
        namespace: "tenant_analytics".to_string(),
        name: "events_raw".to_string(),
        schema: "event_id bigint, payload jsonb".to_string(),
        format: "arrow".to_string(),
        row_count: 500000,
    });

    catalog.add_catalog_workload_entry(CatalogWorkloadEntry {
        workload_id: "wl_heavy_etl".to_string(),
        name: "heavy_etl".to_string(),
        memory_limit: "16GiB".to_string(),
        priority: "High".to_string(),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Test rockstream_catalog.shards
    let shard_rows = simple_rows(
        &client,
        "SELECT shard_id, view_id, worker_id, state, frontier FROM rockstream_catalog.shards;",
    )
    .await;
    assert_eq!(shard_rows.len(), 1);
    assert_eq!(
        shard_rows[0],
        vec![
            Some("42".to_string()),
            Some("analytics_orders".to_string()),
            Some("worker-prod-1".to_string()),
            Some("ACTIVE".to_string()),
            Some("10500".to_string()),
        ]
    );

    // Test WHERE filtering on shards
    let shard_filter_rows = simple_rows(
        &client,
        "SELECT shard_id FROM rockstream_catalog.shards WHERE shard_id = 42;",
    )
    .await;
    assert_eq!(shard_filter_rows.len(), 1);
    assert_eq!(shard_filter_rows[0], vec![Some("42".to_string())]);

    // Test rockstream_catalog.operations
    let op_rows = simple_rows(
        &client,
        "SELECT operation_id, kind, target, phase, progress_pct FROM rockstream_catalog.operations;",
    )
    .await;
    assert_eq!(op_rows.len(), 1);
    assert_eq!(
        op_rows[0],
        vec![
            Some("op-backfill-99".to_string()),
            Some("backfill".to_string()),
            Some("analytics_orders".to_string()),
            Some("STREAMING".to_string()),
            Some("85".to_string()),
        ]
    );

    // Test rockstream_catalog.namespaces
    let ns_rows = simple_rows(
        &client,
        "SELECT name, owner, created_at FROM rockstream_catalog.namespaces WHERE name = 'tenant_analytics';",
    )
    .await;
    assert_eq!(ns_rows.len(), 1);
    assert_eq!(
        ns_rows[0],
        vec![
            Some("tenant_analytics".to_string()),
            Some("admin_user".to_string()),
            Some("2026-09-01 12:00:00+00".to_string()),
        ]
    );

    // Test rockstream_catalog.tables
    let tbl_rows = simple_rows(
        &client,
        "SELECT namespace, name, schema, format, row_count FROM rockstream_catalog.tables WHERE name = 'events_raw';",
    )
    .await;
    assert_eq!(tbl_rows.len(), 1);
    assert_eq!(
        tbl_rows[0],
        vec![
            Some("tenant_analytics".to_string()),
            Some("events_raw".to_string()),
            Some("event_id bigint, payload jsonb".to_string()),
            Some("arrow".to_string()),
            Some("500000".to_string()),
        ]
    );

    // Test rockstream_catalog.workloads
    let wl_rows = simple_rows(
        &client,
        "SELECT workload_id, name, memory_limit, priority FROM rockstream_catalog.workloads WHERE name = 'heavy_etl';",
    )
    .await;
    assert_eq!(wl_rows.len(), 1);
    assert_eq!(
        wl_rows[0],
        vec![
            Some("wl_heavy_etl".to_string()),
            Some("heavy_etl".to_string()),
            Some("16GiB".to_string()),
            Some("High".to_string()),
        ]
    );

    // Negative test: invalid table returns RS-0002
    let neg_err = client
        .simple_query("SELECT * FROM rockstream_catalog.nonexistent_catalog_table;")
        .await
        .unwrap_err();
    let msg = neg_err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-0002") || msg.contains("Configuration error"),
        "Expected RS-0002 error, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_canonical_catalog_scan_bounds() {
    assert_eq!(MAX_SHARDS_SCAN_ROWS, 4096);
    assert_eq!(MAX_OPERATIONS_SCAN_ROWS, 1000);
    assert_eq!(MAX_NAMESPACES_SCAN_ROWS, 256);
    assert_eq!(MAX_CATALOG_TABLES_SCAN_ROWS, 1000);
    assert_eq!(MAX_CATALOG_WORKLOADS_SCAN_ROWS, 256);
}
