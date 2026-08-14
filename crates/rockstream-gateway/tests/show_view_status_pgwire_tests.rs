//! PGWire SQL tests for `SHOW VIEW STATUS` and `SHOW RESOURCE USAGE` (v0.53 Slice 6).

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::workload::{FreshnessSlo, MemoryLimit, WorkloadDef};

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
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_pgwire_show_view_status_all_views() {
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics")
        .with_freshness_slo(FreshnessSlo::new(5000))
        .with_memory_limit(MemoryLimit::new(536870912));
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT id, count(*) FROM users GROUP BY id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    catalog.add_view(CatalogView {
        name: "hourly_revenue".to_string(),
        sql: "SELECT hour, sum(amount) FROM orders GROUP BY hour".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("public"));
    assert_eq!(rows[0][1].as_deref(), Some("active_users"));
    assert_eq!(rows[0][2].as_deref(), Some("RUNNING"));
    assert_eq!(rows[0][3].as_deref(), Some("analytics"));
    assert_eq!(rows[0][4].as_deref(), Some("5000"));
    assert_eq!(rows[0][5].as_deref(), Some("536870912"));

    assert_eq!(rows[1][1].as_deref(), Some("hourly_revenue"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_single_view() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR active_users;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_deref(), Some("active_users"));
    assert_eq!(rows[0][2].as_deref(), Some("RUNNING"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_namespace() {
    let catalog = CatalogStubs::new();
    catalog.add_view_in_namespace(CatalogView {
        name: "prod_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "production".to_string(),
        op_id: None,
    });
    catalog.add_view_in_namespace(CatalogView {
        name: "dev_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "development".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR NAMESPACE production;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("production"));
    assert_eq!(rows[0][1].as_deref(), Some("prod_view"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_pgwire_show_view_status_not_found_rs1001() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW VIEW STATUS FOR nonexistent_view;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1001"), "expected RS-1001 in error: {msg}");
    assert!(msg.contains("nonexistent_view"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_namespace_not_found() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW VIEW STATUS FOR NAMESPACE nonexistent_ns;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1001"), "expected RS-1001 in error: {msg}");
    assert!(msg.contains("nonexistent_ns"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("active_users"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_for_workload() {
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics");
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE FOR WORKLOAD analytics;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("analytics"));
}

#[tokio::test]
async fn test_pgwire_show_cluster_resource_usage() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("cluster"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_workload_not_found() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW RESOURCE USAGE FOR WORKLOAD nonexistent_wl;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1005"), "expected RS-1005 in error: {msg}");
}

#[tokio::test]
async fn test_pgwire_show_cluster_resource_usage_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_pgwire_show_view_status_and_resource_usage() {
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics")
        .with_freshness_slo(FreshnessSlo::new(5000))
        .with_memory_limit(MemoryLimit::new(536870912));
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT id, count(*) FROM users GROUP BY id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let status_rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(status_rows.len(), 1);

    let res_rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(res_rows.len(), 1);

    let cluster_rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(cluster_rows.len(), 1);
}
