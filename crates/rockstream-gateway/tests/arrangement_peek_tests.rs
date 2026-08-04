//! Tests for read-only `SHOW ARRANGEMENT` intermediate Z-set state inspector (v0.51.11).

use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::GatewayError;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct DummyArrangementViewReader;

#[async_trait::async_trait]
impl ViewReader for DummyArrangementViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        Some(100)
    }

    async fn peek_arrangement(
        &self,
        _view_name: &str,
        op_id: u64,
        key: &str,
    ) -> Result<Option<(i64, u64)>, GatewayError> {
        if op_id == 1 && key == "42" {
            Ok(Some((10, 100)))
        } else if op_id == 2 && key == "left_key" {
            Ok(Some((5, 100)))
        } else {
            Ok(None)
        }
    }
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=rockstream"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

fn data_rows_from(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut r = Vec::new();
                for i in 0..row.len() {
                    r.push(row.get(i).map(|s| s.to_string()));
                }
                Some(r)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_show_arrangement_aggregate_key() {
    let _guard = TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "agg_view".to_string(),
        sql: "SELECT k, SUM(v) FROM base GROUP BY k".to_string(),
        columns: vec![
            CatalogColumn {
                name: "k".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "sum".to_string(),
                data_type: "Int64".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: Some(1),
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyArrangementViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("SHOW ARRANGEMENT agg_view 1 42")
        .await
        .expect("SHOW ARRANGEMENT failed");
    let rows = data_rows_from(&msgs);

    assert_eq!(rows.len(), 1, "expected 1 row for existing arrangement key");
    assert_eq!(rows[0].first().and_then(|o| o.as_deref()), Some("agg_view"));
    assert_eq!(rows[0].get(1).and_then(|o| o.as_deref()), Some("1"));
    assert_eq!(rows[0].get(2).and_then(|o| o.as_deref()), Some("42"));
    assert_eq!(rows[0].get(3).and_then(|o| o.as_deref()), Some("10"));
}

#[tokio::test]
async fn test_show_arrangement_join_side_key() {
    let _guard = TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "join_view".to_string(),
        sql: "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: Some(2),
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyArrangementViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("SHOW ARRANGEMENT join_view 2 left_key")
        .await
        .expect("SHOW ARRANGEMENT failed");
    let rows = data_rows_from(&msgs);

    assert_eq!(rows.len(), 1, "expected 1 row for join key");
    assert_eq!(rows[0].get(3).and_then(|o| o.as_deref()), Some("5"));
}

#[tokio::test]
async fn test_show_arrangement_nonexistent_key() {
    let _guard = TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "agg_view".to_string(),
        sql: "SELECT k, SUM(v) FROM base GROUP BY k".to_string(),
        columns: vec![CatalogColumn {
            name: "k".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: Some(1),
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyArrangementViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("SHOW ARRANGEMENT agg_view 1 999999")
        .await
        .expect("SHOW ARRANGEMENT failed");
    let rows = data_rows_from(&msgs);

    assert_eq!(
        rows.len(),
        0,
        "nonexistent key should return 0 rows without error"
    );
}

#[tokio::test]
async fn test_show_arrangement_negative_nonexistent_view() {
    let _guard = TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyArrangementViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let err = client
        .simple_query("SHOW ARRANGEMENT non_existent_view 1 42")
        .await
        .expect_err("should fail for nonexistent view");

    let err_str = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |db_err| db_err.message().to_string());
    assert!(
        err_str.contains("RS-1001") || err_str.contains("RS-2001"),
        "error must carry RS-1001 code, got: {err_str}"
    );
    assert!(
        err_str.contains("Next steps:"),
        "error must carry actionable next steps, got: {err_str}"
    );
}
