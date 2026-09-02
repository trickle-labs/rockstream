//! PGWire integration tests for `EXPLAIN INCREMENTAL ESTIMATE` (v0.59.23 Slice 3).

use std::sync::{Arc, LazyLock};
use tokio_postgres::NoTls;

use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::ViewReadStrategy;
use rockstream_gateway::GatewayError;
use rockstream_types::explain::ArrangementSharingInfo;

static PGWIRE_ESTIMATE_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

struct DummyViewReader;

#[async_trait::async_trait]
impl rockstream_gateway::view_reader::ViewReader for DummyViewReader {
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
async fn mixed_case_estimate_returns_exact_report() {
    let _guard = PGWIRE_ESTIMATE_TEST_LOCK.lock().await;

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(rockstream_gateway::catalog_stubs::CatalogTable {
        name: "orders".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "amount".to_string(),
                data_type: "Float64".to_string(),
            },
        ],
    });
    catalog.add_view(CatalogView {
        name: "orders_view".to_string(),
        sql: "SELECT id, SUM(amount) FROM orders GROUP BY id".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "sum".to_string(),
                data_type: "Float64".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    catalog.set_view_arrangement_sharing(
        "orders_view",
        ArrangementSharingInfo::new(
            Some(rockstream_types::ids::ArrangementId(1)),
            2,
            24_000,
            24_000,
            100,
        ),
    );

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // 1. UPPERCASE command
    let msgs_upper = client
        .simple_query("EXPLAIN INCREMENTAL ESTIMATE orders_view")
        .await
        .expect("EXPLAIN INCREMENTAL ESTIMATE uppercase failed");
    let plan_upper = data_rows_from(&msgs_upper)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_upper.contains("EXPLAIN INCREMENTAL ESTIMATE"));
    assert!(plan_upper.contains("Selected Strategy"));
    assert!(plan_upper.contains("Arrangements"));
    assert!(plan_upper.contains("state_bytes"));
    assert!(plan_upper.contains("epoch_ms"));

    // 2. lowercase command
    let msgs_lower = client
        .simple_query("explain incremental estimate orders_view")
        .await
        .expect("explain incremental estimate lowercase failed");
    let plan_lower = data_rows_from(&msgs_lower)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        plan_upper, plan_lower,
        "uppercase and lowercase estimate reports must match exactly"
    );

    // 3. mixed-case command
    let msgs_mixed = client
        .simple_query("EXPLAIN incremental Estimate orders_view")
        .await
        .expect("EXPLAIN incremental Estimate mixed-case failed");
    let plan_mixed = data_rows_from(&msgs_mixed)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        plan_upper, plan_mixed,
        "mixed-case estimate report must match exactly"
    );
}

#[tokio::test]
async fn negative_test_invalid_sql_returns_actionable_error() {
    let _guard = PGWIRE_ESTIMATE_TEST_LOCK.lock().await;

    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let err = client
        .simple_query("EXPLAIN INCREMENTAL ESTIMATE SELECT INVALID SYNTAX FROM")
        .await
        .expect_err("invalid SQL query must return error, never silent ok");

    let msg = err
        .as_db_error()
        .map(|d| d.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("RS-1012")
            || msg.contains("RS-3031")
            || msg.contains("SQL parse error")
            || msg.contains("syntax"),
        "error message must contain actionable error code/message: {msg}"
    );
}
