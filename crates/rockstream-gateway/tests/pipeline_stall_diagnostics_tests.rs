//! Tests for pipeline stall diagnostics engine and PGWire surface (v0.51.11).

use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::ViewReadStrategy;
use rockstream_gateway::GatewayError;
use rockstream_types::ids::OperatorId;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
async fn test_pipeline_stall_localization_pgwire() {
    let _guard = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();

    // Record operator frontiers: op 1 at epoch 100, op 2 (wedged) at epoch 80
    rockstream_types::metrics::record_operator_frontier(
        "stalled_view",
        OperatorId(1),
        0,
        100,
        true,
    );
    rockstream_types::metrics::record_operator_frontier(
        "stalled_view",
        OperatorId(2),
        0,
        80,
        false,
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "stalled_view".to_string(),
        sql: "SELECT id FROM base".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("SHOW PIPELINE STALLS FOR stalled_view")
        .await
        .expect("SHOW PIPELINE STALLS failed");
    let rows = data_rows_from(&msgs);

    assert_eq!(rows.len(), 2, "expected 2 operator rows in stall report");

    // Find row for op_id 2
    let op2_row = rows
        .iter()
        .find(|r| r.get(1).and_then(|o| o.as_deref()) == Some("2"))
        .expect("op 2 row missing");

    // op 2 at epoch 80 is holding back commit
    assert_eq!(
        op2_row.get(5).and_then(|o| o.as_deref()),
        Some("true"),
        "is_holding_back_commit should be true for op 2"
    );
}

#[tokio::test]
async fn test_pipeline_slow_source_localization_pgwire() {
    let _guard = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();

    // Source op 1 at epoch 70 (slowest source), op 2 at epoch 100
    rockstream_types::metrics::record_operator_frontier(
        "slow_src_view",
        OperatorId(1),
        0,
        70,
        true,
    );
    rockstream_types::metrics::record_operator_frontier(
        "slow_src_view",
        OperatorId(2),
        0,
        100,
        false,
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "slow_src_view".to_string(),
        sql: "SELECT id FROM base".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("SHOW FRONTIERS")
        .await
        .expect("SHOW FRONTIERS failed");
    let rows = data_rows_from(&msgs);

    let op1_row = rows
        .iter()
        .find(|r| r.get(1).and_then(|o| o.as_deref()) == Some("1"))
        .expect("op 1 row missing");

    assert_eq!(
        op1_row.get(4).and_then(|o| o.as_deref()),
        Some("true"),
        "is_slowest_input should be true for source op 1"
    );
}

#[tokio::test]
async fn test_pipeline_stall_oracle_property() {
    let _guard = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();

    // Wedged operator 3 at epoch 50 vs max 100
    rockstream_types::metrics::record_operator_frontier("oracle_view", OperatorId(1), 0, 100, true);
    rockstream_types::metrics::record_operator_frontier(
        "oracle_view",
        OperatorId(2),
        0,
        100,
        false,
    );
    rockstream_types::metrics::record_operator_frontier("oracle_view", OperatorId(3), 0, 50, false);

    let report = rockstream_types::metrics::pipeline_stall_report(Some("oracle_view"));
    let holding_back: Vec<_> = report.iter().filter(|r| r.is_holding_back_commit).collect();

    assert_eq!(
        holding_back.len(),
        1,
        "oracle invariant: exactly 1 wedged operator is holding back commit"
    );
    assert_eq!(holding_back[0].op_id, OperatorId(3));
}

#[tokio::test]
async fn test_pipeline_stall_negative_nonexistent_view() {
    let _guard = TEST_LOCK.lock().await;
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let err = client
        .simple_query("SHOW PIPELINE STALLS FOR non_existent_view")
        .await
        .expect_err("should fail for nonexistent view");

    let err_str = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |db_err| db_err.message().to_string());
    assert!(
        err_str.contains("RS-1001") || err_str.contains("RS-2001"),
        "error must carry RS-1001 or RS-2001 code, got: {err_str}"
    );
    assert!(
        err_str.contains("Next steps:"),
        "error must carry actionable next steps, got: {err_str}"
    );
}

#[tokio::test]
async fn test_pipeline_stall_metrics_endpoint() {
    let _guard = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();

    rockstream_types::metrics::record_operator_frontier(
        "metrics_view",
        OperatorId(1),
        0,
        100,
        true,
    );
    rockstream_types::metrics::record_operator_frontier(
        "metrics_view",
        OperatorId(2),
        0,
        80,
        false,
    );

    let metrics_text = rockstream_types::metrics::generate_prometheus_metrics();

    assert!(
        metrics_text.contains("rockstream_operator_frontier_epoch"),
        "metrics should contain rockstream_operator_frontier_epoch: {metrics_text}"
    );
    assert!(
        metrics_text.contains("rockstream_pipeline_slowest_input_epoch"),
        "metrics should contain rockstream_pipeline_slowest_input_epoch: {metrics_text}"
    );
    assert!(
        metrics_text.contains("rockstream_pipeline_holding_back_frontier"),
        "metrics should contain rockstream_pipeline_holding_back_frontier: {metrics_text}"
    );
}
