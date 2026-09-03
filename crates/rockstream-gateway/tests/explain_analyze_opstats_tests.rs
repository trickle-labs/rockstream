//! Tests for `EXPLAIN INCREMENTAL ANALYZE` live operator statistics integration (v0.51.11).

use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};
use tokio_postgres::NoTls;

use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable, CatalogView};
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::ViewReadStrategy;
use rockstream_gateway::GatewayError;
use rockstream_types::ids::OperatorId;

static OPSTATS_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
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
async fn test_explain_analyze_opstats_aggregate() {
    let _guard = OPSTATS_TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_operator_runtime_sample_at(
        OperatorId(1),
        500,
        42,
        Duration::from_millis(15),
        3,
        SystemTime::now(),
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "base".to_string(),
        columns: vec![
            CatalogColumn {
                name: "k".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "v".to_string(),
                data_type: "Int64".to_string(),
            },
        ],
    });
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
        .simple_query("EXPLAIN INCREMENTAL ANALYZE agg_view")
        .await
        .expect("EXPLAIN INCREMENTAL ANALYZE failed");
    let plan_output = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_output.contains("rows/s="), "output: {plan_output}");
    assert!(
        plan_output.contains("state_reads="),
        "output: {plan_output}"
    );
    assert!(plan_output.contains("p99="), "output: {plan_output}");
    assert!(
        plan_output.contains("dlq_entries="),
        "output: {plan_output}"
    );
}

#[tokio::test]
async fn test_explain_analyze_opstats_join() {
    let _guard = OPSTATS_TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_operator_runtime_sample_at(
        OperatorId(1),
        1000,
        100,
        Duration::from_millis(5),
        0,
        SystemTime::now(),
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "t1".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
    });
    catalog.add_table(CatalogTable {
        name: "t2".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Text".to_string(),
            },
        ],
    });
    catalog.add_view(CatalogView {
        name: "join_view".to_string(),
        sql: "SELECT a.id, b.val FROM t1 a JOIN t2 b ON a.id = b.id".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Text".to_string(),
            },
        ],
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
        .simple_query("EXPLAIN INCREMENTAL ANALYZE join_view")
        .await
        .expect("EXPLAIN INCREMENTAL ANALYZE failed");
    let plan_output = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_output.contains("rows/s="), "output: {plan_output}");
    assert!(
        plan_output.contains("state_reads="),
        "output: {plan_output}"
    );
}

#[tokio::test]
async fn test_explain_analyze_opstats_distinct_minmax() {
    let _guard = OPSTATS_TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_operator_runtime_sample_at(
        OperatorId(1),
        250,
        10,
        Duration::from_millis(2),
        0,
        SystemTime::now(),
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "base".to_string(),
        columns: vec![CatalogColumn {
            name: "k".to_string(),
            data_type: "Int64".to_string(),
        }],
    });
    catalog.add_view(CatalogView {
        name: "distinct_view".to_string(),
        sql: "SELECT DISTINCT k FROM base".to_string(),
        columns: vec![CatalogColumn {
            name: "k".to_string(),
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
        .simple_query("EXPLAIN INCREMENTAL ANALYZE distinct_view")
        .await
        .expect("EXPLAIN INCREMENTAL ANALYZE failed");
    let plan_output = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_output.contains("rows/s="), "output: {plan_output}");
}

#[tokio::test]
async fn test_explain_analyze_opstats_oracle_property() {
    let _guard = OPSTATS_TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let sample_rows = 1200;
    let sample_reads = 85;
    rockstream_types::metrics::record_operator_runtime_sample_at(
        OperatorId(1),
        sample_rows,
        sample_reads,
        Duration::from_millis(10),
        0,
        SystemTime::now(),
    );

    let report = rockstream_types::metrics::operator_runtime_report();
    assert!(
        !report.is_empty(),
        "operator_runtime_report should not be empty"
    );
    let snapshot = &report[0];

    // Oracle Property check: stats from operator_runtime_report match sample values divided by 60s window
    let reported_rows = snapshot.rows_per_s as u64;
    let expected_rows_per_s = sample_rows / 60;
    assert_eq!(
        reported_rows, expected_rows_per_s,
        "Oracle tolerance check failed: reported={reported_rows}, expected={expected_rows_per_s}"
    );
}
