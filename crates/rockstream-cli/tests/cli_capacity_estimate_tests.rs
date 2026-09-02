//! CLI and PGWire capacity estimate equivalence tests (v0.59.23 Slice 3).

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_cli::output::{ExplainEstimateInfo, OutputFormat};
use rockstream_cli::run_explain_view;
use rockstream_cli::transport::CatalogClient;
use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::ViewReadStrategy;
use rockstream_gateway::GatewayError;

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
async fn cli_and_pgwire_reports_match_exactly() {
    let view_name = "active_users";
    let view_query = "SELECT id, count(*) FROM users GROUP BY id";

    // 1. Gateway setup
    let gw_catalog = Arc::new(CatalogStubs::new());
    gw_catalog.add_table(rockstream_gateway::catalog_stubs::CatalogTable {
        name: "users".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
    });
    gw_catalog.add_view(CatalogView {
        name: view_name.to_string(),
        sql: view_query.to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "count".to_string(),
                data_type: "Int64".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        gw_catalog,
        Arc::new(DummyViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // 2. PGWire report
    let msgs = client
        .simple_query(&format!("EXPLAIN INCREMENTAL ESTIMATE {view_name}"))
        .await
        .expect("PGWire EXPLAIN INCREMENTAL ESTIMATE failed");
    let pgwire_report = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.first().and_then(|o| o.clone()))
        .collect::<Vec<_>>()
        .join("\n");

    // 3. CLI report via run_explain_view
    let cli_catalog = CatalogClient::with_defaults();
    let cli_text_report =
        run_explain_view(OutputFormat::Text, &cli_catalog, view_name, true, false)
            .expect("CLI run_explain_view text failed");

    // Both reports must include the complete header, state metrics, and operator table
    assert!(pgwire_report.contains("EXPLAIN INCREMENTAL ESTIMATE (Calibrated Capacity Report)"));
    assert!(cli_text_report.contains("EXPLAIN INCREMENTAL ESTIMATE (Calibrated Capacity Report)"));
    assert_eq!(pgwire_report.trim(), cli_text_report.trim());

    // 4. CLI JSON report derives from the exact same capacity model
    let cli_json_report =
        run_explain_view(OutputFormat::Json, &cli_catalog, view_name, true, false)
            .expect("CLI run_explain_view json failed");
    let est_info: ExplainEstimateInfo = serde_json::from_str(&cli_json_report).unwrap();
    assert_eq!(est_info.view_name, view_name);
    assert_eq!(est_info.formatted_text.trim(), pgwire_report.trim());
    assert!(est_info.capacity_estimate.is_some());
    assert!(!est_info.estimates.is_empty());
}
