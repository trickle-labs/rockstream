//! v0.52 Slice 1 — Dead Letter Queue Catalog Query Integration & Reachability Tests.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
};
use rockstream_types::dlq::{get_global_dlq, quarantine_record};

struct DummyViewReader;
#[async_trait::async_trait]
impl ViewReader for DummyViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: rockstream_gateway::view_reader::ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_test_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(DummyViewReader);
    let server = GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);
    server.serve_background().await.unwrap()
}

#[tokio::test]
async fn test_query_dead_letter_queue_catalog_surface() {
    get_global_dlq().lock().clear();

    quarantine_record(
        "kafka_orders",
        "101",
        "RS-1003",
        "malformed JSON payload",
        b"invalid_json",
    );
    quarantine_record(
        "cdc_users",
        "202",
        "RS-1003",
        "pgoutput decode error",
        b"invalid_cdc",
    );

    let (addr, _handle) = start_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let rows = client
        .query("SELECT arrived_at, source_name, source_offset, error_code, error_message, raw_bytes_hex, replay_attempt FROM rockstream_catalog.dead_letter_queue", &[])
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);

    let row0 = &rows[0];
    let source_name0: String = row0.get("source_name");
    let error_code0: String = row0.get("error_code");
    assert_eq!(source_name0, "kafka_orders");
    assert_eq!(error_code0, "RS-1003");

    let row1 = &rows[1];
    let source_name1: String = row1.get("source_name");
    assert_eq!(source_name1, "cdc_users");

    get_global_dlq().lock().clear();
}
