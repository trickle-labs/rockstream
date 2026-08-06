//! v0.51.19 live pgwire proof: a shared-lock holder panic cannot affect a peer.

use std::sync::Arc;
use std::time::Duration;

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::ViewReader,
    GatewayError, GatewayServer,
};
use rockstream_types::dlq::{get_global_dlq, DlqEntry};

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

fn dlq_entry(source_name: &str) -> DlqEntry {
    DlqEntry {
        arrived_at: 1,
        source_name: source_name.to_string(),
        source_offset: "offset".to_string(),
        error_code: "RS-4008".to_string(),
        error_message: "invalid payload".to_string(),
        raw_bytes_hex: "7b7d".to_string(),
        replay_attempt: 0,
    }
}

async fn start_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "pgwire_peer_view".to_string(),
        sql: "SELECT id FROM source".to_string(),
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
    server.serve_background().await.unwrap()
}

async fn query_value(client: &tokio_postgres::Client) -> String {
    let messages = tokio::time::timeout(Duration::from_secs(2), client.simple_query("SELECT 1"))
        .await
        .expect("live pgwire query must not stall")
        .unwrap();
    messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .expect("SELECT 1 must return one row")
        .to_string()
}

#[tokio::test]
async fn pgwire_peer_connection_survives_lock_holder_panic() {
    let (addr, server_task) = start_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_eq!(query_value(&client).await, "1");
    let extended_rows = tokio::time::timeout(
        Duration::from_secs(2),
        client.query("SELECT id FROM pgwire_peer_view WHERE id = $1", &[&1i64]),
    )
    .await
    .expect("extended query must not stall")
    .unwrap();
    assert!(extended_rows.is_empty());

    let dlq = get_global_dlq();
    dlq.lock().clear();
    let holder_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut entries = dlq.lock();
        entries.push(dlq_entry("abandoned"));
        panic!("injected live pgwire lock holder panic");
    }));
    assert!(holder_panic.is_err());

    assert_eq!(query_value(&client).await, "1");
    assert_eq!(
        dlq.lock().clone(),
        vec![dlq_entry("abandoned")],
        "the peer process must retain the exact DLQ entry after holder panic"
    );

    drop(client);
    connection_task.abort();
    server_task.abort();
    dlq.lock().clear();
}
