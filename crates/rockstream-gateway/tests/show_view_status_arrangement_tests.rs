//! PGWire SQL tests for `SHOW VIEW STATUS` arrangement sharing facts (v0.59.6).

use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::explain::ArrangementSharingInfo;
use rockstream_types::ids::ArrangementId;

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
async fn test_pgwire_show_view_status_arrangement_sharing_facts() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "shared_orders_view".to_string(),
        sql: "SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    catalog.set_view_arrangement_sharing(
        "shared_orders_view",
        ArrangementSharingInfo {
            arrangement_id: Some(ArrangementId(1001)),
            consumer_count: 3,
            shared_state_bytes: 2097152,
            bytes_saved_by_sharing: 4194304,
            compaction_frontier: 50,
        },
    );

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR shared_orders_view;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 27);
    assert_eq!(row[1].as_deref(), Some("shared_orders_view"));
    // Verify the 5 arrangement sharing columns:
    // arrangement_id, consumer_count, shared_state_bytes, bytes_saved_by_sharing, compaction_frontier
    assert_eq!(row[22].as_deref(), Some("arr-1001"));
    assert_eq!(row[23].as_deref(), Some("3"));
    assert_eq!(row[24].as_deref(), Some("2097152"));
    assert_eq!(row[25].as_deref(), Some("4194304"));
    assert_eq!(row[26].as_deref(), Some("50"));
}
