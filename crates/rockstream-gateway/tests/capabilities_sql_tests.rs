//! PGWire SQL tests for Runtime Capability Registry and `SHOW ROCKSTREAM CAPABILITIES` (v0.59.10 OBS-01).

use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::capability::CapabilityRegistry;

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
async fn test_show_capabilities_agrees_with_toml() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let registry = CapabilityRegistry::current();
    let expected_caps = registry.capabilities();

    let rows = simple_rows(&client, "SHOW ROCKSTREAM CAPABILITIES;").await;
    assert_eq!(
        rows.len(),
        expected_caps.len(),
        "Row count must match capabilities.toml exactly"
    );

    for (row, expected) in rows.iter().zip(expected_caps.iter()) {
        assert_eq!(row.len(), 8);
        assert_eq!(row[0].as_deref(), Some(expected.id.as_str()));
        assert_eq!(row[1].as_deref(), Some(expected.kind.as_str()));
        assert_eq!(row[2].as_deref(), Some(expected.name.as_str()));
        assert_eq!(row[3].as_deref(), Some(expected.tier.as_str()));
        assert_eq!(row[4].as_deref(), Some(expected.reachability.as_str()));
        assert_eq!(
            row[5].as_deref(),
            Some(&expected.dispatch_count().to_string()[..])
        );
        assert_eq!(row[6].as_deref(), Some(expected.proof_ref()));
        assert_eq!(row[7].as_deref(), Some(expected.doc_anchor()));
    }
}

#[tokio::test]
async fn test_show_capabilities_alias_and_reachability() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows1 = simple_rows(&client, "SHOW CAPABILITIES;").await;
    let rows2 = simple_rows(&client, "SHOW ROCKSTREAM CAPABILITIES").await;
    assert_eq!(rows1, rows2);
    assert!(!rows1.is_empty());
}

#[tokio::test]
async fn test_show_capabilities_connectors() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(
        &client,
        "SELECT * FROM rockstream_catalog.capabilities WHERE kind = 'connector';",
    )
    .await;
    assert!(!rows.is_empty());
    for row in rows {
        assert_eq!(row[1].as_deref(), Some("connector"));
    }
}

#[tokio::test]
async fn test_show_capabilities_sinks() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(
        &client,
        "SELECT * FROM rockstream_catalog.capabilities WHERE kind = 'sink';",
    )
    .await;
    assert!(!rows.is_empty());
    for row in rows {
        assert_eq!(row[1].as_deref(), Some("sink"));
        assert_eq!(row[0].as_deref(), Some("sink.kafka"));
    }
}

#[tokio::test]
async fn test_catalog_capabilities_predicate_filtering() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(
        &client,
        "SELECT * FROM rockstream_catalog.capabilities WHERE tier = 'Core';",
    )
    .await;
    assert!(!rows.is_empty());
    for row in rows {
        assert_eq!(row[3].as_deref(), Some("Core"));
    }
}

#[tokio::test]
async fn test_show_capabilities_negative() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW CAPABILITIES UNKNOWN_MODIFIER;")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Must be a Postgres DB error");
    assert!(
        db_err.message().contains("RS-1019"),
        "expected RS-1019 code, got: {}",
        db_err.message()
    );
}
