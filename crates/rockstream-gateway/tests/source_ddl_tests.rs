//! E2E Pgwire DDL tests for `CREATE SOURCE`, `ALTER SOURCE`, `SHOW SOURCES`, and `SHOW SOURCE STATUS`.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

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

#[tokio::test]
async fn test_create_alter_show_sources_e2e() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // 1. Create Kafka source
    client
        .execute(
            "CREATE SOURCE kafka_src TYPE kafka (bootstrap.servers='localhost:9092', topic='orders') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    // 2. Create S3 source
    client
        .execute(
            "CREATE SOURCE s3_src TYPE s3 (bucket='mybucket', prefix='data/') FORMAT csv;",
            &[],
        )
        .await
        .unwrap();

    // 3. SHOW SOURCES
    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name0: String = rows[0].get(0);
    let type0: String = rows[0].get(1);
    let format0: String = rows[0].get(2);
    let status0: String = rows[0].get(3);
    assert_eq!(name0, "kafka_src");
    assert_eq!(type0, "kafka");
    assert_eq!(format0, "json");
    assert_eq!(status0, "OK");

    let name1: String = rows[1].get(0);
    let type1: String = rows[1].get(1);
    let format1: String = rows[1].get(2);
    let status1: String = rows[1].get(3);
    assert_eq!(name1, "s3_src");
    assert_eq!(type1, "s3");
    assert_eq!(format1, "csv");
    assert_eq!(status1, "OK");

    // 4. SHOW SOURCE STATUS FOR kafka_src
    let status_rows = client
        .query("SHOW SOURCE STATUS FOR kafka_src;", &[])
        .await
        .unwrap();
    assert_eq!(status_rows.len(), 1);
    let st_name: String = status_rows[0].get(0);
    let st_status: String = status_rows[0].get(3);
    assert_eq!(st_name, "kafka_src");
    assert_eq!(st_status, "OK");

    // 5. ALTER SOURCE PAUSE
    client
        .execute("ALTER SOURCE kafka_src PAUSE;", &[])
        .await
        .unwrap();

    let paused_rows = client
        .query("SHOW SOURCE STATUS FOR kafka_src;", &[])
        .await
        .unwrap();
    let st_status_paused: String = paused_rows[0].get(3);
    assert_eq!(st_status_paused, "PAUSED");

    // 6. ALTER SOURCE RESUME
    client
        .execute("ALTER SOURCE kafka_src RESUME;", &[])
        .await
        .unwrap();

    let resumed_rows = client
        .query("SHOW SOURCE STATUS FOR kafka_src;", &[])
        .await
        .unwrap();
    let st_status_resumed: String = resumed_rows[0].get(3);
    assert_eq!(st_status_resumed, "OK");

    // 7. DROP SOURCE s3_src
    client
        .execute("ALTER SOURCE s3_src DROP;", &[])
        .await
        .unwrap();

    let after_drop_rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(after_drop_rows.len(), 1);
    let remaining_name: String = after_drop_rows[0].get(0);
    assert_eq!(remaining_name, "kafka_src");
}

#[tokio::test]
async fn test_source_ddl_negative_cases() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Invalid format
    let err = client
        .execute("CREATE SOURCE bad_src TYPE kafka FORMAT xml;", &[])
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008 in error: {msg}");

    // Missing TYPE
    let err = client
        .execute("CREATE SOURCE bad_src FORMAT json;", &[])
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008 in error: {msg}");

    // Create valid source
    client
        .execute(
            "CREATE SOURCE my_src TYPE kafka (bootstrap.servers='localhost:9092') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    // Duplicate CREATE SOURCE -> RS-4010
    let err = client
        .execute(
            "CREATE SOURCE my_src TYPE kafka (bootstrap.servers='localhost:9092') FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4010"), "expected RS-4010 in error: {msg}");

    // ALTER non-existent source -> RS-4009
    let err = client
        .execute("ALTER SOURCE missing_src PAUSE;", &[])
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4009"), "expected RS-4009 in error: {msg}");

    // SHOW SOURCE STATUS FOR missing source -> RS-4009
    let err = client
        .query("SHOW SOURCE STATUS FOR missing_src;", &[])
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4009"), "expected RS-4009 in error: {msg}");
}

#[tokio::test]
async fn test_show_sources_lag_and_offset() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    client
        .execute(
            "CREATE SOURCE metrics_src TYPE kafka (topic='metrics') FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0);
    let offset: String = rows[0].get(4);
    let lag: String = rows[0].get(5);
    assert_eq!(name, "metrics_src");
    assert_eq!(offset, "0");
    assert_eq!(lag, "0");
}
