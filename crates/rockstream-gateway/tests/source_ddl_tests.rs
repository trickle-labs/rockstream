//! E2E Pgwire DDL tests for `CREATE SOURCE`, `ALTER SOURCE`, `SHOW SOURCES`, and `SHOW SOURCE STATUS`.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
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

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn show_backfill_status_pgwire_reachable() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.begin_backfill("orders_mv", 12);
    catalog.catch_up_backfill("orders_mv", Some("partition=0,key=42".to_string()));
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    assert_eq!(
        simple_rows(
            &client,
            "SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv",
        )
        .await,
        vec![vec![
            Some("orders_mv".to_string()),
            Some("CATCHING_UP".to_string()),
            Some("partition=0,key=42".to_string()),
            Some("0".to_string()),
            Some("12".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );

    let error = client
        .query("SELECT * FROM orders_mv", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-4022] backfill.not_published: materialized view 'orders_mv' is not published yet. Next steps: run SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv and retry when phase is RUNNING."
    );
}

#[tokio::test]
async fn backfill_publication_gate_blocks_partial_relation() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.begin_backfill("orders_mv", 12);
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let error = client
        .query("SELECT * FROM orders_mv", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-4022] backfill.not_published: materialized view 'orders_mv' is not published yet. Next steps: run SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv and retry when phase is RUNNING."
    );
}

#[tokio::test]
async fn show_backfill_status_rejects_missing_name_with_rs2001() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    let error = client.query("SHOW BACKFILL STATUS", &[]).await.unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-2001] sql.invalid_syntax: expected SHOW BACKFILL STATUS FOR MATERIALIZED VIEW <name>. Next steps: provide a materialized view name."
    );
}

#[tokio::test]
async fn show_backfill_status_unknown_view_returns_rs4022() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    let error = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW missing_mv", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-4022] backfill.not_published: materialized view 'missing_mv' does not exist. Next steps: run CREATE MATERIALIZED VIEW missing_mv AS SELECT ... first."
    );
}

#[tokio::test]
async fn show_backfill_status_reports_exact_phase_cursor_remaining_and_estimate() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    client
        .execute("CREATE MATERIALIZED VIEW orders_mv AS SELECT 1 AS id", &[])
        .await
        .unwrap();

    assert_eq!(
        simple_rows(
            &client,
            "SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv",
        )
        .await,
        vec![vec![
            Some("orders_mv".to_string()),
            Some("RUNNING".to_string()),
            None,
            Some("0".to_string()),
            Some("0".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
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

    // 2. SHOW SOURCES
    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name0: String = rows[0].get(0);
    let type0: String = rows[0].get(1);
    let format0: String = rows[0].get(2);
    let status0: String = rows[0].get(3);
    assert_eq!(name0, "kafka_src");
    assert_eq!(type0, "kafka");
    assert_eq!(format0, "json");
    assert_eq!(status0, "OK");

    // 3. SHOW SOURCE STATUS FOR kafka_src
    let status_rows = client
        .query("SHOW SOURCE STATUS FOR kafka_src;", &[])
        .await
        .unwrap();
    assert_eq!(status_rows.len(), 1);
    let st_name: String = status_rows[0].get(0);
    let st_status: String = status_rows[0].get(3);
    assert_eq!(st_name, "kafka_src");
    assert_eq!(st_status, "OK");

    // 4. ALTER SOURCE PAUSE
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

    // 5. ALTER SOURCE RESUME
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

    // 6. DROP SOURCE kafka_src
    client
        .execute("ALTER SOURCE kafka_src DROP;", &[])
        .await
        .unwrap();

    let after_drop_rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert!(after_drop_rows.is_empty());
}

#[tokio::test]
async fn create_source_binds_same_named_existing_table_schema() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect(&addr.to_string()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, customer TEXT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE SOURCE orders TYPE kafka (bootstrap.servers='localhost:9092', topic='orders') FORMAT json",
        )
        .await
        .unwrap();

    let source = catalog.get_source("orders").unwrap();
    let table = catalog.source_table("orders").unwrap();
    assert_eq!(
        (source.table_name, table.name, table.columns),
        (
            Some("orders".to_string()),
            "orders".to_string(),
            vec![
                rockstream_gateway::catalog_stubs::CatalogColumn {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                rockstream_gateway::catalog_stubs::CatalogColumn {
                    name: "customer".to_string(),
                    data_type: "Utf8".to_string(),
                },
            ],
        )
    );
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

    // Duplicate CREATE SOURCE -> RS-4001
    let err = client
        .execute(
            "CREATE SOURCE my_src TYPE kafka (bootstrap.servers='localhost:9092') FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4001"), "expected RS-4001 in error: {msg}");

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
