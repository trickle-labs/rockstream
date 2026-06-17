//! Gateway integration tests covering S1–S5 green gates.
//!
//! These tests spin up a `GatewayServer` on a random port and connect with
//! `tokio-postgres`.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

// ── No-op ViewReader for catalog-only tests ───────────────────────────────────

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

/// Start a GatewayServer with the given catalog on a random port. Returns the
/// address and a background task handle.
async fn start_gateway(catalog: CatalogStubs) -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

/// Connect with tokio-postgres to `host:port`.
async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').last().unwrap()
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

// ── S1: server_starts_and_accepts_connection ──────────────────────────────────

#[tokio::test]
async fn server_starts_and_accepts_connection() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    // Basic query returns no error
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("simple_query failed");
    // We get at least one message (CommandComplete or DataRow/CommandComplete)
    assert!(!rows.is_empty(), "expected at least one message");
}

// ── S2: proof_pg_catalog_schema_reflection_queries ────────────────────────────

#[tokio::test]
async fn proof_pg_catalog_schema_reflection_queries() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT id, amount FROM orders WHERE amount > 0".to_string(),
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
            namespace: "public".to_string(),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // pg_catalog.pg_tables
    let rows = client
        .simple_query("SELECT schemaname, tablename FROM pg_catalog.pg_tables")
        .await
        .expect("pg_tables query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_views
    let rows = client
        .simple_query("SELECT viewname FROM pg_catalog.pg_views")
        .await
        .expect("pg_views query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_class
    let rows = client
        .simple_query("SELECT oid, relname FROM pg_catalog.pg_class")
        .await
        .expect("pg_class query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_attribute
    let rows = client
        .simple_query("SELECT attrelid, attname, atttypid FROM pg_catalog.pg_attribute")
        .await
        .expect("pg_attribute query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_namespace
    let rows = client
        .simple_query("SELECT oid, nspname FROM pg_catalog.pg_namespace")
        .await
        .expect("pg_namespace query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_type
    let rows = client
        .simple_query("SELECT oid, typname FROM pg_catalog.pg_type")
        .await
        .expect("pg_type query failed");
    assert!(!rows.is_empty());

    // information_schema.tables
    let rows = client
        .simple_query("SELECT table_name FROM information_schema.tables")
        .await
        .expect("information_schema.tables query failed");
    assert!(!rows.is_empty());

    // information_schema.columns
    let rows = client
        .simple_query("SELECT column_name, data_type FROM information_schema.columns")
        .await
        .expect("information_schema.columns query failed");
    assert!(!rows.is_empty());

    // SHOW server_version
    let rows = client
        .simple_query("SHOW server_version")
        .await
        .expect("SHOW server_version failed");
    assert!(!rows.is_empty());

    // SHOW transaction_isolation
    let rows = client
        .simple_query("SHOW transaction_isolation")
        .await
        .expect("SHOW transaction_isolation failed");
    assert!(!rows.is_empty());

    // SET search_path
    client
        .simple_query("SET search_path TO public")
        .await
        .expect("SET search_path failed");

    // Generic SET
    client
        .simple_query("SET standard_conforming_strings = on")
        .await
        .expect("SET failed");
}

// ── S3/S4: view_reader and multi_shard inline tests already in src/ ───────────
// (Tests in view_reader.rs and multi_shard_reader.rs)

// ── S5: extended_query_protocol_parse_bind_execute ────────────────────────────

#[tokio::test]
async fn extended_query_protocol_parse_bind_execute() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "my_view".to_string(),
        sql: "SELECT id, val FROM base".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Float64".to_string(),
            },
        ],
            namespace: "public".to_string(),
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Extended query protocol: prepare() + query()
    let stmt = client
        .prepare("SELECT * FROM my_view")
        .await
        .expect("prepare failed");

    // Verify column count and type OIDs from RowDescription
    assert_eq!(
        stmt.columns().len(),
        2,
        "expected 2 columns in RowDescription"
    );
    // id → INT8 (OID 20), val → FLOAT8 (OID 701)
    let col_types: Vec<u32> = stmt.columns().iter().map(|c| c.type_().oid()).collect();
    assert_eq!(col_types[0], 20, "id column should be INT8 (OID 20)");
    assert_eq!(col_types[1], 701, "val column should be FLOAT8 (OID 701)");

    // Execute via extended query path
    let rows = client.query(&stmt, &[]).await.expect("query failed");
    // No data in view (NoopViewReader returns empty) — just verify no error
    let _ = rows;
}
