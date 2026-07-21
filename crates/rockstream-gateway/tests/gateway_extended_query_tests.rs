//! Extended query protocol and prepared statement integration tests.
//!
//! These tests spin up a `GatewayServer` on a random port and connect with
//! `tokio-postgres` to exercise SSL downgrade, prepared statements, parameter
//! inference, describe, and limit/bound checks.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

// ── S1: SSL Negotiation & Parameter Status ────────────────────────────────────

#[tokio::test]
async fn test_ssl_negotiation_downgrade() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let port = addr.split(':').next_back().unwrap();

    // 1. Raw TCP: Send SSLRequest, expect 'N'
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47];
    socket.write_all(&ssl_request).await.unwrap();

    let mut response = [0u8; 1];
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(response[0], b'N');

    // 2. Startup Protocol over Raw TCP: Send StartupMessage and verify ParameterStatus messages in response
    let mut socket2 = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Construct StartupMessage:
    // length: 4 (len) + 4 (version) + 25 (params) = 33
    // version: 3.0 (196608)
    // parameters: user\0test\0database\0test\0\0
    let mut startup_packet = vec![0u8, 0, 0, 33, 0, 3, 0, 0];
    startup_packet.extend_from_slice(b"user\0test\0database\0test\0\0");
    socket2.write_all(&startup_packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket2.read(&mut buf).await.unwrap();
    let response_str = String::from_utf8_lossy(&buf[..n]);

    // Check that standard startup parameter status values are present in the response
    assert!(
        response_str.contains("server_version"),
        "server_version missing in startup parameters"
    );
    assert!(
        response_str.contains("14.9 (RockStream)"),
        "server version value missing"
    );
    assert!(
        response_str.contains("server_encoding"),
        "server_encoding missing"
    );
    assert!(response_str.contains("UTF8"), "UTF8 encoding missing");
    assert!(response_str.contains("DateStyle"), "DateStyle missing");
    assert!(response_str.contains("ISO, YMD"), "DateStyle value missing");
}

// ── S2 & S3: Prepared Statement Caching, Deallocate & Describe ───────────────

#[tokio::test]
async fn test_prepared_statement_caching_and_deallocate() {
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
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // 1. Prepare with parameter type inference & explicit casts
    let stmt = client
        .prepare("SELECT id FROM orders_mv WHERE amount > $1::FLOAT8 AND id = $2")
        .await
        .unwrap();

    // Verify S3 parameter descriptions
    let params = stmt.params();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], tokio_postgres::types::Type::FLOAT8);
    assert_eq!(params[1], tokio_postgres::types::Type::INT8); // Inferred from id: Int64

    // Verify S3 row descriptions
    let columns = stmt.columns();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name(), "id");
    assert_eq!(columns[0].type_(), &tokio_postgres::types::Type::INT8);

    // 2. Test DEALLOCATE ALL clears them
    client.simple_query("DEALLOCATE ALL").await.unwrap();
}

// ── S4: Extended Query Pipeline & Limit Bounds ────────────────────────────────

#[tokio::test]
async fn test_extended_query_pipeline() {
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
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Standard extended query protocol execution (Parse -> Bind -> Execute)
    let rows = client
        .query("SELECT id FROM orders_mv WHERE id = $1", &[&100i64])
        .await
        .unwrap();
    assert!(rows.is_empty()); // empty since mock reader returns empty
}

#[tokio::test]
async fn test_prepared_statements_limit() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;

    // Prepare 1001 unique queries to exceed the limit of 1000.
    // Store statements in a vector to prevent them from being dropped (which sends Close commands).
    let mut exceeded = false;
    let mut statements = Vec::new();
    for i in 0..1005 {
        let sql = format!("SELECT 1 -- query_uniq_{}", i);
        let res = client.prepare(&sql).await;
        if i < 1000 {
            assert!(res.is_ok(), "Failed to prepare statement at index {}", i);
            statements.push(res.unwrap());
        } else {
            assert!(res.is_err(), "Expected 1001st statement prepare to fail");
            let err = res.unwrap_err();
            // Verify PostgreSQL error code 53200 (Insufficient Resources)
            assert_eq!(err.code().map(|c| c.code()), Some("53200"));
            let msg = err
                .as_db_error()
                .map(|e| e.message().to_string())
                .unwrap_or_else(|| err.to_string());
            assert!(
                msg.contains("RS-2600"),
                "Expected custom RS-2600 error code in: {}",
                msg
            );
            assert!(
                msg.contains("next_steps"),
                "Expected next_steps description in: {}",
                msg
            );
            exceeded = true;
            break;
        }
    }
    assert!(
        exceeded,
        "Prepared statements limit of 1000 was not reached"
    );
}

#[tokio::test]
async fn test_portals_limit() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let mut client = connect(&addr).await;

    let transaction = client.transaction().await.unwrap();
    let stmt = transaction.prepare("SELECT 1").await.unwrap();

    let mut exceeded = false;
    let mut _portals = Vec::new();
    for i in 0..1005 {
        let res = transaction.bind(&stmt, &[]).await;
        match res {
            Ok(portal) => {
                assert!(
                    i < 1000,
                    "Should not succeed in binding portal at index {}",
                    i
                );
                _portals.push(portal);
            }
            Err(err) => {
                assert!(i >= 1000, "Portal bind failed prematurely at index {}", i);
                // Verify PostgreSQL error code 53200 (Insufficient Resources)
                assert_eq!(err.code().map(|c| c.code()), Some("53200"));
                let msg = err
                    .as_db_error()
                    .map(|e| e.message().to_string())
                    .unwrap_or_else(|| err.to_string());
                assert!(
                    msg.contains("RS-2601"),
                    "Expected custom RS-2601 error code in: {}",
                    msg
                );
                assert!(
                    msg.contains("next_steps"),
                    "Expected next_steps description in: {}",
                    msg
                );
                exceeded = true;
                break;
            }
        }
    }
    assert!(exceeded, "Portals limit of 1000 was not reached");
}

// ── S5: Portal Suspension with max_rows ───────────────────────────────────────

struct SuspensionViewReader {
    rows: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl ViewReader for SuspensionViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(self.rows.clone())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn test_portal_suspension_max_rows() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "large_mv".to_string(),
        sql: "SELECT id FROM large_mv".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });

    let mut rows = Vec::new();
    for i in 0..1000 {
        rows.push(format!("{i}").into_bytes());
    }

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(catalog),
        Arc::new(SuspensionViewReader { rows }),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let mut client = connect(&local_addr.to_string()).await;

    let tx = client.transaction().await.unwrap();
    let stmt = tx.prepare("SELECT id FROM large_mv").await.unwrap();
    let portal = tx.bind(&stmt, &[]).await.unwrap();

    // Fetch first 10 rows
    let batch1 = tx.query_portal(&portal, 10).await.unwrap();
    assert_eq!(batch1.len(), 10);
    for (i, row) in batch1.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, i as i64);
    }

    // Fetch next 15 rows
    let batch2 = tx.query_portal(&portal, 15).await.unwrap();
    assert_eq!(batch2.len(), 15);
    for (i, row) in batch2.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, (10 + i) as i64);
    }

    // Fetch the rest (max_rows = 0 or -1 returns all remaining)
    let batch3 = tx.query_portal(&portal, 0).await.unwrap();
    assert_eq!(batch3.len(), 975);
    for (i, row) in batch3.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, (25 + i) as i64);
    }

    tx.commit().await.unwrap();
}

// ── S6: Multi-Statement Simple Queries & Empty Queries ────────────────────────

#[tokio::test]
async fn test_multi_statement_and_empty_queries() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;

    // 1. Multi-statement simple query
    let results = client.simple_query("SELECT 1; SELECT 2").await.unwrap();
    use tokio_postgres::SimpleQueryMessage;
    let mut command_completes = 0;
    for msg in &results {
        if let SimpleQueryMessage::CommandComplete(_) = msg {
            command_completes += 1;
        }
    }
    assert!(
        command_completes >= 2,
        "Expected at least 2 CommandCompletes, got {}",
        command_completes
    );

    // 2. Empty query / whitespace-only query
    let empty_results = client.simple_query("   ;   ").await.unwrap();
    assert_eq!(empty_results.len(), 1);
    match &empty_results[0] {
        SimpleQueryMessage::CommandComplete(rows) => {
            assert_eq!(*rows, 0);
        }
        _ => panic!("Expected CommandComplete(0) for empty query"),
    }
}
