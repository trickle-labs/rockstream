//! Integration tests for System Limits Catalog & Enforcement Hooks (Matrix B, DOC-001).

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::limits::{
    SystemLimitsCatalog, MAX_CONCURRENT_CONNECTIONS, MAX_CONN_MEMORY_BYTES, MAX_CURSORS_PER_CONN,
    MAX_DECIMAL_PRECISION_DIGITS, MAX_IDENTIFIER_BYTE_LENGTH, MAX_PORTALS_PER_CONN,
    MAX_PREPARED_STATEMENTS_PER_CONN, MAX_RESULT_ROWS, MAX_VIEW_DEPENDENCY_DEPTH,
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

async fn spawn_gateway() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog, Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle)
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

#[test]
fn test_system_limits_catalog_completeness() {
    let limits = SystemLimitsCatalog::all();
    assert_eq!(limits.len(), 9, "Must contain all 9 Matrix B limits");

    let ids: Vec<&str> = limits.iter().map(|l| l.id.as_str()).collect();
    assert!(ids.contains(&"MAX_RESULT_ROWS"));
    assert!(ids.contains(&"MAX_CONN_MEMORY"));
    assert!(ids.contains(&"MAX_CONNECTIONS"));
    assert!(ids.contains(&"MAX_PREPARED_STMTS"));
    assert!(ids.contains(&"MAX_PORTALS"));
    assert!(ids.contains(&"MAX_CURSORS"));
    assert!(ids.contains(&"MAX_IDENTIFIER_LEN"));
    assert!(ids.contains(&"MAX_DECIMAL_PRECISION"));
    assert!(ids.contains(&"MAX_VIEW_DAG_DEPTH"));

    // Verify limit values match constants
    let result_rows = limits.iter().find(|l| l.id == "MAX_RESULT_ROWS").unwrap();
    assert_eq!(result_rows.canonical_value, MAX_RESULT_ROWS);
    assert_eq!(result_rows.error_code, "RS-2040");

    let conn_mem = limits.iter().find(|l| l.id == "MAX_CONN_MEMORY").unwrap();
    assert_eq!(conn_mem.canonical_value, MAX_CONN_MEMORY_BYTES);
    assert_eq!(conn_mem.error_code, "RS-2053");

    let conns = limits.iter().find(|l| l.id == "MAX_CONNECTIONS").unwrap();
    assert_eq!(conns.canonical_value, MAX_CONCURRENT_CONNECTIONS);
    assert_eq!(conns.error_code, "RS-2055");

    let prep = limits
        .iter()
        .find(|l| l.id == "MAX_PREPARED_STMTS")
        .unwrap();
    assert_eq!(prep.canonical_value, MAX_PREPARED_STATEMENTS_PER_CONN);
    assert_eq!(prep.error_code, "RS-2600");

    let portals = limits.iter().find(|l| l.id == "MAX_PORTALS").unwrap();
    assert_eq!(portals.canonical_value, MAX_PORTALS_PER_CONN);
    assert_eq!(portals.error_code, "RS-2601");

    let cursors = limits.iter().find(|l| l.id == "MAX_CURSORS").unwrap();
    assert_eq!(cursors.canonical_value, MAX_CURSORS_PER_CONN);
    assert_eq!(cursors.error_code, "RS-2052");

    let ident = limits
        .iter()
        .find(|l| l.id == "MAX_IDENTIFIER_LEN")
        .unwrap();
    assert_eq!(ident.canonical_value, MAX_IDENTIFIER_BYTE_LENGTH);
    assert_eq!(ident.error_code, "RS-1012");

    let prec = limits
        .iter()
        .find(|l| l.id == "MAX_DECIMAL_PRECISION")
        .unwrap();
    assert_eq!(prec.canonical_value, MAX_DECIMAL_PRECISION_DIGITS);
    assert_eq!(prec.error_code, "RS-1016");

    let dag = limits
        .iter()
        .find(|l| l.id == "MAX_VIEW_DAG_DEPTH")
        .unwrap();
    assert_eq!(dag.canonical_value, MAX_VIEW_DEPENDENCY_DEPTH);
    assert_eq!(dag.error_code, "RS-1011");
}

#[tokio::test]
async fn test_cursor_limit_rejection_code_rs2052() {
    let (port, handle) = spawn_gateway().await;
    let client = connect_port(port).await;

    client
        .batch_execute("CREATE TABLE tbl (id int, v int); INSERT INTO tbl VALUES (1, 10);")
        .await
        .unwrap();

    // Open exactly MAX_CURSORS_PER_CONN (64) cursors inside a transaction block
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..MAX_CURSORS_PER_CONN {
        let cursor_name = format!("cur_{i}");
        let sql = format!("DECLARE {cursor_name} CURSOR FOR SELECT * FROM tbl");
        client.simple_query(&sql).await.unwrap();
    }

    // The 65th cursor must fail with RS-2052
    let err = client
        .simple_query("DECLARE cur_overflow CURSOR FOR SELECT * FROM tbl")
        .await
        .unwrap_err();
    let err_msg = err.as_db_error().map(|d| d.message()).unwrap_or("");
    assert!(
        err_msg.contains("RS-2052"),
        "Exceeding MAX_CURSORS_PER_CONN must return RS-2052, got db error message: {err_msg}"
    );

    client.simple_query("ROLLBACK").await.unwrap();
    handle.abort();
}

#[tokio::test]
async fn test_identifier_length_limit_rs1012() {
    assert_eq!(MAX_IDENTIFIER_BYTE_LENGTH, 63);
    let valid_ident = "a".repeat(63);
    assert_eq!(valid_ident.len(), 63);

    let too_long_ident = "a".repeat(64);
    assert_eq!(too_long_ident.len(), 64);
}

#[tokio::test]
async fn test_system_limits_constants_values() {
    assert_eq!(MAX_RESULT_ROWS, 10_000);
    assert_eq!(MAX_CONN_MEMORY_BYTES, 64 * 1024 * 1024);
    assert_eq!(MAX_CONCURRENT_CONNECTIONS, 100);
    assert_eq!(MAX_PREPARED_STATEMENTS_PER_CONN, 100);
    assert_eq!(MAX_PORTALS_PER_CONN, 50);
    assert_eq!(MAX_CURSORS_PER_CONN, 64);
    assert_eq!(MAX_IDENTIFIER_BYTE_LENGTH, 63);
    assert_eq!(MAX_DECIMAL_PRECISION_DIGITS, 38);
    assert_eq!(MAX_VIEW_DEPENDENCY_DEPTH, 16);
}
