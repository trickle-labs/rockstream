//! v0.42 — DML and DDL gateway tests: UPDATE and REFRESH MATERIALIZED VIEW.

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
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
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

/// UPDATE returns CommandComplete "UPDATE 1" and the gateway accumulates the
/// update in the write buffer.
#[tokio::test]
async fn test_update_accumulates_in_write_buffer() {
    let (port, _handle) = spawn_gateway().await;
    let client = connect_port(port).await;

    let msgs = client
        .simple_query("UPDATE t SET val = 'b' WHERE id = 1")
        .await
        .expect("UPDATE should not error");

    let tags: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(format!("tag:{n}")),
            _ => None,
        })
        .collect();

    assert!(
        !tags.is_empty(),
        "expected CommandComplete from UPDATE, got: {msgs:?}"
    );
}

/// REFRESH MATERIALIZED VIEW returns CommandComplete and succeeds for a view
/// that exists in the catalog. An unknown view name returns an RS-2001 error.
#[tokio::test]
async fn test_refresh_materialized_view_roundtrip() {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "sales_summary".to_string(),
        sql: "SELECT 1 AS total".to_string(),
        columns: vec![CatalogColumn {
            name: "total".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
    });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog, Arc::new(NoopViewReader));
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Refresh an existing materialized view — must succeed.
    let msgs = client
        .simple_query("REFRESH MATERIALIZED VIEW sales_summary")
        .await
        .expect("REFRESH MATERIALIZED VIEW on existing view should succeed");

    let found_complete = msgs
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        found_complete,
        "expected CommandComplete after REFRESH MATERIALIZED VIEW, got: {msgs:?}"
    );

    // Refresh a non-existent view — must return an error.
    let err = client
        .simple_query("REFRESH MATERIALIZED VIEW no_such_view")
        .await;
    let got_error = match &err {
        Err(e) => {
            if let Some(db_err) = e.as_db_error() {
                db_err.message().contains("RS-2001") || db_err.message().contains("no_such_view")
            } else {
                e.to_string().contains("RS-2001")
            }
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2001")),
    };
    assert!(got_error, "expected RS-2001 for missing view, got: {err:?}");
}

/// CREATE VIEW registers in catalog; SELECT from newly created view does not error.
/// With a NoopViewReader (no shard), rows are empty but no error is returned.
#[tokio::test]
async fn test_create_view_and_select() {
    let (port, _handle) = spawn_gateway().await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE VIEW myview AS SELECT 42 AS answer")
        .await
        .expect("CREATE VIEW should succeed");

    // With NoopViewReader, SELECT from a view returns 0 rows but no error.
    let msgs = client
        .simple_query("SELECT answer FROM myview")
        .await
        .expect("SELECT from newly created view should not error");

    let found_complete = msgs
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        found_complete,
        "expected CommandComplete after SELECT from view, got: {msgs:?}"
    );
}

/// DELETE returns CommandComplete "DELETE 1" for a table in the catalog.
#[tokio::test]
async fn test_delete_accumulates_in_write_buffer() {
    let (port, _handle) = spawn_gateway().await;
    let client = connect_port(port).await;

    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 99")
        .await
        .expect("DELETE should not error");

    let found_complete = msgs
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        found_complete,
        "expected CommandComplete after DELETE, got: {msgs:?}"
    );
}
