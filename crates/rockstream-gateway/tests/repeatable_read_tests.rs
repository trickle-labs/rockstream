//! v0.51.6 Slice 3 — REPEATABLE READ isolation is rejected honestly with RS-2004.
//!
//! Mirrors the existing SERIALIZABLE → RS-2003 coverage in
//! `gateway_proof_tests.rs::proof_serializable_returns_rs2003`. Both the
//! `BEGIN ISOLATION LEVEL REPEATABLE READ` and
//! `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` forms must return
//! RS-2004, never silently accepted-but-unenforced.

use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
};
use tokio_postgres::NoTls;

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
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

async fn start_gateway_noop(catalog: CatalogStubs) -> (u16, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
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

fn assert_rs2004(result: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>) {
    match result {
        Err(e) => {
            let msg = if let Some(db_err) = e.as_db_error() {
                db_err.message().to_string()
            } else {
                e.to_string()
            };
            assert!(
                msg.contains("RS-2004"),
                "expected RS-2004 in error message, got: {msg}"
            );
        }
        Ok(msgs) => {
            let found = msgs.iter().any(|m| format!("{m:?}").contains("RS-2004"));
            assert!(found, "expected RS-2004 error message in response");
        }
    }
}

/// `BEGIN ISOLATION LEVEL REPEATABLE READ` returns RS-2004, mirroring how
/// `BEGIN ISOLATION LEVEL SERIALIZABLE` returns RS-2003.
#[tokio::test]
async fn begin_isolation_level_repeatable_read_returns_rs2004() {
    let (port, _handle) = start_gateway_noop(CatalogStubs::new()).await;
    let client = connect_port(port).await;

    let result = client
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await;

    assert_rs2004(result);
}

/// `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` returns RS-2004.
#[tokio::test]
async fn set_transaction_isolation_level_repeatable_read_returns_rs2004() {
    let (port, _handle) = start_gateway_noop(CatalogStubs::new()).await;
    let client = connect_port(port).await;

    let result = client
        .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await;

    assert_rs2004(result);
}
