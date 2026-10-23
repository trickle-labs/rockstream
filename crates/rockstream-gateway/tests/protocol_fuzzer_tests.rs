//! Slice 7 — Protocol Fuzzer (v0.39)
//!
//! Generates SQL request sequences through a live `GatewayServer`, asserting:
//! - No panics
//! - No hangs (each sequence must finish within 5 s)
//! - Exact successful probes before and after each generated sequence
//!
//! Runs 200 random sequences and persists shrunk failures beside this test.

use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};

use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use tokio_postgres::{NoTls, SimpleQueryMessage};

// ── NoopViewReader ─────────────────────────────────────────────────────────────

struct FuzzNoopReader;

#[async_trait::async_trait]
impl ViewReader for FuzzNoopReader {
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

// ── Random SQL statement generator ───────────────────────────────────────────

/// Generate a random, structurally plausible SQL statement.
fn random_sql() -> BoxedStrategy<String> {
    prop_oneof![
        Just("SELECT 1".to_string()),
        Just("SELECT 1 + 1".to_string()),
        Just("SELECT 'hello'".to_string()),
        Just("SET rockstream.wait_for_timeout_ms = 5000".to_string()),
        Just("SHOW search_path".to_string()),
        Just("DISCARD ALL".to_string()),
        Just("RESET ALL".to_string()),
        Just("BEGIN".to_string()),
        Just("ROLLBACK".to_string()),
        // Invalid SQL — should get an error response, not a panic
        Just("SELECT * FROM nonexistent_view_xyz".to_string()),
        Just("DECLARE c1 CURSOR FOR SELECT 1".to_string()),
        Just("FETCH 10 FROM c1".to_string()),
        Just("CLOSE c1".to_string()),
        Just("CLOSE ALL".to_string()),
        // Random garbage (should error gracefully)
        "[A-Za-z0-9 ]{1,50}".prop_map(|s| s),
    ]
    .boxed()
}

/// A sequence of 1-10 SQL statements.
fn random_sequence() -> BoxedStrategy<Vec<String>> {
    prop::collection::vec(random_sql(), 1..=10).boxed()
}

async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(FuzzNoopReader),
    )
    .serve_background()
    .await
    .unwrap()
}

async fn run_connection(addr: SocketAddr, sequence: &[String]) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .map_err(|e| format!("connect: {e}"))?;
    let task = tokio::spawn(connection);
    let result = async {
        let response = client
            .simple_query("SELECT 1 AS probe")
            .await
            .map_err(|e| format!("probe query: {e}"))?;
        if !matches!(
            response.as_slice(),
            [SimpleQueryMessage::RowDescription(columns), SimpleQueryMessage::Row(row), SimpleQueryMessage::CommandComplete(1)]
                if columns.len() == 1 && columns[0].name() == "probe"
                    && row.len() == 1 && row.get(0) == Some("1")
        ) {
            return Err(format!("RS-0001 unexpected probe response: {response:?}"));
        }
        for sql in sequence {
            if let Err(error) = client.simple_query(sql).await {
                if error.as_db_error().is_none() {
                    return Err(format!("RS-0004 transport failure for {sql:?}: {error}"));
                }
            }
        }
        Ok(())
    }
    .await;
    drop(client);
    let connection_result = task
        .await
        .map_err(|e| format!("client task panicked: {e}"))?
        .map_err(|e| format!("client connection failed: {e}"));
    result?;
    connection_result
}

async fn exercise_sequence(addr: SocketAddr, sequence: &[String]) -> Result<(), String> {
    run_connection(addr, sequence).await?;
    run_connection(addr, &[]).await
}

#[test]
fn protocol_fuzzer_no_panics_no_hangs() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (addr, server_task) = rt.block_on(start_server());
    let mut runner = TestRunner::new(Config {
        cases: 200,
        timeout: 0,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        source_file: Some(file!()),
        test_name: Some(concat!(
            module_path!(),
            "::protocol_fuzzer_no_panics_no_hangs"
        )),
        ..Config::default()
    });
    runner
        .run(&random_sequence(), |sequence| {
            let outcome = rt.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), exercise_sequence(addr, &sequence))
                    .await
            });
            if server_task.is_finished() {
                return Err(TestCaseError::fail("gateway listener task stopped"));
            }
            outcome
                .map_err(|_| TestCaseError::fail(format!("request timed out: {sequence:?}")))?
                .map_err(|e| TestCaseError::fail(format!("{sequence:?}: {e}")))
        })
        .unwrap();
    server_task.abort();
    rt.block_on(server_task).unwrap_err();
}

#[test]
fn protocol_harness_rejects_unavailable_and_nonresponding_endpoints() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let unused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let unavailable = unused.local_addr().unwrap();
        drop(unused);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), exercise_sequence(unavailable, &[])).await,
            Ok(Err(_))
        ));

        let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_addr = stalled.local_addr().unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            exercise_sequence(stalled_addr, &[])
        )
        .await
        .is_err());
        drop(stalled);

        let (addr, server_task) = start_server().await;
        let normal =
            tokio::time::timeout(Duration::from_secs(5), exercise_sequence(addr, &[])).await;
        assert!(matches!(normal, Ok(Ok(()))), "normal operation: {normal:?}");
        server_task.abort();
        server_task.await.unwrap_err();
    });
}
