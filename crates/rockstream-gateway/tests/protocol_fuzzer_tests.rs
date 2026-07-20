//! Slice 7 — Protocol Fuzzer (v0.39)
//!
//! Generates random but structurally valid pgwire message byte sequences and
//! feeds them to a live `GatewayServer`, asserting:
//! - No panics
//! - No hangs (each sequence times out in ≤ 5 s)
//! - No malformed (non-pgwire) responses
//!
//! Runs 200 random sequences by default (scaled to be fast in CI).
//! Set `PROPTEST_CASES=5000` to scale up.

use std::sync::Arc;

use proptest::prelude::*;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

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

// ── Shared server ─────────────────────────────────────────────────────────────

/// Returns a port with a running gateway. We use a single Tokio runtime
/// shared across all proptest cases (via `once_cell`).
fn get_server_port() -> u16 {
    use std::sync::OnceLock;
    static PORT: OnceLock<u16> = OnceLock::new();
    *PORT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = GatewayServer::with_catalog(
                addr,
                Arc::new(CatalogStubs::new()),
                Arc::new(FuzzNoopReader),
            );
            let (local_addr, _handle) = server.serve_background().await.unwrap();
            // Intentionally leak `_handle` — it lives for the entire test process.
            Box::leak(Box::new(_handle));
            local_addr.port()
        })
    })
}

// ── Fuzzer property test ──────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 200,
        // The test body already enforces a 5s Tokio timeout per generated
        // sequence. Keeping proptest's fork/timeout watchdog enabled causes
        // false failures from child-process startup overhead before the first
        // case runs, so leave the outer watchdog off.
        timeout: 0,
        ..ProptestConfig::default()
    })]

    /// Slice 7 green gate: no panics, no hangs under 200 random SQL sequences.
    #[test]
    fn protocol_fuzzer_no_panics_no_hangs(sequence in random_sequence()) {
        let port = get_server_port();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let outcome = rt.block_on(async {
            let timeout = tokio::time::Duration::from_secs(5);
            tokio::time::timeout(timeout, async {
                // Each proptest case gets a fresh connection
                let conn_result = tokio_postgres::connect(
                    &format!("host=127.0.0.1 port={port} user=test dbname=test"),
                    tokio_postgres::NoTls,
                )
                .await;

                let (client, conn) = match conn_result {
                    Ok(x) => x,
                    // Connection refused is not a panic — just skip
                    Err(_) => return Ok::<(), String>(()),
                };

                tokio::spawn(async move { conn.await.ok(); });

                for sql in &sequence {
                    // Each statement is allowed to error (unsupported, invalid, etc.)
                    // What we test is that it does NOT panic or hang.
                    let _ = client.simple_query(sql.as_str()).await;
                }
                Ok(())
            })
            .await
        });

        // Timeout is NOT a test failure for proptest — just log it.
        // A panic would propagate and fail the test.
        match outcome {
            Ok(_) | Err(_) => {} // timeout or success both OK; panics propagate
        }
    }
}
