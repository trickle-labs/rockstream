//! v0.45.7 S10 — Proof tests that every `COPY FROM STDIN` malformed-statement
//! variant returns `RS-2021` and every `CREATE SINK` malformed-DDL variant
//! returns `RS-4007`, each carrying non-empty, actionable `next_steps` text.
//!
//! Matches the existing `RS-2056`/`RS-2500` assertion pattern already used in
//! `gateway_proof_tests.rs`.

use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::GatewayHandler,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use tokio_postgres::NoTls;

// ── Shared helpers (duplicated per-test-binary convention; see
//    gateway_proof_tests.rs / gateway_dml_tests.rs / golden_wire_tests.rs) ──

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

/// Extract the plain error message text from a `simple_query` failure,
/// whether it surfaces as a structured DB error or a generic client error.
fn db_err_message(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        db_err.message().to_string()
    } else {
        e.to_string()
    }
}

/// Asserts the message carries `code` and a non-empty `Next steps:` (or
/// `next_steps:`) remediation section.
fn assert_code_and_next_steps(msg: &str, code: &str) {
    assert!(
        msg.contains(code),
        "expected {code} in error message; got: {msg}"
    );
    let lower = msg.to_lowercase();
    let marker_pos = lower
        .find("next steps:")
        .or_else(|| lower.find("next_steps:"));
    let marker_pos = marker_pos
        .unwrap_or_else(|| panic!("expected a 'Next steps:' section in message; got: {msg}"));
    let after = msg[marker_pos..]
        .split_once(':')
        .map(|(_, rest)| rest.trim())
        .unwrap_or("");
    assert!(
        !after.is_empty(),
        "expected non-empty next_steps text after 'Next steps:'; got: {msg}"
    );
}

// ── S10: COPY FROM STDIN malformed-statement variants → RS-2021 ─────────────

/// S10 green gate: every `parse_copy_from_stmt` malformed-statement variant
/// returns `RS-2021` with non-empty, actionable `next_steps` text.
///
/// Driven at the `GatewayHandler::copy_from_stdin_response` level (the public
/// test wrapper around `handle_copy_from_stdin`) so that the "not a COPY
/// statement" variant — unreachable through the full pgwire dispatch, which
/// only routes `COPY ... FROM STDIN` text to this handler in the first place
/// — can still be exercised directly, matching the existing
/// `copy_in_auth_enforced_lfs` test's use of the same wrapper.
#[tokio::test]
async fn copy_malformed_variants_return_rs2021() {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
    let handler = GatewayHandler::new(catalog, view_reader);

    let variants: &[&str] = &[
        "SELECT 1",                 // not a COPY statement
        "COPY tbl",                 // missing FROM STDIN
        "COPY  FROM STDIN",         // missing table name
        "COPY (a, b) FROM STDIN",   // missing table name (paren form)
        "COPY tbl(a, b FROM STDIN", // unmatched '('
        "COPY tbl() FROM STDIN",    // empty column list
    ];

    for (i, query) in variants.iter().enumerate() {
        let conn_id = format!("copy-rs2021-conn-{i}");
        let responses = handler
            .copy_from_stdin_response(query, &conn_id)
            .unwrap_or_else(|e| panic!("copy_from_stdin_response errored for {query:?}: {e:?}"));

        let response_count = responses.len();
        let err = responses
            .into_iter()
            .find_map(|r| {
                if let pgwire::api::results::Response::Error(e) = r {
                    Some(e.message.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                panic!("expected an Error response for {query:?}, got {response_count} non-Error response(s)")
            });

        assert_code_and_next_steps(&err, "RS-2021");
    }
}

// ── S10: CREATE SINK malformed-DDL variants → RS-4007 ───────────────────────

async fn start_gateway_with_catalog() -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<rockstream_gateway::catalog_stubs::CatalogStubs>,
) {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, catalog)
}

/// S10 green gate: every `CREATE SINK` malformed-DDL / malformed-option
/// variant returns `RS-4007` with non-empty, actionable `next_steps` text.
#[tokio::test]
async fn create_sink_malformed_ddl_variants_return_rs4007() {
    let (port, _handle, _catalog) = start_gateway_with_catalog().await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE VIEW rs4007_view AS SELECT id FROM base")
        .await
        .expect("CREATE VIEW failed");

    let variants: &[&str] = &[
        // Missing FOR VIEW clause.
        "CREATE SINK s1 TO ICEBERG 'file:///w/s1' WITH (catalog=filesystem)",
        // Missing sink name.
        "CREATE SINK FOR VIEW rs4007_view TO ICEBERG 'file:///w/s1' WITH (catalog=filesystem)",
        // Missing TO clause.
        "CREATE SINK s2 FOR VIEW rs4007_view WITH (catalog=filesystem)",
        // Bad format (neither ICEBERG nor DELTA).
        "CREATE SINK s3 FOR VIEW rs4007_view TO PARQUET 'file:///w/s3' WITH (catalog=filesystem)",
        // Missing WITH clause.
        "CREATE SINK s4 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s4'",
        // Malformed WITH options: bad key=value syntax.
        "CREATE SINK s5 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s5' WITH (catalog)",
        // Malformed ARRAY literal.
        "CREATE SINK s6 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s6' WITH (partition_by=ARRAY[a, b)",
        // Malformed string literal (unterminated).
        "CREATE SINK s7 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s7 WITH (catalog=filesystem)",
        // Non-numeric numeric option.
        "CREATE SINK s8 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s8' WITH (catalog=filesystem, snapshot_interval_ms=notanumber)",
        // Wrong-typed partition_by (not an ARRAY).
        "CREATE SINK s9 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s9' WITH (partition_by='not_an_array')",
        // Wrong-typed catalog (ARRAY instead of identifier/string).
        "CREATE SINK s10 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s10' WITH (catalog=ARRAY['x'])",
        // Unknown view referenced.
        "CREATE SINK s11 FOR VIEW no_such_view TO ICEBERG 'file:///w/s11' WITH (catalog=filesystem)",
        // Invalid catalog value.
        "CREATE SINK s12 FOR VIEW rs4007_view TO ICEBERG 'file:///w/s12' WITH (catalog=bogus_catalog)",
    ];

    for query in variants {
        let result = client.simple_query(query).await;
        let err = match result {
            Err(e) => e,
            Ok(msgs) => panic!("expected CREATE SINK error for {query:?}, got {msgs:?}"),
        };
        let msg = db_err_message(&err);
        assert_code_and_next_steps(&msg, "RS-4007");
    }
}
