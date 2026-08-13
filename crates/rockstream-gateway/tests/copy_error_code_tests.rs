//! v0.45.7 S10 — Proof tests that every `COPY FROM STDIN` malformed-statement
//! variant returns `RS-2021` with non-empty, actionable `next_steps` text.
//!
//! Matches the existing `RS-2056`/`RS-2500` assertion pattern already used in
//! `gateway_proof_tests.rs`.

use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::GatewayHandler,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError,
};

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
