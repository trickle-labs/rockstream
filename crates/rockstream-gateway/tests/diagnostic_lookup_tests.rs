use std::sync::Arc;
use std::time::Duration;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::diagnostic::{
    diagnostic_metrics, global_diagnostic_journal, record_diagnostic, DiagnosticOccurrence,
    MAX_DIAGNOSTIC_OCCURRENCES,
};
use rockstream_types::error_code::RS_2018;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use uuid::Uuid;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn start_gateway() -> (String, tokio::task::JoinHandle<()>) {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.rsplit(':').next().unwrap()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

fn simple_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<Option<String>>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn occurrence(id: u128) -> DiagnosticOccurrence {
    DiagnosticOccurrence::new(
        RS_2018,
        Uuid::from_u128(id),
        [("view".to_string(), "orders_mv".to_string())],
        Some(Duration::from_millis(250)),
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn show_diagnostic_simple_and_extended_exact_rows() {
    let _guard = TEST_LOCK.lock().await;
    let (addr, _handle) = start_gateway().await;
    let client = connect(&addr).await;

    let simple = simple_rows(
        client
            .simple_query("SHOW DIAGNOSTIC RS-2018;")
            .await
            .unwrap(),
    );
    let extended = client
        .query("SHOW DIAGNOSTIC RS-2018", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (0..row.len())
                .map(|index| Some(row.get::<usize, String>(index)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected = vec![vec![
        Some("RS-2018".to_string()),
        Some("session.max_staleness_exceeded".to_string()),
        Some("Published frontier exceeded the session max_staleness bound; query proceeded".to_string()),
        Some("WARN".to_string()),
        Some("01000".to_string()),
        Some("NonRetryable".to_string()),
        Some("Increase rockstream.max_staleness, reduce publish lag, or switch to session_wait_for mode.".to_string()),
        Some("rs-2018".to_string()),
    ]];
    assert_eq!(simple, expected);
    assert_eq!(extended, expected);
}

#[tokio::test]
async fn show_diagnostics_limit_and_eviction_exact_rows() {
    let _guard = TEST_LOCK.lock().await;
    global_diagnostic_journal().lock().clear();
    for id in 0..=MAX_DIAGNOSTIC_OCCURRENCES {
        record_diagnostic(occurrence(id as u128 + 1));
    }
    let (addr, _handle) = start_gateway().await;
    let client = connect(&addr).await;
    let rows = simple_rows(
        client
            .simple_query("SHOW DIAGNOSTICS LIMIT 1;")
            .await
            .unwrap(),
    );
    assert_eq!(rows, vec![vec![
        Some("RS-2018".to_string()),
        Some("session.max_staleness_exceeded".to_string()),
        Some("Published frontier exceeded the session max_staleness bound; query proceeded".to_string()),
        Some("WARN".to_string()),
        Some("01000".to_string()),
        Some("NonRetryable".to_string()),
        Some("Published frontier exceeded the session max_staleness bound; query proceeded (view=orders_mv)".to_string()),
        Some(Uuid::from_u128((MAX_DIAGNOSTIC_OCCURRENCES + 1) as u128).to_string()),
        Some(r#"{"view":"orders_mv"}"#.to_string()),
        Some("250".to_string()),
        None,
        None,
        None,
        None,
        Some("Increase rockstream.max_staleness, reduce publish lag, or switch to session_wait_for mode.".to_string()),
        Some("rs-2018".to_string()),
    ]]);
    assert_eq!(
        diagnostic_metrics().rockstream_diagnostic_occurrences_retained,
        MAX_DIAGNOSTIC_OCCURRENCES
    );
}

#[tokio::test]
async fn show_diagnostics_invalid_limit_returns_exact_catalog_error() {
    let _guard = TEST_LOCK.lock().await;
    let (addr, _handle) = start_gateway().await;
    let client = connect(&addr).await;
    let error = client
        .simple_query("SHOW DIAGNOSTICS LIMIT nope;")
        .await
        .unwrap_err();
    let db_error = error.as_db_error().unwrap();
    assert_eq!(db_error.severity(), "ERROR");
    assert_eq!(db_error.code().code(), "42601");
    assert_eq!(
        db_error.message(),
        "[RS-1012] sql.parse_error: SHOW DIAGNOSTICS LIMIT requires a non-negative integer. next_steps: Check SQL syntax; see docs/language-features.md for the supported SQL subset."
    );
}
