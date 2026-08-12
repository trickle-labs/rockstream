//! v0.52 Slice 3 — ALTER SOURCE REPLAY / DISMISS DEAD_LETTER_QUEUE DDL Command Tests.

#![allow(clippy::await_holding_lock)]

use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
};
use rockstream_types::dlq::{get_global_dlq, quarantine_record};

struct DummyViewReader;
#[async_trait::async_trait]
impl ViewReader for DummyViewReader {
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

async fn start_test_server() -> (
    std::net::SocketAddr,
    Arc<CatalogStubs>,
    tokio::task::JoinHandle<()>,
) {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(DummyViewReader);
    let server =
        GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog.clone(), view_reader);
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr, catalog, handle)
}

static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[tokio::test]
async fn test_alter_source_replay_and_dismiss_dlq() {
    let _guard = TEST_LOCK.lock();
    get_global_dlq().lock().clear();

    let (addr, catalog, _handle) = start_test_server().await;

    // Register source first
    let mut opts = std::collections::HashMap::new();
    opts.insert("topic".to_string(), "orders".to_string());
    let source = rockstream_gateway::catalog_stubs::CatalogSourceEntry {
        name: "orders_src".to_string(),
        table_name: None,
        source_type: "kafka".to_string(),
        options: opts,
        format: "json".to_string(),
        status: "OK".to_string(),
        live_offset: "0".to_string(),
        live_lag: 0,
    };
    catalog.add_source(source);

    quarantine_record("orders_src", "100", "RS-1003", "decode fail 1", b"bad1");
    quarantine_record("orders_src", "101", "RS-1003", "decode fail 2", b"bad2");

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Execute REPLAY
    let res = client
        .execute("ALTER SOURCE orders_src REPLAY DEAD_LETTER_QUEUE", &[])
        .await
        .unwrap();

    assert_eq!(res, 2);

    {
        let dlq = get_global_dlq().lock();
        assert_eq!(dlq[0].replay_attempt, 1);
        assert_eq!(dlq[1].replay_attempt, 1);
    }

    // Execute DISMISS
    let res_dismiss = client
        .execute(
            "ALTER SOURCE orders_src DISMISS DEAD_LETTER_QUEUE WHERE error_code = 'RS-1003'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(res_dismiss, 2);

    {
        let dlq = get_global_dlq().lock();
        assert_eq!(dlq.len(), 0);
    }

    get_global_dlq().lock().clear();
}

#[tokio::test]
async fn test_alter_source_nonexistent_returns_rs4009() {
    let _guard = TEST_LOCK.lock();
    get_global_dlq().lock().clear();

    let (addr, _catalog, _handle) = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=rockstream dbname=test sslmode=disable",
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let err = client
        .execute("ALTER SOURCE nonexistent REPLAY DEAD_LETTER_QUEUE", &[])
        .await
        .unwrap_err();

    assert!(
        format!("{err:?}").contains("RS-4009"),
        "expected RS-4009, got: {:?}",
        err
    );
}
