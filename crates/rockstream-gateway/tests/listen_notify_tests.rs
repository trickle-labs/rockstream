//! v0.41 Slice 10 — LISTEN/NOTIFY proof tests.
//!
//! P1-P4 run without --features testcontainers (in-process gateway only).
//! P5 (psycopg3) is gated under #[cfg(feature = "testcontainers")].

use std::sync::Arc;

use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::{catalog_stubs::CatalogStubs, GatewayError, GatewayServer};
use tokio_postgres::NoTls;

// ── Shared helpers ─────────────────────────────────────────────────────────────

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

async fn start_gateway_noop() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog, Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle)
}

/// Connect, driving Connection in background; returns Client only.
async fn connect_bg(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// Connect and drive Connection, sending each AsyncMessage to a channel.
async fn connect_with_notifications(
    port: u16,
) -> (
    tokio_postgres::Client,
    tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::Notification>,
) {
    let (client, mut conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let msg = futures::future::poll_fn(|cx| conn.poll_message(cx)).await;
            match msg {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    let _ = tx.send(n);
                }
                Some(Ok(_)) => {} // Notice or other
                Some(Err(_)) | None => break,
            }
        }
    });

    (client, rx)
}

// ── S10/P1 — LISTEN/NOTIFY roundtrip ──────────────────────────────────────────

/// P1 green gate: connection A LISTENs, B NOTIFYs, A's next query delivers
/// the NotificationResponse.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_listen_notify_roundtrip() {
    let (port, _handle) = start_gateway_noop().await;

    let (client_a, mut notif_rx) = connect_with_notifications(port).await;
    let client_b = connect_bg(port).await;

    // A LISTENs
    client_a
        .simple_query("LISTEN events")
        .await
        .expect("LISTEN failed");

    // B NOTIFYs
    client_b
        .simple_query("NOTIFY events, 'hello'")
        .await
        .expect("NOTIFY failed");

    // A issues a query to trigger notification delivery.
    client_a
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");

    // Await notification with timeout.
    let received = tokio::time::timeout(tokio::time::Duration::from_secs(2), notif_rx.recv()).await;

    let notif = received
        .expect("timed out waiting for notification")
        .expect("notification channel closed");

    assert_eq!(
        notif.channel(),
        "events",
        "expected channel 'events', got '{}'",
        notif.channel()
    );
    assert_eq!(
        notif.payload(),
        "hello",
        "expected payload 'hello', got '{}'",
        notif.payload()
    );
}

// ── S10/P2 — Transactional NOTIFY deferred to COMMIT ──────────────────────────

/// P2 green gate: NOTIFY inside BEGIN is buffered; connection A gets nothing
/// until COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transactional_notify_deferred() {
    let (port, _handle) = start_gateway_noop().await;

    let (client_a, mut notif_rx) = connect_with_notifications(port).await;
    let client_b = connect_bg(port).await;

    client_a
        .simple_query("LISTEN news")
        .await
        .expect("LISTEN failed");

    client_b.simple_query("BEGIN").await.expect("BEGIN failed");
    client_b
        .simple_query("NOTIFY news, 'draft'")
        .await
        .expect("NOTIFY inside BEGIN failed");

    // A queries — should NOT get notification yet (tx not committed).
    client_a
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");

    // Give a brief moment to see if a premature notification arrives.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let premature = notif_rx.try_recv();
    assert!(
        premature.is_err(),
        "notification should NOT be delivered before COMMIT, but got: {premature:?}"
    );

    // COMMIT — notification should now be buffered in A's outbox.
    client_b
        .simple_query("COMMIT")
        .await
        .expect("COMMIT failed");

    // A queries again to trigger delivery.
    client_a
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 after commit failed");

    let received = tokio::time::timeout(tokio::time::Duration::from_secs(2), notif_rx.recv()).await;

    let notif = received
        .expect("timed out waiting for notification after COMMIT")
        .expect("notification channel closed");

    assert_eq!(notif.channel(), "news");
    assert_eq!(notif.payload(), "draft");
}

// ── S10/P3 — ROLLBACK discards transactional NOTIFY ───────────────────────────

/// P3 green gate: ROLLBACK discards transactional NOTIFYs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rollback_discards_notify() {
    let (port, _handle) = start_gateway_noop().await;

    let (client_a, mut notif_rx) = connect_with_notifications(port).await;
    let client_b = connect_bg(port).await;

    client_a
        .simple_query("LISTEN news")
        .await
        .expect("LISTEN failed");

    client_b.simple_query("BEGIN").await.expect("BEGIN failed");
    client_b
        .simple_query("NOTIFY news, 'draft'")
        .await
        .expect("NOTIFY inside BEGIN failed");
    client_b
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");

    // A queries — should NOT get notification (tx was rolled back).
    client_a
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let not_received = notif_rx.try_recv();
    assert!(
        not_received.is_err(),
        "notification should NOT be delivered after ROLLBACK, but got: {not_received:?}"
    );
}

// ── S10/P4 — UNLISTEN stops delivery ──────────────────────────────────────────

/// P4 green gate: UNLISTEN stops notification delivery for the channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unlisten_stops_delivery() {
    let (port, _handle) = start_gateway_noop().await;

    let (client_a, mut notif_rx) = connect_with_notifications(port).await;
    let client_b = connect_bg(port).await;

    client_a
        .simple_query("LISTEN chan")
        .await
        .expect("LISTEN failed");
    client_a
        .simple_query("UNLISTEN chan")
        .await
        .expect("UNLISTEN failed");

    client_b
        .simple_query("NOTIFY chan, 'msg'")
        .await
        .expect("NOTIFY failed");

    // A queries — notification should not arrive (unlistened).
    client_a
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let not_received = notif_rx.try_recv();
    assert!(
        not_received.is_err(),
        "notification should NOT be delivered after UNLISTEN, but got: {not_received:?}"
    );
}

// ── S10/P5 — psycopg3 LISTEN/NOTIFY (testcontainers) ─────────────────────────

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn test_psycopg3_listen_notify() {
    use testcontainers::{clients::Cli, images::generic::GenericImage};

    let (port, _handle) = start_gateway_noop().await;

    let docker = Cli::default();
    let script = format!(
        "pip install psycopg -q && python3 -c \"\
import psycopg, threading, time\n\
conn = psycopg.connect(host='host.docker.internal', port={port}, user='test', dbname='test', autocommit=True)\n\
conn.execute('LISTEN events')\n\
def notify():\n\
    time.sleep(0.5)\n\
    c2 = psycopg.connect(host='host.docker.internal', port={port}, user='test', dbname='test', autocommit=True)\n\
    c2.execute(\\\"NOTIFY events, 'from-psycopg3'\\\")\n\
    c2.close()\n\
t = threading.Thread(target=notify)\n\
t.start()\n\
gen = conn.notifies(timeout=5)\n\
n = next(gen)\n\
assert n.channel == 'events' and n.payload == 'from-psycopg3', f'got {{n}}'\n\
print('OK')\n\
\""
    );
    let python_image = GenericImage::new("python", "3.11-slim")
        .with_entrypoint("bash")
        .with_cmd(vec!["-c", &script])
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout("OK"));

    let _container = docker.run(python_image);
}
