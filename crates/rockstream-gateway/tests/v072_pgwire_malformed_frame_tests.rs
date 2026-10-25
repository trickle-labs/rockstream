//! Regression for malformed PostgreSQL wire frame handling (issue #227).
//! The legacy pgwire 0.28 decoder must reject these frames without panicking
//! or preventing a subsequent legitimate client from using the gateway.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use object_store::memory::InMemory;
use pgwire::messages::{startup::Startup, Message, PgWireFrontendMessage};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    copy_state::COPY_IN_BUFFER_ROWS,
    server::{ConnectionStateTotals, GatewayHandler},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct EmptyViewReader;

#[async_trait::async_trait]
impl ViewReader for EmptyViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn authenticated_raw_socket(addr: SocketAddr) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    let mut startup = vec![0; 8];
    startup[4..8].copy_from_slice(&196_608i32.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0database\0test\0\0");
    let length = i32::try_from(startup.len()).unwrap();
    startup[0..4].copy_from_slice(&length.to_be_bytes());
    socket.write_all(&startup).await.unwrap();

    // PostgreSQL backend messages each contain a tag and a signed length
    // including the four-byte length field, but excluding the tag.
    for _ in 0..24 {
        let mut tag = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut tag))
            .await
            .unwrap()
            .unwrap();
        let mut size_bytes = [0u8; 4];
        socket.read_exact(&mut size_bytes).await.unwrap();
        let length = i32::from_be_bytes(size_bytes);
        assert!((4..=65_536).contains(&length), "invalid server frame size");
        let mut body = vec![0u8; length as usize - 4];
        socket.read_exact(&mut body).await.unwrap();
        if tag[0] == b'Z' {
            return socket;
        }
        assert_ne!(tag[0], b'E', "unexpected startup error");
    }
    panic!("gateway did not complete startup");
}

async fn send_bad_frame_and_confirm_disconnect(addr: SocketAddr, bad_frame: &[u8]) {
    let mut socket = authenticated_raw_socket(addr).await;
    socket.write_all(bad_frame).await.unwrap();
    let mut data = [0u8; 1];
    let result = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut data))
        .await
        .expect("malformed protocol frame must not leave a stuck connection");
    match result {
        Ok(0) | Err(_) => {}
        Ok(_) => panic!("malformed frame was unexpectedly accepted"),
    }
}

#[tokio::test]
async fn malformed_frames_fail_closed_and_next_client_still_works() {
    let store = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-pgwire-malformed-frame", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(EmptyViewReader),
        shard,
    );
    let (addr, task) = server.serve_background().await.unwrap();

    // Both inputs previously panic in the raw pgwire 0.28 decoder.
    send_bad_frame_and_confirm_disconnect(addr, b"d\x00\x00\x00\x00").await;
    send_bad_frame_and_confirm_disconnect(addr, b"Q\x00\x00\x00\x06xy").await;

    // Truncated Close/Describe must disconnect without panicking in the
    // authenticated production socket path (not just the raw decoder).
    send_bad_frame_and_confirm_disconnect(addr, b"C\x00\x00\x00\x04").await;
    send_bad_frame_and_confirm_disconnect(addr, b"D\x00\x00\x00\x04").await;

    // The bad clients must not poison global gateway availability.
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = client.simple_query("SELECT 1").await.unwrap();
    let rows: Vec<_> = response
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["1".to_string()]);
    connection_task.abort();
    task.abort();
}

#[test]
fn raw_decoder_rejects_original_and_minimized_fuzz_inputs_without_panics() {
    let bad_copy = &b"d\x00\x00\x00\x00"[..];
    let result =
        std::panic::catch_unwind(|| PgWireFrontendMessage::decode(&mut BytesMut::from(bad_copy)));
    assert!(result.is_ok(), "malformed CopyData must not panic");
    assert!(result.unwrap().is_err(), "malformed CopyData must fail");

    // The original 39-byte seed revealed an independent unterminated C-string
    // panic in the startup decoder, before libFuzzer minimized the CopyData
    // framing underflow down to five bytes.
    let original = hex::decode(
        "00000027000300007573657200726f636b73747265616500646174616261007227000300007573",
    )
    .unwrap();
    let result =
        std::panic::catch_unwind(|| Startup::decode(&mut BytesMut::from(original.as_slice())));
    assert!(result.is_ok(), "malformed Startup must not panic");
    assert!(result.unwrap().is_err(), "malformed Startup must fail");

    let mut valid_empty_query = BytesMut::from(&b"Q\x00\x00\x00\x05\x00"[..]);
    assert!(matches!(
        PgWireFrontendMessage::decode(&mut valid_empty_query),
        Ok(Some(PgWireFrontendMessage::Query(_)))
    ));
}

fn frontend_frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = vec![tag];
    frame.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

async fn read_backend_frame(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let tag = socket.read_u8().await.expect("backend message tag");
        let length = socket.read_u32().await.expect("backend frame length");
        assert!((4..=65_536).contains(&length), "invalid backend frame");
        let mut body = vec![0u8; length as usize - 4];
        socket.read_exact(&mut body).await.expect("backend frame body");
        (tag, body)
    })
    .await
    .expect("backend message timed out")
}

async fn enter_copy_in(addr: SocketAddr) -> TcpStream {
    let mut socket = authenticated_raw_socket(addr).await;
    socket
        .write_all(&frontend_frame(b'Q', b"COPY t FROM STDIN\0"))
        .await
        .unwrap();
    let (tag, _) = read_backend_frame(&mut socket).await;
    assert_eq!(
        tag, b'G',
        "must enter real COPY IN mode before injecting malformed data"
    );
    socket
}

async fn wait_for_clean_session(
    handler: &GatewayHandler,
    baseline: ConnectionStateTotals,
    baseline_rows: u64,
) {
    for _ in 0..50 {
        if handler.connection_state_totals() == baseline
            && COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed) == baseline_rows
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        handler.connection_state_totals(),
        baseline,
        "malformed COPY socket leaked per-connection state"
    );
    assert_eq!(
        COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed),
        baseline_rows,
        "malformed COPY socket leaked buffered-row accounting"
    );
}

async fn wait_for_one_buffered_row(baseline_rows: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed) == baseline_rows + 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("valid CopyData row must reach the production COPY buffer");
}

async fn expect_socket_close(socket: &mut TcpStream) {
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut byte))
        .await
        .expect("bad COPY client must not remain connected");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "malformed COPY client received unexpected server data: {read:?}"
    );
}

/// A parser abort must not strand a COPY session or its buffered-row gauge.
/// This runs against GatewayServer::serve_background and the production
/// pgwire::tokio::process_socket path, not the raw decoder harness alone.
#[tokio::test]
async fn malformed_copy_in_transport_cleans_session_and_keeps_gateway_healthy() {
    let store = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("v072-pgwire-copy-transport", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(EmptyViewReader),
        shard,
    );
    let handler = server.handler().clone();
    let baseline = handler.connection_state_totals();
    let baseline_rows = COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed);
    let (addr, task) = server.serve_background().await.unwrap();

    // Create the real COPY target through a normal PgWire client first.
    let (setup, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let setup_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    setup
        .simple_query("CREATE TABLE t (id TEXT, val TEXT)")
        .await
        .expect("COPY target must exist");
    drop(setup);
    tokio::time::timeout(Duration::from_secs(5), setup_task)
        .await
        .expect("setup socket must terminate")
        .expect("setup task panicked");
    wait_for_clean_session(&handler, baseline, baseline_rows).await;

    // Four cases exercise an active, authenticated COPY session without
    // CopyDone: malformed zero-length and negative-length messages, and EOF
    // midway through a framed CopyData payload. Each case first buffers a
    // valid row so leaked resource accounting cannot be masked.
    for bad_frame in [&b"d\x00\x00\x00\x00"[..], &b"d\xff\xff\xff\xff"[..]] {
        let mut socket = enter_copy_in(addr).await;
        socket
            .write_all(&frontend_frame(b'd', b"1\thello\n"))
            .await
            .unwrap();
        wait_for_one_buffered_row(baseline_rows).await;
        assert_eq!(
            handler.connection_state_totals().copy_states,
            baseline.copy_states + 1
        );
        socket.write_all(bad_frame).await.unwrap();
        expect_socket_close(&mut socket).await;
        drop(socket);
        wait_for_clean_session(&handler, baseline, baseline_rows).await;
    }

    // An incomplete length word and an incomplete declared body are NOT
    // malformed complete frames: the socket decoder must wait for more bytes
    // without consuming another message or emitting a premature response.
    for partial in [&b"d\x00\x00"[..], &b"d\x00\x00\x00\x0cabc"[..]] {
        let mut socket = enter_copy_in(addr).await;
        socket
            .write_all(&frontend_frame(b'd', b"2\tpending\n"))
            .await
            .unwrap();
        wait_for_one_buffered_row(baseline_rows).await;
        socket.write_all(partial).await.unwrap();
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), socket.read(&mut byte))
                .await
                .is_err(),
            "incomplete CopyData must wait for additional transport bytes"
        );
        // EOF turns the incomplete frame into a cleanly closed connection;
        // no CopyDone should flush the previously buffered row.
        socket.shutdown().await.unwrap();
        expect_socket_close(&mut socket).await;
        drop(socket);
        wait_for_clean_session(&handler, baseline, baseline_rows).await;
    }

    // A fragmented valid CopyData frame must remain buffered until it is
    // complete, then be dispatched exactly once. An EOF without CopyDone
    // must also release the buffered row.
    let mut socket = enter_copy_in(addr).await;
    let frame = frontend_frame(b'd', b"3\tfragmented\n");
    socket.write_all(&frame[..3]).await.unwrap();
    let mut byte = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.read(&mut byte))
            .await
            .is_err(),
        "partial header must not be decoded or cause a response"
    );
    socket.write_all(&frame[3..]).await.unwrap();
    wait_for_one_buffered_row(baseline_rows).await;
    socket.shutdown().await.unwrap();
    expect_socket_close(&mut socket).await;
    drop(socket);
    wait_for_clean_session(&handler, baseline, baseline_rows).await;

    // Failed COPY clients cannot poison availability or leave buffered rows.
    // A new normal client must still be able to query the gateway.
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let response = client.simple_query("SELECT 1").await.unwrap();
    assert!(response.iter().any(|message| matches!(
        message,
        SimpleQueryMessage::Row(row) if row.get(0) == Some("1")
    )));
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), connection_task)
        .await
        .expect("healthy client socket must close")
        .expect("healthy client task panicked");
    wait_for_clean_session(&handler, baseline, baseline_rows).await;
    task.abort();
}
