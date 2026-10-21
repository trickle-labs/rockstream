//! Regression for malformed PostgreSQL wire frame handling (issue #227).
//! The legacy pgwire 0.28 decoder must reject these frames without panicking
//! or preventing a subsequent legitimate client from using the gateway.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use object_store::memory::InMemory;
use pgwire::messages::{startup::Startup, Message, PgWireFrontendMessage};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
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
    let result = std::panic::catch_unwind(|| {
        PgWireFrontendMessage::decode(&mut BytesMut::from(bad_copy))
    });
    assert!(result.is_ok(), "malformed CopyData must not panic");
    assert!(result.unwrap().is_err(), "malformed CopyData must fail");

    // The original 39-byte seed revealed an independent unterminated C-string
    // panic in the startup decoder, before libFuzzer minimized the CopyData
    // framing underflow down to five bytes.
    let original = hex::decode(
        "00000027000300007573657200726f636b73747265616500646174616261007227000300007573",
    )
    .unwrap();
    let result = std::panic::catch_unwind(|| {
        Startup::decode(&mut BytesMut::from(original.as_slice()))
    });
    assert!(result.is_ok(), "malformed Startup must not panic");
    assert!(result.unwrap().is_err(), "malformed Startup must fail");

    let mut valid_empty_query = BytesMut::from(&b"Q\x00\x00\x00\x05\x00"[..]);
    assert!(matches!(
        PgWireFrontendMessage::decode(&mut valid_empty_query),
        Ok(Some(PgWireFrontendMessage::Query(_)))
    ));
}
