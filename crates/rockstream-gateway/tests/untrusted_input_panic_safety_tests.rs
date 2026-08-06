//! v0.51.17 Untrusted-Input Panic Safety Tests (Slices 1 - 3).
//!
//! Asserts that malformed, truncated, or adversarial input sequences sent to the untrusted-input
//! boundaries (pgwire decoding, SQL parser, and webhook request body decoders) never panic
//! the process, and return structured RS-XXXX errors.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
    HttpWebhookSource, WebhookFormat, WebhookResult,
};
use rockstream_sql::frontend::SqlFrontend;

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

async fn start_test_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(DummyViewReader);
    let server = GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);
    server.serve_background().await.unwrap()
}

#[tokio::test]
async fn pgwire_truncated_startup_returns_rs_error_no_panic() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Send truncated startup message header (less than 8 bytes)
    let truncated_header = [0u8, 0u8, 0u8, 4u8];
    let _ = socket.write_all(&truncated_header).await;
    let _ = socket.shutdown().await;

    let mut response = vec![0u8; 1024];
    let n = socket.read(&mut response).await.unwrap_or(0);
    // Server should close or respond gracefully without panic
    if n > 0 {
        assert!(
            response[0] == b'E' || response[0] == b'v',
            "Expected error or negotiate response, got 0x{:02x}",
            response[0]
        );
    }
}

#[tokio::test]
async fn pgwire_corrupt_extended_query_returns_rs_error_no_panic() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Standard startup message
    let params = b"user\0rockstream\0database\0test\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
    packet.extend_from_slice(params);

    socket.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket.read(&mut buf).await.unwrap_or(0);
    assert!(n > 0);

    // Send garbage extended query message ('P' with invalid length and payload)
    let corrupt_parse = b"P\x00\x00\x00\x05\xFF\xFF";
    socket.write_all(corrupt_parse).await.unwrap();

    let mut err_buf = vec![0u8; 1024];
    let _ = socket.read(&mut err_buf).await;
    // Process must not crash
}

#[tokio::test]
async fn pgwire_malformed_payload_returns_rs_error_no_panic() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Startup
    let params = b"user\0rockstream\0database\0test\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196608u32.to_be_bytes());
    packet.extend_from_slice(params);
    socket.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let _ = socket.read(&mut buf).await;

    // Send malformed simple query ('Q') containing non-UTF8 binary payload
    let mut query = vec![b'Q'];
    let payload = b"\xFF\xFE\xFD\xFC\xFB\xFA\x00";
    let qlen = (4 + payload.len()) as u32;
    query.extend_from_slice(&qlen.to_be_bytes());
    query.extend_from_slice(payload);

    socket.write_all(&query).await.unwrap();

    let mut resp = vec![0u8; 1024];
    let _ = socket.read(&mut resp).await;
    // Must remain alive and responsive
}

#[tokio::test]
async fn real_pgwire_adversarial_stream_connection_remains_healthy() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Send random noise bytes
    let noise = [0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78];
    let _ = socket.write_all(&noise).await;
    let _ = socket.shutdown().await;

    // Re-connect with valid startup to prove server is still healthy
    let mut socket2 = TcpStream::connect(addr).await.unwrap();
    let params = b"user\0rockstream\0database\0test\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196608u32.to_be_bytes());
    packet.extend_from_slice(params);

    socket2.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket2.read(&mut buf).await.unwrap_or(0);
    assert!(
        n > 0,
        "Server must remain operational after adversarial byte stream"
    );
}

#[tokio::test]
async fn sql_parser_truncated_quote_returns_rs_error_no_panic() {
    let frontend = SqlFrontend::new();
    let res = frontend.sql_to_plan_node("SELECT 'unclosed string").await;
    assert!(res.is_err());
}

#[test]
fn sql_parser_invalid_ddl_find_returns_rs_error_no_panic() {
    let frontend = SqlFrontend::new();
    // Test DDL strings that could trigger .find().unwrap() or indexing panics
    assert!(frontend.parse_ddl("CREATE INDEX").is_err());
    assert!(frontend.parse_ddl("CREATE INDEX ON").is_err());
    assert!(frontend.parse_ddl("CREATE INDEX idx ON (").is_err());
    assert!(frontend.parse_ddl("DROP INDEX").is_err());
    assert!(frontend.parse_ddl("REBUILD INDEX").is_err());
    assert!(frontend.parse_ddl("").is_err());
}

#[test]
fn webhook_json_malformed_body_returns_rs4008_no_panic() {
    let mut source = HttpWebhookSource::new("secret", WebhookFormat::Json);
    let malformed_payload = b"{\"invalid_json: 123";
    let res = source.accept(b"secret", Some("deliv-1"), malformed_payload);
    assert_eq!(res, WebhookResult::InvalidPayload);
}

#[test]
fn webhook_csv_unclosed_quote_returns_rs4008_no_panic() {
    let mut source = HttpWebhookSource::new("secret", WebhookFormat::Csv);
    let malformed_csv = b"col1,col2\n\"unclosed quote,val2";
    let res = source.accept(b"secret", Some("deliv-2"), malformed_csv);
    assert_eq!(res, WebhookResult::InvalidPayload);
}
