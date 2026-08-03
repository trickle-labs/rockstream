//! v0.51.7 Slice 1 — Wire Protocol 3.2 Negotiation & Handshake Downgrade Tests.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReader, GatewayError, GatewayServer,
};

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
async fn test_protocol_3_2_negotiation_reply() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Protocol 3.2 startup message:
    // ver: 196610 (0x00030002 -> major 3, minor 2)
    // params: user\0rockstream\0database\0test\0_pq_.client_encoding\0utf8\0\0
    let params = b"user\0rockstream\0database\0test\0_pq_.client_encoding\0utf8\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196610u32.to_be_bytes());
    packet.extend_from_slice(params);

    socket.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket.read(&mut buf).await.unwrap();
    assert!(n > 0, "Server closed connection unexpectedly");

    // First backend message must be NegotiateProtocolVersion ('v')
    assert_eq!(
        buf[0], b'v',
        "Expected NegotiateProtocolVersion ('v') response header, got byte 0x{:02x}",
        buf[0]
    );

    let msg_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let minor_ver = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
    let option_count = u32::from_be_bytes([buf[9], buf[10], buf[11], buf[12]]);

    assert_eq!(minor_ver, 0, "Server must downgrade to minor version 0");
    assert!(
        option_count >= 1,
        "Expected at least 1 unrecognized _pq_ option"
    );

    let neg_msg = &buf[..1 + msg_len];
    let neg_str = String::from_utf8_lossy(neg_msg);
    assert!(
        neg_str.contains("_pq_.client_encoding"),
        "NegotiateProtocolVersion must report unrecognized _pq_ option"
    );

    // After NegotiateProtocolVersion, the server should proceed with protocol 3.0 handshake
    let rest = &buf[1 + msg_len..n];
    if rest.is_empty() {
        let n2 = socket.read(&mut buf).await.unwrap();
        assert!(n2 > 0);
        assert_eq!(
            buf[0], b'R',
            "Handshake after 'v' must continue with Authentication ('R')"
        );
    } else {
        assert_eq!(
            rest[0], b'R',
            "Handshake after 'v' must continue with Authentication ('R')"
        );
    }
}

#[tokio::test]
async fn test_protocol_3_0_handshake_unaffected() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Protocol 3.0 startup message:
    // ver: 196608 (0x00030000 -> major 3, minor 0)
    let params = b"user\0rockstream\0database\0test\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196608u32.to_be_bytes());
    packet.extend_from_slice(params);

    socket.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket.read(&mut buf).await.unwrap();
    assert!(n > 0);

    // Protocol 3.0 client MUST NOT receive 'v'
    assert_ne!(
        buf[0], b'v',
        "Protocol 3.0 client must not receive NegotiateProtocolVersion"
    );
    assert_eq!(
        buf[0], b'R',
        "Protocol 3.0 client must receive Authentication ('R') directly"
    );
}

#[tokio::test]
async fn test_psql_18_e2e_handshake() {
    let (addr, _handle) = start_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Emulate psql 18.4 startup with protocol 3.2 (196610) and multiple _pq_. options
    let params = b"user\0rockstream\0database\0test\0_pq_.client_encoding\0utf8\0_pq_.protocol_version\x003.2\0\0";
    let len = (8 + params.len()) as u32;
    let mut packet = vec![];
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&196610u32.to_be_bytes());
    packet.extend_from_slice(params);

    socket.write_all(&packet).await.unwrap();

    let mut buf = vec![0u8; 2048];
    let n = socket.read(&mut buf).await.unwrap();
    assert!(n > 0);
    assert_eq!(
        buf[0], b'v',
        "psql 18.4 handshake must receive NegotiateProtocolVersion ('v')"
    );
}
