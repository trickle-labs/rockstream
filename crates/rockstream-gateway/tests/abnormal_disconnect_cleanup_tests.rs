//! v0.51.6 Slice 2 proof test: abnormal (raw-TCP-level) disconnect cleanup.
//!
//! Roadmap Proof claim: "A TC test that opens many connections and kills
//! them at the TCP level (no graceful close) shows zero growth in
//! server-side session-state memory after cleanup runs."
//!
//! Per the "TC" (raw-socket) test convention established in v0.51.5's
//! `gateway_tls_tests.rs`, connections here are built from raw
//! `tokio::net::TcpStream`s speaking the pgwire wire format directly (no
//! `tokio-postgres` client), so a connection can be dropped/`shutdown()`
//! without ever sending a `Terminate` ('X') message — a true TCP-level kill,
//! not a graceful disconnect.

use std::sync::Arc;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::{ConnectionStateTotals, GatewayServer},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

/// Build a PostgreSQL StartupMessage (protocol 3.0) for user/dbname.
fn build_startup_message(user: &str, db: &str) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&196608u32.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(db.as_bytes());
    body.push(0);
    body.push(0);
    let len = (body.len() + 4) as u32;
    let mut msg = len.to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    msg
}

/// Build a Parse ('P') message with no parameter type hints.
fn build_parse_message(stmt_name: &str, query: &str) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(stmt_name.as_bytes());
    body.push(0);
    body.extend_from_slice(query.as_bytes());
    body.push(0);
    body.extend_from_slice(&0u16.to_be_bytes()); // num param type oids
    let len = (body.len() + 4) as u32;
    let mut msg = vec![b'P'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Build a Bind ('B') message binding `portal_name` to `stmt_name` with no
/// parameters and default format codes.
fn build_bind_message(portal_name: &str, stmt_name: &str) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(portal_name.as_bytes());
    body.push(0);
    body.extend_from_slice(stmt_name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0u16.to_be_bytes()); // num param format codes
    body.extend_from_slice(&0u16.to_be_bytes()); // num params
    body.extend_from_slice(&0u16.to_be_bytes()); // num result format codes
    let len = (body.len() + 4) as u32;
    let mut msg = vec![b'B'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Build a Sync ('S') message.
fn build_sync_message() -> Vec<u8> {
    vec![b'S', 0, 0, 0, 4]
}

async fn read_u8(stream: &mut TcpStream) -> u8 {
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf).await.unwrap();
    buf[0]
}

async fn read_u32_be(stream: &mut TcpStream) -> u32 {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    u32::from_be_bytes(buf)
}

/// Read and discard messages until a ReadyForQuery ('Z') is received.
async fn drain_until_ready(stream: &mut TcpStream) {
    loop {
        let msg_type = read_u8(stream).await;
        let len = read_u32_be(stream).await as usize;
        let body_len = len - 4;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut body).await.unwrap();
        }
        if msg_type == b'Z' {
            break;
        }
    }
}

/// Open a raw pgwire connection, PREPARE `n_stmts` named statements and BIND
/// `n_portals` named portals against them (no `DISCARD ALL`), returning the
/// live `TcpStream` so the caller can kill it at the TCP level.
async fn open_connection_with_state(port: u16, n_stmts: usize, n_portals: usize) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    drain_until_ready(&mut stream).await;

    for i in 0..n_stmts {
        let stmt_name = format!("s{i}");
        stream
            .write_all(&build_parse_message(&stmt_name, "SELECT 1"))
            .await
            .unwrap();
        stream.write_all(&build_sync_message()).await.unwrap();
        drain_until_ready(&mut stream).await;
    }
    for i in 0..n_portals {
        let portal_name = format!("p{i}");
        let stmt_name = format!("s{}", i % n_stmts.max(1));
        stream
            .write_all(&build_bind_message(&portal_name, &stmt_name))
            .await
            .unwrap();
        stream.write_all(&build_sync_message()).await.unwrap();
        drain_until_ready(&mut stream).await;
    }
    stream
}

/// Opening N connections, each PREPAREing several statements and BINDing
/// portals with no `DISCARD ALL`, then killing each connection at the raw
/// TCP level (dropping the socket — no `Terminate` message), must leave all
/// five per-connection state maps (prepared statements, portals, portal
/// states, sessions, write buffers) back at their pre-connections baseline.
#[tokio::test]
async fn tcp_level_kill_of_many_connections_shows_zero_session_state_growth() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let handler = server.handler().clone();
    let baseline: ConnectionStateTotals = handler.connection_state_totals();

    let (local_addr, _bg_handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    const N_CONN: usize = 20;
    for _ in 0..N_CONN {
        let stream = open_connection_with_state(port, 5, 3).await;
        // Kill at the raw-socket level: drop the TcpStream directly, with
        // no pgwire Terminate ('X') message ever sent. This is exactly the
        // abnormal-disconnect path v0.51.6 Slice 2 must clean up (as
        // opposed to a graceful client close / DISCARD ALL).
        drop(stream);
    }

    // Give the server's connection tasks time to observe EOF/I-O error and
    // run the unconditional post-`process_socket` cleanup.
    let mut totals_after = handler.connection_state_totals();
    for _ in 0..50 {
        if totals_after == baseline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        totals_after = handler.connection_state_totals();
    }

    assert_eq!(
        totals_after, baseline,
        "expected all per-connection state maps to return to baseline \
         {baseline:?} after {N_CONN} abnormal (TCP-level-killed) \
         disconnects, got {totals_after:?}"
    );
}
