//! v0.51.5 Slice 3/4 proof tests: gateway-facing TLS termination and mTLS
//! client-certificate authentication.
//!
//! Ground rule from `.claude/v0.51.5-plan.md`: `test_ssl_negotiation_downgrade`
//! (in `gateway_extended_query_tests.rs`) must keep passing unmodified, proving
//! that a gateway started *without* `--tls-cert-path` still refuses SSLRequest
//! with `'N'` exactly as before. These tests cover the opposite path: a gateway
//! started *with* TLS configured.

use std::io::Write;
use std::sync::Arc;

use object_store::memory::InMemory;
use rcgen::{CertificateParams, DnType, IsCa, KeyPair};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs, server::GatewayServer, view_reader::ViewReadStrategy,
    view_reader::ViewReader, GatewayError,
};
use rockstream_storage::ShardDb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// rustls 0.23 requires a process-level `CryptoProvider` to be installed
/// before any `ClientConfig`/`ServerConfig` is built. `tokio-rustls`'s default
/// features already pull in `aws-lc-rs` (matching pgwire's own default
/// backend), so install it once per test process; a second call is harmless
/// (ignored) if a previous test already installed it.
fn ensure_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

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

/// A self-signed CA plus one leaf certificate signed by it, materialized as
/// temp PEM files so they can be passed to `GatewayServer::with_tls` /
/// `tokio_postgres_rustls` by path.
struct TestPki {
    _dir: tempfile::TempDir,
    ca_cert_pem: String,
    server_cert_path: std::path::PathBuf,
    server_key_path: std::path::PathBuf,
}

struct ClientCert {
    cert_pem: String,
    key_pem: String,
}

fn write_file(dir: &std::path::Path, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path
}

fn make_ca() -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "RockStream Test CA");
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert, key_pair)
}

fn make_leaf(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    cn: &str,
    sans: Vec<String>,
) -> (String, String) {
    let mut params = CertificateParams::new(sans).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, ca_cert, ca_key).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

/// Build a CA + server cert (for `127.0.0.1`/`localhost`) rooted in a fresh
/// temp dir, returning paths suitable for `GatewayServer::with_tls`.
fn build_server_pki() -> TestPki {
    let dir = tempfile::tempdir().unwrap();
    let (ca_cert, ca_key) = make_ca();
    let ca_cert_pem = ca_cert.pem();
    let _ca_cert_path = write_file(dir.path(), "ca.pem", &ca_cert_pem);

    let (server_cert_pem, server_key_pem) = make_leaf(
        &ca_cert,
        &ca_key,
        "localhost",
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
    );
    let server_cert_path = write_file(dir.path(), "server.pem", &server_cert_pem);
    let server_key_path = write_file(dir.path(), "server-key.pem", &server_key_pem);

    TestPki {
        _dir: dir,
        ca_cert_pem,
        server_cert_path,
        server_key_path,
    }
}

fn rustls_client_config_no_client_auth(ca_cert_pem: &str) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert.unwrap()).unwrap();
    }
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn rustls_client_config_with_cert(ca_cert_pem: &str, client: &ClientCert) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut cert_reader = std::io::BufReader::new(client.cert_pem.as_bytes());
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .map(|c| c.unwrap())
        .collect();
    let mut key_reader = std::io::BufReader::new(client.key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap();
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .unwrap()
}

async fn connect_tls(
    addr: &str,
    config: rustls::ClientConfig,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let port = addr.split(':').next_back().unwrap();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test sslmode=require"),
        tls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    Ok(client)
}

async fn new_shard_db(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

// ── gateway_with_tls_starts_and_binds ────────────────────────────────────────

#[tokio::test]
async fn gateway_with_tls_starts_and_binds() {
    ensure_crypto_provider();
    let pki = build_server_pki();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    )
    .with_tls(&pki.server_cert_path, &pki.server_key_path, None)
    .expect("gateway with TLS should start");
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    assert!(local_addr.port() > 0);
}

// ── mtls_auth_without_ca_cert_path_fails_to_start ────────────────────────────

#[tokio::test]
async fn mtls_auth_without_ca_cert_path_fails_to_start() {
    ensure_crypto_provider();
    let pki = build_server_pki();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let shard_db = new_shard_db("mtls-no-ca-shard").await;
    let server = GatewayServer::with_shard_db_and_mtls_auth(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    )
    .with_tls(&pki.server_cert_path, &pki.server_key_path, None);

    match server {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("RS-2403"),
                "expected RS-2403 mtls_requires_ca_cert error, got: {msg}"
            );
        }
        Ok(_) => panic!("expected mTLS without CA cert path to fail fast"),
    }
}

// ── tls_handshake_completes_and_query_succeeds_sslmode_require ───────────────

#[tokio::test]
async fn tls_handshake_completes_and_query_succeeds_sslmode_require() {
    ensure_crypto_provider();
    let pki = build_server_pki();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    )
    .with_tls(&pki.server_cert_path, &pki.server_key_path, None)
    .unwrap();
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let config = rustls_client_config_no_client_auth(&pki.ca_cert_pem);
    let client = connect_tls(&local_addr.to_string(), config)
        .await
        .expect("TLS handshake + startup should succeed");
    let rows = client.simple_query("SELECT 1").await.unwrap();
    let found_row = rows.iter().any(|m| {
        matches!(
            m,
            tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some("1")
        )
    });
    assert!(found_row, "expected a result row containing \"1\"");
}

// ── raw_socket_bytes_after_sslrequest_are_encrypted_not_plaintext ────────────

#[tokio::test]
async fn raw_socket_bytes_after_sslrequest_are_encrypted_not_plaintext() {
    ensure_crypto_provider();
    let pki = build_server_pki();
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    )
    .with_tls(&pki.server_cert_path, &pki.server_key_path, None)
    .unwrap();
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47];
    socket.write_all(&ssl_request).await.unwrap();

    let mut response = [0u8; 1];
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(
        response[0], b'S',
        "gateway with TLS configured must accept SSLRequest with 'S'"
    );

    // Send a plaintext StartupMessage directly (no TLS handshake) and confirm
    // the gateway does NOT respond with a plaintext AuthenticationOk/ReadyForQuery
    // — it must instead be waiting for a TLS ClientHello, so either the write
    // is swallowed or the connection is closed, but plaintext protocol bytes
    // must never come back.
    let mut startup = Vec::new();
    startup.extend_from_slice(&[0u8; 4]); // length placeholder
    startup.extend_from_slice(&196608u32.to_be_bytes()); // protocol 3.0
    startup.extend_from_slice(b"user\0test\0\0");
    let len = (startup.len() as u32).to_be_bytes();
    startup[0..4].copy_from_slice(&len);
    socket.write_all(&startup).await.unwrap();

    let mut buf = [0u8; 64];
    let read_result =
        tokio::time::timeout(std::time::Duration::from_millis(300), socket.read(&mut buf)).await;
    match read_result {
        Ok(Ok(0)) => {} // connection closed: fine, proves plaintext wasn't accepted
        Ok(Ok(n)) => {
            // Whatever bytes came back must not be a plaintext pgwire message
            // (which always starts with an ASCII type byte like 'R', 'E', 'Z').
            let first = buf[0];
            assert!(
                !(first == b'R' || first == b'Z' || first == b'E'),
                "received plaintext pgwire bytes after SSLRequest was accepted: {:?}",
                &buf[..n]
            );
        }
        Ok(Err(_)) => {} // connection reset: also fine
        Err(_) => {}     // timed out waiting: also fine, proves no plaintext reply
    }
}

// ── mtls_end_to_end_authenticates_real_client_cert ───────────────────────────

#[tokio::test]
async fn mtls_end_to_end_authenticates_real_client_cert() {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (ca_cert, ca_key) = make_ca();
    let ca_cert_pem = ca_cert.pem();
    let ca_cert_path = write_file(dir.path(), "ca.pem", &ca_cert_pem);
    let (server_cert_pem, server_key_pem) = make_leaf(
        &ca_cert,
        &ca_key,
        "localhost",
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
    );
    let server_cert_path = write_file(dir.path(), "server.pem", &server_cert_pem);
    let server_key_path = write_file(dir.path(), "server-key.pem", &server_key_pem);
    let (client_cert_pem, client_key_pem) =
        make_leaf(&ca_cert, &ca_key, "alice@rockstream.test", vec![]);

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let shard_db = new_shard_db("mtls-e2e-shard").await;
    let server = GatewayServer::with_shard_db_and_mtls_auth(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    )
    .with_tls(&server_cert_path, &server_key_path, Some(&ca_cert_path))
    .expect("mTLS gateway with CA cert should start");
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let client_cert = ClientCert {
        cert_pem: client_cert_pem,
        key_pem: client_key_pem,
    };
    let config = rustls_client_config_with_cert(&ca_cert_pem, &client_cert);
    let client = connect_tls(&local_addr.to_string(), config)
        .await
        .expect("mTLS handshake with valid client cert should succeed");
    let rows = client.simple_query("SELECT 1").await.unwrap();
    let found_row = rows.iter().any(|m| {
        matches!(
            m,
            tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some("1")
        )
    });
    assert!(found_row, "expected a result row containing \"1\"");
}

// ── mtls_rejects_connection_with_no_client_cert ──────────────────────────────

#[tokio::test]
async fn mtls_rejects_connection_with_no_client_cert() {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (ca_cert, ca_key) = make_ca();
    let ca_cert_pem = ca_cert.pem();
    let ca_cert_path = write_file(dir.path(), "ca.pem", &ca_cert_pem);
    let (server_cert_pem, server_key_pem) = make_leaf(
        &ca_cert,
        &ca_key,
        "localhost",
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
    );
    let server_cert_path = write_file(dir.path(), "server.pem", &server_cert_pem);
    let server_key_path = write_file(dir.path(), "server-key.pem", &server_key_pem);

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let shard_db = new_shard_db("mtls-no-cert-shard").await;
    let server = GatewayServer::with_shard_db_and_mtls_auth(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    )
    .with_tls(&server_cert_path, &server_key_path, Some(&ca_cert_path))
    .unwrap();
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    // Client trusts the CA but presents no client certificate at all — the
    // server's mTLS verifier requires one, so the handshake itself must fail.
    let config = rustls_client_config_no_client_auth(&ca_cert_pem);
    let result = connect_tls(&local_addr.to_string(), config).await;
    assert!(
        result.is_err(),
        "connection without a client cert must be rejected by mTLS"
    );
}

// ── mtls_rejects_connection_with_untrusted_client_cert ───────────────────────

#[tokio::test]
async fn mtls_rejects_connection_with_untrusted_client_cert() {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let (ca_cert, ca_key) = make_ca();
    let ca_cert_pem = ca_cert.pem();
    let ca_cert_path = write_file(dir.path(), "ca.pem", &ca_cert_pem);
    let (server_cert_pem, server_key_pem) = make_leaf(
        &ca_cert,
        &ca_key,
        "localhost",
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
    );
    let server_cert_path = write_file(dir.path(), "server.pem", &server_cert_pem);
    let server_key_path = write_file(dir.path(), "server-key.pem", &server_key_pem);

    // A different, untrusted CA signs the client cert.
    let (other_ca_cert, other_ca_key) = make_ca();
    let (client_cert_pem, client_key_pem) =
        make_leaf(&other_ca_cert, &other_ca_key, "mallory", vec![]);

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let shard_db = new_shard_db("mtls-untrusted-cert-shard").await;
    let server = GatewayServer::with_shard_db_and_mtls_auth(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    )
    .with_tls(&server_cert_path, &server_key_path, Some(&ca_cert_path))
    .unwrap();
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let client_cert = ClientCert {
        cert_pem: client_cert_pem,
        key_pem: client_key_pem,
    };
    // Client trusts the real CA for the server cert, but its own client cert
    // is signed by an unrelated CA the server does not trust.
    let config = rustls_client_config_with_cert(&ca_cert_pem, &client_cert);
    let result = connect_tls(&local_addr.to_string(), config).await;
    assert!(
        result.is_err(),
        "connection with an untrusted client cert must be rejected by mTLS"
    );
}
