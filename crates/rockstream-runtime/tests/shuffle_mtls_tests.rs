use std::sync::Arc;

use parking_lot::RwLock;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ExchangeService};
use rockstream_test_support::pki::TestPki;
use rockstream_types::ids::WorkerId;
use std::collections::HashMap;

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_valid_cert_admitted() {
    let pki = TestPki::generate();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = ExchangeRegistry::new();
    let service = ExchangeService::new(registry).with_internal_tls(pki.worker_tls_config(1));

    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(1), addr.to_string());

    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.worker_tls_config(2));

    // Connect over mTLS from worker 2 to worker 1 and invoke RPC
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let req_stream =
        futures::stream::iter(Vec::<rockstream_runtime::exchange::proto::ShuffleFrame>::new());
    let res = client.shuffle_stream(req_stream).await;
    assert!(
        res.is_ok(),
        "mTLS RPC stream should succeed with valid certificates: {:?}",
        res.err()
    );

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_unauthenticated_rejected() {
    let pki = TestPki::generate();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = ExchangeRegistry::new();
    let service = ExchangeService::new(registry).with_internal_tls(pki.worker_tls_config(1));

    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(1), addr.to_string());

    // Plaintext client without mTLS
    let pool = ShuffleClientPool::new(peers);

    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let req_stream =
        futures::stream::iter(Vec::<rockstream_runtime::exchange::proto::ShuffleFrame>::new());
    let res = client.shuffle_stream(req_stream).await;
    assert!(
        res.is_err(),
        "Unauthenticated plaintext connection should be rejected"
    );

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_untrusted_rejected() {
    let pki = TestPki::generate();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = ExchangeRegistry::new();
    let service = ExchangeService::new(registry).with_internal_tls(pki.worker_tls_config(1));

    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(1), addr.to_string());

    // Client with untrusted certificate
    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.untrusted_worker_tls_config());

    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let req_stream =
        futures::stream::iter(Vec::<rockstream_runtime::exchange::proto::ShuffleFrame>::new());
    let res = client.shuffle_stream(req_stream).await;
    assert!(
        res.is_err(),
        "Untrusted certificate should be rejected during TLS handshake"
    );

    service.shutdown().await;
}
