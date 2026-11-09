use std::sync::Arc;

use parking_lot::RwLock;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ExchangeService};
use rockstream_test_support::pki::TestPki;
use rockstream_types::config::ExchangeConfig;
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
    let mut request = tonic::Request::new(req_stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let res = client.shuffle_stream(request).await;
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
    let mut request = tonic::Request::new(req_stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let res = client.shuffle_stream(request).await;
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
    let mut request = tonic::Request::new(req_stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let res = client.shuffle_stream(request).await;
    assert!(
        res.is_err(),
        "Untrusted certificate should be rejected during TLS handshake"
    );

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_rejects_non_worker_identity() {
    let pki = TestPki::generate();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let service =
        ExchangeService::new(ExchangeRegistry::new()).with_internal_tls(pki.worker_tls_config(1));
    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.cli_tls_config());
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let request_stream =
        futures::stream::iter(Vec::<rockstream_runtime::exchange::proto::ShuffleFrame>::new());
    let mut request = tonic::Request::new(request_stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );

    let status = match client.shuffle_stream(request).await {
        Err(status) => status,
        Ok(_) => panic!("CLI certificate must not be accepted as a worker peer"),
    };
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "RS-2401: worker shuffle requires a worker node identity"
    );

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_client_checks_target_worker_identity() {
    let pki = TestPki::generate();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let service =
        ExchangeService::new(ExchangeRegistry::new()).with_internal_tls(pki.worker_tls_config(2));
    service.start(addr).await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while tokio::net::TcpStream::connect(addr).await.is_err() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok(),
        "worker shuffle listener did not bind"
    );

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers)
        .with_config(ExchangeConfig {
            max_retries: 1,
            connect_timeout_ms: 250,
            ..Default::default()
        })
        .with_internal_tls(pki.worker_tls_config(1));
    let error = pool.get_client(WorkerId(1)).await.unwrap_err();
    assert_eq!(
        error,
        r#"RS-5003: failed to connect to worker WorkerId(1) after 1 attempts (timeout 250ms): tonic::transport::Error(Transport, ConnectError(Custom { kind: InvalidData, error: InvalidCertificate(NotValidForNameContext { expected: DnsName("worker-1"), presented: ["DnsName(\"localhost\")", "IpAddress(127.0.0.1)", "DnsName(\"worker-2\")"] }) })). Next steps: check peer reachability."#
    );

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_fails_closed_without_trust_roots() {
    let pki = TestPki::generate();
    let mut tls = pki.worker_tls_config(1);
    tls.ca_cert_path = None;
    let service = ExchangeService::new(ExchangeRegistry::new()).with_internal_tls(tls);
    let error = match service.start("127.0.0.1:0".parse().unwrap()).await {
        Err(error) => error,
        Ok(_) => panic!("mTLS server must reject missing trust roots before listen"),
    };
    assert_eq!(
        error,
        "RS-2410: client_auth_required is true but ca_cert_path is not set"
    );

    let service = ExchangeService::new(ExchangeRegistry::new());
    let error = match service.start("0.0.0.0:0".parse().unwrap()).await {
        Err(error) => error,
        Ok(_) => panic!("plaintext worker shuffle must not bind beyond loopback"),
    };
    assert_eq!(
        error,
        "RS-2410: plaintext worker shuffle is limited to loopback development"
    );
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_fails_closed_without_credentials() {
    let service =
        ExchangeService::new(ExchangeRegistry::new()).with_internal_tls(Default::default());
    let error = match service.start("127.0.0.1:0".parse().unwrap()).await {
        Err(error) => error,
        Ok(_) => panic!("configured worker mTLS must reject missing credentials before listen"),
    };
    assert_eq!(
        error,
        "RS-2410: worker shuffle TLS certificate and private key are required"
    );

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        "127.0.0.1:1".to_owned(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(Default::default());
    let error = pool.get_client(WorkerId(1)).await.unwrap_err();
    assert_eq!(
        error,
        "RS-2410: worker shuffle TLS certificate and private key are required"
    );

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        "192.0.2.10:50051".to_owned(),
    )])));
    let pool = ShuffleClientPool::new(peers);
    let error = pool.get_client(WorkerId(1)).await.unwrap_err();
    assert_eq!(
        error,
        "RS-2410: plaintext worker shuffle is limited to loopback development"
    );
}
