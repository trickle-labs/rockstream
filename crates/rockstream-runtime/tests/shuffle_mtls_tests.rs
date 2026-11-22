use std::sync::Arc;

use futures::StreamExt;
use parking_lot::RwLock;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::{ExchangeFrame, ShuffleFrame};
use rockstream_runtime::exchange::service::{ExchangeRegistry, ExchangeService};
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, SecurityAuditClass, SecurityAuditOutcome,
};
use rockstream_types::config::ExchangeConfig;
use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation, WorkerMessage,
};
use std::collections::HashMap;
use tokio::sync::mpsc;

fn audit_transport() -> (
    mpsc::Sender<WorkerMessage>,
    mpsc::Receiver<WorkerMessage>,
    rockstream_runtime::exchange::service::SecurityAuditWaiters,
) {
    let (sender, receiver) = mpsc::channel(32);
    let waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    (sender, receiver, waiters)
}

fn with_auto_ack_audit(registry: ExchangeRegistry) -> ExchangeRegistry {
    let (sender, mut receiver, waiters) = audit_transport();
    let ack_waiters = waiters.clone();
    tokio::spawn(async move {
        while let Some(WorkerMessage::ReportSecurityAudit { request_id, .. }) =
            receiver.recv().await
        {
            let waiter = {
                let mut waiters = ack_waiters.lock();
                let waiter = waiters.remove(&request_id);
                rockstream_types::metrics::set_security_audit_transport_metrics(
                    waiters.len() as u64,
                    32,
                );
                waiter
            };
            if let Some(waiter) = waiter {
                let _ = waiter.send(true);
            }
        }
    });
    registry.with_security_audit(sender, waiters)
}

fn registry_with_workers(worker_ids: &[u64]) -> ExchangeRegistry {
    ExchangeRegistry::with_worker_state(
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(worker_infos(worker_ids))),
    )
}

fn worker_infos(worker_ids: &[u64]) -> HashMap<WorkerId, WorkerInfo> {
    worker_ids
        .iter()
        .map(|worker_id| {
            let worker_id = WorkerId(*worker_id);
            (
                worker_id,
                WorkerInfo {
                    worker_id,
                    role: NodeRole::Worker,
                    address: "127.0.0.1:1".to_owned(),
                    capacity_headroom: CapacityHeadroom::FULL,
                    location: WorkerLocation::default(),
                    capabilities: WorkerCapabilities::default(),
                    protocol_range: Default::default(),
                    storage_format_range: Default::default(),
                    registered_at_ms: 1,
                    healthy: true,
                    lifecycle: WorkerLifecycleState::Active,
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_valid_cert_admitted() {
    let pki = TestPki::generate();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let registry = registry_with_workers(&[2]);
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
async fn worker_shuffle_binds_source_shard_to_authenticated_worker() {
    let pki = TestPki::generate();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let target_db = rockstream_storage::shard_db::ShardDb::builder("target", store)
        .build()
        .await
        .unwrap();
    let target_shard = ShardId(20);
    let mut active_shards = HashMap::new();
    active_shards.insert(
        target_shard,
        ShardState {
            lease: ShardLease::new(target_shard, WorkerId(1), LeaseToken(2)),
            db: Some(target_db),
        },
    );
    let shard_leases = Arc::new(RwLock::new(HashMap::from([
        (
            ShardId(10),
            ShardLease::new(ShardId(10), WorkerId(1), LeaseToken(1)),
        ),
        (
            target_shard,
            ShardLease::new(target_shard, WorkerId(1), LeaseToken(2)),
        ),
    ])));
    let (audit_sender, mut audit_receiver, audit_waiters) = audit_transport();
    let registry = ExchangeRegistry::with_worker_state(
        Arc::new(RwLock::new(active_shards)),
        Arc::new(RwLock::new(worker_infos(&[1, 2]))),
    )
    .with_shard_leases(shard_leases.clone())
    .with_security_audit(audit_sender, audit_waiters.clone());
    let (inlet_tx, mut inlet_rx) = tokio::sync::mpsc::channel(2);
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
    ]));
    registry.register(100, target_shard.0 as u32, inlet_tx, schema);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let service = ExchangeService::new(registry).with_internal_tls(pki.worker_tls_config(1));
    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.worker_tls_config(2));
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let payload = rockstream_runtime::exchange::serialization::serialize_zset(
        &ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1),
    )
    .unwrap();

    let mut wrong_source = tonic::Request::new(futures::stream::iter(vec![ShuffleFrame {
        exchange_id: 100,
        src_shard: 10,
        target_shard: target_shard.0 as u32,
        epoch: 1,
        seq: 1,
        payload: payload.to_vec(),
        row_count: 2,
    }]));
    wrong_source.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("3"),
    );
    let mut response = client
        .shuffle_stream(wrong_source)
        .await
        .unwrap()
        .into_inner();
    let ack_waiters = audit_waiters.clone();
    let audit_task = tokio::spawn(async move {
        let WorkerMessage::ReportSecurityAudit { request_id, record } =
            audit_receiver.recv().await.unwrap()
        else {
            panic!("expected security audit report");
        };
        assert_eq!(request_id, record.event_id);
        assert_eq!(record.actor, AuditIdentity::Verified("worker-2".to_owned()));
        assert_eq!(
            record.target,
            AuditField::Known("source_shard:10".to_owned())
        );
        assert_eq!(record.scope, AuditField::Known("worker_shuffle".to_owned()));
        assert_eq!(record.action, SecurityAuditClass::AuthorizationDenial);
        assert_eq!(record.outcome, SecurityAuditOutcome::Denied);
        assert_eq!(
            record.correlation,
            AuditCorrelation::Observed(
                "exchange:100:source:10:target:20:epoch:1:sequence:1".to_owned()
            )
        );
        assert_eq!(record.operation_id, AuditField::NotApplicable);
        assert_eq!(record.policy_revision, AuditField::NotApplicable);
        assert_eq!(record.before_state, AuditField::NotApplicable);
        assert_eq!(record.after_state, AuditField::NotApplicable);
        ack_waiters
            .lock()
            .remove(&request_id)
            .unwrap()
            .send(true)
            .unwrap();
    });
    let status = response.next().await.unwrap().unwrap_err();
    audit_task.await.unwrap();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "RS-2401: authenticated worker worker-2 does not own source shard 10"
    );
    assert!(matches!(
        inlet_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    shard_leases.write().insert(
        ShardId(10),
        ShardLease::new(ShardId(10), WorkerId(2), LeaseToken(1)),
    );
    let mut authorized = tonic::Request::new(futures::stream::iter(vec![ShuffleFrame {
        exchange_id: 100,
        src_shard: 10,
        target_shard: target_shard.0 as u32,
        epoch: 1,
        seq: 2,
        payload: payload.to_vec(),
        row_count: 2,
    }]));
    authorized.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("3"),
    );
    let mut response = client
        .shuffle_stream(authorized)
        .await
        .unwrap()
        .into_inner();
    let ack = response.next().await.unwrap().unwrap();
    assert_eq!(
        (
            ack.exchange_id,
            ack.src_shard,
            ack.target_shard,
            ack.epoch,
            ack.seq,
            ack.credit_grant,
        ),
        (100, 10, target_shard.0 as u32, 1, 2, 2)
    );
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(1, 10), (2, 20)]
    );
    service.shutdown().await;
}

#[tokio::test]
async fn worker_shuffle_rotation_keeps_active_stream_and_accepts_both_generations() {
    let pki = TestPki::generate();
    let (_, _, gen2_server_tls) = pki.generate_gen2_pki();
    let dual_ca = gen2_server_tls.ca_cert_path.clone().unwrap();
    let gen1_client_tls = pki.worker_tls_config_with_ca(2, dual_ca);
    let gen2_client_tls = pki.gen2_worker_tls_config(2);
    let gen1_server_tls = pki.worker_tls_config(1);
    let reloader = Arc::new(
        rockstream_types::tls::TlsCertificateReloader::new(gen1_server_tls.clone()).unwrap(),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let store = Arc::new(object_store::memory::InMemory::new());
    let db = rockstream_storage::ShardDb::builder("rotation-shard", store)
        .build()
        .await
        .unwrap();
    let active_shards = Arc::new(RwLock::new(HashMap::from([(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), WorkerId(1), LeaseToken(1)),
            db: Some(db),
        },
    )])));
    let registry = ExchangeRegistry::with_worker_state(
        active_shards,
        Arc::new(RwLock::new(worker_infos(&[2]))),
    )
    .with_shard_leases(Arc::new(RwLock::new(HashMap::from([(
        ShardId(0),
        ShardLease::new(ShardId(0), WorkerId(2), LeaseToken(2)),
    )]))));
    let (inlet_tx, mut inlet_rx) = tokio::sync::mpsc::channel(1);
    let schema = ArrowZSet::from_ab_rows(&[], 1).schema();
    registry.register(100, 1, inlet_tx, schema);
    let service = ExchangeService::new(registry)
        .with_internal_tls(gen1_server_tls)
        .with_tls_reloader(reloader.clone());
    service.start(addr).await.unwrap();

    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers.clone()).with_internal_tls(pki.worker_tls_config(2));
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
    let stream = futures::stream::unfold(frame_rx, |mut receiver| async move {
        receiver.recv().await.map(|frame| (frame, receiver))
    });
    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let mut response = client.shuffle_stream(request).await.unwrap().into_inner();

    reloader.reload(gen2_server_tls).unwrap();
    let zset = ArrowZSet::from_ab_rows(&[(42, 7)], 1);
    let payload = rockstream_runtime::exchange::serialization::serialize_zset(&zset).unwrap();
    frame_tx
        .send(ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq: 1,
            payload: payload.clone().into(),
            row_count: zset.num_rows() as u32,
        })
        .await
        .unwrap();
    let ack = response.next().await.unwrap().unwrap();
    assert_eq!(
        (
            ack.exchange_id,
            ack.src_shard,
            ack.target_shard,
            ack.epoch,
            ack.seq
        ),
        (100, 0, 1, 1, 1)
    );
    assert_eq!(
        rockstream_runtime::exchange::serialization::serialize_zset(
            &inlet_rx.recv().await.unwrap()
        )
        .unwrap(),
        payload
    );

    for tls in [gen1_client_tls, gen2_client_tls] {
        let pool = ShuffleClientPool::new(peers.clone()).with_internal_tls(tls);
        let mut client = pool.get_client(WorkerId(1)).await.unwrap();
        let mut request = tonic::Request::new(futures::stream::iter(Vec::<ShuffleFrame>::new()));
        request.metadata_mut().insert(
            "protocol_version",
            tonic::metadata::MetadataValue::from_static("1"),
        );
        assert!(client.shuffle_stream(request).await.is_ok());
    }

    service.shutdown().await;
}

#[tokio::test]
async fn test_worker_shuffle_grpc_mtls_rejects_unregistered_worker_identity() {
    let pki = TestPki::generate();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let service = ExchangeService::new(with_auto_ack_audit(registry_with_workers(&[1])))
        .with_internal_tls(pki.worker_tls_config(1));
    service.start(addr).await.unwrap();
    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.worker_tls_config(2));
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();
    let mut request = tonic::Request::new(futures::stream::iter(Vec::<
        rockstream_runtime::exchange::proto::ShuffleFrame,
    >::new()));
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );

    let status = match client.shuffle_stream(request).await {
        Err(status) => status,
        Ok(_) => panic!("unregistered worker certificate must be rejected"),
    };
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "RS-2401: worker node identity is not currently registered"
    );
    service.shutdown().await;
}

#[tokio::test]
async fn exchange_stream_rejects_and_audits_unregistered_worker_identity() {
    let pki = TestPki::generate();
    let (audit_sender, mut audit_receiver, waiters) = audit_transport();
    let registry = registry_with_workers(&[1]).with_security_audit(audit_sender, waiters.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let service = ExchangeService::new(registry).with_internal_tls(pki.worker_tls_config(1));
    service.start(addr).await.unwrap();
    let peers = Arc::new(RwLock::new(HashMap::from([(
        WorkerId(1),
        addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.worker_tls_config(2));
    let mut client = pool.get_client(WorkerId(1)).await.unwrap();

    let ack_waiters = waiters.clone();
    let audit_task = tokio::spawn(async move {
        let Some(WorkerMessage::ReportSecurityAudit { request_id, record }) =
            audit_receiver.recv().await
        else {
            panic!("expected security audit report");
        };
        assert_eq!(request_id, record.event_id);
        assert_eq!(
            record.actor,
            AuditIdentity::Unverified("worker:worker-2".to_owned())
        );
        assert_eq!(
            record.target,
            AuditField::Unverified("worker:worker-2".to_owned())
        );
        assert_eq!(record.scope, AuditField::Known("worker_shuffle".to_owned()));
        assert_eq!(record.action, SecurityAuditClass::NodeIdentityRejection);
        assert_eq!(record.outcome, SecurityAuditOutcome::Denied);
        assert_eq!(record.correlation, AuditCorrelation::Unknown);
        assert_eq!(record.operation_id, AuditField::NotApplicable);
        assert_eq!(record.policy_revision, AuditField::NotApplicable);
        assert_eq!(record.before_state, AuditField::NotApplicable);
        assert_eq!(record.after_state, AuditField::NotApplicable);
        ack_waiters
            .lock()
            .remove(&request_id)
            .unwrap()
            .send(true)
            .unwrap();
    });
    let mut request = tonic::Request::new(futures::stream::iter(Vec::<ExchangeFrame>::new()));
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let status = match client.exchange_stream(request).await {
        Err(status) => status,
        Ok(_) => panic!("unregistered worker identity must be rejected"),
    };
    audit_task.await.unwrap();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "RS-2401: worker node identity is not currently registered"
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

    let service = ExchangeService::new(with_auto_ack_audit(ExchangeRegistry::new()))
        .with_internal_tls(pki.worker_tls_config(1));
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

    let service = ExchangeService::new(ExchangeRegistry::new());
    let error = match service.start("127.0.0.1:0".parse().unwrap()).await {
        Err(error) => error,
        Ok(_) => panic!("plaintext loopback worker shuffle requires explicit opt-in"),
    };
    assert_eq!(
        error,
        "RS-2410: plaintext worker shuffle requires explicit loopback development opt-in"
    );

    let service = ExchangeService::new(ExchangeRegistry::new()).with_insecure_internal_dev();
    let error = match service.start("0.0.0.0:0".parse().unwrap()).await {
        Err(error) => error,
        Ok(_) => panic!("development opt-in must not allow non-loopback plaintext shuffle"),
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
