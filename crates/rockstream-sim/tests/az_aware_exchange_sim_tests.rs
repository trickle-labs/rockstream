#![cfg(feature = "simulation")]

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{
    ControlService, MigrationPersistentStore, ShardManager, TopologyCatalog,
    TopologyPersistentStore,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::classifier::{
    classify_exchange, ExchangeClassificationInput, PeerLocality,
};
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::persistence::delete_outbox_if_present;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::serialize_zset;
use rockstream_runtime::exchange::service::{
    register_shared_memory_endpoint, unregister_shared_memory_endpoint, ExchangeRegistry,
    ShuffleServer,
};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_focus, buggify_init};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::config::{ExchangeConfig, WorkerConfig};
use rockstream_types::exchange::{ExchangeAnn, ExchangePath, ExchangeTransport};
use rockstream_types::ids::{ExchangeId, LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLocation, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

static SHM_TEST_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

async fn send(addr: std::net::SocketAddr, msg: &WorkerMessage) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let line = serde_json::to_string(msg).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    let _ = tokio::time::timeout(Duration::from_millis(50), reader.read_line(&mut buf)).await;
}

fn registration_with_location(id: u64, host_id: &str, az: &str) -> WorkerRegistration {
    WorkerRegistration::new(
        WorkerId(id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 7000 + id),
        CapacityHeadroom::FULL,
    )
    .with_location(WorkerLocation::new(host_id, az))
    .with_capabilities(WorkerCapabilities {
        same_host_arrow_shm_v1: true,
        shuffle_codec_v1: true,
        checkpoint_manifest_codec_v1: true,
    })
}

fn worker_info(worker_id: u64, address: String, host_id: &str, az: &str) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address,
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new(host_id, az),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: true,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
        registered_at_ms: 1,
        healthy: true,
        lifecycle: rockstream_types::topology::WorkerLifecycleState::Active,
    }
}

async fn shard_db(name: &str) -> ShardDb {
    ShardDb::builder(name, Arc::new(InMemory::new()))
        .build()
        .await
        .unwrap()
}

fn make_sender_shards(
    src_db: ShardDb,
    worker_id: WorkerId,
) -> Arc<parking_lot::RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), worker_id, LeaseToken(1)),
            db: Some(src_db),
        },
    );
    Arc::new(parking_lot::RwLock::new(shards))
}

fn make_receiver_registry(
    target_db: ShardDb,
    worker_id: WorkerId,
) -> (ExchangeRegistry, mpsc::Receiver<ArrowZSet>) {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), worker_id, LeaseToken(1)),
            db: Some(target_db),
        },
    );
    let registry = ExchangeRegistry::with_shards(Arc::new(parking_lot::RwLock::new(shards)));
    let (tx, rx) = mpsc::channel(10);
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
    ]));
    registry.register(100, 1, tx, schema);
    (registry, rx)
}

fn ann(source_worker: u64, target_worker: u64) -> ExchangeAnn {
    ExchangeAnn {
        exchange_id: ExchangeId(100),
        law_id: None,
        source_shard: ShardId(0),
        target_shard: ShardId(1),
        source_worker: WorkerId(source_worker),
        target_worker: WorkerId(target_worker),
        path: ExchangePath::Direct,
    }
}

#[tokio::test]
async fn missing_az_metadata_falls_back_without_row_loss_sim() {
    buggify_init(4901);
    buggify_focus("exchange.az_metadata_missing");
    assert!(buggify!("exchange.az_metadata_missing", 1.0));

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let src_db = shard_db("sim-az-src").await;
    let sender_shards = make_sender_shards(src_db, WorkerId(201));
    let target_db = shard_db("sim-az-dst").await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db, WorkerId(202));
    let server = ShuffleServer::new(registry.clone());
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                    server,
                ),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(202), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    let local = worker_info(201, "127.0.0.1:9201".to_string(), "host-a", "az-1");
    let peer = worker_info(202, addr.to_string(), "host-a", "az-1");
    pool.set_local_worker_info(local.clone());
    pool.upsert_peer_info(peer.clone());

    let route = classify_exchange(ExchangeClassificationInput {
        ann: &ann(201, 202),
        local_worker: Some(&local),
        peer_worker: Some(&peer),
        receiver_reachable: true,
        batch_bytes: 1024,
        epoch_exchange_bytes: 1024,
        config: &ExchangeConfig::default(),
    });
    assert_eq!(route.locality, PeerLocality::Unknown);
    assert_eq!(route.transport, ExchangeTransport::Grpc);
    assert!(route.metadata_fallback);

    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
            .with_src_worker(WorkerId(201));
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(7, 70), (8, 80)], 1)).unwrap();
    multiplexer
        .send_frame(
            WorkerId(202),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 1,
                payload: payload.into(),
                row_count: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(7, 70), (8, 80)]
    );
    assert_eq!(registry.grpc_frames_received(), 1);

    let _ = tx_close.send(());
    server_handle.abort();
    buggify_disable();
}

#[tokio::test]
async fn same_host_shared_memory_peer_crash_falls_back_to_grpc_without_duplication_sim() {
    let _guard = SHM_TEST_LOCK.lock().await;
    buggify_init(4902);
    buggify_focus("exchange.shm_segment_unavailable");
    assert!(buggify!("exchange.shm_segment_unavailable", 1.0));

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let src_db = shard_db("sim-shm-src").await;
    let sender_shards = make_sender_shards(src_db.clone(), WorkerId(301));
    let target_db = shard_db("sim-shm-dst").await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone(), WorkerId(302));
    register_shared_memory_endpoint(WorkerId(302), registry.clone());
    let server = ShuffleServer::new(registry.clone());
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                    server,
                ),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(302), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(
        301,
        "127.0.0.1:9301".to_string(),
        "host-a",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(302, addr.to_string(), "host-a", "az-1"));
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
            .with_src_worker(WorkerId(301));

    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(11, 110)], 1)).unwrap();
    let frame = ShuffleFrame {
        exchange_id: 100,
        src_shard: 0,
        target_shard: 1,
        epoch: 1,
        seq: 5,
        payload: payload.into(),
        row_count: 1,
    };
    multiplexer
        .send_frame(WorkerId(302), frame.clone())
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(11, 110)]
    );
    assert_eq!(registry.grpc_frames_received(), 1);

    // The target operator checkpoints epoch 1: its committed frontier is durably
    // advanced. With v0.51 fast-path WAL elision, the gRPC-fallback delivery
    // writes no shuffle_inbox/ and replay-dedup relies on the frontier.
    target_db
        .commit_epoch(rockstream_types::ids::ShardId(1), 1)
        .await
        .unwrap();

    multiplexer.send_frame(WorkerId(302), frame).await.unwrap();
    tokio::select! {
        maybe = inlet_rx.recv() => panic!("unexpected duplicate delivery: {:?}", maybe),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    // Fast-path elision: neither the sender outbox (0x05) nor the receiver inbox
    // (0x04) is persisted for the successfully delivered gRPC-fallback frame.
    assert!(!delete_outbox_if_present(&src_db, 100, 1, 1, 5)
        .await
        .unwrap());
    assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);
    assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);

    unregister_shared_memory_endpoint(WorkerId(302));
    let _ = tx_close.send(());
    server_handle.abort();
    buggify_disable();
}

#[tokio::test]
async fn same_host_shared_memory_quantum_budget_falls_back_without_duplication_sim() {
    let _guard = SHM_TEST_LOCK.lock().await;
    buggify_init(5101);
    buggify_focus("exchange.shm_segment_unavailable");
    assert!(buggify!("exchange.shm_segment_unavailable", 1.0));

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let src_db = shard_db("sim-shm-budget-src").await;
    let sender_shards = make_sender_shards(src_db.clone(), WorkerId(351));
    let target_db = shard_db("sim-shm-budget-dst").await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone(), WorkerId(352));
    register_shared_memory_endpoint(WorkerId(352), registry.clone());
    let server = ShuffleServer::new(registry.clone());
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                    server,
                ),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(352), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(
        351,
        "127.0.0.1:9351".to_string(),
        "host-a",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(352, addr.to_string(), "host-a", "az-1"));
    let flow_controller = FlowController::with_row_budget(1);
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, flow_controller.clone(), sender_shards)
            .with_src_worker(WorkerId(351))
            .with_worker_config(WorkerConfig {
                segment_cache_bytes: 0,
                max_rows_per_quantum: 1,
            });

    let frame = ShuffleFrame {
        exchange_id: 100,
        src_shard: 0,
        target_shard: 1,
        epoch: 1,
        seq: 5,
        payload: serialize_zset(&ArrowZSet::from_ab_rows(&[(11, 110)], 1))
            .unwrap()
            .into(),
        row_count: 1,
    };
    multiplexer
        .send_frame(WorkerId(352), frame.clone())
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(11, 110)]
    );
    assert_eq!(registry.grpc_frames_received(), 1);
    tokio::time::timeout(Duration::from_millis(200), async {
        while flow_controller.rows_in_flight(100, 0, 1) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    target_db.commit_epoch(ShardId(1), 1).await.unwrap();
    multiplexer.send_frame(WorkerId(352), frame).await.unwrap();
    tokio::select! {
        maybe = inlet_rx.recv() => panic!("unexpected duplicate delivery: {:?}", maybe),
        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
    }

    multiplexer
        .send_frame(
            WorkerId(352),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 2,
                seq: 6,
                payload: serialize_zset(&ArrowZSet::from_ab_rows(&[(12, 120)], 1))
                    .unwrap()
                    .into(),
                row_count: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(12, 120)]
    );
    tokio::time::timeout(Duration::from_millis(200), async {
        while flow_controller.rows_in_flight(100, 0, 1) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    unregister_shared_memory_endpoint(WorkerId(352));
    let _ = tx_close.send(());
    server_handle.abort();
    buggify_disable();
}

#[tokio::test]
async fn az_domain_rebuild_during_drain_preserves_delivery_sim() {
    buggify_init(4903);
    buggify_focus("exchange.domain_rebuild_during_drain");
    assert!(buggify!("exchange.domain_rebuild_during_drain", 1.0));

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let store = Arc::new(InMemory::new());
    let handle = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_topology_store(Arc::new(TopologyPersistentStore::new(store.clone())))
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)))
        .with_auto_drain(true)
        .start("127.0.0.1:0")
        .await
        .unwrap();
    for reg in [
        registration_with_location(1, "host-a", "az-1"),
        registration_with_location(2, "host-b", "az-2"),
        registration_with_location(3, "host-c", "az-1"),
    ] {
        send(handle.addr, &WorkerMessage::Register(reg)).await;
    }
    manager.acquire(ShardId(77), WorkerId(1)).unwrap();
    send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(1),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(manager.get(ShardId(77)).unwrap().worker_id, WorkerId(3));

    let local = worker_info(1, "127.0.0.1:9401".to_string(), "host-a", "az-1");
    let cross_az = worker_info(2, "127.0.0.1:9402".to_string(), "host-b", "az-2");
    let route = classify_exchange(ExchangeClassificationInput {
        ann: &ann(1, 2),
        local_worker: Some(&local),
        peer_worker: Some(&cross_az),
        receiver_reachable: true,
        batch_bytes: 1024,
        epoch_exchange_bytes: 1024,
        config: &ExchangeConfig::default(),
    });
    assert_eq!(route.transport, ExchangeTransport::DurableObject);

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let src_db = shard_db("sim-drain-src").await;
    let sender_shards = make_sender_shards(src_db, WorkerId(1));
    let target_db = shard_db("sim-drain-dst").await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db, WorkerId(3));
    let server = ShuffleServer::new(registry);
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                    server,
                ),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(3), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(local);
    pool.upsert_peer_info(worker_info(3, addr.to_string(), "host-c", "az-1"));
    pool.upsert_peer_info(cross_az);
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
            .with_src_worker(WorkerId(1));
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(21, 210)], 1)).unwrap();
    multiplexer
        .send_frame(
            WorkerId(3),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 9,
                payload: payload.into(),
                row_count: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(21, 210)]
    );
    assert_eq!(multiplexer.connection_count(), 1);

    let _ = tx_close.send(());
    server_handle.abort();
    handle.shutdown();
    buggify_disable();
}

#[tokio::test]
async fn exchange_domain_rebuild_releases_row_credits_during_drain_sim() {
    buggify_init(5102);
    buggify_focus("exchange.domain_rebuild_during_drain");
    assert!(buggify!("exchange.domain_rebuild_during_drain", 1.0));

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let store = Arc::new(InMemory::new());
    let handle = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_topology_store(Arc::new(TopologyPersistentStore::new(store.clone())))
        .with_migration_store(Arc::new(MigrationPersistentStore::new(store)))
        .with_auto_drain(true)
        .start("127.0.0.1:0")
        .await
        .unwrap();
    for reg in [
        registration_with_location(11, "host-a", "az-1"),
        registration_with_location(12, "host-b", "az-2"),
        registration_with_location(13, "host-c", "az-1"),
    ] {
        send(handle.addr, &WorkerMessage::Register(reg)).await;
    }
    manager.acquire(ShardId(88), WorkerId(11)).unwrap();
    send(
        handle.addr,
        &WorkerMessage::RequestDrain {
            worker_id: WorkerId(11),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(manager.get(ShardId(88)).unwrap().worker_id, WorkerId(13));

    let src_db = shard_db("sim-drain-budget-src").await;
    let sender_shards = make_sender_shards(src_db, WorkerId(11));
    let target_db = shard_db("sim-drain-budget-dst").await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db, WorkerId(13));
    let flow_controller = FlowController::with_row_budget(1);
    let durable_store = Arc::new(InMemory::new());
    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(
        11,
        "127.0.0.1:9411".to_string(),
        "host-a",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(
        13,
        "127.0.0.1:9413".to_string(),
        "host-c",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(
        12,
        "127.0.0.1:9412".to_string(),
        "host-b",
        "az-2",
    ));
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, flow_controller.clone(), sender_shards)
            .with_object_store(durable_store.clone())
            .with_src_worker(WorkerId(11))
            .with_worker_config(WorkerConfig {
                segment_cache_bytes: 0,
                max_rows_per_quantum: 1,
            });

    for (epoch, seq, row) in [(1, 9, (21, 210)), (2, 10, (22, 220))] {
        multiplexer
            .send_frame(
                WorkerId(13),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch,
                    seq,
                    payload: serialize_zset(&ArrowZSet::from_ab_rows(&[row], 1))
                        .unwrap()
                        .into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        multiplexer
            .catch_up_durable(
                100,
                epoch,
                WorkerId(11),
                WorkerId(13),
                &registry,
                durable_store.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(inlet_rx.recv().await.unwrap().positive_ab_rows(), vec![row]);
        assert_eq!(flow_controller.rows_in_flight(100, 0, 1), 0);
    }

    handle.shutdown();
    buggify_disable();
    let _ = registry;
}
