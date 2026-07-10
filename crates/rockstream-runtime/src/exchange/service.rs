use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

use crate::exchange::proto::{shuffle_service_server::ShuffleService, ShuffleAck, ShuffleFrame};
use crate::exchange::serialization::deserialize_zset;
use rockstream_ops::zset::ArrowZSet;

/// Holds the input channel and Schema metadata for a local exchange target.
#[derive(Clone)]
pub struct ExchangeInlet {
    pub sender: mpsc::Sender<ArrowZSet>,
    pub schema: SchemaRef,
}

/// A registry of active local exchange destinations (e.g. operators waiting for shuffle inputs).
#[derive(Clone)]
pub struct ExchangeRegistry {
    inlets: Arc<RwLock<HashMap<(u64, u32), ExchangeInlet>>>,
    active_shards: Arc<RwLock<HashMap<rockstream_types::ids::ShardId, crate::client::ShardState>>>,
    cluster_frontier: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for ExchangeRegistry {
    fn default() -> Self {
        Self {
            inlets: Arc::new(RwLock::new(HashMap::new())),
            active_shards: Arc::new(RwLock::new(HashMap::new())),
            cluster_frontier: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl ExchangeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_shards(
        active_shards: Arc<
            RwLock<HashMap<rockstream_types::ids::ShardId, crate::client::ShardState>>,
        >,
    ) -> Self {
        ExchangeRegistry {
            inlets: Arc::new(RwLock::new(HashMap::new())),
            active_shards,
            cluster_frontier: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Get the current cluster frontier.
    pub fn cluster_frontier(&self) -> u64 {
        self.cluster_frontier
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Advance the cluster frontier and trigger GC on all active shards.
    pub async fn advance_cluster_frontier(&self, epoch: u64) -> Result<(), String> {
        let mut old = self
            .cluster_frontier
            .load(std::sync::atomic::Ordering::SeqCst);
        while epoch > old {
            match self.cluster_frontier.compare_exchange_weak(
                old,
                epoch,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // Trigger GC on all active shards
                    let dbs: Vec<rockstream_storage::shard_db::ShardDb> = {
                        let shards = self.active_shards.read();
                        shards.values().filter_map(|s| s.db.clone()).collect()
                    };
                    for db in dbs {
                        crate::exchange::persistence::gc_exchange_storage(&db, epoch).await?;
                    }
                    break;
                }
                Err(current) => {
                    old = current;
                }
            }
        }
        Ok(())
    }

    /// Retrieve the ShardDb for a given shard.
    pub fn get_shard_db(&self, shard_id: u32) -> Option<rockstream_storage::shard_db::ShardDb> {
        let shards = self.active_shards.read();
        shards
            .get(&rockstream_types::ids::ShardId(shard_id as u64))
            .and_then(|state| state.db.clone())
    }

    /// Register a local inlet for the given exchange and target shard.
    pub fn register(
        &self,
        exchange_id: u64,
        target_shard: u32,
        sender: mpsc::Sender<ArrowZSet>,
        schema: SchemaRef,
    ) {
        self.inlets.write().insert(
            (exchange_id, target_shard),
            ExchangeInlet { sender, schema },
        );
    }

    /// Unregister a local inlet.
    pub fn unregister(&self, exchange_id: u64, target_shard: u32) {
        self.inlets.write().remove(&(exchange_id, target_shard));
    }

    /// Get a registered local inlet.
    pub fn get(&self, exchange_id: u64, target_shard: u32) -> Option<ExchangeInlet> {
        self.inlets
            .read()
            .get(&(exchange_id, target_shard))
            .cloned()
    }
}

/// gRPC implementation of the ShuffleService.
pub struct ShuffleServer {
    registry: ExchangeRegistry,
}

impl ShuffleServer {
    pub fn new(registry: ExchangeRegistry) -> Self {
        ShuffleServer { registry }
    }
}

#[tonic::async_trait]
impl ShuffleService for ShuffleServer {
    type ShuffleStreamStream = Pin<Box<dyn Stream<Item = Result<ShuffleAck, Status>> + Send>>;

    async fn shuffle_stream(
        &self,
        request: Request<Streaming<ShuffleFrame>>,
    ) -> Result<Response<Self::ShuffleStreamStream>, Status> {
        let mut stream = request.into_inner();
        let registry = self.registry.clone();

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(frame) => {
                        let inlet_opt = registry.get(frame.exchange_id, frame.target_shard);
                        if let Some(inlet) = inlet_opt {
                            if let Some(db) = registry.get_shard_db(frame.target_shard) {
                                if let Err(e) = crate::exchange::persistence::persist_inbox(
                                    &db,
                                    frame.exchange_id,
                                    frame.src_shard,
                                    frame.epoch,
                                    frame.seq,
                                    &frame.payload,
                                )
                                .await
                                {
                                    tracing::error!(
                                        code = %rockstream_types::error_code::RS_0003,
                                        "Failed to persist incoming shuffle frame to inbox: {:?}",
                                        e
                                    );
                                }
                            }
                            match deserialize_zset(&frame.payload, inlet.schema.clone()) {
                                Ok(zset) => {
                                    if inlet.sender.send(zset).await.is_err() {
                                        tracing::warn!(
                                            "Failed to forward shuffle batch to local inlet"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        code = %rockstream_types::error_code::RS_3009,
                                        "Failed to deserialize shuffle payload: {:?}",
                                        e
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                exchange_id = frame.exchange_id,
                                target_shard = frame.target_shard,
                                "Received shuffle frame for unregistered target"
                            );
                        }

                        // Send acknowledgement back to the sender
                        let ack = ShuffleAck {
                            exchange_id: frame.exchange_id,
                            src_shard: frame.src_shard,
                            target_shard: frame.target_shard,
                            epoch: frame.epoch,
                            seq: frame.seq,
                            credit_grant: 1, // Grant 1 credit per processed frame
                        };

                        if tx.send(Ok(ack)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_0001,
                            "gRPC ShuffleStream receive error: {:?}",
                            e
                        );
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(RxStream { rx })))
    }
}

struct RxStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> Stream for RxStream<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::flow_control::FlowController;
    use crate::exchange::multiplexer::WorkerStreamMultiplexer;
    use crate::exchange::pool::ShuffleClientPool;
    use crate::exchange::serialization::serialize_zset;
    use rockstream_types::ids::WorkerId;

    #[tokio::test]
    async fn test_grpc_routing_and_multiplexer() {
        eprintln!("TEST START");
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        eprintln!("Bound to address: {}", addr);

        let registry = ExchangeRegistry::new();
        let server = ShuffleServer::new(registry.clone());

        let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            eprintln!("Server task started");
            let res = tonic::transport::Server::builder()
                .add_service(
                    crate::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                        server,
                    ),
                )
                .serve_with_shutdown(addr, async {
                    let _ = rx_close.await;
                    eprintln!("Shutdown signal received in server");
                })
                .await;
            eprintln!("Server task exited: {:?}", res);
        });

        // Register a local inlet
        let (inlet_tx, mut inlet_rx) = mpsc::channel(10);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false),
        ]));
        registry.register(100, 1, inlet_tx, schema.clone());
        eprintln!("Inlet registered");

        // Give the server a moment to start listening
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Part 1: Connect directly and route a frame
        let dst = format!("http://{}", addr);
        eprintln!("Connecting client directly to {}", dst);
        let channel = tonic::transport::Channel::from_shared(dst)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client =
            crate::exchange::proto::shuffle_service_client::ShuffleServiceClient::new(channel);
        eprintln!("Direct client connected");

        let zset = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
        let payload = serialize_zset(&zset).unwrap();

        let frame = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq: 1,
            payload: payload.clone().into(),
        };

        let (frame_tx, frame_rx) = mpsc::channel(1);
        frame_tx.send(frame).await.unwrap();
        eprintln!("Direct frame sent to local channel");

        let request_stream = RxStream { rx: frame_rx };
        eprintln!("Calling shuffle_stream direct");
        let mut response = client
            .shuffle_stream(request_stream)
            .await
            .unwrap()
            .into_inner();
        eprintln!("shuffle_stream direct response stream received");

        eprintln!("Waiting for Ack");
        let ack = response.next().await.unwrap().unwrap();
        eprintln!("Ack received: {:?}", ack);
        assert_eq!(ack.exchange_id, 100);
        assert_eq!(ack.src_shard, 0);
        assert_eq!(ack.target_shard, 1);
        assert_eq!(ack.seq, 1);

        eprintln!("Waiting for Inlet receive");
        let received = inlet_rx.recv().await.unwrap();
        eprintln!("Inlet received ZSet");
        assert_eq!(received.num_rows(), 2);

        // Part 2: Connect using ShuffleClientPool and WorkerStreamMultiplexer
        eprintln!("Starting Part 2");
        let peers = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
        peers.write().insert(WorkerId(2), addr.to_string());

        let pool = ShuffleClientPool::new(peers);
        let flow_controller = FlowController::new();
        let multiplexer = WorkerStreamMultiplexer::new(pool, flow_controller.clone());

        let frame2 = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq: 2,
            payload: payload.into(),
        };

        eprintln!("Multiplexer sending frame");
        multiplexer.send_frame(WorkerId(2), frame2).await.unwrap();
        eprintln!("Multiplexer sent frame");

        eprintln!("Waiting for Inlet receive 2");
        let received2 = inlet_rx.recv().await.unwrap();
        eprintln!("Inlet received ZSet 2");
        assert_eq!(received2.num_rows(), 2);

        // Let the background ACK handler run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Clean up
        eprintln!("Cleaning up server");
        let _ = tx_close.send(());
        server_handle.abort();
        eprintln!("TEST FINISHED");
    }

    #[tokio::test]
    async fn test_loopback_vs_direct_grpc() {
        use crate::client::ShardState;
        use object_store::memory::InMemory;
        use rockstream_storage::shard_db::ShardDb;
        use rockstream_types::ids::{LeaseToken, ShardId};

        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let registry = ExchangeRegistry::new();
        let server = ShuffleServer::new(registry.clone());

        let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    crate::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                        server,
                    ),
                )
                .serve_with_shutdown(addr, async {
                    let _ = rx_close.await;
                })
                .await;
        });

        // Register a local inlet
        let (inlet_tx, mut inlet_rx) = mpsc::channel(10);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false),
        ]));
        registry.register(100, 1, inlet_tx, schema.clone());

        // Give the server a moment to start listening
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Set up active shards map with mock SlateDbs for loopback router
        let active_shards = Arc::new(RwLock::new(std::collections::HashMap::new()));

        let store1 = Arc::new(InMemory::new());
        let db1 = ShardDb::builder("db1", store1).build().await.unwrap();

        let store2 = Arc::new(InMemory::new());
        let db2 = ShardDb::builder("db2", store2).build().await.unwrap();

        active_shards.write().insert(
            ShardId(0),
            ShardState {
                lease: rockstream_types::lease::ShardLease::new(
                    ShardId(0),
                    WorkerId(1),
                    LeaseToken(1),
                ),
                db: Some(db1.clone()),
            },
        );

        active_shards.write().insert(
            ShardId(1),
            ShardState {
                lease: rockstream_types::lease::ShardLease::new(
                    ShardId(1),
                    WorkerId(1),
                    LeaseToken(1),
                ),
                db: Some(db2.clone()),
            },
        );

        let loopback_router =
            crate::exchange::loopback::LoopbackRouter::new(registry.clone(), active_shards);

        // 1. Direct path routing via gRPC
        let peers = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
        peers.write().insert(WorkerId(2), addr.to_string());

        let pool = ShuffleClientPool::new(peers);
        let flow_controller = FlowController::new();
        let multiplexer = WorkerStreamMultiplexer::new(pool, flow_controller.clone());

        let zset = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
        let payload = crate::exchange::serialization::serialize_zset(&zset).unwrap();

        let frame = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq: 1,
            payload: payload.into(),
        };

        // Send direct
        multiplexer.send_frame(WorkerId(2), frame).await.unwrap();
        let received_direct = inlet_rx.recv().await.unwrap();

        // 2. Loopback path routing
        loopback_router
            .route_loopback(100, 0, 1, 1, 2, &zset)
            .await
            .unwrap();
        let received_loopback = inlet_rx.recv().await.unwrap();

        // Assert they are identical
        assert_eq!(received_direct.num_rows(), received_loopback.num_rows());
        assert_eq!(received_direct.weights, received_loopback.weights);
        assert_eq!(
            received_direct.positive_ab_rows(),
            received_loopback.positive_ab_rows()
        );

        // Clean up
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_credit_backpressure() {
        let flow_controller = FlowController::new();
        let exchange_id = 100u64;
        let src_shard = 1u32;
        let target_shard = 2u32;

        // Senders start with 16 credits. We should be able to acquire 16 times without blocking.
        for _ in 0..16 {
            let fut = flow_controller.acquire_credit(exchange_id, src_shard, target_shard);
            // Must complete immediately
            tokio::time::timeout(std::time::Duration::from_millis(50), fut)
                .await
                .unwrap();
        }

        // Now credits are 0. The next acquire_credit must block/suspend.
        let fc_clone = flow_controller.clone();
        let (tx_done, mut rx_done) = mpsc::channel(1);

        let handle = tokio::spawn(async move {
            fc_clone
                .acquire_credit(exchange_id, src_shard, target_shard)
                .await;
            let _ = tx_done.send(()).await;
        });

        // Check that it hasn't completed yet
        tokio::select! {
            _ = rx_done.recv() => {
                panic!("acquire_credit completed without available credits!");
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Good, it is suspended.
            }
        }

        // Send an ACK to restore 1 credit.
        let ack = ShuffleAck {
            exchange_id,
            src_shard,
            target_shard,
            epoch: 1,
            seq: 1,
            credit_grant: 1,
        };
        flow_controller.handle_ack(&ack);

        // The suspended task should now resume and complete
        tokio::time::timeout(std::time::Duration::from_millis(100), rx_done.recv())
            .await
            .unwrap()
            .unwrap();

        // Cleanup spawn
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_exchange_metadata_durability() {
        use crate::client::ShardState;
        use rockstream_storage::shard_db::ShardDb;
        use rockstream_types::ids::{LeaseToken, ShardId};

        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        // Create databases using temp dirs so they can be reopened to simulate crash-replay
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let store1 =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir1.path()).unwrap());
        let store2 =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir2.path()).unwrap());

        let db1 = ShardDb::builder("db", store1.clone())
            .build()
            .await
            .unwrap();
        let db2 = ShardDb::builder("db", store2.clone())
            .build()
            .await
            .unwrap();

        let active_shards = Arc::new(RwLock::new(std::collections::HashMap::new()));
        active_shards.write().insert(
            ShardId(0),
            ShardState {
                lease: rockstream_types::lease::ShardLease::new(
                    ShardId(0),
                    WorkerId(1),
                    LeaseToken(1),
                ),
                db: Some(db1.clone()),
            },
        );
        active_shards.write().insert(
            ShardId(1),
            ShardState {
                lease: rockstream_types::lease::ShardLease::new(
                    ShardId(1),
                    WorkerId(1),
                    LeaseToken(1),
                ),
                db: Some(db2.clone()),
            },
        );

        let registry = ExchangeRegistry::with_shards(active_shards.clone());
        let server = ShuffleServer::new(registry.clone());

        let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    crate::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                        server,
                    ),
                )
                .serve_with_shutdown(addr, async {
                    let _ = rx_close.await;
                })
                .await;
        });

        // Register local inlet
        let (inlet_tx, mut _inlet_rx) = mpsc::channel(10);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false),
        ]));
        registry.register(100, 1, inlet_tx, schema.clone());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let zset = ArrowZSet::from_ab_rows(&[(5, 500)], 1);

        // --- 1. Loopback Durability Test ---
        let loopback_router =
            crate::exchange::loopback::LoopbackRouter::new(registry.clone(), active_shards.clone());
        loopback_router
            .route_loopback(100, 0, 1, 1, 10, &zset)
            .await
            .unwrap();

        // Check inbox key on db2
        let inbox_prefix = [0x04];
        let inbox_entries = db2.scan_prefix(&inbox_prefix).await.unwrap();
        assert_eq!(inbox_entries.len(), 1, "Loopback inbox entry not written!");

        // --- 2. Direct Path Durability Test ---
        let peers = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
        peers.write().insert(WorkerId(2), addr.to_string());

        let pool = ShuffleClientPool::new(peers);
        let flow_controller = FlowController::new();
        let multiplexer = WorkerStreamMultiplexer::with_shards(
            pool,
            flow_controller.clone(),
            active_shards.clone(),
        );

        let payload = crate::exchange::serialization::serialize_zset(&zset).unwrap();

        let (inlet_tx200, mut inlet_rx200) = mpsc::channel(10);
        registry.register(200, 1, inlet_tx200, schema.clone());

        let frame2 = ShuffleFrame {
            exchange_id: 200,
            src_shard: 0,
            target_shard: 1,
            epoch: 2,
            seq: 21,
            payload: payload.into(),
        };
        multiplexer.send_frame(WorkerId(2), frame2).await.unwrap();
        let _ = inlet_rx200.recv().await.unwrap();

        // Check inbox key on db2 for exchange 200
        let inbox_entries2 = db2.scan_prefix(&inbox_prefix).await.unwrap();
        assert!(inbox_entries2.len() >= 2, "Direct inbox entry not written!");

        // --- 3. Crash-Replay verification ---
        // Close both databases to simulate a crash/shutdown
        db1.close().await.unwrap();
        db2.close().await.unwrap();

        // Reopen them (simulating recovery replay)
        let db1_recovered = ShardDb::builder("db", store1).build().await.unwrap();
        let db2_recovered = ShardDb::builder("db", store2).build().await.unwrap();

        // Verify the persisted keys are still there and can be scanned/read correctly
        let inbox_recovered = db2_recovered.scan_prefix(&inbox_prefix).await.unwrap();
        assert!(inbox_recovered.len() >= 2);

        // Verify we can deserialize the recovered payload
        let recovered_payload = &inbox_recovered[0].1;
        let recovered_zset =
            crate::exchange::serialization::deserialize_zset(recovered_payload, schema.clone())
                .unwrap();
        assert_eq!(recovered_zset.num_rows(), 1);

        // Clean up
        db1_recovered.close().await.unwrap();
        db2_recovered.close().await.unwrap();
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_exchange_garbage_collection() {
        use crate::exchange::persistence::{gc_exchange_storage, persist_inbox, persist_outbox};
        use object_store::memory::InMemory;
        use rockstream_storage::shard_db::ShardDb;

        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("db", store).build().await.unwrap();

        // Persist some inbox entries
        persist_inbox(&db, 100, 1, 1, 1, b"payload1").await.unwrap();
        persist_inbox(&db, 100, 1, 2, 1, b"payload2").await.unwrap();
        persist_inbox(&db, 100, 1, 3, 1, b"payload3").await.unwrap();

        // Persist some outbox entries
        persist_outbox(&db, 200, 2, 1, 1, b"payload1")
            .await
            .unwrap();
        persist_outbox(&db, 200, 2, 2, 1, b"payload2")
            .await
            .unwrap();
        persist_outbox(&db, 200, 2, 3, 1, b"payload3")
            .await
            .unwrap();

        // Check they exist
        let inbox_prefix = [0x04];
        let outbox_prefix = [0x05];
        assert_eq!(db.scan_prefix(&inbox_prefix).await.unwrap().len(), 3);
        assert_eq!(db.scan_prefix(&outbox_prefix).await.unwrap().len(), 3);

        // Run GC up to epoch 2
        gc_exchange_storage(&db, 2).await.unwrap();

        // Verify only epoch 3 remains
        let inbox_entries = db.scan_prefix(&inbox_prefix).await.unwrap();
        assert_eq!(inbox_entries.len(), 1);
        let outbox_entries = db.scan_prefix(&outbox_prefix).await.unwrap();
        assert_eq!(outbox_entries.len(), 1);

        if let Some((_, _, suffix)) =
            rockstream_storage::keys::ShardKeyEncoder::decode(&inbox_entries[0].0)
        {
            let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
            assert_eq!(epoch, 3);
        } else {
            panic!("failed to decode key");
        }
    }
}
