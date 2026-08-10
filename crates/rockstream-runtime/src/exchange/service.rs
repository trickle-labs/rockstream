use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

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
use rockstream_types::config::ExchangeConfig;

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
    grpc_frames_received: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for ExchangeRegistry {
    fn default() -> Self {
        Self {
            inlets: Arc::new(RwLock::new(HashMap::new())),
            active_shards: Arc::new(RwLock::new(HashMap::new())),
            cluster_frontier: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            grpc_frames_received: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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
            grpc_frames_received: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Get total count of gRPC frames received.
    pub fn grpc_frames_received(&self) -> u64 {
        self.grpc_frames_received
            .load(std::sync::atomic::Ordering::Relaxed)
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

    pub fn unregister(&self, exchange_id: u64, target_shard: u32) {
        self.inlets.write().remove(&(exchange_id, target_shard));
    }

    pub fn get(&self, exchange_id: u64, target_shard: u32) -> Option<ExchangeInlet> {
        self.inlets
            .read()
            .get(&(exchange_id, target_shard))
            .cloned()
    }
}

/// Service wrapper managing exchange gRPC server lifecycle with TaskTracker & CancellationToken.
#[derive(Clone)]
pub struct ExchangeService {
    registry: ExchangeRegistry,
    task_tracker: TaskTracker,
    cancel_token: CancellationToken,
    exchange_config: ExchangeConfig,
}

impl ExchangeService {
    pub fn new(registry: ExchangeRegistry) -> Self {
        Self {
            registry,
            task_tracker: TaskTracker::new(),
            cancel_token: CancellationToken::new(),
            exchange_config: ExchangeConfig::default(),
        }
    }

    pub fn with_exchange_config(mut self, exchange_config: ExchangeConfig) -> Self {
        self.exchange_config = exchange_config;
        self
    }

    pub fn exchange_config(&self) -> &ExchangeConfig {
        &self.exchange_config
    }

    pub fn registry(&self) -> &ExchangeRegistry {
        &self.registry
    }

    pub fn task_tracker(&self) -> &TaskTracker {
        &self.task_tracker
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    pub async fn start(
        &self,
        addr: std::net::SocketAddr,
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        let server = ShuffleServer::new_with_tracker(
            self.registry.clone(),
            Some(self.task_tracker.clone()),
            Some(self.cancel_token.clone()),
        )
        .with_exchange_config(self.exchange_config.clone());
        let cancel_token = self.cancel_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    crate::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                        server,
                    ),
                )
                .serve_with_shutdown(addr, async move {
                    cancel_token.cancelled().await;
                })
                .await;
        });
        Ok(handle)
    }

    pub async fn shutdown(&self) {
        self.cancel_token.cancel();
        self.task_tracker.close();
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.task_tracker.wait()).await;
    }
}

/// gRPC implementation of the ShuffleService.
pub struct ShuffleServer {
    registry: ExchangeRegistry,
    task_tracker: Option<TaskTracker>,
    cancel_token: Option<CancellationToken>,
    exchange_config: ExchangeConfig,
}

impl ShuffleServer {
    pub fn new(registry: ExchangeRegistry) -> Self {
        ShuffleServer {
            registry,
            task_tracker: None,
            cancel_token: None,
            exchange_config: ExchangeConfig::default(),
        }
    }

    pub fn new_with_tracker(
        registry: ExchangeRegistry,
        task_tracker: Option<TaskTracker>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        ShuffleServer {
            registry,
            task_tracker,
            cancel_token,
            exchange_config: ExchangeConfig::default(),
        }
    }

    pub fn with_exchange_config(mut self, exchange_config: ExchangeConfig) -> Self {
        self.exchange_config = exchange_config;
        self
    }

    pub fn exchange_config(&self) -> &ExchangeConfig {
        &self.exchange_config
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

        let cap = self.exchange_config.frame_channel_capacity.max(1);
        let (tx, rx) = mpsc::channel(cap);
        let cancel_token = self.cancel_token.clone();

        let worker_task = async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(frame) => {
                        registry
                            .grpc_frames_received
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let inlet_opt = registry.get(frame.exchange_id, frame.target_shard);
                        if let Some(inlet) = inlet_opt {
                            let mut already_reflected = false;
                            if let Some(db) = registry.get_shard_db(frame.target_shard) {
                                match crate::exchange::persistence::committed_frontier(&db).await {
                                    Ok(frontier) => {
                                        already_reflected = frame.epoch <= frontier;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            code = %rockstream_types::error_code::RS_3023,
                                            exchange_id = frame.exchange_id,
                                            target_shard = frame.target_shard,
                                            epoch = frame.epoch,
                                            "failed to read committed frontier for shuffle replay dedup; delivering conservatively: {:?}",
                                            e
                                        );
                                    }
                                }
                            }

                            if !already_reflected {
                                if let Ok(zset) =
                                    deserialize_zset(&frame.payload, inlet.schema.clone())
                                {
                                    if inlet.sender.send(zset).await.is_err() {
                                        tracing::warn!(
                                            exchange_id = frame.exchange_id,
                                            target_shard = frame.target_shard,
                                            "Target inlet receiver dropped"
                                        );
                                    }
                                }
                            }
                        } else {
                            tracing::warn!(
                                exchange_id = frame.exchange_id,
                                target_shard = frame.target_shard,
                                "Received shuffle frame for unregistered target"
                            );
                        }

                        let ack = ShuffleAck {
                            exchange_id: frame.exchange_id,
                            src_shard: frame.src_shard,
                            target_shard: frame.target_shard,
                            epoch: frame.epoch,
                            seq: frame.seq,
                            credit_grant: frame.row_count.max(1),
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
        };

        if let Some(tracker) = &self.task_tracker {
            tracker.spawn(async move {
                if let Some(token) = cancel_token {
                    tokio::select! {
                        _ = token.cancelled() => {}
                        _ = worker_task => {}
                    }
                } else {
                    worker_task.await;
                }
            });
        } else {
            tokio::spawn(worker_task);
        }

        Ok(Response::new(Box::pin(RxStream { rx })))
    }
}

pub fn register_shared_memory_endpoint(
    worker_id: rockstream_types::ids::WorkerId,
    registry: ExchangeRegistry,
) {
    crate::exchange::shared_memory::register_shared_memory_receiver(worker_id, registry);
}

pub fn unregister_shared_memory_endpoint(worker_id: rockstream_types::ids::WorkerId) {
    crate::exchange::shared_memory::unregister_shared_memory_receiver(worker_id);
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
    use crate::exchange::persistence::{delete_outbox_if_present, outbox_key};
    use crate::exchange::pool::ShuffleClientPool;
    use crate::exchange::serialization::{framed_payload_codec, serialize_zset};
    use rockstream_storage::shard_db::ShardDb;
    use rockstream_types::config::{ExchangeConfig, WorkerConfig};
    use rockstream_types::exchange::ShuffleCompression;
    use rockstream_types::ids::WorkerId;
    use rockstream_types::lease::ShardLease;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
        WorkerLocation,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::LazyLock;
    use tokio::sync::Mutex as AsyncMutex;

    /// Same-host shared-memory transport tests read/assert the process-global
    /// `shuffle_shm_bytes_used` / `shuffle_shm_segments_in_use` gauges. Serialize
    /// these tests (async-aware so the guard can be held across `.await`) so
    /// concurrent test threads don't observe each other's transient segment
    /// allocations, mirroring the TEST_LOCK precedent in
    /// `rockstream_types::metrics::tests`.
    static SHM_TEST_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));
    static FLOW_CONTROL_TEST_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

    fn worker_info(worker_id: u64, address: String, host_id: &str, az: &str) -> WorkerInfo {
        worker_info_with_codecs(worker_id, address, host_id, az, true, true)
    }

    fn worker_info_with_codecs(
        worker_id: u64,
        address: String,
        host_id: &str,
        az: &str,
        same_host_shm: bool,
        shuffle_codec: bool,
    ) -> WorkerInfo {
        WorkerInfo {
            worker_id: WorkerId(worker_id),
            role: NodeRole::Worker,
            address,
            capacity_headroom: CapacityHeadroom::FULL,
            location: WorkerLocation::new(host_id, az),
            capabilities: WorkerCapabilities {
                same_host_arrow_shm_v1: same_host_shm,
                shuffle_codec_v1: shuffle_codec,
                checkpoint_manifest_codec_v1: true,
            },
            registered_at_ms: 1,
            healthy: true,
            lifecycle: WorkerLifecycleState::Active,
        }
    }

    async fn shard_db(name: &str) -> (Arc<object_store::memory::InMemory>, ShardDb) {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = ShardDb::builder(name, store.clone()).build().await.unwrap();
        (store, db)
    }

    fn make_sender_shards(
        src_db: ShardDb,
    ) -> Arc<parking_lot::RwLock<HashMap<rockstream_types::ids::ShardId, crate::client::ShardState>>>
    {
        let mut shards = HashMap::new();
        shards.insert(
            rockstream_types::ids::ShardId(0),
            crate::client::ShardState {
                lease: ShardLease::new(
                    rockstream_types::ids::ShardId(0),
                    WorkerId(1),
                    rockstream_types::ids::LeaseToken(1),
                ),
                db: Some(src_db),
            },
        );
        Arc::new(parking_lot::RwLock::new(shards))
    }

    fn make_receiver_registry(target_db: ShardDb) -> (ExchangeRegistry, mpsc::Receiver<ArrowZSet>) {
        let mut shards = HashMap::new();
        shards.insert(
            rockstream_types::ids::ShardId(1),
            crate::client::ShardState {
                lease: ShardLease::new(
                    rockstream_types::ids::ShardId(1),
                    WorkerId(2),
                    rockstream_types::ids::LeaseToken(1),
                ),
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

    async fn read_only_inbox_payload(db: &ShardDb) -> bytes::Bytes {
        let entries = db.scan_prefix(&[0x04]).await.unwrap();
        assert_eq!(entries.len(), 1);
        entries[0].1.clone()
    }

    fn make_wide_compressible_zset(rows: usize) -> ArrowZSet {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
        ]));
        let a_vals = vec![7_i64; rows];
        let b_vals: Vec<i64> = (0..rows).map(|idx| (idx % 8) as i64).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(a_vals)),
                Arc::new(arrow::array::Int64Array::from(b_vals)),
            ],
        )
        .unwrap();
        ArrowZSet::new(batch, vec![1; rows])
    }

    #[derive(Clone, Default)]
    struct DelayedAckShuffleService {
        release_first_ack: Arc<tokio::sync::Notify>,
        frames_seen: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl crate::exchange::proto::shuffle_service_server::ShuffleService for DelayedAckShuffleService {
        type ShuffleStreamStream = Pin<Box<dyn Stream<Item = Result<ShuffleAck, Status>> + Send>>;

        async fn shuffle_stream(
            &self,
            request: Request<Streaming<ShuffleFrame>>,
        ) -> Result<Response<Self::ShuffleStreamStream>, Status> {
            let mut stream = request.into_inner();
            let (tx, rx) = mpsc::channel(8);
            let release_first_ack = self.release_first_ack.clone();
            let frames_seen = self.frames_seen.clone();
            tokio::spawn(async move {
                while let Some(frame) = stream.next().await.transpose()? {
                    let index = frames_seen.fetch_add(1, Ordering::Relaxed);
                    if index == 0 {
                        release_first_ack.notified().await;
                    }
                    tx.send(Ok(ShuffleAck {
                        exchange_id: frame.exchange_id,
                        src_shard: frame.src_shard,
                        target_shard: frame.target_shard,
                        epoch: frame.epoch,
                        seq: frame.seq,
                        credit_grant: frame.row_count.max(1),
                    }))
                    .await
                    .map_err(|_| Status::cancelled("ack receiver dropped"))?;
                }
                Ok::<(), Status>(())
            });
            Ok(Response::new(
                Box::pin(RxStream { rx }) as Self::ShuffleStreamStream
            ))
        }
    }

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
            row_count: zset.num_rows() as u32,
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
            row_count: zset.num_rows() as u32,
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
            row_count: zset.num_rows() as u32,
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
        let flow_controller = FlowController::with_row_budget(16);
        let exchange_id = 100u64;
        let src_shard = 1u32;
        let target_shard = 2u32;

        // Senders start with 16 row credits. We should be able to acquire 16 single-row sends without blocking.
        for _ in 0..16 {
            let fut = flow_controller.acquire_credit(exchange_id, src_shard, target_shard, 1);
            // Must complete immediately
            tokio::time::timeout(std::time::Duration::from_millis(50), fut)
                .await
                .unwrap()
                .unwrap();
        }

        // Now credits are 0. The next acquire_credit must block/suspend.
        let fc_clone = flow_controller.clone();
        let (tx_done, mut rx_done) = mpsc::channel(1);

        let handle = tokio::spawn(async move {
            fc_clone
                .acquire_credit(exchange_id, src_shard, target_shard, 1)
                .await
                .unwrap();
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
    async fn ack_releases_rows_not_frames() {
        let _guard = FLOW_CONTROL_TEST_LOCK.lock().await;
        rockstream_types::metrics::reset_all();
        let flow_controller = FlowController::with_row_budget(8);
        flow_controller.acquire_credit(7, 1, 2, 5).await.unwrap();
        assert_eq!(flow_controller.get_credits(7, 1, 2), 3);
        assert_eq!(flow_controller.rows_in_flight(7, 1, 2), 5);
        assert_eq!(rockstream_types::metrics::read_shuffle_rows_in_flight(), 5);

        flow_controller.handle_ack(&ShuffleAck {
            exchange_id: 7,
            src_shard: 1,
            target_shard: 2,
            epoch: 1,
            seq: 1,
            credit_grant: 5,
        });
        assert_eq!(flow_controller.get_credits(7, 1, 2), 8);
        assert_eq!(flow_controller.rows_in_flight(7, 1, 2), 0);
        assert_eq!(rockstream_types::metrics::read_shuffle_rows_in_flight(), 0);
    }

    #[tokio::test]
    async fn send_frame_blocks_once_rows_in_flight_reach_max_rows_per_quantum() {
        let _guard = FLOW_CONTROL_TEST_LOCK.lock().await;
        rockstream_types::metrics::reset_all();
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let service = DelayedAckShuffleService::default();
        let release_first_ack = service.release_first_ack.clone();
        let frames_seen = service.frames_seen.clone();
        let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    crate::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(
                        service,
                    ),
                )
                .serve_with_shutdown(addr, async {
                    let _ = rx_close.await;
                })
                .await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(502), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            501,
            "127.0.0.1:9501".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(502, addr.to_string(), "host-b", "az-1"));
        let flow_controller = FlowController::with_row_budget(2);
        let multiplexer = WorkerStreamMultiplexer::new(pool, flow_controller.clone())
            .with_worker_config(WorkerConfig {
                segment_cache_bytes: 0,
                max_rows_per_quantum: 2,
            });

        let first = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
        let second = ArrowZSet::from_ab_rows(&[(3, 30)], 1);
        multiplexer
            .send_frame(
                WorkerId(502),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: serialize_zset(&first).unwrap().into(),
                    row_count: first.num_rows() as u32,
                },
            )
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(200), async {
            while frames_seen.load(Ordering::Relaxed) < 1 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(flow_controller.rows_in_flight(100, 0, 1), 2);

        let blocked = tokio::spawn({
            let multiplexer = multiplexer.clone();
            let payload = serialize_zset(&second).unwrap();
            async move {
                multiplexer
                    .send_frame(
                        WorkerId(502),
                        ShuffleFrame {
                            exchange_id: 100,
                            src_shard: 0,
                            target_shard: 1,
                            epoch: 1,
                            seq: 2,
                            payload: payload.into(),
                            row_count: second.num_rows() as u32,
                        },
                    )
                    .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(frames_seen.load(Ordering::Relaxed), 1);

        release_first_ack.notify_waiters();
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), blocked)
            .await
            .unwrap()
            .unwrap();
        result.unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(200), async {
            while flow_controller.rows_in_flight(100, 0, 1) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let _ = tx_close.send(());
        server_handle.abort();
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

        // Fast-path shuffle WAL elision (v0.51): the loopback fast path no
        // longer persists a `shuffle_inbox/` (0x04) or `shuffle_outbox/` (0x05)
        // key. Recovery relies on the checkpoint frontier + source replay, not
        // on fast-path shuffle WAL.
        let inbox_prefix = [0x04];
        let outbox_prefix = [0x05];
        // Drain the loopback delivery so the assertion is deterministic.
        let _ = _inlet_rx.recv().await.unwrap();
        assert_eq!(
            db2.scan_prefix(&inbox_prefix).await.unwrap().len(),
            0,
            "Loopback must not write a shuffle_inbox WAL entry on the fast path"
        );
        assert_eq!(
            db1.scan_prefix(&outbox_prefix).await.unwrap().len(),
            0,
            "Loopback must not write a shuffle_outbox WAL entry on the fast path"
        );

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
            row_count: zset.num_rows() as u32,
        };
        multiplexer.send_frame(WorkerId(2), frame2).await.unwrap();
        let received200 = inlet_rx200.recv().await.unwrap();
        assert_eq!(received200.positive_ab_rows(), vec![(5, 500)]);

        // Direct gRPC fast path also elides shuffle WAL on both sides.
        assert_eq!(
            db2.scan_prefix(&inbox_prefix).await.unwrap().len(),
            0,
            "Direct gRPC must not write a shuffle_inbox WAL entry on the fast path"
        );
        assert_eq!(
            db1.scan_prefix(&outbox_prefix).await.unwrap().len(),
            0,
            "Direct gRPC must not write a shuffle_outbox WAL entry on the fast path"
        );

        // --- 3. Crash-Replay verification ---
        // Close both databases to simulate a crash/shutdown.
        db1.close().await.unwrap();
        db2.close().await.unwrap();

        // Reopen them (simulating recovery replay): no elided fast-path shuffle
        // WAL should reappear.
        let db1_recovered = ShardDb::builder("db", store1).build().await.unwrap();
        let db2_recovered = ShardDb::builder("db", store2).build().await.unwrap();
        assert_eq!(
            db2_recovered
                .scan_prefix(&inbox_prefix)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            db1_recovered
                .scan_prefix(&outbox_prefix)
                .await
                .unwrap()
                .len(),
            0
        );

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

    #[tokio::test]
    async fn same_host_shared_memory_fast_path_delivers_exact_rows() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-shm-fast").await;
        let sender_shards = make_sender_shards(src_db.clone());
        let (registry, mut inlet_rx) = make_receiver_registry(shard_db("dst-shm-fast").await.1);
        register_shared_memory_endpoint(WorkerId(102), registry.clone());

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            101,
            "127.0.0.1:8101".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            102,
            "127.0.0.1:8102".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(101));

        let zset = ArrowZSet::from_ab_rows(&[(7, 70), (8, 80)], 1);
        let payload = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(102),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();

        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(7, 70), (8, 80)]);
        assert_eq!(
            rockstream_types::metrics::read_shuffle_shm_segments_in_use(),
            0
        );
        assert_eq!(rockstream_types::metrics::read_shuffle_shm_bytes_used(), 0);
        unregister_shared_memory_endpoint(WorkerId(102));
    }

    #[tokio::test]
    async fn same_host_shared_memory_fast_path_avoids_shuffle_wal_and_delivers_exact_rows() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-shm-noshufwal").await;
        let sender_shards = make_sender_shards(src_db.clone());
        let (_dst_store, target_db) = shard_db("dst-shm-noshufwal").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        register_shared_memory_endpoint(WorkerId(122), registry.clone());

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            121,
            "127.0.0.1:8121".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            122,
            "127.0.0.1:8122".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(121));

        let zset = ArrowZSet::from_ab_rows(&[(7, 70), (8, 80), (9, 90)], 1);
        let payload = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(122),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();

        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(7, 70), (8, 80), (9, 90)]);
        // Slice 2: the same-host shared-memory fast path writes neither the
        // receiver inbox (0x04) nor the sender outbox (0x05) on success.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);
        // No leaked shared-memory segments.
        assert_eq!(
            rockstream_types::metrics::read_shuffle_shm_segments_in_use(),
            0
        );
        assert_eq!(rockstream_types::metrics::read_shuffle_shm_bytes_used(), 0);
        unregister_shared_memory_endpoint(WorkerId(122));
    }

    #[tokio::test]
    async fn direct_grpc_fast_path_avoids_shuffle_wal_and_delivers_exact_rows() {
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (_src_store, src_db) = shard_db("src-direct-noshufwal").await;
        let sender_shards = make_sender_shards(src_db.clone());
        let (_dst_store, target_db) = shard_db("dst-direct-noshufwal").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        let server = ShuffleServer::new(registry);
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(132), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        // No shared-memory / same-host locality -> forces the direct-gRPC path.
        pool.set_local_worker_info(worker_info(
            131,
            "127.0.0.1:9131".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(132, addr.to_string(), "host-b", "az-1"));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(131));

        let zset = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], 1);
        let payload = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(132),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();

        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(
            received.positive_ab_rows(),
            vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
        );
        // Slice 1: the direct-gRPC fast path writes neither the receiver inbox
        // (0x04) nor the sender outbox (0x05) for a successfully delivered frame.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn same_host_shared_memory_never_opens_a_grpc_payload_stream() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-shm-nogrpc").await;
        let sender_shards = make_sender_shards(src_db);
        let (registry, mut inlet_rx) = make_receiver_registry(shard_db("dst-shm-nogrpc").await.1);
        register_shared_memory_endpoint(WorkerId(104), registry.clone());

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            103,
            "127.0.0.1:8201".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            104,
            "127.0.0.1:8202".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(103));

        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(1, 10)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(104),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 2,
                    payload: payload.into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let _ = inlet_rx.recv().await.unwrap();
        assert_eq!(registry.grpc_frames_received(), 0);
        unregister_shared_memory_endpoint(WorkerId(104));
    }

    #[tokio::test]
    async fn shared_memory_segment_pool_enforces_bound_and_falls_back_to_grpc() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (_src_store, src_db) = shard_db("src-shm-fallback").await;
        let sender_shards = make_sender_shards(src_db);
        let (registry, mut inlet_rx) = make_receiver_registry(shard_db("dst-shm-fallback").await.1);
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(2), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            1,
            "127.0.0.1:8301".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(2, addr.to_string(), "host-a", "az-1"));
        let config = ExchangeConfig {
            same_host_shm_segment_bytes: 1,
            ..ExchangeConfig::default()
        };
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(1))
                .with_exchange_config(config);

        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(2),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 3,
                    payload: payload.into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(5, 50)]);
        assert_eq!(registry.grpc_frames_received(), 1);
        assert_eq!(
            rockstream_types::metrics::read_shuffle_shm_segments_in_use(),
            0
        );
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn same_host_shared_memory_fast_path_writes_no_outbox() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-shm-ack").await;
        let sender_shards = make_sender_shards(src_db.clone());
        let (registry, mut inlet_rx) = make_receiver_registry(shard_db("dst-shm-ack").await.1);
        register_shared_memory_endpoint(WorkerId(106), registry);

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            105,
            "127.0.0.1:8401".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            106,
            "127.0.0.1:8402".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(105));

        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(9, 90)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(106),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 4,
                    payload: payload.into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let _ = inlet_rx.recv().await.unwrap();
        assert!(!delete_outbox_if_present(&src_db, 100, 1, 1, 4)
            .await
            .unwrap());
        assert!(src_db
            .get(&outbox_key(100, 1, 1, 4))
            .await
            .unwrap()
            .is_none());
        unregister_shared_memory_endpoint(WorkerId(106));
    }

    #[tokio::test]
    async fn same_host_shared_memory_duplicate_delivery_is_deduped_by_frontier() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-shm-dedupe").await;
        let sender_shards = make_sender_shards(src_db);
        let (_dst_store, target_db) = shard_db("dst-shm-dedupe").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        register_shared_memory_endpoint(WorkerId(108), registry);

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            107,
            "127.0.0.1:8501".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            108,
            "127.0.0.1:8502".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(107));

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
            .send_frame(WorkerId(108), frame.clone())
            .await
            .unwrap();
        assert_eq!(
            inlet_rx.recv().await.unwrap().positive_ab_rows(),
            vec![(11, 110)]
        );

        // Simulate the target operator checkpointing epoch 1: the committed
        // frontier now reflects this delivery. With fast-path WAL elision, a
        // re-sent frame at epoch <= committed frontier is deduped by frontier
        // (no persisted inbox key is involved).
        target_db
            .commit_epoch(rockstream_types::ids::ShardId(1), 1)
            .await
            .unwrap();

        multiplexer.send_frame(WorkerId(108), frame).await.unwrap();
        tokio::select! {
            maybe = inlet_rx.recv() => panic!("unexpected duplicate delivery: {:?}", maybe),
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
        unregister_shared_memory_endpoint(WorkerId(108));
    }

    #[tokio::test]
    async fn same_host_shared_memory_replay_deduped_by_frontier_after_restart() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (src_store, src_db) = shard_db("src-shm-replay").await;
        let sender_shards = make_sender_shards(src_db.clone());
        let (target_store, target_db) = shard_db("dst-shm-replay").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        register_shared_memory_endpoint(WorkerId(110), registry);

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            109,
            "127.0.0.1:8601".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            110,
            "127.0.0.1:8602".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(109));

        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(12, 120)], 1)).unwrap();
        let frame = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq: 6,
            payload: payload.into(),
            row_count: 1,
        };
        multiplexer
            .send_frame(WorkerId(110), frame.clone())
            .await
            .unwrap();
        assert_eq!(
            inlet_rx.recv().await.unwrap().positive_ab_rows(),
            vec![(12, 120)]
        );

        // The target operator checkpoints epoch 1: its committed frontier is
        // durably advanced. No `shuffle_inbox/` key exists (fast-path elision).
        target_db
            .commit_epoch(rockstream_types::ids::ShardId(1), 1)
            .await
            .unwrap();
        // No fast-path shuffle WAL was written on either side.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);

        unregister_shared_memory_endpoint(WorkerId(110));
        src_db.close().await.unwrap();
        target_db.close().await.unwrap();
        let reopened_src = ShardDb::builder("src-shm-replay", src_store)
            .build()
            .await
            .unwrap();
        let reopened_sender = make_sender_shards(reopened_src);
        let reopened_target = ShardDb::builder("dst-shm-replay", target_store)
            .build()
            .await
            .unwrap();
        let (registry2, mut inlet_rx2) = make_receiver_registry(reopened_target);
        register_shared_memory_endpoint(WorkerId(110), registry2);

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            109,
            "127.0.0.1:8601".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            110,
            "127.0.0.1:8602".to_string(),
            "host-a",
            "az-1",
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), reopened_sender)
                .with_src_worker(WorkerId(109));
        // Source replays epoch 1 after restart. Because the restored committed
        // frontier already reflects epoch 1, the receiver dedups by frontier and
        // does NOT re-deliver.
        multiplexer.send_frame(WorkerId(110), frame).await.unwrap();
        tokio::select! {
            maybe = inlet_rx2.recv() => {
                panic!("unexpected replay delivery after restart: {:?}", maybe)
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
        unregister_shared_memory_endpoint(WorkerId(110));
    }

    #[tokio::test]
    async fn loopback_fast_path_delivers_lz4_and_writes_no_wal() {
        use crate::exchange::serialization::serialize_zset_with_compression;
        let (_src_store, src_db) = shard_db("src-loopback-lz4").await;
        let (_dst_store, target_db) = shard_db("dst-loopback-lz4").await;
        let active_shards = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        active_shards.write().insert(
            rockstream_types::ids::ShardId(0),
            crate::client::ShardState {
                lease: ShardLease::new(
                    rockstream_types::ids::ShardId(0),
                    WorkerId(1),
                    rockstream_types::ids::LeaseToken(1),
                ),
                db: Some(src_db.clone()),
            },
        );
        active_shards.write().insert(
            rockstream_types::ids::ShardId(1),
            crate::client::ShardState {
                lease: ShardLease::new(
                    rockstream_types::ids::ShardId(1),
                    WorkerId(1),
                    rockstream_types::ids::LeaseToken(1),
                ),
                db: Some(target_db.clone()),
            },
        );
        let registry = ExchangeRegistry::with_shards(active_shards.clone());
        let (tx, mut rx) = mpsc::channel(10);
        registry.register(
            100,
            1,
            tx,
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
            ])),
        );
        let router = crate::exchange::loopback::LoopbackRouter::new(registry, active_shards);
        let zset = ArrowZSet::from_ab_rows(&[(21, 210)], 1);
        router.route_loopback(100, 0, 1, 1, 1, &zset).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(21, 210)]);

        // Fast-path shuffle WAL elision: loopback writes neither inbox (0x04)
        // nor outbox (0x05).
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);

        // The loopback path still uses the LZ4 shuffle codec for its wire-format
        // parity check with the network paths.
        let framed = serialize_zset_with_compression(&zset, ShuffleCompression::Lz4, true).unwrap();
        assert_eq!(framed_payload_codec(&framed), Some(ShuffleCompression::Lz4));
    }

    #[tokio::test]
    async fn direct_grpc_writes_lz4_payloads_when_codec_capability_is_present() {
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (_src_store, src_db) = shard_db("src-direct-lz4").await;
        let sender_shards = make_sender_shards(src_db);
        let (_dst_store, target_db) = shard_db("dst-direct-lz4").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        let server = ShuffleServer::new(registry);
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(202), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            201,
            "127.0.0.1:9201".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            202,
            addr.to_string(),
            "host-b",
            "az-1",
            false,
            true,
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(201));
        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(22, 220)], 1)).unwrap();
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
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(22, 220)]);
        // Fast-path elision: no inbox (0x04) is written on the receiver.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        // The direct-gRPC wire frame uses the LZ4 shuffle codec when the peer
        // advertises codec capability.
        let framed = crate::exchange::serialization::serialize_zset_with_compression(
            &ArrowZSet::from_ab_rows(&[(22, 220)], 1),
            ShuffleCompression::Lz4,
            true,
        )
        .unwrap();
        assert_eq!(framed_payload_codec(&framed), Some(ShuffleCompression::Lz4));
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn durable_shuffle_writer_writes_zstd_payloads() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let (_src_store, src_db) = shard_db("src-durable-zstd").await;
        let sender_shards = make_sender_shards(src_db);
        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            301,
            "127.0.0.1:9301".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            302,
            "127.0.0.1:9302".to_string(),
            "host-b",
            "az-2",
            false,
            true,
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_object_store(store.clone())
                .with_src_worker(WorkerId(301));
        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(23, 230)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(302),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let path = object_store::path::Path::from("shuffle/100/1/301/302");
        let footer =
            crate::exchange::durable::DurableShuffleReader::read_footer(store.as_ref(), &path)
                .await
                .unwrap();
        let stored = crate::exchange::durable::DurableShuffleReader::read_frame(
            store.as_ref(),
            &path,
            &footer.entries[0],
        )
        .await
        .unwrap();
        assert_eq!(
            framed_payload_codec(&stored),
            Some(ShuffleCompression::Zstd)
        );
    }

    #[tokio::test]
    async fn legacy_peer_receives_raw_ipc_until_capability_floor_advances() {
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (_src_store, src_db) = shard_db("src-direct-legacy").await;
        let sender_shards = make_sender_shards(src_db);
        let (_dst_store, target_db) = shard_db("dst-direct-legacy").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        let server = ShuffleServer::new(registry);
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(402), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            401,
            "127.0.0.1:9401".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            402,
            addr.to_string(),
            "host-b",
            "az-1",
            false,
            false,
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(401));
        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(24, 240)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(402),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.clone().into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(received.positive_ab_rows(), vec![(24, 240)]);
        // Fast-path elision: no inbox (0x04) key is written.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        // A legacy peer without codec capability receives the raw IPC payload
        // (no shuffle framing) on the wire.
        assert_eq!(
            crate::exchange::serialization::frame_payload_bytes(
                &payload,
                ShuffleCompression::Lz4,
                false,
            )
            .unwrap()
            .as_ref(),
            &payload[..]
        );
        assert_eq!(framed_payload_codec(&payload), None);
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn mixed_legacy_and_compressed_shuffle_frames_replay_exactly_once() {
        let store = object_store::memory::InMemory::new();
        let path = object_store::path::Path::from("shuffle/100/1/501/502");
        let raw = serialize_zset(&ArrowZSet::from_ab_rows(&[(31, 310)], 1)).unwrap();
        let compressed = crate::exchange::serialization::frame_payload_bytes(
            &serialize_zset(&ArrowZSet::from_ab_rows(&[(32, 320)], 1)).unwrap(),
            ShuffleCompression::Zstd,
            true,
        )
        .unwrap();
        let mut writer = crate::exchange::durable::DurableShuffleWriter::new();
        writer.add_frame(0, 1, 1, &raw).unwrap();
        writer.add_frame(0, 1, 2, &compressed).unwrap();
        writer.finish(&store, &path).await.unwrap();

        let (_target_store, target_db) = shard_db("dst-mixed-replay").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db);
        let multiplexer = WorkerStreamMultiplexer::new(
            ShuffleClientPool::new(Arc::new(parking_lot::RwLock::new(HashMap::new()))),
            FlowController::new(),
        );
        multiplexer
            .catch_up_durable(100, 1, WorkerId(501), WorkerId(502), &registry, &store)
            .await
            .unwrap();
        assert_eq!(
            inlet_rx.recv().await.unwrap().positive_ab_rows(),
            vec![(31, 310)]
        );
        assert_eq!(
            inlet_rx.recv().await.unwrap().positive_ab_rows(),
            vec![(32, 320)]
        );
        multiplexer
            .catch_up_durable(100, 1, WorkerId(501), WorkerId(502), &registry, &store)
            .await
            .unwrap();
        tokio::select! {
            maybe = inlet_rx.recv() => panic!("unexpected duplicate replay delivery: {:?}", maybe),
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
    }

    #[tokio::test]
    async fn cross_az_peers_never_open_direct_grpc_streams() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let (_src_store, src_db) = shard_db("src-cross-az").await;
        let sender_shards = make_sender_shards(src_db);
        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            601,
            "127.0.0.1:9601".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            602,
            "127.0.0.1:9602".to_string(),
            "host-b",
            "az-2",
            false,
            true,
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_object_store(store.clone())
                .with_src_worker(WorkerId(601));
        let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(41, 410)], 1)).unwrap();
        multiplexer
            .send_frame(
                WorkerId(602),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 1,
                    payload: payload.into(),
                    row_count: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(multiplexer.connection_count(), 0);
        let footer = crate::exchange::durable::DurableShuffleReader::read_footer(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/601/602"),
        )
        .await
        .unwrap();
        assert_eq!(footer.entries.len(), 1);
    }

    #[tokio::test]
    async fn proof_wide_shuffle_network_bytes_drop_over_40_percent() {
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (_src_store, src_db) = shard_db("src-proof-wide-lz4").await;
        let sender_shards = make_sender_shards(src_db);
        let (_dst_store, target_db) = shard_db("dst-proof-wide-lz4").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());
        let server = ShuffleServer::new(registry);
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        peers.write().insert(WorkerId(902), addr.to_string());
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            901,
            "127.0.0.1:9901".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            902,
            addr.to_string(),
            "host-b",
            "az-1",
            false,
            true,
        ));
        let exchange_config = ExchangeConfig {
            exchange_direct_threshold_bytes: usize::MAX,
            ..ExchangeConfig::default()
        };
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(901))
                .with_exchange_config(exchange_config)
                .with_worker_config(WorkerConfig {
                    segment_cache_bytes: 0,
                    max_rows_per_quantum: 16_384,
                });
        let zset = make_wide_compressible_zset(16_384);
        let expected = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(902),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 11,
                    payload: expected.clone().into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(serialize_zset(&received).unwrap(), expected);
        // Fast-path elision: no inbox (0x04) key is written on the receiver.
        assert_eq!(target_db.scan_prefix(&[0x04]).await.unwrap().len(), 0);
        // The wire frame the sender emits for a codec-capable peer uses LZ4 and
        // drops the on-wire byte count by well over 40% for wide, compressible
        // batches.
        let encoded = crate::exchange::serialization::frame_payload_bytes(
            &expected,
            ShuffleCompression::Lz4,
            true,
        )
        .unwrap();
        assert_eq!(
            framed_payload_codec(&encoded),
            Some(ShuffleCompression::Lz4)
        );
        let raw = expected.len() as f64;
        let saved = (expected.len() - encoded.len()) as f64;
        assert!(
            saved / raw > 0.40,
            "expected >40% byte savings, raw={raw}, saved={saved}"
        );
        let _ = tx_close.send(());
        server_handle.abort();
    }

    #[tokio::test]
    async fn proof_cross_az_direct_bytes_drop_to_near_zero() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let (_src_store, src_db) = shard_db("src-proof-cross-az").await;
        let sender_shards = make_sender_shards(src_db);
        let (_dst_store, target_db) = shard_db("dst-proof-cross-az").await;
        let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            911,
            "127.0.0.1:9911".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info_with_codecs(
            912,
            "127.0.0.1:9912".to_string(),
            "host-b",
            "az-2",
            false,
            true,
        ));
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_object_store(store.clone())
                .with_src_worker(WorkerId(911))
                .with_worker_config(WorkerConfig {
                    segment_cache_bytes: 0,
                    max_rows_per_quantum: 8_192,
                });
        let zset = make_wide_compressible_zset(8_192);
        let expected = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(912),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 12,
                    payload: expected.clone().into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();
        assert_eq!(multiplexer.connection_count(), 0);
        let path = object_store::path::Path::from("shuffle/100/1/911/912");
        let footer =
            crate::exchange::durable::DurableShuffleReader::read_footer(store.as_ref(), &path)
                .await
                .unwrap();
        let stored = crate::exchange::durable::DurableShuffleReader::read_frame(
            store.as_ref(),
            &path,
            &footer.entries[0],
        )
        .await
        .unwrap();
        assert_eq!(
            framed_payload_codec(&stored),
            Some(ShuffleCompression::Zstd)
        );
        multiplexer
            .catch_up_durable(
                100,
                1,
                WorkerId(911),
                WorkerId(912),
                &registry,
                store.as_ref(),
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(serialize_zset(&received).unwrap(), expected);
        assert_eq!(
            framed_payload_codec(&read_only_inbox_payload(&target_db).await),
            Some(ShuffleCompression::Zstd)
        );
    }

    #[tokio::test]
    async fn proof_same_host_shared_memory_cpu_profile_reports_zero_copies() {
        let _shm_guard = SHM_TEST_LOCK.lock().await;
        let (_src_store, src_db) = shard_db("src-proof-shm").await;
        let sender_shards = make_sender_shards(src_db);
        let (registry, mut inlet_rx) = make_receiver_registry(shard_db("dst-proof-shm").await.1);
        register_shared_memory_endpoint(WorkerId(922), registry.clone());

        let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let pool = ShuffleClientPool::new(peers);
        pool.set_local_worker_info(worker_info(
            921,
            "127.0.0.1:9921".to_string(),
            "host-a",
            "az-1",
        ));
        pool.upsert_peer_info(worker_info(
            922,
            "127.0.0.1:9922".to_string(),
            "host-a",
            "az-1",
        ));
        let exchange_config = ExchangeConfig {
            exchange_direct_threshold_bytes: usize::MAX,
            ..ExchangeConfig::default()
        };
        let multiplexer =
            WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
                .with_src_worker(WorkerId(921))
                .with_exchange_config(exchange_config)
                .with_worker_config(WorkerConfig {
                    segment_cache_bytes: 0,
                    max_rows_per_quantum: 8_192,
                });
        let zset = make_wide_compressible_zset(8_192);
        let expected = serialize_zset(&zset).unwrap();
        multiplexer
            .send_frame(
                WorkerId(922),
                ShuffleFrame {
                    exchange_id: 100,
                    src_shard: 0,
                    target_shard: 1,
                    epoch: 1,
                    seq: 13,
                    payload: expected.clone().into(),
                    row_count: zset.num_rows() as u32,
                },
            )
            .await
            .unwrap();
        let received = inlet_rx.recv().await.unwrap();
        assert_eq!(serialize_zset(&received).unwrap(), expected);
        assert_eq!(registry.grpc_frames_received(), 0);
        unregister_shared_memory_endpoint(WorkerId(922));
    }

    #[test]
    fn test_frame_channel_row_budget_sizing() {
        let registry = ExchangeRegistry::new();
        let config = ExchangeConfig {
            frame_channel_capacity: 512,
            ..ExchangeConfig::default()
        };
        let service = ExchangeService::new(registry.clone()).with_exchange_config(config.clone());
        assert_eq!(service.exchange_config().frame_channel_capacity, 512);

        let server = ShuffleServer::new(registry).with_exchange_config(config);
        assert_eq!(server.exchange_config().frame_channel_capacity, 512);
    }
}
