use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use rockstream_types::config::{ExchangeConfig, WorkerConfig};
use rockstream_types::exchange::{
    ExchangeAnn, ExchangePath, ExchangeTransport, ShuffleCompression,
};
use tokio::sync::mpsc;
use tonic::{metadata::MetadataValue, Request};

use crate::exchange::pool::ShuffleClientPool;
use crate::exchange::proto::ShuffleFrame;
use crate::exchange::shared_memory::{ticket, SharedMemorySegmentPool};
use rockstream_types::ids::{ExchangeId, ShardId, WorkerId};

/// Stream wrapper to adapt mpsc::Receiver to Stream for tonic.
struct RxStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> Stream for RxStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Multiplexes outgoing shuffle frames to peer workers over pooled bidirectional streams.
#[derive(Clone)]
pub struct WorkerStreamMultiplexer {
    client_pool: ShuffleClientPool,
    flow_controller: crate::exchange::flow_control::FlowController,
    active_shards: Arc<RwLock<HashMap<rockstream_types::ids::ShardId, crate::client::ShardState>>>,
    streams: Arc<Mutex<HashMap<WorkerId, mpsc::Sender<ShuffleFrame>>>>,
    object_store: Option<Arc<dyn ObjectStore>>,
    src_worker: Option<WorkerId>,
    exchange_config: ExchangeConfig,
    shared_memory: SharedMemorySegmentPool,
    task_tracker: TaskTracker,
    cancel_token: CancellationToken,
}

impl WorkerStreamMultiplexer {
    /// Create a new multiplexer.
    pub fn new(
        client_pool: ShuffleClientPool,
        flow_controller: crate::exchange::flow_control::FlowController,
    ) -> Self {
        WorkerStreamMultiplexer {
            client_pool,
            flow_controller,
            active_shards: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            object_store: None,
            src_worker: None,
            exchange_config: ExchangeConfig::default(),
            shared_memory: SharedMemorySegmentPool::new(ExchangeConfig::default()),
            task_tracker: TaskTracker::new(),
            cancel_token: CancellationToken::new(),
        }
    }

    /// Create a new multiplexer with shard tracking.
    pub fn with_shards(
        client_pool: ShuffleClientPool,
        flow_controller: crate::exchange::flow_control::FlowController,
        active_shards: Arc<
            RwLock<HashMap<rockstream_types::ids::ShardId, crate::client::ShardState>>,
        >,
    ) -> Self {
        WorkerStreamMultiplexer {
            client_pool,
            flow_controller,
            active_shards,
            streams: Arc::new(Mutex::new(HashMap::new())),
            object_store: None,
            src_worker: None,
            exchange_config: ExchangeConfig::default(),
            shared_memory: SharedMemorySegmentPool::new(ExchangeConfig::default()),
            task_tracker: TaskTracker::new(),
            cancel_token: CancellationToken::new(),
        }
    }

    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    pub fn task_tracker(&self) -> &TaskTracker {
        &self.task_tracker
    }

    pub async fn shutdown(&self) {
        self.cancel_token.cancel();
        self.task_tracker.close();
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.task_tracker.wait()).await;
    }

    /// Add an object store for durable fallback.
    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Add a source worker ID.
    pub fn with_src_worker(mut self, src_worker: WorkerId) -> Self {
        self.src_worker = Some(src_worker);
        self
    }

    pub fn with_exchange_config(mut self, exchange_config: ExchangeConfig) -> Self {
        self.shared_memory = SharedMemorySegmentPool::new(exchange_config.clone());
        self.exchange_config = exchange_config;
        self
    }

    pub fn with_worker_config(self, worker_config: WorkerConfig) -> Self {
        self.flow_controller
            .set_row_budget(worker_config.max_rows_per_quantum as u32);
        self
    }

    /// Retrieve the ShardDb for a given shard.
    pub fn get_shard_db(&self, shard_id: u32) -> Option<rockstream_storage::shard_db::ShardDb> {
        let shards = self.active_shards.read();
        shards
            .get(&rockstream_types::ids::ShardId(shard_id as u64))
            .and_then(|state| state.db.clone())
    }

    /// Helper to retrieve the ObjectStore.
    fn get_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        if let Some(store) = &self.object_store {
            return Some(store.clone());
        }
        let shards = self.active_shards.read();
        shards
            .values()
            .find_map(|state| state.db.as_ref().map(|db| db.object_store()))
    }

    /// Helper to retrieve the src worker ID.
    fn get_src_worker(&self) -> Option<WorkerId> {
        if let Some(wid) = self.src_worker {
            return Some(wid);
        }
        let shards = self.active_shards.read();
        shards.values().map(|state| state.lease.worker_id).next()
    }

    fn classify_route(
        &self,
        target_worker: WorkerId,
        frame: &ShuffleFrame,
    ) -> crate::exchange::classifier::ResolvedExchangeRoute {
        let src_worker = self.get_src_worker().unwrap_or(WorkerId(0));
        let local_worker = self.client_pool.local_worker_info();
        let peer_worker = self.client_pool.peer_info(target_worker);
        crate::exchange::classifier::classify_exchange(
            crate::exchange::classifier::ExchangeClassificationInput {
                ann: &ExchangeAnn {
                    exchange_id: ExchangeId(frame.exchange_id),
                    law_id: None,
                    source_shard: ShardId(frame.src_shard as u64),
                    target_shard: ShardId(frame.target_shard as u64),
                    source_worker: src_worker,
                    target_worker,
                    path: if src_worker == target_worker {
                        ExchangePath::Loopback
                    } else {
                        ExchangePath::Direct
                    },
                },
                local_worker: local_worker.as_ref(),
                peer_worker: peer_worker.as_ref(),
                receiver_reachable: true,
                batch_bytes: frame.payload.len(),
                epoch_exchange_bytes: frame.payload.len() as u64,
                config: &self.exchange_config,
            },
        )
    }

    fn encode_payload_for_route(
        &self,
        target_worker: WorkerId,
        route: &crate::exchange::classifier::ResolvedExchangeRoute,
        payload: &[u8],
    ) -> Result<bytes::Bytes, String> {
        let codec_capability_floor =
            matches!(route.path, ExchangePath::Loopback | ExchangePath::Elided)
                || self
                    .client_pool
                    .peer_info(target_worker)
                    .map(|worker| worker.capabilities.shuffle_codec_v1)
                    .unwrap_or(false);
        if matches!(route.compression, ShuffleCompression::None) {
            return Ok(bytes::Bytes::copy_from_slice(payload));
        }
        crate::exchange::serialization::frame_payload_bytes(
            payload,
            route.compression,
            codec_capability_floor,
        )
    }

    /// Send a frame to a target worker, establishing the stream if necessary.
    /// Falls back to the durable object store path if gRPC connectivity fails.
    pub async fn send_frame(
        &self,
        target_worker: WorkerId,
        frame: ShuffleFrame,
    ) -> Result<(), String> {
        let permit = self
            .flow_controller
            .acquire_batch_permit(
                frame.exchange_id,
                frame.src_shard,
                frame.target_shard,
                frame.payload.len(),
                frame.row_count,
            )
            .await?;
        self.send_frame_after_credit(target_worker, frame, Some(permit))
            .await
    }

    async fn send_frame_after_credit(
        &self,
        target_worker: WorkerId,
        frame: ShuffleFrame,
        mut permit: Option<crate::exchange::flow_control::FlowPermit>,
    ) -> Result<(), String> {
        let route = self.classify_route(target_worker, &frame);
        let encoded_payload =
            self.encode_payload_for_route(target_worker, &route, &frame.payload)?;
        if matches!(
            route.transport,
            ExchangeTransport::Grpc | ExchangeTransport::SharedMemory
        ) {
            rockstream_types::metrics::add_shuffle_direct_bytes_total(frame.payload.len() as u64);
            if matches!(
                route.locality,
                crate::exchange::classifier::PeerLocality::CrossAvailabilityZone
            ) {
                rockstream_types::metrics::add_shuffle_cross_az_direct_bytes_total(
                    frame.payload.len() as u64,
                );
            }
        }
        if encoded_payload.len() < frame.payload.len() {
            let saved = (frame.payload.len() - encoded_payload.len()) as u64;
            match route.compression {
                ShuffleCompression::Lz4 => {
                    rockstream_types::metrics::add_shuffle_lz4_bytes_saved_total(saved)
                }
                ShuffleCompression::Zstd => {
                    rockstream_types::metrics::add_shuffle_zstd_bytes_saved_total(saved)
                }
                ShuffleCompression::None => {}
            }
        }
        let frame = ShuffleFrame {
            payload: encoded_payload.into(),
            ..frame
        };
        if route.metadata_fallback {
            tracing::warn!(
                code = %rockstream_types::error_code::RS_3021,
                exchange_id = frame.exchange_id,
                target_worker = %target_worker,
                "exchange route used safe fallback because worker locality metadata was unavailable"
            );
        }
        // Fast-path shuffle WAL elision (v0.51, Slice 1): a successfully
        // delivered direct/shared-memory/loopback frame is NOT persisted to the
        // local `shuffle_outbox/`. Recovery relies on the checkpoint frontier +
        // source replay + durable object-store fallback, not on fast-path WAL
        // entries. Only the durable fallback below persists (to the object
        // store), which remains the explicit recovery path.

        let mut fast_path_ok = false;

        if matches!(route.transport, ExchangeTransport::SharedMemory) {
            match self.send_shared_memory_frame(target_worker, &frame).await {
                Ok(_) => {
                    fast_path_ok = true;
                }
                Err(error) => {
                    tracing::warn!(
                        code = %rockstream_types::error_code::RS_3019,
                        target_worker = %target_worker,
                        error = %error,
                        "shared-memory exchange failed; falling back to gRPC"
                    );
                }
            }
        }

        if !fast_path_ok && !matches!(route.transport, ExchangeTransport::DurableObject) {
            let tx_opt = {
                let streams = self.streams.lock();
                streams.get(&target_worker).cloned()
            };

            if let Some(tx) = tx_opt {
                if let Ok(slot) = tx.reserve().await {
                    permit.take().unwrap().handoff(frame.epoch, frame.seq);
                    slot.send(frame.clone());
                    fast_path_ok = true;
                } else {
                    let mut streams = self.streams.lock();
                    streams.remove(&target_worker);
                    rockstream_types::metrics::set_exchange_multiplexer_streams_size(
                        streams.len() as u64
                    );
                }
            }
        }

        if !fast_path_ok
            && matches!(
                route.transport,
                ExchangeTransport::Grpc | ExchangeTransport::SharedMemory
            )
        {
            // Attempt to connect or send via client pool
            match self.client_pool.get_client(target_worker).await {
                Ok(mut client) => {
                    let (tx, rx) = mpsc::channel::<ShuffleFrame>(64);
                    let request_stream = RxStream { rx };
                    let protocol_version =
                        self.client_pool.protocol_version_for_peer(target_worker);
                    let mut request = Request::new(request_stream);
                    request.metadata_mut().insert(
                        "protocol_version",
                        MetadataValue::from_str(&protocol_version.0.to_string()).map_err(
                            |error| format!("RS-5021: invalid protocol metadata: {error}"),
                        )?,
                    );
                    match client.shuffle_stream(request).await {
                        Ok(response) => {
                            let mut response_stream = response.into_inner();
                            let flow_controller = self.flow_controller.clone();
                            // Spawn task to process ACKs from the peer. With
                            // fast-path WAL elision (v0.51) there is no local
                            // outbox entry to delete on ACK; the ACK only drives
                            // flow-control credit release.
                            let cancel_token = self.cancel_token.clone();
                            self.task_tracker.spawn(async move {
                                loop {
                                    tokio::select! {
                                        _ = cancel_token.cancelled() => break,
                                        res = response_stream.next() => {
                                            match res {
                                                Some(Ok(ack)) => {
                                                    flow_controller.handle_ack(&ack);
                                                }
                                                Some(Err(e)) => {
                                                    tracing::error!(
                                                        code = %rockstream_types::error_code::RS_0001,
                                                        worker_id = ?target_worker,
                                                        "Shuffle stream ACK error: {:?}",
                                                        e
                                                    );
                                                    break;
                                                }
                                                None => break,
                                            }
                                        }
                                    }
                                }
                            });

                            {
                                let mut streams = self.streams.lock();
                                streams.insert(target_worker, tx.clone());
                                rockstream_types::metrics::set_exchange_multiplexer_streams_size(
                                    streams.len() as u64,
                                );
                            }
                            if let Ok(slot) = tx.reserve().await {
                                permit.take().unwrap().handoff(frame.epoch, frame.seq);
                                slot.send(frame.clone());
                                fast_path_ok = true;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to establish shuffle stream to {:?}: {:?}",
                                target_worker,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to {:?}: {:?}", target_worker, e);
                }
            }
        }

        if !fast_path_ok || matches!(route.transport, ExchangeTransport::DurableObject) {
            tracing::info!(
                transport = ?route.transport,
                "shuffle fast path to {:?} unavailable or bypassed; falling back to durable shuffle.",
                target_worker
            );

            let store = self.get_object_store().ok_or_else(|| {
                format!(
                    "[{}] Durable fallback failed: no object store available for multiplexer",
                    rockstream_types::error_code::RS_3012
                )
            })?;

            let src_worker = self.get_src_worker().unwrap_or(WorkerId(0));
            let path_str = format!(
                "shuffle/{}/{}/{}/{}",
                frame.exchange_id, frame.epoch, src_worker.0, target_worker.0
            );
            let path = object_store::path::Path::from(path_str);

            // Coalesce frame writing: check if file already exists in object store.
            // If it exists, read footer and all frames, reconstruct the writer, and append.
            let mut writer = crate::exchange::durable::DurableShuffleWriter::new();

            match crate::exchange::durable::DurableShuffleReader::read_footer(store.as_ref(), &path)
                .await
            {
                Ok(footer) => {
                    for entry in footer.entries {
                        match crate::exchange::durable::DurableShuffleReader::read_frame(
                            store.as_ref(),
                            &path,
                            &entry,
                        )
                        .await
                        {
                            Ok(existing_payload) => {
                                if let Err(e) = writer.add_frame(
                                    entry.src_shard,
                                    entry.target_shard,
                                    entry.seq,
                                    &existing_payload,
                                ) {
                                    return Err(format!(
                                        "[{}] Failed to add existing frame during coalescing: {:?}",
                                        rockstream_types::error_code::RS_3013,
                                        e
                                    ));
                                }
                            }
                            Err(e) => {
                                return Err(format!(
                                    "[{}] Failed to read existing frame during coalescing: {:?}",
                                    rockstream_types::error_code::RS_3012,
                                    e
                                ));
                            }
                        }
                    }
                }
                Err(_) => {
                    // File does not exist or footer is invalid; start with a clean writer.
                }
            }

            // Add the new frame
            writer
                .add_frame(
                    frame.src_shard,
                    frame.target_shard,
                    frame.seq,
                    &frame.payload,
                )
                .map_err(|e| {
                    format!(
                        "[{}] Fallback write error: {:?}",
                        rockstream_types::error_code::RS_3013,
                        e
                    )
                })?;

            // Upload the coalesced file
            writer.finish(store.as_ref(), &path).await.map_err(|e| {
                format!(
                    "[{}] Fallback upload error: {:?}",
                    rockstream_types::error_code::RS_3012,
                    e
                )
            })?;

            // No local outbox to delete: with fast-path WAL elision (v0.51) the
            // outbox is never written; the durable object upload above is the
            // persisted recovery path.
        }

        Ok(())
    }

    async fn send_shared_memory_frame(
        &self,
        target_worker: WorkerId,
        frame: &ShuffleFrame,
    ) -> Result<crate::exchange::proto::ShuffleAck, String> {
        let shm_ticket = self.shared_memory.publish(target_worker, frame)?;
        match self
            .client_pool
            .get_shared_memory_client(target_worker)
            .await
        {
            Ok(client) => match client.deliver(ticket(&shm_ticket)?).await {
                Ok(response) => {
                    self.shared_memory
                        .release_usage(target_worker, frame.payload.len());
                    Ok(response)
                }
                Err(error) => {
                    self.shared_memory.revoke(target_worker, &shm_ticket);
                    Err(format!(
                        "[{}] flight do_get failed: {error}",
                        rockstream_types::error_code::RS_3019
                    ))
                }
            },
            Err(error) => {
                self.shared_memory.revoke(target_worker, &shm_ticket);
                Err(error)
            }
        }
    }

    /// Pull and process durable shuffle frames for a given exchange, epoch, and sender worker.
    /// This allows a receiver worker to catch up on frames that were written to the object store.
    pub async fn catch_up_durable(
        &self,
        exchange_id: u64,
        epoch: u64,
        src_worker: WorkerId,
        target_worker: WorkerId,
        registry: &crate::exchange::service::ExchangeRegistry,
        store: &dyn ObjectStore,
    ) -> Result<(), String> {
        let path_str = format!(
            "shuffle/{}/{}/{}/{}",
            exchange_id, epoch, src_worker.0, target_worker.0
        );
        let path = object_store::path::Path::from(path_str);

        // Read footer. If the file is not found, we simply return Ok(()) as there's nothing to catch up.
        let footer =
            match crate::exchange::durable::DurableShuffleReader::read_footer(store, &path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("No durable shuffle footer found for {:?}: {:?}", path, e);
                    return Ok(());
                }
            };

        for entry in footer.entries {
            // Check if the target shard is active on this worker by checking if the inlet is registered
            if let Some(inlet) = registry.get(exchange_id, entry.target_shard) {
                // Check if this frame is a duplicate by checking if it already exists in the inbox db
                if let Some(db) = registry.get_shard_db(entry.target_shard) {
                    let inbox_key = crate::exchange::persistence::inbox_key(
                        exchange_id,
                        entry.src_shard,
                        epoch,
                        entry.seq,
                    );
                    if db
                        .get(&inbox_key)
                        .await
                        .map_err(|e| format!("Inbox DB get error: {:?}", e))?
                        .is_some()
                    {
                        // Frame already processed; skip to avoid duplicates.
                        continue;
                    }

                    // Not a duplicate: read payload from object store
                    let payload = crate::exchange::durable::DurableShuffleReader::read_frame(
                        store, &path, &entry,
                    )
                    .await?;

                    // Persist to inbox db
                    crate::exchange::persistence::persist_inbox(
                        &db,
                        exchange_id,
                        entry.src_shard,
                        epoch,
                        entry.seq,
                        &payload,
                    )
                    .await?;

                    // Deserialize ZSet and forward to target inlet
                    let zset = crate::exchange::serialization::deserialize_zset(
                        &payload,
                        inlet.schema.clone(),
                    )?;
                    if inlet.sender.send(zset).await.is_err() {
                        tracing::warn!("Failed to forward durable shuffle batch to local inlet");
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the number of active cached worker-to-worker streams.
    pub fn connection_count(&self) -> usize {
        self.streams.lock().len()
    }

    /// Evict a dead or drained worker's stream from the multiplexer and update metrics gauge.
    pub fn evict_worker(&self, worker_id: WorkerId) {
        let mut streams = self.streams.lock();
        streams.remove(&worker_id);
        rockstream_types::metrics::set_exchange_multiplexer_streams_size(streams.len() as u64);
    }
}

#[cfg(test)]
#[path = "credit_wait_tests.rs"]
mod credit_wait_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::flow_control::FlowController;
    use crate::exchange::proto::ShuffleAck;
    use crate::exchange::serialization::{deserialize_zset, serialize_zset};
    use rockstream_ops::zset::ArrowZSet;
    use std::future::Future;
    use std::time::Duration;

    fn frame(batch: &ArrowZSet, seq: u64) -> ShuffleFrame {
        ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard: 1,
            epoch: 1,
            seq,
            payload: serialize_zset(batch).unwrap().into(),
            row_count: batch.num_rows() as u32,
        }
    }

    fn sender(
        controller: &FlowController,
    ) -> (
        WorkerStreamMultiplexer,
        mpsc::Sender<ShuffleFrame>,
        mpsc::Receiver<ShuffleFrame>,
    ) {
        let multiplexer =
            WorkerStreamMultiplexer::new(ShuffleClientPool::default(), controller.clone());
        let (tx, rx) = mpsc::channel(1);
        // Use the production cached-stream path, with queue capacity as the delivery barrier.
        multiplexer.streams.lock().insert(WorkerId(2), tx.clone());
        (multiplexer, tx, rx)
    }

    fn snapshot(controller: &FlowController) -> (u32, u32, usize, usize, usize) {
        (
            controller.get_credits(100, 0, 1),
            controller.rows_in_flight(100, 0, 1),
            controller.batches_in_flight(100, 0, 1),
            controller.bytes_in_flight(),
            controller.pending_requests(),
        )
    }

    fn ack(controller: &FlowController, frame: &ShuffleFrame) {
        controller.handle_ack(&ShuffleAck {
            exchange_id: frame.exchange_id,
            src_shard: frame.src_shard,
            target_shard: frame.target_shard,
            epoch: frame.epoch,
            seq: frame.seq,
            credit_grant: frame.row_count.max(1),
        });
    }

    fn assert_batch(received: &ShuffleFrame, expected: &ShuffleFrame, batch: &ArrowZSet) {
        assert_eq!(received, expected);
        let decoded = deserialize_zset(&received.payload, batch.data.schema()).unwrap();
        assert_eq!(decoded.data, batch.data);
        assert_eq!(decoded.weights, batch.weights);
    }

    #[tokio::test]
    async fn production_sender_enforces_each_non_row_limit_until_matching_ack() {
        let first_batch = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
        let second_batch = ArrowZSet::from_ab_rows(&[(2, 20)], 1);
        let first = frame(&first_batch, 1);
        let second = frame(&second_batch, 2);
        for controller in [
            FlowController::with_row_budget(4).with_max_inflight_batches(1),
            FlowController::with_row_budget(4).with_max_inflight_bytes(first.payload.len()),
            FlowController::with_row_budget(4).with_max_pending_requests(1),
        ] {
            let (multiplexer, _tx, mut rx) = sender(&controller);
            let baseline = snapshot(&controller);
            assert_eq!(baseline, (4, 0, 0, 0, 0));
            multiplexer
                .send_frame(WorkerId(2), first.clone())
                .await
                .unwrap();
            let received = rx.try_recv().unwrap();
            assert_batch(&received, &first, &first_batch);
            assert_eq!(snapshot(&controller), (3, 1, 1, first.payload.len(), 1));

            let second_send = multiplexer.send_frame(WorkerId(2), second.clone());
            tokio::pin!(second_send);
            assert!(futures::poll!(&mut second_send).is_pending());
            assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
            assert_eq!(snapshot(&controller), (3, 1, 1, first.payload.len(), 1));

            ack(&controller, &received);
            assert_eq!(snapshot(&controller), baseline);
            tokio::time::timeout(Duration::from_secs(2), second_send)
                .await
                .unwrap()
                .unwrap();
            let received = rx.try_recv().unwrap();
            assert_batch(&received, &second, &second_batch);
            assert_eq!(snapshot(&controller), (3, 1, 1, second.payload.len(), 1));
            // A stale acknowledgement must not release the second frame's reservation.
            ack(&controller, &first);
            assert_eq!(snapshot(&controller), (3, 1, 1, second.payload.len(), 1));
            ack(&controller, &received);
            assert_eq!(snapshot(&controller), baseline);
            assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        }
    }

    #[tokio::test]
    async fn production_sender_rejects_oversized_frames_without_reserving_or_delivering() {
        let batch = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
        let oversized = frame(&batch, 1);
        for limit in ["max_batch_bytes", "max_inflight_bytes"] {
            let bytes = oversized.payload.len() - 1;
            let controller = if limit == "max_batch_bytes" {
                FlowController::with_row_budget(4).with_max_batch_bytes(bytes)
            } else {
                FlowController::with_row_budget(4).with_max_inflight_bytes(bytes)
            };
            let (multiplexer, _tx, mut rx) = sender(&controller);
            let baseline = snapshot(&controller);
            assert_eq!(
                multiplexer.send_frame(WorkerId(2), oversized.clone()).await,
                Err(format!(
                    "[RS-3006] exchange frame size ({} bytes) exceeds {limit} ({bytes}) limit. Next steps: split large record batches into smaller chunks within configured transport budget.",
                    oversized.payload.len()
                ))
            );
            assert_eq!(snapshot(&controller), baseline);
            assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        }
    }

    #[tokio::test]
    async fn production_sender_cancellation_before_delivery_restores_full_budget() {
        let controller = FlowController::with_row_budget(2);
        let (multiplexer, tx, mut rx) = sender(&controller);
        let baseline = snapshot(&controller);
        let blocked_slot = tx.reserve_owned().await.unwrap();
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 3);
        let original = frame(&batch, 1);
        let (reserved_tx, reserved_rx) = tokio::sync::oneshot::channel();
        let send_task = tokio::spawn({
            let multiplexer = multiplexer.clone();
            let original = original.clone();
            async move {
                let send = multiplexer.send_frame(WorkerId(2), original);
                tokio::pin!(send);
                std::future::poll_fn(|cx| {
                    assert!(send.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                reserved_tx.send(()).unwrap();
                send.await
            }
        });
        reserved_rx.await.unwrap();
        assert_eq!(snapshot(&controller), (0, 2, 1, original.payload.len(), 1));
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        send_task.abort();
        assert!(send_task.await.unwrap_err().is_cancelled());
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        drop(blocked_slot);

        tokio::time::timeout(
            Duration::from_secs(2),
            multiplexer.send_frame(WorkerId(2), original.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        let received = rx.try_recv().unwrap();
        assert_batch(&received, &original, &batch);
        assert_eq!(snapshot(&controller), (0, 2, 1, original.payload.len(), 1));
        ack(&controller, &received);
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn production_sender_cancellation_after_delivery_keeps_reservation_until_ack() {
        let controller = FlowController::with_row_budget(2);
        let (multiplexer, _tx, mut rx) = sender(&controller);
        let baseline = snapshot(&controller);
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], -2);
        let original = frame(&batch, 1);
        let send_task = tokio::spawn({
            let original = original.clone();
            async move {
                multiplexer.send_frame(WorkerId(2), original).await.unwrap();
                std::future::pending::<()>().await;
            }
        });
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_batch(&received, &original, &batch);
        assert_eq!(snapshot(&controller), (0, 2, 1, original.payload.len(), 1));
        send_task.abort();
        assert!(send_task.await.unwrap_err().is_cancelled());
        assert_eq!(snapshot(&controller), (0, 2, 1, original.payload.len(), 1));
        ack(&controller, &received);
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn production_sender_teardown_reclaims_delivered_reservations() {
        let controller = FlowController::with_row_budget(2);
        let (multiplexer, _tx, mut rx) = sender(&controller);
        let baseline = snapshot(&controller);
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
        let original = frame(&batch, 1);
        multiplexer
            .send_frame(WorkerId(2), original.clone())
            .await
            .unwrap();
        assert_batch(&rx.try_recv().unwrap(), &original, &batch);
        assert_eq!(snapshot(&controller), (0, 2, 1, original.payload.len(), 1));
        controller.teardown_exchange(100);
        assert_eq!(snapshot(&controller), baseline);
        ack(&controller, &original);
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn production_sender_accounts_each_overlapping_retransmission() {
        let controller = FlowController::with_row_budget(2);
        let (multiplexer, _tx, mut rx) = sender(&controller);
        let baseline = snapshot(&controller);
        let batch = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
        let original = frame(&batch, 1);
        for _ in 0..2 {
            multiplexer
                .send_frame(WorkerId(2), original.clone())
                .await
                .unwrap();
            assert_batch(&rx.try_recv().unwrap(), &original, &batch);
        }
        assert_eq!(
            snapshot(&controller),
            (0, 2, 2, 2 * original.payload.len(), 2)
        );
        ack(&controller, &original);
        assert_eq!(snapshot(&controller), (1, 1, 1, original.payload.len(), 1));
        ack(&controller, &original);
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn production_sender_failed_delivery_releases_reservation() {
        let controller = FlowController::with_row_budget(2);
        let multiplexer =
            WorkerStreamMultiplexer::new(ShuffleClientPool::default(), controller.clone());
        let baseline = snapshot(&controller);
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
        assert_eq!(
            multiplexer.send_frame(WorkerId(2), frame(&batch, 1)).await,
            Err(
                "[RS-3012] Durable fallback failed: no object store available for multiplexer"
                    .to_string()
            )
        );
        assert_eq!(snapshot(&controller), baseline);
    }

    #[tokio::test]
    async fn production_sender_durable_delivery_releases_reservation_without_ack() {
        let controller = FlowController::with_row_budget(2);
        let (multiplexer, _tx, mut rx) = sender(&controller);
        let store = Arc::new(object_store::memory::InMemory::new());
        let multiplexer = multiplexer
            .with_object_store(store.clone())
            .with_exchange_config(ExchangeConfig {
                exchange_force_durable: true,
                ..ExchangeConfig::default()
            });
        let baseline = snapshot(&controller);
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 3);
        let original = frame(&batch, 1);
        multiplexer.send_frame(WorkerId(2), original).await.unwrap();
        assert_eq!(snapshot(&controller), baseline);
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        let path = object_store::path::Path::from("shuffle/100/1/0/2");
        let footer =
            crate::exchange::durable::DurableShuffleReader::read_footer(store.as_ref(), &path)
                .await
                .unwrap();
        assert_eq!(
            footer.entries,
            vec![crate::exchange::durable::ShuffleIndexEntry {
                src_shard: 0,
                target_shard: 1,
                seq: 1,
                offset: 0,
                length: serialize_zset(&batch).unwrap().len() as u64,
            }]
        );
        let entry = footer.entries.first().unwrap();
        let payload = crate::exchange::durable::DurableShuffleReader::read_frame(
            store.as_ref(),
            &path,
            entry,
        )
        .await
        .unwrap();
        let recovered = deserialize_zset(&payload, batch.data.schema()).unwrap();
        assert_eq!(recovered.data, batch.data);
        assert_eq!(recovered.weights, batch.weights);
    }
}
