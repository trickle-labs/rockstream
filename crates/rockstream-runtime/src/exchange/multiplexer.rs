use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

use crate::exchange::pool::ShuffleClientPool;
use crate::exchange::proto::ShuffleFrame;
use rockstream_types::ids::WorkerId;

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
        }
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

    /// Send a frame to a target worker, establishing the stream if necessary.
    /// Falls back to the durable object store path if gRPC connectivity fails.
    pub async fn send_frame(
        &self,
        target_worker: WorkerId,
        frame: ShuffleFrame,
    ) -> Result<(), String> {
        // Persist to outbox if source shard db is active on this worker
        if let Some(db) = self.get_shard_db(frame.src_shard) {
            crate::exchange::persistence::persist_outbox(
                &db,
                frame.exchange_id,
                frame.target_shard,
                frame.epoch,
                frame.seq,
                &frame.payload,
            )
            .await?;
        }

        let mut fast_path_ok = false;

        let tx_opt = {
            let streams = self.streams.lock();
            streams.get(&target_worker).cloned()
        };

        if let Some(tx) = tx_opt {
            if tx.send(frame.clone()).await.is_ok() {
                fast_path_ok = true;
            } else {
                // Connection was broken, remove it from cached streams
                self.streams.lock().remove(&target_worker);
            }
        }

        if !fast_path_ok {
            // Attempt to connect or send via client pool
            match self.client_pool.get_client(target_worker).await {
                Ok(mut client) => {
                    let (tx, rx) = mpsc::channel::<ShuffleFrame>(64);
                    let request_stream = RxStream { rx };
                    match client.shuffle_stream(request_stream).await {
                        Ok(response) => {
                            let mut response_stream = response.into_inner();
                            let flow_controller = self.flow_controller.clone();
                            let self_clone = self.clone();
                            // Spawn task to process ACKs from the peer
                            tokio::spawn(async move {
                                while let Some(res) = response_stream.next().await {
                                    match res {
                                        Ok(ack) => {
                                            flow_controller.handle_ack(&ack);
                                            if let Some(db) = self_clone.get_shard_db(ack.src_shard)
                                            {
                                                let _ =
                                                    crate::exchange::persistence::delete_outbox(
                                                        &db,
                                                        ack.exchange_id,
                                                        ack.target_shard,
                                                        ack.epoch,
                                                        ack.seq,
                                                    )
                                                    .await;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                code = %rockstream_types::error_code::RS_0001,
                                                worker_id = ?target_worker,
                                                "Shuffle stream ACK error: {:?}",
                                                e
                                            );
                                            break;
                                        }
                                    }
                                }
                            });

                            self.streams.lock().insert(target_worker, tx.clone());
                            if tx.send(frame.clone()).await.is_ok() {
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

        if !fast_path_ok {
            tracing::info!(
                "gRPC connection to {:?} failed or unavailable. Falling back to durable shuffle.",
                target_worker
            );

            let store = self.get_object_store().ok_or_else(|| {
                format!(
                    "[{}] Durable fallback failed: no object store available for multiplexer",
                    rockstream_types::error_code::RS_3010
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
                                        rockstream_types::error_code::RS_3010,
                                        e
                                    ));
                                }
                            }
                            Err(e) => {
                                return Err(format!(
                                    "[{}] Failed to read existing frame during coalescing: {:?}",
                                    rockstream_types::error_code::RS_3010,
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
                        rockstream_types::error_code::RS_3010,
                        e
                    )
                })?;

            // Upload the coalesced file
            writer.finish(store.as_ref(), &path).await.map_err(|e| {
                format!(
                    "[{}] Fallback upload error: {:?}",
                    rockstream_types::error_code::RS_3010,
                    e
                )
            })?;

            // Delete from local outbox on successful upload, since delivery is now guaranteed via object store.
            if let Some(db) = self.get_shard_db(frame.src_shard) {
                let _ = crate::exchange::persistence::delete_outbox(
                    &db,
                    frame.exchange_id,
                    frame.target_shard,
                    frame.epoch,
                    frame.seq,
                )
                .await;
            }
        }

        Ok(())
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
}
