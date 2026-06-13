use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
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
        }
    }

    /// Retrieve the ShardDb for a given shard.
    pub fn get_shard_db(&self, shard_id: u32) -> Option<rockstream_storage::shard_db::ShardDb> {
        let shards = self.active_shards.read();
        shards
            .get(&rockstream_types::ids::ShardId(shard_id as u64))
            .and_then(|state| state.db.clone())
    }

    /// Send a frame to a target worker, establishing the stream if necessary.
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

        let mut tx_opt = {
            let streams = self.streams.lock();
            streams.get(&target_worker).cloned()
        };

        if tx_opt.is_none() {
            let mut client = self.client_pool.get_client(target_worker).await?;
            let (tx, rx) = mpsc::channel::<ShuffleFrame>(64);

            let request_stream = RxStream { rx };
            let response = client
                .shuffle_stream(request_stream)
                .await
                .map_err(|e| format!("gRPC shuffle_stream call failed: {:?}", e))?;

            let mut response_stream = response.into_inner();

            let flow_controller = self.flow_controller.clone();
            let self_clone = self.clone();
            // Spawn task to process ACKs from the peer
            tokio::spawn(async move {
                while let Some(res) = response_stream.next().await {
                    match res {
                        Ok(ack) => {
                            // Process credit update
                            flow_controller.handle_ack(&ack);

                            // Delete from outbox on successful ACK
                            if let Some(db) = self_clone.get_shard_db(ack.src_shard) {
                                let _ = crate::exchange::persistence::delete_outbox(
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
            tx_opt = Some(tx);
        }

        let tx = tx_opt.unwrap();
        tx.send(frame)
            .await
            .map_err(|e| format!("Failed to send frame over channel: {:?}", e))?;
        Ok(())
    }

    /// Returns the number of active cached worker-to-worker streams.
    pub fn connection_count(&self) -> usize {
        self.streams.lock().len()
    }
}
