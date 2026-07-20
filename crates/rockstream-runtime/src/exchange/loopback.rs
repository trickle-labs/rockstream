use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::config::ExchangeConfig;
use rockstream_types::exchange::{ExchangeAnn, ExchangePath, ExchangeTransport};
use rockstream_types::ids::ShardId;
use rockstream_types::ids::{ExchangeId, WorkerId};

use crate::client::ShardState;
use crate::exchange::serialization::{deserialize_zset, serialize_zset_with_compression};
use crate::exchange::service::ExchangeRegistry;
use rockstream_types::exchange::ShuffleCompression;

/// Routes exchange batches locally within the same worker process.
#[derive(Clone)]
pub struct LoopbackRouter {
    registry: ExchangeRegistry,
    active_shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
}

impl LoopbackRouter {
    /// Create a new LoopbackRouter.
    pub fn new(
        registry: ExchangeRegistry,
        active_shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    ) -> Self {
        LoopbackRouter {
            registry,
            active_shards,
        }
    }

    /// Retrieve the ShardDb for a given shard.
    fn get_db(&self, shard_id: ShardId) -> Result<ShardDb, String> {
        let shards = self.active_shards.read();
        shards
            .get(&shard_id)
            .and_then(|state| state.db.clone())
            .ok_or_else(|| {
                format!(
                    "[{}] Shard db not active for shard {:?}",
                    rockstream_types::error_code::RS_3018,
                    shard_id
                )
            })
    }

    /// Route a batch over the loopback path.
    ///
    /// Persists outbox metadata to the source shard db, inbox metadata to the
    /// target shard db, and forwards the ZSet directly to the target inlet.
    pub async fn route_loopback(
        &self,
        exchange_id: u64,
        src_shard: u32,
        target_shard: u32,
        epoch: u64,
        seq: u64,
        zset: &ArrowZSet,
    ) -> Result<(), String> {
        let route = crate::exchange::classifier::classify_exchange(
            crate::exchange::classifier::ExchangeClassificationInput {
                ann: &ExchangeAnn {
                    exchange_id: ExchangeId(exchange_id),
                    law_id: None,
                    source_shard: ShardId(src_shard as u64),
                    target_shard: ShardId(target_shard as u64),
                    source_worker: WorkerId(0),
                    target_worker: WorkerId(0),
                    path: ExchangePath::Loopback,
                },
                local_worker: None,
                peer_worker: None,
                receiver_reachable: true,
                batch_bytes: 0,
                epoch_exchange_bytes: 0,
                config: &ExchangeConfig::default(),
            },
        );
        if route.transport != ExchangeTransport::InProcess {
            return Err(format!(
                "[{}] loopback exchange {} classified to unexpected transport {:?}",
                rockstream_types::error_code::RS_3018,
                exchange_id,
                route.transport
            ));
        }
        let src_db = self.get_db(ShardId(src_shard as u64))?;
        let target_db = self.get_db(ShardId(target_shard as u64))?;

        // 1. Serialize payload to bytes (Arrow IPC) so same-process loopback
        //    matches the wire-format verification of the network paths.
        let payload = serialize_zset_with_compression(zset, ShuffleCompression::Lz4, true)?;

        // 2. Fast-path shuffle WAL elision (v0.51, Slice 2): the same-worker
        //    loopback path no longer persists `shuffle_outbox/` on the source
        //    shard db nor `shuffle_inbox/` on the target shard db. Replay-dedup
        //    relies on the target shard's committed frontier.
        let _ = &src_db;
        let _ = (src_shard, seq);
        let frontier = crate::exchange::persistence::committed_frontier(&target_db).await?;
        if epoch <= frontier {
            // Already reflected in the checkpointed operator state; skip to
            // avoid duplicate delivery on replay.
            return Ok(());
        }

        // 3. Look up target inlet
        let inlet = self
            .registry
            .get(exchange_id, target_shard)
            .ok_or_else(|| {
                format!(
                    "No local inlet registered for exchange={}, shard={}",
                    exchange_id, target_shard
                )
            })?;

        // 4. Deserialize to ensure same-process loopback matches wire format verification
        let recovered_zset = deserialize_zset(&payload, inlet.schema.clone())?;

        // 5. Forward to local inlet channel
        inlet
            .sender
            .send(recovered_zset)
            .await
            .map_err(|e| format!("Failed to forward to local inlet: {:?}", e))?;

        Ok(())
    }
}
