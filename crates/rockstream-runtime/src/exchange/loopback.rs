use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::ids::ShardId;

use crate::client::ShardState;
use crate::exchange::persistence::{delete_outbox, persist_inbox, persist_outbox};
use crate::exchange::serialization::{deserialize_zset, serialize_zset};
use crate::exchange::service::ExchangeRegistry;

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
        let src_db = self.get_db(ShardId(src_shard as u64))?;
        let target_db = self.get_db(ShardId(target_shard as u64))?;

        // 1. Serialize payload to bytes (Arrow IPC)
        let payload = serialize_zset(zset)?;

        // 2. Persist to outbox on the source shard db
        persist_outbox(&src_db, exchange_id, target_shard, epoch, seq, &payload).await?;

        // 3. Persist to inbox on the target shard db
        persist_inbox(&target_db, exchange_id, src_shard, epoch, seq, &payload).await?;

        // 4. Look up target inlet
        let inlet = self
            .registry
            .get(exchange_id, target_shard)
            .ok_or_else(|| {
                format!(
                    "No local inlet registered for exchange={}, shard={}",
                    exchange_id, target_shard
                )
            })?;

        // 5. Deserialize to ensure same-process loopback matches wire format verification
        let recovered_zset = deserialize_zset(&payload, inlet.schema.clone())?;

        // 6. Forward to local inlet channel
        inlet
            .sender
            .send(recovered_zset)
            .await
            .map_err(|e| format!("Failed to forward to local inlet: {:?}", e))?;

        // 7. Delete from outbox once successfully delivered
        delete_outbox(&src_db, exchange_id, target_shard, epoch, seq).await?;

        Ok(())
    }
}
