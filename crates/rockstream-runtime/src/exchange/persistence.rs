use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::shard_db::ShardDb;

/// Encodes a shuffle outbox key.
pub fn outbox_key(exchange_id: u64, target_shard: u32, epoch: u64, seq: u64) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(4 + 8 + 8);
    suffix.extend_from_slice(&target_shard.to_be_bytes());
    suffix.extend_from_slice(&epoch.to_be_bytes());
    suffix.extend_from_slice(&seq.to_be_bytes());
    ShardKeyEncoder::encode(ShardPrefix::ShuffleOutbox, exchange_id, &suffix)
}

/// Encodes a shuffle inbox key.
pub fn inbox_key(exchange_id: u64, src_shard: u32, epoch: u64, seq: u64) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(4 + 8 + 8);
    suffix.extend_from_slice(&src_shard.to_be_bytes());
    suffix.extend_from_slice(&epoch.to_be_bytes());
    suffix.extend_from_slice(&seq.to_be_bytes());
    ShardKeyEncoder::encode(ShardPrefix::ShuffleInbox, exchange_id, &suffix)
}

/// Persists a frame in the outbox database.
pub async fn persist_outbox(
    db: &ShardDb,
    exchange_id: u64,
    target_shard: u32,
    epoch: u64,
    seq: u64,
    payload: &[u8],
) -> Result<(), String> {
    let key = outbox_key(exchange_id, target_shard, epoch, seq);
    db.put(&key, payload)
        .await
        .map_err(|e| format!("Failed to persist outbox: {:?}", e))
}

/// Persists a frame in the inbox database.
pub async fn persist_inbox(
    db: &ShardDb,
    exchange_id: u64,
    src_shard: u32,
    epoch: u64,
    seq: u64,
    payload: &[u8],
) -> Result<(), String> {
    let key = inbox_key(exchange_id, src_shard, epoch, seq);
    db.put(&key, payload)
        .await
        .map_err(|e| format!("Failed to persist inbox: {:?}", e))
}

/// Deletes an outbox entry.
pub async fn delete_outbox(
    db: &ShardDb,
    exchange_id: u64,
    target_shard: u32,
    epoch: u64,
    seq: u64,
) -> Result<(), String> {
    let key = outbox_key(exchange_id, target_shard, epoch, seq);
    db.delete(&key)
        .await
        .map_err(|e| format!("Failed to delete outbox: {:?}", e))
}

/// Deletes an inbox entry.
pub async fn delete_inbox(
    db: &ShardDb,
    exchange_id: u64,
    src_shard: u32,
    epoch: u64,
    seq: u64,
) -> Result<(), String> {
    let key = inbox_key(exchange_id, src_shard, epoch, seq);
    db.delete(&key)
        .await
        .map_err(|e| format!("Failed to delete inbox: {:?}", e))
}
