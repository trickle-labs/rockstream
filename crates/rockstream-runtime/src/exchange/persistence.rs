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

/// Garbage collects shuffle inbox and outbox entries in the shard database for epochs up to and including `up_to_epoch`.
pub async fn gc_exchange_storage(db: &ShardDb, up_to_epoch: u64) -> Result<(), String> {
    let mut batch = rockstream_storage::WriteBatch::new();

    // Scan and GC Inbox
    let inbox_prefix = [ShardPrefix::ShuffleInbox.as_byte()];
    let inbox_entries = db
        .scan_prefix(&inbox_prefix)
        .await
        .map_err(|e| format!("Failed to scan inbox keys for GC: {:?}", e))?;
    for (key, _) in inbox_entries {
        if let Some((_, _, suffix)) = ShardKeyEncoder::decode(&key) {
            if suffix.len() >= 12 {
                let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
                if epoch <= up_to_epoch {
                    batch.delete(&key);
                }
            }
        }
    }

    // Scan and GC Outbox
    let outbox_prefix = [ShardPrefix::ShuffleOutbox.as_byte()];
    let outbox_entries = db
        .scan_prefix(&outbox_prefix)
        .await
        .map_err(|e| format!("Failed to scan outbox keys for GC: {:?}", e))?;
    for (key, _) in outbox_entries {
        if let Some((_, _, suffix)) = ShardKeyEncoder::decode(&key) {
            if suffix.len() >= 12 {
                let epoch = u64::from_be_bytes(suffix[4..12].try_into().unwrap());
                if epoch <= up_to_epoch {
                    batch.delete(&key);
                }
            }
        }
    }

    if !batch.is_empty() {
        db.write_batch(batch)
            .await
            .map_err(|e| format!("Failed to write GC batch: {:?}", e))?;
    }

    Ok(())
}
