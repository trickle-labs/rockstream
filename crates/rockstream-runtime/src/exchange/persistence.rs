use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::shard_db::ShardDb;

/// Reads the committed frontier epoch for a shard.
///
/// With fast-path shuffle WAL elision (v0.51), successful direct-gRPC,
/// same-host shared-memory, and loopback deliveries no longer persist
/// `shuffle_inbox/` keys. Replay-dedup therefore relies on the shard's durable
/// committed frontier: any shuffle frame whose `epoch <= committed_frontier` is
/// already reflected in the checkpointed operator state and must not be
/// re-delivered after a restart/replay.
///
/// The read is best-effort. On a storage read failure it returns `0` (treating
/// the frame as not-yet-reflected so it is delivered conservatively) and the
/// caller logs [`rockstream_types::error_code::RS_3023`].
pub async fn committed_frontier(db: &ShardDb) -> Result<u64, String> {
    let key = ShardKeyEncoder::frontier_key();
    let value = db
        .get(&key)
        .await
        .map_err(|e| format!("Failed to read committed frontier: {:?}", e))?;
    Ok(value
        .and_then(|bytes| {
            if bytes.len() == 8 {
                Some(u64::from_be_bytes(bytes[..8].try_into().unwrap()))
            } else {
                None
            }
        })
        .unwrap_or(0))
}

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

/// Deletes an outbox entry only if it still exists, returning whether a delete happened.
pub async fn delete_outbox_if_present(
    db: &ShardDb,
    exchange_id: u64,
    target_shard: u32,
    epoch: u64,
    seq: u64,
) -> Result<bool, String> {
    let key = outbox_key(exchange_id, target_shard, epoch, seq);
    let exists = db
        .get(&key)
        .await
        .map_err(|e| format!("Failed to read outbox before delete: {:?}", e))?
        .is_some();
    if exists {
        db.delete(&key)
            .await
            .map_err(|e| format!("Failed to delete outbox: {:?}", e))?;
    }
    Ok(exists)
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

#[cfg(test)]
mod tests {
    #[test]
    fn exchange_codec_gc_uses_scan_and_delete_not_range_delete() {
        let source = std::fs::read_to_string(format!(
            "{}/src/exchange/persistence.rs",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(production.contains("scan_prefix"));
        assert!(production.contains("batch.delete"));
        assert!(!production.contains(".range_delete("));
        assert!(!production.contains("delete_range("));
    }
}
