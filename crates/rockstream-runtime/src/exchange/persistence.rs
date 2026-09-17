use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::error_code::{RS_3004, RS_3008, RS_3011};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestIdentity {
    pub workload_id: u64,
    pub shard_id: u64,
    pub operator_id: u64,
    pub epoch: u64,
    pub request_id: u64,
}

impl RequestIdentity {
    pub fn new(
        workload_id: u64,
        shard_id: u64,
        operator_id: u64,
        epoch: u64,
        request_id: u64,
    ) -> Self {
        Self {
            workload_id,
            shard_id,
            operator_id,
            epoch,
            request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableExecutionOutcome {
    pub payload_digest: [u8; 32],
    pub lease_token: u64,
    pub committed_epoch: u64,
    pub success: bool,
}

pub fn compute_payload_digest(payload: &[u8]) -> [u8; 32] {
    *blake3::hash(payload).as_bytes()
}

pub fn request_identity_key(identity: &RequestIdentity) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(7 + 8 * 5);
    suffix.extend_from_slice(b"req_id:");
    suffix.extend_from_slice(&identity.workload_id.to_be_bytes());
    suffix.extend_from_slice(&identity.shard_id.to_be_bytes());
    suffix.extend_from_slice(&identity.operator_id.to_be_bytes());
    suffix.extend_from_slice(&identity.epoch.to_be_bytes());
    suffix.extend_from_slice(&identity.request_id.to_be_bytes());
    ShardKeyEncoder::meta_key(&suffix)
}

pub async fn get_request_identity(
    db: &ShardDb,
    identity: &RequestIdentity,
) -> Result<Option<DurableExecutionOutcome>, String> {
    let key = request_identity_key(identity);
    let raw = db
        .get(&key)
        .await
        .map_err(|e| format!("Failed to read request identity: {:?}", e))?;
    if let Some(bytes) = raw {
        if bytes.len() >= 49 {
            let mut payload_digest = [0u8; 32];
            payload_digest.copy_from_slice(&bytes[..32]);
            let lease_token = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
            let committed_epoch = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
            let success = bytes[48] != 0;
            return Ok(Some(DurableExecutionOutcome {
                payload_digest,
                lease_token,
                committed_epoch,
                success,
            }));
        }
    }
    Ok(None)
}

pub async fn record_request_identity(
    db: &ShardDb,
    identity: &RequestIdentity,
    outcome: &DurableExecutionOutcome,
) -> Result<(), String> {
    let key = request_identity_key(identity);
    let mut val = Vec::with_capacity(32 + 8 + 8 + 1);
    val.extend_from_slice(&outcome.payload_digest);
    val.extend_from_slice(&outcome.lease_token.to_be_bytes());
    val.extend_from_slice(&outcome.committed_epoch.to_be_bytes());
    val.push(if outcome.success { 1 } else { 0 });
    db.put(&key, &val)
        .await
        .map_err(|e| format!("Failed to record request identity: {:?}", e))
}

pub async fn advance_committed_frontier(db: &ShardDb, epoch: u64) -> Result<(), String> {
    let key = ShardKeyEncoder::frontier_key();
    db.put(&key, &epoch.to_be_bytes())
        .await
        .map_err(|e| format!("Failed to advance committed frontier: {:?}", e))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestProcessResult {
    /// Newly processed and durably committed
    Committed(DurableExecutionOutcome),
    /// Idempotent replay: already committed with matching digest, returned cached outcome
    Replayed(DurableExecutionOutcome),
}

impl RequestProcessResult {
    pub fn outcome(&self) -> &DurableExecutionOutcome {
        match self {
            Self::Committed(o) | Self::Replayed(o) => o,
        }
    }

    pub fn is_replayed(&self) -> bool {
        matches!(self, Self::Replayed(_))
    }
}

pub async fn execute_durable_request<F, Fut, T>(
    db: &ShardDb,
    identity: &RequestIdentity,
    payload: &[u8],
    lease_token: u64,
    active_lease: u64,
    op: F,
) -> Result<(RequestProcessResult, Option<T>), String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    // 1. Lease token check (RS-3004)
    if lease_token < active_lease {
        return Err(format!(
            "[{RS_3004}] stale lease token {lease_token} < active worker lease {active_lease}. Next steps: refresh worker shard lease from control plane."
        ));
    }

    // 2. Monotonic epoch frontier check (RS-3011)
    let current_frontier = committed_frontier(db).await?;
    if identity.epoch < current_frontier {
        return Err(format!(
            "[{RS_3011}] out-of-order epoch {} < committed frontier {current_frontier}. Next steps: submit frames in monotonic epoch order.",
            identity.epoch
        ));
    }

    let payload_digest = compute_payload_digest(payload);

    // 3. Replay check
    if let Some(existing) = get_request_identity(db, identity).await? {
        if existing.payload_digest == payload_digest {
            // Safe idempotent replay: return cached outcome without executing op
            return Ok((RequestProcessResult::Replayed(existing), None));
        } else {
            // Conflicting payload with identical request ID (RS-3008)
            return Err(format!(
                "[{RS_3008}] conflicting payload digest for request_id {} in epoch {}. Next steps: ensure idempotent retry uses identical payload, or generate fresh request ID.",
                identity.request_id, identity.epoch
            ));
        }
    }

    // 4. Fresh execution: run computation
    let op_result = op().await?;

    // 5. Commit: record request identity + advance frontier atomically in a WriteBatch
    let outcome = DurableExecutionOutcome {
        payload_digest,
        lease_token,
        committed_epoch: identity.epoch,
        success: true,
    };

    let mut batch = rockstream_storage::WriteBatch::new();

    let id_key = request_identity_key(identity);
    let mut val = Vec::with_capacity(32 + 8 + 8 + 1);
    val.extend_from_slice(&outcome.payload_digest);
    val.extend_from_slice(&outcome.lease_token.to_be_bytes());
    val.extend_from_slice(&outcome.committed_epoch.to_be_bytes());
    val.push(if outcome.success { 1 } else { 0 });
    batch.put(&id_key, &val);

    let frontier_key = ShardKeyEncoder::frontier_key();
    batch.put(&frontier_key, &identity.epoch.to_be_bytes());

    db.write_batch(batch)
        .await
        .map_err(|e| format!("Failed to commit request identity batch: {:?}", e))?;

    // ACK is returned strictly AFTER durable write batch flush
    Ok((RequestProcessResult::Committed(outcome), Some(op_result)))
}

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
