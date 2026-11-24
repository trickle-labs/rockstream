//! Durable, replayable source routing decisions for automatic skew handling.
//!
//! The journal lives on the lowest stable workload shard. A prepare is durable
//! before any participant receives data; finish installs the routing state and
//! point mutations atomically after every participant acknowledges its commit.
//! Ownership may move, but the shard IDs and decision remain unchanged.

use std::collections::BTreeMap;

use rockstream_storage::{ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::data_plane::{SourceDeltaRequest, WorkloadTopologySnapshot};
use rockstream_types::ids::{LeaseToken, ShardId, WorkloadId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::client::{WorkerClientHandle, WorkerDeployments};
use crate::exchange::proto::ShuffleFrame;

pub const ROUTING_VERSION: u32 = 1;
pub const MAX_ROUTING_METADATA_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_ROUTING_POINT_KEYS: usize = 65_536;
pub const MAX_ROUTING_KEY_BYTES: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoutingEpochRecord {
    pub version: u32,
    pub workload_id: WorkloadId,
    pub source: String,
    pub request_id: String,
    pub epoch: u64,
    pub input_digest: [u8; 32],
    pub generation: u64,
    pub shard_ids: Vec<ShardId>,
    pub plan: Value,
    pub next_state: Value,
    /// Bounded mutations for this epoch; historical row ledgers use separate
    /// point keys and therefore do not impose a dataset-size ceiling.
    pub point_mutations: BTreeMap<String, Option<Value>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RoutingSnapshot {
    pub generation: u64,
    pub state: Value,
    pub pending: Option<RoutingEpochRecord>,
    pub completed: Option<RoutingEpochRecord>,
    #[serde(default)]
    pub anchor_frontier: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingOperation {
    Read,
    GetMany,
    Prepare,
    Finish,
    AggregatePartial,
}

/// Only this small envelope travels in metadata. Bounded command data travels
/// in the flow-controlled frame payload, avoiding HTTP/2 header-size ceilings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingCommand {
    pub routing_version: u32,
    pub workload_id: WorkloadId,
    pub shard_id: ShardId,
    pub lease_token: LeaseToken,
    pub operation: RoutingOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishRoutingEpoch {
    pub record: RoutingEpochRecord,
    pub shard_leases: Vec<(ShardId, LeaseToken)>,
}

pub fn input_digest(request: &SourceDeltaRequest) -> Result<[u8; 32], String> {
    let bytes = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    Ok(Sha256::digest(&bytes).into())
}

pub fn stable_shards(topology: &WorkloadTopologySnapshot) -> Vec<ShardId> {
    let mut shards: Vec<_> = topology.shards.iter().map(|shard| shard.shard_id).collect();
    shards.sort();
    shards.dedup();
    shards
}

pub fn matches_request(
    record: &RoutingEpochRecord,
    request: &SourceDeltaRequest,
) -> Result<bool, String> {
    Ok(record.workload_id == request.workload_id
        && record.source == request.source
        && record.request_id == request.request_id
        && record.epoch == request.epoch
        && record.input_digest == input_digest(request)?)
}

fn journal_key(workload: WorkloadId) -> Vec<u8> {
    ShardKeyEncoder::meta_key(format!("skew-routing-v1/{}", workload.0).as_bytes())
}

fn point_key(workload: WorkloadId, key: &str) -> Result<Vec<u8>, String> {
    if key.is_empty() || key.len() > MAX_ROUTING_KEY_BYTES {
        return Err("invalid skew routing point key".to_owned());
    }
    Ok(ShardKeyEncoder::meta_key(
        format!("skew-routing-point-v1/{}/{key}", workload.0).as_bytes(),
    ))
}

fn bounded_json<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if bytes.len() > MAX_ROUTING_METADATA_BYTES {
        return Err("skew routing metadata exceeds the per-epoch byte budget".to_owned());
    }
    Ok(bytes)
}

pub async fn load_snapshot(db: &ShardDb, workload: WorkloadId) -> Result<RoutingSnapshot, String> {
    match db
        .get(&journal_key(workload))
        .await
        .map_err(|error| error.to_string())?
    {
        Some(bytes) => {
            if bytes.len() > MAX_ROUTING_METADATA_BYTES {
                return Err("invalid oversized skew routing journal".to_owned());
            }
            let snapshot: RoutingSnapshot = serde_json::from_slice(&bytes)
                .map_err(|_| "invalid skew routing journal".to_owned())?;
            if let Some(completed) = &snapshot.completed {
                validate_record(completed, workload)?;
                if completed.generation != snapshot.generation
                    || completed.next_state != snapshot.state
                {
                    return Err("inconsistent completed skew routing generation".to_owned());
                }
            } else if snapshot.generation != 0 || !snapshot.state.is_null() {
                return Err("routing state has no completed generation".to_owned());
            }
            if let Some(pending) = &snapshot.pending {
                validate_record(pending, workload)?;
                if snapshot.generation.checked_add(1) != Some(pending.generation)
                    || snapshot
                        .completed
                        .as_ref()
                        .is_some_and(|completed| pending.epoch <= completed.epoch)
                {
                    return Err("inconsistent pending skew routing generation".to_owned());
                }
            }
            Ok(snapshot)
        }
        None => Ok(RoutingSnapshot::default()),
    }
}

fn validate_record(record: &RoutingEpochRecord, workload: WorkloadId) -> Result<(), String> {
    if record.version != ROUTING_VERSION
        || record.workload_id != workload
        || record.generation == 0
        || record.shard_ids.is_empty()
        || record.shard_ids.len() > 4096
        || record.shard_ids.windows(2).any(|pair| pair[0] >= pair[1])
        || record.point_mutations.len() > MAX_ROUTING_POINT_KEYS
    {
        return Err("invalid skew routing record identity".to_owned());
    }
    bounded_json(record)?;
    for key in record.point_mutations.keys() {
        point_key(workload, key)?;
    }
    Ok(())
}

/// The caller holds the anchor deployment's execution mutex and lease fence.
pub async fn prepare_record(
    db: &ShardDb,
    record: RoutingEpochRecord,
) -> Result<RoutingSnapshot, String> {
    validate_record(&record, record.workload_id)?;
    let mut snapshot = load_snapshot(db, record.workload_id).await?;
    if let Some(pending) = &snapshot.pending {
        if pending == &record {
            return Ok(snapshot);
        }
        return Err("another skew routing epoch requires reconciliation".to_owned());
    }
    if snapshot.completed.as_ref() == Some(&record) {
        return Ok(snapshot);
    }
    if record.generation
        != snapshot
            .generation
            .checked_add(1)
            .ok_or("routing generation exhausted")?
        || snapshot
            .completed
            .as_ref()
            .is_some_and(|done| record.epoch <= done.epoch)
    {
        return Err("stale skew routing generation or epoch".to_owned());
    }
    snapshot.pending = Some(record.clone());
    db.put(&journal_key(record.workload_id), &bounded_json(&snapshot)?)
        .await
        .map_err(|error| error.to_string())?;
    Ok(snapshot)
}

/// Atomically commits the generation and point ledger. No range deletions or
/// total-history scans are needed, including during crash replay.
pub async fn finish_record(
    db: &ShardDb,
    record: &RoutingEpochRecord,
) -> Result<RoutingSnapshot, String> {
    validate_record(record, record.workload_id)?;
    let mut snapshot = load_snapshot(db, record.workload_id).await?;
    if snapshot.pending.is_none() && snapshot.completed.as_ref() == Some(record) {
        return Ok(snapshot);
    }
    if snapshot.pending.as_ref() != Some(record) {
        return Err("skew routing completion does not match pending epoch".to_owned());
    }
    let mut writes = WriteBatch::new();
    for (key, value) in &record.point_mutations {
        let key = point_key(record.workload_id, key)?;
        match value {
            Some(value) => writes.put(&key, &bounded_json(value)?),
            None => writes.delete(&key),
        }
    }
    snapshot.generation = record.generation;
    snapshot.state = record.next_state.clone();
    snapshot.pending = None;
    snapshot.completed = Some(record.clone());
    writes.put(&journal_key(record.workload_id), &bounded_json(&snapshot)?);
    db.write_batch(writes)
        .await
        .map_err(|error| error.to_string())?;
    Ok(snapshot)
}

pub(crate) async fn handle_command(
    deployments: &WorkerDeployments,
    client: &WorkerClientHandle,
    frame: ShuffleFrame,
    command: RoutingCommand,
) -> Result<bytes::Bytes, String> {
    let deployment = deployments
        .read()
        .get(&(command.workload_id, command.shard_id))
        .cloned()
        .ok_or("RS-3004: routing shard is not deployed")?;
    if command.routing_version != ROUTING_VERSION
        || frame.exchange_id != command.workload_id.0
        || frame.target_shard != deployment.descriptor.shard_index
        || frame.seq != 0
        || command.lease_token != deployment.descriptor.shard.lease_token
        || !client.verify_local_lease_fence(command.shard_id, command.lease_token)
    {
        return Err("RS-3004: stale routing command identity".to_owned());
    }
    deployment
        .db
        .verify_lease_token(command.lease_token)
        .await
        .map_err(|error| error.to_string())?;
    let payload = crate::exchange::serialization::decode_bounded_payload(
        &frame.payload,
        MAX_ROUTING_METADATA_BYTES,
    )?;
    if matches!(command.operation, RoutingOperation::AggregatePartial) {
        let task = serde_json::from_slice(&payload)
            .map_err(|error| format!("invalid partial task: {error}"))?;
        let started = std::time::Instant::now();
        let result = crate::skew_aggregate::compute_partial(&deployment.descriptor, task)?;
        rockstream_types::metrics::add_r1_worker_rows(
            deployment.descriptor.shard.worker_id,
            result.input_rows,
            result.deltas.len() as u64,
        );
        let _ =
            client
                .msg_tx
                .try_send(rockstream_types::topology::WorkerMessage::ShardLoadReport {
                    worker_id: deployment.descriptor.shard.worker_id,
                    samples: vec![rockstream_types::topology::ShardLoadSample {
                        shard_id: command.shard_id,
                        state_bytes: 0,
                        rows_per_epoch: result.input_rows,
                        cpu_nanos: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        bytes_per_epoch: payload.len() as u64,
                        state_writes_per_epoch: 0,
                        key_loads: Vec::new(),
                    }],
                });
        return Ok(bounded_json(&result)?.into());
    }
    // Serialize CAS and completion with owned application commits on this shard.
    let needs_recovery = deployment.execution_requires_recovery.lock().await;
    if *needs_recovery {
        return Err("routing anchor requires recovery".to_owned());
    }
    let mut completed = None;
    let result = match command.operation {
        RoutingOperation::Read => {
            let mut snapshot = load_snapshot(&deployment.db, command.workload_id).await?;
            if let Some(bytes) = deployment
                .db
                .get(&ShardKeyEncoder::frontier_key())
                .await
                .map_err(|error| error.to_string())?
            {
                let bytes: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| "invalid anchor frontier")?;
                snapshot.anchor_frontier = u64::from_be_bytes(bytes);
            }
            bounded_json(&snapshot)?
        }
        RoutingOperation::GetMany => {
            let keys: Vec<String> =
                serde_json::from_slice(&payload).map_err(|error| error.to_string())?;
            if keys.len() > MAX_ROUTING_POINT_KEYS {
                return Err("too many routing point reads".to_owned());
            }
            let mut values = BTreeMap::<String, Value>::new();
            let mut total = 0usize;
            for key in keys {
                if let Some(bytes) = deployment
                    .db
                    .get(&point_key(command.workload_id, &key)?)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    total = total
                        .checked_add(bytes.len())
                        .ok_or("routing point response overflow")?;
                    if total > MAX_ROUTING_METADATA_BYTES {
                        return Err("routing point response exceeds epoch budget".to_owned());
                    }
                    values.insert(
                        key,
                        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
                    );
                }
            }
            bounded_json(&values)?
        }
        RoutingOperation::Prepare => {
            let record: RoutingEpochRecord =
                serde_json::from_slice(&payload).map_err(|error| error.to_string())?;
            validate_record(&record, command.workload_id)?;
            if record.epoch != frame.epoch || record.shard_ids.first() != Some(&command.shard_id) {
                return Err("routing prepare does not match anchor or epoch".to_owned());
            }
            bounded_json(&prepare_record(&deployment.db, record).await?)?
        }
        RoutingOperation::Finish => {
            let finish: FinishRoutingEpoch =
                serde_json::from_slice(&payload).map_err(|error| error.to_string())?;
            validate_record(&finish.record, command.workload_id)?;
            if finish.record.epoch != frame.epoch
                || finish.record.shard_ids.first() != Some(&command.shard_id)
                || finish
                    .shard_leases
                    .iter()
                    .map(|(shard, _)| *shard)
                    .collect::<Vec<_>>()
                    != finish.record.shard_ids
            {
                return Err("routing finish does not match anchor or membership".to_owned());
            }
            let bytes = bounded_json(&finish_record(&deployment.db, &finish.record).await?)?;
            client.direct_grants.revoke_completed(&finish.record);
            completed = Some(finish);
            bytes
        }
        RoutingOperation::AggregatePartial => unreachable!(),
    };
    deployment
        .db
        .verify_lease_token(command.lease_token)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(finish) = completed {
        client
            .msg_tx
            .send(
                rockstream_types::topology::WorkerMessage::RoutingEpochCommitted {
                    worker_id: deployment.descriptor.shard.worker_id,
                    workload_id: command.workload_id,
                    anchor_shard_id: command.shard_id,
                    anchor_lease_token: command.lease_token,
                    epoch: finish.record.epoch,
                    generation: finish.record.generation,
                    request_id: finish.record.request_id.clone(),
                    input_digest: finish.record.input_digest,
                    decision: serde_json::Value::Null,
                    shard_leases: finish.shard_leases,
                },
            )
            .await
            .map_err(|_| {
                "committed routing notification requires control reconnection".to_owned()
            })?;
    }
    Ok(result.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn record() -> RoutingEpochRecord {
        RoutingEpochRecord {
            version: 1,
            workload_id: WorkloadId(7),
            source: "t".into(),
            request_id: "r1".into(),
            epoch: 1,
            input_digest: [9; 32],
            generation: 1,
            shard_ids: vec![ShardId(1), ShardId(2)],
            plan: serde_json::json!({"heavy":[42],"buckets":2}),
            next_state: serde_json::json!({"promoted":42}),
            point_mutations: BTreeMap::from([("row:42:9".to_owned(), Some(serde_json::json!(3)))]),
        }
    }

    #[tokio::test]
    async fn prepare_replay_and_finish_preserve_exact_generation_and_point_state() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = ShardDb::builder("routing-journal", store)
            .build()
            .await
            .unwrap();
        let record = record();
        let prepared = prepare_record(&db, record.clone()).await.unwrap();
        assert_eq!(
            prepared,
            RoutingSnapshot {
                pending: Some(record.clone()),
                ..Default::default()
            }
        );
        assert_eq!(prepare_record(&db, record.clone()).await.unwrap(), prepared);
        assert_eq!(load_snapshot(&db, WorkloadId(7)).await.unwrap(), prepared);
        assert_eq!(
            db.get(&point_key(WorkloadId(7), "row:42:9").unwrap())
                .await
                .unwrap(),
            None
        );
        let mut wrong = record.clone();
        wrong.input_digest = [8; 32];
        assert_eq!(
            finish_record(&db, &wrong).await.unwrap_err(),
            "skew routing completion does not match pending epoch"
        );
        let completed = finish_record(&db, &record).await.unwrap();
        assert_eq!(
            completed,
            RoutingSnapshot {
                generation: 1,
                state: record.next_state.clone(),
                pending: None,
                completed: Some(record.clone()),
                anchor_frontier: 0
            }
        );
        assert_eq!(finish_record(&db, &record).await.unwrap(), completed);
        assert_eq!(
            db.get(&point_key(WorkloadId(7), "row:42:9").unwrap())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"3"
        );
        let mut next = record;
        next.generation = 2;
        next.epoch = 2;
        next.request_id = "r2".into();
        next.point_mutations.insert("row:42:9".to_owned(), None);
        prepare_record(&db, next.clone()).await.unwrap();
        finish_record(&db, &next).await.unwrap();
        assert_eq!(
            db.get(&point_key(WorkloadId(7), "row:42:9").unwrap())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn pending_routing_survives_reopen_and_rejects_a_different_replay() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = ShardDb::builder("routing-reopen", store.clone())
            .build()
            .await
            .unwrap();
        let expected = record();
        let prepared = prepare_record(&db, expected.clone()).await.unwrap();
        db.clone().close().await.unwrap();
        drop(db);
        let reopened = ShardDb::builder("routing-reopen", store)
            .build()
            .await
            .unwrap();
        assert_eq!(
            load_snapshot(&reopened, WorkloadId(7)).await.unwrap(),
            prepared
        );
        let mut forged = expected.clone();
        forged.input_digest = [2; 32];
        assert_eq!(
            prepare_record(&reopened, forged).await.unwrap_err(),
            "another skew routing epoch requires reconciliation"
        );
        assert_eq!(
            load_snapshot(&reopened, WorkloadId(7)).await.unwrap(),
            prepared
        );
        assert_eq!(
            prepare_record(&reopened, expected.clone()).await.unwrap(),
            prepared
        );
        let finished = finish_record(&reopened, &expected).await.unwrap();
        assert_eq!(finished.generation, 1);
        assert_eq!(finished.completed, Some(expected));
        assert_eq!(finished.pending, None);
        assert_eq!(finished.state, serde_json::json!({"promoted":42}));
    }
}
