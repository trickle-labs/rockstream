//! Receiver-side capability checks for authenticated gateway shuffle frames.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use rockstream_types::direct_epoch::{
    AuthorizedOwnedContext, DirectEpochGrant, SourceEpochMetadata, MAX_DIRECT_EPOCH_GRANTS,
};
use rockstream_types::ids::WorkloadId;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as AdmissionLock};

use crate::exchange::proto::ShuffleFrame;
use crate::skew_routing::{FinishRoutingEpoch, RoutingEpochRecord, MAX_ROUTING_METADATA_BYTES};

#[derive(Clone, Default)]
pub(crate) struct DirectGrantRegistry {
    grants: Arc<RwLock<HashMap<uuid::Uuid, DirectEpochGrant>>>,
    admission: Arc<Mutex<HashMap<WorkloadId, Weak<AdmissionLock<()>>>>>,
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl DirectGrantRegistry {
    fn admission_lock(&self, workload: WorkloadId) -> Result<Arc<AdmissionLock<()>>, String> {
        let mut locks = self.admission.lock();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&workload).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        if locks.len() >= MAX_DIRECT_EPOCH_GRANTS {
            return Err("RS-0003: direct epoch admission capacity is unavailable".into());
        }
        let lock = Arc::new(AdmissionLock::new(()));
        locks.insert(workload, Arc::downgrade(&lock));
        Ok(lock)
    }

    /// Commands retain this guard through their durable execution receipt.
    /// Revalidate the capability after acquiring it: a waiter may outlive its
    /// grant while a replacement admission drains earlier commands.
    pub(crate) async fn command_guard(
        &self,
        context: &bytes::Bytes,
        now: u64,
    ) -> Result<Option<OwnedRwLockReadGuard<()>>, String> {
        let Ok(envelope) = serde_json::from_slice::<AuthorizedOwnedContext>(context) else {
            return Ok(None);
        };
        let workload = {
            let grants = self.grants.read();
            let grant = grants
                .get(&envelope.direct_grant_id)
                .ok_or("RS-2401: direct epoch capability is unknown or evicted")?;
            if grant.expires_at_ms <= now || grant.metadata != envelope.metadata {
                return Err(
                    "RS-2401: direct epoch capability is expired or bound to another request"
                        .into(),
                );
            }
            grant.metadata.workload_id
        };
        Ok(Some(self.admission_lock(workload)?.read_owned().await))
    }

    /// Installers and ownership transfers take admission before execution
    /// gates, so pre-expiry commands cannot appear after the idle check.
    pub(crate) async fn installation_guard(
        &self,
        workload: WorkloadId,
    ) -> Result<OwnedRwLockWriteGuard<()>, String> {
        Ok(self.admission_lock(workload)?.write_owned().await)
    }

    pub(crate) fn install(&self, grant: DirectEpochGrant, now: u64) -> bool {
        if !grant.metadata.is_valid()
            || grant.members.is_empty()
            || grant.members.len() > 4096
            || grant.expires_at_ms <= now
        {
            return false;
        }
        let mut grants = self.grants.write();
        grants.retain(|_, entry| entry.expires_at_ms > now);
        if grants.len() >= MAX_DIRECT_EPOCH_GRANTS && !grants.contains_key(&grant.grant_id) {
            return false;
        }
        if grants.values().any(|existing| {
            existing.metadata.workload_id == grant.metadata.workload_id
                && existing.metadata != grant.metadata
        }) {
            return false;
        }
        match grants.get(&grant.grant_id) {
            Some(existing) => existing == &grant,
            None => {
                grants.insert(grant.grant_id, grant);
                true
            }
        }
    }

    pub(crate) fn revoke(&self, metadata: &SourceEpochMetadata, grant_id: Option<uuid::Uuid>) {
        self.grants.write().retain(|id, grant| {
            grant.metadata != *metadata || grant_id.is_some_and(|selected| selected != *id)
        });
    }

    pub(crate) fn revoke_completed(&self, record: &RoutingEpochRecord) {
        self.grants
            .write()
            .retain(|_, grant| !metadata_matches_record(&grant.metadata, record));
    }

    pub(crate) fn unwrap_context(
        &self,
        context: bytes::Bytes,
        frame: &ShuffleFrame,
        required: bool,
        now: u64,
    ) -> Result<bytes::Bytes, String> {
        let envelope = serde_json::from_slice::<AuthorizedOwnedContext>(&context);
        let envelope =
            match envelope {
                Ok(envelope) => envelope,
                Err(_) if !required => return Ok(context),
                Err(_) => return Err(
                    "RS-2401: direct gateway writes require an actor-authorized epoch capability"
                        .into(),
                ),
            };
        let grant = self
            .grants
            .read()
            .get(&envelope.direct_grant_id)
            .cloned()
            .ok_or("RS-2401: direct epoch capability is unknown or evicted")?;
        if grant.expires_at_ms <= now
            || grant.metadata != envelope.metadata
            || frame.exchange_id != grant.metadata.workload_id.0
            || frame.epoch != grant.metadata.epoch
        {
            return Err(
                "RS-2401: direct epoch capability is expired or bound to another request".into(),
            );
        }
        let workload = envelope
            .inner
            .get("workload_id")
            .and_then(serde_json::Value::as_u64);
        let shard = envelope
            .inner
            .get("shard_id")
            .and_then(serde_json::Value::as_u64)
            .map(rockstream_types::ids::ShardId);
        let lease = envelope
            .inner
            .get("lease_token")
            .and_then(serde_json::Value::as_u64)
            .map(rockstream_types::ids::LeaseToken);
        if workload != Some(grant.metadata.workload_id.0)
            || shard.and_then(|id| grant.members.get(&id).copied()) != lease
            || lease.is_none()
        {
            return Err(
                "RS-2401: direct epoch capability does not authorize this shard incarnation".into(),
            );
        }
        authorize_payload(&grant, &envelope.inner, frame)?;
        serde_json::to_vec(&envelope.inner)
            .map(bytes::Bytes::from)
            .map_err(|error| error.to_string())
    }
}

pub(crate) fn metadata_matches_record(
    metadata: &SourceEpochMetadata,
    record: &RoutingEpochRecord,
) -> bool {
    record.version == crate::skew_routing::ROUTING_VERSION
        && record.workload_id == metadata.workload_id
        && record.source == metadata.source
        && record.request_id == metadata.request_id
        && record.epoch == metadata.epoch
        && record.input_digest == metadata.input_digest
}

fn authorize_source(
    metadata: &SourceEpochMetadata,
    value: &serde_json::Value,
) -> Result<(), String> {
    if value.get("workload_id").and_then(serde_json::Value::as_u64) != Some(metadata.workload_id.0)
        || value.get("source").and_then(serde_json::Value::as_str) != Some(metadata.source.as_str())
        || value.get("request_id").and_then(serde_json::Value::as_str)
            != Some(metadata.request_id.as_str())
        || value.get("epoch").and_then(serde_json::Value::as_u64) != Some(metadata.epoch)
    {
        return Err("RS-2401: direct epoch capability does not authorize this source epoch".into());
    }
    Ok(())
}

fn authorize_payload(
    grant: &DirectEpochGrant,
    context: &serde_json::Value,
    frame: &ShuffleFrame,
) -> Result<(), String> {
    let metadata = &grant.metadata;
    let Some(operation) = context.get("operation") else {
        if frame.epoch != metadata.epoch {
            return Err(
                "RS-2401: direct epoch capability does not authorize this source epoch".into(),
            );
        }
        return authorize_source(metadata, context);
    };
    let operation = operation
        .as_str()
        .ok_or("RS-2401: invalid direct routing operation")?;
    if context
        .get("routing_version")
        .and_then(serde_json::Value::as_u64)
        != Some(u64::from(crate::skew_routing::ROUTING_VERSION))
    {
        return Err("RS-2401: invalid direct routing envelope".into());
    }
    // Read-only commands carry no source mutation. Their workload and exact
    // shard incarnation were checked against the same installed capability.
    if matches!(operation, "Read" | "GetMany" | "JoinRowStats") {
        return Ok(());
    }
    if frame.epoch != metadata.epoch {
        return Err("RS-2401: direct epoch capability does not authorize this source epoch".into());
    }
    let payload = crate::exchange::serialization::decode_bounded_payload(
        &frame.payload,
        MAX_ROUTING_METADATA_BYTES,
    )
    .map_err(|_| "RS-2401: invalid direct routing payload")?;
    match operation {
        "Prepare" | "Finish" => {
            let record: RoutingEpochRecord = if operation == "Prepare" {
                serde_json::from_slice(&payload)
                    .map_err(|_| "RS-2401: invalid direct routing payload")?
            } else {
                let finish: FinishRoutingEpoch = serde_json::from_slice(&payload)
                    .map_err(|_| "RS-2401: invalid direct routing payload")?;
                if finish.shard_leases
                    != grant
                        .members
                        .iter()
                        .map(|(shard, lease)| (*shard, *lease))
                        .collect::<Vec<_>>()
                {
                    return Err(
                        "RS-2401: routing completion does not match authorized membership".into(),
                    );
                }
                finish.record
            };
            if !metadata_matches_record(metadata, &record)
                || record.shard_ids != grant.members.keys().copied().collect::<Vec<_>>()
            {
                return Err("RS-2401: routing record does not match authorized input".into());
            }
            Ok(())
        }
        "JoinExecute" => {
            let input: serde_json::Value = serde_json::from_slice(&payload)
                .map_err(|_| "RS-2401: invalid direct routing payload")?;
            authorize_source(metadata, &input)
        }
        "AggregatePartial" => {
            let task: crate::skew_aggregate::PartialAggregateTask =
                serde_json::from_slice(&payload)
                    .map_err(|_| "RS-2401: invalid direct routing payload")?;
            if task.spec.source != metadata.source {
                return Err(
                    "RS-2401: direct epoch capability does not authorize this source epoch".into(),
                );
            }
            // Partial and final shard batches are transformed subsets of the
            // source input. Only the journal carries its original input digest.
            Ok(())
        }
        _ => Err("RS-2401: invalid direct routing operation".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::direct_epoch::SourceEpochMetadata;
    use rockstream_types::ids::{LeaseToken, ShardId, WorkloadId};
    use std::collections::BTreeMap;

    fn fixture(
        operation: &str,
    ) -> (
        DirectGrantRegistry,
        DirectEpochGrant,
        AuthorizedOwnedContext,
        ShuffleFrame,
    ) {
        let registry = DirectGrantRegistry::default();
        let grant = DirectEpochGrant {
            grant_id: uuid::Uuid::from_u128(9),
            metadata: SourceEpochMetadata {
                version: 2,
                workload_id: WorkloadId(3),
                request_id: "r".into(),
                source: "events".into(),
                epoch: 4,
                input_digest: [7; 32],
            },
            expires_at_ms: 200,
            members: BTreeMap::from([(ShardId(5), LeaseToken(6))]),
        };
        assert!(registry.install(grant.clone(), 100));
        let context = AuthorizedOwnedContext {
            direct_grant_id: grant.grant_id,
            metadata: grant.metadata.clone(),
            inner: serde_json::json!({
                "routing_version":1,"workload_id":3,"shard_id":5,
                "lease_token":6,"operation":operation
            }),
        };
        let frame = ShuffleFrame {
            exchange_id: 3,
            src_shard: 0,
            target_shard: 0,
            epoch: 4,
            seq: 0,
            payload: Vec::new(),
            row_count: 0,
        };
        (registry, grant, context, frame)
    }

    fn encoded(context: &AuthorizedOwnedContext) -> bytes::Bytes {
        serde_json::to_vec(context).unwrap().into()
    }

    #[test]
    fn routing_mutations_require_exact_authorized_input_and_membership() {
        for operation in ["Prepare", "Finish"] {
            let (registry, grant, context, mut frame) = fixture(operation);
            let record = RoutingEpochRecord {
                version: 1,
                workload_id: grant.metadata.workload_id,
                source: grant.metadata.source.clone(),
                request_id: grant.metadata.request_id.clone(),
                epoch: grant.metadata.epoch,
                input_digest: grant.metadata.input_digest,
                generation: 1,
                shard_ids: vec![ShardId(5)],
                plan: serde_json::json!({"kind":"passthrough"}),
                next_state: serde_json::Value::Null,
                point_mutations: BTreeMap::new(),
            };
            let payload = |record: serde_json::Value| {
                if operation == "Finish" {
                    serde_json::json!({"record":record,"shard_leases":[[5,6]]})
                } else {
                    record
                }
            };
            let record = serde_json::to_value(record).unwrap();
            frame.payload = serde_json::to_vec(&payload(record.clone())).unwrap();
            assert_eq!(
                registry.unwrap_context(encoded(&context), &frame, true, 100),
                Ok(serde_json::to_vec(&context.inner).unwrap().into())
            );
            for (field, value) in [
                ("version", serde_json::json!(2)),
                ("workload_id", serde_json::json!(8)),
                ("source", serde_json::json!("private_events")),
                ("request_id", serde_json::json!("another-request")),
                ("epoch", serde_json::json!(5)),
                ("input_digest", serde_json::json!(vec![8; 32])),
                ("shard_ids", serde_json::json!([5, 7])),
            ] {
                let mut changed = record.clone();
                changed[field] = value;
                frame.payload = serde_json::to_vec(&payload(changed)).unwrap();
                assert_eq!(
                    registry.unwrap_context(encoded(&context), &frame, true, 100),
                    Err("RS-2401: routing record does not match authorized input".into()),
                    "{operation} must bind {field}"
                );
            }
            if operation == "Finish" {
                let mut changed = payload(record);
                changed["shard_leases"][0][1] = serde_json::json!(7);
                frame.payload = serde_json::to_vec(&changed).unwrap();
                assert_eq!(
                    registry.unwrap_context(encoded(&context), &frame, true, 100),
                    Err("RS-2401: routing completion does not match authorized membership".into())
                );
            }
        }
    }

    #[test]
    fn read_operation_cannot_disguise_a_source_execution_context() {
        let (registry, _, mut context, frame) = fixture("Read");
        context.inner = serde_json::json!({
            "version":2,"workload_id":3,"shard_id":5,"lease_token":6,
            "operator_id":1,"source":"private_events","request_id":"r","epoch":4,
            "rows":[],"operation":"Read"
        });
        assert_eq!(
            registry.unwrap_context(encoded(&context), &frame, true, 100),
            Err("RS-2401: invalid direct routing envelope".into())
        );
    }

    #[test]
    fn source_frames_and_join_bootstrap_bind_source_request_and_epoch() {
        let (registry, _, context, mut frame) = fixture("JoinExecute");
        let input = serde_json::json!({
            "version":2,"workload_id":3,"shard_id":5,"lease_token":6,
            "operator_id":1,"source":"events","request_id":"r","epoch":4,
            "rows":[],"join_bootstrap":{"source":"lookup","rows":[]}
        });
        for bootstrap in [false, true] {
            let mut envelope = context.clone();
            if bootstrap {
                frame.payload = serde_json::to_vec(&input).unwrap();
            } else {
                envelope.inner = input.clone();
            }
            assert_eq!(
                registry.unwrap_context(encoded(&envelope), &frame, true, 100),
                Ok(serde_json::to_vec(&envelope.inner).unwrap().into())
            );
            for (field, value) in [
                ("source", serde_json::json!("lookup")),
                ("request_id", serde_json::json!("another-request")),
                ("epoch", serde_json::json!(5)),
            ] {
                let mut changed = input.clone();
                changed[field] = value;
                if bootstrap {
                    frame.payload = serde_json::to_vec(&changed).unwrap();
                } else {
                    envelope.inner = changed;
                }
                assert_eq!(
                    registry.unwrap_context(encoded(&envelope), &frame, true, 100),
                    Err(
                        "RS-2401: direct epoch capability does not authorize this source epoch"
                            .into()
                    ),
                    "bootstrap={bootstrap} must bind {field}"
                );
            }
        }
    }

    #[test]
    fn partial_tasks_bind_source_and_read_commands_stay_within_granted_fences() {
        let (registry, _, context, mut frame) = fixture("AggregatePartial");
        let mut task = crate::skew_aggregate::PartialAggregateTask {
            spec: crate::skew_aggregate::AggregateSpec {
                source: "events".into(),
                key_column: 0,
                value_column: Some(1),
                literal_value: None,
                column_count: 2,
                function: rockstream_plan::AggregateFunc::Sum,
            },
            rows: Vec::new(),
        };
        frame.payload = serde_json::to_vec(&task).unwrap();
        assert_eq!(
            registry.unwrap_context(encoded(&context), &frame, true, 100),
            Ok(serde_json::to_vec(&context.inner).unwrap().into())
        );
        task.spec.source = "private_events".into();
        frame.payload = serde_json::to_vec(&task).unwrap();
        assert_eq!(
            registry.unwrap_context(encoded(&context), &frame, true, 100),
            Err("RS-2401: direct epoch capability does not authorize this source epoch".into())
        );
        frame.payload = vec![b' '; MAX_ROUTING_METADATA_BYTES + 1];
        assert_eq!(
            registry.unwrap_context(encoded(&context), &frame, true, 100),
            Err("RS-2401: invalid direct routing payload".into())
        );
        frame.payload.clear();
        for operation in ["Read", "GetMany", "JoinRowStats"] {
            let mut read = context.clone();
            read.inner["operation"] = serde_json::json!(operation);
            assert_eq!(
                registry.unwrap_context(encoded(&read), &frame, true, 100),
                Ok(serde_json::to_vec(&read.inner).unwrap().into())
            );
            read.inner["lease_token"] = serde_json::json!(8);
            assert_eq!(
                registry.unwrap_context(encoded(&read), &frame, true, 100),
                Err(
                    "RS-2401: direct epoch capability does not authorize this shard incarnation"
                        .into()
                )
            );
        }
        let mut read = context;
        read.inner["operation"] = serde_json::json!("Read");
        frame.epoch = 0;
        assert_eq!(
            registry.unwrap_context(encoded(&read), &frame, true, 100),
            Err("RS-2401: direct epoch capability is expired or bound to another request".into())
        );
    }

    #[test]
    fn control_restart_cannot_install_conflicting_unexpired_workload_grant() {
        let (registry, grant, _, _) = fixture("Read");
        let mut renewal = grant.clone();
        renewal.grant_id = uuid::Uuid::from_u128(10);
        renewal.expires_at_ms = 250;
        assert!(registry.install(renewal, 100));
        let mut replacement = grant;
        replacement.grant_id = uuid::Uuid::from_u128(11);
        replacement.metadata.request_id = "next".into();
        replacement.expires_at_ms = 400;
        assert!(!registry.install(replacement.clone(), 201));
        assert!(registry.install(replacement.clone(), 250));
        assert_eq!(
            registry.grants.read().clone(),
            HashMap::from([(replacement.grant_id, replacement)])
        );
    }

    #[tokio::test]
    async fn replacement_drains_admitted_commands_and_waiters_revalidate_expiry() {
        let (registry, grant, context, frame) = fixture("Read");
        let context = encoded(&context);
        let admitted = registry.command_guard(&context, 100).await.unwrap();
        let mut replacement = Box::pin(registry.installation_guard(grant.metadata.workload_id));
        assert!(futures::poll!(replacement.as_mut()).is_pending());
        drop(admitted);
        let replacement = replacement.await.unwrap();
        let mut queued = Box::pin(registry.command_guard(&context, 199));
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(replacement);
        let queued = queued.await.unwrap();
        assert_eq!(
            registry.unwrap_context(context, &frame, true, 200),
            Err("RS-2401: direct epoch capability is expired or bound to another request".into())
        );
        drop(queued);
        let _next = registry.installation_guard(WorkloadId(8)).await.unwrap();
        assert_eq!(
            registry
                .admission
                .lock()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![WorkloadId(8)]
        );
    }

    #[test]
    fn rollback_revokes_only_attempted_grant_and_completion_releases_the_workload() {
        let (registry, grant, _, _) = fixture("Read");
        let mut renewal = grant.clone();
        renewal.grant_id = uuid::Uuid::from_u128(10);
        assert!(registry.install(renewal.clone(), 100));
        registry.revoke(&renewal.metadata, Some(renewal.grant_id));
        assert_eq!(
            registry.grants.read().clone(),
            HashMap::from([(grant.grant_id, grant.clone())])
        );
        let record = RoutingEpochRecord {
            version: 1,
            workload_id: grant.metadata.workload_id,
            source: grant.metadata.source.clone(),
            request_id: grant.metadata.request_id.clone(),
            epoch: grant.metadata.epoch,
            input_digest: grant.metadata.input_digest,
            generation: 1,
            shard_ids: vec![ShardId(5)],
            plan: serde_json::Value::Null,
            next_state: serde_json::Value::Null,
            point_mutations: BTreeMap::new(),
        };
        registry.revoke_completed(&record);
        assert_eq!(registry.grants.read().clone(), HashMap::new());
        let mut next = grant;
        next.grant_id = uuid::Uuid::from_u128(11);
        next.metadata.epoch += 1;
        next.metadata.request_id = "next".into();
        assert!(registry.install(next.clone(), 100));
        registry.revoke(&record_metadata(&record), None);
        assert_eq!(
            registry.grants.read().clone(),
            HashMap::from([(next.grant_id, next.clone())])
        );
        registry.revoke(&next.metadata, None);
        assert_eq!(registry.grants.read().clone(), HashMap::new());
    }

    fn record_metadata(record: &RoutingEpochRecord) -> SourceEpochMetadata {
        SourceEpochMetadata {
            version: 2,
            workload_id: record.workload_id,
            source: record.source.clone(),
            request_id: record.request_id.clone(),
            epoch: record.epoch,
            input_digest: record.input_digest,
        }
    }

    #[test]
    fn grant_binds_workload_source_epoch_fence_and_expiration_exactly() {
        let registry = DirectGrantRegistry::default();
        let grant = DirectEpochGrant {
            grant_id: uuid::Uuid::from_u128(9),
            metadata: SourceEpochMetadata {
                version: 2,
                workload_id: WorkloadId(3),
                request_id: "r".into(),
                source: "events".into(),
                epoch: 4,
                input_digest: [7; 32],
            },
            expires_at_ms: 200,
            members: BTreeMap::from([(ShardId(5), LeaseToken(6))]),
        };
        assert!(registry.install(grant.clone(), 100));
        let inner = serde_json::json!({"workload_id":3,"shard_id":5,"lease_token":6,"source":"events","request_id":"r","epoch":4});
        let context = AuthorizedOwnedContext {
            direct_grant_id: grant.grant_id,
            metadata: grant.metadata,
            inner: inner.clone(),
        };
        let bytes = bytes::Bytes::from(serde_json::to_vec(&context).unwrap());
        let frame = ShuffleFrame {
            exchange_id: 3,
            src_shard: 0,
            target_shard: 0,
            epoch: 4,
            seq: 0,
            payload: Vec::new(),
            row_count: 0,
        };
        assert_eq!(
            registry
                .unwrap_context(bytes.clone(), &frame, true, 199)
                .unwrap(),
            bytes::Bytes::from(serde_json::to_vec(&inner).unwrap())
        );
        assert_eq!(
            registry
                .unwrap_context(bytes.clone(), &frame, true, 200)
                .unwrap_err(),
            "RS-2401: direct epoch capability is expired or bound to another request"
        );
        let mut changed = context.clone();
        changed.inner["lease_token"] = serde_json::json!(7);
        assert_eq!(
            registry
                .unwrap_context(
                    serde_json::to_vec(&changed).unwrap().into(),
                    &frame,
                    true,
                    100
                )
                .unwrap_err(),
            "RS-2401: direct epoch capability does not authorize this shard incarnation"
        );
        changed = context;
        changed.inner["source"] = serde_json::json!("other");
        assert_eq!(
            registry
                .unwrap_context(
                    serde_json::to_vec(&changed).unwrap().into(),
                    &frame,
                    true,
                    100
                )
                .unwrap_err(),
            "RS-2401: direct epoch capability does not authorize this source epoch"
        );
        assert_eq!(
            registry
                .unwrap_context(b"{}".as_slice().into(), &frame, true, 100)
                .unwrap_err(),
            "RS-2401: direct gateway writes require an actor-authorized epoch capability"
        );
    }
}
