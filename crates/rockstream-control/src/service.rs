//! Control-plane network service for RockStream.
//!
//! The `ControlService` listens on a TCP address and accepts connections from
//! worker nodes. Workers send [`WorkerMessage`] frames (newline-delimited JSON)
//! and receive [`ControlMessage`] responses.
//!
//! ## Wire protocol
//!
//! Each message is a single-line JSON object terminated by `\n`.
//! Messages are framed without any length prefix: each line is one message.
//!
//! ```text
//! Worker → Control:  {"type":"register", ...}\n
//! Control → Worker:  {"type":"registered","worker_id":1}\n
//! Worker → Control:  {"type":"heartbeat","worker_id":1,"capacity_headroom":0.8}\n
//! Worker → Control:  {"type":"request_shard","worker_id":1,"shard_id":5}\n
//! Control → Worker:  {"type":"shard_assigned","lease":{...}}\n
//! Worker → Control:  {"type":"fence_write","shard_id":5,"lease_token":3}\n
//! Control → Worker:  {"type":"fence_ack","shard_id":5,"valid":true}\n
//! Worker → Control:  {"type":"deregister","worker_id":1}\n
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex};

use rockstream_storage::catalog::CatalogStore;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, PolicyScope, SecurityAction, SecurityAuditClass,
    SecurityAuditOutcome, SecurityAuditRecord, SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::data_plane::{
    DeploymentDescriptor, DeploymentRequest, RuntimeExchangeMessage, RuntimeRow, ShardOutput,
    SourceDeltaRequest, WorkerExecutionStatus, WorkloadSnapshot,
};
use rockstream_types::direct_epoch::{DirectEpochGrant, SourceEpochMetadata};
use rockstream_types::error_code::{
    ErrorCode, RS_0001, RS_0003, RS_2400, RS_2401, RS_2410, RS_2411, RS_2412, RS_3604, RS_3610,
    RS_3611, RS_3612, RS_8004,
};
use rockstream_types::identity::{InternalTlsConfig, NodeIdentity, NodeRole};
use rockstream_types::ids::{LeaseToken, PrincipalId, ShardId, WorkerId, WorkloadId};
use rockstream_types::lease::ShardRevokeReason;
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};
use rockstream_types::topology::{
    ControlMessage, DrainRequest, RaftRoleWire, WorkerLifecycleState, WorkerMessage,
};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::frontier::FrontierAggregator;
use crate::management_store::{ManagementOperationStore, OperationStatus, OperationUpdate};
use crate::migration::{
    MigrationPersistentStore, MigrationShard, MigrationStartupOutcome, MigrationStartupStatus,
};
use crate::placement::PlacementAlgorithm;
use crate::raft::{RaftHandle, RaftRole};
use crate::scheduler::ShardScheduler;
use crate::secret_store::{SecretStore, SecretStoreError};
use crate::shard::{ShardManager, ShardPersistentStore};
use crate::topology::{TopologyCatalog, TopologyPersistentStore};

const DEFAULT_DRAIN_DEADLINE_MS: u64 = 30_000;
const DEFAULT_DECOMMISSION_GRACE_MS: u64 = 5_000;
const MAX_DRAIN_QUEUE: usize = 1024;
const DEFAULT_VIRTUAL_BUCKET_COUNT: u64 = 4096;
pub(crate) const MAX_MANAGEMENT_ACK_WAITERS: usize = 64;

pub(crate) type ManagementAckWaiters =
    Arc<AsyncMutex<HashMap<String, oneshot::Sender<WorkerMessage>>>>;

fn try_insert_management_ack_waiter(
    waiters: &mut HashMap<String, oneshot::Sender<WorkerMessage>>,
    key: String,
    sender: oneshot::Sender<WorkerMessage>,
) -> Result<(), &'static str> {
    if waiters.contains_key(&key) {
        return Err("management ACK waiter key already exists");
    }
    if waiters.len() >= MAX_MANAGEMENT_ACK_WAITERS {
        return Err("management ACK waiter capacity 64 reached");
    }
    waiters.insert(key, sender);
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn configured_shared_shard_store_id() -> Option<[u8; 32]> {
    let endpoint = std::env::var("ROCKSTREAM_OBJECT_STORE_ENDPOINT").ok()?;
    let bucket = std::env::var("ROCKSTREAM_OBJECT_STORE_BUCKET").ok()?;
    let region =
        std::env::var("ROCKSTREAM_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let mut digest = Sha256::new();
    for value in [endpoint, bucket, region] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    Some(digest.finalize().into())
}

fn secret_error_code(error: &SecretStoreError) -> rockstream_types::error_code::ErrorCode {
    match error {
        SecretStoreError::NotFound { code, .. }
        | SecretStoreError::AlreadyExists { code, .. }
        | SecretStoreError::EncryptionFailed { code, .. }
        | SecretStoreError::TokenInvalid { code, .. }
        | SecretStoreError::DdlInvalid { code, .. }
        | SecretStoreError::RotationFailed { code, .. }
        | SecretStoreError::InUse { code, .. }
        | SecretStoreError::CapacityExceeded { code, .. }
        | SecretStoreError::Storage { code, .. } => *code,
    }
}

fn error_code_for_secret_error(error: &SecretStoreError) -> String {
    secret_error_code(error).to_string()
}

#[derive(Debug, Clone)]
struct DrainTask {
    donor_worker_id: WorkerId,
    recipient_worker_id: WorkerId,
    shard_id: ShardId,
}

#[derive(Debug, Clone)]
struct DrainFailure {
    code: &'static str,
    message: String,
    next_steps: String,
}

impl DrainFailure {
    fn new(
        code: impl std::fmt::Display,
        message: impl Into<String>,
        next_steps: impl Into<String>,
    ) -> Self {
        let code = Box::leak(code.to_string().into_boxed_str());
        Self {
            code,
            message: message.into(),
            next_steps: next_steps.into(),
        }
    }
}

#[derive(Default)]
struct DrainState {
    queue: std::collections::VecDeque<DrainTask>,
    operation_ids: HashMap<WorkerId, Vec<String>>,
    acked_workers: HashSet<WorkerId>,
    processing_workers: HashSet<WorkerId>,
}

#[derive(Default)]
struct DataPlaneState {
    direct_epochs: HashMap<WorkloadId, rockstream_types::direct_epoch::SourceEpochMetadata>,
    direct_epoch_deadlines: HashMap<WorkloadId, u64>,
    /// One installation owner per workload; deadlines are not unique attempt IDs.
    direct_installing: HashMap<WorkloadId, uuid::Uuid>,
    /// Revocation must finish before another capability can be installed.
    direct_completing: HashMap<WorkloadId, SourceEpochMetadata>,
    /// At most one latest receipt per deployed workload; redeployment clears it.
    direct_completed: HashMap<WorkloadId, SourceEpochMetadata>,
    direct_grant_waiters: HashMap<uuid::Uuid, mpsc::Sender<(WorkerId, bool)>>,
    deployments: HashMap<WorkloadId, DeploymentState>,
    source_waiters: HashMap<String, SourceWaiter>,
    operator_stats_waiters: HashMap<String, OperatorStatsWaiter>,
    workload_read_waiters: HashMap<String, WorkloadReadWaiter>,
    owned_shuffle_workers: HashSet<WorkerId>,
    /// Shards currently paused in the brief donor-to-recipient handoff window.
    handoff_shards: HashSet<ShardId>,
    /// Notifies waiting `submit_source_delta` callers when a shard handoff ends.
    handoff_notify: Arc<tokio::sync::Notify>,
    /// One-shot waiters notified when a recipient worker confirms
    /// `DeploymentReady` during an online migration or drain handoff.
    handoff_ready_waiters: HashMap<(WorkloadId, ShardId, WorkerId), oneshot::Sender<()>>,
    /// Monotonically increasing placement/topology epoch for direct data-plane
    /// clients (Issue #400 R1/R6).
    topology_epoch: u64,
    /// Explicit single-owner connector lease manager (Issue #400 R1).
    connector_leases: crate::shard::ConnectorLeaseManager,
    /// Total row payloads submitted through the legacy control-mediated
    /// `SubmitSourceDelta` path (must remain 0 on the direct Arrow IPC path).
    row_payloads_received: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone)]
struct DelegatedActor {
    principal_id: PrincipalId,
    secret_use_scopes: Vec<PolicyScope>,
}

struct DeploymentState {
    request: DeploymentRequest,
    actor: Option<DelegatedActor>,
    secret_names: Vec<String>,
    descriptors: HashMap<ShardId, DeploymentDescriptor>,
    workers: HashMap<WorkerId, WorkerExecutionStatus>,
    ready_shards: HashSet<ShardId>,
    ready_waiter: Option<mpsc::Sender<ControlMessage>>,
    // shortcut: recovery state is in memory; persist it before restart-safe admission.
    recovery_required: bool,
}

struct AuthorizedWorkerSecret {
    namespace_id: u128,
    scope: PolicyScope,
    actor_principal_id: Option<PrincipalId>,
    policy_revision: AuditField<u64>,
}

#[derive(Debug)]
enum WorkerSecretAuthorizationError {
    Denied(&'static str),
    DeniedForActor {
        message: &'static str,
        actor: PrincipalId,
        scope: PolicyScope,
        policy_revision: AuditField<u64>,
    },
    Unavailable(&'static str),
}

struct SourceWaiter {
    attempt_id: Option<uuid::Uuid>,
    sender: mpsc::Sender<ControlMessage>,
    workload_id: WorkloadId,
    version: u32,
    source: String,
    expected: HashSet<ShardId>,
    received: HashSet<ShardId>,
    dispatchers: HashMap<ShardId, WorkerId>,
    owned_intent: Option<rockstream_types::data_plane::OwnedEpochIntent>,
    prepare_reply: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    finish_reply: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    finishing: bool,
    epoch: u64,
}

enum SourceCompletion {
    Immediate(Box<SourceWaiter>, String),
    Owned(
        WorkerId,
        rockstream_types::data_plane::OwnedEpochIntent,
        tokio::sync::oneshot::Receiver<Result<(), String>>,
    ),
    MissingAnchor(String, WorkloadId),
}

impl SourceWaiter {
    fn record_progress(
        &mut self,
        output: &rockstream_types::data_plane::RuntimeOutputDelta,
    ) -> bool {
        if self.attempt_id != output.attempt_id
            || self.workload_id != output.workload_id
            || self.epoch != output.epoch
            || self.version != output.version
            || self.source != output.source
            || !self.expected.contains(&output.shard_id)
            || !self.received.insert(output.shard_id)
        {
            return false;
        }
        self.received == self.expected
    }
}

fn take_owned_epoch_reply(
    state: &mut DataPlaneState,
    connected_worker: Option<WorkerId>,
    worker_id: WorkerId,
    intent: &rockstream_types::data_plane::OwnedEpochIntent,
    finishing: bool,
) -> Option<tokio::sync::oneshot::Sender<Result<(), String>>> {
    if connected_worker != Some(worker_id)
        || state
            .deployments
            .get(&intent.workload_id)
            .and_then(|deployment| deployment.descriptors.get(&intent.anchor_shard))
            .is_none_or(|descriptor| descriptor.shard.worker_id != worker_id)
    {
        return None;
    }
    let waiter = state.source_waiters.get_mut(&intent.request_id)?;
    if waiter.owned_intent.as_ref() != Some(intent) || waiter.finishing != finishing {
        return None;
    }
    if finishing {
        waiter.finish_reply.take()
    } else {
        waiter.prepare_reply.take()
    }
}

struct OperatorStatsWaiter {
    workload_id: WorkloadId,
    expected_workers: HashSet<WorkerId>,
    received_workers: HashSet<WorkerId>,
    operator_ids: HashSet<rockstream_types::ids::OperatorId>,
    sender: mpsc::Sender<(
        WorkerId,
        Vec<rockstream_types::explain::IdentifiedOperatorStats>,
    )>,
}

struct WorkloadReadWaiter {
    workload_id: WorkloadId,
    expected_shards: HashMap<WorkerId, HashMap<ShardId, LeaseToken>>,
    received_workers: HashSet<WorkerId>,
    sender: mpsc::Sender<(WorkerId, bool, Vec<ShardOutput>)>,
}

fn workload_topology_snapshot(
    state: &DataPlaneState,
    workload_id: WorkloadId,
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
) -> Option<rockstream_types::data_plane::WorkloadTopologySnapshot> {
    let deployment = state.deployments.get(&workload_id)?;
    let workers = catalog.all_workers();
    let worker_addrs: HashMap<_, _> = workers
        .iter()
        .map(|worker| (worker.worker_id, worker.address.clone()))
        .collect();
    let mut shards: Vec<_> = deployment
        .descriptors
        .values()
        .map(|descriptor| {
            let lease = shard_manager
                .get(descriptor.shard.shard_id)
                .unwrap_or_else(|| descriptor.shard.clone());
            rockstream_types::data_plane::WorkloadShardPlacement {
                shard_id: lease.shard_id,
                shard_index: descriptor.shard_index,
                worker_id: lease.worker_id,
                lease_token: lease.lease_token,
                worker_address: worker_addrs
                    .get(&lease.worker_id)
                    .cloned()
                    .unwrap_or_default(),
                operator_id: descriptor.sink_operator_id,
                storage_identity: descriptor.storage_identity.clone(),
                frontier: descriptor.frontier,
            }
        })
        .collect();
    shards.sort_by_key(|shard| (shard.shard_index, shard.shard_id));
    Some(rockstream_types::data_plane::WorkloadTopologySnapshot {
        topology_epoch: state.topology_epoch.max(1),
        workload_id,
        plan_json: deployment.request.plan_json.clone(),
        routing_columns: deployment.request.routing_columns.clone(),
        schemas: deployment.request.schemas.clone(),
        shards,
        workers,
    })
}

async fn authorize_direct_epoch(
    metadata: rockstream_types::direct_epoch::SourceEpochMetadata,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
) -> ControlMessage {
    use rockstream_types::direct_epoch::DIRECT_EPOCH_GRANT_TTL_MS;
    if !metadata.is_valid() {
        return data_plane_failure("RS-1012: invalid direct epoch metadata");
    }
    let topology = {
        let state = data_plane.lock().await;
        let Some(deployment) = state.deployments.get(&metadata.workload_id) else {
            return data_plane_failure("workload is not deployed");
        };
        match direct_completion_status(&state, &metadata) {
            Ok(true) => return direct_completion_reply(&metadata),
            Err(error) => return data_plane_failure(error),
            Ok(false) => {}
        }
        if deployment.recovery_required
            || deployment.ready_waiter.is_some()
            || deployment
                .descriptors
                .keys()
                .any(|shard| state.handoff_shards.contains(shard))
            || deployment.descriptors.iter().any(|(id, descriptor)| {
                !shard_manager
                    .get(*id)
                    .is_some_and(|lease| lease.same_owner(&descriptor.shard))
            })
        {
            return data_plane_failure("RS-3004: workload ownership is changing");
        }
        let Some(topology) =
            workload_topology_snapshot(&state, metadata.workload_id, catalog, shard_manager)
        else {
            return data_plane_failure("workload is not deployed");
        };
        topology
    };
    let target_workers: HashSet<_> = topology
        .shards
        .iter()
        .map(|shard| shard.worker_id)
        .collect();
    if topology.shards.is_empty()
        || topology.shards.len() > 4096
        || topology
            .shards
            .iter()
            .any(|shard| shard.worker_address.is_empty())
        || target_workers
            .iter()
            .any(|id| !catalog.get(*id).is_some_and(|worker| worker.healthy))
    {
        return data_plane_failure("RS-3004: direct epoch recipients are not ready");
    }
    let grant = DirectEpochGrant {
        grant_id: uuid::Uuid::new_v4(),
        metadata,
        expires_at_ms: now_ms().saturating_add(DIRECT_EPOCH_GRANT_TTL_MS),
        members: topology
            .shards
            .iter()
            .map(|shard| (shard.shard_id, shard.lease_token))
            .collect(),
    };
    let (tx, mut rx) = mpsc::channel(target_workers.len());
    let previous_reservation = {
        let mut state = data_plane.lock().await;
        match direct_completion_status(&state, &grant.metadata) {
            Ok(true) => return direct_completion_reply(&grant.metadata),
            Err(error) => return data_plane_failure(error),
            Ok(false) => {}
        }
        match reserve_direct_grant(&mut state, &grant, tx, now_ms()) {
            Ok(previous) => previous,
            Err(error) => return data_plane_failure(error),
        }
    };
    let targets: Vec<_> = {
        let senders = worker_senders.lock().await;
        target_workers
            .iter()
            .filter_map(|id| senders.get(id).cloned())
            .collect()
    };
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        if targets.len() != target_workers.len() {
            return false;
        }
        for target in targets {
            if target
                .send(ControlMessage::InstallDirectEpochGrant {
                    grant: grant.clone(),
                })
                .await
                .is_err()
            {
                return false;
            }
        }
        let mut accepted = HashSet::new();
        while accepted.len() < target_workers.len() {
            let Some((worker, ready)) = rx.recv().await else {
                return false;
            };
            if !target_workers.contains(&worker) || !ready {
                return false;
            }
            accepted.insert(worker);
        }
        true
    })
    .await
    .unwrap_or(false);
    let ownership_current = topology.shards.iter().all(|shard| {
        shard_manager.get(shard.shard_id).is_some_and(|lease| {
            lease.worker_id == shard.worker_id && lease.lease_token == shard.lease_token
        })
    });
    {
        let mut state = data_plane.lock().await;
        state.direct_grant_waiters.remove(&grant.grant_id);
        if result
            && ownership_current
            && state.direct_installing.get(&grant.metadata.workload_id) == Some(&grant.grant_id)
            && state.direct_epochs.get(&grant.metadata.workload_id) == Some(&grant.metadata)
            && state.direct_completed.get(&grant.metadata.workload_id) != Some(&grant.metadata)
            && !state
                .direct_completing
                .contains_key(&grant.metadata.workload_id)
        {
            state.direct_installing.remove(&grant.metadata.workload_id);
            return ControlMessage::DirectEpochAuthorized { grant, topology };
        }
    }
    // A completion can arrive while recipients are acknowledging a renewal.
    // The notification records it without waiting on this install, so its
    // worker connection remains able to deliver the pending acknowledgements.
    revoke_direct_grants(
        &grant.metadata,
        Some(grant.grant_id),
        &target_workers,
        worker_senders,
    )
    .await;
    let completed = {
        let mut state = data_plane.lock().await;
        if state.direct_completed.get(&grant.metadata.workload_id) == Some(&grant.metadata) {
            true
        } else {
            restore_failed_direct_reservation(&mut state, &grant, previous_reservation);
            false
        }
    };
    if completed {
        revoke_direct_grants(&grant.metadata, None, &target_workers, worker_senders).await;
        let mut state = data_plane.lock().await;
        if state.direct_installing.get(&grant.metadata.workload_id) == Some(&grant.grant_id) {
            state.direct_installing.remove(&grant.metadata.workload_id);
            release_completed_direct_reservation(&mut state, &grant.metadata);
        }
        return direct_completion_reply(&grant.metadata);
    }
    data_plane_failure(if result && !ownership_current {
        "RS-3004: direct epoch recipient ownership changed"
    } else {
        "RS-3004: direct epoch recipient authorization was not acknowledged"
    })
}

fn direct_completion_status(
    state: &DataPlaneState,
    metadata: &SourceEpochMetadata,
) -> Result<bool, &'static str> {
    match state.direct_completed.get(&metadata.workload_id) {
        Some(completed) if completed == metadata => Ok(true),
        Some(completed) if metadata.epoch <= completed.epoch => {
            Err("RS-1012: direct source epoch conflicts with completed input")
        }
        _ => Ok(false),
    }
}

fn direct_completion_reply(metadata: &SourceEpochMetadata) -> ControlMessage {
    ControlMessage::SourceDeltaCommitted {
        request_id: metadata.request_id.clone(),
        epoch: metadata.epoch,
    }
}

type DirectReservation = Option<(SourceEpochMetadata, Option<u64>)>;

fn reserve_direct_grant(
    state: &mut DataPlaneState,
    grant: &DirectEpochGrant,
    waiter: mpsc::Sender<(WorkerId, bool)>,
    now: u64,
) -> Result<DirectReservation, &'static str> {
    let workload = grant.metadata.workload_id;
    if state.direct_installing.contains_key(&workload)
        || state.direct_completing.contains_key(&workload)
        || (state
            .direct_epochs
            .get(&workload)
            .is_some_and(|pending| pending != &grant.metadata)
            && state
                .direct_epoch_deadlines
                .get(&workload)
                .is_none_or(|deadline| *deadline > now))
        || state
            .source_waiters
            .values()
            .any(|pending| pending.workload_id == workload)
        || state
            .workload_read_waiters
            .values()
            .any(|reader| reader.workload_id == workload)
        || grant
            .members
            .keys()
            .any(|shard| state.handoff_shards.contains(shard))
    {
        return Err("RS-3004: workload has an active read, write, or ownership handoff");
    }
    if state.direct_grant_waiters.len() >= rockstream_types::direct_epoch::MAX_DIRECT_EPOCH_GRANTS {
        return Err("RS-5036: direct epoch authorization capacity exhausted");
    }
    let previous = state.direct_epochs.get(&workload).cloned().map(|metadata| {
        (
            metadata,
            state.direct_epoch_deadlines.get(&workload).copied(),
        )
    });
    state.direct_grant_waiters.insert(grant.grant_id, waiter);
    state.direct_installing.insert(workload, grant.grant_id);
    state.direct_epochs.insert(workload, grant.metadata.clone());
    // Recipients validate durable pending journals before acknowledging a
    // replacement; expiry only releases the control reservation's time bound.
    state
        .direct_epoch_deadlines
        .insert(workload, grant.expires_at_ms);
    Ok(previous)
}

/// Returns a revoke action only when no installer is responsible for cleanup.
/// In-flight installation must continue receiving worker ACKs, not wait here.
fn record_direct_completion(
    state: &mut DataPlaneState,
    metadata: SourceEpochMetadata,
) -> Option<SourceEpochMetadata> {
    let workload = metadata.workload_id;
    if !state.deployments.contains_key(&workload)
        || state.direct_epochs.get(&workload) != Some(&metadata)
    {
        return None;
    }
    state.direct_completed.insert(workload, metadata.clone());
    if state.direct_completing.contains_key(&workload) {
        return None;
    }
    state.direct_completing.insert(workload, metadata.clone());
    (!state.direct_installing.contains_key(&workload)).then_some(metadata)
}

fn release_completed_direct_reservation(
    state: &mut DataPlaneState,
    metadata: &SourceEpochMetadata,
) {
    let workload = metadata.workload_id;
    if state.direct_installing.contains_key(&workload)
        || state.direct_completing.get(&workload) != Some(metadata)
    {
        return;
    }
    if state.direct_epochs.get(&workload) == Some(metadata) {
        state.direct_epochs.remove(&workload);
        state.direct_epoch_deadlines.remove(&workload);
    }
    state.direct_completing.remove(&workload);
}

fn routing_completion_members(
    state: &DataPlaneState,
    shard_manager: &ShardManager,
    workload: WorkloadId,
    worker: WorkerId,
    anchor: ShardId,
    anchor_lease: LeaseToken,
    reported: &[(ShardId, LeaseToken)],
) -> Option<BTreeMap<ShardId, LeaseToken>> {
    if reported.is_empty()
        || reported.len() > 4096
        || reported.windows(2).any(|pair| pair[0].0 >= pair[1].0)
    {
        return None;
    }
    let deployment = state.deployments.get(&workload)?;
    let anchor_descriptor = deployment
        .descriptors
        .values()
        .min_by_key(|descriptor| descriptor.shard.shard_id)?;
    if anchor_descriptor.shard.shard_id != anchor
        || anchor_descriptor.shard.worker_id != worker
        || anchor_descriptor.shard.lease_token != anchor_lease
    {
        return None;
    }
    let mut expected = BTreeMap::new();
    for (shard_id, descriptor) in &deployment.descriptors {
        let lease = shard_manager.get(*shard_id)?;
        if !lease.same_owner(&descriptor.shard) {
            return None;
        }
        expected.insert(*shard_id, lease.lease_token);
    }
    let members: BTreeMap<_, _> = reported.iter().copied().collect();
    (members == expected).then_some(members)
}

async fn revoke_direct_grants(
    metadata: &rockstream_types::direct_epoch::SourceEpochMetadata,
    grant_id: Option<uuid::Uuid>,
    target_workers: &HashSet<WorkerId>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
) {
    let senders: Vec<_> = {
        let senders = worker_senders.lock().await;
        target_workers
            .iter()
            .filter_map(|id| senders.get(id).cloned())
            .collect()
    };
    // Enqueue before releasing the admission pin. Subsequent installations use
    // these same ordered control channels, so no stale capability survives a
    // successfully committed epoch or a partially installed failed grant.
    for sender in senders {
        let _ = sender
            .send(ControlMessage::RevokeDirectEpochGrants {
                metadata: metadata.clone(),
                grant_id,
            })
            .await;
    }
}

fn restore_failed_direct_reservation(
    state: &mut DataPlaneState,
    grant: &DirectEpochGrant,
    previous: DirectReservation,
) {
    let workload = grant.metadata.workload_id;
    if state.direct_installing.get(&workload) != Some(&grant.grant_id)
        || state.direct_epochs.get(&workload) != Some(&grant.metadata)
    {
        return;
    }
    state.direct_installing.remove(&workload);
    match previous {
        Some((metadata, deadline)) => {
            state.direct_epochs.insert(workload, metadata);
            if let Some(deadline) = deadline {
                state.direct_epoch_deadlines.insert(workload, deadline);
            } else {
                state.direct_epoch_deadlines.remove(&workload);
            }
        }
        None => {
            state.direct_epochs.remove(&workload);
            state.direct_epoch_deadlines.remove(&workload);
        }
    }
}

/// Convert the internal [`RaftRole`] to its wire-serializable mirror.
fn raft_role_wire(role: RaftRole) -> RaftRoleWire {
    match role {
        RaftRole::Follower => RaftRoleWire::Follower,
        RaftRole::Candidate => RaftRoleWire::Candidate,
        RaftRole::Leader => RaftRoleWire::Leader,
    }
}

/// Lazily re-synchronize this node's [`ShardManager`] from the shared
/// control-plane object store the first time it observes itself as leader
/// at a given control-leader epoch (v0.45.2 M7-S4/S5).
///
/// A real control node process starts with an empty in-memory
/// `ShardManager` (there is no cross-process replication of every
/// `acquire`/`release` call — only the durable snapshot at
/// `control/shard_manager/state.json`). Without this step, a newly-elected
/// leader on a *different real process* than the one that most recently
/// held leases would believe every shard is unleased and could grant a
/// conflicting lease to a different worker — exactly the split-brain
/// window S3/S4 must rule out. `synced_epoch` remembers the last epoch this
/// node has already synced for, so the (network-bound, for the MinIO/TC
/// profile) load only happens once per leadership term, not on every
/// request.
async fn ensure_shard_state_synced(
    shard_manager: &ShardManager,
    shard_store: &ShardPersistentStore,
    synced_epoch: &AsyncMutex<Option<u64>>,
    current_epoch: u64,
) {
    let mut guard = synced_epoch.lock().await;
    if *guard != Some(current_epoch) {
        let snapshot = shard_store.load().await;
        shard_manager.restore(snapshot);
        shard_manager.set_leader_epoch(current_epoch);
        *guard = Some(current_epoch);
        tracing::info!(
            epoch = current_epoch,
            leases = shard_manager.len(),
            "control: shard-manager state synced from shared store on leadership takeover"
        );
    }
}

/// Persist the current `ShardManager` state to the shared store, if one is
/// configured (v0.45.2 M7-S4/S5 write-through — the *next* leader, possibly
/// on a different real process, must be able to see this write).
async fn persist_shard_state(shard_manager: &ShardManager, shard_store: &ShardPersistentStore) {
    let snapshot = shard_manager.snapshot();
    shard_store.save(&snapshot).await;
}

/// Handle to the running control service.
pub struct ControlServiceHandle {
    /// Bound address.
    pub addr: SocketAddr,
    /// Shutdown sender; drop or send to stop the service.
    shutdown_tx: broadcast::Sender<()>,
    management: Option<crate::management::ManagementServiceHandle>,
    /// Explicit outcomes for every active record inspected on startup.
    pub migration_recovery: Vec<MigrationStartupOutcome>,
    /// TLS certificate reloader (if internal mTLS is enabled).
    pub reloader: Option<Arc<crate::tls::TlsCertificateReloader>>,
    management_reloader: Option<Arc<crate::tls::TlsCertificateReloader>>,
    row_payloads_received: Arc<std::sync::atomic::AtomicU64>,
}

impl ControlServiceHandle {
    /// Total row payloads received via legacy control-mediated `SubmitSourceDelta`.
    /// Must remain `0` when all ingestion flows over the direct Arrow IPC path.
    pub fn row_payloads_received(&self) -> u64 {
        self.row_payloads_received
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Signal the service to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        if let Some(management) = &self.management {
            management.shutdown();
        }
    }

    /// Reload the server certificate, private key, and/or CA certificate without restarting.
    pub fn reload_tls(&self, new_config: InternalTlsConfig) -> Result<(), String> {
        if let Some(ref r) = self.reloader {
            r.reload(new_config)
        } else {
            Err("RS-2410: internal TLS is not enabled on this control service".to_string())
        }
    }

    /// Reload a separately configured management listener certificate in place.
    pub fn reload_management_tls(&self, new_config: InternalTlsConfig) -> Result<(), String> {
        if let Some(reloader) = &self.management_reloader {
            reloader.reload(new_config)
        } else {
            Err("RS-2410: management TLS is not enabled on this control service".to_owned())
        }
    }
}

type BackupSource = (
    Arc<dyn object_store::ObjectStore>,
    Option<[u8; 32]>,
    Option<WorkerId>,
);

/// Control-plane service: listens for worker registrations and shard lease
/// requests on TCP.
pub struct ControlService {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    audit: Option<Arc<FileAuditLog>>,
    /// If attached, leader-gated writes (shard lease grants — M7-S2) are
    /// rejected with `RS-1731`/[`ControlMessage::NotLeader`] unless this
    /// node is currently the Raft-elected control-plane leader. Absent by
    /// default, preserving exact pre-v0.45.2 single-node behavior for every
    /// existing caller/test.
    raft: Option<RaftHandle>,
    /// If attached, the `ShardManager`'s lease state is loaded from (and
    /// written through to) this shared store whenever this node observes
    /// itself becoming the Raft leader at a new epoch (v0.45.2 M7-S4/S5).
    /// Absent by default: the `ShardManager` then stays purely in-memory,
    /// exactly as before v0.45.2.
    shard_store: Option<Arc<ShardPersistentStore>>,
    /// If attached, `ReportShardFrontier` messages are ingested into this
    /// aggregator and, when this node is the current leader, published as
    /// `ClusterFrontierAdvanced` (v0.45.2 M7-S4 "frontier publication
    /// resumes within budget"). Absent by default (pre-v0.45.2 behavior:
    /// no frontier ingestion over the worker wire protocol at all).
    frontier: Option<Arc<FrontierAggregator>>,
    /// Optional durable topology store for worker lifecycle persistence (v0.46).
    topology_store: Option<Arc<TopologyPersistentStore>>,
    /// Optional durable migration record store (v0.46).
    migration_store: Option<Arc<MigrationPersistentStore>>,
    /// Already-open donor shards authorized for startup cleanup replay.
    migration_recovery_donors: BTreeMap<ShardId, MigrationShard>,
    /// Shared drain queue state with a named bound.
    drain_state: Arc<AsyncMutex<DrainState>>,
    /// Optional internal TLS configuration for control plane mTLS.
    internal_tls: Option<InternalTlsConfig>,
    management_tls: Option<InternalTlsConfig>,
    /// Explicit loopback-only plaintext transport for local development.
    insecure_dev_mode: bool,
    secret_store: Arc<SecretStore>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    management: Option<(
        String,
        Arc<dyn object_store::ObjectStore>,
        rockstream_types::config::NodeConfig,
    )>,
    security_catalog: Option<Arc<dyn CatalogStore>>,
    actor_assertion_verifier: Option<Arc<crate::delegation::ActorAssertionVerifier>>,
    backup_source: Option<BackupSource>,
}

impl ControlService {
    /// Create a new `ControlService` backed by the given catalog.
    pub fn new(catalog: TopologyCatalog) -> Self {
        Self {
            catalog,
            shard_manager: ShardManager::new(),
            audit: None,
            raft: None,
            shard_store: None,
            frontier: None,
            topology_store: None,
            migration_store: None,
            migration_recovery_donors: BTreeMap::new(),
            drain_state: Arc::new(AsyncMutex::new(DrainState::default())),
            internal_tls: None,
            management_tls: None,
            insecure_dev_mode: false,
            secret_store: Arc::new(SecretStore::new(
                None,
                Arc::new(crate::kek::EnvKekProvider::from_env()),
            )),
            worker_senders: Arc::new(AsyncMutex::new(HashMap::new())),
            data_plane: Arc::new(AsyncMutex::new(DataPlaneState::default())),
            management: None,
            security_catalog: None,
            actor_assertion_verifier: None,
            backup_source: None,
        }
    }

    /// Attach a pre-existing [`ShardManager`].  Useful when tests or the
    /// binary want to share a manager instance across multiple services.
    pub fn with_shard_manager(mut self, manager: ShardManager) -> Self {
        self.shard_manager = manager;
        self
    }

    /// Attach an audit log; topology events will be written to it.
    pub fn with_audit(mut self, audit: Arc<FileAuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Attach a [`RaftHandle`] so shard-lease-grant writes are gated on
    /// current control-plane leadership (v0.45.2, M7-S2).
    pub fn with_raft(mut self, raft: RaftHandle) -> Self {
        self.raft = Some(raft);
        self
    }

    /// Attach a shared [`ShardPersistentStore`] so a newly-elected leader
    /// (on any real process in the control group) picks up the last-known
    /// lease state instead of starting from an empty map (v0.45.2 M7-S4/S5).
    pub fn with_shard_store(mut self, store: Arc<ShardPersistentStore>) -> Self {
        self.shard_store = Some(store);
        self
    }

    /// Attach a [`FrontierAggregator`] so `ReportShardFrontier` messages are
    /// ingested and (when this node is leader) published (v0.45.2 M7-S4).
    pub fn with_frontier(mut self, frontier: Arc<FrontierAggregator>) -> Self {
        self.frontier = Some(frontier);
        self
    }

    /// Attach durable worker-topology persistence (v0.46 drain durability).
    pub fn with_topology_store(mut self, store: Arc<TopologyPersistentStore>) -> Self {
        self.topology_store = Some(store);
        self
    }

    /// Attach durable migration-record persistence (v0.46).
    pub fn with_migration_store(mut self, store: Arc<MigrationPersistentStore>) -> Self {
        self.migration_store = Some(store);
        self
    }

    /// Supply an already-open donor shard for replaying durably authorized cleanup.
    pub fn with_migration_recovery_donor(mut self, donor: MigrationShard) -> Self {
        self.migration_recovery_donors.insert(donor.shard_id, donor);
        self
    }

    /// Attach an [`InternalTlsConfig`] for internal mTLS mutual authentication.
    /// Enabled mTLS also requires the durable security catalog before startup.
    pub fn with_internal_tls(mut self, config: InternalTlsConfig) -> Self {
        self.internal_tls = Some(config);
        self
    }

    /// Configure a separate mTLS identity for the management listener.
    pub fn with_management_tls(mut self, config: InternalTlsConfig) -> Self {
        self.management_tls = Some(config);
        self
    }

    /// Allow plaintext control traffic only when the listener binds to loopback.
    ///
    /// This is intended for local development and tests. Production callers
    /// must configure internal mTLS instead.
    pub fn with_insecure_dev_mode(mut self) -> Self {
        self.insecure_dev_mode = true;
        self
    }

    /// Attach the catalog secret store used by worker token requests.
    pub fn with_secret_store(mut self, secret_store: Arc<SecretStore>) -> Self {
        self.secret_store = secret_store;
        self
    }

    /// Attach the versioned management API on its own listener.
    pub fn with_management(
        mut self,
        bind_addr: impl Into<String>,
        operation_store: Arc<dyn object_store::ObjectStore>,
        config: rockstream_types::config::NodeConfig,
    ) -> Self {
        self.management = Some((bind_addr.into(), operation_store, config));
        self
    }

    /// Attach the durable principals, policy revisions, and audit catalog used by management and internal mTLS.
    pub fn with_security_catalog(mut self, catalog: Arc<dyn CatalogStore>) -> Self {
        self.security_catalog = Some(catalog);
        self
    }

    /// Trust the deployment-managed RS256 key used by gateway actor assertions.
    pub fn with_actor_assertion_verifier(
        mut self,
        verifier: crate::delegation::ActorAssertionVerifier,
    ) -> Self {
        self.actor_assertion_verifier = Some(Arc::new(verifier));
        self
    }

    /// Attach the authoritative shard object store used by management backups.
    pub fn with_backup_source_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        shared_store_id: [u8; 32],
    ) -> Self {
        self.backup_source = Some((store, Some(shared_store_id), None));
        self
    }

    /// Attach a local shard store for the embedded `--role all` worker only.
    pub fn with_single_worker_local_backup_source_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        worker_id: WorkerId,
    ) -> Self {
        self.backup_source = Some((store, None, Some(worker_id)));
        self
    }

    /// Start the service on `bind_addr`.
    ///
    /// Returns a [`ControlServiceHandle`] which can be used to query the
    /// bound address and send a shutdown signal.
    pub async fn start(self, bind_addr: &str) -> io::Result<ControlServiceHandle> {
        let reloader = match self.internal_tls.as_ref() {
            Some(tls_cfg) if tls_cfg.is_enabled() => {
                if !tls_cfg.client_auth_required {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "RS-2405: control service requires client certificate authentication",
                    ));
                }
                Some(Arc::new(
                    crate::tls::TlsCertificateReloader::new(tls_cfg.clone()).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("RS-2405: failed to initialize internal mTLS: {error}"),
                        )
                    })?,
                ))
            }
            tls_cfg => {
                let tls_material_configured = tls_cfg.is_some_and(|cfg| {
                    cfg.cert_path.is_some() || cfg.key_path.is_some() || cfg.ca_cert_path.is_some()
                });
                if tls_material_configured {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "RS-2405: internal mTLS requires a certificate, private key, and trust roots",
                    ));
                }
                let addr = bind_addr.parse::<SocketAddr>().ok();
                if !self.insecure_dev_mode || !addr.is_some_and(|addr| addr.ip().is_loopback()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "RS-2405: control service requires internal mTLS; plaintext development mode must be explicitly enabled on a loopback address",
                    ));
                }
                tracing::warn!(
                    bind_addr,
                    "control service is using explicitly enabled loopback-only plaintext development transport"
                );
                None
            }
        };
        if reloader.is_some() && self.security_catalog.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RS-2405: internal mTLS requires the durable security audit catalog",
            ));
        }
        let management_reloader = match self.management_tls.as_ref() {
            Some(config) => {
                if !config.client_auth_required {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "RS-2405: management API requires client certificate authentication",
                    ));
                }
                Some(Arc::new(
                    crate::tls::TlsCertificateReloader::new(config.clone()).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("RS-2405: failed to initialize management mTLS: {error}"),
                        )
                    })?,
                ))
            }
            None => reloader.clone(),
        };

        if self.raft.is_none() && self.shard_manager.is_empty() {
            if let Some(store) = &self.shard_store {
                self.shard_manager.restore(store.load().await);
            }
        }
        if let Some(store) = &self.topology_store {
            if let Ok(workers) = store.load_all().await {
                self.catalog.restore_workers(workers);
            }
        }
        let migration_recovery = if let Some(store) = &self.migration_store {
            let outcomes = store
                .recover_on_start(
                    &self.shard_manager,
                    &self.migration_recovery_donors,
                    self.audit.as_deref(),
                )
                .await
                .map_err(|error| io::Error::other(format!("migration recovery: {error}")))?;
            for outcome in &outcomes {
                match outcome.status {
                    MigrationStartupStatus::Blocked(reason) => {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_5033,
                            migration_id = %outcome.recovery.record.migration_id,
                            phase = %outcome.recovery.record.state,
                            ?reason,
                            "migration recovery blocked; active record retained"
                        );
                    }
                    status => {
                        tracing::info!(
                            migration_id = %outcome.recovery.record.migration_id,
                            ?status,
                            "migration recovered during control startup"
                        );
                    }
                }
            }
            outcomes
        } else {
            Vec::new()
        };
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(addr = %addr, "control service listening");

        let operation_store = self
            .management
            .as_ref()
            .map(|(_, store, _)| ManagementOperationStore::new(store.clone()));
        let migration_waiters = Arc::new(AsyncMutex::new(HashMap::new()));
        let backup_waiters = Arc::new(AsyncMutex::new(HashMap::new()));
        let management = if let Some((management_addr, management_store, config)) = &self.management
        {
            let operations = operation_store
                .as_ref()
                .expect("management config has an operation store")
                .clone();
            let drain_runtime = ManagementDrainRuntime {
                catalog: self.catalog.clone(),
                shard_manager: self.shard_manager.clone(),
                data_plane: self.data_plane.clone(),
                audit: self.audit.clone(),
                shard_store: self.shard_store.clone(),
                topology_store: self.topology_store.clone(),
                drain_state: self.drain_state.clone(),
                worker_senders: self.worker_senders.clone(),
                migration_waiters: migration_waiters.clone(),
                operations: operations.clone(),
                started: Arc::new(AsyncMutex::new(HashSet::new())),
            };
            let migration_runtime = ManagementMigrationRuntime {
                catalog: self.catalog.clone(),
                shard_manager: self.shard_manager.clone(),
                data_plane: self.data_plane.clone(),
                shard_store: self.shard_store.clone(),
                worker_senders: self.worker_senders.clone(),
                waiters: migration_waiters.clone(),
                operations: operations.clone(),
                gc_pins: crate::checkpoint_store::GcPinStore::new(management_store.clone()),
                started: Arc::new(AsyncMutex::new(HashSet::new())),
            };
            let backup_source = self.backup_source.clone().or_else(|| {
                let store_id = configured_shared_shard_store_id()?;
                let store =
                    rockstream_storage::build_runtime_object_store(std::path::Path::new("."), "")
                        .ok()?;
                Some((store, Some(store_id), None))
            });
            let management = crate::management::ManagementService::new(
                self.catalog.clone(),
                self.shard_manager.clone(),
                operations,
                config.clone(),
            )
            .with_security_catalog(self.security_catalog.clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "RS-2405: management API requires the durable security catalog",
                )
            })?)
            .with_drain_runtime(drain_runtime)
            .with_migration_runtime(migration_runtime);
            let management = if let Some((source, shared_store_id, local_worker_id)) = backup_source
            {
                management.with_backup_runtime(ManagementBackupRuntime {
                    catalog: self.catalog.clone(),
                    shard_manager: self.shard_manager.clone(),
                    data_plane: self.data_plane.clone(),
                    worker_senders: self.worker_senders.clone(),
                    waiters: backup_waiters.clone(),
                    operations: operation_store
                        .as_ref()
                        .expect("management config has an operation store")
                        .clone(),
                    source: source.clone(),
                    source_store_id: shared_store_id,
                    local_worker_id,
                    manifests: crate::checkpoint_store::CheckpointManifestStore::new(
                        source.clone(),
                    ),
                    gc_pins: crate::checkpoint_store::GcPinStore::new(source),
                    started: Arc::new(AsyncMutex::new(HashSet::new())),
                    serial: Arc::new(AsyncMutex::new(())),
                })
            } else {
                management
            };
            let management_tls = management_reloader.clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "RS-2405: management API requires internal mTLS",
                )
            })?;
            Some(management.start(management_addr, management_tls).await?)
        } else {
            None
        };

        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_tx2 = shutdown_tx.clone();

        // Shared across every connection this service ever accepts: which
        // leader-epoch this node has already synced its `ShardManager`
        // state for (v0.45.2 M7-S4/S5). `None` until the first sync.
        let synced_epoch: Arc<AsyncMutex<Option<u64>>> = Arc::new(AsyncMutex::new(None));
        let ctx = ConnectionContext {
            catalog: self.catalog.clone(),
            shard_manager: self.shard_manager.clone(),
            audit: self.audit.clone(),
            raft: self.raft.clone(),
            shard_store: self.shard_store.clone(),
            synced_epoch,
            frontier: self.frontier.clone(),
            topology_store: self.topology_store.clone(),
            operation_store,
            drain_state: self.drain_state.clone(),
            secret_store: self.secret_store.clone(),
            security_catalog: self.security_catalog.clone(),
            actor_assertion_verifier: self.actor_assertion_verifier.clone(),
            worker_senders: self.worker_senders.clone(),
            migration_waiters,
            backup_waiters,
            data_plane: self.data_plane.clone(),
        };

        let reloader_for_handle = reloader.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx2.subscribe();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer)) => {
                                tracing::debug!(%peer, "control: new connection");
                                let conn_ctx = ctx.clone();
                                let mut sd = shutdown_tx2.subscribe();
                                let acceptor = reloader.as_ref().map(|r| r.current_acceptor());
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = accept_and_handle(stream, peer, conn_ctx, acceptor) => {}
                                        _ = sd.recv() => {}
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "control: accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("control service shutting down");
                        if let Some(store) = &ctx.shard_store {
                            let snapshot = ctx.shard_manager.snapshot();
                            store.save(&snapshot).await;
                        }
                        if let Some(audit) = &ctx.audit {
                            let event = AuditEvent::now("control", "server.stopping", "control-service");
                            let _ = audit.append(&event);
                        }
                        if let Some(raft) = &ctx.raft {
                            if raft.is_leader() {
                                raft.step_down();
                            }
                        }
                        break;
                    }
                }
            }
            if let Some(audit) = &ctx.audit {
                let event = AuditEvent::now("control", "server.stopped", "control-service");
                let _ = audit.append(&event);
            }
        });

        let row_payloads_received = self.data_plane.lock().await.row_payloads_received.clone();
        Ok(ControlServiceHandle {
            addr,
            shutdown_tx,
            management,
            migration_recovery,
            reloader: reloader_for_handle,
            management_reloader,
            row_payloads_received,
        })
    }

    /// Collect live operator statistics for a pipeline.
    pub fn collect_operator_stats(
        &self,
        operator_ids: &[rockstream_types::ids::OperatorId],
    ) -> Vec<rockstream_types::explain::IdentifiedOperatorStats> {
        rockstream_types::metrics::operator_runtime_stats_for_ids(operator_ids)
    }
}

/// Per-connection shared state for [`handle_connection`].
///
/// Bundled into one struct (rather than passed as individual parameters) to
/// keep `handle_connection`'s argument count within `clippy::too_many_arguments`;
/// every field here is `Clone`-cheap (an `Arc`/handle wrapper), so cloning the
/// whole context per accepted connection is equivalent to cloning each field
/// individually.
#[derive(Clone)]
struct ConnectionContext {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    audit: Option<Arc<FileAuditLog>>,
    raft: Option<RaftHandle>,
    shard_store: Option<Arc<ShardPersistentStore>>,
    synced_epoch: Arc<AsyncMutex<Option<u64>>>,
    frontier: Option<Arc<FrontierAggregator>>,
    topology_store: Option<Arc<TopologyPersistentStore>>,
    operation_store: Option<ManagementOperationStore>,
    drain_state: Arc<AsyncMutex<DrainState>>,
    secret_store: Arc<SecretStore>,
    security_catalog: Option<Arc<dyn CatalogStore>>,
    actor_assertion_verifier: Option<Arc<crate::delegation::ActorAssertionVerifier>>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    migration_waiters: Arc<AsyncMutex<HashMap<String, oneshot::Sender<WorkerMessage>>>>,
    backup_waiters: Arc<AsyncMutex<HashMap<String, oneshot::Sender<WorkerMessage>>>>,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
}

#[derive(Clone)]
pub(crate) struct ManagementDrainRuntime {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    audit: Option<Arc<FileAuditLog>>,
    shard_store: Option<Arc<ShardPersistentStore>>,
    topology_store: Option<Arc<TopologyPersistentStore>>,
    drain_state: Arc<AsyncMutex<DrainState>>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    migration_waiters: ManagementAckWaiters,
    operations: ManagementOperationStore,
    started: Arc<AsyncMutex<HashSet<String>>>,
}

impl ManagementDrainRuntime {
    pub(crate) async fn schedule(&self, worker_id: WorkerId, operation_id: String) {
        if !self.started.lock().await.insert(operation_id.clone()) {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.execute(worker_id, &operation_id).await;
            runtime.started.lock().await.remove(&operation_id);
        });
    }

    async fn execute(&self, worker_id: WorkerId, operation_id: &str) {
        let previous = match self.operations.get(operation_id).await {
            Ok(Some(record)) => record,
            Ok(None) | Err(_) => return,
        };
        if !matches!(
            previous.status(),
            OperationStatus::Pending | OperationStatus::Waiting
        ) {
            return;
        }
        let expected_phase = previous.phase().map(str::to_owned);
        let start_phase = expected_phase
            .clone()
            .unwrap_or_else(|| "validating_worker_and_shard_ownership".to_owned());
        let transition = self
            .operations
            .transition_if(
                operation_id,
                previous.status(),
                expected_phase.as_deref(),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: previous.progress().or(Some(0)),
                    phase: Some(start_phase.clone()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await;
        if let Err(error) = transition {
            tracing::warn!(%operation_id, %error, "drain operation was cancelled or could not start");
            return;
        }

        let worker_sender = self.worker_senders.lock().await.get(&worker_id).cloned();
        let Some(worker_sender) = worker_sender.filter(|sender| !sender.is_closed()) else {
            self.wait(
                operation_id,
                "worker has no active control connection; retry after it reconnects",
            )
            .await;
            return;
        };
        if self
            .operations
            .transition_if(
                operation_id,
                OperationStatus::Running,
                Some(&start_phase),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(5),
                    phase: Some("worker_handoff_started".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            return;
        }
        match request_worker_drain(
            &self.catalog,
            &self.shard_manager,
            self.audit.as_ref(),
            self.topology_store.as_ref(),
            &self.drain_state,
            worker_id,
            Some(operation_id.to_owned()),
        )
        .await
        {
            Ok((state, queue_fill, queue_capacity, request)) => {
                if self
                    .drain_state
                    .lock()
                    .await
                    .acked_workers
                    .contains(&worker_id)
                {
                    process_drain_queue(
                        &self.catalog,
                        &self.shard_manager,
                        &self.data_plane,
                        self.audit.as_ref(),
                        self.shard_store.as_ref(),
                        self.topology_store.as_ref(),
                        &self.drain_state,
                        &self.worker_senders,
                        worker_id,
                        &self.migration_waiters,
                        Some(&self.operations),
                    )
                    .await;
                    return;
                }
                send_message(&worker_sender, &ControlMessage::BeginDrain(request)).await;
                send_message(
                    &worker_sender,
                    &ControlMessage::DrainStatus {
                        worker_id,
                        state,
                        queue_fill,
                        queue_capacity,
                    },
                )
                .await;
                if let Err(error) = self
                    .operations
                    .transition(
                        operation_id,
                        OperationUpdate {
                            status: OperationStatus::Running,
                            updated_at_ms: chrono::Utc::now().timestamp_millis(),
                            progress: Some(10),
                            phase: Some("waiting_for_worker_flush_ack".to_owned()),
                            error_code: None,
                            next_steps: vec![
                                "Query the operation again after the worker flushes its shard data.".to_owned(),
                            ],
                        },
                    )
                    .await
                {
                    tracing::error!(code = %rockstream_types::error_code::RS_0001, %operation_id, %error, "drain was sent but its progress record could not be persisted");
                }
            }
            Err(error) => {
                self.fail(operation_id, error.code, &error.message, &error.next_steps)
                    .await;
            }
        }
    }

    async fn fail(&self, operation_id: &str, code: &str, message: &str, next_steps: &str) {
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Failed,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: None,
                    phase: Some("rejected_before_worker_handoff".to_owned()),
                    error_code: Some(code.to_owned()),
                    next_steps: vec![message.to_owned(), next_steps.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %code, %operation_id, %error, "failed drain outcome could not be persisted");
        }
    }

    async fn wait(&self, operation_id: &str, message: &str) {
        let phase = self
            .operations
            .get(operation_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.phase().map(str::to_owned))
            .unwrap_or_else(|| "waiting_for_worker_connection".to_owned());
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Waiting,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: None,
                    phase: Some(phase),
                    error_code: Some("RS-3610".to_owned()),
                    next_steps: vec![message.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %rockstream_types::error_code::RS_3610, %operation_id, %error, "drain wait state could not be persisted");
        }
    }
}

#[derive(Clone)]
pub(crate) struct ManagementMigrationRuntime {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    shard_store: Option<Arc<ShardPersistentStore>>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    pub(crate) waiters: ManagementAckWaiters,
    operations: ManagementOperationStore,
    gc_pins: crate::checkpoint_store::GcPinStore,
    started: Arc<AsyncMutex<HashSet<String>>>,
}

impl ManagementMigrationRuntime {
    pub(crate) async fn schedule(&self, shard_id: ShardId, target: WorkerId, operation_id: String) {
        if !self.started.lock().await.insert(operation_id.clone()) {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.execute(shard_id, target, &operation_id).await;
            runtime.started.lock().await.remove(&operation_id);
        });
    }

    async fn acquire_migration_pin(&self, operation_id: &str) {
        let pin = crate::checkpoint_store::GcPin {
            pin_id: format!("migration-{operation_id}"),
            kind: crate::checkpoint_store::GcPinKind::Migration,
            checkpoint_id: None,
            min_epoch: None,
            shard_snapshots: Vec::new(),
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        if let Err(error) = self.gc_pins.acquire_pin(&pin).await {
            tracing::warn!(%operation_id, %error, "failed to persist migration GC pin");
        }
    }

    async fn release_migration_pin(&self, operation_id: &str) {
        let pin_id = format!("migration-{operation_id}");
        if let Err(error) = self.gc_pins.release_pin(&pin_id).await {
            tracing::warn!(%operation_id, %error, "failed to release migration GC pin");
        }
    }

    async fn execute(&self, shard_id: ShardId, target: WorkerId, operation_id: &str) {
        let previous = match self.operations.get(operation_id).await {
            Ok(Some(record)) => record,
            Ok(None) | Err(_) => return,
        };
        if !matches!(
            previous.status(),
            OperationStatus::Pending | OperationStatus::Waiting
        ) {
            return;
        }
        let expected_phase = previous.phase().map(str::to_owned);
        let start_phase = expected_phase
            .clone()
            .unwrap_or_else(|| "validating_lease_and_workers".to_owned());
        if self
            .operations
            .transition_if(
                operation_id,
                previous.status(),
                expected_phase.as_deref(),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: previous.progress().or(Some(0)),
                    phase: Some(start_phase.clone()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            return;
        }
        if let Some(lease) = self.shard_manager.get(shard_id) {
            if lease.worker_id == target
                && matches!(
                    previous.phase(),
                    Some("lease_transfer_started" | "recipient_opening")
                )
            {
                if let Some(sender) = self
                    .worker_senders
                    .lock()
                    .await
                    .get(&target)
                    .filter(|sender| !sender.is_closed())
                    .cloned()
                {
                    if self
                        .send_and_wait(
                            &sender,
                            ControlMessage::ShardAssigned {
                                lease: lease.clone(),
                                operation_id: Some(operation_id.to_owned()),
                            },
                            operation_id,
                            "recipient",
                            target,
                            &lease,
                        )
                        .await
                        .is_ok()
                    {
                        if let Err(error) =
                            redeploy_shard_workloads(&self.data_plane, &self.worker_senders, &lease)
                                .await
                        {
                            self.wait(operation_id, &error).await;
                            return;
                        }
                        self.release_migration_pin(operation_id).await;
                        let _ = self
                            .operations
                            .transition(
                                operation_id,
                                OperationUpdate {
                                    status: OperationStatus::Succeeded,
                                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                                    progress: Some(100),
                                    phase: Some("completed".to_owned()),
                                    error_code: None,
                                    next_steps: Vec::new(),
                                },
                            )
                            .await;
                    } else {
                        self.wait(
                            operation_id,
                            "target worker must reconnect to finish opening the transferred shard",
                        )
                        .await;
                    }
                } else {
                    self.wait(
                        operation_id,
                        "target worker must reconnect to finish opening the transferred shard",
                    )
                    .await;
                }
                return;
            }
            if lease.worker_id != target && previous.phase() == Some("donor_flushed_and_closed") {
                if let Some(sender) = self
                    .worker_senders
                    .lock()
                    .await
                    .get(&lease.worker_id)
                    .filter(|sender| !sender.is_closed())
                    .cloned()
                {
                    if self
                        .send_and_wait(
                            &sender,
                            ControlMessage::ShardAssigned {
                                lease: lease.clone(),
                                operation_id: Some(operation_id.to_owned()),
                            },
                            operation_id,
                            "recipient",
                            lease.worker_id,
                            &lease,
                        )
                        .await
                        .is_err()
                    {
                        self.wait(
                            operation_id,
                            "current shard owner must reconnect before migration can resume",
                        )
                        .await;
                        return;
                    }
                    let _ =
                        redeploy_shard_workloads(&self.data_plane, &self.worker_senders, &lease)
                            .await;
                } else {
                    self.wait(
                        operation_id,
                        "current shard owner must reconnect before migration can resume",
                    )
                    .await;
                    return;
                }
            }
        }
        if self
            .operations
            .transition_if(
                operation_id,
                OperationStatus::Running,
                Some(&start_phase),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(0),
                    phase: Some("validating_lease_and_workers".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            return;
        }
        let Some(lease) = self.shard_manager.get(shard_id) else {
            self.fail(operation_id, "RS-3604", "shard has no active lease")
                .await;
            return;
        };
        let donor = lease.worker_id;
        let Some(donor_info) = self.catalog.get(donor) else {
            self.fail(
                operation_id,
                "RS-3610",
                "current shard owner is unavailable",
            )
            .await;
            return;
        };
        if !donor_info.healthy || !donor_info.lifecycle.is_active() {
            self.fail(
                operation_id,
                "RS-3610",
                "current shard owner is not active and healthy",
            )
            .await;
            return;
        }
        let Some(target_info) = self
            .catalog
            .get(target)
            .filter(|worker| worker.healthy && worker.lifecycle.is_active())
        else {
            self.fail(
                operation_id,
                "RS-3604",
                "target worker is not active and healthy",
            )
            .await;
            return;
        };
        if donor == target
            || donor_info.capabilities.shared_shard_store_id.is_none()
            || donor_info.capabilities.shared_shard_store_id
                != target_info.capabilities.shared_shard_store_id
        {
            self.fail(
                operation_id,
                "RS-3604",
                "source and target must be different workers sharing the same shard store",
            )
            .await;
            return;
        }
        let senders = self.worker_senders.lock().await.clone();
        let Some(donor_sender) = senders.get(&donor).filter(|sender| !sender.is_closed()) else {
            self.wait(
                operation_id,
                "current shard owner must reconnect before migration can resume",
            )
            .await;
            return;
        };
        let Some(target_sender) = senders.get(&target).filter(|sender| !sender.is_closed()) else {
            self.wait(
                operation_id,
                "target worker must reconnect before migration can resume",
            )
            .await;
            return;
        };
        if self
            .operations
            .transition_if(
                operation_id,
                OperationStatus::Running,
                Some("validating_lease_and_workers"),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(10),
                    phase: Some("donor_handoff_started".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            return;
        }
        self.acquire_migration_pin(operation_id).await;
        begin_shard_handoff(&self.data_plane, shard_id).await;
        if let Err(error) = self
            .send_and_wait(
                donor_sender,
                ControlMessage::PrepareShardTransfer {
                    operation_id: operation_id.to_owned(),
                    lease: lease.clone(),
                },
                operation_id,
                "donor",
                donor,
                &lease,
            )
            .await
        {
            if self.operation_cancelled(operation_id).await {
                self.reopen_donor(operation_id, &lease).await;
            } else if error.contains("connection closed")
                || error.contains("timed out")
                || error.contains("capacity")
            {
                self.wait(operation_id, &error).await;
            } else {
                self.fail(operation_id, "RS-3610", &error).await;
            }
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            return;
        }
        if self
            .operations
            .transition_if(
                operation_id,
                OperationStatus::Running,
                Some("donor_handoff_started"),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(50),
                    phase: Some("donor_flushed_and_closed".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            if self.operation_cancelled(operation_id).await {
                self.reopen_donor(operation_id, &lease).await;
            }
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            return;
        }
        if !self
            .shard_manager
            .get(shard_id)
            .is_some_and(|current| current.same_owner(&lease))
        {
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            self.fail(
                operation_id,
                "RS-3604",
                "shard lease changed during preparation",
            )
            .await;
            return;
        }
        if self
            .operations
            .transition_if(
                operation_id,
                OperationStatus::Running,
                Some("donor_flushed_and_closed"),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(60),
                    phase: Some("lease_transfer_started".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            self.reopen_donor(operation_id, &lease).await;
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            return;
        }
        let Some(new_lease) = self.shard_manager.transfer_if_owner(&lease, target) else {
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            self.fail(
                operation_id,
                "RS-3604",
                "shard ownership changed before transfer",
            )
            .await;
            return;
        };
        if let Some(store) = &self.shard_store {
            store.save(&self.shard_manager.snapshot()).await;
        }
        broadcast_message(
            &self.worker_senders,
            topology_changed_message(&self.catalog, &self.shard_manager),
        )
        .await;
        if self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(80),
                    phase: Some("recipient_opening".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            self.rollback(operation_id, &lease, &new_lease).await;
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            return;
        }
        if let Err(error) = self
            .send_and_wait(
                target_sender,
                ControlMessage::ShardAssigned {
                    lease: new_lease.clone(),
                    operation_id: Some(operation_id.to_owned()),
                },
                operation_id,
                "recipient",
                target,
                &new_lease,
            )
            .await
        {
            self.rollback(operation_id, &lease, &new_lease).await;
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            let message = format!("target failed to open shard; transfer rolled back: {error}");
            if error.contains("connection closed")
                || error.contains("timed out")
                || error.contains("capacity")
            {
                self.wait(operation_id, &message).await;
            } else {
                self.fail(operation_id, "RS-3610", &message).await;
            }
            return;
        }
        if let Err(error) =
            redeploy_shard_workloads(&self.data_plane, &self.worker_senders, &new_lease).await
        {
            self.rollback(operation_id, &lease, &new_lease).await;
            end_shard_handoff(&self.data_plane, shard_id).await;
            self.release_migration_pin(operation_id).await;
            let message =
                format!("target failed to activate active workload; transfer rolled back: {error}");
            self.wait(operation_id, &message).await;
            return;
        }
        end_shard_handoff(&self.data_plane, shard_id).await;
        self.release_migration_pin(operation_id).await;
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Succeeded,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: Some(100),
                    phase: Some("completed".to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
        {
            tracing::error!(code = %rockstream_types::error_code::RS_0001, %operation_id, %error, "migrated shard but could not persist success");
        }
    }

    async fn send_and_wait(
        &self,
        sender: &mpsc::Sender<ControlMessage>,
        message: ControlMessage,
        operation_id: &str,
        stage: &str,
        worker_id: WorkerId,
        lease: &rockstream_types::lease::ShardLease,
    ) -> Result<(), String> {
        let key = format!("{operation_id}:{stage}");
        let (tx, rx) = oneshot::channel();
        try_insert_management_ack_waiter(&mut *self.waiters.lock().await, key.clone(), tx)?;
        send_message(sender, &message).await;
        let result = match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(WorkerMessage::ShardTransferAck {
                operation_id: actual,
                stage: actual_stage,
                worker_id: actual_worker,
                shard_id,
                lease_token,
                success,
                error,
            })) if actual == operation_id
                && actual_stage == stage
                && actual_worker == worker_id
                && shard_id == lease.shard_id
                && lease_token == lease.lease_token =>
            {
                if success {
                    Ok(())
                } else {
                    Err(error.unwrap_or_else(|| format!("{stage} rejected shard transfer")))
                }
            }
            Ok(Ok(_)) => Err(format!(
                "RS-3610: unexpected {stage} shard transfer acknowledgement"
            )),
            Ok(Err(_)) => Err(format!(
                "RS-3610: {stage} connection closed before acknowledgement"
            )),
            Err(_) => Err(format!("RS-3610: {stage} acknowledgement timed out")),
        };
        self.waiters.lock().await.remove(&key);
        result
    }

    async fn rollback(
        &self,
        operation_id: &str,
        original: &rockstream_types::lease::ShardLease,
        transferred: &rockstream_types::lease::ShardLease,
    ) {
        if self.shard_manager.get(original.shard_id) != Some(transferred.clone()) {
            return;
        }
        let Some(lease) = self
            .shard_manager
            .transfer_if_owner(transferred, original.worker_id)
        else {
            return;
        };
        if let Some(store) = &self.shard_store {
            store.save(&self.shard_manager.snapshot()).await;
        }
        broadcast_message(
            &self.worker_senders,
            topology_changed_message(&self.catalog, &self.shard_manager),
        )
        .await;
        let sender = self
            .worker_senders
            .lock()
            .await
            .get(&original.worker_id)
            .filter(|sender| !sender.is_closed())
            .cloned();
        if let Some(sender) = sender {
            let _ = self
                .send_and_wait(
                    &sender,
                    ControlMessage::ShardAssigned {
                        lease: lease.clone(),
                        operation_id: Some(operation_id.to_owned()),
                    },
                    operation_id,
                    "recipient",
                    original.worker_id,
                    &lease,
                )
                .await;
            let _ = redeploy_shard_workloads(&self.data_plane, &self.worker_senders, &lease).await;
        }
    }

    async fn operation_cancelled(&self, operation_id: &str) -> bool {
        self.operations
            .get(operation_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|operation| operation.status() == OperationStatus::Cancelled)
    }

    async fn reopen_donor(&self, operation_id: &str, lease: &rockstream_types::lease::ShardLease) {
        let sender = self
            .worker_senders
            .lock()
            .await
            .get(&lease.worker_id)
            .filter(|sender| !sender.is_closed())
            .cloned();
        if let Some(sender) = sender {
            let _ = self
                .send_and_wait(
                    &sender,
                    ControlMessage::ShardAssigned {
                        lease: lease.clone(),
                        operation_id: Some(operation_id.to_owned()),
                    },
                    operation_id,
                    "recipient",
                    lease.worker_id,
                    lease,
                )
                .await;
            let _ = redeploy_shard_workloads(&self.data_plane, &self.worker_senders, lease).await;
        }
    }

    async fn fail(&self, operation_id: &str, code: &str, message: &str) {
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Failed,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: None,
                    phase: Some("failed".to_owned()),
                    error_code: Some(code.to_owned()),
                    next_steps: vec![message.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %code, %operation_id, %error, "migration failure could not be persisted");
        }
    }

    async fn wait(&self, operation_id: &str, message: &str) {
        let phase = self
            .operations
            .get(operation_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.phase().map(str::to_owned))
            .unwrap_or_else(|| "waiting_for_worker_connection".to_owned());
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Waiting,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    progress: None,
                    phase: Some(phase),
                    error_code: Some("RS-3610".to_owned()),
                    next_steps: vec![message.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %rockstream_types::error_code::RS_3610, %operation_id, %error, "migration wait state could not be persisted");
        }
    }
}

#[derive(Clone)]
pub(crate) struct ManagementBackupRuntime {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    pub(crate) waiters: ManagementAckWaiters,
    operations: ManagementOperationStore,
    source: Arc<dyn object_store::ObjectStore>,
    source_store_id: Option<[u8; 32]>,
    local_worker_id: Option<WorkerId>,
    manifests: crate::checkpoint_store::CheckpointManifestStore,
    gc_pins: crate::checkpoint_store::GcPinStore,
    started: Arc<AsyncMutex<HashSet<String>>>,
    serial: Arc<AsyncMutex<()>>,
}

impl ManagementBackupRuntime {
    pub(crate) async fn schedule(&self, operation_id: String) {
        if !self.started.lock().await.insert(operation_id.clone()) {
            return;
        }
        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.execute(&operation_id).await;
            runtime.started.lock().await.remove(&operation_id);
        });
    }

    async fn execute(&self, operation_id: &str) {
        let _serial = self.serial.lock().await;
        let Some(record) = self.operations.get(operation_id).await.ok().flatten() else {
            return;
        };
        if !matches!(
            record.status(),
            OperationStatus::Pending | OperationStatus::Waiting
        ) {
            return;
        }
        let Some(destination) = record
            .request()
            .and_then(|request| request.get("destination"))
            .and_then(serde_json::Value::as_str)
        else {
            self.fail(operation_id, "RS-0002", "backup destination is missing")
                .await;
            return;
        };
        let destination_store = match rockstream_storage::build_migration_object_store(destination)
        {
            Ok(store) => store,
            Err(error) => {
                self.fail(operation_id, "RS-0002", &error).await;
                return;
            }
        };

        let checkpoint_id = match record.phase().and_then(parse_backup_checkpoint_id) {
            Some(checkpoint_id) => checkpoint_id,
            None => match self.manifests.load_latest_manifest().await {
                Ok(manifest) => match manifest
                    .map(|manifest| manifest.checkpoint_id.checked_next())
                    .unwrap_or(Some(CheckpointId(1)))
                {
                    Some(checkpoint_id) => checkpoint_id,
                    None => {
                        self.fail(operation_id, "RS-3604", "checkpoint id space is exhausted")
                            .await;
                        return;
                    }
                },
                Err(error) => {
                    self.fail(operation_id, "RS-3022", &error).await;
                    return;
                }
            },
        };
        let checkpoint_phase = format!("backup_checkpoint:{}", checkpoint_id.0);
        if self
            .operations
            .transition_if(
                operation_id,
                record.status(),
                record.phase(),
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: now_ms() as i64,
                    progress: Some(5),
                    phase: Some(checkpoint_phase.clone()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            return;
        }

        let checkpoint = match self.manifests.load_manifest_exact(checkpoint_id).await {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => match self.create_manifest(operation_id, checkpoint_id).await {
                Ok(checkpoint) => checkpoint,
                Err(BackupFailure::Waiting(message)) => {
                    self.wait(operation_id, &message).await;
                    return;
                }
                Err(BackupFailure::Failed(code, message)) => {
                    self.fail(operation_id, code, &message).await;
                    return;
                }
            },
            Err(error) => {
                self.fail(operation_id, "RS-3022", &error).await;
                return;
            }
        };

        let pin_id = format!("backup-{operation_id}");
        let shard_snapshots: Vec<(ShardId, String)> = checkpoint
            .shards
            .iter()
            .filter_map(|(shard_id, entry)| {
                entry
                    .snapshot_id
                    .as_ref()
                    .map(|snap| (*shard_id, snap.clone()))
            })
            .collect();
        let backup_pin = crate::checkpoint_store::GcPin {
            pin_id: pin_id.clone(),
            kind: crate::checkpoint_store::GcPinKind::Backup,
            checkpoint_id: Some(checkpoint_id),
            min_epoch: None,
            shard_snapshots,
            created_at_ms: now_ms() as i64,
        };
        if let Err(error) = self.gc_pins.acquire_pin(&backup_pin).await {
            self.wait(operation_id, &format!("acquire backup GC pin: {error}"))
                .await;
            return;
        }

        let export_phase = format!("backup_exporting:{}", checkpoint_id.0);
        if self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Running,
                    updated_at_ms: now_ms() as i64,
                    progress: Some(70),
                    phase: Some(export_phase),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .is_err()
        {
            let _ = self.gc_pins.release_pin(&pin_id).await;
            return;
        }
        let generation = format!("management-{operation_id}");
        let exporter = crate::checkpoint_export::CheckpointExportService::new();
        match exporter
            .validate_generation(destination_store.clone(), &generation)
            .await
        {
            Ok(_) => {}
            Err(crate::checkpoint_export::CheckpointExportError::Integrity(message))
                if message.contains("terminal commit marker is missing") =>
            {
                if let Err(error) = exporter
                    .export_prefix(
                        self.source.clone(),
                        destination_store.clone(),
                        checkpoint,
                        generation.clone(),
                        &object_store::path::Path::from(""),
                    )
                    .await
                {
                    let _ = self.gc_pins.release_pin(&pin_id).await;
                    self.handle_export_error(operation_id, error).await;
                    return;
                }
            }
            Err(error) => {
                let _ = self.gc_pins.release_pin(&pin_id).await;
                self.handle_export_error(operation_id, error).await;
                return;
            }
        }
        let validation = exporter
            .validate_generation(destination_store, &generation)
            .await;
        let _ = self.gc_pins.release_pin(&pin_id).await;
        match validation {
            Ok(outcome)
                if outcome.checkpoint_id == checkpoint_id.0
                    && outcome.generation == generation
                    && outcome.status == "SUCCESS" =>
            {
                if let Err(error) = self
                    .operations
                    .transition(
                        operation_id,
                        OperationUpdate {
                            status: OperationStatus::Succeeded,
                            updated_at_ms: now_ms() as i64,
                            progress: Some(100),
                            phase: Some("completed".to_owned()),
                            error_code: None,
                            next_steps: Vec::new(),
                        },
                    )
                    .await
                {
                    tracing::error!(code = %rockstream_types::error_code::RS_0001, %operation_id, %error, "validated backup could not persist success");
                }
            }
            Ok(_) => {
                self.fail(
                    operation_id,
                    "RS-3614",
                    "export commit marker did not match the accepted backup",
                )
                .await
            }
            Err(error) => self.handle_export_error(operation_id, error).await,
        }
    }

    async fn create_manifest(
        &self,
        operation_id: &str,
        checkpoint_id: CheckpointId,
    ) -> Result<ClusterCheckpoint, BackupFailure> {
        // Snapshot any active workload relative shard paths without holding
        // `data_plane` across worker RPCs so foreground ingestion and reads
        // continue unblocked during online backup.
        let workload_shard_paths: HashMap<ShardId, String> = {
            let state = self.data_plane.lock().await;
            let mut paths = HashMap::new();
            for deployment in state.deployments.values() {
                let root_prefix =
                    format!("{}/", deployment.request.storage_root.trim_end_matches('/'));
                for (shard_id, descriptor) in &deployment.descriptors {
                    let identity = descriptor
                        .storage_identity
                        .strip_prefix("lfs:")
                        .unwrap_or(&descriptor.storage_identity);
                    if let Some(rel) = identity.strip_prefix(&root_prefix) {
                        if !rel.is_empty() {
                            paths.insert(*shard_id, rel.to_owned());
                        }
                    }
                }
            }
            paths
        };
        let leases = self.shard_manager.snapshot().leases;
        if let Some(local_worker_id) = self.local_worker_id {
            let workers = self.catalog.all_workers();
            if workers.len() != 1 || workers[0].worker_id != local_worker_id {
                return Err(BackupFailure::Failed(
                    "RS-3604",
                    "local backup requires exactly its embedded worker; use a shared object store for multiple workers".to_owned(),
                ));
            }
        }
        let senders = self.worker_senders.lock().await.clone();
        let mut checkpoint = ClusterCheckpoint::new(checkpoint_id);
        let mut expected_leases = leases.values().cloned().collect::<Vec<_>>();
        expected_leases.sort_by_key(|lease| lease.shard_id);
        for lease in &expected_leases {
            let worker = self.catalog.get(lease.worker_id).filter(|worker| {
                worker.healthy
                    && worker.lifecycle.is_active()
                    && if let Some(local_worker_id) = self.local_worker_id {
                        lease.worker_id == local_worker_id
                    } else {
                        worker.capabilities.shared_shard_store_id == self.source_store_id
                    }
            });
            if worker.is_none() {
                return Err(BackupFailure::Failed(
                    "RS-3604",
                    format!(
                        "shard {} owner is unavailable or uses a different shard store",
                        lease.shard_id.0
                    ),
                ));
            }
            let Some(sender) = senders
                .get(&lease.worker_id)
                .filter(|sender| !sender.is_closed())
            else {
                return Err(BackupFailure::Waiting(format!(
                    "worker {} must reconnect before backup can resume",
                    lease.worker_id.0
                )));
            };
            let request_id = format!("{operation_id}:{}:{}", checkpoint_id.0, lease.shard_id.0);
            let (tx, rx) = oneshot::channel();
            if let Err(error) = try_insert_management_ack_waiter(
                &mut *self.waiters.lock().await,
                request_id.clone(),
                tx,
            ) {
                return Err(BackupFailure::Waiting(error.to_owned()));
            }
            if sender
                .send(ControlMessage::CreateShardCheckpoint {
                    request_id: request_id.clone(),
                    checkpoint_id,
                    lease: lease.clone(),
                })
                .await
                .is_err()
            {
                self.waiters.lock().await.remove(&request_id);
                return Err(BackupFailure::Waiting(format!(
                    "worker {} disconnected before checkpoint request",
                    lease.worker_id.0
                )));
            }
            let response = tokio::time::timeout(Duration::from_secs(30), rx).await;
            self.waiters.lock().await.remove(&request_id);
            match response {
                Ok(Ok(WorkerMessage::ShardCheckpointAck {
                    request_id: actual_request,
                    checkpoint_id: actual_checkpoint,
                    worker_id,
                    shard_id,
                    lease_token,
                    shard_checkpoint_id: Some(shard_checkpoint_id),
                    snapshot_id: Some(snapshot_id),
                    error: None,
                })) if actual_request == request_id
                    && actual_checkpoint == checkpoint_id
                    && worker_id == lease.worker_id
                    && shard_id == lease.shard_id
                    && lease_token == lease.lease_token =>
                {
                    let mut per_shard = PerShardCheckpoint::new(checkpoint_id, shard_checkpoint_id)
                        .with_snapshot_id(snapshot_id);
                    if let Some(shard_path) = workload_shard_paths.get(&lease.shard_id) {
                        per_shard = per_shard.with_shard_path(shard_path.clone());
                    }
                    checkpoint.record_shard(lease.shard_id, per_shard);
                }
                Ok(Ok(WorkerMessage::ShardCheckpointAck {
                    error: Some(error), ..
                })) => {
                    return Err(BackupFailure::Failed("RS-3604", error));
                }
                Ok(Ok(_)) => {
                    return Err(BackupFailure::Failed(
                        "RS-2401",
                        format!(
                            "worker {} returned an invalid checkpoint acknowledgement",
                            lease.worker_id.0
                        ),
                    ));
                }
                Ok(Err(_)) => {
                    return Err(BackupFailure::Waiting(format!(
                        "worker {} disconnected before checkpoint acknowledgement",
                        lease.worker_id.0
                    )));
                }
                Err(_) => {
                    return Err(BackupFailure::Waiting(format!(
                        "worker {} checkpoint acknowledgement timed out",
                        lease.worker_id.0
                    )));
                }
            }
        }
        if self.shard_manager.snapshot().leases != leases {
            return Err(BackupFailure::Waiting(
                "shard ownership changed while checkpoints were being created".to_owned(),
            ));
        }
        if checkpoint.shards.len() != expected_leases.len() {
            return Err(BackupFailure::Failed(
                "RS-3604",
                "checkpoint manifest is incomplete".to_owned(),
            ));
        }
        if let Err(error) = self.manifests.save_manifest(&checkpoint, false, None).await {
            return Err(BackupFailure::Failed("RS-3022", error));
        }
        Ok(checkpoint)
    }

    async fn handle_export_error(
        &self,
        operation_id: &str,
        error: crate::checkpoint_export::CheckpointExportError,
    ) {
        match error {
            crate::checkpoint_export::CheckpointExportError::Integrity(message) => {
                self.fail(operation_id, "RS-3614", &message).await;
            }
            error => self.wait(operation_id, &error.to_string()).await,
        }
    }

    async fn fail(&self, operation_id: &str, code: &str, message: &str) {
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Failed,
                    updated_at_ms: now_ms() as i64,
                    progress: None,
                    phase: Some("failed".to_owned()),
                    error_code: Some(code.to_owned()),
                    next_steps: vec![message.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %code, %operation_id, %error, "backup failure could not be persisted");
        }
    }

    async fn wait(&self, operation_id: &str, message: &str) {
        let phase = self
            .operations
            .get(operation_id)
            .await
            .ok()
            .flatten()
            .and_then(|record| record.phase().map(str::to_owned))
            .unwrap_or_else(|| "waiting_for_worker_connection".to_owned());
        if let Err(error) = self
            .operations
            .transition(
                operation_id,
                OperationUpdate {
                    status: OperationStatus::Waiting,
                    updated_at_ms: now_ms() as i64,
                    progress: None,
                    phase: Some(phase),
                    error_code: Some("RS-3610".to_owned()),
                    next_steps: vec![message.to_owned()],
                },
            )
            .await
        {
            tracing::error!(code = %rockstream_types::error_code::RS_3610, %operation_id, %error, "backup wait state could not be persisted");
        }
    }
}

enum BackupFailure {
    Waiting(String),
    Failed(&'static str, String),
}

fn parse_backup_checkpoint_id(phase: &str) -> Option<CheckpointId> {
    phase
        .strip_prefix("backup_checkpoint:")
        .or_else(|| phase.strip_prefix("backup_exporting:"))
        .and_then(|id| id.parse().ok())
        .map(CheckpointId)
}

async fn persist_worker_if_needed(
    topology_store: Option<&Arc<TopologyPersistentStore>>,
    worker: &rockstream_types::topology::WorkerInfo,
) {
    if let Some(store) = topology_store {
        let _ = store.save_worker(worker).await;
    }
}

async fn delete_worker_if_needed(
    topology_store: Option<&Arc<TopologyPersistentStore>>,
    worker_id: WorkerId,
) {
    if let Some(store) = topology_store {
        let _ = store.delete_worker(worker_id).await;
    }
}

fn data_plane_failure(message: impl Into<String>) -> ControlMessage {
    ControlMessage::OperationFailed {
        code: rockstream_types::error_code::RS_0001.to_string(),
        message: message.into(),
        next_steps: "Retry after the workload and workers are ready.".to_string(),
    }
}

fn workload_recovery_failure() -> ControlMessage {
    workload_failure("workload requires recovery after failed execution")
}

fn workload_failure(message: impl Into<String>) -> ControlMessage {
    ControlMessage::OperationFailed {
        code: rockstream_types::error_code::RS_0001.to_string(),
        message: message.into(),
        next_steps: "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned(),
    }
}

#[allow(dead_code)]
fn stable_route(value: &str, shard_count: usize) -> usize {
    let hash = value
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    hash as usize % shard_count
}

#[derive(Clone, Copy)]
struct WorkloadDeploymentContext<'a> {
    secret_catalog: Option<&'a Arc<dyn CatalogStore>>,
    sender: &'a mpsc::Sender<ControlMessage>,
    catalog: &'a TopologyCatalog,
    shard_manager: &'a ShardManager,
    worker_senders: &'a Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: &'a Arc<AsyncMutex<DataPlaneState>>,
}
async fn deploy_workload(
    request: DeploymentRequest,
    requested_shard_count: Option<usize>,
    actor: Option<DelegatedActor>,
    context: WorkloadDeploymentContext<'_>,
) {
    let WorkloadDeploymentContext {
        secret_catalog,
        sender,
        catalog,
        shard_manager,
        worker_senders,
        data_plane,
    } = context;
    let secret_names = match secret_catalog {
        Some(catalog) => match secret_names_for_deployment(Some(catalog), &request.schemas).await {
            Ok(secret_names) => secret_names,
            Err(error) => {
                send_message(
                    sender,
                    &ControlMessage::OperationFailed {
                        code: RS_0003.to_string(),
                        message: format!("workload secret metadata is unavailable: {error:?}"),
                        next_steps: rockstream_types::error_code::next_steps(RS_0003).to_string(),
                    },
                )
                .await;
                return;
            }
        },
        None => Vec::new(),
    };
    let healthy = catalog.healthy_workers();
    let shard_count = requested_shard_count.unwrap_or(healthy.len()).max(1);
    let workers = PlacementAlgorithm::assign_shards(&healthy, shard_count);
    if workers.is_empty() {
        send_message(sender, &data_plane_failure("no healthy workers available")).await;
        return;
    }

    let senders = worker_senders.lock().await.clone();
    if workers.iter().any(|worker| !senders.contains_key(worker)) {
        send_message(
            sender,
            &data_plane_failure("not all healthy workers have an active control connection"),
        )
        .await;
        return;
    }

    let mut state = data_plane.lock().await;
    let admission_failure = if state
        .deployments
        .get(&request.workload_id)
        .is_some_and(|deployment| deployment.recovery_required)
    {
        Some(workload_recovery_failure())
    } else if state
        .source_waiters
        .values()
        .any(|waiter| waiter.workload_id == request.workload_id)
        || state.direct_epochs.contains_key(&request.workload_id)
        || state.direct_installing.contains_key(&request.workload_id)
        || state.direct_completing.contains_key(&request.workload_id)
    {
        Some(data_plane_failure(
            "another source delta is already in flight for this workload",
        ))
    } else {
        None
    };
    if let Some(failure) = admission_failure {
        drop(state);
        send_message(sender, &failure).await;
        return;
    }

    let mut descriptors = HashMap::new();
    let workload_id = request.workload_id;
    // Reuse previously allocated shard IDs if redeploying the exact same
    // workload with the same shard count; otherwise allocate fresh globally
    // unique, collision-free shard IDs from `ShardManager` (Issue #400 R5).
    let allocated_shard_ids: Vec<ShardId> = state
        .deployments
        .get(&workload_id)
        .filter(|existing| existing.descriptors.len() == workers.len())
        .map(|existing| {
            let mut ordered: Vec<_> = existing.descriptors.values().collect();
            ordered.sort_by_key(|d| (d.shard_index, d.shard.shard_id));
            ordered.into_iter().map(|d| d.shard.shard_id).collect()
        })
        .unwrap_or_else(|| shard_manager.allocate_shard_ids(workers.len()));

    for (index, (worker_id, shard_id)) in workers.into_iter().zip(allocated_shard_ids).enumerate() {
        let (lease, _) = shard_manager.force_acquire(shard_id, worker_id);
        let storage_identity = format!(
            "{}/workload-{}/shard-{}",
            request.storage_root, request.workload_id.0, shard_id.0
        );
        descriptors.insert(
            shard_id,
            DeploymentDescriptor::with_shard_index(
                request.clone(),
                lease,
                storage_identity,
                index as u32,
            ),
        );
    }

    state.topology_epoch = state.topology_epoch.saturating_add(1);
    state.direct_completed.remove(&workload_id);
    state.deployments.insert(
        request.workload_id,
        DeploymentState {
            request,
            actor,
            secret_names: secret_names.clone(),
            descriptors: descriptors.clone(),
            workers: HashMap::new(),
            ready_shards: HashSet::new(),
            ready_waiter: Some(sender.clone()),
            recovery_required: false,
        },
    );
    drop(state);
    broadcast_message(
        worker_senders,
        topology_changed_message(catalog, shard_manager),
    )
    .await;
    let assigned_workers: HashSet<_> = descriptors
        .values()
        .map(|descriptor| descriptor.shard.worker_id)
        .collect();
    for descriptor in descriptors.into_values() {
        if let Some(target) = senders.get(&descriptor.shard.worker_id) {
            send_message(
                target,
                &ControlMessage::ShardAssigned {
                    lease: descriptor.shard.clone(),
                    operation_id: None,
                },
            )
            .await;
            send_message(target, &ControlMessage::Deploy { descriptor }).await;
        }
    }
    for worker_id in assigned_workers {
        if let Some(target) = senders.get(&worker_id) {
            for secret_name in &secret_names {
                send_message(
                    target,
                    &ControlMessage::SecretRefreshRequired {
                        workload_id,
                        secret_name: secret_name.clone(),
                    },
                )
                .await;
            }
        }
    }
}

fn route_source_delta(
    deployment: &DeploymentState,
    request: &SourceDeltaRequest,
) -> Result<Vec<(WorkerId, RuntimeExchangeMessage)>, String> {
    let routing_column = deployment
        .request
        .routing_columns
        .get(&request.source)
        .copied()
        .or_else(|| {
            deployment
                .request
                .routing_columns
                .iter()
                .find(|(source, _)| source.eq_ignore_ascii_case(&request.source))
                .map(|(_, column)| *column)
        })
        .ok_or_else(|| {
            format!(
                "no routing column is registered for source {}; refusing arbitrary placement",
                request.source
            )
        })?;

    let mut descriptors: Vec<_> = deployment.descriptors.values().collect();
    descriptors.sort_by_key(|descriptor| descriptor.shard.shard_id);
    let shard_ids: Vec<_> = descriptors
        .iter()
        .map(|descriptor| descriptor.shard.shard_id)
        .collect();
    if shard_ids.is_empty() {
        return Err(format!("{}: workload has no active shard leases", RS_0001));
    }

    let mut rows_by_shard = BTreeMap::<ShardId, Vec<RuntimeRow>>::new();
    for row in &request.rows {
        let key = row
            .values_tsv
            .split('\t')
            .nth(routing_column)
            .ok_or_else(|| {
                format!(
                    "source {} row is missing routing column {}",
                    request.source, routing_column
                )
            })?;
        let bucket = fnv1a_64(key.as_bytes()) % DEFAULT_VIRTUAL_BUCKET_COUNT;
        let shard_id = rendezvous_hash(bucket, &shard_ids, 1)
            .ok_or_else(|| "workload has no active shard leases".to_string())?;
        rows_by_shard.entry(shard_id).or_default().push(row.clone());
    }

    Ok(rows_by_shard
        .into_iter()
        .map(|(shard_id, rows)| {
            let descriptor = deployment
                .descriptors
                .get(&shard_id)
                .expect("routed shard must have a deployment descriptor");
            (
                descriptor.shard.worker_id,
                RuntimeExchangeMessage {
                    join_bootstrap: None,
                    attempt_id: None,
                    version: request.version,
                    request_id: request.request_id.clone(),
                    workload_id: request.workload_id,
                    shard_id,
                    epoch: request.epoch,
                    operator_id: descriptor.sink_operator_id,
                    lease_token: descriptor.shard.lease_token,
                    source: request.source.clone(),
                    rows,
                },
            )
        })
        .collect())
}

async fn fail_source_request(
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    request_id: &str,
    workload_id: WorkloadId,
    message: impl Into<String>,
) {
    let waiter = {
        let mut state = data_plane.lock().await;
        let waiter = state.source_waiters.remove(request_id);
        if waiter.is_some() {
            if let Some(deployment) = state.deployments.get_mut(&workload_id) {
                deployment.recovery_required = true;
            }
        }
        waiter
    };
    if let Some(waiter) = waiter {
        send_message(&waiter.sender, &workload_failure(message)).await;
    }
}

async fn finish_owned_source(
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    anchor_worker: WorkerId,
    intent: rockstream_types::data_plane::OwnedEpochIntent,
    reply: tokio::sync::oneshot::Receiver<Result<(), String>>,
) {
    let sender = worker_senders.lock().await.get(&anchor_worker).cloned();
    let permit = match &sender {
        Some(sender) => sender.reserve().await.ok(),
        None => None,
    };
    let Some(permit) = permit else {
        fail_source_request(
            &data_plane,
            &intent.request_id,
            intent.workload_id,
            "owned epoch anchor disconnected before completion",
        )
        .await;
        return;
    };
    {
        let state = data_plane.lock().await;
        let valid = state
            .source_waiters
            .get(&intent.request_id)
            .is_some_and(|waiter| {
                waiter.finishing && waiter.owned_intent.as_ref() == Some(&intent)
            });
        if !valid {
            return;
        }
        permit.send(ControlMessage::FinishOwnedEpoch {
            intent: intent.clone(),
        });
    }
    match tokio::time::timeout(Duration::from_secs(60), reply).await {
        Ok(Ok(Ok(()))) => {
            let waiter = {
                let mut state = data_plane.lock().await;
                if state
                    .source_waiters
                    .get(&intent.request_id)
                    .is_some_and(|waiter| {
                        waiter.finishing && waiter.owned_intent.as_ref() == Some(&intent)
                    })
                {
                    state.source_waiters.remove(&intent.request_id)
                } else {
                    None
                }
            };
            if let Some(waiter) = waiter {
                send_message(
                    &waiter.sender,
                    &ControlMessage::SourceDeltaCommitted {
                        request_id: intent.request_id,
                        epoch: intent.epoch,
                    },
                )
                .await;
            }
        }
        Ok(Ok(Err(error))) => {
            fail_source_request(
                &data_plane,
                &intent.request_id,
                intent.workload_id,
                format!("owned epoch completion failed: {error}"),
            )
            .await;
        }
        Ok(Err(_)) | Err(_) => {
            fail_source_request(
                &data_plane,
                &intent.request_id,
                intent.workload_id,
                "owned epoch completion did not finish",
            )
            .await;
        }
    }
}

async fn begin_shard_handoff(data_plane: &Arc<AsyncMutex<DataPlaneState>>, shard_id: ShardId) {
    data_plane.lock().await.handoff_shards.insert(shard_id);
}

async fn end_shard_handoff(data_plane: &Arc<AsyncMutex<DataPlaneState>>, shard_id: ShardId) {
    let notify = {
        let mut state = data_plane.lock().await;
        state.handoff_shards.remove(&shard_id);
        state.handoff_notify.clone()
    };
    notify.notify_waiters();
}

/// Canonical online active-workload handoff helper shared by `MigrateShard` and
/// `DrainWorker`. Re-deploys every active workload descriptor referencing
/// `lease.shard_id` onto `lease.worker_id`, waits for the recipient worker to
/// restore durable state and emit `DeploymentReady`, and atomically updates
/// routing descriptors and worker status.
async fn redeploy_shard_workloads(
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    lease: &rockstream_types::lease::ShardLease,
) -> Result<(), String> {
    let affected: Vec<(WorkloadId, DeploymentDescriptor, Vec<String>)> = {
        let state = data_plane.lock().await;
        state
            .deployments
            .iter()
            .filter_map(|(workload_id, deployment)| {
                let mut descriptor = deployment.descriptors.get(&lease.shard_id)?.clone();
                descriptor.shard = lease.clone();
                let secret_names = deployment.secret_names.clone();
                Some((*workload_id, descriptor, secret_names))
            })
            .collect()
    };
    if affected.is_empty() {
        return Ok(());
    }
    let sender = worker_senders
        .lock()
        .await
        .get(&lease.worker_id)
        .filter(|sender| !sender.is_closed())
        .cloned()
        .ok_or_else(|| {
            format!(
                "RS-3610: worker {} has no active control connection for workload activation",
                lease.worker_id.0
            )
        })?;

    for (workload_id, descriptor, secret_names) in affected {
        let waiter_key = (workload_id, lease.shard_id, lease.worker_id);
        let (tx, rx) = oneshot::channel();
        data_plane
            .lock()
            .await
            .handoff_ready_waiters
            .insert(waiter_key, tx);
        send_message(
            &sender,
            &ControlMessage::Deploy {
                descriptor: descriptor.clone(),
            },
        )
        .await;
        for secret_name in &secret_names {
            send_message(
                &sender,
                &ControlMessage::SecretRefreshRequired {
                    workload_id,
                    secret_name: secret_name.clone(),
                },
            )
            .await;
        }
        let ready = tokio::time::timeout(Duration::from_secs(30), rx).await;
        data_plane
            .lock()
            .await
            .handoff_ready_waiters
            .remove(&waiter_key);
        match ready {
            Ok(Ok(())) => {
                let mut state = data_plane.lock().await;
                if let Some(deployment) = state.deployments.get_mut(&workload_id) {
                    let old_worker = deployment
                        .descriptors
                        .get(&lease.shard_id)
                        .map(|existing| existing.shard.worker_id);
                    deployment
                        .descriptors
                        .insert(lease.shard_id, descriptor.clone());
                    if let Some(old_worker_id) = old_worker {
                        if old_worker_id != lease.worker_id {
                            if let Some(old_status) = deployment.workers.get_mut(&old_worker_id) {
                                old_status
                                    .shard_ids
                                    .retain(|shard_id| *shard_id != lease.shard_id);
                            }
                        }
                    }
                    let status = deployment.workers.entry(lease.worker_id).or_insert(
                        WorkerExecutionStatus {
                            worker_id: lease.worker_id,
                            process_id: 0,
                            shard_ids: Vec::new(),
                            input_rows: 0,
                            output_rows: 0,
                            frontier: descriptor.frontier,
                            ready: true,
                        },
                    );
                    status.ready = true;
                    status.shard_ids.push(lease.shard_id);
                    status.shard_ids.sort();
                    status.shard_ids.dedup();
                }
            }
            Ok(Err(_)) => {
                return Err(format!(
                    "RS-3610: worker {} disconnected before confirming DeploymentReady for shard {}",
                    lease.worker_id.0, lease.shard_id.0
                ));
            }
            Err(_) => {
                return Err(format!(
                    "RS-3610: worker {} timed out waiting for DeploymentReady on shard {}",
                    lease.worker_id.0, lease.shard_id.0
                ));
            }
        }
    }
    Ok(())
}

async fn submit_source_delta(
    request: SourceDeltaRequest,
    sender: &mpsc::Sender<ControlMessage>,
    catalog: &TopologyCatalog,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
) {
    let early_failure = {
        let state = data_plane.lock().await;
        if state
            .deployments
            .get(&request.workload_id)
            .is_some_and(|deployment| deployment.recovery_required)
        {
            Some(workload_recovery_failure())
        } else if state
            .deployments
            .get(&request.workload_id)
            .is_some_and(|deployment| deployment.ready_waiter.is_some())
        {
            Some(data_plane_failure("workload deployment is not ready"))
        } else if request.rows.is_empty()
            && state
                .source_waiters
                .values()
                .any(|waiter| waiter.workload_id == request.workload_id)
        {
            Some(data_plane_failure(
                "another source delta is already in flight for this workload",
            ))
        } else {
            None
        }
    };
    if let Some(failure) = early_failure {
        send_message(sender, &failure).await;
        return;
    }
    if request.rows.is_empty() {
        send_message(
            sender,
            &ControlMessage::SourceDeltaCommitted {
                request_id: request.request_id,
                epoch: request.epoch,
            },
        )
        .await;
        return;
    }

    // If any shard belonging to this workload is in the brief donor-to-recipient
    // handoff window, wait for the atomic routing update to complete before
    // routing so foreground writes continue seamlessly without dropped deltas.
    let handoff_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let notify = {
            let state = data_plane.lock().await;
            let blocked = state
                .deployments
                .get(&request.workload_id)
                .is_some_and(|deployment| {
                    deployment
                        .descriptors
                        .keys()
                        .any(|shard_id| state.handoff_shards.contains(shard_id))
                });
            if blocked {
                Some(state.handoff_notify.clone())
            } else {
                None
            }
        };
        let Some(notify) = notify else {
            break;
        };
        if tokio::time::timeout_at(handoff_deadline, notify.notified())
            .await
            .is_err()
        {
            break;
        }
    }

    let routed = {
        let state = data_plane.lock().await;
        match state.deployments.get(&request.workload_id) {
            Some(deployment) if request.version != deployment.request.version => Err(format!(
                "{}: source delta version {} does not match deployment version {}",
                RS_0001, request.version, deployment.request.version
            )),
            Some(deployment) => route_source_delta(deployment, &request),
            None => Err(format!("{}: workload is not deployed", RS_0001)),
        }
    };
    let mut routed = match routed {
        Ok(routed) => routed,
        Err(error) => {
            send_message(sender, &data_plane_failure(error)).await;
            return;
        }
    };

    let senders = worker_senders.lock().await.clone();
    if routed
        .iter()
        .any(|(worker_id, _)| !senders.contains_key(worker_id))
    {
        send_message(
            sender,
            &data_plane_failure("a routed shard owner has no active worker connection"),
        )
        .await;
        return;
    }

    let mut preparation = None;
    let admission_failure = {
        let mut state = data_plane.lock().await;
        if state
            .deployments
            .get(&request.workload_id)
            .is_some_and(|deployment| deployment.recovery_required)
        {
            Some(workload_recovery_failure())
        } else if state
            .deployments
            .get(&request.workload_id)
            .is_some_and(|deployment| deployment.ready_waiter.is_some())
        {
            Some(data_plane_failure("workload deployment is not ready"))
        } else if state.source_waiters.contains_key(&request.request_id) {
            Some(data_plane_failure(
                "source delta request id is already in flight",
            ))
        } else if state.direct_epochs.contains_key(&request.workload_id)
            || state
                .workload_read_waiters
                .values()
                .any(|waiter| waiter.workload_id == request.workload_id)
            || state
                .deployments
                .get(&request.workload_id)
                .is_some_and(|deployment| {
                    deployment
                        .descriptors
                        .keys()
                        .any(|id| state.handoff_shards.contains(id))
                })
        {
            Some(data_plane_failure(
                "workload has an active read, direct epoch, or ownership handoff",
            ))
        } else if state
            .source_waiters
            .values()
            .any(|waiter| waiter.workload_id == request.workload_id)
        {
            Some(data_plane_failure(
                "another source delta is already in flight for this workload",
            ))
        } else if !state
            .deployments
            .get(&request.workload_id)
            .is_some_and(|deployment| {
                routed.iter().all(|(worker_id, frame)| {
                    deployment
                        .descriptors
                        .get(&frame.shard_id)
                        .is_some_and(|descriptor| {
                            descriptor.shard.worker_id == *worker_id
                                && descriptor.sink_operator_id == frame.operator_id
                                && frame.lease_token == descriptor.shard.lease_token
                        })
                })
            })
        {
            Some(data_plane_failure(
                "workload deployment changed while routing the source delta",
            ))
        } else {
            let mut candidates: Vec<_> = state.deployments[&request.workload_id]
                .descriptors
                .values()
                .map(|descriptor| descriptor.shard.worker_id)
                .collect();
            candidates.sort();
            candidates.dedup();
            let mut dispatchers: HashMap<_, _> = routed
                .iter()
                .filter_map(|(target, frame)| {
                    if !state.owned_shuffle_workers.contains(target) {
                        return None;
                    }
                    let target_info = catalog.get(*target)?;
                    let source = candidates
                        .iter()
                        .copied()
                        .find(|candidate| {
                            if candidate == target
                                || !state.owned_shuffle_workers.contains(candidate)
                                || !senders.contains_key(candidate)
                            {
                                return false;
                            }
                            catalog.get(*candidate).is_some_and(|source_info| {
                                let same_zone =
                                    source_info.location.has_same_az_as(&target_info.location);
                                let reachable =
                                    source_info.location.has_same_host_as(&target_info.location)
                                        || target_info
                                            .address
                                            .parse::<std::net::SocketAddr>()
                                            .is_ok_and(|addr| !addr.ip().is_loopback());
                                same_zone && reachable
                            })
                        })
                        .unwrap_or(*target);
                    Some((frame.shard_id, source))
                })
                .collect();
            if dispatchers.len() != routed.len() {
                dispatchers.clear();
            }
            let attempt_id = (!dispatchers.is_empty()).then(uuid::Uuid::new_v4);
            for (_, frame) in &mut routed {
                frame.attempt_id = attempt_id;
            }
            let mut target_shards: Vec<_> =
                routed.iter().map(|(_, frame)| frame.shard_id).collect();
            target_shards.sort();
            target_shards.dedup();
            let owned_intent = dispatchers.keys().min().copied().map(|anchor_shard| {
                rockstream_types::data_plane::OwnedEpochIntent {
                    attempt_id: attempt_id.expect("owned dispatch has an attempt"),
                    version: request.version,
                    request_id: request.request_id.clone(),
                    workload_id: request.workload_id,
                    epoch: request.epoch,
                    source: request.source.clone(),
                    anchor_shard,
                    target_shards,
                }
            });
            let prepare_reply = owned_intent.as_ref().map(|intent| {
                let (reply, receiver) = tokio::sync::oneshot::channel();
                let anchor_worker = state.deployments[&request.workload_id].descriptors
                    [&intent.anchor_shard]
                    .shard
                    .worker_id;
                preparation = Some((anchor_worker, intent.clone(), receiver));
                reply
            });
            state.source_waiters.insert(
                request.request_id.clone(),
                SourceWaiter {
                    attempt_id,
                    sender: sender.clone(),
                    workload_id: request.workload_id,
                    version: request.version,
                    source: request.source.clone(),
                    expected: routed.iter().map(|(_, frame)| frame.shard_id).collect(),
                    received: HashSet::new(),
                    dispatchers,
                    owned_intent,
                    prepare_reply,
                    finish_reply: None,
                    finishing: false,
                    epoch: request.epoch,
                },
            );
            None
        }
    };
    if let Some(failure) = admission_failure {
        send_message(sender, &failure).await;
        return;
    }

    if let Some((anchor_worker, intent, reply)) = preparation {
        let permit = match senders.get(&anchor_worker) {
            Some(anchor) => anchor.reserve().await.ok(),
            None => None,
        };
        let Some(permit) = permit else {
            fail_source_request(
                data_plane,
                &request.request_id,
                request.workload_id,
                "owned epoch anchor disconnected before preparation",
            )
            .await;
            return;
        };
        {
            let state = data_plane.lock().await;
            if !state.source_waiters.contains_key(&request.request_id) {
                return;
            }
            permit.send(ControlMessage::PrepareOwnedEpoch { intent });
        }
        match tokio::time::timeout(Duration::from_secs(60), reply).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                fail_source_request(
                    data_plane,
                    &request.request_id,
                    request.workload_id,
                    format!("owned epoch preparation failed: {error}"),
                )
                .await;
                return;
            }
            Ok(Err(_)) | Err(_) => {
                fail_source_request(
                    data_plane,
                    &request.request_id,
                    request.workload_id,
                    "owned epoch preparation did not complete",
                )
                .await;
                return;
            }
        }
    }

    for (target_worker, frame) in routed {
        let dispatcher = {
            let state = data_plane.lock().await;
            let Some(waiter) = state.source_waiters.get(&request.request_id) else {
                return;
            };
            waiter.dispatchers.get(&frame.shard_id).copied()
        };
        let target_info = dispatcher.and_then(|_| catalog.get(target_worker));
        let destination = dispatcher.unwrap_or(target_worker);
        let permit = match (
            senders.get(&destination),
            dispatcher.is_some() && target_info.is_none(),
        ) {
            (Some(target), false) => target.reserve().await.ok(),
            _ => None,
        };
        let Some(permit) = permit else {
            let removed = {
                let mut state = data_plane.lock().await;
                let removed = state.source_waiters.remove(&request.request_id).is_some();
                if removed {
                    if let Some(deployment) = state.deployments.get_mut(&request.workload_id) {
                        deployment.recovery_required = true;
                    }
                }
                removed
            };
            if removed {
                send_message(
                    sender,
                    &workload_failure("routed shard owner disconnected before ingest"),
                )
                .await;
            }
            return;
        };
        let state = data_plane.lock().await;
        if !state.source_waiters.contains_key(&request.request_id) {
            return;
        }
        let message = match target_info {
            Some(target) => ControlMessage::DispatchOwnedShuffle { target, frame },
            None => ControlMessage::Execute { frame },
        };
        permit.send(message);
    }
}

fn drain_failure_message(err: DrainFailure) -> ControlMessage {
    ControlMessage::OperationFailed {
        code: err.code.to_string(),
        message: err.message,
        next_steps: err.next_steps,
    }
}

async fn request_worker_drain(
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
    audit: Option<&Arc<FileAuditLog>>,
    topology_store: Option<&Arc<TopologyPersistentStore>>,
    drain_state: &Arc<AsyncMutex<DrainState>>,
    worker_id: WorkerId,
    operation_id: Option<String>,
) -> Result<(WorkerLifecycleState, u32, u32, DrainRequest), DrainFailure> {
    let Some(worker) = catalog.get(worker_id) else {
        return Err(DrainFailure::new(
            RS_3610,
            format!("worker {worker_id} is not present in the topology"),
            "Run `rockstream cluster status` to confirm the worker id, then retry the drain request.",
        ));
    };
    if let WorkerLifecycleState::Draining { started_at_ms, .. } = &worker.lifecycle {
        let guard = drain_state.lock().await;
        let resumes_this_operation = operation_id.as_ref().is_some_and(|operation_id| {
            guard
                .operation_ids
                .get(&worker_id)
                .is_some_and(|operation_ids| operation_ids.iter().any(|id| id == operation_id))
        });
        if resumes_this_operation {
            return Ok((
                worker.lifecycle.clone(),
                guard.queue.len() as u32,
                MAX_DRAIN_QUEUE as u32,
                DrainRequest {
                    worker_id,
                    deadline_ms: started_at_ms.saturating_add(DEFAULT_DRAIN_DEADLINE_MS),
                },
            ));
        }
    }
    if matches!(
        worker.lifecycle,
        WorkerLifecycleState::Decommissioned { .. }
    ) {
        return Err(DrainFailure::new(
            RS_3604,
            format!("worker {worker_id} is already draining or decommissioned"),
            "Wait for the existing drain to complete, or query the worker lifecycle state before retrying.",
        ));
    }

    let shards: Vec<ShardId> = shard_manager
        .leases()
        .into_iter()
        .filter(|lease| lease.worker_id == worker_id)
        .map(|lease| lease.shard_id)
        .collect();
    let (recipients, preferred_az) = {
        #[cfg(feature = "simulation")]
        {
            let mut recipients = catalog.healthy_workers();
            let mut preferred_az = worker.location.availability_zone.clone();
            if rockstream_sim::buggify!("exchange.domain_rebuild_during_drain", 1.0) {
                tokio::task::yield_now().await;
                recipients = catalog.healthy_workers();
                preferred_az = catalog
                    .get(worker_id)
                    .map(|current| current.location.availability_zone)
                    .unwrap_or(preferred_az);
            }
            (recipients, preferred_az)
        }
        #[cfg(not(feature = "simulation"))]
        {
            (
                catalog.healthy_workers(),
                worker.location.availability_zone.clone(),
            )
        }
    };
    let mut chosen = Vec::with_capacity(shards.len());
    if !shards.is_empty() && worker.capabilities.shared_shard_store_id.is_none() {
        return Err(DrainFailure::new(
            RS_3611,
            format!("worker {worker_id} cannot drain shards stored outside a verified shared object store"),
            "Configure every worker that owns these shards to use the same ROCKSTREAM_OBJECT_STORE_ENDPOINT, BUCKET, and REGION, then restart the workers.",
        ));
    }
    for shard_id in &shards {
        let eligible: Vec<_> = recipients
            .iter()
            .filter(|candidate| {
                candidate.worker_id != worker_id
                    && candidate.capabilities.shared_shard_store_id
                        == worker.capabilities.shared_shard_store_id
            })
            .cloned()
            .collect();
        let Some(recipient) = crate::placement::PlacementAlgorithm::choose_with_preference(
            &eligible,
            Some(&preferred_az),
        ) else {
            return Err(DrainFailure::new(
                RS_3611,
                format!("worker {worker_id} cannot drain shard {shard_id}: no active recipient worker is available"),
                "Register or recover another active worker with the same verified shared shard store, then retry the drain request.",
            ));
        };
        chosen.push((*shard_id, recipient.worker_id));
    }

    let mut guard = drain_state.lock().await;
    if guard.queue.len() + chosen.len() > MAX_DRAIN_QUEUE {
        return Err(DrainFailure::new(
            RS_3612,
            format!(
                "worker drain queue would exceed its bound ({}/{MAX_DRAIN_QUEUE})",
                guard.queue.len() + chosen.len()
            ),
            "Let the existing drain queue drain, or increase the configured bound only if memory headroom allows.",
        ));
    }
    let started_at_ms = now_ms();
    let lifecycle = WorkerLifecycleState::draining(shards.len() as u32, started_at_ms);
    let updated = catalog
        .set_lifecycle(worker_id, lifecycle.clone())
        .expect("worker existence checked above");
    persist_worker_if_needed(topology_store, &updated).await;

    for (shard_id, recipient_worker_id) in chosen {
        guard.queue.push_back(DrainTask {
            donor_worker_id: worker_id,
            recipient_worker_id,
            shard_id,
        });
    }
    if let Some(operation_id) = operation_id {
        guard
            .operation_ids
            .entry(worker_id)
            .or_default()
            .push(operation_id);
    }
    let queue_fill = guard.queue.len() as u32;
    drop(guard);

    if let Some(audit) = audit {
        let event = AuditEvent::now("control", "worker.drain_requested", worker_id.to_string())
            .with_detail(format!("shards={}", shards.len()));
        let _ = audit.append(&event);
    }
    Ok((
        lifecycle,
        queue_fill,
        MAX_DRAIN_QUEUE as u32,
        DrainRequest {
            worker_id,
            deadline_ms: started_at_ms.saturating_add(DEFAULT_DRAIN_DEADLINE_MS),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn process_drain_queue(
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    audit: Option<&Arc<FileAuditLog>>,
    shard_store: Option<&Arc<ShardPersistentStore>>,
    topology_store: Option<&Arc<TopologyPersistentStore>>,
    drain_state: &Arc<AsyncMutex<DrainState>>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    donor_worker_id: WorkerId,
    migration_waiters: &Arc<AsyncMutex<HashMap<String, oneshot::Sender<WorkerMessage>>>>,
    operation_store: Option<&ManagementOperationStore>,
) {
    let (tasks, operation_id) = {
        let mut guard = drain_state.lock().await;
        if !guard.acked_workers.contains(&donor_worker_id)
            || !guard.processing_workers.insert(donor_worker_id)
        {
            return;
        }
        let operation_id = guard
            .operation_ids
            .get(&donor_worker_id)
            .and_then(|operation_ids| operation_ids.first())
            .cloned();
        let mut tasks = Vec::new();
        guard.queue.retain(|task| {
            if task.donor_worker_id == donor_worker_id {
                tasks.push(task.clone());
                false
            } else {
                true
            }
        });
        (tasks, operation_id)
    };
    let mut moved = 0usize;
    let mut retry = Vec::new();
    for task in tasks {
        let Some(worker) = catalog.get(task.donor_worker_id) else {
            retry.push(task);
            continue;
        };
        let Some(recipient) = catalog.get(task.recipient_worker_id).filter(|candidate| {
            candidate.healthy
                && candidate.capabilities.shared_shard_store_id
                    == worker.capabilities.shared_shard_store_id
                && worker.capabilities.shared_shard_store_id.is_some()
        }) else {
            tracing::warn!(shard = %task.shard_id, recipient = %task.recipient_worker_id, "drain recipient no longer supports the shared shard store");
            retry.push(task);
            continue;
        };
        let Some(current_lease) = shard_manager.get(task.shard_id) else {
            retry.push(task);
            continue;
        };
        if current_lease.worker_id != task.donor_worker_id
            && current_lease.worker_id != recipient.worker_id
        {
            tracing::warn!(shard = %task.shard_id, donor = %task.donor_worker_id, "drain shard ownership changed before handoff");
            retry.push(task);
            continue;
        }
        let senders = worker_senders.lock().await.clone();
        let Some(donor_sender) = senders
            .get(&task.donor_worker_id)
            .filter(|sender| !sender.is_closed())
        else {
            retry.push(task);
            continue;
        };
        let Some(recipient_sender) = senders
            .get(&recipient.worker_id)
            .filter(|sender| !sender.is_closed())
        else {
            retry.push(task);
            continue;
        };
        let acknowledgement_id = operation_id
            .as_ref()
            .map(|operation_id| format!("{operation_id}:shard:{}", task.shard_id.0))
            .unwrap_or_else(|| {
                format!(
                    "drain:{}:{}:{}",
                    donor_worker_id.0, task.shard_id.0, current_lease.lease_token.0
                )
            });
        let key = format!("{acknowledgement_id}:recipient");
        let (tx, rx) = oneshot::channel();
        if try_insert_management_ack_waiter(&mut *migration_waiters.lock().await, key.clone(), tx)
            .is_err()
        {
            retry.push(task);
            continue;
        }
        let task_shard_id = task.shard_id;
        begin_shard_handoff(data_plane, task_shard_id).await;
        let lease = if current_lease.worker_id == task.donor_worker_id {
            let (lease, evicted) = shard_manager.force_acquire(task.shard_id, recipient.worker_id);
            if evicted != Some(task.donor_worker_id) {
                end_shard_handoff(data_plane, task_shard_id).await;
                migration_waiters.lock().await.remove(&key);
                retry.push(task);
                continue;
            }
            if let Some(store) = shard_store {
                persist_shard_state(shard_manager, store).await;
            }
            lease
        } else {
            current_lease
        };
        broadcast_message(
            worker_senders,
            topology_changed_message(catalog, shard_manager),
        )
        .await;
        if donor_sender
            .send(ControlMessage::ShardRevoked {
                shard_id: task.shard_id,
                reason: ShardRevokeReason::WorkerDrain,
            })
            .await
            .is_err()
        {
            end_shard_handoff(data_plane, task_shard_id).await;
            migration_waiters.lock().await.remove(&key);
            retry.push(task);
            continue;
        }
        if recipient_sender
            .send(ControlMessage::ShardAssigned {
                lease: lease.clone(),
                operation_id: Some(acknowledgement_id.clone()),
            })
            .await
            .is_err()
        {
            end_shard_handoff(data_plane, task_shard_id).await;
            migration_waiters.lock().await.remove(&key);
            retry.push(task);
            continue;
        }
        let acknowledgement = tokio::time::timeout(Duration::from_secs(30), rx).await;
        migration_waiters.lock().await.remove(&key);
        match acknowledgement {
            Ok(Ok(WorkerMessage::ShardTransferAck {
                operation_id: actual_operation_id,
                stage,
                worker_id,
                shard_id,
                lease_token,
                success,
                error: _,
            })) if actual_operation_id == acknowledgement_id
                && stage == "recipient"
                && worker_id == recipient.worker_id
                && shard_id == task.shard_id
                && lease_token == lease.lease_token
                && success =>
            {
                if let Err(error) =
                    redeploy_shard_workloads(data_plane, worker_senders, &lease).await
                {
                    tracing::warn!(
                        shard = %task.shard_id,
                        recipient = %recipient.worker_id,
                        %error,
                        "drain recipient failed to activate active workload"
                    );
                    retry.push(task);
                } else {
                    moved += 1;
                }
            }
            Ok(Ok(WorkerMessage::ShardTransferAck { error, .. })) => {
                tracing::warn!(
                    shard = %task.shard_id,
                    recipient = %recipient.worker_id,
                    error = ?error,
                    "drain recipient acknowledgement did not confirm the assigned lease"
                );
                retry.push(task);
            }
            Ok(Err(_)) | Err(_) => {
                retry.push(task);
            }
            Ok(Ok(_)) => retry.push(task),
        }
        end_shard_handoff(data_plane, task_shard_id).await;
    }
    let remaining = shard_manager
        .leases()
        .into_iter()
        .filter(|lease| lease.worker_id == donor_worker_id)
        .count();
    let (completed, pending, operation_ids) = {
        let mut drain_state = drain_state.lock().await;
        drain_state.queue.extend(retry);
        drain_state.processing_workers.remove(&donor_worker_id);
        let pending = drain_state
            .queue
            .iter()
            .any(|task| task.donor_worker_id == donor_worker_id);
        let completed =
            remaining == 0 && !pending && drain_state.acked_workers.remove(&donor_worker_id);
        let operation_ids = if completed {
            drain_state
                .operation_ids
                .remove(&donor_worker_id)
                .unwrap_or_default()
        } else if pending {
            drain_state
                .operation_ids
                .get(&donor_worker_id)
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        (completed, pending, operation_ids)
    };
    if completed {
        if let Some(updated) = catalog.set_lifecycle(
            donor_worker_id,
            WorkerLifecycleState::Decommissioned {
                completed_at_ms: now_ms(),
            },
        ) {
            persist_worker_if_needed(topology_store, &updated).await;
        }
        if let Some(audit) = audit {
            let event = AuditEvent::now(
                "control",
                "worker.drain_completed",
                donor_worker_id.to_string(),
            )
            .with_detail(format!("shards_moved={moved}"));
            let _ = audit.append(&event);
        }
        if let Some(operations) = operation_store {
            for operation_id in operation_ids {
                if let Err(error) = operations
                    .transition(
                        &operation_id,
                        OperationUpdate {
                            status: OperationStatus::Succeeded,
                            updated_at_ms: chrono::Utc::now().timestamp_millis(),
                            progress: Some(100),
                            phase: Some("completed".to_owned()),
                            error_code: None,
                            next_steps: Vec::new(),
                        },
                    )
                    .await
                {
                    tracing::error!(code = %rockstream_types::error_code::RS_0001, %operation_id, %error, "drain completed but operation record update failed");
                }
            }
        }
    } else if pending {
        if let Some(operations) = operation_store {
            for operation_id in operation_ids {
                let Ok(Some(record)) = operations.get(&operation_id).await else {
                    continue;
                };
                if record.status() != OperationStatus::Running {
                    continue;
                }
                if let Err(error) = operations
                    .transition_if(
                        &operation_id,
                        OperationStatus::Running,
                        record.phase(),
                        OperationUpdate {
                            status: OperationStatus::Waiting,
                            updated_at_ms: now_ms() as i64,
                            progress: record.progress(),
                            phase: record.phase().map(str::to_owned),
                            error_code: Some(RS_3610.to_string()),
                            next_steps: vec![
                                "A recipient ACK was missing or rejected; management will retry after reconciliation.".to_owned(),
                            ],
                        },
                    )
                    .await
                {
                    tracing::warn!(%operation_id, %error, "drain retry state could not be persisted");
                }
            }
        }
    }
}

async fn cleanup_decommissioned_workers(
    catalog: &TopologyCatalog,
    topology_store: Option<&Arc<TopologyPersistentStore>>,
) {
    let removed = catalog.remove_decommissioned_older_than(now_ms(), DEFAULT_DECOMMISSION_GRACE_MS);
    for worker in removed {
        delete_worker_if_needed(topology_store, worker.worker_id).await;
    }
}

fn append_required_security_audit(
    audit: Option<&Arc<FileAuditLog>>,
    event: &AuditEvent,
    context: &'static str,
) -> bool {
    let Some(audit) = audit else {
        tracing::error!(code = %rockstream_types::error_code::RS_0003, context, "required security audit log is unavailable");
        return false;
    };
    match audit.append(event) {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(code = %rockstream_types::error_code::RS_0003, %error, context, "required security audit append failed");
            false
        }
    }
}

async fn append_node_identity_rejection(
    catalog: Option<&Arc<dyn CatalogStore>>,
    identity: &NodeIdentity,
    connected_worker_id: Option<WorkerId>,
    claimed_worker_id: WorkerId,
) -> bool {
    let Some(catalog) = catalog else {
        tracing::error!(code = %RS_0003, "required node identity audit catalog is unavailable");
        return false;
    };
    let actor = connected_worker_id.map_or_else(
        || AuditIdentity::Unverified(identity.to_cn()),
        |_| AuditIdentity::Verified(identity.to_cn()),
    );
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64;
    let record = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("node-identity-rejection:{}", uuid::Uuid::new_v4()),
        timestamp_ms: timestamp_ms.max(1),
        actor,
        target: AuditField::Unverified(claimed_worker_id.to_string()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::NodeIdentityRejection,
        outcome: SecurityAuditOutcome::Denied,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::NotApplicable,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    if let Err(error) = crate::management::append_security_audit(catalog.as_ref(), record).await {
        tracing::error!(code = %RS_0003, %error, "required node identity audit append failed");
        return false;
    }
    true
}

async fn append_validated_security_audit(
    catalog: Option<&Arc<dyn CatalogStore>>,
    request_id: &str,
    record: SecurityAuditRecord,
) -> bool {
    if request_id != record.event_id
        || record.validate().is_err()
        || !matches!(
            record.action,
            SecurityAuditClass::AuthorizationDenial | SecurityAuditClass::NodeIdentityRejection
        )
        || record.outcome != SecurityAuditOutcome::Denied
    {
        tracing::error!(code = %RS_0003, "worker reported an invalid security audit record");
        return false;
    }
    let Some(catalog) = catalog else {
        tracing::error!(code = %RS_0003, "required worker security audit catalog is unavailable");
        return false;
    };
    if let Err(error) = crate::management::append_security_audit(catalog.as_ref(), record).await {
        tracing::error!(code = %RS_0003, %error, "required worker security audit append failed");
        return false;
    }
    true
}

async fn allow_gateway_data_plane_peer(
    sender: &mpsc::Sender<ControlMessage>,
    catalog: Option<&Arc<dyn CatalogStore>>,
    identity: Option<&NodeIdentity>,
    operation: &str,
) -> bool {
    let Some(identity) = identity else {
        // The listener permits no identity only in explicitly enabled loopback development mode.
        return true;
    };
    if identity.role == NodeRole::Gateway {
        return true;
    }

    let timestamp_ms = now_ms().max(1);
    let record = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("node-identity-rejection:{}", uuid::Uuid::new_v4()),
        timestamp_ms,
        actor: AuditIdentity::Unverified(identity.to_cn()),
        target: AuditField::Known(operation.to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::NodeIdentityRejection,
        outcome: SecurityAuditOutcome::Denied,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::NotApplicable,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let Some(catalog) = catalog else {
        tracing::error!(code = %RS_0003, "required node identity audit catalog is unavailable");
        return false;
    };
    if let Err(error) = crate::management::append_security_audit(catalog.as_ref(), record).await {
        tracing::error!(code = %RS_0003, %error, "required node identity audit append failed");
        return false;
    }
    send_message(
        sender,
        &ControlMessage::OperationFailed {
            code: RS_2412.to_string(),
            message: "certificate identity is not authorized for the gateway data plane".to_owned(),
            next_steps: rockstream_types::error_code::next_steps(RS_2412).to_string(),
        },
    )
    .await;
    false
}

fn is_gateway_data_plane_message(message: &WorkerMessage) -> bool {
    matches!(
        message,
        WorkerMessage::DeployWorkload(_)
            | WorkerMessage::DeployWorkloadSharded { .. }
            | WorkerMessage::SubmitSourceDelta(_)
            | WorkerMessage::AuthorizeSourceEpoch { .. }
            | WorkerMessage::ReadOperatorStats { .. }
            | WorkerMessage::ReadWorkload { .. }
            | WorkerMessage::GetWorkloadTopology { .. }
    )
}

async fn gateway_data_plane_scope(
    request: &WorkerMessage,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
) -> Option<(SecurityAction, PolicyScope)> {
    use rockstream_types::authorization::ResourceKind;

    match request {
        WorkerMessage::DeployWorkload(deployment)
        | WorkerMessage::DeployWorkloadSharded {
            request: deployment,
            ..
        } => Some((
            SecurityAction::ViewAuthor,
            PolicyScope::Resource {
                namespace: deployment.namespace.clone(),
                kind: ResourceKind::View,
                name: deployment.workload_name.clone(),
            },
        )),
        WorkerMessage::SubmitSourceDelta(delta) => {
            let state = data_plane.lock().await;
            let deployment = state.deployments.get(&delta.workload_id)?;
            if !deployment
                .request
                .schemas
                .iter()
                .any(|schema| schema.relation.eq_ignore_ascii_case(&delta.source))
            {
                return None;
            }
            Some((
                SecurityAction::DataWrite,
                PolicyScope::Resource {
                    namespace: deployment.request.namespace.clone(),
                    kind: ResourceKind::Table,
                    name: delta.source.clone(),
                },
            ))
        }
        WorkerMessage::AuthorizeSourceEpoch { metadata } => {
            if !metadata.is_valid() {
                return None;
            }
            let state = data_plane.lock().await;
            let deployment = state.deployments.get(&metadata.workload_id)?;
            if !deployment
                .request
                .schemas
                .iter()
                .any(|schema| schema.relation.eq_ignore_ascii_case(&metadata.source))
            {
                return None;
            }
            Some((
                SecurityAction::DataWrite,
                PolicyScope::Resource {
                    namespace: deployment.request.namespace.clone(),
                    kind: ResourceKind::Table,
                    name: metadata.source.clone(),
                },
            ))
        }
        WorkerMessage::ReadWorkload { workload_id, .. }
        | WorkerMessage::ReadOperatorStats { workload_id, .. }
        | WorkerMessage::GetWorkloadTopology { workload_id } => {
            let state = data_plane.lock().await;
            let deployment = state.deployments.get(workload_id)?;
            Some((
                SecurityAction::DataRead,
                PolicyScope::Resource {
                    namespace: deployment.request.namespace.clone(),
                    kind: ResourceKind::View,
                    name: deployment.request.workload_name.clone(),
                },
            ))
        }
        _ => None,
    }
}

async fn append_gateway_delegation_audit(
    catalog: Option<&Arc<dyn CatalogStore>>,
    actor: AuditIdentity,
    target: AuditField<String>,
    scope: AuditField<String>,
    action: SecurityAuditClass,
    outcome: SecurityAuditOutcome,
    policy_revision: AuditField<u64>,
) -> bool {
    let record = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("gateway-delegation:{}", uuid::Uuid::new_v4()),
        timestamp_ms: now_ms().max(1),
        actor,
        target,
        scope,
        action,
        outcome,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let Some(catalog) = catalog else {
        tracing::error!(code = %RS_0003, "required gateway delegation audit catalog is unavailable");
        return false;
    };
    if let Err(error) = crate::management::append_security_audit(catalog.as_ref(), record).await {
        tracing::error!(code = %RS_0003, %error, "required gateway delegation audit append failed");
        return false;
    }
    true
}

#[derive(Clone, Copy)]
struct GatewayActorAuthorizationContext<'a> {
    sender: &'a mpsc::Sender<ControlMessage>,
    catalog: Option<&'a Arc<dyn CatalogStore>>,
    verifier: Option<&'a crate::delegation::ActorAssertionVerifier>,
    identity: &'a NodeIdentity,
    data_plane: &'a Arc<AsyncMutex<DataPlaneState>>,
}

struct GatewayActorRejectionAudit {
    target: AuditField<String>,
    scope: AuditField<String>,
    action: SecurityAuditClass,
    actor: AuditIdentity,
    policy_revision: AuditField<u64>,
}

impl GatewayActorRejectionAudit {
    fn authentication(scope: AuditField<String>, actor: AuditIdentity) -> Self {
        Self {
            target: AuditField::Known("gateway_data_plane".to_owned()),
            scope,
            action: SecurityAuditClass::AuthenticationFailure,
            actor,
            policy_revision: AuditField::NotApplicable,
        }
    }

    fn unknown_target_authentication(actor: AuditIdentity) -> Self {
        Self {
            target: AuditField::Unknown,
            scope: AuditField::Unknown,
            action: SecurityAuditClass::AuthenticationFailure,
            actor,
            policy_revision: AuditField::NotApplicable,
        }
    }

    fn authorization(
        target: AuditField<String>,
        scope: AuditField<String>,
        actor: AuditIdentity,
        policy_revision: AuditField<u64>,
    ) -> Self {
        Self {
            target,
            scope,
            action: SecurityAuditClass::AuthorizationDenial,
            actor,
            policy_revision,
        }
    }
}

async fn reject_gateway_actor_assertion(
    context: &GatewayActorAuthorizationContext<'_>,
    code: ErrorCode,
    message: &str,
    audit: GatewayActorRejectionAudit,
) -> bool {
    if !append_gateway_delegation_audit(
        context.catalog,
        audit.actor,
        audit.target,
        audit.scope,
        audit.action,
        SecurityAuditOutcome::Denied,
        audit.policy_revision,
    )
    .await
    {
        return false;
    }
    let next_steps = if code == RS_2400 {
        rockstream_types::error_code::next_steps(RS_2400)
    } else {
        rockstream_types::error_code::next_steps(RS_2401)
    };
    tracing::warn!(peer = %context.identity.to_cn(), %code, "gateway actor assertion rejected");
    send_message(
        context.sender,
        &ControlMessage::OperationFailed {
            code: code.to_string(),
            message: message.to_owned(),
            next_steps: next_steps.to_string(),
        },
    )
    .await;
    false
}

#[allow(clippy::too_many_arguments)]
async fn authorize_gateway_actor_assertion(
    context: &GatewayActorAuthorizationContext<'_>,
    assertion: Option<&str>,
    request: &WorkerMessage,
    authorized_actor: &mut Option<DelegatedActor>,
) -> bool {
    let Some(assertion) = assertion else {
        return reject_gateway_actor_assertion(
            context,
            RS_2400,
            "a valid user actor assertion is required for gateway data-plane requests",
            GatewayActorRejectionAudit::authentication(
                AuditField::Unknown,
                AuditIdentity::Unverified(context.identity.to_cn()),
            ),
        )
        .await;
    };
    let Some(verifier) = context.verifier else {
        return reject_gateway_actor_assertion(
            context,
            RS_2400,
            "gateway actor assertion verification is not configured",
            GatewayActorRejectionAudit::authentication(
                AuditField::Unknown,
                AuditIdentity::Unverified(context.identity.to_cn()),
            ),
        )
        .await;
    };
    let expected = gateway_data_plane_scope(request, context.data_plane).await;
    let digest = match crate::delegation::request_digest(request) {
        Ok(digest) => digest,
        Err(_) => {
            return reject_gateway_actor_assertion(
                context,
                RS_2400,
                "gateway actor assertion could not be bound to this request",
                GatewayActorRejectionAudit::authentication(
                    AuditField::Unknown,
                    AuditIdentity::Unverified(context.identity.to_cn()),
                ),
            )
            .await;
        }
    };
    let now = now_ms().max(1) / 1_000;
    let claims = match verifier.verify(assertion, digest, now) {
        Ok(claims) => claims,
        Err(_) => {
            return reject_gateway_actor_assertion(
                context,
                RS_2400,
                "gateway actor assertion is invalid, expired, or bound to another deployment/request",
                GatewayActorRejectionAudit::authentication(
                    AuditField::Unknown,
                    AuditIdentity::Unverified(context.identity.to_cn()),
                ),
            )
            .await;
        }
    };
    let (expected_action, expected_scope) = match expected {
        Some(expected) => expected,
        None => {
            return reject_gateway_actor_assertion(
                context,
                RS_2401,
                "gateway actor assertion does not identify an active scoped workload request",
                GatewayActorRejectionAudit::authorization(
                    AuditField::Known("gateway_data_plane".to_owned()),
                    AuditField::Known(format!("{:?}", claims.scope())),
                    AuditIdentity::Verified(claims.principal_id().to_string()),
                    AuditField::NotApplicable,
                ),
            )
            .await;
        }
    };
    if claims.action() != expected_action || claims.scope() != &expected_scope {
        return reject_gateway_actor_assertion(
            context,
            RS_2401,
            "gateway actor assertion allowance does not match the requested operation and scope",
            GatewayActorRejectionAudit::authorization(
                AuditField::Known(format!("{expected_action:?}")),
                AuditField::Known(format!("{expected_scope:?}")),
                AuditIdentity::Verified(claims.principal_id().to_string()),
                AuditField::Known(claims.policy_revision()),
            ),
        )
        .await;
    }
    let Some(catalog_store) = context.catalog else {
        tracing::error!(code = %RS_0003, "durable actor policy is unavailable");
        return false;
    };
    let principals = match catalog_store.list_security_principals().await {
        Ok(principals) => principals,
        Err(_) => {
            return reject_gateway_actor_assertion(
                context,
                RS_2400,
                "durable actor policy is unavailable",
                GatewayActorRejectionAudit::unknown_target_authentication(AuditIdentity::Verified(
                    claims.principal_id().to_string(),
                )),
            )
            .await;
        }
    };
    let Some(principal) = principals
        .into_iter()
        .find(|principal| principal.id == claims.principal_id() && principal.enabled)
    else {
        return reject_gateway_actor_assertion(
            context,
            RS_2400,
            "asserted user actor is no longer enabled",
            GatewayActorRejectionAudit::unknown_target_authentication(AuditIdentity::Verified(
                claims.principal_id().to_string(),
            )),
        )
        .await;
    };
    if !crate::authorization::allows(&principal, expected_action, &expected_scope) {
        return reject_gateway_actor_assertion(
            context,
            RS_2401,
            "asserted user actor is no longer authorized for this operation and scope",
            GatewayActorRejectionAudit::authorization(
                AuditField::Known(format!("{expected_action:?}")),
                AuditField::Known(format!("{expected_scope:?}")),
                AuditIdentity::Verified(principal.id.to_string()),
                AuditField::Known(principal.revision),
            ),
        )
        .await;
    }
    let audited = append_gateway_delegation_audit(
        Some(catalog_store),
        AuditIdentity::Verified(principal.id.to_string()),
        AuditField::Known(format!("{expected_action:?}")),
        AuditField::Known(format!("{expected_scope:?}")),
        SecurityAuditClass::AuthenticationSuccess,
        SecurityAuditOutcome::Success,
        AuditField::Known(principal.revision),
    )
    .await;
    if audited && matches!(request, WorkerMessage::DeployWorkload(_)) {
        *authorized_actor = Some(DelegatedActor {
            principal_id: principal.id,
            secret_use_scopes: claims.secret_use_scopes().to_vec(),
        });
    }
    audited
}

async fn authorize_worker_secret_request(
    catalog: Option<&Arc<dyn CatalogStore>>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_id: WorkerId,
    workload_id: WorkloadId,
    secret_name: &str,
) -> Result<AuthorizedWorkerSecret, WorkerSecretAuthorizationError> {
    let Some(catalog) = catalog else {
        return Err(WorkerSecretAuthorizationError::Unavailable(
            "durable secret authorization catalog is unavailable",
        ));
    };
    let (request, delegated_actor) = {
        let state = data_plane.lock().await;
        let Some(deployment) = state.deployments.get(&workload_id) else {
            return Err(WorkerSecretAuthorizationError::Denied(
                "workload is not active",
            ));
        };
        if !deployment
            .descriptors
            .values()
            .any(|descriptor| descriptor.shard.worker_id == worker_id)
        {
            return Err(WorkerSecretAuthorizationError::Denied(
                "worker is not assigned to this workload",
            ));
        }
        (deployment.request.clone(), deployment.actor.clone())
    };
    let sources = catalog.list_sources().await.map_err(|_| {
        WorkerSecretAuthorizationError::Unavailable("durable source catalog is unavailable")
    })?;
    let mut matches = Vec::new();
    for source in sources {
        if !source.table_name.as_ref().is_some_and(|table| {
            request
                .schemas
                .iter()
                .any(|schema| schema.relation.eq_ignore_ascii_case(table))
        }) {
            continue;
        }
        let Some(reference) = source.options.get("secret") else {
            continue;
        };
        if rockstream_types::secret::secret_name_from_reference(reference) != Some(secret_name) {
            continue;
        }
        let (Some(namespace), Some(consumer)) = (source.secret_namespace, source.secret_consumer)
        else {
            return Err(WorkerSecretAuthorizationError::Denied(
                "source secret scope is incomplete",
            ));
        };
        matches.push((namespace, consumer));
    }
    let Some((namespace, consumer_id)) = matches.first().cloned() else {
        return Err(WorkerSecretAuthorizationError::Denied(
            "secret is not referenced by this workload",
        ));
    };
    if matches.iter().any(|(other_namespace, other_consumer)| {
        other_namespace != &namespace || *other_consumer != consumer_id
    }) {
        return Err(WorkerSecretAuthorizationError::Denied(
            "secret name is ambiguous within this workload",
        ));
    }
    let scope = PolicyScope::Secret {
        namespace: namespace.clone(),
        name: secret_name.to_owned(),
        consumer: Some(consumer_id),
    };
    let namespace_id = catalog
        .get_namespace_by_name(&namespace)
        .await
        .map_err(|_| {
            WorkerSecretAuthorizationError::Unavailable("durable namespace catalog is unavailable")
        })?
        .map(|namespace| u128::from(namespace.id.0))
        .ok_or(WorkerSecretAuthorizationError::Denied(
            "secret namespace is no longer present",
        ))?;
    let principals = catalog.list_security_principals().await.map_err(|_| {
        WorkerSecretAuthorizationError::Unavailable("durable principal catalog is unavailable")
    })?;
    let Some(consumer) = principals
        .iter()
        .find(|principal| principal.id == consumer_id && principal.enabled)
    else {
        return Err(WorkerSecretAuthorizationError::Denied(
            "source consumer is no longer enabled",
        ));
    };
    if !crate::authorization::allows(consumer, SecurityAction::SecretUse, &scope) {
        return Err(WorkerSecretAuthorizationError::Denied(
            "source consumer is no longer authorized for secret use",
        ));
    }
    let mut policy_revision = AuditField::NotApplicable;
    let actor_principal_id = delegated_actor.as_ref().map(|actor| actor.principal_id);
    if let Some(delegated_actor) = delegated_actor {
        let actor_id = delegated_actor.principal_id;
        let Some(actor) = principals
            .iter()
            .find(|principal| principal.id == actor_id && principal.enabled)
        else {
            return Err(WorkerSecretAuthorizationError::DeniedForActor {
                message: "delegated actor is no longer enabled",
                actor: actor_id,
                scope,
                policy_revision: AuditField::NotApplicable,
            });
        };
        if !crate::authorization::allows(actor, SecurityAction::SecretUse, &scope) {
            return Err(WorkerSecretAuthorizationError::DeniedForActor {
                message: "delegated actor is no longer authorized for secret use",
                actor: actor_id,
                scope,
                policy_revision: AuditField::Known(actor.revision),
            });
        }
        if !delegated_actor.secret_use_scopes.contains(&scope) {
            return Err(WorkerSecretAuthorizationError::DeniedForActor {
                message: "delegation assertion does not allow secret use for this scope",
                actor: actor_id,
                scope,
                policy_revision: AuditField::Known(actor.revision),
            });
        }
        policy_revision = AuditField::Known(actor.revision);
    }
    Ok(AuthorizedWorkerSecret {
        namespace_id,
        scope,
        actor_principal_id,
        policy_revision,
    })
}

async fn workloads_for_secret_rotation(
    catalog: Option<&Arc<dyn CatalogStore>>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_id: WorkerId,
    secret_name: &str,
) -> Result<Vec<WorkloadId>, WorkerSecretAuthorizationError> {
    let Some(catalog) = catalog else {
        return Err(WorkerSecretAuthorizationError::Unavailable(
            "durable source catalog is unavailable",
        ));
    };
    let sources = catalog.list_sources().await.map_err(|_| {
        WorkerSecretAuthorizationError::Unavailable("durable source catalog is unavailable")
    })?;
    let source_tables: HashSet<_> = sources
        .into_iter()
        .filter(|source| {
            source.options.get("secret").and_then(|reference| {
                rockstream_types::secret::secret_name_from_reference(reference)
            }) == Some(secret_name)
        })
        .filter_map(|source| source.table_name.map(|name| name.to_lowercase()))
        .collect();
    let state = data_plane.lock().await;
    Ok(state
        .deployments
        .iter()
        .filter(|(_, deployment)| {
            deployment
                .descriptors
                .values()
                .any(|descriptor| descriptor.shard.worker_id == worker_id)
                && deployment
                    .request
                    .schemas
                    .iter()
                    .any(|schema| source_tables.contains(&schema.relation.to_lowercase()))
        })
        .map(|(workload_id, _)| *workload_id)
        .collect())
}

async fn secret_names_for_deployment(
    catalog: Option<&Arc<dyn CatalogStore>>,
    schemas: &[rockstream_types::data_plane::DeploymentSchema],
) -> Result<Vec<String>, WorkerSecretAuthorizationError> {
    let Some(catalog) = catalog else {
        return Err(WorkerSecretAuthorizationError::Unavailable(
            "durable source catalog is unavailable",
        ));
    };
    let sources = catalog.list_sources().await.map_err(|_| {
        WorkerSecretAuthorizationError::Unavailable("durable source catalog is unavailable")
    })?;
    let mut secret_names = HashSet::new();
    for source in sources {
        if !source.table_name.as_ref().is_some_and(|table| {
            schemas
                .iter()
                .any(|schema| schema.relation.eq_ignore_ascii_case(table))
        }) {
            continue;
        }
        if let Some(reference) = source.options.get("secret") {
            let Some(secret_name) = rockstream_types::secret::secret_name_from_reference(reference)
            else {
                return Err(WorkerSecretAuthorizationError::Denied(
                    "source secret reference is invalid",
                ));
            };
            secret_names.insert(secret_name.to_owned());
        }
    }
    let mut secret_names: Vec<_> = secret_names.into_iter().collect();
    secret_names.sort();
    Ok(secret_names)
}

fn claimed_worker_id(message: &WorkerMessage) -> Option<WorkerId> {
    match message {
        WorkerMessage::DeploymentReady { worker_id, .. }
        | WorkerMessage::OperatorStatsReport { worker_id, .. }
        | WorkerMessage::Heartbeat { worker_id, .. }
        | WorkerMessage::Deregister { worker_id }
        | WorkerMessage::RequestShard { worker_id, .. }
        | WorkerMessage::DrainAck { worker_id, .. }
        | WorkerMessage::ShardTransferAck { worker_id, .. }
        | WorkerMessage::ShardCheckpointAck { worker_id, .. }
        | WorkerMessage::LifecycleState { worker_id, .. }
        | WorkerMessage::ShardLoadReport { worker_id, .. }
        | WorkerMessage::RoutingEpochCommitted { worker_id, .. }
        | WorkerMessage::DirectEpochGrantInstalled { worker_id, .. }
        | WorkerMessage::RequestDrain { worker_id } => Some(*worker_id),
        _ => None,
    }
}

async fn deny_authenticated_worker_action(
    sender: &mpsc::Sender<ControlMessage>,
    audit: Option<&Arc<FileAuditLog>>,
    identity: &NodeIdentity,
    resource: String,
    detail: String,
) -> bool {
    let event = AuditEvent::now(identity.to_cn(), "security.authorization_denied", resource)
        .with_detail(detail);
    if !append_required_security_audit(audit, &event, "worker identity claim denial") {
        return false;
    }
    send_message(
        sender,
        &ControlMessage::OperationFailed {
            code: RS_2401.to_string(),
            message: "worker identity claim is not authorized for this connection".to_owned(),
            next_steps: rockstream_types::error_code::next_steps(RS_2401).to_string(),
        },
    )
    .await;
    true
}

/// Accept a connection and perform TLS handshake if configured.
async fn accept_and_handle(
    stream: TcpStream,
    peer: SocketAddr,
    ctx: ConnectionContext,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
) {
    if let Some(tls_acceptor) = acceptor {
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => {
                let identity = match crate::tls::extract_peer_identity(&tls_stream) {
                    Ok(id) => Some(id),
                    Err(e) => {
                        tracing::warn!(%peer, error = %e, "control: mTLS identity extraction failed");
                        let event = AuditEvent::now(
                            "unknown",
                            "security.internal_mtls_denied",
                            format!("peer={peer}"),
                        )
                        .with_error_code(rockstream_types::error_code::RS_2411.to_string())
                        .with_detail(format!(
                            "identity extraction failed: {e}, error_code=RS-2411"
                        ));
                        append_required_security_audit(
                            ctx.audit.as_ref(),
                            &event,
                            "internal mTLS identity extraction failure",
                        );
                        return;
                    }
                };
                if let Some(ref id) = identity {
                    if id.role == NodeRole::Cli {
                        let event = AuditEvent::now(
                            id.to_cn(),
                            "cli.authenticated",
                            format!("peer={peer}"),
                        );
                        if !append_required_security_audit(
                            ctx.audit.as_ref(),
                            &event,
                            "internal CLI authentication",
                        ) {
                            return;
                        }
                    }
                }
                let (reader, writer) = tokio::io::split(tls_stream);
                handle_connection_stream(reader, writer, peer, ctx, identity).await;
            }
            Err(e) => {
                let err_str = e.to_string();
                let code = if err_str.contains("NoCertificate") || err_str.contains("missing") {
                    RS_2410
                } else {
                    RS_2411
                };
                tracing::warn!(%peer, error = %e, %code, "control: mTLS handshake rejected");
                let event = AuditEvent::now(
                    "unknown",
                    "security.internal_mtls_denied",
                    format!("peer={peer}"),
                )
                .with_error_code(code.to_string())
                .with_detail(format!("TLS handshake failed: {e}, error_code={code}"));
                append_required_security_audit(
                    ctx.audit.as_ref(),
                    &event,
                    "internal mTLS handshake rejection",
                );
            }
        }
    } else {
        let (reader, writer) = stream.into_split();
        handle_connection_stream(reader, writer, peer, ctx, None).await;
    }
}

/// Handle a single worker connection over plaintext.
#[allow(dead_code)]
async fn handle_connection(stream: TcpStream, peer: SocketAddr, ctx: ConnectionContext) {
    accept_and_handle(stream, peer, ctx, None).await;
}

async fn request_operator_stats(
    request_id: String,
    workload_id: WorkloadId,
    operator_ids: Vec<rockstream_types::ids::OperatorId>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
) -> ControlMessage {
    let unavailable = || ControlMessage::OperatorStatsSnapshot {
        request_id: request_id.clone(),
        available: false,
        stats: Vec::new(),
    };
    if operator_ids.is_empty() {
        return unavailable();
    }

    let expected_workers = {
        let state = data_plane.lock().await;
        let Some(deployment) = state.deployments.get(&workload_id) else {
            return unavailable();
        };
        if deployment.descriptors.is_empty()
            || deployment.ready_shards.len() != deployment.descriptors.len()
        {
            return unavailable();
        }
        let workers: HashSet<_> = deployment
            .descriptors
            .values()
            .map(|descriptor| descriptor.shard.worker_id)
            .collect();
        if workers.iter().any(|worker_id| {
            !deployment
                .workers
                .get(worker_id)
                .is_some_and(|status| status.ready)
        }) {
            return unavailable();
        }
        workers
    };
    if expected_workers.is_empty() {
        return unavailable();
    }

    let mut worker_targets = {
        let senders = worker_senders.lock().await;
        expected_workers
            .iter()
            .filter_map(|worker_id| {
                senders
                    .get(worker_id)
                    .cloned()
                    .map(|sender| (*worker_id, sender))
            })
            .collect::<Vec<_>>()
    };
    if worker_targets.len() != expected_workers.len() {
        return unavailable();
    }
    worker_targets.sort_by_key(|(worker_id, _)| *worker_id);

    let (report_tx, mut report_rx) = mpsc::channel(expected_workers.len());
    let operator_ids = operator_ids.into_iter().collect::<HashSet<_>>();
    {
        let mut state = data_plane.lock().await;
        if state.operator_stats_waiters.contains_key(&request_id) {
            return unavailable();
        }
        state.operator_stats_waiters.insert(
            request_id.clone(),
            OperatorStatsWaiter {
                workload_id,
                expected_workers: expected_workers.clone(),
                received_workers: HashSet::new(),
                operator_ids: operator_ids.clone(),
                sender: report_tx,
            },
        );
    }

    let command = ControlMessage::ReadOperatorStats {
        request_id: request_id.clone(),
        workload_id,
        operator_ids: operator_ids.iter().copied().collect(),
    };
    for (_, worker_sender) in worker_targets {
        if worker_sender.send(command.clone()).await.is_err() {
            data_plane
                .lock()
                .await
                .operator_stats_waiters
                .remove(&request_id);
            return unavailable();
        }
    }

    let reports = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut received_workers = HashSet::new();
        let mut combined = HashMap::<
            rockstream_types::ids::OperatorId,
            rockstream_types::explain::IdentifiedOperatorStats,
        >::new();
        let mut stats_workers =
            HashMap::<rockstream_types::ids::OperatorId, HashSet<WorkerId>>::new();
        while received_workers.len() < expected_workers.len() {
            let Some((worker_id, stats)) = report_rx.recv().await else {
                break;
            };
            if !expected_workers.contains(&worker_id) || !received_workers.insert(worker_id) {
                continue;
            }
            for stat in stats {
                if !operator_ids.contains(&stat.operator_id) {
                    continue;
                }
                stats_workers
                    .entry(stat.operator_id)
                    .or_default()
                    .insert(worker_id);
                if let Some(total) = combined.get_mut(&stat.operator_id) {
                    total.stats.rows_per_s += stat.stats.rows_per_s;
                    total.stats.state_reads += stat.stats.state_reads;
                    total.stats.p99_latency_ms =
                        total.stats.p99_latency_ms.max(stat.stats.p99_latency_ms);
                    total.stats.dlq_entries += stat.stats.dlq_entries;
                    total.rmw_avoided += stat.rmw_avoided;
                    total.rmw_required += stat.rmw_required;
                    let rmw_total = total.rmw_avoided + total.rmw_required;
                    total.stats.rmw_ratio = if rmw_total == 0 {
                        0.0
                    } else {
                        total.rmw_required as f64 / rmw_total as f64
                    };
                } else {
                    combined.insert(stat.operator_id, stat);
                }
            }
        }
        (received_workers, combined, stats_workers)
    })
    .await;
    data_plane
        .lock()
        .await
        .operator_stats_waiters
        .remove(&request_id);

    let Ok((received_workers, mut combined, stats_workers)) = reports else {
        return unavailable();
    };
    if received_workers != expected_workers || combined.is_empty() {
        return unavailable();
    }
    combined.retain(|operator_id, _| {
        stats_workers
            .get(operator_id)
            .is_some_and(|workers| workers.len() == expected_workers.len())
    });
    if combined.is_empty() {
        return unavailable();
    }
    let mut stats: Vec<_> = combined.into_values().collect();
    stats.sort_by_key(|stat| stat.operator_id);
    ControlMessage::OperatorStatsSnapshot {
        request_id: request_id.clone(),
        available: true,
        stats,
    }
}

/// Reports with a routing certificate must describe one complete epoch on
/// every participant. Legacy reports without a journal retain wire support.
fn consistent_routing_snapshot(shards: &[ShardOutput]) -> bool {
    if shards.is_empty() {
        return false;
    }
    let anchors = shards
        .iter()
        .filter(|shard| shard.routing_epoch.is_some())
        .collect::<Vec<_>>();
    if shards
        .iter()
        .all(|shard| shard.committed_epoch.is_none() && shard.routing_epoch.is_none())
    {
        return true;
    }
    if anchors.len() != 1 {
        return false;
    }
    let anchor = anchors[0];
    if shards.iter().map(|shard| shard.shard_id).min() != Some(anchor.shard_id) {
        return false;
    }
    shards
        .iter()
        .all(|shard| shard.committed_epoch == anchor.routing_epoch)
}

async fn request_workload_snapshot(
    workload_id: WorkloadId,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
) -> ControlMessage {
    let unavailable = || data_plane_failure("complete workload output is unavailable");
    let (deployment_request, workers, expected_shards) = {
        let state = data_plane.lock().await;
        let Some(deployment) = state.deployments.get(&workload_id) else {
            return unavailable();
        };
        if deployment.recovery_required {
            return workload_recovery_failure();
        }
        if deployment.ready_waiter.is_some()
            || state
                .source_waiters
                .values()
                .any(|waiter| waiter.workload_id == workload_id)
        {
            return data_plane_failure("workload has an incomplete execution");
        }
        let descriptor_shards = deployment
            .descriptors
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        if descriptor_shards.is_empty() || deployment.ready_shards != descriptor_shards {
            return unavailable();
        }
        let mut expected_shards = HashMap::<WorkerId, HashMap<ShardId, LeaseToken>>::new();
        for (shard_id, descriptor) in &deployment.descriptors {
            if descriptor.shard.shard_id != *shard_id
                || !shard_manager.get(*shard_id).is_some_and(|lease| {
                    lease.worker_id == descriptor.shard.worker_id
                        && lease.lease_token == descriptor.shard.lease_token
                })
            {
                return unavailable();
            }
            expected_shards
                .entry(descriptor.shard.worker_id)
                .or_default()
                .insert(*shard_id, descriptor.shard.lease_token);
        }
        for worker_id in expected_shards.keys() {
            let Some(worker) = catalog.get(*worker_id) else {
                return unavailable();
            };
            if !worker.healthy
                || !worker.lifecycle.is_active()
                || !worker
                    .protocol_range
                    .contains(rockstream_types::compatibility::ProtocolVersion::V4)
                || !deployment
                    .workers
                    .get(worker_id)
                    .is_some_and(|status| status.ready)
            {
                return unavailable();
            }
        }
        let mut workers: Vec<_> = deployment.workers.values().cloned().collect();
        workers.sort_by_key(|status| status.worker_id);
        (deployment.request.clone(), workers, expected_shards)
    };
    let request_id = format!("workload-read:{}", uuid::Uuid::new_v4());
    let mut worker_targets = {
        let senders = worker_senders.lock().await;
        let mut targets = Vec::with_capacity(expected_shards.len());
        for (worker_id, shard_leases) in &expected_shards {
            let Some(sender) = senders.get(worker_id).cloned() else {
                return unavailable();
            };
            let mut shard_ids: Vec<_> = shard_leases.keys().copied().collect();
            shard_ids.sort();
            targets.push((*worker_id, sender, shard_ids));
        }
        targets
    };
    worker_targets.sort_by_key(|(worker_id, _, _)| *worker_id);

    let (report_tx, mut report_rx) = mpsc::channel(expected_shards.len());
    {
        let mut state = data_plane.lock().await;
        if state.workload_read_waiters.contains_key(&request_id) {
            return unavailable();
        }
        state.workload_read_waiters.insert(
            request_id.clone(),
            WorkloadReadWaiter {
                workload_id,
                expected_shards: expected_shards.clone(),
                received_workers: HashSet::new(),
                sender: report_tx,
            },
        );
    }
    for (_, worker_sender, shard_ids) in worker_targets {
        if worker_sender
            .send(ControlMessage::ReadWorkload {
                version: rockstream_types::data_plane::WORKLOAD_SNAPSHOT_VERSION,
                request_id: request_id.clone(),
                workload_id,
                shard_ids,
            })
            .await
            .is_err()
        {
            data_plane
                .lock()
                .await
                .workload_read_waiters
                .remove(&request_id);
            return unavailable();
        }
    }

    let expected_shard_count = expected_shards.values().map(HashMap::len).sum::<usize>();
    let reports = tokio::time::timeout(Duration::from_secs(5), async {
        let expected_workers = expected_shards.keys().copied().collect::<HashSet<_>>();
        let mut received_workers = HashSet::new();
        let mut outputs = HashMap::<ShardId, ShardOutput>::new();
        while received_workers.len() < expected_workers.len() {
            let (worker_id, available, shards) = report_rx.recv().await?;
            if !expected_workers.contains(&worker_id) || !received_workers.insert(worker_id) {
                continue;
            }
            if !available {
                return None;
            }
            let expected = expected_shards.get(&worker_id)?;
            if shards.len() != expected.len() {
                return None;
            }
            for shard in shards {
                if expected.get(&shard.shard_id).copied() != shard.lease_token
                    || outputs.insert(shard.shard_id, shard).is_some()
                {
                    return None;
                }
            }
        }
        (received_workers == expected_workers && outputs.len() == expected_shard_count)
            .then_some(outputs)
    })
    .await;
    data_plane
        .lock()
        .await
        .workload_read_waiters
        .remove(&request_id);
    let Ok(Some(mut outputs)) = reports else {
        return unavailable();
    };
    for (worker_id, shard_leases) in &expected_shards {
        for (shard_id, lease_token) in shard_leases {
            if !shard_manager.get(*shard_id).is_some_and(|lease| {
                lease.worker_id == *worker_id && lease.lease_token == *lease_token
            }) {
                return unavailable();
            }
        }
    }
    let mut shards: Vec<_> = outputs.drain().map(|(_, output)| output).collect();
    shards.sort_by_key(|shard| shard.shard_id);
    if !consistent_routing_snapshot(&shards) {
        return data_plane_failure(
            "workload read crossed a routing epoch; retry the complete snapshot",
        );
    }
    ControlMessage::WorkloadSnapshot {
        snapshot: WorkloadSnapshot {
            version: rockstream_types::data_plane::WORKLOAD_SNAPSHOT_VERSION,
            complete: true,
            deployment: deployment_request,
            shards,
            workers,
        },
    }
}

/// Handle a single worker connection over an arbitrary stream.
async fn handle_connection_stream<R, W>(
    reader: R,
    writer: W,
    peer: SocketAddr,
    ctx: ConnectionContext,
    peer_identity: Option<NodeIdentity>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let ConnectionContext {
        catalog,
        shard_manager,
        audit,
        raft,
        shard_store,
        synced_epoch,
        frontier,
        topology_store,
        operation_store,
        drain_state,
        secret_store,
        security_catalog,
        actor_assertion_verifier,
        worker_senders,
        migration_waiters,
        backup_waiters,
        data_plane,
    } = ctx;
    let (sender, mut outbound) = mpsc::channel::<ControlMessage>(32);
    let writer_task = tokio::spawn(async move {
        let mut writer = writer;
        while let Some(message) = outbound.recv().await {
            let Ok(mut line) = serde_json::to_string(&message) else {
                continue;
            };
            line.push('\n');
            if writer.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });
    let mut lines = BufReader::new(reader).lines();
    let mut connected_worker_id: Option<rockstream_types::ids::WorkerId> = None;
    let mut rotation_rx = secret_store.subscribe_rotation();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let msg: WorkerMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "control: invalid message");
                continue;
            }
        };

        let (msg, actor_assertion) = match msg {
            WorkerMessage::DelegatedDataPlane {
                version,
                actor_assertion,
                request,
            } if version == rockstream_types::topology::DATA_PLANE_DELEGATION_VERSION
                && is_gateway_data_plane_message(&request) =>
            {
                (*request, Some(actor_assertion))
            }
            WorkerMessage::DelegatedDataPlane { .. } => {
                let actor = peer_identity
                    .as_ref()
                    .map_or(AuditIdentity::Unknown, |identity| {
                        AuditIdentity::Unverified(identity.to_cn())
                    });
                if !append_gateway_delegation_audit(
                    security_catalog.as_ref(),
                    actor,
                    AuditField::Known("gateway_data_plane".to_owned()),
                    AuditField::Unknown,
                    SecurityAuditClass::AuthenticationFailure,
                    SecurityAuditOutcome::Denied,
                    AuditField::NotApplicable,
                )
                .await
                {
                    break;
                }
                send_message(
                    &sender,
                    &ControlMessage::OperationFailed {
                        code: RS_2400.to_string(),
                        message: "gateway data-plane delegation envelope is invalid".to_owned(),
                        next_steps: rockstream_types::error_code::next_steps(RS_2400).to_string(),
                    },
                )
                .await;
                break;
            }
            request => (request, None),
        };

        if let (Some(identity), Some(claimed_id)) = (&peer_identity, claimed_worker_id(&msg)) {
            if identity.role != NodeRole::Worker
                || !identity.matches_worker_id(claimed_id.0)
                || connected_worker_id != Some(claimed_id)
            {
                let registered_id = connected_worker_id
                    .map(|worker_id| worker_id.to_string())
                    .unwrap_or_else(|| "unknown".to_owned());
                if append_node_identity_rejection(
                    security_catalog.as_ref(),
                    identity,
                    connected_worker_id,
                    claimed_id,
                )
                .await
                {
                    let event = AuditEvent::now(
                        identity.to_cn(),
                        "security.authorization_denied",
                        claimed_id.to_string(),
                    )
                    .with_error_code(RS_2401.to_string())
                    .with_detail(format!(
                        "registered_worker_id={registered_id}, claimed_worker_id={claimed_id}, error_code={RS_2401}"
                    ));
                    if append_required_security_audit(
                        audit.as_ref(),
                        &event,
                        "worker node identity rejection",
                    ) {
                        send_message(
                            &sender,
                            &ControlMessage::OperationFailed {
                                code: RS_2401.to_string(),
                                message:
                                    "worker identity claim is not authorized for this connection"
                                        .to_owned(),
                                next_steps: rockstream_types::error_code::next_steps(RS_2401)
                                    .to_string(),
                            },
                        )
                        .await;
                    }
                }
                break;
            }
        }

        let mut delegated_actor = None;
        if is_gateway_data_plane_message(&msg) {
            if !allow_gateway_data_plane_peer(
                &sender,
                security_catalog.as_ref(),
                peer_identity.as_ref(),
                "gateway_data_plane",
            )
            .await
            {
                break;
            }
            if let Some(identity) = peer_identity.as_ref() {
                let authorization_context = GatewayActorAuthorizationContext {
                    sender: &sender,
                    catalog: security_catalog.as_ref(),
                    verifier: actor_assertion_verifier.as_deref(),
                    identity,
                    data_plane: &data_plane,
                };
                if !authorize_gateway_actor_assertion(
                    &authorization_context,
                    actor_assertion.as_deref(),
                    &msg,
                    &mut delegated_actor,
                )
                .await
                {
                    break;
                }
            } else if actor_assertion.is_some() {
                if !append_gateway_delegation_audit(
                    security_catalog.as_ref(),
                    AuditIdentity::Unknown,
                    AuditField::Known("gateway_data_plane".to_owned()),
                    AuditField::Unknown,
                    SecurityAuditClass::AuthenticationFailure,
                    SecurityAuditOutcome::Denied,
                    AuditField::NotApplicable,
                )
                .await
                {
                    break;
                }
                send_message(
                    &sender,
                    &ControlMessage::OperationFailed {
                        code: RS_2400.to_string(),
                        message:
                            "gateway actor assertions require an authenticated node connection"
                                .to_owned(),
                        next_steps: rockstream_types::error_code::next_steps(RS_2400).to_string(),
                    },
                )
                .await;
                break;
            }
        }

        match msg {
            WorkerMessage::DelegatedDataPlane { .. } => break,
            WorkerMessage::Register(reg) => {
                if let Some(id) = &peer_identity {
                    if id.role != NodeRole::Worker || !id.matches_worker_id(reg.worker_id.0) {
                        tracing::warn!(
                            %peer,
                            cert_identity = %id.to_cn(),
                            registered_id = %reg.worker_id,
                            "control: worker mTLS identity mismatch rejected"
                        );
                        if !append_node_identity_rejection(
                            security_catalog.as_ref(),
                            id,
                            connected_worker_id,
                            reg.worker_id,
                        )
                        .await
                        {
                            return;
                        }
                        let event = AuditEvent::now(
                            id.to_cn(),
                            "security.internal_mtls_denied",
                            format!("worker_id={}, peer={peer}", reg.worker_id),
                        )
                        .with_error_code(rockstream_types::error_code::RS_2412.to_string())
                        .with_detail(format!(
                            "node identity mismatch: cert={}, requested_worker_id={}, error_code=RS-2412",
                            id.to_cn(),
                            reg.worker_id
                        ));
                        if !append_required_security_audit(
                            audit.as_ref(),
                            &event,
                            "worker node identity rejection",
                        ) {
                            return;
                        }
                        let reply = ControlMessage::OperationFailed {
                            code: RS_2412.to_string(),
                            message: format!(
                                "certificate identity {} does not match requested worker_id {}",
                                id.to_cn(),
                                reg.worker_id
                            ),
                            next_steps: rockstream_types::error_code::next_steps(RS_2412)
                                .to_string(),
                        };
                        send_message(&sender, &reply).await;
                        return;
                    }
                    if !reg
                        .protocol_range
                        .contains(rockstream_types::compatibility::ProtocolVersion::V4)
                    {
                        let record = SecurityAuditRecord {
                            format_version: SECURITY_AUDIT_FORMAT_VERSION,
                            event_id: format!("worker-protocol-denial:{}", uuid::Uuid::new_v4()),
                            timestamp_ms: now_ms().max(1),
                            actor: AuditIdentity::Verified(id.to_cn()),
                            target: AuditField::Known(reg.worker_id.to_string()),
                            scope: AuditField::Known("worker_control".to_owned()),
                            action: SecurityAuditClass::AuthorizationDenial,
                            outcome: SecurityAuditOutcome::Denied,
                            correlation: AuditCorrelation::Unknown,
                            operation_id: AuditField::NotApplicable,
                            policy_revision: AuditField::NotApplicable,
                            before_state: AuditField::NotApplicable,
                            after_state: AuditField::NotApplicable,
                        };
                        let request_id = record.event_id.clone();
                        if !append_validated_security_audit(
                            security_catalog.as_ref(),
                            &request_id,
                            record,
                        )
                        .await
                        {
                            return;
                        }
                        let event = AuditEvent::now(
                            id.to_cn(),
                            "security.internal_mtls_denied",
                            format!("worker_id={}, peer={peer}", reg.worker_id),
                        )
                        .with_error_code(rockstream_types::error_code::RS_5021.to_string())
                        .with_detail(format!(
                            "worker protocol range {:?} does not include v4 required for complete workload reads",
                            reg.protocol_range
                        ));
                        if !append_required_security_audit(
                            audit.as_ref(),
                            &event,
                            "worker protocol version rejection",
                        ) {
                            return;
                        }
                        send_message(
                            &sender,
                            &ControlMessage::OperationFailed {
                                code: rockstream_types::error_code::RS_5021.to_string(),
                                message:
                                    "RS-5021: authenticated worker shuffle requires protocol v4"
                                        .to_owned(),
                                next_steps: rockstream_types::error_code::next_steps(
                                    rockstream_types::error_code::RS_5021,
                                )
                                .to_string(),
                            },
                        )
                        .await;
                        return;
                    }
                }
                let worker_id = catalog.register(&reg);
                connected_worker_id = Some(worker_id);
                worker_senders
                    .lock()
                    .await
                    .insert(worker_id, sender.clone());
                tracing::info!(
                    worker_id = %worker_id,
                    address = %reg.address,
                    host_id = %reg.location.host_id,
                    availability_zone = %reg.location.availability_zone,
                    headroom = %reg.capacity_headroom,
                    "control: worker registered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.registered", worker_id.to_string())
                            .with_detail(format!(
                                "address={}, host_id={}, availability_zone={}, headroom={}, same_host_arrow_shm_v1={}, shuffle_codec_v1={}, checkpoint_manifest_codec_v1={}, protocol_range={:?}, storage_format_range={:?}",
                                reg.address,
                                reg.location.host_id,
                                reg.location.availability_zone,
                                reg.capacity_headroom,
                                reg.capabilities.same_host_arrow_shm_v1,
                                reg.capabilities.shuffle_codec_v1,
                                reg.capabilities.checkpoint_manifest_codec_v1,
                                reg.protocol_range,
                                reg.storage_format_range
                            ));
                    let _ = aud.append(&event);
                }
                if let Some(worker) = catalog.get(worker_id) {
                    persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                }
                let reply = ControlMessage::Registered {
                    worker_id,
                    protocol_version: rockstream_types::compatibility::ProtocolVersion::V4,
                };
                send_message(&sender, &reply).await;
                broadcast_message(
                    &worker_senders,
                    topology_changed_message(&catalog, &shard_manager),
                )
                .await;
                let acked = drain_state
                    .lock()
                    .await
                    .acked_workers
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                for donor_worker_id in acked {
                    process_drain_queue(
                        &catalog,
                        &shard_manager,
                        &data_plane,
                        audit.as_ref(),
                        shard_store.as_ref(),
                        topology_store.as_ref(),
                        &drain_state,
                        &worker_senders,
                        donor_worker_id,
                        &migration_waiters,
                        operation_store.as_ref(),
                    )
                    .await;
                }
            }
            WorkerMessage::OwnedShuffleReady { worker_id } => {
                if connected_worker_id == Some(worker_id) {
                    data_plane
                        .lock()
                        .await
                        .owned_shuffle_workers
                        .insert(worker_id);
                }
            }
            WorkerMessage::OwnedEpochRecoveryRequired {
                worker_id,
                workload_id,
                shard_id,
            } => {
                if connected_worker_id == Some(worker_id) {
                    let mut state = data_plane.lock().await;
                    if let Some(deployment) = state.deployments.get_mut(&workload_id) {
                        if deployment
                            .descriptors
                            .get(&shard_id)
                            .is_some_and(|descriptor| descriptor.shard.worker_id == worker_id)
                        {
                            deployment.recovery_required = true;
                        }
                    }
                }
            }
            WorkerMessage::OwnedEpochPrepared {
                worker_id,
                intent,
                error,
            } => {
                let reply = take_owned_epoch_reply(
                    &mut *data_plane.lock().await,
                    connected_worker_id,
                    worker_id,
                    &intent,
                    false,
                );
                if let Some(reply) = reply {
                    let _ = reply.send(error.map_or(Ok(()), Err));
                }
            }
            WorkerMessage::OwnedEpochFinished {
                worker_id,
                intent,
                error,
            } => {
                let reply = take_owned_epoch_reply(
                    &mut *data_plane.lock().await,
                    connected_worker_id,
                    worker_id,
                    &intent,
                    true,
                );
                if let Some(reply) = reply {
                    let _ = reply.send(error.map_or(Ok(()), Err));
                }
            }
            WorkerMessage::ReportSecurityAudit { request_id, record } => {
                let authenticated_reporter = peer_identity.as_ref().is_some_and(|identity| {
                    connected_worker_id.is_some_and(|worker_id| {
                        identity.role == NodeRole::Worker && identity.matches_worker_id(worker_id.0)
                    })
                });
                let persisted = authenticated_reporter
                    && append_validated_security_audit(
                        security_catalog.as_ref(),
                        &request_id,
                        record,
                    )
                    .await;
                send_message(
                    &sender,
                    &ControlMessage::SecurityAuditRecorded {
                        request_id,
                        persisted,
                    },
                )
                .await;
            }
            WorkerMessage::DeployWorkload(request) => {
                deploy_workload(
                    request,
                    None,
                    delegated_actor,
                    WorkloadDeploymentContext {
                        secret_catalog: security_catalog.as_ref(),
                        sender: &sender,
                        catalog: &catalog,
                        shard_manager: &shard_manager,
                        worker_senders: &worker_senders,
                        data_plane: &data_plane,
                    },
                )
                .await;
                if let Some(store) = &shard_store {
                    persist_shard_state(&shard_manager, store).await;
                }
            }
            WorkerMessage::DeployWorkloadSharded {
                request,
                shard_count,
            } => {
                deploy_workload(
                    request,
                    Some(shard_count),
                    delegated_actor,
                    WorkloadDeploymentContext {
                        secret_catalog: security_catalog.as_ref(),
                        sender: &sender,
                        catalog: &catalog,
                        shard_manager: &shard_manager,
                        worker_senders: &worker_senders,
                        data_plane: &data_plane,
                    },
                )
                .await;
                if let Some(store) = &shard_store {
                    persist_shard_state(&shard_manager, store).await;
                }
            }
            WorkerMessage::DeploymentReady {
                workload_id,
                shard_id,
                worker_id,
                process_id,
                operator_ids,
                frontier: worker_frontier,
                ..
            } => {
                let (reply, handoff_waiter) = {
                    let mut state = data_plane.lock().await;
                    let handoff_waiter =
                        state
                            .handoff_ready_waiters
                            .remove(&(workload_id, shard_id, worker_id));
                    let Some(deployment) = state.deployments.get_mut(&workload_id) else {
                        if let Some(waiter) = handoff_waiter {
                            let _ = waiter.send(());
                        }
                        continue;
                    };
                    let valid = deployment
                        .descriptors
                        .get(&shard_id)
                        .map(|descriptor| {
                            (descriptor.shard.worker_id == worker_id || handoff_waiter.is_some())
                                && operator_ids.contains(&descriptor.sink_operator_id)
                        })
                        .unwrap_or(false);
                    let reply =
                        if !valid {
                            None
                        } else {
                            deployment.ready_shards.insert(shard_id);
                            let status = deployment.workers.entry(worker_id).or_insert(
                                WorkerExecutionStatus {
                                    worker_id,
                                    process_id,
                                    shard_ids: Vec::new(),
                                    input_rows: 0,
                                    output_rows: 0,
                                    frontier: worker_frontier,
                                    ready: true,
                                },
                            );
                            status.process_id = process_id;
                            status.frontier = status.frontier.max(worker_frontier);
                            status.ready = true;
                            status.shard_ids.push(shard_id);
                            status.shard_ids.sort();
                            status.shard_ids.dedup();
                            if deployment.ready_shards.len() == deployment.descriptors.len() {
                                let mut workers: Vec<_> =
                                    deployment.workers.values().cloned().collect();
                                workers.sort_by_key(|status| status.worker_id);
                                let reply = if deployment.recovery_required {
                                    workload_recovery_failure()
                                } else {
                                    ControlMessage::DeploymentReady {
                                        workload_id,
                                        workers,
                                    }
                                };
                                deployment.ready_waiter.take().map(|waiter| (waiter, reply))
                            } else {
                                None
                            }
                        };
                    (reply, if valid { handoff_waiter } else { None })
                };
                if let Some(waiter) = handoff_waiter {
                    let _ = waiter.send(());
                }
                if let Some((waiter, reply)) = reply {
                    send_message(&waiter, &reply).await;
                }
            }
            WorkerMessage::SubmitSourceDelta(request) => {
                {
                    let state = data_plane.lock().await;
                    state.row_payloads_received.fetch_add(
                        request.rows.len() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                if connected_worker_id.is_some() {
                    let sender = sender.clone();
                    let catalog = catalog.clone();
                    let worker_senders = worker_senders.clone();
                    let data_plane = data_plane.clone();
                    tokio::spawn(async move {
                        submit_source_delta(
                            request,
                            &sender,
                            &catalog,
                            &worker_senders,
                            &data_plane,
                        )
                        .await;
                    });
                } else {
                    submit_source_delta(request, &sender, &catalog, &worker_senders, &data_plane)
                        .await;
                }
            }
            WorkerMessage::ShuffleDispatchFailed { output, message } => {
                let waiter = {
                    let mut state = data_plane.lock().await;
                    let valid_target = state
                        .deployments
                        .get(&output.workload_id)
                        .and_then(|deployment| deployment.descriptors.get(&output.shard_id))
                        .is_some_and(|descriptor| {
                            descriptor.sink_operator_id == output.operator_id
                                && descriptor.shard.lease_token == output.lease_token
                        });
                    let valid_request =
                        state
                            .source_waiters
                            .get(&output.request_id)
                            .is_some_and(|waiter| {
                                waiter.attempt_id == output.attempt_id
                                    && waiter.workload_id == output.workload_id
                                    && waiter.version == output.version
                                    && waiter.epoch == output.epoch
                                    && waiter.source == output.source
                                    && waiter.expected.contains(&output.shard_id)
                                    && !waiter.received.contains(&output.shard_id)
                                    && connected_worker_id.is_some_and(|worker| {
                                        waiter.dispatchers.get(&output.shard_id) == Some(&worker)
                                    })
                            });
                    if valid_target && valid_request && output.rows.is_empty() {
                        if let Some(deployment) = state.deployments.get_mut(&output.workload_id) {
                            deployment.recovery_required = true;
                        }
                        state.source_waiters.remove(&output.request_id)
                    } else {
                        None
                    }
                };
                if let Some(waiter) = waiter {
                    send_message(
                        &waiter.sender,
                        &workload_failure(format!("shuffle dispatch failed: {message}")),
                    )
                    .await;
                }
            }
            WorkerMessage::ExecutionFailed { output, message } => {
                let assigned_worker = data_plane
                    .lock()
                    .await
                    .deployments
                    .get(&output.workload_id)
                    .and_then(|deployment| deployment.descriptors.get(&output.shard_id))
                    .map(|descriptor| descriptor.shard.worker_id);
                if let (Some(identity), Some(assigned_worker)) = (&peer_identity, assigned_worker) {
                    if identity.role != NodeRole::Worker
                        || connected_worker_id != Some(assigned_worker)
                        || !identity.matches_worker_id(assigned_worker.0)
                    {
                        let registered_id = connected_worker_id
                            .map(|worker_id| worker_id.to_string())
                            .unwrap_or_else(|| "unknown".to_owned());
                        if !deny_authenticated_worker_action(
                            &sender,
                            audit.as_ref(),
                            identity,
                            format!("{}/{}", output.workload_id, output.shard_id),
                            format!(
                                "operation=execution_progress, registered_worker_id={registered_id}, assigned_worker_id={assigned_worker}, error_code={RS_2401}"
                            ),
                        )
                        .await
                        {
                            return;
                        }
                        continue;
                    }
                }
                let waiter = {
                    let mut state = data_plane.lock().await;
                    let valid_owner = state
                        .deployments
                        .get(&output.workload_id)
                        .and_then(|deployment| deployment.descriptors.get(&output.shard_id))
                        .is_some_and(|descriptor| {
                            connected_worker_id == Some(descriptor.shard.worker_id)
                                && descriptor.shard.lease_token == output.lease_token
                                && descriptor.sink_operator_id == output.operator_id
                        });
                    let valid_request =
                        state
                            .source_waiters
                            .get(&output.request_id)
                            .is_some_and(|waiter| {
                                waiter.attempt_id == output.attempt_id
                                    && waiter.workload_id == output.workload_id
                                    && waiter.version == output.version
                                    && waiter.epoch == output.epoch
                                    && waiter.source == output.source
                                    && waiter.expected.contains(&output.shard_id)
                                    && !waiter.received.contains(&output.shard_id)
                            });
                    if valid_owner && valid_request && output.rows.is_empty() {
                        if let Some(deployment) = state.deployments.get_mut(&output.workload_id) {
                            deployment.recovery_required = true;
                        }
                        state.source_waiters.remove(&output.request_id)
                    } else {
                        None
                    }
                };
                if let Some(waiter) = waiter {
                    send_message(
                        &waiter.sender,
                        &workload_failure(format!("worker execution failed: {message}")),
                    )
                    .await;
                }
            }

            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
                let assigned_worker = data_plane
                    .lock()
                    .await
                    .deployments
                    .get(&output.workload_id)
                    .and_then(|deployment| deployment.descriptors.get(&output.shard_id))
                    .map(|descriptor| descriptor.shard.worker_id);
                if let (Some(identity), Some(assigned_worker)) = (&peer_identity, assigned_worker) {
                    if identity.role != NodeRole::Worker
                        || connected_worker_id != Some(assigned_worker)
                        || !identity.matches_worker_id(assigned_worker.0)
                    {
                        let registered_id = connected_worker_id
                            .map(|worker_id| worker_id.to_string())
                            .unwrap_or_else(|| "unknown".to_owned());
                        if !deny_authenticated_worker_action(
                            &sender,
                            audit.as_ref(),
                            identity,
                            format!("{}/{}", output.workload_id, output.shard_id),
                            format!(
                                "operation=execution_progress, registered_worker_id={registered_id}, assigned_worker_id={assigned_worker}, error_code={RS_2401}"
                            ),
                        )
                        .await
                        {
                            return;
                        }
                        continue;
                    }
                }
                let valid = data_plane
                    .lock()
                    .await
                    .deployments
                    .get(&output.workload_id)
                    .and_then(|deployment| deployment.descriptors.get(&output.shard_id))
                    .map(|descriptor| {
                        descriptor.shard.lease_token == output.lease_token
                            && descriptor.sink_operator_id == output.operator_id
                    })
                    .unwrap_or(false);
                if !valid {
                    send_message(
                        &sender,
                        &data_plane_failure("execution progress has a stale fence or operator"),
                    )
                    .await;
                    continue;
                }
                let completion = {
                    let mut state = data_plane.lock().await;
                    let deployment = state.deployments.get_mut(&output.workload_id).unwrap();
                    if deployment.recovery_required {
                        continue;
                    }
                    let worker_id = deployment.descriptors[&output.shard_id].shard.worker_id;
                    if let Some(status) = deployment.workers.get_mut(&worker_id) {
                        status.input_rows += input_rows;
                        status.output_rows += output_rows;
                        status.frontier = status.frontier.max(output.epoch);
                    }
                    let completed = state
                        .source_waiters
                        .get_mut(&output.request_id)
                        .is_some_and(|waiter| waiter.record_progress(&output));
                    if !completed {
                        None
                    } else if let Some(intent) = state.source_waiters[&output.request_id]
                        .owned_intent
                        .clone()
                    {
                        let anchor = state
                            .deployments
                            .get(&intent.workload_id)
                            .and_then(|deployment| deployment.descriptors.get(&intent.anchor_shard))
                            .map(|descriptor| descriptor.shard.worker_id);
                        if let Some(anchor) = anchor {
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            let waiter = state.source_waiters.get_mut(&output.request_id).unwrap();
                            waiter.finishing = true;
                            waiter.finish_reply = Some(sender);
                            Some(SourceCompletion::Owned(anchor, intent, receiver))
                        } else {
                            Some(SourceCompletion::MissingAnchor(
                                output.request_id.clone(),
                                output.workload_id,
                            ))
                        }
                    } else {
                        state
                            .source_waiters
                            .remove(&output.request_id)
                            .map(|waiter| {
                                SourceCompletion::Immediate(
                                    Box::new(waiter),
                                    output.request_id.clone(),
                                )
                            })
                    }
                };
                match completion {
                    Some(SourceCompletion::Immediate(waiter, request_id)) => {
                        send_message(
                            &waiter.sender,
                            &ControlMessage::SourceDeltaCommitted {
                                request_id,
                                epoch: waiter.epoch,
                            },
                        )
                        .await;
                    }
                    Some(SourceCompletion::Owned(anchor, intent, reply)) => {
                        tokio::spawn(finish_owned_source(
                            data_plane.clone(),
                            worker_senders.clone(),
                            anchor,
                            intent,
                            reply,
                        ));
                    }
                    Some(SourceCompletion::MissingAnchor(request_id, workload_id)) => {
                        fail_source_request(
                            &data_plane,
                            &request_id,
                            workload_id,
                            "owned epoch anchor is unavailable",
                        )
                        .await;
                    }
                    None => {}
                }
            }
            WorkerMessage::WorkloadReadReport {
                version,
                request_id,
                workload_id,
                worker_id,
                available,
                shards,
            } => {
                if version == rockstream_types::data_plane::WORKLOAD_SNAPSHOT_VERSION
                    && connected_worker_id == Some(worker_id)
                {
                    let report_target = {
                        let mut state = data_plane.lock().await;
                        state
                            .workload_read_waiters
                            .get_mut(&request_id)
                            .and_then(|waiter| {
                                if waiter.workload_id != workload_id
                                    || waiter.received_workers.contains(&worker_id)
                                {
                                    return None;
                                }
                                let expected = waiter.expected_shards.get(&worker_id)?;
                                let valid = if available {
                                    shards.len() == expected.len()
                                        && shards.iter().all(|shard| {
                                            expected.get(&shard.shard_id).copied()
                                                == shard.lease_token
                                        })
                                } else {
                                    shards.is_empty()
                                };
                                if !valid || !waiter.received_workers.insert(worker_id) {
                                    return None;
                                }
                                Some(waiter.sender.clone())
                            })
                    };
                    if let Some(report_sender) = report_target {
                        let _ = report_sender.send((worker_id, available, shards)).await;
                    }
                }
            }
            WorkerMessage::OperatorStatsReport {
                request_id,
                workload_id,
                worker_id,
                mut stats,
            } => {
                if connected_worker_id == Some(worker_id) {
                    let report_target = {
                        let mut state = data_plane.lock().await;
                        state
                            .operator_stats_waiters
                            .get_mut(&request_id)
                            .and_then(|waiter| {
                                if waiter.workload_id == workload_id
                                    && waiter.expected_workers.contains(&worker_id)
                                    && waiter.received_workers.insert(worker_id)
                                {
                                    Some((waiter.sender.clone(), waiter.operator_ids.clone()))
                                } else {
                                    None
                                }
                            })
                    };
                    if let Some((report_sender, operator_ids)) = report_target {
                        stats.retain(|stat| operator_ids.contains(&stat.operator_id));
                        let _ = report_sender.send((worker_id, stats)).await;
                    }
                }
            }
            WorkerMessage::ReadOperatorStats {
                request_id,
                workload_id,
                operator_ids,
            } => {
                let reply = if connected_worker_id.is_none() {
                    request_operator_stats(
                        request_id,
                        workload_id,
                        operator_ids,
                        &data_plane,
                        &worker_senders,
                    )
                    .await
                } else {
                    ControlMessage::OperatorStatsSnapshot {
                        request_id,
                        available: false,
                        stats: Vec::new(),
                    }
                };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::ReadWorkload {
                version,
                workload_id,
            } => {
                let reply = if connected_worker_id.is_none()
                    && version == rockstream_types::data_plane::WORKLOAD_SNAPSHOT_VERSION
                {
                    request_workload_snapshot(
                        workload_id,
                        &data_plane,
                        &worker_senders,
                        &catalog,
                        &shard_manager,
                    )
                    .await
                } else {
                    data_plane_failure("workload read protocol is unavailable")
                };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::DiscoverShards => {
                let cluster_frontier = frontier.as_ref().and_then(|f| f.cluster_frontier().epoch);
                let shards = shard_manager
                    .leases()
                    .into_iter()
                    .map(|lease| rockstream_types::topology::ShardServingInfo {
                        shard_id: lease.shard_id,
                        remote_prefix: format!("shards/{}", lease.shard_id.0),
                        db_path: "db".to_owned(),
                        worker_id: lease.worker_id,
                        lease_token: lease.lease_token,
                        published_frontier: cluster_frontier,
                    })
                    .collect();
                send_message(
                    &sender,
                    &ControlMessage::ServingTopology {
                        shards,
                        cluster_frontier,
                    },
                )
                .await;
            }
            WorkerMessage::AuthorizeSourceEpoch { metadata } => {
                let reply = authorize_direct_epoch(
                    metadata,
                    &data_plane,
                    &worker_senders,
                    &catalog,
                    &shard_manager,
                )
                .await;
                send_message(&sender, &reply).await;
            }
            WorkerMessage::DirectEpochGrantInstalled {
                worker_id,
                grant_id,
                accepted,
            } => {
                if connected_worker_id == Some(worker_id) {
                    if let Some(waiter) =
                        data_plane.lock().await.direct_grant_waiters.get(&grant_id)
                    {
                        let _ = waiter.try_send((worker_id, accepted));
                    }
                }
            }
            WorkerMessage::GetWorkloadTopology { workload_id } => {
                let reply = {
                    let state = data_plane.lock().await;
                    match state.deployments.get(&workload_id) {
                        Some(deployment) => {
                            let workers = catalog.all_workers();
                            let worker_addrs: HashMap<WorkerId, String> = workers
                                .iter()
                                .map(|w| (w.worker_id, w.address.clone()))
                                .collect();
                            let mut shards: Vec<_> = deployment
                                .descriptors
                                .values()
                                .map(|descriptor| {
                                    let active_lease = shard_manager
                                        .get(descriptor.shard.shard_id)
                                        .unwrap_or_else(|| descriptor.shard.clone());
                                    let worker_address = worker_addrs
                                        .get(&active_lease.worker_id)
                                        .cloned()
                                        .unwrap_or_default();
                                    rockstream_types::data_plane::WorkloadShardPlacement {
                                        shard_id: active_lease.shard_id,
                                        shard_index: descriptor.shard_index,
                                        worker_id: active_lease.worker_id,
                                        worker_address,
                                        lease_token: active_lease.lease_token,
                                        operator_id: descriptor.sink_operator_id,
                                        storage_identity: descriptor.storage_identity.clone(),
                                        frontier: descriptor.frontier,
                                    }
                                })
                                .collect();
                            shards.sort_by_key(|s| (s.shard_index, s.shard_id));
                            ControlMessage::WorkloadTopology {
                                snapshot: rockstream_types::data_plane::WorkloadTopologySnapshot {
                                    topology_epoch: state.topology_epoch.max(1),
                                    workload_id,
                                    plan_json: deployment.request.plan_json.clone(),
                                    routing_columns: deployment.request.routing_columns.clone(),
                                    schemas: deployment.request.schemas.clone(),
                                    shards,
                                    workers,
                                },
                            }
                        }
                        None => data_plane_failure("workload is not deployed"),
                    }
                };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::AcquireConnectorLease {
                connector_id,
                owner_id,
                ttl_ms,
            } => {
                let leases = data_plane.lock().await.connector_leases.clone();
                let reply = match leases.acquire(connector_id, &owner_id, ttl_ms, now_ms()) {
                    Ok(lease) => ControlMessage::ConnectorLeaseGranted { lease },
                    Err((current_owner, message)) => {
                        let code = if message.starts_with("RS-1701") {
                            "RS-1701"
                        } else {
                            "RS-1703"
                        };
                        ControlMessage::ConnectorLeaseDenied {
                            connector_id,
                            current_owner,
                            code: code.to_owned(),
                            message,
                        }
                    }
                };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::RenewConnectorLease {
                connector_id,
                owner_id,
                lease_token,
                ttl_ms,
            } => {
                let leases = data_plane.lock().await.connector_leases.clone();
                let reply =
                    match leases.renew(connector_id, &owner_id, lease_token, ttl_ms, now_ms()) {
                        Ok(lease) => ControlMessage::ConnectorLeaseGranted { lease },
                        Err((current_owner, message)) => {
                            let code = if message.starts_with("RS-1702") {
                                "RS-1702"
                            } else {
                                "RS-1703"
                            };
                            ControlMessage::ConnectorLeaseDenied {
                                connector_id,
                                current_owner,
                                code: code.to_owned(),
                                message,
                            }
                        }
                    };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::ReleaseConnectorLease {
                connector_id,
                owner_id,
                lease_token,
            } => {
                let leases = data_plane.lock().await.connector_leases.clone();
                let released = leases.release(connector_id, &owner_id, lease_token);
                send_message(
                    &sender,
                    &ControlMessage::ConnectorLeaseReleased {
                        connector_id,
                        released,
                    },
                )
                .await;
            }
            WorkerMessage::Heartbeat {
                worker_id,
                capacity_headroom,
            } => {
                if catalog.heartbeat(worker_id, capacity_headroom) {
                    tracing::debug!(
                        %worker_id,
                        headroom = %capacity_headroom,
                        "control: heartbeat"
                    );
                    if let Some(worker) = catalog.get(worker_id) {
                        persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                    }
                } else {
                    tracing::warn!(
                        %worker_id,
                        "control: heartbeat from unknown worker"
                    );
                }
            }
            WorkerMessage::Deregister { worker_id } => {
                let removed = catalog.deregister(worker_id);
                tracing::info!(
                    %worker_id,
                    found = removed.is_some(),
                    "control: worker deregistered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.deregistered", worker_id.to_string());
                    let _ = aud.append(&event);
                }
                delete_worker_if_needed(topology_store.as_ref(), worker_id).await;
                // Release all shard leases held by this worker.
                let freed = shard_manager.release_worker(worker_id);
                if !freed.is_empty() {
                    tracing::info!(
                        %worker_id,
                        freed_shards = freed.len(),
                        "control: released shard leases on deregister"
                    );
                    if let Some(aud) = &audit {
                        let event = AuditEvent::now(
                            "control",
                            "worker.shards_released",
                            worker_id.to_string(),
                        )
                        .with_detail(format!("freed_shards={}", freed.len()));
                        let _ = aud.append(&event);
                    }
                    if let Some(store) = &shard_store {
                        persist_shard_state(&shard_manager, store).await;
                    }
                    // Notify about shard revocations.
                    for shard_id in freed {
                        let revoke = ControlMessage::ShardRevoked {
                            shard_id,
                            reason: ShardRevokeReason::WorkerDead,
                        };
                        send_message(&sender, &revoke).await;
                    }
                }
                // Notify remaining workers about topology change.
                let notify = topology_changed_message(&catalog, &shard_manager);
                broadcast_message(&worker_senders, notify).await;
            }
            WorkerMessage::RequestShard {
                worker_id,
                shard_id,
            } => {
                if !catalog
                    .get(worker_id)
                    .map(|worker| worker.lifecycle.is_active())
                    .unwrap_or(false)
                {
                    let reply = ControlMessage::OperationFailed {
                        code: RS_3604.to_string(),
                        message: format!(
                            "worker {worker_id} cannot receive shard {shard_id} while draining or decommissioned"
                        ),
                        next_steps: "Wait for the drain to complete or target an active worker instead."
                            .to_string(),
                    };
                    send_message(&sender, &reply).await;
                    continue;
                }
                let requested_worker = catalog.get(worker_id).expect("worker was checked above");
                let healthy_workers = catalog.healthy_workers();
                if !rockstream_types::topology::assignment_compatible(
                    &healthy_workers,
                    requested_worker.protocol_range.max,
                    requested_worker.storage_format_range.max,
                ) {
                    tracing::warn!(
                        %worker_id,
                        %shard_id,
                        protocol = %requested_worker.protocol_range.max,
                        storage_format = %requested_worker.storage_format_range.max,
                        "control: assignment withheld by compatibility floor"
                    );
                    if let Some(aud) = &audit {
                        let event = AuditEvent::now(
                            "control",
                            "assignment.compatibility_withheld",
                            shard_id.to_string(),
                        )
                        .with_detail(format!(
                            "worker={}, protocol={}, storage_format={}",
                            worker_id,
                            requested_worker.protocol_range.max,
                            requested_worker.storage_format_range.max
                        ));
                        let _ = aud.append(&event);
                    }
                    let reply = ControlMessage::OperationFailed {
                        code: rockstream_types::error_code::RS_5021.to_string(),
                        message: format!(
                            "assignment withheld: worker {worker_id} requires protocol {} and storage format {}, but the affected workers do not meet that compatibility floor",
                            requested_worker.protocol_range.max,
                            requested_worker.storage_format_range.max
                        ),
                        next_steps: rockstream_types::error_code::next_steps(
                            rockstream_types::error_code::RS_5021,
                        )
                        .to_string(),
                    };
                    send_message(&sender, &reply).await;
                    continue;
                }
                // M7-S2 leader-only write gate: a shard lease grant is a
                // control-plane write and must only be accepted while this
                // node is the Raft-elected leader.
                if let Some(rft) = &raft {
                    if rft.require_leader().is_err() {
                        tracing::warn!(
                            %worker_id,
                            %shard_id,
                            "control: shard lease request rejected — not leader"
                        );
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "shard.lease_rejected_not_leader",
                                shard_id.to_string(),
                            )
                            .with_detail(format!("worker={worker_id}"));
                            let _ = aud.append(&event);
                        }
                        let reply = ControlMessage::NotLeader {
                            current_leader: rft.current_leader(),
                        };
                        send_message(&sender, &reply).await;
                        continue;
                    }
                    // v0.45.2 M7-S4/S5: this node is confirmed leader — make
                    // sure its ShardManager reflects the shared store's
                    // latest state before minting any lease this term.
                    if let Some(store) = &shard_store {
                        let epoch = rft
                            .leader_epoch()
                            .expect("require_leader() succeeded above, so this node is Leader");
                        ensure_shard_state_synced(&shard_manager, store, &synced_epoch, epoch)
                            .await;
                    }
                }
                match shard_manager.acquire(shard_id, worker_id) {
                    Ok(lease) => {
                        tracing::info!(
                            %worker_id,
                            %shard_id,
                            token = lease.lease_token.0,
                            "control: shard lease granted"
                        );
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "shard.lease_granted",
                                shard_id.to_string(),
                            )
                            .with_detail(format!(
                                "worker={}, token={}",
                                worker_id, lease.lease_token
                            ));
                            let _ = aud.append(&event);
                        }
                        if let Some(store) = &shard_store {
                            persist_shard_state(&shard_manager, store).await;
                        }
                        broadcast_message(
                            &worker_senders,
                            topology_changed_message(&catalog, &shard_manager),
                        )
                        .await;
                        let reply = ControlMessage::ShardAssigned {
                            lease,
                            operation_id: None,
                        };
                        send_message(&sender, &reply).await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            %worker_id,
                            %shard_id,
                            error = %e,
                            "control: shard lease denied"
                        );
                        // Connection close signals the denial to the worker.
                    }
                }
            }
            WorkerMessage::FenceWrite {
                shard_id,
                lease_token,
            } => {
                // v0.45.2 M7-S4/S5: sync before answering a fence check too,
                // so a freshly-promoted leader doesn't wrongly report a
                // genuinely-still-valid lease as invalid just because its
                // own in-memory map hasn't caught up yet.
                if let (Some(rft), Some(store)) = (&raft, &shard_store) {
                    if let Some(epoch) = rft.leader_epoch() {
                        ensure_shard_state_synced(&shard_manager, store, &synced_epoch, epoch)
                            .await;
                    }
                }
                let connection_owns_shard = peer_identity.as_ref().is_none_or(|identity| {
                    identity.role == NodeRole::Worker
                        && connected_worker_id.is_some_and(|worker_id| {
                            identity.matches_worker_id(worker_id.0)
                                && shard_manager
                                    .get(shard_id)
                                    .is_some_and(|lease| lease.worker_id == worker_id)
                        })
                });
                if !connection_owns_shard {
                    if let Some(identity) = &peer_identity {
                        let registered_id = connected_worker_id
                            .map(|worker_id| worker_id.to_string())
                            .unwrap_or_else(|| "unknown".to_owned());
                        let event = AuditEvent::now(
                            identity.to_cn(),
                            "security.authorization_denied",
                            shard_id.to_string(),
                        )
                        .with_detail(format!(
                            "operation=fence_write, registered_worker_id={registered_id}, error_code={RS_2401}"
                        ));
                        if !append_required_security_audit(
                            audit.as_ref(),
                            &event,
                            "fence check identity denial",
                        ) {
                            return;
                        }
                    }
                }
                // A partitioned former leader must never certify a stale
                // lease. Only a live Raft quorum authority may fence writes.
                let valid = connection_owns_shard
                    && raft
                        .as_ref()
                        .is_none_or(|node| node.require_leader().is_ok())
                    && shard_manager.is_valid_writer(shard_id, lease_token);
                tracing::debug!(
                    %shard_id,
                    token = lease_token.0,
                    valid,
                    "control: fence write check"
                );
                let reply = ControlMessage::FenceAck { shard_id, valid };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::DrainAck {
                worker_id,
                shards_remaining,
            } => {
                if connected_worker_id != Some(worker_id) {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: "RS-2401".to_owned(),
                            message: "drain acknowledgement worker does not match this connection".to_owned(),
                            next_steps: "Send drain acknowledgements only on the registered worker connection.".to_owned(),
                        },
                    )
                    .await;
                    continue;
                }
                tracing::info!(
                    %worker_id,
                    shards_remaining,
                    "control: drain ack received"
                );
                if shards_remaining == 0 {
                    drain_state.lock().await.acked_workers.insert(worker_id);
                    process_drain_queue(
                        &catalog,
                        &shard_manager,
                        &data_plane,
                        audit.as_ref(),
                        shard_store.as_ref(),
                        topology_store.as_ref(),
                        &drain_state,
                        &worker_senders,
                        worker_id,
                        &migration_waiters,
                        operation_store.as_ref(),
                    )
                    .await;
                    cleanup_decommissioned_workers(&catalog, topology_store.as_ref()).await;
                } else {
                    let state = WorkerLifecycleState::draining(shards_remaining, now_ms());
                    if let Some(worker) = catalog.set_lifecycle(worker_id, state) {
                        persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                    }
                }
            }
            WorkerMessage::ShardTransferAck {
                operation_id,
                stage,
                worker_id,
                shard_id,
                lease_token,
                success,
                error,
            } => {
                if connected_worker_id != Some(worker_id) {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: "RS-2401".to_owned(),
                            message: "shard transfer acknowledgement worker does not match this connection".to_owned(),
                            next_steps: "Send migration acknowledgements only on the registered worker connection.".to_owned(),
                        },
                    )
                    .await;
                    continue;
                }
                let key = format!("{operation_id}:{stage}");
                if let Some(waiter) = migration_waiters.lock().await.remove(&key) {
                    let _ = waiter.send(WorkerMessage::ShardTransferAck {
                        operation_id,
                        stage,
                        worker_id,
                        shard_id,
                        lease_token,
                        success,
                        error,
                    });
                }
            }
            WorkerMessage::ShardCheckpointAck {
                request_id,
                checkpoint_id,
                worker_id,
                shard_id,
                lease_token,
                shard_checkpoint_id,
                snapshot_id,
                error,
            } => {
                if connected_worker_id != Some(worker_id) {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: "RS-2401".to_owned(),
                            message: "shard checkpoint acknowledgement worker does not match this connection".to_owned(),
                            next_steps: "Send checkpoint acknowledgements only on the registered worker connection.".to_owned(),
                        },
                    )
                    .await;
                    continue;
                }
                if let Some(waiter) = backup_waiters.lock().await.remove(&request_id) {
                    let _ = waiter.send(WorkerMessage::ShardCheckpointAck {
                        request_id,
                        checkpoint_id,
                        worker_id,
                        shard_id,
                        lease_token,
                        shard_checkpoint_id,
                        snapshot_id,
                        error,
                    });
                }
            }
            WorkerMessage::LifecycleState { worker_id, state } => {
                if connected_worker_id != Some(worker_id) {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: "RS-2401".to_owned(),
                            message: "lifecycle update worker does not match this connection"
                                .to_owned(),
                            next_steps:
                                "Send lifecycle updates only on the registered worker connection."
                                    .to_owned(),
                        },
                    )
                    .await;
                    continue;
                }
                if matches!(
                    state,
                    WorkerLifecycleState::Decommissioned { .. }
                        | WorkerLifecycleState::Draining {
                            shards_remaining: 0,
                            ..
                        }
                ) {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: "RS-3604".to_owned(),
                            message: "only the control plane can complete a drain after shard handoff".to_owned(),
                            next_steps: "Request a drain and wait for the control plane to transfer every shard lease.".to_owned(),
                        },
                    )
                    .await;
                    continue;
                }
                tracing::info!(
                    %worker_id,
                    state = ?state,
                    "control: worker lifecycle state update"
                );
                if let Some(worker) = catalog.set_lifecycle(worker_id, state) {
                    persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                }
            }
            WorkerMessage::RoutingEpochCommitted {
                worker_id,
                workload_id,
                anchor_shard_id,
                anchor_lease_token,
                epoch,
                request_id,
                input_digest,
                shard_leases,
                ..
            } => {
                if connected_worker_id != Some(worker_id) || shard_leases.len() > 4096 {
                    continue;
                }
                let completion = {
                    let mut state = data_plane.lock().await;
                    routing_completion_members(
                        &state,
                        &shard_manager,
                        workload_id,
                        worker_id,
                        anchor_shard_id,
                        anchor_lease_token,
                        &shard_leases,
                    )
                    .map(|members| {
                        let metadata = state
                            .direct_epochs
                            .get(&workload_id)
                            .filter(|pending| {
                                pending.request_id == request_id
                                    && pending.epoch == epoch
                                    && pending.input_digest == input_digest
                            })
                            .cloned();
                        let revoke = metadata
                            .and_then(|metadata| record_direct_completion(&mut state, metadata));
                        (members, revoke)
                    })
                };
                if let Some((members, Some(metadata))) = completion {
                    let targets = members
                        .keys()
                        .filter_map(|id| shard_manager.get(*id).map(|lease| lease.worker_id))
                        .collect();
                    revoke_direct_grants(&metadata, None, &targets, &worker_senders).await;
                    let mut state = data_plane.lock().await;
                    release_completed_direct_reservation(&mut state, &metadata);
                }
            }
            WorkerMessage::ShardLoadReport { worker_id, samples } => {
                tracing::debug!(
                    %worker_id,
                    sample_count = samples.len(),
                    "control: shard load report received"
                );
            }
            WorkerMessage::ResolveSecretToken { secret_name } => {
                let actor = peer_identity
                    .as_ref()
                    .map_or(AuditIdentity::Unknown, |identity| {
                        AuditIdentity::Verified(identity.to_cn())
                    });
                let audited = append_gateway_delegation_audit(
                    security_catalog.as_ref(),
                    actor,
                    AuditField::Known(secret_name),
                    AuditField::Unknown,
                    SecurityAuditClass::AuthorizationDenial,
                    SecurityAuditOutcome::Denied,
                    AuditField::NotApplicable,
                )
                .await;
                let code = if audited { RS_2401 } else { RS_0003 };
                send_message(
                    &sender,
                    &ControlMessage::OperationFailed {
                        code: code.to_string(),
                        message: "unscoped worker secret requests are disabled".to_owned(),
                        next_steps: rockstream_types::error_code::next_steps(code).to_string(),
                    },
                )
                .await;
            }
            WorkerMessage::ResolveWorkloadSecretToken {
                workload_id,
                secret_name,
            } => {
                let Some(identity) = peer_identity.as_ref().filter(|id| {
                    id.role == NodeRole::Worker
                        && connected_worker_id
                            .is_some_and(|worker_id| id.matches_worker_id(worker_id.0))
                }) else {
                    let actor = peer_identity
                        .as_ref()
                        .map_or(AuditIdentity::Unknown, |identity| {
                            AuditIdentity::Verified(identity.to_cn())
                        });
                    let audited = append_gateway_delegation_audit(
                        security_catalog.as_ref(),
                        actor,
                        AuditField::Known(secret_name),
                        AuditField::Known(format!("workload:{}", workload_id.0)),
                        SecurityAuditClass::AuthenticationFailure,
                        SecurityAuditOutcome::Denied,
                        AuditField::NotApplicable,
                    )
                    .await;
                    let code = if audited { RS_2410 } else { RS_0003 };
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: code.to_string(),
                            message:
                                "secret token requests require the assigned worker certificate"
                                    .to_string(),
                            next_steps: rockstream_types::error_code::next_steps(code).to_string(),
                        },
                    )
                    .await;
                    continue;
                };
                let Some(worker_id) = connected_worker_id else {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: RS_2410.to_string(),
                            message: "worker registration is not complete".to_owned(),
                            next_steps: rockstream_types::error_code::next_steps(RS_2410)
                                .to_string(),
                        },
                    )
                    .await;
                    continue;
                };
                let authorized = match authorize_worker_secret_request(
                    security_catalog.as_ref(),
                    &data_plane,
                    worker_id,
                    workload_id,
                    &secret_name,
                )
                .await
                {
                    Ok(authorized) => authorized,
                    Err(error) => {
                        let (code, message, action, retry, audit_actor, audit_scope, revision) =
                            match error {
                                WorkerSecretAuthorizationError::Denied(message) => (
                                    RS_2401,
                                    message,
                                    SecurityAuditClass::AuthorizationDenial,
                                    false,
                                    AuditIdentity::Verified(identity.to_cn()),
                                    AuditField::Known(format!("workload:{}", workload_id.0)),
                                    AuditField::NotApplicable,
                                ),
                                WorkerSecretAuthorizationError::DeniedForActor {
                                    message,
                                    actor,
                                    scope,
                                    policy_revision,
                                } => (
                                    RS_2401,
                                    message,
                                    SecurityAuditClass::AuthorizationDenial,
                                    false,
                                    AuditIdentity::Verified(actor.to_string()),
                                    AuditField::Known(format!("{scope:?}")),
                                    policy_revision,
                                ),
                                WorkerSecretAuthorizationError::Unavailable(message) => (
                                    RS_0003,
                                    message,
                                    SecurityAuditClass::AuthenticationFailure,
                                    true,
                                    AuditIdentity::Verified(identity.to_cn()),
                                    AuditField::Known(format!("workload:{}", workload_id.0)),
                                    AuditField::NotApplicable,
                                ),
                            };
                        let audited = append_gateway_delegation_audit(
                            security_catalog.as_ref(),
                            audit_actor,
                            AuditField::Known(secret_name.clone()),
                            audit_scope,
                            action,
                            SecurityAuditOutcome::Denied,
                            revision,
                        )
                        .await;
                        let code = if audited { code } else { RS_0003 };
                        send_message(
                            &sender,
                            &ControlMessage::SecretTokenRejected {
                                workload_id,
                                secret_name: secret_name.clone(),
                                code: code.to_string(),
                                message: message.to_owned(),
                                retry: retry || !audited,
                            },
                        )
                        .await;
                        continue;
                    }
                };
                match secret_store
                    .issue_worker_token(
                        authorized.namespace_id,
                        workload_id,
                        &secret_name,
                        &identity.node_id,
                        300,
                        &identity.to_cn(),
                    )
                    .await
                {
                    Ok(token) => {
                        let actor = authorized.actor_principal_id.map_or_else(
                            || AuditIdentity::Verified(identity.to_cn()),
                            |principal| AuditIdentity::Verified(principal.to_string()),
                        );
                        if append_gateway_delegation_audit(
                            security_catalog.as_ref(),
                            actor,
                            AuditField::Known(secret_name.clone()),
                            AuditField::Known(format!("{:?}", authorized.scope)),
                            SecurityAuditClass::AuthenticationSuccess,
                            SecurityAuditOutcome::Success,
                            authorized.policy_revision,
                        )
                        .await
                        {
                            send_message(&sender, &ControlMessage::SecretTokenIssued { token })
                                .await;
                        } else {
                            send_message(
                                &sender,
                                &ControlMessage::SecretTokenRejected {
                                    workload_id,
                                    secret_name: secret_name.clone(),
                                    code: RS_0003.to_string(),
                                    message: "secret authorization audit is unavailable".into(),
                                    retry: true,
                                },
                            )
                            .await;
                        }
                    }
                    Err(error) => {
                        let reply = ControlMessage::SecretTokenRejected {
                            workload_id,
                            secret_name: secret_name.clone(),
                            code: error_code_for_secret_error(&error),
                            message: error.to_string(),
                            retry: true,
                        };
                        send_message(&sender, &reply).await;
                    }
                }
            }
            WorkerMessage::ClusterStatusQuery => {
                let reply = if let Some(rft) = &raft {
                    ControlMessage::ClusterStatusReport {
                        node_id: Some(rft.node_id()),
                        role: raft_role_wire(rft.role()),
                        term: rft.current_term(),
                    }
                } else {
                    // Pre-v0.45.2 single-node control mode: this node is
                    // implicitly the (only) writer — there is no group to
                    // contend leadership with.
                    ControlMessage::ClusterStatusReport {
                        node_id: None,
                        role: rockstream_types::topology::RaftRoleWire::NoRaft,
                        term: 0,
                    }
                };
                send_message(&sender, &reply).await;
            }
            WorkerMessage::ReportShardFrontier { shard_id, epoch } => {
                if let Some(agg) = &frontier {
                    let authorized = connected_worker_id.is_some_and(|worker_id| {
                        shard_manager.get(shard_id).is_some_and(|lease| {
                            lease.worker_id == worker_id
                                && shard_manager.is_valid_writer(shard_id, lease.lease_token)
                        })
                    });
                    if !authorized {
                        tracing::warn!(
                            %shard_id,
                            epoch,
                            "control: frontier report lacks the current shard lease"
                        );
                        send_message(
                            &sender,
                            &ControlMessage::OperationFailed {
                                code: RS_8004.to_string(),
                                message: "frontier report lacks the current shard lease".to_owned(),
                                next_steps: rockstream_types::error_code::next_steps(RS_8004)
                                    .to_string(),
                            },
                        )
                        .await;
                        continue;
                    }
                    if let Err(e) = agg
                        .ingest(rockstream_types::frontier::ShardFrontierReport { shard_id, epoch })
                    {
                        tracing::warn!(%shard_id, epoch, error = %e, "control: frontier ingest failed");
                        send_message(
                            &sender,
                            &ControlMessage::OperationFailed {
                                code: RS_8004.to_string(),
                                message: e.to_string(),
                                next_steps: rockstream_types::error_code::next_steps(RS_8004)
                                    .to_string(),
                            },
                        )
                        .await;
                        continue;
                    }
                }
                // v0.45.2 M7-S4: only the current leader "publishes" — a
                // non-leader control node still ingested the report above
                // (so the meet computation is already warm once it becomes
                // leader), but must not claim authority over the cluster
                // frontier it does not currently hold.
                let is_leader = match &raft {
                    Some(rft) => rft.require_leader().is_ok(),
                    None => true,
                };
                if is_leader {
                    if let Some(agg) = &frontier {
                        let published = agg.cluster_frontier().epoch;
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "frontier.published",
                                shard_id.to_string(),
                            )
                            .with_detail(format!("epoch={epoch}"));
                            let _ = aud.append(&event);
                        }
                        let reply = ControlMessage::ClusterFrontierAdvanced {
                            epoch: published.unwrap_or(0),
                        };
                        send_message(&sender, &reply).await;
                    }
                } else if let Some(rft) = &raft {
                    let reply = ControlMessage::NotLeader {
                        current_leader: rft.current_leader(),
                    };
                    send_message(&sender, &reply).await;
                }
            }
            WorkerMessage::ReportShardFrontierV2 { report } => {
                let shard_id = report.shard_id;
                let epoch = report.epoch;
                let authorized = connected_worker_id.is_some_and(|worker_id| {
                    shard_manager.get(shard_id).is_some_and(|lease| {
                        lease.worker_id == worker_id
                            && lease.lease_token == report.lease_token
                            && shard_manager.is_valid_writer(shard_id, report.lease_token)
                    })
                });
                if !authorized {
                    tracing::warn!(
                        %shard_id,
                        epoch,
                        "control: membership frontier report lacks the current shard lease"
                    );
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: RS_8004.to_string(),
                            message: "membership frontier report lacks the current shard lease"
                                .to_owned(),
                            next_steps: rockstream_types::error_code::next_steps(RS_8004)
                                .to_string(),
                        },
                    )
                    .await;
                    continue;
                }
                if let Some(agg) = &frontier {
                    if let Err(e) = agg.ingest_membership_report(report) {
                        tracing::warn!(%shard_id, epoch, error = %e, "control: membership frontier ingest failed");
                        send_message(
                            &sender,
                            &ControlMessage::OperationFailed {
                                code: RS_8004.to_string(),
                                message: e.to_string(),
                                next_steps: rockstream_types::error_code::next_steps(RS_8004)
                                    .to_string(),
                            },
                        )
                        .await;
                        continue;
                    }
                }
                let is_leader = match &raft {
                    Some(rft) => rft.require_leader().is_ok(),
                    None => true,
                };
                if is_leader {
                    if let Some(agg) = &frontier {
                        let published = agg.cluster_frontier().epoch;
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "frontier.published",
                                shard_id.to_string(),
                            )
                            .with_detail(format!("epoch={epoch},membership_aware=true"));
                            let _ = aud.append(&event);
                        }
                        let reply = ControlMessage::ClusterFrontierAdvanced {
                            epoch: published.unwrap_or(0),
                        };
                        send_message(&sender, &reply).await;
                    }
                } else if let Some(rft) = &raft {
                    let reply = ControlMessage::NotLeader {
                        current_leader: rft.current_leader(),
                    };
                    send_message(&sender, &reply).await;
                }
            }
            WorkerMessage::RequestDrain { worker_id } => {
                let worker_sender = if connected_worker_id == Some(worker_id) {
                    Some(sender.clone())
                } else {
                    worker_senders.lock().await.get(&worker_id).cloned()
                };
                let Some(worker_sender) = worker_sender else {
                    send_message(
                        &sender,
                        &ControlMessage::OperationFailed {
                            code: RS_3610.to_string(),
                            message: format!("worker {worker_id} has no active control connection"),
                            next_steps:
                                "Wait for the worker to reconnect, then retry the drain request."
                                    .to_owned(),
                        },
                    )
                    .await;
                    continue;
                };
                match request_worker_drain(
                    &catalog,
                    &shard_manager,
                    audit.as_ref(),
                    topology_store.as_ref(),
                    &drain_state,
                    worker_id,
                    None,
                )
                .await
                {
                    Ok((state, queue_fill, queue_capacity, request)) => {
                        send_message(&worker_sender, &ControlMessage::BeginDrain(request)).await;
                        let status = ControlMessage::DrainStatus {
                            worker_id,
                            state,
                            queue_fill,
                            queue_capacity,
                        };
                        send_message(&sender, &status).await;
                    }
                    Err(err) => {
                        let reply = drain_failure_message(err);
                        send_message(&sender, &reply).await;
                    }
                }
            }
        }

        cleanup_decommissioned_workers(&catalog, topology_store.as_ref()).await;

        if rotation_rx.has_changed().unwrap_or(false) {
            let rotation = {
                let current = rotation_rx.borrow_and_update();
                current.clone()
            };
            if let Some(rotation) = rotation {
                if let Some(worker_id) = connected_worker_id {
                    match workloads_for_secret_rotation(
                        security_catalog.as_ref(),
                        &data_plane,
                        worker_id,
                        &rotation.secret_name,
                    )
                    .await
                    {
                        Ok(workloads) => {
                            for workload_id in workloads {
                                send_message(
                                    &sender,
                                    &ControlMessage::SecretRefreshRequired {
                                        workload_id,
                                        secret_name: rotation.secret_name.clone(),
                                    },
                                )
                                .await;
                            }
                        }
                        Err(error) => tracing::error!(
                            code = %rockstream_types::error_code::RS_2425,
                            worker_id = %worker_id,
                            secret_name = %rotation.secret_name,
                            ?error,
                            "secret rotation could not be scoped to active workloads"
                        ),
                    }
                }
            }
        }
    }

    // A dropped worker session is a death signal: remove it from live topology
    // and reassign its fenced shard leases to the remaining workers.
    if let Some(worker_id) = connected_worker_id {
        if let Some(aud) = &audit {
            let _ = aud.append(&AuditEvent::now(
                "control",
                "worker.disconnected",
                worker_id.to_string(),
            ));
        }
        worker_senders.lock().await.remove(&worker_id);
        let interrupted = {
            let mut state = data_plane.lock().await;
            state.owned_shuffle_workers.remove(&worker_id);
            let requests: Vec<_> = state
                .source_waiters
                .iter()
                .filter(|(_, waiter)| {
                    waiter
                        .dispatchers
                        .values()
                        .any(|source| *source == worker_id)
                        || state
                            .deployments
                            .get(&waiter.workload_id)
                            .is_some_and(|deployment| {
                                waiter.expected.iter().any(|shard| {
                                    deployment.descriptors.get(shard).is_some_and(|descriptor| {
                                        descriptor.shard.worker_id == worker_id
                                    })
                                })
                            })
                })
                .map(|(request_id, _)| request_id.clone())
                .collect();
            let mut interrupted = Vec::new();
            for request_id in requests {
                if let Some(waiter) = state.source_waiters.remove(&request_id) {
                    if let Some(deployment) = state.deployments.get_mut(&waiter.workload_id) {
                        deployment.recovery_required = true;
                    }
                    interrupted.push(waiter.sender);
                }
            }
            interrupted
        };
        for recipient in interrupted {
            send_message(
                &recipient,
                &workload_failure("worker disconnected before source delta completed"),
            )
            .await;
        }
        let scheduler = ShardScheduler::new(catalog.clone(), shard_manager.clone());
        catalog.deregister(worker_id);
        let assignments = scheduler.on_worker_dead(worker_id).unwrap_or_default();
        if !assignments.is_empty() {
            tracing::info!(
                %worker_id,
                reassigned_shards = assignments.len(),
                "control: reassigned shard leases on disconnect"
            );
            if let Some(aud) = &audit {
                let event = AuditEvent::now(
                    "control",
                    "worker.shards_released_on_disconnect",
                    worker_id.to_string(),
                )
                .with_detail(format!("reassigned_shards={}", assignments.len()));
                let _ = aud.append(&event);
            }
            if let Some(store) = &shard_store {
                persist_shard_state(&shard_manager, store).await;
            }
        }
        for assignment in assignments {
            let replacement = {
                let mut state = data_plane.lock().await;
                state.deployments.values_mut().find_map(|deployment| {
                    deployment
                        .descriptors
                        .get_mut(&assignment.lease.shard_id)
                        .map(|descriptor| {
                            descriptor.shard = assignment.lease.clone();
                            deployment.ready_shards.remove(&assignment.lease.shard_id);
                            deployment.workers.remove(&worker_id);
                            descriptor.clone()
                        })
                })
            };
            if let Some(target) = worker_senders
                .lock()
                .await
                .get(&assignment.lease.worker_id)
                .cloned()
            {
                send_message(
                    &target,
                    &ControlMessage::ShardAssigned {
                        lease: assignment.lease,
                        operation_id: None,
                    },
                )
                .await;
                if let Some(descriptor) = replacement {
                    send_message(&target, &ControlMessage::Deploy { descriptor }).await;
                }
            }
        }
        broadcast_message(
            &worker_senders,
            topology_changed_message(&catalog, &shard_manager),
        )
        .await;
    }

    drop(sender);
    let _ = writer_task.await;
    tracing::debug!(%peer, "control: connection closed");
}

async fn send_message(sender: &mpsc::Sender<ControlMessage>, msg: &ControlMessage) {
    if sender.send(msg.clone()).await.is_err() {
        tracing::warn!("control: connection writer closed");
    }
}

fn topology_changed_message(
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
) -> ControlMessage {
    ControlMessage::TopologyChanged {
        workers: catalog.healthy_workers(),
        shard_leases: shard_manager.leases(),
    }
}

async fn broadcast_message(
    senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    message: ControlMessage,
) {
    let senders: Vec<_> = senders.lock().await.values().cloned().collect();
    for sender in senders {
        send_message(&sender, &message).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::ShardManager;
    use crate::topology::TopologyCatalog;
    use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, RaftRoleWire, WorkerCapabilities, WorkerLifecycleState,
        WorkerLocation, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    fn direct_lifecycle_fixture() -> (
        DataPlaneState,
        ShardManager,
        TopologyCatalog,
        DirectEpochGrant,
    ) {
        let manager = ShardManager::new();
        let catalog = TopologyCatalog::new();
        let request = DeploymentRequest {
            version: rockstream_types::data_plane::DEPLOYMENT_DESCRIPTOR_VERSION,
            workload_id: WorkloadId(3),
            namespace: "public".into(),
            workload_name: "direct".into(),
            plan_json: "{}".into(),
            join_strategy: Default::default(),
            schemas: Vec::new(),
            frontier: 0,
            storage_root: "test".into(),
            sink_operator_id: rockstream_types::ids::OperatorId(1),
            output_columns: Vec::new(),
            primary_key: vec![0],
            merge_key_columns: Vec::new(),
            routing_columns: BTreeMap::new(),
        };
        let mut descriptors = HashMap::new();
        let mut members = BTreeMap::new();
        for id in [1, 2] {
            catalog.register(&WorkerRegistration::new(
                WorkerId(id),
                NodeRole::Worker,
                format!("127.0.0.1:{}", 9000 + id),
                CapacityHeadroom::new(0.8),
            ));
            let lease = manager.acquire(ShardId(id), WorkerId(id)).unwrap();
            members.insert(lease.shard_id, lease.lease_token);
            descriptors.insert(
                lease.shard_id,
                DeploymentDescriptor::with_shard_index(
                    request.clone(),
                    lease,
                    format!("test/{id}"),
                    (id - 1) as u32,
                ),
            );
        }
        let mut state = DataPlaneState::default();
        state.deployments.insert(
            request.workload_id,
            DeploymentState {
                request,
                actor: None,
                secret_names: Vec::new(),
                descriptors,
                workers: HashMap::new(),
                ready_shards: members.keys().copied().collect(),
                ready_waiter: None,
                recovery_required: false,
            },
        );
        let grant = DirectEpochGrant {
            grant_id: uuid::Uuid::from_u128(1),
            metadata: SourceEpochMetadata {
                version: rockstream_types::data_plane::DEPLOYMENT_DESCRIPTOR_VERSION,
                workload_id: WorkloadId(3),
                source: "items".into(),
                request_id: "direct-1".into(),
                epoch: 7,
                input_digest: [9; 32],
            },
            expires_at_ms: 100,
            members,
        };
        (state, manager, catalog, grant)
    }

    #[test]
    fn equal_deadline_renewals_have_distinct_install_owners_and_stale_rollback_is_inert() {
        let (mut state, _, _, first) = direct_lifecycle_fixture();
        let (waiter, _) = mpsc::channel(2);
        assert_eq!(
            reserve_direct_grant(&mut state, &first, waiter.clone(), 10),
            Ok(None)
        );
        let mut second = first.clone();
        second.grant_id = uuid::Uuid::from_u128(2);
        assert_eq!(
            reserve_direct_grant(&mut state, &second, waiter.clone(), 10),
            Err("RS-3004: workload has an active read, write, or ownership handoff")
        );
        // The first install has returned successfully, leaving its live pin.
        state.direct_installing.remove(&first.metadata.workload_id);
        state.direct_grant_waiters.remove(&first.grant_id);
        assert_eq!(
            reserve_direct_grant(&mut state, &second, waiter, 10),
            Ok(Some((first.metadata.clone(), Some(100))))
        );
        restore_failed_direct_reservation(&mut state, &first, None);
        assert_eq!(
            (
                state.direct_epochs.clone(),
                state.direct_epoch_deadlines.clone(),
                state.direct_installing.clone()
            ),
            (
                HashMap::from([(WorkloadId(3), second.metadata.clone())]),
                HashMap::from([(WorkloadId(3), 100)]),
                HashMap::from([(WorkloadId(3), second.grant_id)]),
            )
        );
        restore_failed_direct_reservation(
            &mut state,
            &second,
            Some((first.metadata.clone(), Some(100))),
        );
        assert_eq!(
            (
                state.direct_epochs,
                state.direct_epoch_deadlines,
                state.direct_installing
            ),
            (
                HashMap::from([(WorkloadId(3), first.metadata)]),
                HashMap::from([(WorkloadId(3), 100)]),
                HashMap::new()
            )
        );
    }

    #[tokio::test]
    async fn completion_during_install_keeps_acks_live_revokes_tentative_grants_and_replays_receipt(
    ) {
        let (state, manager, catalog, fixture) = direct_lifecycle_fixture();
        let data_plane = Arc::new(AsyncMutex::new(state));
        let mut receivers = Vec::new();
        let mut senders = HashMap::new();
        for id in [WorkerId(1), WorkerId(2)] {
            let (sender, receiver) = mpsc::channel(4);
            senders.insert(id, sender);
            receivers.push((id, receiver));
        }
        let worker_senders = Arc::new(AsyncMutex::new(senders));
        let authorize = tokio::spawn({
            let metadata = fixture.metadata.clone();
            let data_plane = data_plane.clone();
            let worker_senders = worker_senders.clone();
            let manager = manager.clone();
            let catalog = catalog.clone();
            async move {
                authorize_direct_epoch(metadata, &data_plane, &worker_senders, &catalog, &manager)
                    .await
            }
        });
        let mut installed = Vec::new();
        for (_, receiver) in &mut receivers {
            let message = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
                .await
                .unwrap()
                .unwrap();
            let ControlMessage::InstallDirectEpochGrant { grant } = message else {
                panic!("unexpected authorization message: {message:?}");
            };
            installed.push(grant);
        }
        let grant = installed[0].clone();
        assert_eq!(installed, vec![grant.clone(), grant.clone()]);
        {
            let mut state = data_plane.lock().await;
            assert_eq!(
                record_direct_completion(&mut state, grant.metadata.clone()),
                None
            );
            assert_eq!(
                state.direct_installing.get(&WorkloadId(3)),
                Some(&grant.grant_id)
            );
            // The completion notification does not await the install on the
            // worker reader: both acknowledgement messages can still arrive.
            let waiter = state.direct_grant_waiters[&grant.grant_id].clone();
            waiter.try_send((WorkerId(1), true)).unwrap();
            waiter.try_send((WorkerId(2), true)).unwrap();
        }
        let reply = authorize.await.unwrap();
        assert_eq!(
            serde_json::to_value(reply).unwrap(),
            serde_json::json!({"type":"source_delta_committed","request_id":"direct-1","epoch":7})
        );
        for (_, receiver) in &mut receivers {
            for grant_id in [Some(grant.grant_id), None] {
                let actual = receiver.recv().await.unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::json!({
                        "type":"revoke_direct_epoch_grants", "metadata":grant.metadata, "grant_id":grant_id
                    })
                );
            }
        }
        {
            let state = data_plane.lock().await;
            assert_eq!(
                state.direct_completed,
                HashMap::from([(WorkloadId(3), grant.metadata.clone())])
            );
            assert_eq!(
                (
                    state.direct_epochs.clone(),
                    state.direct_epoch_deadlines.clone(),
                    state.direct_installing.clone(),
                    state.direct_completing.clone(),
                    state
                        .direct_grant_waiters
                        .keys()
                        .copied()
                        .collect::<Vec<_>>()
                ),
                (
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    Vec::new()
                )
            );
        }
        let replay = authorize_direct_epoch(
            grant.metadata.clone(),
            &data_plane,
            &worker_senders,
            &catalog,
            &manager,
        )
        .await;
        assert_eq!(
            serde_json::to_value(replay).unwrap(),
            serde_json::json!({"type":"source_delta_committed","request_id":"direct-1","epoch":7})
        );
        for (_, receiver) in &mut receivers {
            assert_eq!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            );
        }
    }

    #[test]
    fn completion_revocation_blocks_renewal_until_cleanup_and_conflicting_replay_is_rejected() {
        let (mut state, _, _, grant) = direct_lifecycle_fixture();
        state
            .direct_epochs
            .insert(WorkloadId(3), grant.metadata.clone());
        state.direct_epoch_deadlines.insert(WorkloadId(3), 100);
        assert_eq!(
            record_direct_completion(&mut state, grant.metadata.clone()),
            Some(grant.metadata.clone())
        );
        let mut next = grant.clone();
        next.metadata.epoch += 1;
        next.metadata.request_id = "direct-2".into();
        next.grant_id = uuid::Uuid::from_u128(2);
        let (waiter, _) = mpsc::channel(2);
        assert_eq!(
            reserve_direct_grant(&mut state, &next, waiter.clone(), 101),
            Err("RS-3004: workload has an active read, write, or ownership handoff")
        );
        assert_eq!(direct_completion_status(&state, &grant.metadata), Ok(true));
        let mut changed = grant.metadata.clone();
        changed.input_digest[0] ^= 1;
        assert_eq!(
            direct_completion_status(&state, &changed),
            Err("RS-1012: direct source epoch conflicts with completed input")
        );
        release_completed_direct_reservation(&mut state, &grant.metadata);
        assert_eq!(
            reserve_direct_grant(&mut state, &next, waiter, 101),
            Ok(None)
        );
        assert_eq!(
            state.direct_epochs,
            HashMap::from([(WorkloadId(3), next.metadata)])
        );
        assert_eq!(
            state.direct_completed,
            HashMap::from([(WorkloadId(3), grant.metadata)])
        );
    }

    #[test]
    fn routing_completion_rejects_missing_extra_duplicate_and_stale_members() {
        let (state, manager, _, grant) = direct_lifecycle_fixture();
        let valid: Vec<_> = grant
            .members
            .iter()
            .map(|(id, token)| (*id, *token))
            .collect();
        let anchor_lease = valid[0].1;
        let mut extra = valid.clone();
        extra.push((ShardId(3), LeaseToken(1)));
        let mut stale = valid.clone();
        stale[1].1 = LeaseToken(stale[1].1 .0 + 1);
        let mut reversed = valid.clone();
        reversed.reverse();
        let results = [
            valid.clone(),
            vec![valid[0]],
            extra,
            vec![valid[0], valid[0]],
            stale,
            reversed,
            Vec::new(),
        ]
        .into_iter()
        .map(|members| {
            routing_completion_members(
                &state,
                &manager,
                WorkloadId(3),
                WorkerId(1),
                ShardId(1),
                anchor_lease,
                &members,
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(
            results,
            vec![Some(grant.members), None, None, None, None, None, None]
        );
        assert_eq!(
            routing_completion_members(
                &state,
                &manager,
                WorkloadId(3),
                WorkerId(2),
                ShardId(1),
                anchor_lease,
                &valid
            ),
            None
        );
        let moved = manager.get(ShardId(2)).unwrap();
        manager.transfer_if_owner(&moved, WorkerId(3)).unwrap();
        assert_eq!(
            routing_completion_members(
                &state,
                &manager,
                WorkloadId(3),
                WorkerId(1),
                ShardId(1),
                anchor_lease,
                &valid
            ),
            None
        );
    }

    #[test]
    fn complete_routing_snapshots_reject_concurrent_mixed_and_missing_epoch_reports() {
        let shard = |id, epoch, routing| ShardOutput {
            shard_id: ShardId(id),
            lease_token: Some(LeaseToken(id)),
            committed_epoch: epoch,
            routing_epoch: routing,
            deltas: Vec::new(),
            rows: vec![RuntimeRow {
                values_tsv: format!("row-{id}"),
                weight: 1,
            }],
        };
        let cases = vec![
            (
                vec![shard(1, Some(8), Some(8)), shard(2, Some(8), None)],
                true,
            ),
            // The anchor was read just before prepare and another shard just
            // after commit; reversing arrival order must not change rejection.
            (
                vec![shard(1, Some(8), Some(8)), shard(2, Some(9), None)],
                false,
            ),
            (
                vec![shard(2, Some(9), None), shard(1, Some(8), Some(8))],
                false,
            ),
            (
                vec![shard(1, Some(9), Some(9)), shard(2, Some(8), None)],
                false,
            ),
            (
                vec![shard(1, Some(0), Some(0)), shard(2, Some(1), None)],
                false,
            ),
            (
                vec![shard(1, Some(8), Some(8)), shard(2, None, None)],
                false,
            ),
            (
                vec![shard(1, Some(8), None), shard(2, Some(8), None)],
                false,
            ),
            (
                vec![shard(1, Some(8), Some(8)), shard(2, Some(8), Some(8))],
                false,
            ),
            (
                vec![shard(1, Some(8), None), shard(2, Some(8), Some(8))],
                false,
            ),
            (vec![shard(1, None, None), shard(2, None, None)], true),
            (vec![], false),
        ];
        assert_eq!(
            cases
                .iter()
                .map(|(shards, _)| consistent_routing_snapshot(shards))
                .collect::<Vec<_>>(),
            cases
                .iter()
                .map(|(_, expected)| *expected)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn source_waiter_completes_only_after_each_expected_shard_reports() {
        use rockstream_types::data_plane::RuntimeOutputDelta;
        use rockstream_types::ids::OperatorId;

        let (sender, _receiver) = mpsc::channel(1);
        let mut waiter = SourceWaiter {
            attempt_id: None,
            sender,
            workload_id: WorkloadId(7),
            version: 1,
            source: "input".to_owned(),
            expected: HashSet::from([ShardId(1), ShardId(2)]),
            received: HashSet::new(),
            dispatchers: HashMap::new(),
            owned_intent: None,
            prepare_reply: None,
            finish_reply: None,
            finishing: false,
            epoch: 9,
        };
        let mut output = RuntimeOutputDelta {
            attempt_id: None,
            version: 1,
            request_id: "request".to_owned(),
            workload_id: WorkloadId(7),
            shard_id: ShardId(1),
            epoch: 9,
            operator_id: OperatorId(3),
            lease_token: LeaseToken(4),
            source: "input".to_owned(),
            rows: Vec::new(),
        };
        let mut transcript = Vec::new();
        output.workload_id = WorkloadId(8);
        transcript.push(waiter.record_progress(&output));
        output.workload_id = WorkloadId(7);
        output.epoch = 10;
        transcript.push(waiter.record_progress(&output));
        output.epoch = 9;
        output.shard_id = ShardId(3);
        transcript.push(waiter.record_progress(&output));
        output.shard_id = ShardId(1);
        transcript.push(waiter.record_progress(&output));
        transcript.push(waiter.record_progress(&output));
        output.shard_id = ShardId(2);
        transcript.push(waiter.record_progress(&output));
        transcript.push(waiter.record_progress(&output));
        assert_eq!(
            transcript,
            vec![false, false, false, false, false, true, false]
        );
        assert_eq!(waiter.received, HashSet::from([ShardId(1), ShardId(2)]));
    }

    #[test]
    fn management_ack_waiters_reject_overflow_without_replacing_entries() {
        let mut waiters = HashMap::new();
        for index in 0..MAX_MANAGEMENT_ACK_WAITERS {
            let (sender, _receiver) = oneshot::channel();
            try_insert_management_ack_waiter(&mut waiters, format!("op-{index}"), sender).unwrap();
        }
        assert_eq!(waiters.len(), 64);

        let (overflow_sender, _overflow_receiver) = oneshot::channel();
        assert_eq!(
            try_insert_management_ack_waiter(&mut waiters, "overflow".to_owned(), overflow_sender),
            Err("management ACK waiter capacity 64 reached")
        );
        assert_eq!(waiters.len(), 64);
        assert!(!waiters.contains_key("overflow"));

        let (duplicate_sender, _duplicate_receiver) = oneshot::channel();
        assert_eq!(
            try_insert_management_ack_waiter(&mut waiters, "op-0".to_owned(), duplicate_sender),
            Err("management ACK waiter key already exists")
        );
        assert_eq!(waiters.len(), 64);

        waiters.remove("op-0");
        let (recovered_sender, _recovered_receiver) = oneshot::channel();
        try_insert_management_ack_waiter(&mut waiters, "recovered".to_owned(), recovered_sender)
            .unwrap();
        assert_eq!(waiters.len(), 64);
        assert!(waiters.contains_key("recovered"));
    }

    #[tokio::test(start_paused = true)]
    async fn management_ack_waiter_timeout_releases_its_bounded_slot() {
        let waiters = Arc::new(AsyncMutex::new(HashMap::new()));
        let runtime = ManagementMigrationRuntime {
            catalog: TopologyCatalog::new(),
            shard_manager: ShardManager::new(),
            data_plane: Arc::new(AsyncMutex::new(DataPlaneState::default())),
            shard_store: None,
            worker_senders: Arc::new(AsyncMutex::new(HashMap::new())),
            waiters: waiters.clone(),
            operations: ManagementOperationStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )),
            gc_pins: crate::checkpoint_store::GcPinStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )),
            started: Arc::new(AsyncMutex::new(HashSet::new())),
        };
        let worker_id = WorkerId(7);
        let lease = rockstream_types::lease::ShardLease::new(ShardId(3), worker_id, LeaseToken(1));
        let (sender, mut receiver) = mpsc::channel(1);
        let wait = tokio::spawn(async move {
            runtime
                .send_and_wait(
                    &sender,
                    ControlMessage::PrepareShardTransfer {
                        operation_id: "op-timeout".to_owned(),
                        lease: lease.clone(),
                    },
                    "op-timeout",
                    "donor",
                    worker_id,
                    &lease,
                )
                .await
        });

        assert!(matches!(
            receiver.recv().await,
            Some(ControlMessage::PrepareShardTransfer { .. })
        ));
        assert_eq!(waiters.lock().await.len(), 1);
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(
            !wait.is_finished(),
            "ACK waiter expired before its 30-second bound"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            wait.await.unwrap(),
            Err("RS-3610: donor acknowledgement timed out".to_owned())
        );
        assert!(waiters.lock().await.is_empty());
    }

    #[tokio::test]
    async fn worker_drain_queue_rejects_overflow_and_recovers_after_pressure() {
        let catalog = TopologyCatalog::new();
        let shard_manager = ShardManager::new();
        for worker_id in 1..=3 {
            catalog.register(&registration_with_location(
                worker_id,
                0.9,
                &format!("host-{worker_id}"),
                "az-1",
            ));
        }
        for shard_id in 0..MAX_DRAIN_QUEUE as u64 {
            shard_manager
                .acquire(ShardId(shard_id), WorkerId(1))
                .unwrap();
        }
        shard_manager
            .acquire(ShardId(MAX_DRAIN_QUEUE as u64), WorkerId(2))
            .unwrap();
        let drain_state = Arc::new(AsyncMutex::new(DrainState::default()));

        let (lifecycle, queue_fill, queue_capacity, request) = request_worker_drain(
            &catalog,
            &shard_manager,
            None,
            None,
            &drain_state,
            WorkerId(1),
            None,
        )
        .await
        .unwrap();
        let started_at_ms = match lifecycle {
            WorkerLifecycleState::Draining { started_at_ms, .. } => started_at_ms,
            lifecycle => panic!("expected draining lifecycle, got {lifecycle:?}"),
        };
        assert_eq!(queue_fill, MAX_DRAIN_QUEUE as u32);
        assert_eq!(queue_capacity, MAX_DRAIN_QUEUE as u32);
        assert_eq!(
            request.deadline_ms - started_at_ms,
            DEFAULT_DRAIN_DEADLINE_MS
        );

        let overflow = request_worker_drain(
            &catalog,
            &shard_manager,
            None,
            None,
            &drain_state,
            WorkerId(2),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(overflow.code, "RS-3612");
        assert_eq!(
            overflow.message,
            "worker drain queue would exceed its bound (1025/1024)"
        );
        assert_eq!(
            overflow.next_steps,
            "Let the existing drain queue drain, or increase the configured bound only if memory headroom allows."
        );
        assert_eq!(drain_state.lock().await.queue.len(), MAX_DRAIN_QUEUE);
        assert!(catalog.get(WorkerId(2)).unwrap().lifecycle.is_active());

        drain_state.lock().await.queue.pop_front().unwrap();
        let (_, recovered_fill, recovered_capacity, _) = request_worker_drain(
            &catalog,
            &shard_manager,
            None,
            None,
            &drain_state,
            WorkerId(2),
            None,
        )
        .await
        .unwrap();
        assert_eq!(recovered_fill, MAX_DRAIN_QUEUE as u32);
        assert_eq!(recovered_capacity, MAX_DRAIN_QUEUE as u32);
        assert_eq!(drain_state.lock().await.queue.len(), MAX_DRAIN_QUEUE);
    }

    #[tokio::test]
    async fn control_listener_requires_mtls_or_loopback_dev_opt_in() {
        let error = ControlService::new(TopologyCatalog::new())
            .start("127.0.0.1:0")
            .await
            .err()
            .expect("plaintext listener must fail closed by default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "RS-2405: control service requires internal mTLS; plaintext development mode must be explicitly enabled on a loopback address"
        );

        let error = ControlService::new(TopologyCatalog::new())
            .with_insecure_dev_mode()
            .start("0.0.0.0:0")
            .await
            .err()
            .expect("plaintext development mode must reject non-loopback listeners");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "RS-2405: control service requires internal mTLS; plaintext development mode must be explicitly enabled on a loopback address"
        );
    }

    async fn start_test_service() -> (ControlServiceHandle, TopologyCatalog) {
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone()).with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        (handle, catalog)
    }

    async fn send_and_recv(stream: &mut TcpStream, msg: &WorkerMessage) -> String {
        let line = serde_json::to_string(msg).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(&mut *stream);
        loop {
            let mut resp = String::new();
            reader.read_line(&mut resp).await.unwrap();
            if matches!(msg, WorkerMessage::Register(_))
                || !matches!(
                    serde_json::from_str(&resp),
                    Ok(ControlMessage::TopologyChanged { .. })
                )
            {
                return resp;
            }
        }
    }

    fn registration_with_location(
        worker_id: u64,
        headroom: f64,
        host: &str,
        az: &str,
    ) -> WorkerRegistration {
        WorkerRegistration::new(
            WorkerId(worker_id),
            NodeRole::Worker,
            format!("127.0.0.1:{}", 7000 + worker_id),
            CapacityHeadroom::new(headroom),
        )
        .with_location(WorkerLocation::new(host, az))
        .with_capabilities(WorkerCapabilities {
            shared_shard_store_id: Some([1; 32]),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn worker_registers_and_receives_ack() {
        let (handle, catalog) = start_test_service().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();

        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:7001",
            CapacityHeadroom::new(0.9),
        );
        let resp = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::Registered { worker_id, .. } => {
                assert_eq!(worker_id, WorkerId(1));
            }
            _ => panic!("expected Registered reply"),
        }
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get(WorkerId(1)).unwrap().address, "127.0.0.1:7001");

        handle.shutdown();
    }

    #[tokio::test]
    async fn topology_catalog_updated_after_registration() {
        let (handle, catalog) = start_test_service().await;
        let mut streams = Vec::new();

        for i in 1..=3u64 {
            let mut stream = TcpStream::connect(handle.addr).await.unwrap();
            let reg = WorkerRegistration::new(
                WorkerId(i),
                NodeRole::Worker,
                format!("127.0.0.1:{}", 7000 + i),
                CapacityHeadroom::new(0.5 + i as f64 * 0.1),
            );
            let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
            stream.write_all(line.as_bytes()).await.unwrap();
            // wait for ack
            let mut reader = BufReader::new(&mut stream);
            let mut resp = String::new();
            reader.read_line(&mut resp).await.unwrap();
            drop(reader);
            streams.push(stream);
        }

        // Allow async tasks to process
        tokio::task::yield_now().await;
        assert_eq!(catalog.len(), 3);
        handle.shutdown();
    }

    #[tokio::test]
    async fn tier2_start_flow() {
        // Tier 2: --role=all means control + worker start in the same process.
        // Verify the control service starts and a worker can self-register.
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone()).with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        let addr = handle.addr.to_string();

        // Simulate a worker connecting to its own in-process control service.
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(100),
            NodeRole::All,
            addr.clone(),
            CapacityHeadroom::FULL,
        );
        let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut stream);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(matches!(reply, ControlMessage::Registered { .. }));

        assert!(catalog.get(WorkerId(100)).is_some());
        handle.shutdown();
    }

    // -----------------------------------------------------------------------
    // v0.29: Shard lease and fence tests
    // -----------------------------------------------------------------------

    async fn start_test_service_with_manager(
    ) -> (ControlServiceHandle, TopologyCatalog, ShardManager) {
        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let svc = ControlService::new(catalog.clone())
            .with_shard_manager(manager.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        (handle, catalog, manager)
    }

    #[tokio::test]
    async fn worker_can_request_and_receive_shard_lease() {
        use rockstream_types::ids::ShardId;

        let (handle, _catalog, manager) = start_test_service_with_manager().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();

        // Register first.
        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        // Request a shard lease.
        let req = WorkerMessage::RequestShard {
            worker_id: WorkerId(1),
            shard_id: ShardId(42),
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::ShardAssigned { lease, .. } => {
                assert_eq!(lease.shard_id, ShardId(42));
                assert_eq!(lease.worker_id, WorkerId(1));
                // Verify the manager also has the lease.
                assert!(manager.is_valid_writer(ShardId(42), lease.lease_token));
            }
            _ => panic!("expected ShardAssigned, got: {reply:?}"),
        }

        handle.shutdown();
    }

    // -----------------------------------------------------------------------
    // v0.45.2 M7-S2: leader-only write gating for shard-lease requests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn shard_lease_request_rejected_with_not_leader_when_not_raft_leader() {
        use crate::raft::{spawn_raft_node, RaftConfig};
        use object_store::memory::InMemory;
        use rockstream_types::ids::ShardId;
        use std::sync::Arc;

        // A non-bootstrap node with no reachable peers cannot win an
        // election before its own randomized election-timeout floor
        // (150ms) elapses, so checking immediately after spawn
        // deterministically observes it as a Follower.
        let config = RaftConfig::new(0, Vec::new(), false);
        let node = spawn_raft_node("127.0.0.1:0", config, Arc::new(InMemory::new()))
            .await
            .unwrap();
        assert!(!node.handle.is_leader());

        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let svc = ControlService::new(catalog.clone())
            .with_shard_manager(manager.clone())
            .with_raft(node.handle.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        let req = WorkerMessage::RequestShard {
            worker_id: WorkerId(1),
            shard_id: ShardId(42),
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(
            matches!(reply, ControlMessage::NotLeader { .. }),
            "expected NotLeader, got: {reply:?}"
        );

        // No lease was actually granted.
        assert!(manager.get(ShardId(42)).is_none());

        handle.shutdown();
        node.shutdown();
    }

    #[test]
    fn collect_operator_stats_uses_live_windowed_metrics() {
        use rockstream_types::ids::OperatorId;
        use rockstream_types::merge_law::MergeLawId;
        use rockstream_types::metrics::{self, LawMetricKey};
        use std::time::{Duration, SystemTime};

        metrics::reset_all();
        let op_id = OperatorId(7);
        let at = SystemTime::now();
        metrics::record_operator_runtime_sample_at(op_id, 300, 12, Duration::from_millis(4), 1, at);
        metrics::record_operator_runtime_sample_at(op_id, 200, 8, Duration::from_millis(9), 0, at);
        metrics::record_operator_runtime_sample_at(
            op_id,
            100,
            10,
            Duration::from_millis(15),
            1,
            at,
        );

        let metric_key = LawMetricKey {
            law_id: MergeLawId(1),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: Some(op_id),
        };
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_required(&metric_key);

        let svc = ControlService::new(TopologyCatalog::new());
        let stats = svc.collect_operator_stats(&[op_id]);
        let stat = stats
            .iter()
            .find(|s| (s.stats.rows_per_s - 10.0).abs() < 1e-9)
            .expect("expected driven operator stats");

        assert_eq!(stat.operator_id, op_id);
        assert_ne!(stat.stats.rows_per_s, 12500.0);
        assert_ne!(stat.stats.state_reads, 120);
        assert_ne!(stat.stats.p99_latency_ms, 12.0);
        assert_eq!(stat.stats.rows_per_s, 10.0);
        assert_eq!(stat.stats.state_reads, 30);
        assert!(
            (stat.stats.rmw_ratio - 0.25).abs() < 1e-9,
            "rmw_ratio={}",
            stat.stats.rmw_ratio
        );
        assert_eq!(stat.stats.p99_latency_ms, 15.0);
        assert_eq!(stat.stats.dlq_entries, 2);

        metrics::reset_all();
    }

    #[tokio::test]
    async fn fence_write_confirms_valid_token() {
        use rockstream_types::ids::{LeaseToken, ShardId};

        let (handle, _catalog, manager) = start_test_service_with_manager().await;

        // Pre-create a lease directly in the manager (bypassing the network for
        // setup speed).
        let lease = manager.acquire(ShardId(5), WorkerId(7)).unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        // Register so the connection is associated.
        let reg = WorkerRegistration::new(
            WorkerId(7),
            NodeRole::Worker,
            "127.0.0.1:9002",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        // Fence with valid token.
        let fence_req = WorkerMessage::FenceWrite {
            shard_id: ShardId(5),
            lease_token: lease.lease_token,
        };
        let resp = send_and_recv(&mut stream, &fence_req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::FenceAck { shard_id, valid } => {
                assert_eq!(shard_id, ShardId(5));
                assert!(valid, "current token must be valid");
            }
            _ => panic!("expected FenceAck, got: {reply:?}"),
        }

        // Fence with stale token (simulate worker A being fenced by worker B).
        let stale_token = LeaseToken(0); // guaranteed to be lower
        let fence_stale = WorkerMessage::FenceWrite {
            shard_id: ShardId(5),
            lease_token: stale_token,
        };
        let resp2 = send_and_recv(&mut stream, &fence_stale).await;
        let reply2: ControlMessage = serde_json::from_str(resp2.trim()).unwrap();
        match reply2 {
            ControlMessage::FenceAck { valid, .. } => {
                assert!(!valid, "stale token must be rejected");
            }
            _ => panic!("expected FenceAck, got: {reply2:?}"),
        }

        handle.shutdown();
    }

    #[tokio::test]
    async fn shard_leases_released_on_worker_disconnect() {
        use rockstream_types::ids::ShardId;

        let (handle, _catalog, manager) = start_test_service_with_manager().await;

        // Register worker and acquire shards over the wire.
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(3),
            NodeRole::Worker,
            "127.0.0.1:9003",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        let r1 = WorkerMessage::RequestShard {
            worker_id: WorkerId(3),
            shard_id: ShardId(10),
        };
        let r2 = WorkerMessage::RequestShard {
            worker_id: WorkerId(3),
            shard_id: ShardId(11),
        };
        let _ = send_and_recv(&mut stream, &r1).await;
        let _ = send_and_recv(&mut stream, &r2).await;

        assert_eq!(manager.len(), 2);

        // Drop the TCP stream — simulates worker death.
        drop(stream);
        // Give the async handler time to notice the disconnect.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // All leases should be released.
        assert!(
            manager.is_empty(),
            "shard leases must be released when the worker disconnects"
        );

        handle.shutdown();
    }

    // -----------------------------------------------------------------------
    // v0.45.2 M7-S4: cluster status query, frontier gating, and cross-process
    // shard-manager takeover
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cluster_status_query_without_raft_reports_no_raft() {
        let (handle, _catalog) = start_test_service().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let resp = send_and_recv(&mut stream, &WorkerMessage::ClusterStatusQuery).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::ClusterStatusReport {
                node_id,
                role,
                term,
            } => {
                assert_eq!(node_id, None);
                assert_eq!(role, RaftRoleWire::NoRaft);
                assert_eq!(term, 0);
            }
            _ => panic!("expected ClusterStatusReport, got: {reply:?}"),
        }
        handle.shutdown();
    }

    #[tokio::test]
    async fn cluster_status_query_with_raft_reports_leader() {
        use crate::raft::{spawn_raft_node, RaftConfig};
        use object_store::memory::InMemory;

        let config = RaftConfig::new(0, Vec::new(), true);
        let node = spawn_raft_node("127.0.0.1:0", config, Arc::new(InMemory::new()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(node.handle.is_leader());

        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog)
            .with_raft(node.handle.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let resp = send_and_recv(&mut stream, &WorkerMessage::ClusterStatusQuery).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::ClusterStatusReport {
                node_id,
                role,
                term,
            } => {
                assert_eq!(node_id, Some(0));
                assert_eq!(role, RaftRoleWire::Leader);
                assert_eq!(term, node.handle.current_term());
            }
            _ => panic!("expected ClusterStatusReport, got: {reply:?}"),
        }
        handle.shutdown();
        node.shutdown();
    }

    #[tokio::test]
    async fn report_shard_frontier_without_raft_always_publishes() {
        use rockstream_types::ids::ShardId;

        let (handle, _catalog) = start_test_service().await;
        let svc = ControlService::new(TopologyCatalog::new());
        drop(svc); // constructed only to document the default-no-frontier path
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        // No frontier aggregator attached to `start_test_service` — ingestion
        // is a no-op, but the leader-authority reply path still must not
        // wrongly claim NotLeader in single-node (no-raft) mode. Since no
        // aggregator is attached at all, no reply is sent for this message;
        // assert the connection stays healthy by following up with a status
        // query.
        let _ = tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            format!(
                "{}\n",
                serde_json::to_string(&WorkerMessage::ReportShardFrontier {
                    shard_id: ShardId(1),
                    epoch: 5,
                })
                .unwrap()
            )
            .as_bytes(),
        )
        .await;
        let resp = send_and_recv(&mut stream, &WorkerMessage::ClusterStatusQuery).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(matches!(reply, ControlMessage::ClusterStatusReport { .. }));
        handle.shutdown();
    }

    #[tokio::test]
    async fn report_shard_frontier_with_frontier_and_no_raft_publishes_advance() {
        use rockstream_types::ids::ShardId;

        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let frontier = Arc::new(FrontierAggregator::new());
        let svc = ControlService::new(catalog)
            .with_shard_manager(manager)
            .with_frontier(frontier.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let registration = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(registration)).await;
        let lease_reply = send_and_recv(
            &mut stream,
            &WorkerMessage::RequestShard {
                worker_id: WorkerId(1),
                shard_id: ShardId(1),
            },
        )
        .await;
        assert!(matches!(
            serde_json::from_str::<ControlMessage>(lease_reply.trim()).unwrap(),
            ControlMessage::ShardAssigned { .. }
        ));
        let req = WorkerMessage::ReportShardFrontier {
            shard_id: ShardId(1),
            epoch: 42,
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::ClusterFrontierAdvanced { epoch } => assert_eq!(epoch, 42),
            _ => panic!("expected ClusterFrontierAdvanced, got: {reply:?}"),
        }
        assert_eq!(frontier.cluster_frontier().epoch, Some(42));
        handle.shutdown();
    }

    #[tokio::test]
    async fn legacy_frontier_report_requires_current_shard_lease() {
        use rockstream_types::ids::ShardId;

        let manager = ShardManager::new();
        let frontier = Arc::new(FrontierAggregator::new());
        let svc = ControlService::new(TopologyCatalog::new())
            .with_shard_manager(manager)
            .with_frontier(frontier.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let registration = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(registration)).await;
        let reply: ControlMessage = serde_json::from_str(
            send_and_recv(
                &mut stream,
                &WorkerMessage::ReportShardFrontier {
                    shard_id: ShardId(1),
                    epoch: 42,
                },
            )
            .await
            .trim(),
        )
        .unwrap();

        assert!(matches!(
            reply,
            ControlMessage::OperationFailed { ref code, .. } if code == "RS-8004"
        ));
        assert_eq!(frontier.cluster_frontier().epoch, None);
        handle.shutdown();
    }

    #[tokio::test]
    async fn membership_frontier_requires_current_shard_lease() {
        use rockstream_types::frontier::{
            FrontierGeneration, FrontierMembership, MembershipFrontierReport, ShardIncarnation,
        };

        let manager = ShardManager::new();
        let membership = FrontierMembership::new("view-1", FrontierGeneration(0))
            .with_active(ShardId(42), ShardIncarnation(1));
        let frontier = Arc::new(FrontierAggregator::with_membership(membership));
        let svc = ControlService::new(TopologyCatalog::new())
            .with_shard_manager(manager.clone())
            .with_frontier(frontier.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let registration = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(registration)).await;
        let lease_reply = send_and_recv(
            &mut stream,
            &WorkerMessage::RequestShard {
                worker_id: WorkerId(1),
                shard_id: ShardId(42),
            },
        )
        .await;
        let lease = match serde_json::from_str::<ControlMessage>(lease_reply.trim()).unwrap() {
            ControlMessage::ShardAssigned { lease, .. } => lease,
            reply => panic!("expected ShardAssigned, got: {reply:?}"),
        };

        let invalid = WorkerMessage::ReportShardFrontierV2 {
            report: MembershipFrontierReport::new(
                "view-1",
                FrontierGeneration(0),
                ShardId(42),
                ShardIncarnation(1),
                LeaseToken(lease.lease_token.0 + 1),
                42,
            ),
        };
        let reply: ControlMessage =
            serde_json::from_str(send_and_recv(&mut stream, &invalid).await.trim()).unwrap();
        assert!(matches!(
            reply,
            ControlMessage::OperationFailed { ref code, .. } if code == "RS-8004"
        ));
        assert_eq!(frontier.cluster_frontier().epoch, None);

        let invalid_membership = WorkerMessage::ReportShardFrontierV2 {
            report: MembershipFrontierReport::new(
                "other-view",
                FrontierGeneration(0),
                ShardId(42),
                ShardIncarnation(1),
                lease.lease_token,
                42,
            ),
        };
        let reply: ControlMessage =
            serde_json::from_str(send_and_recv(&mut stream, &invalid_membership).await.trim())
                .unwrap();
        match reply {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => {
                assert_eq!(code, RS_8004.to_string());
                assert_eq!(message, "RS-8004 frontier report rejected: scope mismatch");
                assert_eq!(
                    next_steps,
                    rockstream_types::error_code::next_steps(RS_8004).to_string()
                );
            }
            reply => panic!("expected OperationFailed, got: {reply:?}"),
        }
        assert_eq!(frontier.cluster_frontier().epoch, None);

        let valid = WorkerMessage::ReportShardFrontierV2 {
            report: MembershipFrontierReport::new(
                "view-1",
                FrontierGeneration(0),
                ShardId(42),
                ShardIncarnation(1),
                lease.lease_token,
                42,
            ),
        };
        let reply: ControlMessage =
            serde_json::from_str(send_and_recv(&mut stream, &valid).await.trim()).unwrap();
        assert!(matches!(
            reply,
            ControlMessage::ClusterFrontierAdvanced { epoch: 42 }
        ));
        assert_eq!(frontier.cluster_frontier().epoch, Some(42));
        assert!(manager.is_valid_writer(ShardId(42), lease.lease_token));
        handle.shutdown();
    }

    #[tokio::test]
    async fn report_shard_frontier_rejected_with_not_leader_when_demoted() {
        use crate::raft::{spawn_raft_node, RaftConfig};
        use object_store::memory::InMemory;
        use rockstream_types::ids::ShardId;

        let config = RaftConfig::new(0, Vec::new(), false);
        let node = spawn_raft_node("127.0.0.1:0", config, Arc::new(InMemory::new()))
            .await
            .unwrap();
        assert!(!node.handle.is_leader());

        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        manager.acquire(ShardId(1), WorkerId(1)).unwrap();
        let frontier = Arc::new(FrontierAggregator::new());
        let svc = ControlService::new(catalog)
            .with_shard_manager(manager)
            .with_raft(node.handle.clone())
            .with_frontier(frontier.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let registration = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(registration)).await;
        let req = WorkerMessage::ReportShardFrontier {
            shard_id: ShardId(1),
            epoch: 7,
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(
            matches!(reply, ControlMessage::NotLeader { .. }),
            "expected NotLeader, got: {reply:?}"
        );
        // Ingestion still happened (meet computation carries over once this
        // node becomes leader), even though publication was rejected.
        assert_eq!(frontier.cluster_frontier().epoch, Some(7));

        handle.shutdown();
        node.shutdown();
    }

    /// v0.45.2 M7-S4/S5: a *second, independent* `ControlService` sharing the
    /// same backing object store (models a different real control-node
    /// process) picks up the first node's persisted lease state as soon as
    /// it becomes leader, instead of granting a conflicting lease for a
    /// shard the first node already leased out.
    #[tokio::test]
    async fn newly_leading_control_service_adopts_shared_shard_state() {
        use crate::raft::{spawn_raft_node, RaftConfig};
        use crate::shard::ShardPersistentStore;
        use object_store::memory::InMemory;
        use rockstream_types::ids::ShardId;

        let shared_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // "Node A": becomes leader, grants shard 1 to worker 10, then is
        // simulated to crash (its ControlService is simply dropped/shutdown
        // without ever explicitly deregistering the worker).
        let node_a = spawn_raft_node(
            "127.0.0.1:0",
            RaftConfig::new(0, Vec::new(), true),
            shared_store.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(node_a.handle.is_leader());

        let catalog_a = TopologyCatalog::new();
        let manager_a = ShardManager::new();
        let svc_a = ControlService::new(catalog_a)
            .with_shard_manager(manager_a.clone())
            .with_raft(node_a.handle.clone())
            .with_shard_store(Arc::new(ShardPersistentStore::new(shared_store.clone())))
            .with_insecure_dev_mode();
        let handle_a = svc_a.start("127.0.0.1:0").await.unwrap();

        let mut stream_a = TcpStream::connect(handle_a.addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(10),
            NodeRole::Worker,
            "127.0.0.1:9010",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream_a, &WorkerMessage::Register(reg)).await;
        let req = WorkerMessage::RequestShard {
            worker_id: WorkerId(10),
            shard_id: ShardId(1),
        };
        let resp = send_and_recv(&mut stream_a, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(matches!(reply, ControlMessage::ShardAssigned { .. }));
        assert_eq!(manager_a.len(), 1);

        // Node A crashes.
        handle_a.shutdown();
        node_a.shutdown();

        // "Node B": a different real control node — its own independent
        // Raft node id/term and its own empty in-memory `ShardManager` —
        // but wired to the SAME shared object store. It bootstraps as its
        // own single-node group (models the surviving majority electing a
        // new leader) and becomes leader at a higher epoch.
        let node_b = spawn_raft_node(
            "127.0.0.1:0",
            RaftConfig::new(1, Vec::new(), true),
            shared_store.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(node_b.handle.is_leader());

        let catalog_b = TopologyCatalog::new();
        let manager_b = ShardManager::new();
        assert!(manager_b.is_empty(), "node B starts with an empty map");
        let svc_b = ControlService::new(catalog_b)
            .with_shard_manager(manager_b.clone())
            .with_raft(node_b.handle.clone())
            .with_shard_store(Arc::new(ShardPersistentStore::new(shared_store)))
            .with_insecure_dev_mode();
        let handle_b = svc_b.start("127.0.0.1:0").await.unwrap();

        // A different worker (worker 20) tries to grab the SAME shard that
        // worker 10 already holds, against node B. The control-plane wire
        // protocol does not reply at all when a shard-lease request is
        // denied (the denial is signaled by the absence of a
        // `ShardAssigned` reply, not a response message) — so this sends
        // the request and asserts against `manager_b`'s state directly
        // rather than blocking on a reply that will never arrive.
        let mut stream_b = TcpStream::connect(handle_b.addr).await.unwrap();
        let reg2 = WorkerRegistration::new(
            WorkerId(20),
            NodeRole::Worker,
            "127.0.0.1:9020",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream_b, &WorkerMessage::Register(reg2)).await;
        let req2 = WorkerMessage::RequestShard {
            worker_id: WorkerId(20),
            shard_id: ShardId(1),
        };
        let line2 = serde_json::to_string(&req2).unwrap() + "\n";
        AsyncWriteExt::write_all(&mut stream_b, line2.as_bytes())
            .await
            .unwrap();
        // Give the server time to process the denied request; then confirm
        // no reply is forthcoming within a bounded window (the "silence
        // means denial" wire-protocol contract), and that node B did NOT
        // grant a conflicting lease.
        let mut reader_b = BufReader::new(&mut stream_b);
        let mut unexpected = None;
        let _ = tokio::time::timeout(std::time::Duration::from_millis(300), async {
            loop {
                let mut response = String::new();
                if reader_b.read_line(&mut response).await.unwrap() == 0 {
                    break;
                }
                let reply = serde_json::from_str(response.trim()).unwrap();
                if !matches!(reply, ControlMessage::TopologyChanged { .. }) {
                    unexpected = Some(reply);
                    break;
                }
            }
        })
        .await;
        assert!(
            unexpected.is_none(),
            "split-brain: node B replied to worker 20's conflicting request \
             instead of silently denying it (reply={unexpected:?})"
        );
        // Connection is closed (LeaseError::AlreadyLeased) — correct:
        // node B adopted node A's persisted state and knows shard 1
        // already belongs to worker 10.
        assert_eq!(
            manager_b.get(ShardId(1)).unwrap().worker_id,
            WorkerId(10),
            "node B must have synced worker 10's lease from the shared store"
        );

        handle_b.shutdown();
        node_b.shutdown();
    }

    #[tokio::test]
    async fn drain_prefers_same_az_recipient_before_cross_az() {
        let catalog = TopologyCatalog::new();
        let shard_manager = ShardManager::new();
        catalog.register(&registration_with_location(1, 0.9, "host-a", "az-1"));
        catalog.register(&registration_with_location(2, 0.9, "host-b", "az-2"));
        catalog.register(&registration_with_location(3, 0.9, "host-c", "az-1"));
        shard_manager
            .acquire(rockstream_types::ids::ShardId(7), WorkerId(1))
            .unwrap();
        let drain_state = Arc::new(AsyncMutex::new(DrainState::default()));

        let _ = request_worker_drain(
            &catalog,
            &shard_manager,
            None,
            None,
            &drain_state,
            WorkerId(1),
            None,
        )
        .await
        .unwrap();

        let guard = drain_state.lock().await;
        assert_eq!(guard.queue.len(), 1);
        assert_eq!(guard.queue[0].recipient_worker_id, WorkerId(3));
    }

    #[tokio::test]
    async fn test_lease_grant_raft_replication() {
        use crate::raft::{spawn_raft_node, RaftConfig};
        use crate::shard::ShardPersistentStore;
        use object_store::memory::InMemory;
        use rockstream_types::ids::ShardId;

        let shared_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

        // Spawn Raft leader node
        let node_leader = spawn_raft_node(
            "127.0.0.1:0",
            RaftConfig::new(0, Vec::new(), true),
            shared_store.clone(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(node_leader.handle.is_leader());
        assert!(node_leader.handle.require_leader().is_ok());

        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let store = Arc::new(ShardPersistentStore::new(shared_store.clone()));
        let svc = ControlService::new(catalog)
            .with_shard_manager(manager.clone())
            .with_raft(node_leader.handle.clone())
            .with_shard_store(store.clone())
            .with_insecure_dev_mode();
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(101),
            NodeRole::Worker,
            "127.0.0.1:9101",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        // Issue lease request to leader
        let req = WorkerMessage::RequestShard {
            worker_id: WorkerId(101),
            shard_id: ShardId(42),
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();

        if let ControlMessage::ShardAssigned { lease, .. } = reply {
            assert_eq!(lease.shard_id, ShardId(42));
            assert_eq!(lease.worker_id, WorkerId(101));
        } else {
            panic!("Expected ShardAssigned, got: {reply:?}");
        }

        // Verify lease state is persisted into shared Raft store and can be restored
        let snapshot = store.load().await;
        assert_eq!(snapshot.leases.len(), 1);
        let persisted_lease = snapshot.leases.get(&ShardId(42)).unwrap();
        assert_eq!(persisted_lease.worker_id, WorkerId(101));

        handle.shutdown();
        node_leader.shutdown();
    }
}
