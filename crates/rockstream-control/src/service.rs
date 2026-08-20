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

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex};

use rockstream_types::data_plane::{
    DeploymentDescriptor, DeploymentRequest, RuntimeExchangeMessage, RuntimeOutputDelta,
    ShardOutput, WorkerExecutionStatus, WorkloadSnapshot,
};
use rockstream_types::error_code::{RS_2410, RS_2411, RS_2412, RS_3604, RS_3610, RS_3611, RS_3612};
use rockstream_types::identity::{InternalTlsConfig, NodeIdentity, NodeRole};
use rockstream_types::ids::{ShardId, WorkerId, WorkloadId};
use rockstream_types::lease::ShardRevokeReason;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};
use rockstream_types::topology::{
    ControlMessage, DrainRequest, RaftRoleWire, WorkerLifecycleState, WorkerMessage,
};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::frontier::FrontierAggregator;
use crate::migration::{MigrationCoordinator, MigrationPersistentStore};
use crate::placement::PlacementAlgorithm;
use crate::raft::{RaftHandle, RaftRole};
use crate::scheduler::ShardScheduler;
use crate::secret_store::{SecretStore, SecretStoreError};
use crate::shard::{ShardManager, ShardPersistentStore};
use crate::topology::{TopologyCatalog, TopologyPersistentStore};

const DEFAULT_DRAIN_DEADLINE_MS: u64 = 30_000;
const DEFAULT_DECOMMISSION_GRACE_MS: u64 = 5_000;
const MAX_DRAIN_QUEUE: usize = 1024;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

fn secret_error_next_steps(error: &SecretStoreError) -> String {
    rockstream_types::error_code::next_steps(secret_error_code(error)).to_string()
}

#[derive(Debug, Clone)]
struct DrainTask {
    migration_id: String,
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
    next_migration_id: u64,
}

#[derive(Default)]
struct DataPlaneState {
    deployments: HashMap<WorkloadId, DeploymentState>,
    source_waiters: HashMap<String, SourceWaiter>,
}

struct DeploymentState {
    request: DeploymentRequest,
    descriptors: HashMap<ShardId, DeploymentDescriptor>,
    outputs: HashMap<ShardId, Vec<RuntimeOutputDelta>>,
    workers: HashMap<WorkerId, WorkerExecutionStatus>,
    ready_shards: HashSet<ShardId>,
    ready_waiter: Option<mpsc::Sender<ControlMessage>>,
}

struct SourceWaiter {
    sender: mpsc::Sender<ControlMessage>,
    expected: usize,
    received: usize,
    epoch: u64,
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
    /// TLS certificate reloader (if internal mTLS is enabled).
    pub reloader: Option<Arc<crate::tls::TlsCertificateReloader>>,
}

impl ControlServiceHandle {
    /// Signal the service to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Reload the server certificate, private key, and/or CA certificate without restarting.
    pub fn reload_tls(&self, new_config: InternalTlsConfig) -> Result<(), String> {
        if let Some(ref r) = self.reloader {
            r.reload(new_config)
        } else {
            Err("RS-2410: internal TLS is not enabled on this control service".to_string())
        }
    }
}

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
    /// Shared drain queue state with a named bound.
    drain_state: Arc<AsyncMutex<DrainState>>,
    /// Automatically process queued drain migrations in the background.
    auto_drain: bool,
    /// Optional internal TLS configuration for control plane mTLS.
    internal_tls: Option<InternalTlsConfig>,
    secret_store: Arc<SecretStore>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
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
            drain_state: Arc::new(AsyncMutex::new(DrainState::default())),
            auto_drain: false,
            internal_tls: None,
            secret_store: Arc::new(SecretStore::new(
                None,
                Arc::new(crate::kek::EnvKekProvider::from_env_or_default(
                    "rockstream-default-kek",
                )),
            )),
            worker_senders: Arc::new(AsyncMutex::new(HashMap::new())),
            data_plane: Arc::new(AsyncMutex::new(DataPlaneState::default())),
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

    /// Enable automatic background drain processing.
    pub fn with_auto_drain(mut self, enabled: bool) -> Self {
        self.auto_drain = enabled;
        self
    }

    /// Attach an [`InternalTlsConfig`] for internal mTLS mutual authentication.
    pub fn with_internal_tls(mut self, config: InternalTlsConfig) -> Self {
        self.internal_tls = Some(config);
        self
    }

    /// Attach the catalog secret store used by worker token requests.
    pub fn with_secret_store(mut self, secret_store: Arc<SecretStore>) -> Self {
        self.secret_store = secret_store;
        self
    }

    /// Start the service on `bind_addr`.
    ///
    /// Returns a [`ControlServiceHandle`] which can be used to query the
    /// bound address and send a shutdown signal.
    pub async fn start(self, bind_addr: &str) -> io::Result<ControlServiceHandle> {
        if let Some(store) = &self.topology_store {
            if let Ok(workers) = store.load_all().await {
                self.catalog.restore_workers(workers);
            }
        }
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(addr = %addr, "control service listening");

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
            migration_store: self.migration_store.clone(),
            drain_state: self.drain_state.clone(),
            auto_drain: self.auto_drain,
            secret_store: self.secret_store.clone(),
            worker_senders: self.worker_senders.clone(),
            data_plane: self.data_plane.clone(),
        };

        let reloader = if let Some(tls_cfg) = &self.internal_tls {
            if tls_cfg.is_enabled() {
                match crate::tls::TlsCertificateReloader::new(tls_cfg.clone()) {
                    Ok(r) => Some(Arc::new(r)),
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("RS-2405: failed to initialize internal TLS: {e}"),
                        ));
                    }
                }
            } else {
                None
            }
        } else {
            None
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
                        break;
                    }
                }
            }
        });

        Ok(ControlServiceHandle {
            addr,
            shutdown_tx,
            reloader: reloader_for_handle,
        })
    }

    /// Collect live operator statistics for a pipeline.
    pub fn collect_operator_stats(
        &self,
        _pipeline_id: u64,
    ) -> Vec<rockstream_types::explain::OperatorStats> {
        rockstream_types::metrics::operator_runtime_report()
            .into_iter()
            .map(|snapshot| {
                let (rmw_avoided, rmw_required) =
                    rockstream_types::metrics::operator_rmw_totals(snapshot.operator_id);
                let rmw_total = rmw_avoided + rmw_required;
                let rmw_ratio = if rmw_total == 0 {
                    0.0
                } else {
                    rmw_required as f64 / rmw_total as f64
                };
                rockstream_types::explain::OperatorStats {
                    rows_per_s: snapshot.rows_per_s,
                    state_reads: snapshot.state_reads,
                    rmw_ratio,
                    p99_latency_ms: snapshot.p99_latency_ms,
                    dlq_entries: snapshot.dlq_entries,
                }
            })
            .collect()
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
    migration_store: Option<Arc<MigrationPersistentStore>>,
    drain_state: Arc<AsyncMutex<DrainState>>,
    auto_drain: bool,
    secret_store: Arc<SecretStore>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
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

fn stable_route(value: &str, shard_count: usize) -> usize {
    let hash = value
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    hash as usize % shard_count
}

async fn deploy_workload(
    request: DeploymentRequest,
    sender: &mpsc::Sender<ControlMessage>,
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
) {
    let healthy = catalog.healthy_workers();
    let workers = PlacementAlgorithm::assign_n(&healthy, healthy.len());
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

    let mut descriptors = HashMap::new();
    for (index, worker_id) in workers.into_iter().enumerate() {
        let shard_id = ShardId(
            request
                .workload_id
                .0
                .wrapping_mul(16)
                .wrapping_add(index as u64),
        );
        let (lease, _) = shard_manager.force_acquire(shard_id, worker_id);
        let storage_identity = format!(
            "{}/workload-{}/shard-{}",
            request.storage_root, request.workload_id.0, index
        );
        descriptors.insert(
            shard_id,
            DeploymentDescriptor::new(request.clone(), lease, storage_identity),
        );
    }

    data_plane.lock().await.deployments.insert(
        request.workload_id,
        DeploymentState {
            request,
            descriptors: descriptors.clone(),
            outputs: HashMap::new(),
            workers: HashMap::new(),
            ready_shards: HashSet::new(),
            ready_waiter: Some(sender.clone()),
        },
    );
    for descriptor in descriptors.into_values() {
        if let Some(target) = senders.get(&descriptor.shard.worker_id) {
            send_message(
                target,
                &ControlMessage::ShardAssigned {
                    lease: descriptor.shard.clone(),
                },
            )
            .await;
            send_message(target, &ControlMessage::Deploy { descriptor }).await;
        }
    }
}

async fn submit_source_delta(
    request: rockstream_types::data_plane::SourceDeltaRequest,
    sender: &mpsc::Sender<ControlMessage>,
    worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: &Arc<AsyncMutex<DataPlaneState>>,
) {
    let (routing_column, operator_id, descriptors) = {
        let state = data_plane.lock().await;
        let Some(deployment) = state.deployments.get(&request.workload_id) else {
            send_message(sender, &data_plane_failure("workload is not deployed")).await;
            return;
        };
        let Some(column) = deployment.request.routing_columns.get(&request.source) else {
            send_message(sender, &data_plane_failure("source has no routing column")).await;
            return;
        };
        let mut descriptors: Vec<_> = deployment.descriptors.values().cloned().collect();
        descriptors.sort_by_key(|descriptor| descriptor.shard.shard_id);
        (*column, deployment.request.sink_operator_id, descriptors)
    };

    let mut routed: HashMap<usize, Vec<_>> = HashMap::new();
    for row in request.rows {
        let Some(value) = row.values_tsv.split('\t').nth(routing_column) else {
            send_message(
                sender,
                &data_plane_failure("row is missing its routing field"),
            )
            .await;
            return;
        };
        routed
            .entry(stable_route(value, descriptors.len()))
            .or_default()
            .push(row);
    }
    if routed.is_empty() {
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

    data_plane.lock().await.source_waiters.insert(
        request.request_id.clone(),
        SourceWaiter {
            sender: sender.clone(),
            expected: routed.len(),
            received: 0,
            epoch: request.epoch,
        },
    );
    let senders = worker_senders.lock().await.clone();
    for (index, rows) in routed {
        let descriptor = &descriptors[index];
        let frame = RuntimeExchangeMessage {
            version: request.version,
            request_id: request.request_id.clone(),
            workload_id: request.workload_id,
            shard_id: descriptor.shard.shard_id,
            epoch: request.epoch,
            operator_id,
            lease_token: descriptor.shard.lease_token,
            source: request.source.clone(),
            rows,
        };
        if let Some(target) = senders.get(&descriptor.shard.worker_id) {
            send_message(target, &ControlMessage::Execute { frame }).await;
        }
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
) -> Result<(WorkerLifecycleState, u32, u32, DrainRequest), DrainFailure> {
    let Some(worker) = catalog.get(worker_id) else {
        return Err(DrainFailure::new(
            RS_3610,
            format!("worker {worker_id} is not present in the topology"),
            "Run `rockstream cluster status` to confirm the worker id, then retry the drain request.",
        ));
    };
    if matches!(
        worker.lifecycle,
        WorkerLifecycleState::Draining { .. } | WorkerLifecycleState::Decommissioned { .. }
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
    for shard_id in &shards {
        let eligible: Vec<_> = recipients
            .iter()
            .filter(|candidate| candidate.worker_id != worker_id)
            .cloned()
            .collect();
        let Some(recipient) = crate::placement::PlacementAlgorithm::choose_with_preference(
            &eligible,
            Some(&preferred_az),
        ) else {
            return Err(DrainFailure::new(
                RS_3611,
                format!("worker {worker_id} cannot drain shard {shard_id}: no active recipient worker is available"),
                "Register or recover at least one other active worker, then retry the drain request.",
            ));
        };
        chosen.push((*shard_id, recipient.worker_id));
    }

    let started_at_ms = now_ms();
    let lifecycle = WorkerLifecycleState::draining(shards.len() as u32, started_at_ms);
    let updated = catalog
        .set_lifecycle(worker_id, lifecycle.clone())
        .expect("worker existence checked above");
    persist_worker_if_needed(topology_store, &updated).await;

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
    for (shard_id, recipient_worker_id) in chosen {
        let migration_id = format!("drain-{worker_id}-{shard_id}-{}", guard.next_migration_id);
        guard.next_migration_id += 1;
        guard.queue.push_back(DrainTask {
            migration_id,
            donor_worker_id: worker_id,
            recipient_worker_id,
            shard_id,
        });
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

async fn process_drain_queue(
    catalog: &TopologyCatalog,
    shard_manager: &ShardManager,
    audit: Option<&Arc<FileAuditLog>>,
    shard_store: Option<&Arc<ShardPersistentStore>>,
    topology_store: Option<&Arc<TopologyPersistentStore>>,
    migration_store: Option<&Arc<MigrationPersistentStore>>,
    drain_state: &Arc<AsyncMutex<DrainState>>,
) {
    let mut local = Vec::new();
    {
        let mut guard = drain_state.lock().await;
        while let Some(task) = guard.queue.pop_front() {
            local.push(task);
        }
    }

    let coordinator = MigrationCoordinator::new();
    for task in local {
        let mut record = MigrationRecord::new(
            task.migration_id.clone(),
            vec![task.shard_id],
            task.shard_id,
            BucketSet::new([task.shard_id.0]),
            0,
            0,
        );
        if let Some(store) = migration_store {
            let _ = store.save(&record).await;
        }
        for state in [
            MigrationState::Snapshotting,
            MigrationState::Copying,
            MigrationState::DualWriting,
            MigrationState::CatchingUp,
            MigrationState::FencingOld,
            MigrationState::Cutover,
            MigrationState::Verifying,
            MigrationState::GcEligible,
            MigrationState::Done,
        ] {
            let _ = coordinator
                .begin_dual_writing(&mut record, audit.map(|value| value.as_ref()))
                .or_else(|_| {
                    coordinator
                        .advance_to_catching_up(&mut record, audit.map(|value| value.as_ref()))
                });
            let _ = record.apply_transition(state);
            if state == MigrationState::Cutover {
                record.cutover_epoch = Some(0);
            }
            if let Some(store) = migration_store {
                let _ = store.save(&record).await;
            }
        }
        if let Some(store) = migration_store {
            let _ = store
                .archive(&record, audit.map(|value| value.as_ref()))
                .await;
        }

        let _ = shard_manager.force_acquire(task.shard_id, task.recipient_worker_id);
        if let Some(store) = shard_store {
            persist_shard_state(shard_manager, store).await;
        }

        let current = catalog.get(task.donor_worker_id);
        if let Some(worker) = current {
            let remaining = shard_manager
                .leases()
                .into_iter()
                .filter(|lease| lease.worker_id == task.donor_worker_id)
                .count() as u32;
            let next_state = if remaining == 0 {
                WorkerLifecycleState::Decommissioned {
                    completed_at_ms: now_ms(),
                }
            } else {
                let mut state = worker.lifecycle.clone();
                state.advance_drain_progress(remaining, None, None);
                state
            };
            if let Some(updated) = catalog.set_lifecycle(task.donor_worker_id, next_state.clone()) {
                persist_worker_if_needed(topology_store, &updated).await;
            }
            if remaining == 0 {
                if let Some(audit) = audit {
                    let event = AuditEvent::now(
                        "control",
                        "worker.drain_completed",
                        task.donor_worker_id.to_string(),
                    )
                    .with_detail(format!("recipient={}", task.recipient_worker_id));
                    let _ = audit.append(&event);
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
                        if let Some(aud) = &ctx.audit {
                            let event = AuditEvent::now(
                                "control",
                                "security.internal_mtls_denied",
                                format!("peer={peer}"),
                            )
                            .with_detail(format!(
                                "identity extraction failed: {e}, error_code=RS-2411"
                            ));
                            let _ = aud.append(&event);
                        }
                        return;
                    }
                };
                if let Some(ref id) = identity {
                    if id.role == NodeRole::Cli {
                        if let Some(aud) = &ctx.audit {
                            let event = AuditEvent::now(
                                id.to_cn(),
                                "cli.authenticated",
                                format!("peer={peer}"),
                            );
                            let _ = aud.append(&event);
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
                if let Some(aud) = &ctx.audit {
                    let event = AuditEvent::now(
                        "control",
                        "security.internal_mtls_denied",
                        format!("peer={peer}"),
                    )
                    .with_detail(format!("TLS handshake failed: {e}, error_code={code}"));
                    let _ = aud.append(&event);
                }
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
        migration_store,
        drain_state,
        auto_drain,
        secret_store,
        worker_senders,
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

        match msg {
            WorkerMessage::Register(reg) => {
                if let Some(id) = &peer_identity {
                    if id.role != NodeRole::Worker
                        || (!id.matches_worker_id(reg.worker_id.0)
                            && !id.matches_worker_str(&reg.address))
                    {
                        tracing::warn!(
                            %peer,
                            cert_identity = %id.to_cn(),
                            registered_id = %reg.worker_id,
                            "control: worker mTLS identity mismatch rejected"
                        );
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "security.internal_mtls_denied",
                                format!("worker_id={}, peer={peer}", reg.worker_id),
                            )
                            .with_detail(format!(
                                "node identity mismatch: cert={}, requested_worker_id={}, error_code=RS-2412",
                                id.to_cn(),
                                reg.worker_id
                            ));
                            let _ = aud.append(&event);
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
                let reply = ControlMessage::Registered { worker_id };
                send_message(&sender, &reply).await;
                broadcast_message(
                    &worker_senders,
                    ControlMessage::TopologyChanged {
                        workers: catalog.healthy_workers(),
                    },
                )
                .await;
            }
            WorkerMessage::DeployWorkload(request) => {
                deploy_workload(
                    request,
                    &sender,
                    &catalog,
                    &shard_manager,
                    &worker_senders,
                    &data_plane,
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
                let reply =
                    {
                        let mut state = data_plane.lock().await;
                        let Some(deployment) = state.deployments.get_mut(&workload_id) else {
                            continue;
                        };
                        let valid = deployment
                            .descriptors
                            .get(&shard_id)
                            .map(|descriptor| {
                                descriptor.shard.worker_id == worker_id
                                    && operator_ids.contains(&descriptor.sink_operator_id)
                            })
                            .unwrap_or(false);
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
                                deployment.ready_waiter.take().map(|waiter| {
                                    (
                                        waiter,
                                        ControlMessage::DeploymentReady {
                                            workload_id,
                                            workers,
                                        },
                                    )
                                })
                            } else {
                                None
                            }
                        }
                    };
                if let Some((waiter, reply)) = reply {
                    send_message(&waiter, &reply).await;
                }
            }
            WorkerMessage::SubmitSourceDelta(request) => {
                submit_source_delta(request, &sender, &worker_senders, &data_plane).await;
            }
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
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
                    let worker_id = deployment.descriptors[&output.shard_id].shard.worker_id;
                    deployment
                        .outputs
                        .entry(output.shard_id)
                        .or_default()
                        .push(output.clone());
                    if let Some(status) = deployment.workers.get_mut(&worker_id) {
                        status.input_rows += input_rows;
                        status.output_rows += output_rows;
                        status.frontier = status.frontier.max(output.epoch);
                    }
                    let waiter = state.source_waiters.get_mut(&output.request_id);
                    waiter.and_then(|waiter| {
                        waiter.received += 1;
                        (waiter.received == waiter.expected).then(|| {
                            (
                                waiter.sender.clone(),
                                output.request_id.clone(),
                                waiter.epoch,
                            )
                        })
                    })
                };
                if let Some((waiter, request_id, epoch)) = completion {
                    data_plane.lock().await.source_waiters.remove(&request_id);
                    send_message(
                        &waiter,
                        &ControlMessage::SourceDeltaCommitted { request_id, epoch },
                    )
                    .await;
                }
            }
            WorkerMessage::ReadWorkload { workload_id } => {
                let snapshot = {
                    let state = data_plane.lock().await;
                    state.deployments.get(&workload_id).map(|deployment| {
                        let mut shards: Vec<_> = deployment
                            .descriptors
                            .keys()
                            .map(|shard_id| ShardOutput {
                                shard_id: *shard_id,
                                deltas: deployment
                                    .outputs
                                    .get(shard_id)
                                    .cloned()
                                    .unwrap_or_default(),
                            })
                            .collect();
                        shards.sort_by_key(|shard| shard.shard_id);
                        let mut workers: Vec<_> = deployment.workers.values().cloned().collect();
                        workers.sort_by_key(|status| status.worker_id);
                        WorkloadSnapshot {
                            deployment: deployment.request.clone(),
                            shards,
                            workers,
                        }
                    })
                };
                let reply = snapshot
                    .map(|snapshot| ControlMessage::WorkloadSnapshot { snapshot })
                    .unwrap_or_else(|| data_plane_failure("workload is not deployed"));
                send_message(&sender, &reply).await;
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
                let workers = catalog.healthy_workers();
                let notify = ControlMessage::TopologyChanged { workers };
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
                        let reply = ControlMessage::ShardAssigned { lease };
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
                let valid = shard_manager.is_valid_writer(shard_id, lease_token);
                tracing::debug!(
                    %shard_id,
                    token = lease_token.0,
                    valid,
                    "control: fence write check"
                );
                let reply = ControlMessage::FenceAck { shard_id, valid };
                send_message(&sender, &reply).await;
            }
            // v0.38 drain / lifecycle messages — acknowledged but not yet
            // fully handled by the control-plane service stub.
            WorkerMessage::DrainAck {
                worker_id,
                shards_remaining,
            } => {
                tracing::info!(
                    %worker_id,
                    shards_remaining,
                    "control: drain ack received"
                );
                let state = if shards_remaining == 0 {
                    WorkerLifecycleState::Decommissioned {
                        completed_at_ms: now_ms(),
                    }
                } else {
                    WorkerLifecycleState::draining(shards_remaining, now_ms())
                };
                if let Some(worker) = catalog.set_lifecycle(worker_id, state) {
                    persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                }
                if shards_remaining == 0 {
                    shard_manager.release_worker(worker_id);
                    let mut guard = drain_state.lock().await;
                    guard.queue.retain(|task| task.donor_worker_id != worker_id);
                }
            }
            WorkerMessage::LifecycleState { worker_id, state } => {
                tracing::info!(
                    %worker_id,
                    state = ?state,
                    "control: worker lifecycle state update"
                );
                if matches!(state, WorkerLifecycleState::Decommissioned { .. })
                    || matches!(state, WorkerLifecycleState::Draining { shards_remaining, .. } if shards_remaining == 0)
                {
                    shard_manager.release_worker(worker_id);
                    let mut guard = drain_state.lock().await;
                    guard.queue.retain(|task| task.donor_worker_id != worker_id);
                }
                if let Some(worker) = catalog.set_lifecycle(worker_id, state) {
                    persist_worker_if_needed(topology_store.as_ref(), &worker).await;
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
                let Some(identity) = peer_identity
                    .as_ref()
                    .filter(|id| id.role == NodeRole::Worker)
                else {
                    let reply = ControlMessage::OperationFailed {
                        code: RS_2410.to_string(),
                        message:
                            "secret token requests require an authenticated worker certificate"
                                .to_string(),
                        next_steps: rockstream_types::error_code::next_steps(RS_2410).to_string(),
                    };
                    send_message(&sender, &reply).await;
                    continue;
                };
                match secret_store
                    .issue_worker_token(0, &secret_name, &identity.node_id, 300, &identity.to_cn())
                    .await
                {
                    Ok(token) => {
                        send_message(&sender, &ControlMessage::SecretTokenIssued { token }).await
                    }
                    Err(error) => {
                        let reply = ControlMessage::OperationFailed {
                            code: error_code_for_secret_error(&error),
                            message: error.to_string(),
                            next_steps: secret_error_next_steps(&error),
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
                    if let Err(e) = agg
                        .ingest(rockstream_types::frontier::ShardFrontierReport { shard_id, epoch })
                    {
                        tracing::warn!(%shard_id, epoch, error = %e, "control: frontier ingest failed");
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
            WorkerMessage::RequestDrain { worker_id } => {
                match request_worker_drain(
                    &catalog,
                    &shard_manager,
                    audit.as_ref(),
                    topology_store.as_ref(),
                    &drain_state,
                    worker_id,
                )
                .await
                {
                    Ok((state, queue_fill, queue_capacity, request)) => {
                        send_message(&sender, &ControlMessage::BeginDrain(request)).await;
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

        if rotation_rx.has_changed().unwrap_or(false) {
            let rotation = {
                let current = rotation_rx.borrow_and_update();
                current.clone()
            };
            if let Some(rotation) = rotation {
                send_message(&sender, &ControlMessage::SecretRotated { rotation }).await;
            }
        }

        if auto_drain {
            process_drain_queue(
                &catalog,
                &shard_manager,
                audit.as_ref(),
                shard_store.as_ref(),
                topology_store.as_ref(),
                migration_store.as_ref(),
                &drain_state,
            )
            .await;
            cleanup_decommissioned_workers(&catalog, topology_store.as_ref()).await;
        }
    }

    // A dropped worker session is a death signal: remove it from live topology
    // and reassign its fenced shard leases to the remaining workers.
    if let Some(worker_id) = connected_worker_id {
        worker_senders.lock().await.remove(&worker_id);
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
            ControlMessage::TopologyChanged {
                workers: catalog.healthy_workers(),
            },
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
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, RaftRoleWire, WorkerLocation, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    async fn start_test_service() -> (ControlServiceHandle, TopologyCatalog) {
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone());
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
            ControlMessage::Registered { worker_id } => {
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
        let svc = ControlService::new(catalog.clone());
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
        let svc = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
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
            ControlMessage::ShardAssigned { lease } => {
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
            .with_raft(node.handle.clone());
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
        let stats = svc.collect_operator_stats(0);
        let stat = stats
            .iter()
            .find(|s| (s.rows_per_s - 10.0).abs() < 1e-9)
            .expect("expected driven operator stats");

        assert_ne!(stat.rows_per_s, 12500.0);
        assert_ne!(stat.state_reads, 120);
        assert_ne!(stat.p99_latency_ms, 12.0);
        assert_eq!(stat.rows_per_s, 10.0);
        assert_eq!(stat.state_reads, 30);
        assert!(
            (stat.rmw_ratio - 0.25).abs() < 1e-9,
            "rmw_ratio={}",
            stat.rmw_ratio
        );
        assert_eq!(stat.p99_latency_ms, 15.0);
        assert_eq!(stat.dlq_entries, 2);

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
        let svc = ControlService::new(catalog).with_raft(node.handle.clone());
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
        let frontier = Arc::new(FrontierAggregator::new());
        let svc = ControlService::new(catalog).with_frontier(frontier.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
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
        let frontier = Arc::new(FrontierAggregator::new());
        let svc = ControlService::new(catalog)
            .with_raft(node.handle.clone())
            .with_frontier(frontier.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
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
            .with_shard_store(Arc::new(ShardPersistentStore::new(shared_store.clone())));
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
            .with_shard_store(Arc::new(ShardPersistentStore::new(shared_store)));
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
            .with_shard_store(store.clone());
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

        if let ControlMessage::ShardAssigned { lease } = reply {
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
