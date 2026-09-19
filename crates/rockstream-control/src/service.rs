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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex};

use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::data_plane::{
    DeploymentDescriptor, DeploymentRequest, ShardOutput, WorkerExecutionStatus, WorkloadSnapshot,
};
use rockstream_types::error_code::{
    RS_2410, RS_2411, RS_2412, RS_3604, RS_3610, RS_3611, RS_3612, RS_8004,
};
use rockstream_types::identity::{InternalTlsConfig, NodeIdentity, NodeRole};
use rockstream_types::ids::{ShardId, WorkerId, WorkloadId};
use rockstream_types::lease::ShardRevokeReason;
use rockstream_types::topology::{
    ControlMessage, DrainRequest, RaftRoleWire, WorkerLifecycleState, WorkerMessage,
};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::frontier::FrontierAggregator;
use crate::management_store::{ManagementOperationStore, OperationStatus, OperationUpdate};
use crate::migration::MigrationPersistentStore;
use crate::placement::PlacementAlgorithm;
use crate::raft::{RaftHandle, RaftRole};
use crate::scheduler::ShardScheduler;
use crate::secret_store::{SecretStore, SecretStoreError};
use crate::shard::{ShardManager, ShardPersistentStore};
use crate::topology::{TopologyCatalog, TopologyPersistentStore};

const DEFAULT_DRAIN_DEADLINE_MS: u64 = 30_000;
const DEFAULT_DECOMMISSION_GRACE_MS: u64 = 5_000;
const MAX_DRAIN_QUEUE: usize = 1024;
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

fn secret_error_next_steps(error: &SecretStoreError) -> String {
    rockstream_types::error_code::next_steps(secret_error_code(error)).to_string()
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
    deployments: HashMap<WorkloadId, DeploymentState>,
    source_waiters: HashMap<String, SourceWaiter>,
}

struct DeploymentState {
    request: DeploymentRequest,
    descriptors: HashMap<ShardId, DeploymentDescriptor>,
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
    management: Option<crate::management::ManagementServiceHandle>,
    /// TLS certificate reloader (if internal mTLS is enabled).
    pub reloader: Option<Arc<crate::tls::TlsCertificateReloader>>,
}

impl ControlServiceHandle {
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
    /// Shared drain queue state with a named bound.
    drain_state: Arc<AsyncMutex<DrainState>>,
    /// Optional internal TLS configuration for control plane mTLS.
    internal_tls: Option<InternalTlsConfig>,
    secret_store: Arc<SecretStore>,
    worker_senders: Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    data_plane: Arc<AsyncMutex<DataPlaneState>>,
    management: Option<(
        String,
        Arc<dyn object_store::ObjectStore>,
        rockstream_types::config::NodeConfig,
    )>,
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
            drain_state: Arc::new(AsyncMutex::new(DrainState::default())),
            internal_tls: None,
            secret_store: Arc::new(SecretStore::new(
                None,
                Arc::new(crate::kek::EnvKekProvider::from_env_or_default(
                    "rockstream-default-kek",
                )),
            )),
            worker_senders: Arc::new(AsyncMutex::new(HashMap::new())),
            data_plane: Arc::new(AsyncMutex::new(DataPlaneState::default())),
            management: None,
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
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(addr = %addr, "control service listening");

        let operation_store = self
            .management
            .as_ref()
            .map(|(_, store, _)| ManagementOperationStore::new(store.clone()));
        let migration_waiters = Arc::new(AsyncMutex::new(HashMap::new()));
        let backup_waiters = Arc::new(AsyncMutex::new(HashMap::new()));
        let management = if let Some((management_addr, _, config)) = &self.management {
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
                    manifests: crate::checkpoint_store::CheckpointManifestStore::new(source),
                    started: Arc::new(AsyncMutex::new(HashSet::new())),
                    serial: Arc::new(AsyncMutex::new(())),
                })
            } else {
                management
            };
            Some(management.start(management_addr).await?)
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
            worker_senders: self.worker_senders.clone(),
            migration_waiters,
            backup_waiters,
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

        Ok(ControlServiceHandle {
            addr,
            shutdown_tx,
            management,
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
    operation_store: Option<ManagementOperationStore>,
    drain_state: Arc<AsyncMutex<DrainState>>,
    secret_store: Arc<SecretStore>,
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

        let active_workload = self
            .data_plane
            .lock()
            .await
            .deployments
            .values()
            .any(|deployment| {
                deployment
                    .descriptors
                    .values()
                    .any(|descriptor| descriptor.shard.worker_id == worker_id)
            });
        if active_workload {
            self.fail(
                operation_id,
                "RS-3604",
                "worker has active workloads; stop them before draining in v0.66",
                "Retry after the worker has no active workload deployments.",
            )
            .await;
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
        if matches!(
            previous.phase(),
            None | Some("validating_lease_and_workers")
        ) && self
            .data_plane
            .lock()
            .await
            .deployments
            .values()
            .any(|deployment| deployment.descriptors.contains_key(&shard_id))
        {
            self.fail(
                operation_id,
                "RS-3604",
                "shard has active workloads; stop them before migrating in v0.66",
            )
            .await;
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
            return;
        }
        if self.shard_manager.get(shard_id) != Some(lease.clone()) {
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
            return;
        }
        let Some(new_lease) = self.shard_manager.transfer_if_owner(&lease, target) else {
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
                    self.handle_export_error(operation_id, error).await;
                    return;
                }
            }
            Err(error) => {
                self.handle_export_error(operation_id, error).await;
                return;
            }
        }
        match exporter
            .validate_generation(destination_store, &generation)
            .await
        {
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
                    "RS-5035",
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
        let state = self.data_plane.lock().await;
        if !state.deployments.is_empty() || !state.source_waiters.is_empty() {
            return Err(BackupFailure::Failed(
                "RS-3604",
                "CreateBackup requires an idle cluster with no active workloads or source writes"
                    .to_owned(),
            ));
        }
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
                    checkpoint.record_shard(
                        lease.shard_id,
                        PerShardCheckpoint::new(checkpoint_id, shard_checkpoint_id)
                            .with_snapshot_id(snapshot_id),
                    );
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
        drop(state);
        Ok(checkpoint)
    }

    async fn handle_export_error(
        &self,
        operation_id: &str,
        error: crate::checkpoint_export::CheckpointExportError,
    ) {
        match error {
            crate::checkpoint_export::CheckpointExportError::Integrity(message) => {
                self.fail(operation_id, "RS-5035", &message).await;
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
                    operation_id: None,
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
    _worker_senders: &Arc<AsyncMutex<HashMap<WorkerId, mpsc::Sender<ControlMessage>>>>,
    _data_plane: &Arc<AsyncMutex<DataPlaneState>>,
) {
    if !request.rows.is_empty() {
        send_message(
            sender,
            &ControlMessage::OperationFailed {
                code: rockstream_types::error_code::RS_3001.to_string(),
                message: "control plane does not route data plane row payloads; stream directly to worker shard owner".into(),
                next_steps: "Resolve shard placement from control metadata and send record batches directly via data plane gRPC".into(),
            },
        ).await;
        return;
    }

    send_message(
        sender,
        &ControlMessage::SourceDeltaCommitted {
            request_id: request.request_id,
            epoch: request.epoch,
        },
    )
    .await;
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
        let lease = if current_lease.worker_id == task.donor_worker_id {
            let (lease, evicted) = shard_manager.force_acquire(task.shard_id, recipient.worker_id);
            if evicted != Some(task.donor_worker_id) {
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
        if donor_sender
            .send(ControlMessage::ShardRevoked {
                shard_id: task.shard_id,
                reason: ShardRevokeReason::WorkerDrain,
            })
            .await
            .is_err()
        {
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
                moved += 1
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
        operation_store,
        drain_state,
        secret_store,
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
                                deltas: Vec::new(),
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
                send_message(&sender, &ControlMessage::SecretRotated { rotation }).await;
            }
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
    use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, RaftRoleWire, WorkerCapabilities, WorkerLifecycleState,
        WorkerLocation, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

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
            .with_frontier(frontier.clone());
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
