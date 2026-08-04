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

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex as AsyncMutex};

use rockstream_types::error_code::{RS_3604, RS_3610, RS_3611, RS_3612};
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::lease::ShardRevokeReason;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};
use rockstream_types::topology::{
    ControlMessage, DrainRequest, RaftRoleWire, WorkerLifecycleState, WorkerMessage,
};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::frontier::FrontierAggregator;
use crate::migration::{MigrationCoordinator, MigrationPersistentStore};
use crate::raft::{RaftHandle, RaftRole};
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
}

impl ControlServiceHandle {
    /// Signal the service to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
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
        };

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
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = handle_connection(stream, peer, conn_ctx) => {}
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

        Ok(ControlServiceHandle { addr, shutdown_tx })
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
    let lifecycle = WorkerLifecycleState::Draining {
        shards_remaining: shards.len() as u32,
        started_at_ms,
    };
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
                match worker.lifecycle {
                    WorkerLifecycleState::Draining { started_at_ms, .. } => {
                        WorkerLifecycleState::Draining {
                            shards_remaining: remaining,
                            started_at_ms,
                        }
                    }
                    _ => WorkerLifecycleState::Draining {
                        shards_remaining: remaining,
                        started_at_ms: now_ms(),
                    },
                }
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

/// Handle a single worker connection.
async fn handle_connection(stream: TcpStream, peer: SocketAddr, ctx: ConnectionContext) {
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
    } = ctx;
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut connected_worker_id: Option<rockstream_types::ids::WorkerId> = None;

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
                let worker_id = catalog.register(&reg);
                connected_worker_id = Some(worker_id);
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
                                "address={}, host_id={}, availability_zone={}, headroom={}, same_host_arrow_shm_v1={}, shuffle_codec_v1={}, checkpoint_manifest_codec_v1={}",
                                reg.address,
                                reg.location.host_id,
                                reg.location.availability_zone,
                                reg.capacity_headroom,
                                reg.capabilities.same_host_arrow_shm_v1,
                                reg.capabilities.shuffle_codec_v1,
                                reg.capabilities.checkpoint_manifest_codec_v1
                            ));
                    let _ = aud.append(&event);
                }
                if let Some(worker) = catalog.get(worker_id) {
                    persist_worker_if_needed(topology_store.as_ref(), &worker).await;
                }
                let reply = ControlMessage::Registered { worker_id };
                send_message(&mut writer, &reply).await;
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
                        send_message(&mut writer, &revoke).await;
                    }
                }
                // Notify remaining workers about topology change.
                let workers = catalog.healthy_workers();
                let notify = ControlMessage::TopologyChanged { workers };
                send_message(&mut writer, &notify).await;
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
                    send_message(&mut writer, &reply).await;
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
                        send_message(&mut writer, &reply).await;
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
                        send_message(&mut writer, &reply).await;
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
                send_message(&mut writer, &reply).await;
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
            }
            WorkerMessage::LifecycleState { worker_id, state } => {
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
                send_message(&mut writer, &reply).await;
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
                        send_message(&mut writer, &reply).await;
                    }
                } else if let Some(rft) = &raft {
                    let reply = ControlMessage::NotLeader {
                        current_leader: rft.current_leader(),
                    };
                    send_message(&mut writer, &reply).await;
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
                        send_message(&mut writer, &ControlMessage::BeginDrain(request)).await;
                        let status = ControlMessage::DrainStatus {
                            worker_id,
                            state,
                            queue_fill,
                            queue_capacity,
                        };
                        send_message(&mut writer, &status).await;
                    }
                    Err(err) => {
                        let reply = drain_failure_message(err);
                        send_message(&mut writer, &reply).await;
                    }
                }
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

    // Connection dropped without explicit deregister: release all shard leases
    // but keep the topology catalog entry (it stays until explicit Deregister).
    if let Some(worker_id) = connected_worker_id {
        let freed = shard_manager.release_worker(worker_id);
        if !freed.is_empty() {
            tracing::info!(
                %worker_id,
                freed_shards = freed.len(),
                "control: released shard leases on disconnect"
            );
            if let Some(aud) = &audit {
                let event = AuditEvent::now(
                    "control",
                    "worker.shards_released_on_disconnect",
                    worker_id.to_string(),
                )
                .with_detail(format!("freed_shards={}", freed.len()));
                let _ = aud.append(&event);
            }
            if let Some(store) = &shard_store {
                persist_shard_state(&shard_manager, store).await;
            }
        }
    }

    tracing::debug!(%peer, "control: connection closed");
}

async fn send_message(writer: &mut tokio::net::tcp::OwnedWriteHalf, msg: &ControlMessage) {
    match serde_json::to_string(msg) {
        Ok(mut line) => {
            line.push('\n');
            if let Err(e) = writer.write_all(line.as_bytes()).await {
                tracing::warn!(error = %e, "control: write error");
            }
        }
        Err(e) => {
            tracing::error!(
                code = %rockstream_types::error_code::RS_0001,
                error = %e,
                "control: failed to serialize message"
            );
        }
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
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        resp
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
        let mut resp2 = String::new();
        let read_result = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            reader_b.read_line(&mut resp2),
        )
        .await;
        assert!(
            read_result.is_err(),
            "split-brain: node B replied to worker 20's conflicting request \
             instead of silently denying it (resp={resp2:?})"
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
