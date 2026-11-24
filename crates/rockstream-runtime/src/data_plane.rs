//! Direct Arrow IPC & control-metadata data-plane client (Issue #400 R1/R6).
//!
//! Control-plane metadata operations (`deploy`, `deploy_with_shards`,
//! `get_workload_topology`, `discover_shards`, connector leases) talk to
//! `ControlService` over internal mTLS (or explicit loopback dev mode).
//!
//! Row ingestion (`submit_delta`, `submit_arrow_batch`) partitions batches
//! client-side using cached, versioned placement metadata
//! (`WorkloadTopologySnapshot`) and streams Arrow IPC frames directly to shard
//! owners over persistent multiplexed gRPC channels (`ShuffleClientPool` +
//! `WorkerStreamMultiplexer`), with **zero row payloads or TSV strings passing
//! through `ControlService`**. Stale topology epochs refresh and redirect
//! cleanly; malformed frames, missing routing keys, unauthorized peers, or
//! evicted leases fail closed with documented `RS-*` error codes.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use parking_lot::RwLock;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::data_plane::{
    ConnectorLease, DeploymentRequest, RuntimeExchangeMessage, RuntimeRow, SourceDeltaRequest,
    WorkloadSnapshot, WorkloadTopologySnapshot, DEPLOYMENT_DESCRIPTOR_VERSION,
    WORKLOAD_SNAPSHOT_VERSION,
};
use rockstream_types::error_code::{RS_0001, RS_1012, RS_2405, RS_3004};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{ConnectorId, LeaseToken, ShardId, WorkloadId};
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};
use rockstream_types::topology::{ControlMessage, WorkerMessage};

const DEFAULT_VIRTUAL_BUCKET_COUNT: u64 = 4096;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::exchange::flow_control::FlowController;
use crate::exchange::multiplexer::WorkerStreamMultiplexer;
use crate::exchange::pool::ShuffleClientPool;

static NEXT_OPERATOR_STATS_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

const MAX_STALE_TOPOLOGY_RETRIES: usize = 3;

#[derive(Clone)]
pub struct DataPlaneClient {
    control_addr: String,
    tls_connector: Option<TlsConnector>,
    internal_tls: Option<InternalTlsConfig>,
    insecure_internal_dev: bool,
    topology_cache: Arc<RwLock<HashMap<WorkloadId, WorkloadTopologySnapshot>>>,
    shuffle_pool: ShuffleClientPool,
    shuffle_mux: WorkerStreamMultiplexer,
    direct_grant: Option<rockstream_types::direct_epoch::DirectEpochGrant>,
}

pub type ControlDataPlaneClient = DataPlaneClient;

impl DataPlaneClient {
    pub fn new(control_addr: impl Into<String>) -> Self {
        let shuffle_pool = ShuffleClientPool::default();
        let shuffle_mux = WorkerStreamMultiplexer::new(shuffle_pool.clone(), FlowController::new());
        Self {
            control_addr: control_addr.into(),
            tls_connector: None,
            internal_tls: None,
            insecure_internal_dev: false,
            topology_cache: Arc::new(RwLock::new(HashMap::new())),
            shuffle_pool,
            shuffle_mux,
            direct_grant: None,
        }
    }

    pub fn with_internal_tls(mut self, config: &InternalTlsConfig) -> Result<Self, String> {
        if !config.client_auth_required || !config.is_enabled() {
            return Err("RS-2405: data-plane client requires internal mTLS".to_owned());
        }
        config.validate()?;
        self.tls_connector = Some(crate::tls::build_client_tls_connector(config)?);
        self.internal_tls = Some(config.clone());
        self.shuffle_pool = ShuffleClientPool::default().with_internal_tls(config.clone());
        self.shuffle_mux =
            WorkerStreamMultiplexer::new(self.shuffle_pool.clone(), FlowController::new());
        Ok(self)
    }

    /// Enable plaintext only for explicitly authorized loopback development.
    pub fn with_insecure_internal_dev(mut self) -> Self {
        self.insecure_internal_dev = true;
        self
    }

    pub fn uses_insecure_internal_dev(&self) -> bool {
        self.insecure_internal_dev && self.tls_connector.is_none()
    }

    /// Return the cached placement snapshot for `workload_id`, if present.
    pub fn cached_topology(&self, workload_id: WorkloadId) -> Option<WorkloadTopologySnapshot> {
        self.topology_cache.read().get(&workload_id).cloned()
    }

    /// Evict cached placement metadata for `workload_id`, forcing a refresh on
    /// the next direct write.
    pub fn invalidate_topology(&self, workload_id: WorkloadId) {
        self.topology_cache.write().remove(&workload_id);
    }

    /// Override the cached placement metadata for `workload_id` (useful for
    /// stale-topology redirect verification tests).
    pub fn seed_cached_topology(&self, snapshot: WorkloadTopologySnapshot) {
        self.shuffle_pool
            .replace_peer_infos(snapshot.workers.clone());
        self.topology_cache
            .write()
            .insert(snapshot.workload_id, snapshot);
    }

    async fn request_with_assertion(
        &self,
        request: WorkerMessage,
        actor_assertion: Option<&str>,
    ) -> io::Result<ControlMessage> {
        let addr = self
            .control_addr
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let stream_addr = addr.parse::<SocketAddr>().ok();
        if self.tls_connector.is_none()
            && (!self.insecure_internal_dev
                || !stream_addr.is_some_and(|address| address.ip().is_loopback()))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{RS_2405}: data-plane plaintext is restricted to explicitly enabled loopback development"
                ),
            ));
        }
        let request = if let Some(actor_assertion) = actor_assertion {
            WorkerMessage::DelegatedDataPlane {
                version: rockstream_types::topology::DATA_PLANE_DELEGATION_VERSION,
                actor_assertion: actor_assertion.to_owned(),
                request: Box::new(request),
            }
        } else {
            request
        };
        let stream = TcpStream::connect(addr).await?;
        if let Some(connector) = &self.tls_connector {
            let server_name = data_plane_server_name(addr)?;
            let stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("RS-2411: data-plane TLS handshake failed: {error}"),
                    )
                })?;
            Self::exchange(stream, request).await
        } else {
            Self::exchange(stream, request).await
        }
    }

    async fn exchange<S>(mut stream: S, request: WorkerMessage) -> io::Result<ControlMessage>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let wire = serde_json::to_string(&request)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            + "\n";
        stream.write_all(wire.as_bytes()).await?;
        let mut line = String::new();
        let mut reader = BufReader::new(stream);
        reader.read_line(&mut line).await?;
        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "control closed without a response",
            ));
        }
        let response = serde_json::from_str(&line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
        reader.get_mut().shutdown().await?;
        response
    }

    fn unexpected(response: ControlMessage) -> io::Error {
        match response {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => {
                let message = if message.contains(&next_steps) {
                    format!("{code}: {message}")
                } else {
                    format!("{code}: {message}; {next_steps}")
                };
                io::Error::other(message)
            }
            other => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected control response: {other:?}"),
            ),
        }
    }

    pub async fn deploy(&self, request: DeploymentRequest) -> io::Result<()> {
        self.deploy_request(request, None, None).await
    }

    /// Deploy a workload with an explicit logical shard count decoupled from
    /// the current healthy worker count (Issue #400 R5).
    pub async fn deploy_with_shards(
        &self,
        request: DeploymentRequest,
        shard_count: usize,
    ) -> io::Result<()> {
        self.deploy_request(request, Some(shard_count), None).await
    }

    pub async fn deploy_delegated(
        &self,
        request: DeploymentRequest,
        actor_assertion: &str,
    ) -> io::Result<()> {
        self.deploy_request(request, None, Some(actor_assertion))
            .await
    }

    async fn deploy_request(
        &self,
        request: DeploymentRequest,
        shard_count: Option<usize>,
        actor_assertion: Option<&str>,
    ) -> io::Result<()> {
        let workload_id = request.workload_id;
        let msg = match shard_count {
            Some(count) => WorkerMessage::DeployWorkloadSharded {
                request,
                shard_count: count,
            },
            None => WorkerMessage::DeployWorkload(request),
        };
        match self.request_with_assertion(msg, actor_assertion).await? {
            ControlMessage::DeploymentReady {
                workload_id: actual,
                ..
            } if actual == workload_id => {
                self.invalidate_topology(workload_id);
                Ok(())
            }
            response => Err(Self::unexpected(response)),
        }
    }

    /// Fetch fresh versioned placement metadata for `workload_id` from
    /// `ControlService` (metadata only; zero row payloads) and update the local
    /// routing cache and `ShuffleClientPool`.
    pub async fn get_workload_topology(
        &self,
        workload_id: WorkloadId,
    ) -> io::Result<WorkloadTopologySnapshot> {
        self.get_workload_topology_with_assertion(workload_id, None)
            .await
    }

    async fn get_workload_topology_with_assertion(
        &self,
        workload_id: WorkloadId,
        actor_assertion: Option<&str>,
    ) -> io::Result<WorkloadTopologySnapshot> {
        match self
            .request_with_assertion(
                WorkerMessage::GetWorkloadTopology { workload_id },
                actor_assertion,
            )
            .await?
        {
            ControlMessage::WorkloadTopology { snapshot } => {
                self.shuffle_pool
                    .replace_peer_infos(snapshot.workers.clone());
                self.topology_cache
                    .write()
                    .insert(workload_id, snapshot.clone());
                Ok(snapshot)
            }
            response => Err(Self::unexpected(response)),
        }
    }

    /// Acquire an explicit single-owner connector lease from `ControlService`
    /// so multiple gateway replicas never duplicate ingestion (Issue #400 R1).
    pub async fn acquire_connector_lease(
        &self,
        connector_id: ConnectorId,
        owner_id: impl Into<String>,
        ttl_ms: u64,
    ) -> io::Result<ConnectorLease> {
        match self
            .request_with_assertion(
                WorkerMessage::AcquireConnectorLease {
                    connector_id,
                    owner_id: owner_id.into(),
                    ttl_ms,
                },
                None,
            )
            .await?
        {
            ControlMessage::ConnectorLeaseGranted { lease } => Ok(lease),
            ControlMessage::ConnectorLeaseDenied { code, message, .. } => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{code}: {message}"),
            )),
            response => Err(Self::unexpected(response)),
        }
    }

    /// Renew an active single-owner connector lease.
    pub async fn renew_connector_lease(
        &self,
        connector_id: ConnectorId,
        owner_id: impl Into<String>,
        lease_token: LeaseToken,
        ttl_ms: u64,
    ) -> io::Result<ConnectorLease> {
        match self
            .request_with_assertion(
                WorkerMessage::RenewConnectorLease {
                    connector_id,
                    owner_id: owner_id.into(),
                    lease_token,
                    ttl_ms,
                },
                None,
            )
            .await?
        {
            ControlMessage::ConnectorLeaseGranted { lease } => Ok(lease),
            ControlMessage::ConnectorLeaseDenied { code, message, .. } => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{code}: {message}"),
            )),
            response => Err(Self::unexpected(response)),
        }
    }

    /// Release a held single-owner connector lease.
    pub async fn release_connector_lease(
        &self,
        connector_id: ConnectorId,
        owner_id: impl Into<String>,
        lease_token: LeaseToken,
    ) -> io::Result<bool> {
        match self
            .request_with_assertion(
                WorkerMessage::ReleaseConnectorLease {
                    connector_id,
                    owner_id: owner_id.into(),
                    lease_token,
                },
                None,
            )
            .await?
        {
            ControlMessage::ConnectorLeaseReleased { released, .. } => Ok(released),
            response => Err(Self::unexpected(response)),
        }
    }

    /// Submit a source delta using the direct Arrow IPC path to shard owners
    /// (bypassing `ControlService` for all row payloads).
    pub async fn submit_delta(&self, request: SourceDeltaRequest) -> io::Result<u64> {
        if self.uses_insecure_internal_dev() {
            self.submit_delta_with_assertion(request, None).await
        } else {
            self.submit_delta_direct(request).await
        }
    }

    pub async fn submit_delta_delegated(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: &str,
    ) -> io::Result<u64> {
        self.submit_delta_via_control(request, Some(actor_assertion))
            .await
    }

    /// Metadata request signed by the gateway actor. Source rows stay on the
    /// direct worker path; current policy and every target fence are checked
    /// before control installs the bounded capability on recipients.
    pub fn source_authorization_request(request: &SourceDeltaRequest) -> io::Result<WorkerMessage> {
        Ok(WorkerMessage::AuthorizeSourceEpoch {
            metadata: rockstream_types::direct_epoch::SourceEpochMetadata::from_request(request)
                .map_err(io::Error::other)?,
        })
    }

    pub async fn submit_delta_authorized(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: &str,
    ) -> io::Result<u64> {
        self.submit_delta_with_assertion(request, Some(actor_assertion))
            .await
    }

    async fn submit_delta_with_assertion(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: Option<&str>,
    ) -> io::Result<u64> {
        let metadata = Self::source_authorization_request(&request)?;
        let mut last_error = None;
        for _ in 0..MAX_STALE_TOPOLOGY_RETRIES {
            let reply = self
                .request_with_assertion(metadata.clone(), actor_assertion)
                .await?;
            let (grant, topology) = match reply {
                ControlMessage::DirectEpochAuthorized { grant, topology } => (grant, topology),
                ControlMessage::SourceDeltaCommitted { request_id, epoch }
                    if request_id == request.request_id && epoch == request.epoch =>
                {
                    return Ok(epoch);
                }
                reply => {
                    let error = Self::unexpected(reply);
                    if error.to_string().contains("RS-3004") {
                        last_error = Some(error);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                    return Err(error);
                }
            };
            let mut authorized = self.clone();
            authorized.direct_grant = Some(grant);
            authorized.seed_cached_topology(topology);
            match authorized.submit_delta_direct(request.clone()).await {
                Ok(epoch) => return Ok(epoch),
                Err(error)
                    if error.to_string().contains("RS-3004")
                        || error.to_string().contains("RS-1702")
                        || error.to_string().contains("RS-2401: direct epoch capability is unknown or evicted")
                        || error.to_string().contains("RS-2401: direct epoch capability is expired or bound to another request") =>
                {
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::other("RS-3004: direct epoch ownership changed during authorization")
        }))
    }

    pub(crate) fn owned_context<T: serde::Serialize>(&self, context: &T) -> io::Result<Vec<u8>> {
        match &self.direct_grant {
            Some(grant) => {
                serde_json::to_vec(&rockstream_types::direct_epoch::AuthorizedOwnedContext {
                    direct_grant_id: grant.grant_id,
                    metadata: grant.metadata.clone(),
                    inner: serde_json::to_value(context).map_err(io::Error::other)?,
                })
            }
            None => serde_json::to_vec(context),
        }
        .map_err(io::Error::other)
    }

    /// Partition and stream a native [`ArrowZSet`] directly to shard owners
    /// over persistent multiplexed gRPC channels (`ShuffleClientPool`) with
    /// zero TSV conversion and zero row payloads through `ControlService`.
    pub async fn submit_arrow_batch(
        &self,
        workload_id: WorkloadId,
        epoch: u64,
        request_id: String,
        source: String,
        zset: &ArrowZSet,
    ) -> io::Result<u64> {
        let rows = crate::client::zset_to_rows(zset)?;
        self.submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id,
            workload_id,
            epoch,
            source,
            rows,
        })
        .await
    }

    /// Direct Arrow IPC ingestion path: uses cached versioned placement
    /// metadata (`WorkloadTopologySnapshot`) and persistent multiplexed gRPC
    /// channels (`ShuffleClientPool`), refreshing cleanly on stale topology
    /// epochs without sending any row payloads through `ControlService`.
    pub async fn submit_delta_direct(&self, request: SourceDeltaRequest) -> io::Result<u64> {
        let mut last_err: Option<io::Error> = None;
        for attempt in 0..MAX_STALE_TOPOLOGY_RETRIES {
            let topology = match self.cached_topology(request.workload_id) {
                Some(cached) => cached,
                None => match self.get_workload_topology(request.workload_id).await {
                    Ok(fresh) => fresh,
                    Err(refresh_err) => return Err(last_err.unwrap_or(refresh_err)),
                },
            };
            // Check whether every shard owner advertises a reachable direct
            // exchange address and is currently healthy in topology.
            if topology.shards.is_empty() {
                return self.submit_delta_via_control(request, None).await;
            }
            let active_worker_ids: std::collections::HashSet<_> = topology
                .workers
                .iter()
                .filter(|w| w.healthy)
                .map(|w| w.worker_id)
                .collect();
            if topology
                .shards
                .iter()
                .any(|s| !active_worker_ids.contains(&s.worker_id))
            {
                return Err(io::Error::other(format!(
                    "{RS_0001}: a routed shard owner has no active worker connection; Retry after the workload and workers are ready."
                )));
            }
            let all_direct_capable = topology
                .shards
                .iter()
                .all(|s| !s.worker_address.trim().is_empty());
            if !all_direct_capable {
                return self.submit_delta_via_control(request, None).await;
            }

            // Ensure ShuffleClientPool has peer info from the cached topology.
            for w in &topology.workers {
                self.shuffle_pool.upsert_peer_info(w.clone());
            }

            match self.submit_skew_epoch(&topology, &request).await {
                Ok(true) => return Ok(request.epoch),
                Ok(false) => {}
                Err(error) => {
                    if self.direct_grant.is_some() {
                        return Err(error);
                    }
                    let message = error.to_string();
                    if attempt + 1 < MAX_STALE_TOPOLOGY_RETRIES
                        && (message.contains("RS-3004")
                            || message.contains("RS-1702")
                            || message.contains("not deployed")
                            || message.contains("transport")
                            || message.contains("connection"))
                    {
                        self.invalidate_topology(request.workload_id);
                        last_err = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }

            let partitioned = Self::partition_request(&topology, &request)?;
            let schemas = crate::client::deployment_schemas_from_slice(&topology.schemas)?;
            let source_schema = schemas.get(&request.source).cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{RS_1012}: unknown source relation '{}' for workload {}",
                        request.source, request.workload_id
                    ),
                )
            })?;

            let mut stale_detected = false;
            for (shard, rows) in partitioned {
                let zset = crate::client::rows_to_zset(&rows, source_schema.clone())?;
                let row_count = u32::try_from(zset.num_rows()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{RS_1012}: batch row count exceeds u32"),
                    )
                })?;
                let payload = crate::exchange::serialization::serialize_zset(&zset)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let owner_context = RuntimeExchangeMessage {
                    join_bootstrap: None,
                    attempt_id: None,
                    version: request.version,
                    request_id: request.request_id.clone(),
                    workload_id: request.workload_id,
                    shard_id: shard.shard_id,
                    epoch: request.epoch,
                    operator_id: shard.operator_id,
                    lease_token: shard.lease_token,
                    source: request.source.clone(),
                    rows: Vec::new(),
                };
                let context_bytes = self.owned_context(&owner_context)?;
                let shuffle = crate::exchange::proto::ShuffleFrame {
                    exchange_id: request.workload_id.0,
                    src_shard: 0,
                    target_shard: shard.shard_index,
                    epoch: request.epoch,
                    seq: 0,
                    payload: payload.into(),
                    row_count,
                };
                match self
                    .shuffle_mux
                    .send_owned_frame(shard.worker_id, shuffle, context_bytes.into())
                    .await
                {
                    Ok(()) => {}
                    Err(err) => {
                        let is_stale = err.contains("RS-1702")
                            || err.contains("RS-3004")
                            || err.contains("stale")
                            || err.contains("not deployed")
                            || err.contains("lease fence");
                        let is_connect_failure = err.contains("tcp connect error")
                            || err.contains("transport error")
                            || err.contains("Connection refused")
                            || err.contains("connection closed");
                        if (is_stale || is_connect_failure)
                            && attempt + 1 < MAX_STALE_TOPOLOGY_RETRIES
                        {
                            self.invalidate_topology(request.workload_id);
                            self.shuffle_pool.evict_worker(shard.worker_id);
                            last_err = Some(if is_connect_failure {
                                io::Error::other(format!(
                                    "{RS_0001}: a routed shard owner has no active worker connection; Retry after the workload and workers are ready."
                                ))
                            } else {
                                io::Error::new(
                                    io::ErrorKind::PermissionDenied,
                                    format!("{RS_3004}: stale topology or lease ({err})"),
                                )
                            });
                            stale_detected = true;
                            break;
                        }
                        if is_connect_failure {
                            return Err(io::Error::other(format!(
                                "{RS_0001}: a routed shard owner has no active worker connection; Retry after the workload and workers are ready."
                            )));
                        }
                        return Err(io::Error::new(
                            if is_stale {
                                io::ErrorKind::PermissionDenied
                            } else {
                                io::ErrorKind::Other
                            },
                            err,
                        ));
                    }
                }
            }
            if !stale_detected {
                return Ok(request.epoch);
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!(
                "{RS_3004}: direct routing failed after topology refresh"
            ))
        }))
    }

    /// Exact persisted routing decisions, including trigger thresholds,
    /// admission fallback keys and partial worker row counts.
    pub async fn routing_diagnostics(
        &self,
        workload_id: WorkloadId,
    ) -> io::Result<crate::skew_routing::RoutingSnapshot> {
        let topology = self.get_workload_topology(workload_id).await?;
        for worker in &topology.workers {
            self.shuffle_pool.upsert_peer_info(worker.clone());
        }
        let anchor = crate::skew_routing::stable_shards(&topology)
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("workload has no routing anchor"))?;
        self.routing_command(
            &topology,
            anchor,
            0,
            crate::skew_routing::RoutingOperation::Read,
            &serde_json::Value::Null,
        )
        .await
    }

    /// Bounded runtime decisions visible through the same delegated view-read
    /// policy as EXPLAIN ANALYZE. Raw source values are never returned here.
    pub async fn adaptive_diagnostics(
        &self,
        workload_id: WorkloadId,
        actor_assertion: Option<&str>,
    ) -> io::Result<Vec<serde_json::Value>> {
        match self
            .request_with_assertion(
                WorkerMessage::ReadAdaptiveDiagnostics { workload_id },
                actor_assertion,
            )
            .await?
        {
            ControlMessage::AdaptiveDiagnostics {
                workload_id: actual,
                events,
            } if actual == workload_id => Ok(events),
            response => Err(Self::unexpected(response)),
        }
    }

    async fn routing_command<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        topology: &WorkloadTopologySnapshot,
        shard_id: ShardId,
        epoch: u64,
        operation: crate::skew_routing::RoutingOperation,
        payload: &T,
    ) -> io::Result<R> {
        let shard = topology
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| io::Error::other("RS-3004: routing shard is absent"))?;
        let command = crate::skew_routing::RoutingCommand {
            routing_version: crate::skew_routing::ROUTING_VERSION,
            workload_id: topology.workload_id,
            shard_id,
            lease_token: shard.lease_token,
            operation,
        };
        let context = self.owned_context(&command)?;
        let payload = serde_json::to_vec(payload).map_err(io::Error::other)?;
        if payload.len() > crate::skew_routing::MAX_ROUTING_METADATA_BYTES {
            return Err(io::Error::other(
                "routing command exceeds per-epoch byte budget",
            ));
        }
        let frame = crate::exchange::proto::ShuffleFrame {
            exchange_id: topology.workload_id.0,
            src_shard: 0,
            target_shard: shard.shard_index,
            epoch,
            seq: 0,
            payload,
            row_count: 0,
        };
        let result = self
            .shuffle_mux
            .send_owned_command(shard.worker_id, frame, context.into())
            .await
            .map_err(io::Error::other)?;
        if result.len() > crate::skew_routing::MAX_ROUTING_METADATA_BYTES {
            return Err(io::Error::other(
                "routing response exceeds per-epoch byte budget",
            ));
        }
        serde_json::from_slice(&result).map_err(io::Error::other)
    }

    async fn submit_skew_epoch(
        &self,
        topology: &WorkloadTopologySnapshot,
        request: &SourceDeltaRequest,
    ) -> io::Result<bool> {
        use crate::skew_routing::{RoutingEpochRecord, RoutingOperation, RoutingSnapshot};
        let join_eligible =
            crate::skew_join::join_merge_key_columns(&topology.plan_json, topology).is_some();
        if topology.shards.is_empty() {
            return Ok(false);
        }
        let shards = crate::skew_routing::stable_shards(topology);
        let anchor = shards[0];
        let snapshot: RoutingSnapshot = self
            .routing_command(
                topology,
                anchor,
                request.epoch,
                RoutingOperation::Read,
                &serde_json::Value::Null,
            )
            .await?;
        // An existing join cannot start a low-side snapshot halfway through
        // its history. Only a journal initialized before the first durable
        // input can safely promote without scanning the old arrangements.
        let legacy_join = join_eligible
            && (snapshot
                .state
                .get("kind")
                .and_then(serde_json::Value::as_str)
                == Some("join_tracking_disabled")
                || (snapshot.state.is_null()
                    && snapshot.pending.is_none()
                    && snapshot.anchor_frontier > 0));
        if let Some(completed) = &snapshot.completed {
            if crate::skew_routing::matches_request(completed, request).map_err(io::Error::other)? {
                // Authorization may have installed a new control reservation
                // for this retry. Reissue the durable completion certificate
                // even though no participant needs to execute again.
                self.finish_skew_epoch(topology, completed).await?;
                return Ok(true);
            }
        }
        let record = if let Some(pending) = snapshot.pending {
            if !crate::skew_routing::matches_request(&pending, request).map_err(io::Error::other)?
                || pending.shard_ids != shards
            {
                return Err(io::Error::other(
                    "a different skew routing epoch requires replay before new input",
                ));
            }
            pending
        } else {
            let (plan, next_state, point_mutations) = if legacy_join {
                (
                    serde_json::json!({"kind":"passthrough","reason":"join_history_predates_routing_journal"}),
                    serde_json::json!({"kind":"join_tracking_disabled","reason":"incomplete_historical_low_snapshot"}),
                    BTreeMap::new(),
                )
            } else if join_eligible {
                let keys =
                    crate::skew_join::join_state_keys(request, topology, &topology.plan_json)
                        .map_err(io::Error::other)?;
                let mut points: BTreeMap<String, serde_json::Value> = self
                    .routing_command(
                        topology,
                        anchor,
                        request.epoch,
                        RoutingOperation::GetMany,
                        &keys,
                    )
                    .await?;
                let scans = crate::skew_join::join_fallback_scans(
                    &points,
                    request,
                    topology,
                    &topology.plan_json,
                )
                .map_err(io::Error::other)?;
                if !scans.is_empty() {
                    let proofs: BTreeMap<String, serde_json::Value> = self
                        .routing_command(
                            topology,
                            anchor,
                            request.epoch,
                            RoutingOperation::JoinRowStats,
                            &scans,
                        )
                        .await?;
                    points.extend(proofs);
                }
                let previous = (!snapshot.state.is_null()).then_some(&snapshot.state);
                let planned = crate::skew_join::prepare_join_epoch(
                    previous,
                    &points,
                    request,
                    topology,
                    &topology.plan_json,
                )
                .map_err(io::Error::other)?
                .ok_or_else(|| io::Error::other("eligible join lost its routing specification"))?;
                (planned.plan, planned.next_state, planned.point_mutations)
            } else {
                let planned = crate::skew_aggregate::prepare_aggregate_epoch(
                    Some(&snapshot.state),
                    request,
                    topology,
                )
                .map_err(io::Error::other)?;
                match planned {
                    Some(planned) => (planned.plan, planned.next_state, BTreeMap::new()),
                    None => (
                        serde_json::json!({"kind":"passthrough","reason":"operands_not_admitted"}),
                        serde_json::Value::Null,
                        BTreeMap::new(),
                    ),
                }
            };
            let record = RoutingEpochRecord {
                version: crate::skew_routing::ROUTING_VERSION,
                workload_id: request.workload_id,
                source: request.source.clone(),
                request_id: request.request_id.clone(),
                epoch: request.epoch,
                input_digest: crate::skew_routing::input_digest(request)
                    .map_err(io::Error::other)?,
                generation: snapshot
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("routing generation exhausted"))?,
                shard_ids: shards.clone(),
                plan,
                next_state,
                point_mutations,
            };
            let prepared: RoutingSnapshot = self
                .routing_command(
                    topology,
                    anchor,
                    request.epoch,
                    RoutingOperation::Prepare,
                    &record,
                )
                .await?;
            if prepared.completed.as_ref() == Some(&record) && prepared.pending.is_none() {
                self.finish_skew_epoch(topology, &record).await?;
                return Ok(true);
            }
            if prepared.pending.as_ref() != Some(&record) {
                return Err(io::Error::other(
                    "routing prepare returned another decision",
                ));
            }
            record
        };
        let mut routed = if record.plan.get("kind").and_then(serde_json::Value::as_str)
            == Some("aggregate")
        {
            let dispatch =
                crate::skew_aggregate::dispatch_aggregate(&record.plan, request, &shards)
                    .map_err(io::Error::other)?;
            let mut raw = dispatch.raw;
            // Partial work happens concurrently on distinct worker owners. Sort
            // responses back into task order before creating the replay digest.
            let mut results = futures::stream::iter(dispatch.tasks.into_iter().enumerate())
                .map(|(index, (shard, task))| async move {
                    let result: crate::skew_aggregate::PartialAggregateResult = self
                        .routing_command(
                            topology,
                            shard,
                            request.epoch,
                            RoutingOperation::AggregatePartial,
                            &task,
                        )
                        .await?;
                    Ok::<_, io::Error>((index, result))
                })
                .buffer_unordered(crate::skew_aggregate::MAX_PARTIAL_BUCKETS)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<io::Result<Vec<_>>>()?;
            results.sort_by_key(|(index, _)| *index);
            for (_, result) in results {
                crate::skew_aggregate::append_final_operands(
                    &record.plan,
                    result,
                    &shards,
                    &mut raw,
                )
                .map_err(io::Error::other)?;
            }
            raw.into_iter()
                .map(|(shard, rows)| {
                    (
                        shard,
                        crate::skew_join::JoinShardBatch {
                            rows,
                            bootstrap: None,
                        },
                    )
                })
                .collect()
        } else if record.plan.get("kind").and_then(serde_json::Value::as_str)
            == Some("heavy_light_join/v1")
        {
            crate::skew_join::route_join_epoch(&record.plan, request).map_err(io::Error::other)?
        } else if record.plan.get("kind").and_then(serde_json::Value::as_str) == Some("passthrough")
        {
            Self::partition_request(topology, request)?
                .into_iter()
                .map(|(shard, rows)| {
                    (
                        shard.shard_id,
                        crate::skew_join::JoinShardBatch {
                            rows,
                            bootstrap: None,
                        },
                    )
                })
                .collect()
        } else {
            return Err(io::Error::other(
                "unsupported persisted skew routing decision",
            ));
        };
        // Every virtual shard commits the source epoch, even when it receives
        // no rows. The completion certificate therefore describes a complete
        // membership frontier, not the maximum progress of one worker.
        for shard in &shards {
            routed
                .entry(*shard)
                .or_insert_with(|| crate::skew_join::JoinShardBatch {
                    rows: Vec::new(),
                    bootstrap: None,
                });
        }
        let schemas = crate::client::deployment_schemas_from_slice(&topology.schemas)?;
        let schema = schemas
            .get(&request.source)
            .cloned()
            .ok_or_else(|| io::Error::other("unknown routed source"))?;
        let results = futures::stream::iter(routed)
            .map(|(shard, batch)| {
                let schema = schema.clone();
                async move {
                    self.send_skew_final(topology, request, shard, batch, schema)
                        .await
                }
            })
            .buffer_unordered(crate::skew_aggregate::MAX_PARTIAL_BUCKETS)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            result?;
        }
        self.finish_skew_epoch(topology, &record).await?;
        Ok(true)
    }

    async fn finish_skew_epoch(
        &self,
        topology: &WorkloadTopologySnapshot,
        record: &crate::skew_routing::RoutingEpochRecord,
    ) -> io::Result<()> {
        use crate::skew_routing::{FinishRoutingEpoch, RoutingOperation, RoutingSnapshot};
        let shards = crate::skew_routing::stable_shards(topology);
        if record.workload_id != topology.workload_id
            || shards.is_empty()
            || record.shard_ids != shards
        {
            return Err(io::Error::other(
                "routing completion membership has changed",
            ));
        }
        let mut leases = topology
            .shards
            .iter()
            .map(|shard| (shard.shard_id, shard.lease_token))
            .collect::<Vec<_>>();
        leases.sort_by_key(|(shard, _)| *shard);
        let finished: RoutingSnapshot = self
            .routing_command(
                topology,
                shards[0],
                record.epoch,
                RoutingOperation::Finish,
                &FinishRoutingEpoch {
                    record: record.clone(),
                    shard_leases: leases,
                },
            )
            .await?;
        if finished.pending.is_some() || finished.completed.as_ref() != Some(record) {
            return Err(io::Error::other("routing epoch did not finish durably"));
        }
        Ok(())
    }

    async fn send_skew_final(
        &self,
        topology: &WorkloadTopologySnapshot,
        request: &SourceDeltaRequest,
        shard_id: ShardId,
        batch: crate::skew_join::JoinShardBatch,
        schema: arrow::datatypes::SchemaRef,
    ) -> io::Result<()> {
        let shard = topology
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| io::Error::other("RS-3004: final shard absent"))?;
        let input = crate::client::rows_to_zset(&batch.rows, schema)?;
        let payload =
            crate::exchange::serialization::serialize_zset(&input).map_err(io::Error::other)?;
        let mut owner = RuntimeExchangeMessage {
            attempt_id: None,
            version: request.version,
            request_id: request.request_id.clone(),
            workload_id: request.workload_id,
            shard_id,
            epoch: request.epoch,
            operator_id: shard.operator_id,
            lease_token: shard.lease_token,
            source: request.source.clone(),
            rows: Vec::new(),
            join_bootstrap: batch.bootstrap,
        };
        if owner.join_bootstrap.is_some() {
            // Large low-side snapshots belong in the flow-controlled payload,
            // not in HTTP/2 metadata. The worker still uses execute_frame's
            // single arrangement/output/frontier/replay transaction.
            owner.rows = batch.rows;
            let _: serde_json::Value = self
                .routing_command(
                    topology,
                    shard_id,
                    request.epoch,
                    crate::skew_routing::RoutingOperation::JoinExecute,
                    &owner,
                )
                .await?;
            return Ok(());
        }
        let context = self.owned_context(&owner)?;

        let row_count = u32::try_from(input.num_rows()).map_err(io::Error::other)?;
        let frame = crate::exchange::proto::ShuffleFrame {
            exchange_id: request.workload_id.0,
            src_shard: 0,
            target_shard: shard.shard_index,
            epoch: request.epoch,
            seq: 0,
            payload: payload.into(),
            row_count,
        };
        self.shuffle_mux
            .send_owned_frame(shard.worker_id, frame, context.into())
            .await
            .map_err(io::Error::other)
    }

    fn partition_request<'a>(
        topology: &'a WorkloadTopologySnapshot,
        request: &SourceDeltaRequest,
    ) -> io::Result<
        Vec<(
            &'a rockstream_types::data_plane::WorkloadShardPlacement,
            Vec<RuntimeRow>,
        )>,
    > {
        let routing_column = topology
            .routing_columns
            .get(&request.source)
            .copied()
            .or_else(|| {
                topology
                    .routing_columns
                    .iter()
                    .find(|(source, _)| source.eq_ignore_ascii_case(&request.source))
                    .map(|(_, column)| *column)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{RS_1012}: no routing column is registered for source {}; refusing arbitrary placement",
                        request.source
                    ),
                )
            })?;

        if topology.shards.is_empty() {
            return Err(io::Error::other(format!(
                "{RS_0001}: workload {} has no active shard leases",
                request.workload_id
            )));
        }

        let shard_ids: Vec<ShardId> = topology.shards.iter().map(|s| s.shard_id).collect();
        let shard_by_id: HashMap<ShardId, &rockstream_types::data_plane::WorkloadShardPlacement> =
            topology.shards.iter().map(|s| (s.shard_id, s)).collect();

        let mut rows_by_shard = BTreeMap::<ShardId, Vec<RuntimeRow>>::new();
        for row in &request.rows {
            let key = row
                .values_tsv
                .split('\t')
                .nth(routing_column)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{RS_1012}: source {} row is missing routing column {}",
                            request.source, routing_column
                        ),
                    )
                })?;
            let bucket = fnv1a_64(key.as_bytes()) % DEFAULT_VIRTUAL_BUCKET_COUNT;
            let shard_id = rendezvous_hash(bucket, &shard_ids, 1).ok_or_else(|| {
                io::Error::other(format!("{RS_0001}: workload has no active shard leases"))
            })?;
            rows_by_shard.entry(shard_id).or_default().push(row.clone());
        }

        // If the batch had 0 rows (e.g. empty epoch marker), route to shard 0
        // so the epoch advances cleanly.
        if rows_by_shard.is_empty() {
            rows_by_shard.insert(shard_ids[0], Vec::new());
        }

        Ok(rows_by_shard
            .into_iter()
            .map(|(shard_id, rows)| (shard_by_id[&shard_id], rows))
            .collect())
    }

    async fn submit_delta_via_control(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: Option<&str>,
    ) -> io::Result<u64> {
        let request_id = request.request_id.clone();
        match self
            .request_with_assertion(WorkerMessage::SubmitSourceDelta(request), actor_assertion)
            .await?
        {
            ControlMessage::SourceDeltaCommitted {
                request_id: actual,
                epoch,
            } if actual == request_id => Ok(epoch),
            response => Err(Self::unexpected(response)),
        }
    }

    pub async fn read_workload(&self, workload_id: WorkloadId) -> io::Result<WorkloadSnapshot> {
        self.read_workload_request(workload_id, None).await
    }

    pub async fn read_workload_delegated(
        &self,
        workload_id: WorkloadId,
        actor_assertion: &str,
    ) -> io::Result<WorkloadSnapshot> {
        self.read_workload_request(workload_id, Some(actor_assertion))
            .await
    }

    async fn read_workload_request(
        &self,
        workload_id: WorkloadId,
        actor_assertion: Option<&str>,
    ) -> io::Result<WorkloadSnapshot> {
        match self
            .request_with_assertion(
                WorkerMessage::ReadWorkload {
                    version: WORKLOAD_SNAPSHOT_VERSION,
                    workload_id,
                },
                actor_assertion,
            )
            .await?
        {
            ControlMessage::WorkloadSnapshot { snapshot } => {
                let expected_shards = snapshot
                    .workers
                    .iter()
                    .flat_map(|worker| worker.shard_ids.iter().copied())
                    .collect::<std::collections::HashSet<_>>();
                let actual_shards = snapshot
                    .shards
                    .iter()
                    .map(|shard| shard.shard_id)
                    .collect::<std::collections::HashSet<_>>();
                let valid = snapshot.deployment.workload_id == workload_id
                    && snapshot.version == WORKLOAD_SNAPSHOT_VERSION
                    && snapshot.complete
                    && !expected_shards.is_empty()
                    && snapshot.workers.iter().all(|worker| worker.ready)
                    && actual_shards.len() == snapshot.shards.len()
                    && actual_shards == expected_shards
                    && snapshot
                        .shards
                        .iter()
                        .all(|shard| shard.lease_token.is_some());
                if valid {
                    Ok(snapshot)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{}: control returned an incomplete or unsupported workload snapshot",
                            rockstream_types::error_code::RS_0001
                        ),
                    ))
                }
            }
            response => Err(Self::unexpected(response)),
        }
    }

    pub async fn read_operator_stats(
        &self,
        workload_id: WorkloadId,
        operator_ids: Vec<rockstream_types::ids::OperatorId>,
    ) -> io::Result<Option<Vec<rockstream_types::explain::IdentifiedOperatorStats>>> {
        self.read_operator_stats_request(workload_id, operator_ids, None, None)
            .await
    }

    pub async fn read_operator_stats_delegated(
        &self,
        workload_id: WorkloadId,
        operator_ids: Vec<rockstream_types::ids::OperatorId>,
        request_id: String,
        actor_assertion: &str,
    ) -> io::Result<Option<Vec<rockstream_types::explain::IdentifiedOperatorStats>>> {
        self.read_operator_stats_request(
            workload_id,
            operator_ids,
            Some(request_id),
            Some(actor_assertion),
        )
        .await
    }

    async fn read_operator_stats_request(
        &self,
        workload_id: WorkloadId,
        operator_ids: Vec<rockstream_types::ids::OperatorId>,
        request_id: Option<String>,
        actor_assertion: Option<&str>,
    ) -> io::Result<Option<Vec<rockstream_types::explain::IdentifiedOperatorStats>>> {
        let request_id = request_id.unwrap_or_else(|| {
            format!(
                "{}-{}-operator-stats-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                NEXT_OPERATOR_STATS_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
            )
        });
        let response = tokio::time::timeout(
            Duration::from_secs(7),
            self.request_with_assertion(
                WorkerMessage::ReadOperatorStats {
                    request_id: request_id.clone(),
                    workload_id,
                    operator_ids,
                },
                actor_assertion,
            ),
        )
        .await
        .map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "operator stats request timed out")
        })??;
        match response {
            ControlMessage::OperatorStatsSnapshot {
                request_id: actual,
                available,
                stats,
            } if actual == request_id => Ok(available.then_some(stats)),
            response => Err(Self::unexpected(response)),
        }
    }

    /// Discover active shards, their shared object-store prefixes, and published
    /// frontiers from the control plane.
    pub async fn discover_shards(
        &self,
    ) -> io::Result<(
        Vec<rockstream_types::topology::ShardServingInfo>,
        Option<u64>,
    )> {
        match self
            .request_with_assertion(WorkerMessage::DiscoverShards, None)
            .await?
        {
            ControlMessage::ServingTopology {
                shards,
                cluster_frontier,
            } => Ok((shards, cluster_frontier)),
            response => Err(Self::unexpected(response)),
        }
    }
}

fn data_plane_server_name(addr: &str) -> io::Result<rustls::pki_types::ServerName<'static>> {
    let host = if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        socket_addr.ip().to_string()
    } else {
        let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
        host.strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host)
            .to_owned()
    };
    rustls::pki_types::ServerName::try_from(host).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("RS-2411: invalid data-plane TLS server name: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plaintext_requires_explicit_loopback_development() {
        let request = WorkerMessage::ClusterStatusQuery;
        for client in [
            DataPlaneClient::new("192.0.2.1:9200"),
            DataPlaneClient::new("192.0.2.1:9200").with_insecure_internal_dev(),
        ] {
            let error = client
                .request_with_assertion(request.clone(), None)
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("RS-2405"));
        }
    }
}
