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
        self.submit_delta_direct(request).await
    }

    pub async fn submit_delta_delegated(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: &str,
    ) -> io::Result<u64> {
        self.submit_delta_via_control(request, Some(actor_assertion))
            .await
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
        self.submit_delta_direct(SourceDeltaRequest {
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
                let context_bytes = serde_json::to_vec(&owner_context)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
