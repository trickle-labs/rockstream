#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use rockstream_management_proto::{ensure_protocol_version, v1};
use rockstream_types::config::NodeConfig;
use rockstream_types::diagnostic::redact_secrets;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::WorkerInfo;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

use crate::management_store::{
    ManagementOperationStore, OperationRecord, OperationStatus, OperationStoreError,
    MAX_OPERATION_PAGE_SIZE,
};
use crate::service::{
    ManagementAckWaiters, ManagementBackupRuntime, ManagementDrainRuntime,
    ManagementMigrationRuntime, MAX_MANAGEMENT_ACK_WAITERS,
};
use crate::shard::ShardManager;
use crate::topology::TopologyCatalog;

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_MANAGEMENT_REQUESTS: usize = 64;

#[derive(Serialize)]
struct DrainWorkerDigest {
    worker_id: u64,
}

#[derive(Serialize)]
struct MigrateShardDigest {
    shard_id: u64,
    target_node_id: u64,
}

#[derive(Serialize)]
struct CreateBackupDigest {
    destination: String,
}

#[derive(Clone)]
pub struct ManagementService {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    operations: ManagementOperationStore,
    config: Arc<NodeConfig>,
    drain_runtime: Option<ManagementDrainRuntime>,
    migration_runtime: Option<ManagementMigrationRuntime>,
    backup_runtime: Option<ManagementBackupRuntime>,
    migration_waiters: Option<ManagementAckWaiters>,
    backup_waiters: Option<ManagementAckWaiters>,
    request_gate: Arc<Semaphore>,
}

impl ManagementService {
    pub fn new(
        catalog: TopologyCatalog,
        shard_manager: ShardManager,
        operations: ManagementOperationStore,
        config: NodeConfig,
    ) -> Self {
        Self {
            catalog,
            shard_manager,
            operations,
            config: Arc::new(config),
            drain_runtime: None,
            migration_runtime: None,
            backup_runtime: None,
            migration_waiters: None,
            backup_waiters: None,
            request_gate: Arc::new(Semaphore::new(MAX_MANAGEMENT_REQUESTS)),
        }
    }

    pub(crate) fn with_drain_runtime(mut self, runtime: ManagementDrainRuntime) -> Self {
        self.drain_runtime = Some(runtime);
        self
    }

    pub(crate) fn with_migration_runtime(mut self, runtime: ManagementMigrationRuntime) -> Self {
        self.migration_waiters = Some(runtime.waiters.clone());
        self.migration_runtime = Some(runtime);
        self
    }

    pub(crate) fn with_backup_runtime(mut self, runtime: ManagementBackupRuntime) -> Self {
        self.backup_waiters = Some(runtime.waiters.clone());
        self.backup_runtime = Some(runtime);
        self
    }

    pub async fn start(&self, bind_addr: &str) -> std::io::Result<ManagementServiceHandle> {
        let mut recover = self
            .operations
            .nonterminal()
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut recovery_conflicts = std::collections::HashSet::new();
        for record in &mut recover {
            if record.status() == crate::management_store::OperationStatus::Running {
                match self
                    .operations
                    .transition_if(
                        record.operation_id(),
                        record.status(),
                        record.phase(),
                        crate::management_store::OperationUpdate {
                            status: crate::management_store::OperationStatus::Waiting,
                            updated_at_ms: Utc::now().timestamp_millis(),
                            progress: record.progress(),
                            phase: record.phase().map(str::to_owned),
                            error_code: None,
                            next_steps: vec![
                                "Management restarted; reconcile this operation before retrying."
                                    .to_owned(),
                            ],
                        },
                    )
                    .await
                {
                    Ok(recovered) => *record = recovered,
                    Err(crate::management_store::OperationStoreError::TransitionConflict(_)) => {
                        recovery_conflicts.insert(record.operation_id().to_owned());
                    }
                    Err(error) => {
                        return Err(std::io::Error::other(error.to_string()));
                    }
                }
            }
        }
        recover.retain(|record| {
            !recovery_conflicts.contains(record.operation_id())
                && matches!(
                    record.status(),
                    crate::management_store::OperationStatus::Pending
                        | crate::management_store::OperationStatus::Waiting
                )
        });
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, _) = broadcast::channel(1);
        let mut shutdown_rx = shutdown_tx.subscribe();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let service = self.clone();
        tokio::spawn(async move {
            let api = v1::management_service_server::ManagementServiceServer::new(service)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES);
            let result = tonic::transport::Server::builder()
                .max_concurrent_streams(Some(64))
                .add_service(api)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await;
            if let Err(error) = result {
                tracing::error!(code = %rockstream_types::error_code::RS_0001, %error, "management service stopped");
            }
        });
        let reconciler = self.clone();
        let mut reconcile_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => match reconciler.operations.nonterminal().await {
                        Ok(records) => {
                            for record in records {
                                reconciler.reconcile(&record).await;
                            }
                        }
                        Err(error) => tracing::warn!(%error, "management reconciliation scan failed"),
                    },
                    _ = reconcile_shutdown.recv() => break,
                }
            }
        });
        for record in recover {
            self.reconcile(&record).await;
        }
        Ok(ManagementServiceHandle { addr, shutdown_tx })
    }

    fn ensure_version(version: u32) -> Result<(), Status> {
        ensure_protocol_version(version)
    }

    fn acquire_request(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.request_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("management request limit of 64 reached"))
    }

    fn nodes(&self) -> (Vec<WorkerInfo>, String) {
        let version = self.catalog.version();
        (self.catalog.all_workers(), format!("topology:{version}"))
    }

    fn node(worker: WorkerInfo, source_version: String) -> v1::Node {
        let state = if !worker.healthy {
            "unhealthy".to_string()
        } else {
            worker.lifecycle.progress_phase()
        };
        v1::Node {
            node_id: worker.worker_id.0.to_string(),
            address: worker.address,
            state,
            source_version,
            role: format!("{:?}", worker.role).to_lowercase(),
            capacity_headroom: worker.capacity_headroom.0,
            host_id: worker.location.host_id,
            availability_zone: worker.location.availability_zone,
            healthy: worker.healthy,
            registered_at_ms: worker.registered_at_ms,
            lifecycle_state: format!("{:?}", worker.lifecycle).to_lowercase(),
        }
    }

    fn operation(record: OperationRecord) -> v1::Operation {
        let kind = match record.kind() {
            crate::management_store::OperationKind::DrainWorker => "drain_worker",
            crate::management_store::OperationKind::MigrateShard => "migrate_shard",
            crate::management_store::OperationKind::CreateBackup => "create_backup",
        };
        v1::Operation {
            operation_id: record.operation_id().to_owned(),
            kind: kind.to_owned(),
            state: format!("{:?}", record.status()).to_lowercase(),
            started_at: timestamp(record.started_at_ms()),
            updated_at: timestamp(record.updated_at_ms()),
            progress: record
                .progress()
                .map(|value| format!("{value}%"))
                .unwrap_or_default(),
            phase: record.phase().unwrap_or_default().to_owned(),
            error_code: record.error_code().unwrap_or_default().to_owned(),
            next_steps: record.next_steps().to_vec(),
            source_version: format!("operation-record:{}", record.record_version()),
        }
    }

    async fn reconcile(&self, record: &OperationRecord) {
        if !matches!(
            record.status(),
            crate::management_store::OperationStatus::Pending
                | crate::management_store::OperationStatus::Waiting
        ) {
            return;
        }
        let Some(request) = record.request() else {
            return;
        };
        match record.kind() {
            crate::management_store::OperationKind::DrainWorker => {
                if let (Some(runtime), Some(worker_id)) = (
                    self.drain_runtime.as_ref(),
                    request.get("worker_id").and_then(serde_json::Value::as_u64),
                ) {
                    runtime
                        .schedule(WorkerId(worker_id), record.operation_id().to_owned())
                        .await;
                }
            }
            crate::management_store::OperationKind::MigrateShard => {
                if let (Some(runtime), Some(shard_id), Some(target_node_id)) = (
                    self.migration_runtime.as_ref(),
                    request.get("shard_id").and_then(serde_json::Value::as_u64),
                    request
                        .get("target_node_id")
                        .and_then(serde_json::Value::as_u64),
                ) {
                    runtime
                        .schedule(
                            ShardId(shard_id),
                            WorkerId(target_node_id),
                            record.operation_id().to_owned(),
                        )
                        .await;
                }
            }
            crate::management_store::OperationKind::CreateBackup => {
                if let Some(runtime) = self.backup_runtime.as_ref() {
                    runtime.schedule(record.operation_id().to_owned()).await;
                }
            }
        }
    }
}

pub struct ManagementServiceHandle {
    pub addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
}

impl ManagementServiceHandle {
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

fn timestamp(epoch_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| format!("unix-ms:{epoch_ms}"))
}

fn page_size(requested: u32) -> Result<usize, Status> {
    let size = if requested == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        requested as usize
    };
    if size > MAX_OPERATION_PAGE_SIZE {
        return Err(Status::invalid_argument(format!(
            "page size must be between 1 and {MAX_OPERATION_PAGE_SIZE}"
        )));
    }
    Ok(size)
}

fn page<T>(items: Vec<T>, size: usize, token: &str) -> Result<(Vec<T>, String), Status> {
    let offset = if token.is_empty() {
        0
    } else {
        token
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid page token"))?
    };
    if offset > items.len() {
        return Err(Status::invalid_argument("invalid page token"));
    }
    let end = offset.saturating_add(size).min(items.len());
    let next = if end < items.len() {
        end.to_string()
    } else {
        String::new()
    };
    Ok((items.into_iter().skip(offset).take(size).collect(), next))
}

fn store_error(error: OperationStoreError) -> Status {
    match error {
        OperationStoreError::NotFound(id) => Status::not_found(format!("operation {id} not found")),
        OperationStoreError::InvalidPageToken | OperationStoreError::InvalidPageSize => {
            Status::invalid_argument(error.to_string())
        }
        OperationStoreError::ActiveLimit
        | OperationStoreError::RetainedLimit
        | OperationStoreError::HistoryLimit
        | OperationStoreError::TransitionLimit(_) => Status::resource_exhausted(error.to_string()),
        OperationStoreError::IdempotencyConflict { .. } => {
            Status::already_exists(error.to_string())
        }
        OperationStoreError::IdempotencyExpired { .. } => {
            Status::failed_precondition(error.to_string())
        }
        OperationStoreError::TransitionConflict(_) => {
            Status::failed_precondition(error.to_string())
        }
        _ => Status::unavailable(error.to_string()),
    }
}

fn flatten_config(prefix: &str, value: &toml::Value, values: &mut Vec<v1::ConfigValue>) {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_config(&path, child, values);
            }
        }
        _ => {
            let raw = value.to_string();
            let sensitive_key = ["password", "secret", "token", "private_key", "access_key"]
                .iter()
                .any(|key| prefix.to_ascii_lowercase().contains(key));
            let redacted_value = redact_secrets(&raw);
            let redacted = sensitive_key || redacted_value != raw;
            values.push(v1::ConfigValue {
                key: prefix.to_owned(),
                value: if sensitive_key {
                    "[REDACTED]".to_owned()
                } else {
                    redacted_value
                },
                redacted,
            });
        }
    }
}

#[tonic::async_trait]
impl v1::management_service_server::ManagementService for ManagementService {
    async fn get_cluster_status(
        &self,
        request: Request<v1::GetClusterStatusRequest>,
    ) -> Result<Response<v1::GetClusterStatusResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.into_inner().protocol_version)?;
        let (workers, source_version) = self.nodes();
        let mut nodes = workers
            .into_iter()
            .map(|worker| Self::node(worker, source_version.clone()))
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let (active_operations, retained_operations) =
            self.operations.counts().await.map_err(store_error)?;
        let mut ack_waiter_fill = 0u32;
        let mut ack_waiter_capacity = 0u32;
        for waiters in [&self.migration_waiters, &self.backup_waiters]
            .into_iter()
            .flatten()
        {
            ack_waiter_fill += waiters.lock().await.len() as u32;
            ack_waiter_capacity += MAX_MANAGEMENT_ACK_WAITERS as u32;
        }
        let state = if self.catalog.healthy_workers().is_empty() {
            "unknown"
        } else if self
            .catalog
            .all_workers()
            .iter()
            .any(|worker| !worker.healthy)
        {
            "degraded"
        } else {
            "healthy"
        };
        Ok(Response::new(v1::GetClusterStatusResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version,
            state: state.to_owned(),
            nodes,
            active_operations: active_operations as u32,
            retained_operations: retained_operations as u32,
            request_fill: (MAX_MANAGEMENT_REQUESTS - self.request_gate.available_permits()) as u32,
            request_capacity: MAX_MANAGEMENT_REQUESTS as u32,
            ack_waiter_fill,
            ack_waiter_capacity,
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<v1::ListNodesRequest>,
    ) -> Result<Response<v1::ListNodesResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let size = page_size(request.page_size)?;
        let (workers, source_version) = self.nodes();
        let mut nodes = workers
            .into_iter()
            .map(|worker| Self::node(worker, source_version.clone()))
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let (items, next_page_token) = page(nodes, size, &request.page_token)?;
        Ok(Response::new(v1::ListNodesResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version,
            nodes: items,
            next_page_token,
        }))
    }

    async fn get_node(
        &self,
        request: Request<v1::GetNodeRequest>,
    ) -> Result<Response<v1::GetNodeResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let worker_id = request
            .node_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("node_id must be an unsigned integer"))?;
        let (source, source_version) = self.nodes();
        let worker = source
            .into_iter()
            .find(|worker| worker.worker_id == WorkerId(worker_id))
            .ok_or_else(|| Status::not_found(format!("node {worker_id} not found")))?;
        Ok(Response::new(v1::GetNodeResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: source_version.clone(),
            node: Some(Self::node(worker, source_version)),
        }))
    }

    async fn list_shards(
        &self,
        request: Request<v1::ListShardsRequest>,
    ) -> Result<Response<v1::ListShardsResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let size = page_size(request.page_size)?;
        let snapshot = self.shard_manager.snapshot();
        let source_version = format!("lease:{}:{}", snapshot.leader_epoch, snapshot.next_token);
        let mut shards = self
            .shard_manager
            .leases()
            .into_iter()
            .map(|lease| v1::Shard {
                shard_id: lease.shard_id.0.to_string(),
                owner_node_id: lease.worker_id.0.to_string(),
                state: "leased".to_owned(),
                source_version: source_version.clone(),
                lease_token: lease.lease_token.0,
                key_range: String::new(),
                key_range_known: false,
            })
            .collect::<Vec<_>>();
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        let (shards, next_page_token) = page(shards, size, &request.page_token)?;
        Ok(Response::new(v1::ListShardsResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version,
            shards,
            next_page_token,
        }))
    }

    async fn get_shard(
        &self,
        request: Request<v1::GetShardRequest>,
    ) -> Result<Response<v1::GetShardResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let shard_id = request
            .shard_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("shard_id must be an unsigned integer"))?;
        let lease = self
            .shard_manager
            .get(ShardId(shard_id))
            .ok_or_else(|| Status::not_found(format!("shard {shard_id} not found")))?;
        let snapshot = self.shard_manager.snapshot();
        Ok(Response::new(v1::GetShardResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: format!("lease:{}:{}", snapshot.leader_epoch, snapshot.next_token),
            shard: Some(v1::Shard {
                shard_id: lease.shard_id.0.to_string(),
                owner_node_id: lease.worker_id.0.to_string(),
                state: "leased".to_owned(),
                source_version: format!("lease:{}:{}", snapshot.leader_epoch, snapshot.next_token),
                lease_token: lease.lease_token.0,
                key_range: String::new(),
                key_range_known: false,
            }),
        }))
    }

    async fn list_operations(
        &self,
        request: Request<v1::ListOperationsRequest>,
    ) -> Result<Response<v1::ListOperationsResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let size = page_size(request.page_size)?;
        let (items, next_page_token) = self
            .operations
            .list(size, &request.page_token)
            .await
            .map_err(store_error)?;
        Ok(Response::new(v1::ListOperationsResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: "management-operation-records:v1".to_owned(),
            operations: items.into_iter().map(Self::operation).collect(),
            next_page_token,
        }))
    }

    async fn get_operation(
        &self,
        request: Request<v1::GetOperationRequest>,
    ) -> Result<Response<v1::GetOperationResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let record = self
            .operations
            .get(&request.operation_id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| {
                Status::not_found(format!("operation {} not found", request.operation_id))
            })?;
        Ok(Response::new(v1::GetOperationResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: "management-operation-records:v1".to_owned(),
            operation: Some(Self::operation(record)),
        }))
    }

    async fn get_config_summary(
        &self,
        request: Request<v1::GetConfigSummaryRequest>,
    ) -> Result<Response<v1::GetConfigSummaryResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.into_inner().protocol_version)?;
        let redacted = toml::Value::try_from(self.config.as_ref()).map_err(|error| {
            Status::internal(format!("effective config serialization failed: {error}"))
        })?;
        let mut values = Vec::new();
        flatten_config("", &redacted, &mut values);
        Ok(Response::new(v1::GetConfigSummaryResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: "effective-node-config:v0.62".to_owned(),
            values,
        }))
    }

    async fn get_capabilities(
        &self,
        request: Request<v1::GetCapabilitiesRequest>,
    ) -> Result<Response<v1::GetCapabilitiesResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.into_inner().protocol_version)?;
        let mut capabilities = vec![
            "GetClusterStatus".to_owned(),
            "ListNodes".to_owned(),
            "GetNode".to_owned(),
            "ListShards".to_owned(),
            "GetShard".to_owned(),
            "ListOperations".to_owned(),
            "GetOperation".to_owned(),
            "GetConfigSummary".to_owned(),
            "GetCapabilities".to_owned(),
            "GetHealth".to_owned(),
        ];
        if self.drain_runtime.is_some() {
            capabilities.push("DrainWorker".to_owned());
            capabilities.push("CancelOperation".to_owned());
        }
        if self.migration_runtime.is_some() {
            capabilities.push("MigrateShard".to_owned());
        }
        if self.backup_runtime.is_some() {
            capabilities.push("CreateBackup".to_owned());
        }
        Ok(Response::new(v1::GetCapabilitiesResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: "management-service:v1".to_owned(),
            capabilities,
        }))
    }

    async fn get_health(
        &self,
        request: Request<v1::GetHealthRequest>,
    ) -> Result<Response<v1::GetHealthResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.into_inner().protocol_version)?;
        Ok(Response::new(v1::GetHealthResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source_version: format!("topology:{}", self.catalog.version()),
            state: "unknown".to_owned(),
            reason: "authoritative process health telemetry is not registered".to_owned(),
        }))
    }

    async fn drain_worker(
        &self,
        request: Request<v1::DrainWorkerRequest>,
    ) -> Result<Response<v1::DrainWorkerResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let runtime = self
            .drain_runtime
            .as_ref()
            .ok_or_else(|| Status::unavailable("DrainWorker executor is not attached"))?;
        let worker_id = request
            .worker_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("worker_id must be an unsigned integer"))?;
        let accepted = self
            .operations
            .accept_idempotent(
                &request.idempotency_key,
                request.protocol_version as u16,
                &DrainWorkerDigest { worker_id },
                uuid::Uuid::new_v4().to_string(),
                crate::management_store::OperationKind::DrainWorker,
                Utc::now().timestamp_millis(),
            )
            .await
            .map_err(store_error)?;
        if matches!(
            accepted.status(),
            crate::management_store::OperationStatus::Pending
                | crate::management_store::OperationStatus::Waiting
        ) {
            runtime
                .schedule(WorkerId(worker_id), accepted.operation_id().to_owned())
                .await;
        }
        Ok(Response::new(v1::DrainWorkerResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation: Some(Self::operation(accepted)),
        }))
    }

    async fn migrate_shard(
        &self,
        request: Request<v1::MigrateShardRequest>,
    ) -> Result<Response<v1::MigrateShardResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let shard_id = request
            .shard_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("shard_id must be an unsigned integer"))?;
        let target_node_id = request
            .target_node_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("target_node_id must be an unsigned integer"))?;
        if self.shard_manager.get(ShardId(shard_id)).is_none() {
            return Err(Status::not_found(format!("shard {shard_id} not found")));
        }
        if self.catalog.get(WorkerId(target_node_id)).is_none() {
            return Err(Status::not_found(format!(
                "node {target_node_id} not found"
            )));
        }
        let runtime = self
            .migration_runtime
            .as_ref()
            .ok_or_else(|| Status::unavailable("MigrateShard executor is not attached"))?;
        let accepted = self
            .operations
            .accept_idempotent(
                &request.idempotency_key,
                request.protocol_version as u16,
                &MigrateShardDigest {
                    shard_id,
                    target_node_id,
                },
                uuid::Uuid::new_v4().to_string(),
                crate::management_store::OperationKind::MigrateShard,
                Utc::now().timestamp_millis(),
            )
            .await
            .map_err(store_error)?;
        if matches!(
            accepted.status(),
            crate::management_store::OperationStatus::Pending
                | crate::management_store::OperationStatus::Waiting
        ) {
            runtime
                .schedule(
                    ShardId(shard_id),
                    WorkerId(target_node_id),
                    accepted.operation_id().to_owned(),
                )
                .await;
        }
        Ok(Response::new(v1::MigrateShardResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation: Some(Self::operation(accepted)),
        }))
    }

    async fn create_backup(
        &self,
        request: Request<v1::CreateBackupRequest>,
    ) -> Result<Response<v1::CreateBackupResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let runtime = self
            .backup_runtime
            .as_ref()
            .ok_or_else(|| Status::unavailable("CreateBackup executor is not attached"))?;
        let destination = request.destination.trim();
        if destination.is_empty()
            || destination.chars().any(char::is_control)
            || destination
                .split_once("://")
                .is_some_and(|(scheme, _)| !matches!(scheme, "file" | "s3"))
            || destination
                .strip_prefix("s3://")
                .is_some_and(|rest| rest.split('/').next().unwrap_or_default().is_empty())
            || destination == "file://"
        {
            return Err(Status::invalid_argument(
                "destination must be a local path, file:// path, or s3://bucket/prefix",
            ));
        }
        let accepted = self
            .operations
            .accept_idempotent(
                &request.idempotency_key,
                request.protocol_version as u16,
                &CreateBackupDigest {
                    destination: destination.to_owned(),
                },
                uuid::Uuid::new_v4().to_string(),
                crate::management_store::OperationKind::CreateBackup,
                Utc::now().timestamp_millis(),
            )
            .await
            .map_err(store_error)?;
        if matches!(
            accepted.status(),
            OperationStatus::Pending | OperationStatus::Waiting
        ) {
            runtime.schedule(accepted.operation_id().to_owned()).await;
        }
        Ok(Response::new(v1::CreateBackupResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation: Some(Self::operation(accepted)),
        }))
    }

    async fn cancel_operation(
        &self,
        request: Request<v1::CancelOperationRequest>,
    ) -> Result<Response<v1::CancelOperationResponse>, Status> {
        let _permit = self.acquire_request()?;
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let record = self
            .operations
            .get(&request.operation_id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| {
                Status::not_found(format!("operation {} not found", request.operation_id))
            })?;
        let safe = record.status() == OperationStatus::Pending
            || (matches!(
                record.status(),
                OperationStatus::Running | OperationStatus::Waiting
            ) && matches!(
                record.phase(),
                Some(
                    "validating_worker_and_shard_ownership"
                        | "validating_lease_and_workers"
                        | "donor_handoff_started"
                        | "donor_flushed_and_closed"
                )
            ));
        if !safe {
            return Err(Status::failed_precondition(
                "operation cancellation is past its safe boundary",
            ));
        }
        let status = record.status();
        let phase = if status == OperationStatus::Pending {
            "cancelled_before_start"
        } else {
            "cancelled_before_lease_transfer"
        };
        let record = self
            .operations
            .transition_if(
                record.operation_id(),
                status,
                record.phase(),
                crate::management_store::OperationUpdate {
                    status: OperationStatus::Cancelled,
                    updated_at_ms: Utc::now().timestamp_millis(),
                    progress: record.progress(),
                    phase: Some(phase.to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .map_err(store_error)?;
        Ok(Response::new(v1::CancelOperationResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation: Some(Self::operation(record)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_types::topology::WorkerMessage;

    async fn cluster_status(
        service: &ManagementService,
    ) -> Result<v1::GetClusterStatusResponse, Status> {
        <ManagementService as v1::management_service_server::ManagementService>::get_cluster_status(
            service,
            Request::new(v1::GetClusterStatusRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            }),
        )
        .await
        .map(Response::into_inner)
    }

    #[tokio::test]
    async fn cluster_status_reports_request_and_ack_waiter_fill_and_recovery() {
        let mut service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            ManagementOperationStore::new(Arc::new(InMemory::new())),
            NodeConfig::default(),
        );
        let migration_waiters: ManagementAckWaiters =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let backup_waiters: ManagementAckWaiters =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        service.migration_waiters = Some(migration_waiters.clone());
        service.backup_waiters = Some(backup_waiters.clone());
        let (migration_sender, _migration_receiver) =
            tokio::sync::oneshot::channel::<WorkerMessage>();
        let (backup_sender, _backup_receiver) = tokio::sync::oneshot::channel::<WorkerMessage>();
        migration_waiters
            .lock()
            .await
            .insert("migration".to_owned(), migration_sender);
        backup_waiters
            .lock()
            .await
            .insert("backup".to_owned(), backup_sender);

        let full = cluster_status(&service).await.unwrap();
        assert_eq!(full.request_fill, 1);
        assert_eq!(full.request_capacity, 64);
        assert_eq!(full.ack_waiter_fill, 2);
        assert_eq!(full.ack_waiter_capacity, 128);

        migration_waiters.lock().await.remove("migration");
        backup_waiters.lock().await.remove("backup");
        let recovered = cluster_status(&service).await.unwrap();
        assert_eq!(recovered.ack_waiter_fill, 0);
        assert_eq!(recovered.ack_waiter_capacity, 128);

        let mut permits = (0..MAX_MANAGEMENT_REQUESTS)
            .map(|_| service.request_gate.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        let saturated = cluster_status(&service).await.unwrap_err();
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            saturated.message(),
            "management request limit of 64 reached"
        );
        permits.pop();
        let after_release = cluster_status(&service).await.unwrap();
        assert_eq!(after_release.request_fill, 64);
        assert_eq!(after_release.request_capacity, 64);
        drop(permits);
        let after_recovery = cluster_status(&service).await.unwrap();
        assert_eq!(after_recovery.request_fill, 1);
    }
}
