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
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

use crate::management_store::{
    ManagementOperationStore, OperationRecord, OperationStatus, OperationStoreError,
    MAX_OPERATION_PAGE_SIZE,
};
use crate::service::ManagementDrainRuntime;
use crate::shard::ShardManager;
use crate::topology::TopologyCatalog;

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

#[derive(Serialize)]
struct DrainWorkerDigest {
    worker_id: u64,
}

#[derive(Clone)]
pub struct ManagementService {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    operations: ManagementOperationStore,
    config: Arc<NodeConfig>,
    drain_runtime: Option<ManagementDrainRuntime>,
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
        }
    }

    pub(crate) fn with_drain_runtime(mut self, runtime: ManagementDrainRuntime) -> Self {
        self.drain_runtime = Some(runtime);
        self
    }

    pub async fn start(&self, bind_addr: &str) -> std::io::Result<ManagementServiceHandle> {
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
                tracing::error!(%error, "management service stopped");
            }
        });
        Ok(ManagementServiceHandle { addr, shutdown_tx })
    }

    fn ensure_version(version: u32) -> Result<(), Status> {
        ensure_protocol_version(version)
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
        | OperationStoreError::HistoryLimit => Status::resource_exhausted(error.to_string()),
        OperationStoreError::IdempotencyConflict { .. } => {
            Status::already_exists(error.to_string())
        }
        OperationStoreError::IdempotencyExpired { .. } => {
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
        Self::ensure_version(request.into_inner().protocol_version)?;
        let (workers, source_version) = self.nodes();
        let nodes = workers
            .into_iter()
            .map(|worker| Self::node(worker, source_version.clone()))
            .collect();
        let (active_operations, retained_operations) =
            self.operations.counts().await.map_err(store_error)?;
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
            request_fill: 0,
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<v1::ListNodesRequest>,
    ) -> Result<Response<v1::ListNodesResponse>, Status> {
        let request = request.into_inner();
        Self::ensure_version(request.protocol_version)?;
        let size = page_size(request.page_size)?;
        let (workers, source_version) = self.nodes();
        let (items, next_page_token) = page(
            workers
                .into_iter()
                .map(|worker| Self::node(worker, source_version.clone()))
                .collect(),
            size,
            &request.page_token,
        )?;
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
            }),
        }))
    }

    async fn list_operations(
        &self,
        request: Request<v1::ListOperationsRequest>,
    ) -> Result<Response<v1::ListOperationsResponse>, Status> {
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
        if accepted.status() == OperationStatus::Pending {
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
        Self::ensure_version(request.into_inner().protocol_version)?;
        Err(Status::unavailable("MigrateShard executor is not attached"))
    }

    async fn create_backup(
        &self,
        request: Request<v1::CreateBackupRequest>,
    ) -> Result<Response<v1::CreateBackupResponse>, Status> {
        Self::ensure_version(request.into_inner().protocol_version)?;
        Err(Status::unavailable("CreateBackup executor is not attached"))
    }

    async fn cancel_operation(
        &self,
        request: Request<v1::CancelOperationRequest>,
    ) -> Result<Response<v1::CancelOperationResponse>, Status> {
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
        if record.status() != OperationStatus::Pending {
            return Err(Status::failed_precondition(
                "operation cancellation is only safe before execution starts",
            ));
        }
        let record = self
            .operations
            .transition(
                record.operation_id(),
                crate::management_store::OperationUpdate {
                    status: OperationStatus::Cancelled,
                    updated_at_ms: Utc::now().timestamp_millis(),
                    progress: record.progress(),
                    phase: Some("cancelled_before_start".to_owned()),
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
