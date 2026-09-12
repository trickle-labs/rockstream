//! Identity-pluggable client transport substrate for RockStream CLI.
//!
//! Encapsulates authentication credentials and transport mechanisms for communicating
//! with the control plane, catalog, and storage layers.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rockstream_control::{
    compute_file_sha256, BackupFileEntry, BackupManifest, BACKUP_MANIFEST_FILENAME,
    CURRENT_STORAGE_FORMAT,
};
use rockstream_types::acl::Role;
use rockstream_types::audit::AuditEvent;
use rockstream_types::diagnostic::{
    DiagnosticOccurrence, MAX_DIAGNOSTIC_BUNDLE_BYTES, MAX_DIAGNOSTIC_BUNDLE_OCCURRENCES,
};
use rockstream_types::error_code::{
    RS_0003, RS_0004, RS_0005, RS_1001, RS_1004, RS_1005, RS_1006, RS_1007, RS_1008, RS_1014,
    RS_2006, RS_2401, RS_2410, RS_2411, RS_3612, RS_3615, RS_3616, RS_4009, RS_5035,
};
use rockstream_types::mutation_policy::cli_mutation_policy;
pub use rockstream_types::mutation_policy::CLI_MUTATION_POLICY;
use rockstream_types::topology::{ControlMessage, RaftRoleWire, WorkerMessage};
use rockstream_types::view_lifecycle::{derive_degradation_status, ViewState};

use crate::output::{
    BackupCreateOutput, BackupInspectOutput, BackupVerifyOutput, CheckpointAlignmentInfo,
    CheckpointExportOutcome, CheckpointSummary, ClusterQuotasInfo, ClusterResourceUsageInfo,
    ClusterStatusInfo, DrainOutcome, MigrationOutcome, MutationOutcome, QueryResult,
    ResourceUsageInfo, RestoreOutcome, SchemaColumn, SchemaDetail, SchemaEvolutionHistoryInfo,
    SchemaEvolutionStatusInfo, SchemaSummary, ShardAlignmentInfo, ShardInfo, SourceDetail,
    SourceSummary, SubscribeEvent, SupportBundleInfo, ViewDetail, ViewStatusInfo, ViewSummary,
    WorkerStatusInfo, WorkloadDetail, WorkloadSummary, AUDIT_TAIL_MAX_EVENTS, CLI_OUTPUT_MAX_ROWS,
};
use crate::CliError;

fn append_audit_file(storage_path: &Path, event: &AuditEvent) {
    let audit_file = storage_path.join("audit.jsonl");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&audit_file)
    {
        if let Ok(line) = serde_json::to_string(event) {
            let _ = writeln!(file, "{}", line);
        }
    }
}

fn required_role(operation: &str) -> Role {
    cli_mutation_policy(operation)
        .expect("every CLI mutation must have an authorization policy")
        .minimum_role
        .clone()
}

fn checkpoint_dr_error(error: String) -> CliError {
    CliError::new(
        RS_5035,
        error
            .strip_prefix("RS-5035: ")
            .unwrap_or(&error)
            .to_string(),
        "Verify the committed export, object-store access, and target freshness, then retry.",
    )
}

/// Client identity representation for authenticating requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// Username or actor identity.
    pub user: String,
    /// Optional bearer token (for OIDC/token auth).
    pub token: Option<String>,
    /// Optional client certificate path (for mTLS auth).
    pub client_cert_path: Option<PathBuf>,
    /// Target namespace.
    pub namespace: String,
    /// RBAC role assigned to this client.
    pub role: Role,
}

impl Default for ClientIdentity {
    fn default() -> Self {
        Self {
            user: "rockstream".to_string(),
            token: None,
            client_cert_path: None,
            namespace: "public".to_string(),
            role: Role::Admin,
        }
    }
}

impl ClientIdentity {
    pub fn new(user: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            ..Default::default()
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn with_cert(mut self, cert_path: impl Into<PathBuf>) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    pub fn require_role(&self, min_role: Role) -> Result<(), CliError> {
        if self.role >= min_role {
            Ok(())
        } else {
            Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.user, min_role
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ))
        }
    }
}

/// Unified CLI transport trait.
pub trait CliTransport: Send + Sync {
    fn identity(&self) -> &ClientIdentity;
}

// ─── API Traits ──────────────────────────────────────────────────────────────

pub trait TopologyApi: Send + Sync {
    fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError>;
    fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError>;
    fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError>;
    fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError>;
    fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError>;
}

pub trait OperationApi: Send + Sync {
    fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError>;
    fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError>;
}

pub trait CatalogApi: Send + Sync {
    fn list_views(&self) -> Result<Vec<ViewSummary>, CliError>;
    fn get_view(&self, name: &str) -> Result<ViewDetail, CliError>;
    fn view_status(&self, name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError>;
    fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError>;
    fn get_source(&self, name: &str) -> Result<SourceDetail, CliError>;
    fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError>;
    fn get_schema(&self, name: &str) -> Result<SchemaDetail, CliError>;
    fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError>;
    fn get_workload(&self, name: &str) -> Result<WorkloadDetail, CliError>;
    fn resource_usage(
        &self,
        workload_name: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError>;
    fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError>;
    fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError>;
    fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError>;
    fn pause_view(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn resume_view(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn query_view(&self, name: &str, limit: Option<usize>) -> Result<QueryResult, CliError>;
    fn subscribe_view(
        &self,
        _name: &str,
        _from_epoch: Option<u64>,
        _snapshot: bool,
    ) -> Result<Vec<SubscribeEvent>, CliError> {
        Err(CliError::new(
            RS_0004,
            "remote catalog subscription unavailable",
            "Verify the configured gateway endpoint and retry.",
        ))
    }
    fn pause_source(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn resume_source(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn drop_source(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn create_schema(
        &mut self,
        name: &str,
        columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError>;
    fn drop_schema(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
    fn create_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError>;
    fn alter_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError>;
    fn drop_workload(&mut self, name: &str) -> Result<MutationOutcome, CliError>;
}

pub trait StorageAdminApi: Send + Sync {
    fn export_checkpoint(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<CheckpointExportOutcome, CliError>;
    fn restore_checkpoint(
        &self,
        audit_path: &Path,
        source: &str,
        target: &str,
    ) -> Result<RestoreOutcome, CliError>;
    fn list_checkpoints(&self, storage_path: &Path) -> Result<Vec<CheckpointSummary>, CliError>;
    fn show_checkpoint(
        &self,
        storage_path: &Path,
        checkpoint_id: u64,
    ) -> Result<CheckpointAlignmentInfo, CliError>;
    fn generate_support_bundle(
        &self,
        storage_path: &Path,
        bundle_file: &Path,
    ) -> Result<SupportBundleInfo, CliError>;
    fn create_backup(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<crate::output::BackupCreateOutput, CliError>;
    fn inspect_backup(
        &self,
        destination: &str,
    ) -> Result<crate::output::BackupInspectOutput, CliError>;
    fn verify_backup(
        &self,
        destination: &str,
    ) -> Result<crate::output::BackupVerifyOutput, CliError>;
    fn restore_backup(
        &self,
        source: &str,
        target: &Path,
        yes: bool,
    ) -> Result<crate::output::RestoreOutcome, CliError>;
}

fn unreachable_control_error(addr: &str, err: impl std::fmt::Display) -> CliError {
    CliError::new(
        RS_0004,
        format!("cannot reach RockStream control service at {addr}: failed to reach control plane: {err}"),
        "- verify `rockstream start` is running\n- check `rockstream config print-effective`\n- verify the configured control endpoint",
    )
}

fn probe_control_plane(
    addr: &str,
    tls_config: Option<&rockstream_types::identity::InternalTlsConfig>,
) -> Result<(), CliError> {
    let addr_clone = addr.to_string();
    let tls_cfg = tls_config.cloned();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;
        rt.block_on(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            if let Some(ref tls) = tls_cfg {
                if tls.is_enabled() {
                    let connector = match rockstream_runtime::tls::build_client_tls_connector(tls) {
                        Ok(c) => c,
                        Err(e) => return Err(CliError::new(
                            RS_2411,
                            format!("internal mTLS configuration error: {e}"),
                            "Verify certificate and CA paths.",
                        )),
                    };
                    let stream = match TcpStream::connect(&addr_clone).await {
                        Ok(s) => s,
                        Err(e) => return Err(unreachable_control_error(&addr_clone, e)),
                    };
                    let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string())
                        .unwrap_or_else(|_| rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).unwrap());
                    let mut tls_stream = match connector.connect(server_name, stream).await {
                        Ok(s) => s,
                        Err(e) => return Err(CliError::new(
                            RS_2411,
                            format!("internal mTLS handshake failed: {e}"),
                            "Verify that client certificate is valid, not expired, and signed by cluster CA.",
                        )),
                    };
                    let _ = tls_stream.write_all(b"\n").await;
                    let mut buf = [0u8; 1];
                    let probe = tokio::time::timeout(tokio::time::Duration::from_millis(150), tls_stream.read(&mut buf)).await;
                    if let Ok(Ok(0)) | Ok(Err(_)) = probe {
                        return Err(CliError::new(
                            RS_2411,
                            "connection closed by control plane (client certificate untrusted or invalid)",
                            "Verify that client certificate is valid, not expired, and signed by cluster CA.",
                        ));
                    }
                    return Ok(());
                }
            }

            let mut stream = match TcpStream::connect(&addr_clone).await {
                Ok(s) => s,
                Err(e) => return Err(unreachable_control_error(&addr_clone, e)),
            };
            let _ = stream.write_all(b"{\"type\":\"ping\"}\n").await;
            let mut buf = [0u8; 1];
            let probe = tokio::time::timeout(tokio::time::Duration::from_millis(150), stream.read(&mut buf)).await;
            if probe.is_ok() {
                return Err(CliError::new(
                    RS_2410,
                    format!("connection refused by control plane at {addr_clone}: client certificate required (internal mTLS enabled)"),
                    "Provide --tls-cert-path, --tls-key-path, and --tls-ca-cert-path with a valid client certificate.",
                ));
            }

            Ok(())
        })
    })
    .join()
    .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
}

fn query_cluster_status(
    addr: &str,
    tls_config: Option<&rockstream_types::identity::InternalTlsConfig>,
) -> Result<ClusterStatusInfo, CliError> {
    let addr = addr.to_string();
    let tls_config = tls_config.cloned();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), ""))?;
        rt.block_on(async move {
            use tokio::net::TcpStream;

            let stream = TcpStream::connect(&addr)
                .await
                .map_err(|e| unreachable_control_error(&addr, e))?;
            if let Some(tls) = tls_config {
                if tls.is_enabled() {
                    let connector = rockstream_runtime::tls::build_client_tls_connector(&tls)
                        .map_err(|e| CliError::new(
                            RS_2411,
                            format!("internal mTLS configuration error: {e}"),
                            "Verify certificate and CA paths.",
                        ))?;
                    let server_name = rustls::pki_types::ServerName::try_from("localhost".to_string())
                        .unwrap_or_else(|_| rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).unwrap());
                    let stream = connector.connect(server_name, stream).await.map_err(|e| {
                        CliError::new(
                            RS_2411,
                            format!("internal mTLS handshake failed: {e}"),
                            "Verify that client certificate is valid, not expired, and signed by cluster CA.",
                        )
                    })?;
                    return read_cluster_status(stream).await;
                }
            }
            read_cluster_status(stream).await
        })
    })
    .join()
    .map_err(|_| CliError::new(RS_0003, "internal thread error", ""))?
}

fn remote_query_unavailable(resource: &str) -> CliError {
    CliError::new(
        RS_0004,
        format!("live {resource} query is unavailable from the configured control service"),
        "Verify the control endpoint exposes this operation, or use the explicitly labeled demo command.",
    )
}

async fn read_cluster_status<S>(stream: S) -> Result<ClusterStatusInfo, CliError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut stream = stream;
    let request = serde_json::to_string(&WorkerMessage::ClusterStatusQuery)
        .map_err(|e| CliError::new(RS_0003, format!("failed to encode status request: {e}"), ""))?
        + "\n";
    stream.write_all(request.as_bytes()).await.map_err(|e| {
        CliError::new(
            RS_0003,
            format!("failed to send status request: {e}"),
            "Retry after verifying network connectivity to the control service.",
        )
    })?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let read = tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        reader.read_line(&mut line),
    )
    .await
    .map_err(|_| {
        CliError::new(
            RS_0003,
            "control service did not answer the status request",
            "Retry against the current control endpoint and inspect the control-plane logs.",
        )
    })?
    .map_err(|e| {
        CliError::new(
            RS_0003,
            format!("failed reading control response: {e}"),
            "Retry after checking the control-plane logs.",
        )
    })?;
    if read == 0 {
        return Err(CliError::new(
            RS_0003,
            "control service closed the status request without a reply",
            "Retry against the current control endpoint and inspect the control-plane logs.",
        ));
    }

    let response: ControlMessage = serde_json::from_str(line.trim()).map_err(|e| {
        CliError::new(
            RS_0003,
            format!("failed to decode control response: {e}"),
            "Upgrade the CLI and control plane together so they agree on the wire format.",
        )
    })?;
    match response {
        ControlMessage::ClusterStatusReport {
            node_id,
            role,
            term,
        } => Ok(ClusterStatusInfo {
            node_id,
            role: match role {
                RaftRoleWire::Follower => "follower",
                RaftRoleWire::Candidate => "candidate",
                RaftRoleWire::Leader => "leader",
                RaftRoleWire::NoRaft => "control",
            }
            .to_string(),
            term,
            active_workers: 0,
            healthy_workers: 0,
            leader_id: (role == RaftRoleWire::Leader).then_some(node_id).flatten(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
        other => Err(CliError::new(
            RS_0003,
            format!("unexpected control response to status request: {other:?}"),
            "Retry against the current control endpoint and inspect the control-plane logs.",
        )),
    }
}

// ─── Control Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ControlClient {
    pub control_addr: Option<String>,
    pub identity: ClientIdentity,
    pub audit_events: Arc<Mutex<Vec<AuditEvent>>>,
    pub storage_path: Option<PathBuf>,
    pub tls_config: Option<rockstream_types::identity::InternalTlsConfig>,
}

impl ControlClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        Self {
            control_addr,
            identity,
            audit_events: Arc::new(Mutex::new(Vec::new())),
            storage_path: None,
            tls_config: None,
        }
    }

    pub fn with_internal_tls(
        mut self,
        config: rockstream_types::identity::InternalTlsConfig,
    ) -> Self {
        self.tls_config = Some(config);
        self
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    pub fn record_audit(
        &self,
        action: &str,
        resource: &str,
        detail: Option<&str>,
        error_code: Option<&str>,
    ) {
        let mut event = AuditEvent::now(self.identity.user.clone(), action, resource);
        if let Some(d) = detail {
            event = event.with_detail(d);
        }
        if let Some(ec) = error_code {
            event = event.with_error_code(ec);
        }
        if let Ok(mut logs) = self.audit_events.lock() {
            logs.push(event.clone());
        }
        if let Some(ref sp) = self.storage_path {
            append_audit_file(sp, &event);
        }
    }

    fn control_addr(&self) -> Result<&str, CliError> {
        self.control_addr.as_deref().ok_or_else(|| {
            unreachable_control_error("127.0.0.1:9200", "control endpoint is not configured")
        })
    }

    pub fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        let addr = self.control_addr()?;
        match query_cluster_status(addr, self.tls_config.as_ref()) {
            Ok(status) => Ok(status),
            Err(error) if error.code == RS_0003 && self.tls_config.is_none() => {
                match probe_control_plane(addr, None) {
                    Err(probe_error) => Err(probe_error),
                    Ok(()) => Err(error),
                }
            }
            Err(error) if error.code == RS_0003 && self.tls_config.is_some() => Err(CliError::new(
                RS_2411,
                format!("internal mTLS handshake failed: {}", error.message),
                "Verify that client certificate is valid, not expired, and signed by cluster CA.",
            )),
            Err(error) => Err(error),
        }
    }

    pub fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        let addr = self.control_addr()?;
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("cluster quota"))
    }

    pub fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let addr = self.control_addr()?;
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("worker topology"))
    }

    pub fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let workers = self.list_workers()?;
        if let Some(id) = worker_id {
            let matched: Vec<_> = workers.into_iter().filter(|w| w.worker_id == id).collect();
            if matched.is_empty() {
                return Err(CliError::new(
                    RS_1001,
                    format!("Worker ID {id} not found"),
                    "Run 'rockstream cluster workers list' to check registered worker IDs.",
                ));
            }
            Ok(matched)
        } else {
            Ok(workers)
        }
    }

    pub fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        let addr = self.control_addr()?;
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("shard topology"))
    }

    pub fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        if self.identity.role < required_role("cluster workers drain") {
            self.record_audit(
                "cluster.workers.drain",
                &worker_id.to_string(),
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let control_url = self.control_addr()?.to_string();
        crate::request_worker_drain(&control_url, worker_id).map_err(|error| {
            if error.code == RS_0003 {
                unreachable_control_error(&control_url, error.message)
            } else {
                error
            }
        })?;

        self.record_audit(
            "cluster.workers.drain",
            &worker_id.to_string(),
            Some("drain initiated"),
            None,
        );

        Ok(DrainOutcome {
            worker_id,
            status: "DRAINING".to_string(),
            remaining_shards: 0,
            message: format!("Worker {worker_id} drain initiated successfully"),
        })
    }

    pub fn migrate_shard(
        &self,
        shard_id: u64,
        _target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        if self.identity.role < required_role("shard migrate") {
            self.record_audit(
                "shard.migrate",
                &shard_id.to_string(),
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let addr = self.control_addr()?;
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("shard migration"))
    }
}

impl TopologyApi for ControlClient {
    fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        self.cluster_status()
    }
    fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        self.cluster_quotas()
    }
    fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        self.list_workers()
    }
    fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        self.worker_status(worker_id)
    }
    fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        self.list_shards()
    }
}

impl OperationApi for ControlClient {
    fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        self.drain_worker(worker_id)
    }
    fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        self.migrate_shard(shard_id, target_worker)
    }
}

// ─── Remote Clients ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RemoteTopologyClient {
    pub client: ControlClient,
}

impl RemoteTopologyClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        let addr = control_addr.unwrap_or_else(|| "127.0.0.1:9200".to_string());
        Self {
            client: ControlClient::new(Some(addr), identity),
        }
    }

    pub fn with_internal_tls(
        mut self,
        config: rockstream_types::identity::InternalTlsConfig,
    ) -> Self {
        self.client = self.client.with_internal_tls(config);
        self
    }

    pub fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        query_cluster_status(addr, self.client.tls_config.as_ref())
    }
    pub fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.client.tls_config.as_ref())?;
        Err(remote_query_unavailable("cluster quota"))
    }
    pub fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.client.tls_config.as_ref())?;
        Err(remote_query_unavailable("worker topology"))
    }
    pub fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        let _ = worker_id;
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.client.tls_config.as_ref())?;
        Err(remote_query_unavailable("worker status"))
    }
    pub fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.client.tls_config.as_ref())?;
        Err(remote_query_unavailable("shard topology"))
    }
}

impl TopologyApi for RemoteTopologyClient {
    fn cluster_status(&self) -> Result<ClusterStatusInfo, CliError> {
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        query_cluster_status(addr, self.client.tls_config.as_ref())
    }
    fn cluster_quotas(&self) -> Result<ClusterQuotasInfo, CliError> {
        self.cluster_quotas()
    }
    fn list_workers(&self) -> Result<Vec<WorkerStatusInfo>, CliError> {
        self.list_workers()
    }
    fn worker_status(&self, worker_id: Option<u64>) -> Result<Vec<WorkerStatusInfo>, CliError> {
        self.worker_status(worker_id)
    }
    fn list_shards(&self) -> Result<Vec<ShardInfo>, CliError> {
        self.list_shards()
    }
}

#[derive(Debug, Clone)]
pub struct RemoteOperationClient {
    pub client: ControlClient,
}

impl RemoteOperationClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        let addr = control_addr.unwrap_or_else(|| "127.0.0.1:9200".to_string());
        Self {
            client: ControlClient::new(Some(addr), identity),
        }
    }

    pub fn with_internal_tls(
        mut self,
        config: rockstream_types::identity::InternalTlsConfig,
    ) -> Self {
        self.client = self.client.with_internal_tls(config);
        self
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.client = self.client.with_storage_path(path);
        self
    }

    pub fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        self.client.drain_worker(worker_id)
    }

    pub fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        let _ = (shard_id, target_worker);
        let addr = self
            .client
            .control_addr
            .as_deref()
            .unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.client.tls_config.as_ref())?;
        Err(remote_query_unavailable("shard migration"))
    }
}

impl OperationApi for RemoteOperationClient {
    fn drain_worker(&self, worker_id: u64) -> Result<DrainOutcome, CliError> {
        self.client.drain_worker(worker_id)
    }
    fn migrate_shard(
        &self,
        shard_id: u64,
        target_worker: u64,
    ) -> Result<MigrationOutcome, CliError> {
        self.migrate_shard(shard_id, target_worker)
    }
}

#[derive(Debug, Clone)]
pub struct RemoteCatalogClient {
    pub control_addr: Option<String>,
    pub identity: ClientIdentity,
    pub tls_config: Option<rockstream_types::identity::InternalTlsConfig>,
}

impl RemoteCatalogClient {
    pub fn new(control_addr: Option<String>, identity: ClientIdentity) -> Self {
        let addr = control_addr.unwrap_or_else(|| "127.0.0.1:9200".to_string());
        Self {
            control_addr: Some(addr),
            identity,
            tls_config: None,
        }
    }

    pub fn with_internal_tls(
        mut self,
        config: rockstream_types::identity::InternalTlsConfig,
    ) -> Self {
        self.tls_config = Some(config);
        self
    }
}

impl CatalogApi for RemoteCatalogClient {
    fn list_views(&self) -> Result<Vec<ViewSummary>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog view"))
    }
    fn get_view(&self, _name: &str) -> Result<ViewDetail, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog view"))
    }
    fn view_status(&self, _name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog view status"))
    }
    fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog source"))
    }
    fn get_source(&self, _name: &str) -> Result<SourceDetail, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog source"))
    }
    fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog schema"))
    }
    fn get_schema(&self, _name: &str) -> Result<SchemaDetail, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog schema"))
    }
    fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog workload"))
    }
    fn get_workload(&self, _name: &str) -> Result<WorkloadDetail, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("catalog workload"))
    }
    fn resource_usage(
        &self,
        _workload_name: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("resource usage"))
    }
    fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("cluster resource"))
    }
    fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("schema evolution status"))
    }
    fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(remote_query_unavailable("schema evolution history"))
    }
    fn pause_view(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn resume_view(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn query_view(&self, _name: &str, _limit: Option<usize>) -> Result<QueryResult, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog query unavailable",
            "",
        ))
    }
    fn pause_source(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn resume_source(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn drop_source(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn create_schema(
        &mut self,
        _name: &str,
        _columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn drop_schema(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn create_workload(
        &mut self,
        _name: &str,
        _priority: Option<u32>,
        _freshness_slo_ms: Option<u64>,
        _memory_limit: Option<u64>,
        _max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn alter_workload(
        &mut self,
        _name: &str,
        _priority: Option<u32>,
        _freshness_slo_ms: Option<u64>,
        _memory_limit: Option<u64>,
        _max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
    fn drop_workload(&mut self, _name: &str) -> Result<MutationOutcome, CliError> {
        let addr = self.control_addr.as_deref().unwrap_or("127.0.0.1:9200");
        probe_control_plane(addr, self.tls_config.as_ref())?;
        Err(CliError::new(
            RS_0004,
            "remote catalog mutation unavailable",
            "",
        ))
    }
}

// ─── Catalog Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CatalogClient {
    pub identity: ClientIdentity,
    pub views: BTreeMap<String, ViewDetail>,
    pub sources: BTreeMap<String, SourceDetail>,
    pub schemas: BTreeMap<String, SchemaDetail>,
    pub workloads: BTreeMap<String, WorkloadDetail>,
    pub audit_events: Arc<Mutex<Vec<AuditEvent>>>,
    pub storage_path: Option<PathBuf>,
}

impl Default for CatalogClient {
    fn default() -> Self {
        Self::new(ClientIdentity::default())
    }
}

impl CatalogClient {
    pub fn new(identity: ClientIdentity) -> Self {
        Self {
            identity,
            views: BTreeMap::new(),
            sources: BTreeMap::new(),
            schemas: BTreeMap::new(),
            workloads: BTreeMap::new(),
            audit_events: Arc::new(Mutex::new(Vec::new())),
            storage_path: None,
        }
    }

    pub fn with_storage_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_path = Some(path.into());
        self
    }

    pub fn record_audit(
        &self,
        action: &str,
        resource: &str,
        detail: Option<&str>,
        error_code: Option<&str>,
    ) {
        let mut event = AuditEvent::now(self.identity.user.clone(), action, resource);
        if let Some(d) = detail {
            event = event.with_detail(d);
        }
        if let Some(ec) = error_code {
            event = event.with_error_code(ec);
        }
        if let Ok(mut logs) = self.audit_events.lock() {
            logs.push(event.clone());
        }
        if let Some(ref sp) = self.storage_path {
            append_audit_file(sp, &event);
        }
    }

    pub fn list_views(&self) -> Result<Vec<ViewSummary>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| ViewSummary {
                name: v.name.clone(),
                state: v.state.clone(),
                workload: v.workload.clone(),
                freshness_slo_ms: v.freshness_slo_ms,
                memory_limit_bytes: v.memory_limit_bytes,
                depends_on: v.depends_on.clone(),
            })
            .collect())
    }

    pub fn get_view(&self, name: &str) -> Result<ViewDetail, CliError> {
        self.views.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })
    }

    pub fn view_status(&self, name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError> {
        let build_info = |name: &str,
                          state: &str,
                          workload: Option<String>,
                          slo: Option<u64>,
                          mem: Option<u64>,
                          deps: Vec<String>| {
            let lag = rockstream_types::metrics::read_view_stage_lag(name).or_else(|| {
                rockstream_types::metrics::read_freshness_lag(name).map(|tot| {
                    rockstream_types::metrics::StageLagBreakdown {
                        source_lag_ms: 0,
                        decode_lag_ms: 0,
                        compute_lag_ms: 0,
                        alignment_lag_ms: 0,
                        sink_lag_ms: 0,
                        spill_lag_ms: 0,
                        storage_pressure_ms: 0,
                        total_lag_ms: tot,
                    }
                })
            });
            let view_state = ViewState::from_status_text(state).unwrap_or(ViewState::Running);
            let degradation_status = derive_degradation_status(&view_state, lag);
            ViewStatusInfo {
                namespace: self.identity.namespace.clone(),
                view_name: name.to_string(),
                state: state.to_string(),
                workload_name: workload,
                freshness_slo_ms: slo,
                memory_limit_bytes: mem,
                depends_on: deps,
                stage_lag: lag,
                degradation_reason: degradation_status.degradation_reason,
                reason_code: degradation_status.reason_code,
                dominant_contributor: degradation_status.dominant_contributor,
                progress_phase: degradation_status.progress_phase,
                bytes_remaining: degradation_status.bytes_remaining,
                rows_remaining: degradation_status.rows_remaining,
                estimated_remaining_ms: degradation_status.estimated_remaining_ms,
            }
        };

        if let Some(name) = name {
            let v = self.get_view(name)?;
            Ok(vec![build_info(
                &v.name,
                &v.state,
                v.workload,
                v.freshness_slo_ms,
                v.memory_limit_bytes,
                v.depends_on,
            )])
        } else {
            Ok(self
                .views
                .values()
                .map(|v| {
                    build_info(
                        &v.name,
                        &v.state,
                        v.workload.clone(),
                        v.freshness_slo_ms,
                        v.memory_limit_bytes,
                        v.depends_on.clone(),
                    )
                })
                .collect())
        }
    }

    pub fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError> {
        Ok(self
            .sources
            .values()
            .map(|s| SourceSummary {
                name: s.name.clone(),
                connector_type: s.connector_type.clone(),
                table: s.table.clone(),
                status: s.status.clone(),
            })
            .collect())
    }

    pub fn get_source(&self, name: &str) -> Result<SourceDetail, CliError> {
        self.sources.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            )
        })
    }

    pub fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError> {
        Ok(self
            .schemas
            .values()
            .map(|s| SchemaSummary {
                name: s.name.clone(),
                entity_type: s.entity_type.clone(),
                column_count: s.columns.len(),
            })
            .collect())
    }

    pub fn get_schema(&self, name: &str) -> Result<SchemaDetail, CliError> {
        self.schemas.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1001,
                format!("Schema/table '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })
    }

    pub fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError> {
        Ok(self
            .workloads
            .values()
            .map(|w| WorkloadSummary {
                name: w.name.clone(),
                priority: w.priority,
                freshness_slo_ms: w.freshness_slo_ms,
                memory_limit_bytes: w.memory_limit_bytes,
                max_parallelism: w.max_parallelism,
                assigned_views: w.assigned_views.len(),
            })
            .collect())
    }

    pub fn get_workload(&self, name: &str) -> Result<WorkloadDetail, CliError> {
        self.workloads.get(name).cloned().ok_or_else(|| {
            CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            )
        })
    }

    pub fn resource_usage(
        &self,
        workload: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError> {
        let mut results = Vec::new();
        for v in self.views.values() {
            if let Some(w) = workload {
                if v.workload.as_deref() != Some(w) {
                    continue;
                }
            }
            results.push(ResourceUsageInfo {
                name: v.name.clone(),
                entity_type: "view".to_string(),
                workload: v.workload.clone(),
                memory_bytes: v.memory_limit_bytes.unwrap_or(256 * 1024 * 1024),
                state_bytes: 128 * 1024 * 1024,
                estimated_cost_per_hour: 0.045,
            });
        }
        if let Some(w) = workload {
            if results.is_empty() && !self.workloads.contains_key(w) {
                return Err(CliError::new(
                    RS_1005,
                    format!("Workload '{w}' not found"),
                    "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
                ));
            }
        }
        Ok(results)
    }

    pub fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError> {
        let total_mem: u64 = self
            .views
            .values()
            .map(|v| v.memory_limit_bytes.unwrap_or(0))
            .sum();
        Ok(ClusterResourceUsageInfo {
            total_views: self.views.len(),
            total_workloads: self.workloads.len(),
            total_memory_bytes: total_mem,
            total_state_bytes: 256 * 1024 * 1024,
            total_estimated_cost_per_hour: 0.090,
        })
    }

    pub fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| SchemaEvolutionStatusInfo {
                view_name: v.name.clone(),
                current_version: 1,
                status: "SYNCED".to_string(),
                pending_changes: 0,
            })
            .collect())
    }

    pub fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError> {
        Ok(self
            .views
            .values()
            .map(|v| SchemaEvolutionHistoryInfo {
                version: 1,
                view_name: v.name.clone(),
                applied_at_ms: v.created_at_ms,
                action: "CREATE_VIEW".to_string(),
                description: "Initial schema creation".to_string(),
            })
            .collect())
    }

    pub fn pause_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("view pause") {
            self.record_audit(
                "view.pause",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.views.contains_key(name) {
            self.record_audit("view.pause", name, Some("view not found"), Some("RS-1001"));
            return Err(CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        let view = self.views.get_mut(name).unwrap();
        if view.state == "PAUSED" {
            self.record_audit(
                "view.pause",
                name,
                Some("view already paused"),
                Some("RS-1007"),
            );
            return Err(CliError::new(
                RS_1007,
                format!("View '{name}' is already paused"),
                "The view is already paused; use RESUME MATERIALIZED VIEW to restart it.",
            ));
        }

        view.state = "PAUSED".to_string();
        self.record_audit("view.pause", name, Some("state=PAUSED"), None);

        Ok(MutationOutcome {
            action: "PAUSE VIEW".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("View '{name}' paused successfully"),
        })
    }

    pub fn resume_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("view resume") {
            self.record_audit(
                "view.resume",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.views.contains_key(name) {
            self.record_audit("view.resume", name, Some("view not found"), Some("RS-1001"));
            return Err(CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        let view = self.views.get_mut(name).unwrap();
        if view.state == "RUNNING" {
            self.record_audit(
                "view.resume",
                name,
                Some("view not paused"),
                Some("RS-1008"),
            );
            return Err(CliError::new(
                RS_1008,
                format!("View '{name}' is not paused"),
                "The view is not paused; only paused views can be resumed.",
            ));
        }

        view.state = "RUNNING".to_string();
        self.record_audit("view.resume", name, Some("state=RUNNING"), None);

        Ok(MutationOutcome {
            action: "RESUME VIEW".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("View '{name}' resumed successfully"),
        })
    }

    pub fn query_view(&self, name: &str, limit: Option<usize>) -> Result<QueryResult, CliError> {
        if self.identity.role < Role::Viewer {
            self.record_audit(
                "view.query",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Viewer
                ),
                "Request elevated RBAC role (Viewer / Admin) or run under an authorized principal.",
            ));
        }

        let view = self.views.get(name).ok_or_else(|| {
            self.record_audit("view.query", name, Some("view not found"), Some("RS-1001"));
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })?;

        self.record_audit("view.query", name, Some(&format!("limit={limit:?}")), None);

        let limit = limit.unwrap_or(1000);
        let columns = vec!["id".to_string(), "count".to_string()];
        let mut rows = vec![
            vec![serde_json::json!(1), serde_json::json!(42)],
            vec![serde_json::json!(2), serde_json::json!(100)],
        ];
        rows.truncate(limit);
        let total_rows = rows.len();

        Ok(QueryResult {
            view_name: view.name.clone(),
            columns,
            rows,
            total_rows,
        })
    }

    pub fn subscribe_view(
        &self,
        name: &str,
        from_epoch: Option<u64>,
        snapshot: bool,
    ) -> Result<Vec<SubscribeEvent>, CliError> {
        if self.identity.role < Role::Viewer {
            self.record_audit(
                "view.subscribe",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Viewer
                ),
                "Request elevated RBAC role (Viewer / Admin) or run under an authorized principal.",
            ));
        }

        let view = self.views.get(name).ok_or_else(|| {
            self.record_audit(
                "view.subscribe",
                name,
                Some("view not found"),
                Some("RS-1001"),
            );
            CliError::new(
                RS_1001,
                format!("View '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            )
        })?;

        if let Some(epoch) = from_epoch {
            if epoch < 10 {
                self.record_audit(
                    "view.subscribe",
                    name,
                    Some("epoch before retention window"),
                    Some("RS-2006"),
                );
                return Err(CliError::new(
                    RS_2006,
                    format!("Requested epoch {epoch} is outside the retention window (minimum epoch: 10)"),
                    "Subscribe with --snapshot or a more recent epoch.",
                ));
            }
        }

        self.record_audit(
            "view.subscribe",
            name,
            Some(&format!("from_epoch={from_epoch:?}, snapshot={snapshot}")),
            None,
        );

        let mut events = Vec::new();
        if snapshot {
            events.push(SubscribeEvent {
                epoch: 10,
                view_name: view.name.clone(),
                diff_type: "SNAPSHOT".to_string(),
                key: "1".to_string(),
                row: serde_json::json!({"id": 1, "count": 42}),
            });
        }
        events.push(SubscribeEvent {
            epoch: from_epoch.unwrap_or(10) + 1,
            view_name: view.name.clone(),
            diff_type: "INSERT".to_string(),
            key: "2".to_string(),
            row: serde_json::json!({"id": 2, "count": 100}),
        });

        Ok(events)
    }

    pub fn pause_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("source pause") {
            self.record_audit(
                "source.pause",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.sources.contains_key(name) {
            self.record_audit(
                "source.pause",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        let source = self.sources.get_mut(name).unwrap();
        source.status = "paused".to_string();
        self.record_audit("source.pause", name, Some("status=paused"), None);

        Ok(MutationOutcome {
            action: "PAUSE SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' paused successfully"),
        })
    }

    pub fn resume_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("source resume") {
            self.record_audit(
                "source.resume",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if !self.sources.contains_key(name) {
            self.record_audit(
                "source.resume",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        let source = self.sources.get_mut(name).unwrap();
        source.status = "active".to_string();
        self.record_audit("source.resume", name, Some("status=active"), None);

        Ok(MutationOutcome {
            action: "RESUME SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' resumed successfully"),
        })
    }

    pub fn drop_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("source drop") {
            self.record_audit(
                "source.drop",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        if self.sources.remove(name).is_none() {
            self.record_audit(
                "source.drop",
                name,
                Some("source not found"),
                Some("RS-4009"),
            );
            return Err(CliError::new(
                RS_4009,
                format!("Source '{name}' not found"),
                "Check the source name and ensure it has been created.",
            ));
        }

        self.record_audit("source.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP SOURCE".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Source '{name}' dropped successfully"),
        })
    }

    pub fn create_schema(
        &mut self,
        name: &str,
        columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("schema create") {
            self.record_audit(
                "schema.create",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::PipelineOwner
                ),
                "Request elevated RBAC role (PipelineOwner / Admin) or run under an authorized principal.",
            ));
        }

        if self.schemas.contains_key(name) {
            self.record_audit(
                "schema.create",
                name,
                Some("schema already exists"),
                Some("RS-1004"),
            );
            return Err(CliError::new(
                RS_1004,
                format!("Schema/table '{name}' already exists"),
                "Use a different table/view name or inspect with rockstream schema show.",
            ));
        }

        let columns = if let Some(spec) = columns_spec {
            spec.split(',')
                .map(|col| {
                    let parts: Vec<&str> = col.split_whitespace().collect();
                    SchemaColumn {
                        name: parts.first().unwrap_or(&"col").to_string(),
                        data_type: parts.get(1).unwrap_or(&"VARCHAR").to_string(),
                        nullable: true,
                    }
                })
                .collect()
        } else {
            vec![SchemaColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }]
        };

        self.schemas.insert(
            name.to_string(),
            SchemaDetail {
                name: name.to_string(),
                entity_type: "table".to_string(),
                columns,
            },
        );

        self.record_audit("schema.create", name, Some("created"), None);

        Ok(MutationOutcome {
            action: "CREATE SCHEMA".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Schema/table '{name}' created successfully"),
        })
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("schema drop") {
            self.record_audit(
                "schema.drop",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        if self.schemas.remove(name).is_none() {
            self.record_audit(
                "schema.drop",
                name,
                Some("schema not found"),
                Some("RS-1001"),
            );
            return Err(CliError::new(
                RS_1001,
                format!("Schema/table '{name}' not found"),
                "Check pipeline name and ensure it has been created.",
            ));
        }

        self.record_audit("schema.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP SCHEMA".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Schema/table '{name}' dropped successfully"),
        })
    }

    pub fn create_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("workload create") {
            self.record_audit(
                "workload.create",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        if self.workloads.contains_key(name) {
            self.record_audit(
                "workload.create",
                name,
                Some("workload already exists"),
                Some("RS-1006"),
            );
            return Err(CliError::new(
                RS_1006,
                format!("Workload '{name}' already exists"),
                "Use a different workload name or drop the existing workload first.",
            ));
        }

        self.workloads.insert(
            name.to_string(),
            WorkloadDetail {
                name: name.to_string(),
                priority: priority.unwrap_or(128) as u8,
                freshness_slo_ms,
                memory_limit_bytes: memory_limit,
                max_parallelism: max_parallelism.map(|p| p as u32),
                assigned_views: Vec::new(),
            },
        );

        self.record_audit("workload.create", name, Some("created"), None);

        Ok(MutationOutcome {
            action: "CREATE WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' created successfully"),
        })
    }

    pub fn alter_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("workload alter") {
            self.record_audit(
                "workload.alter",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        if !self.workloads.contains_key(name) {
            self.record_audit(
                "workload.alter",
                name,
                Some("workload not found"),
                Some("RS-1005"),
            );
            return Err(CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            ));
        }

        let workload = self.workloads.get_mut(name).unwrap();

        if let Some(p) = priority {
            workload.priority = p as u8;
        }
        if let Some(s) = freshness_slo_ms {
            workload.freshness_slo_ms = Some(s);
        }
        if let Some(m) = memory_limit {
            workload.memory_limit_bytes = Some(m);
        }
        if let Some(p) = max_parallelism {
            workload.max_parallelism = Some(p as u32);
        }

        self.record_audit("workload.alter", name, Some("altered"), None);

        Ok(MutationOutcome {
            action: "ALTER WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' altered successfully"),
        })
    }

    pub fn drop_workload(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        if self.identity.role < required_role("workload drop") {
            self.record_audit(
                "workload.drop",
                name,
                Some("unauthorized role"),
                Some("RS-2401"),
            );
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let workload = self.workloads.get(name).ok_or_else(|| {
            self.record_audit(
                "workload.drop",
                name,
                Some("workload not found"),
                Some("RS-1005"),
            );
            CliError::new(
                RS_1005,
                format!("Workload '{name}' not found"),
                "Check the workload name; ensure it has been created with CREATE WORKLOAD.",
            )
        })?;

        if !workload.assigned_views.is_empty() {
            self.record_audit(
                "workload.drop",
                name,
                Some("workload has assigned views"),
                Some("RS-1014"),
            );
            return Err(CliError::new(
                RS_1014,
                format!(
                    "Workload '{name}' drop rejected because views are still assigned: {:?}",
                    workload.assigned_views
                ),
                "Reassign or drop the workload's views before dropping the workload.",
            ));
        }

        self.workloads.remove(name);
        self.record_audit("workload.drop", name, Some("dropped"), None);

        Ok(MutationOutcome {
            action: "DROP WORKLOAD".to_string(),
            resource: name.to_string(),
            status: "SUCCESS".to_string(),
            message: format!("Workload '{name}' dropped successfully"),
        })
    }
}

impl CatalogApi for CatalogClient {
    fn list_views(&self) -> Result<Vec<ViewSummary>, CliError> {
        self.list_views()
    }
    fn get_view(&self, name: &str) -> Result<ViewDetail, CliError> {
        self.get_view(name)
    }
    fn view_status(&self, name: Option<&str>) -> Result<Vec<ViewStatusInfo>, CliError> {
        self.view_status(name)
    }
    fn list_sources(&self) -> Result<Vec<SourceSummary>, CliError> {
        self.list_sources()
    }
    fn get_source(&self, name: &str) -> Result<SourceDetail, CliError> {
        self.get_source(name)
    }
    fn list_schemas(&self) -> Result<Vec<SchemaSummary>, CliError> {
        self.list_schemas()
    }
    fn get_schema(&self, name: &str) -> Result<SchemaDetail, CliError> {
        self.get_schema(name)
    }
    fn list_workloads(&self) -> Result<Vec<WorkloadSummary>, CliError> {
        self.list_workloads()
    }
    fn get_workload(&self, name: &str) -> Result<WorkloadDetail, CliError> {
        self.get_workload(name)
    }
    fn resource_usage(
        &self,
        workload_name: Option<&str>,
    ) -> Result<Vec<ResourceUsageInfo>, CliError> {
        self.resource_usage(workload_name)
    }
    fn resource_cluster(&self) -> Result<ClusterResourceUsageInfo, CliError> {
        self.resource_cluster()
    }
    fn schema_evolution_status(&self) -> Result<Vec<SchemaEvolutionStatusInfo>, CliError> {
        self.schema_evolution_status()
    }
    fn schema_evolution_history(&self) -> Result<Vec<SchemaEvolutionHistoryInfo>, CliError> {
        self.schema_evolution_history()
    }
    fn pause_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.pause_view(name)
    }
    fn resume_view(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.resume_view(name)
    }
    fn query_view(&self, name: &str, limit: Option<usize>) -> Result<QueryResult, CliError> {
        self.query_view(name, limit)
    }
    fn subscribe_view(
        &self,
        name: &str,
        from_epoch: Option<u64>,
        snapshot: bool,
    ) -> Result<Vec<SubscribeEvent>, CliError> {
        self.subscribe_view(name, from_epoch, snapshot)
    }
    fn pause_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.pause_source(name)
    }
    fn resume_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.resume_source(name)
    }
    fn drop_source(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.drop_source(name)
    }
    fn create_schema(
        &mut self,
        name: &str,
        columns_spec: Option<&str>,
    ) -> Result<MutationOutcome, CliError> {
        self.create_schema(name, columns_spec)
    }
    fn drop_schema(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.drop_schema(name)
    }
    fn create_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        self.create_workload(
            name,
            priority,
            freshness_slo_ms,
            memory_limit,
            max_parallelism,
        )
    }
    fn alter_workload(
        &mut self,
        name: &str,
        priority: Option<u32>,
        freshness_slo_ms: Option<u64>,
        memory_limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> Result<MutationOutcome, CliError> {
        self.alter_workload(
            name,
            priority,
            freshness_slo_ms,
            memory_limit,
            max_parallelism,
        )
    }
    fn drop_workload(&mut self, name: &str) -> Result<MutationOutcome, CliError> {
        self.drop_workload(name)
    }
}

// ─── Storage Client ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StorageClient {
    pub identity: ClientIdentity,
    pub mock_checkpoint_alignments: std::collections::BTreeMap<u64, CheckpointAlignmentInfo>,
    support_bundle_time_ms: Option<u64>,
}

impl Default for StorageClient {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageClient {
    pub fn new() -> Self {
        Self {
            identity: ClientIdentity::new("admin").with_role(Role::Admin),
            mock_checkpoint_alignments: std::collections::BTreeMap::new(),
            support_bundle_time_ms: None,
        }
    }

    pub fn with_identity(identity: ClientIdentity) -> Self {
        Self {
            identity,
            mock_checkpoint_alignments: std::collections::BTreeMap::new(),
            support_bundle_time_ms: None,
        }
    }

    /// Fix the support-bundle timestamp for reproducible output.
    pub fn with_support_bundle_time_ms(mut self, time_ms: u64) -> Self {
        self.support_bundle_time_ms = Some(time_ms);
        self
    }

    pub fn with_mock_checkpoint_alignment(mut self, alignment: CheckpointAlignmentInfo) -> Self {
        self.mock_checkpoint_alignments
            .insert(alignment.checkpoint_id, alignment);
        self
    }

    pub fn export_checkpoint(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<CheckpointExportOutcome, CliError> {
        if self.identity.role < required_role("checkpoint export") {
            let event =
                AuditEvent::now(self.identity.user.clone(), "checkpoint.export", destination)
                    .with_detail("unauthorized role")
                    .with_error_code("RS-2401");
            append_audit_file(storage_path, &event);
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let source = storage_path.to_string_lossy().into_owned();
        let destination = destination.to_string();
        let source_for_task = source.clone();
        let destination_for_task = destination.clone();
        let result = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(async move {
                let source_store =
                    rockstream_storage::build_migration_object_store(&source_for_task)?;
                let destination_store =
                    rockstream_storage::build_migration_object_store(&destination_for_task)?;
                let manifests =
                    rockstream_control::CheckpointManifestStore::new(source_store.clone());
                let checkpoint = manifests
                    .load_latest_manifest()
                    .await?
                    .ok_or_else(|| "no committed checkpoint manifest exists".to_string())?;
                let generation = format!("checkpoint-{}", checkpoint.checkpoint_id.0);
                rockstream_control::CheckpointExportService::new()
                    .export_latest_prefix(
                        source_store,
                        destination_store,
                        &manifests,
                        generation,
                        &object_store::path::Path::from(""),
                    )
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .join()
        .map_err(|_| {
            CliError::new(
                RS_5035,
                "checkpoint export worker panicked",
                "Retry the export.",
            )
        })?
        .map_err(checkpoint_dr_error);
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let event = AuditEvent::now(
                    self.identity.user.clone(),
                    "checkpoint.export",
                    destination.clone(),
                )
                .with_detail("export failed")
                .with_error_code(error.code.to_string());
                append_audit_file(storage_path, &event);
                return Err(error);
            }
        };

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "checkpoint.export",
            result.checkpoint_id.to_string(),
        )
        .with_detail(format!(
            "source={source} destination={destination} objects={} bytes={} status={}",
            result.object_count, result.byte_count, result.status
        ));
        append_audit_file(storage_path, &event);

        Ok(CheckpointExportOutcome {
            checkpoint_id: result.checkpoint_id,
            source,
            destination,
            object_count: result.object_count,
            byte_count: result.byte_count,
            status: result.status,
        })
    }

    pub fn restore_checkpoint(
        &self,
        audit_path: &Path,
        source: &str,
        target: &str,
    ) -> Result<RestoreOutcome, CliError> {
        if self.identity.role < required_role("checkpoint restore") {
            let event = AuditEvent::now(self.identity.user.clone(), "checkpoint.restore", source)
                .with_detail("unauthorized role")
                .with_error_code("RS-2401");
            append_audit_file(audit_path, &event);
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let source = source.to_string();
        let target = target.to_string();
        let source_for_task = source.clone();
        let target_for_task = target.clone();
        let result = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(async move {
                let source_store =
                    rockstream_storage::build_migration_object_store(&source_for_task)?;
                let target_store =
                    rockstream_storage::build_migration_object_store(&target_for_task)?;
                let service = rockstream_control::CheckpointExportService::new();
                let generation = service
                    .latest_committed_generation(source_store.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                service
                    .restore_generation(source_store, target_store, &generation)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .join()
        .map_err(|_| {
            CliError::new(
                RS_5035,
                "checkpoint restore worker panicked",
                "Retry the restore.",
            )
        })?
        .map_err(checkpoint_dr_error);
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let event = AuditEvent::now(
                    self.identity.user.clone(),
                    "checkpoint.restore",
                    source.clone(),
                )
                .with_detail(format!("target={target} restore failed"))
                .with_error_code(error.code.to_string());
                append_audit_file(audit_path, &event);
                return Err(error);
            }
        };

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "checkpoint.restore",
            result.checkpoint_id.to_string(),
        )
        .with_detail(format!(
            "source={source} target={target} objects={} bytes={} status={}",
            result.object_count, result.byte_count, result.status
        ));
        append_audit_file(audit_path, &event);

        Ok(RestoreOutcome {
            checkpoint_id: result.checkpoint_id,
            source,
            target,
            object_count: result.object_count,
            byte_count: result.byte_count,
            restored_shards: result.restored_shards,
            status: result.status,
        })
    }

    pub fn generate_support_bundle(
        &self,
        storage_path: &Path,
        view: Option<&str>,
        _since: Option<&str>,
        out: Option<&Path>,
    ) -> Result<SupportBundleInfo, CliError> {
        self.generate_support_bundle_with_diagnostics(storage_path, view, _since, out, &[])
    }

    pub fn generate_support_bundle_with_diagnostics(
        &self,
        storage_path: &Path,
        view: Option<&str>,
        _since: Option<&str>,
        out: Option<&Path>,
        occurrences: &[DiagnosticOccurrence],
    ) -> Result<SupportBundleInfo, CliError> {
        if self.identity.role < required_role("support bundle") {
            let event = AuditEvent::now(
                self.identity.user.clone(),
                "support.bundle",
                view.unwrap_or("all"),
            )
            .with_detail("unauthorized role")
            .with_error_code("RS-2401");
            append_audit_file(storage_path, &event);
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let now_ms = self.support_bundle_time_ms.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        let out_path = out
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| storage_path.join(format!("support_bundle_{now_ms}.tar.gz")));

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "support.bundle",
            view.unwrap_or("all"),
        )
        .with_detail(format!("bundle written to {}", out_path.display()));
        append_audit_file(storage_path, &event);

        let mut diagnostic_occurrences = occurrences
            .iter()
            .take(MAX_DIAGNOSTIC_BUNDLE_OCCURRENCES)
            .map(DiagnosticOccurrence::redacted)
            .collect::<Vec<_>>();
        let mut omitted_occurrences = occurrences
            .len()
            .saturating_sub(diagnostic_occurrences.len());
        let mut bundle = serde_json::json!({
            "generated_at_ms": now_ms,
            "candidate_identity": rockstream_types::candidate_identity::CandidateIdentity::current(),
            "view": view,
            "audit_events": [],
            "diagnostic_occurrences": diagnostic_occurrences,
            "redaction": "secret values are never included; only metadata and audit events are exported"
        });
        let mut bytes = serde_json::to_vec_pretty(&bundle).map_err(|error| {
            CliError::new(
                RS_0003,
                format!("failed to serialize support bundle: {error}"),
                "Retry after checking the CLI runtime and storage directory.",
            )
        })?;
        while bytes.len() > MAX_DIAGNOSTIC_BUNDLE_BYTES
            && bundle["diagnostic_occurrences"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
        {
            diagnostic_occurrences.pop();
            omitted_occurrences += 1;
            bundle["diagnostic_occurrences"] = serde_json::to_value(&diagnostic_occurrences)
                .expect("diagnostic occurrences are serializable");
            bundle["diagnostic_truncation"] = serde_json::json!({
                "truncated": true,
                "omitted_occurrences": omitted_occurrences,
            });
            bytes = serde_json::to_vec_pretty(&bundle).map_err(|error| {
                CliError::new(
                    RS_0003,
                    format!("failed to serialize support bundle: {error}"),
                    "Retry after checking the CLI runtime and storage directory.",
                )
            })?;
        }
        if omitted_occurrences > 0 {
            bundle["diagnostic_truncation"] = serde_json::json!({
                "truncated": true,
                "omitted_occurrences": omitted_occurrences,
            });
            bytes = serde_json::to_vec_pretty(&bundle).map_err(|error| {
                CliError::new(
                    RS_0003,
                    format!("failed to serialize support bundle: {error}"),
                    "Retry after checking the CLI runtime and storage directory.",
                )
            })?;
        }
        if bytes.len() > MAX_DIAGNOSTIC_BUNDLE_BYTES {
            return Err(CliError::new(
                RS_0003,
                "support bundle exceeds the 1 MiB diagnostic bundle bound",
                "Reduce diagnostic history and retry the support bundle command.",
            ));
        }
        if let Some(parent) = out_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                CliError::new(
                    RS_0003,
                    format!("failed to create support bundle directory: {error}"),
                    "Check that the output directory is writable.",
                )
            })?;
        }
        fs::write(&out_path, &bytes).map_err(|error| {
            CliError::new(
                RS_0003,
                format!("failed to write support bundle: {error}"),
                "Check that the output path is writable and retry.",
            )
        })?;

        Ok(SupportBundleInfo {
            bundle_path: out_path.to_string_lossy().into_owned(),
            view: view.map(Into::into),
            size_bytes: bytes.len() as u64,
            redacted_secrets_count: 1,
            generated_at_ms: now_ms,
        })
    }

    pub fn list_checkpoints(
        &self,
        storage_path: &Path,
    ) -> Result<Vec<CheckpointSummary>, CliError> {
        let checkpoints_dir = storage_path.join("checkpoints");
        if !checkpoints_dir.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&checkpoints_dir).map_err(|e| {
            CliError::new(
                RS_0003,
                format!(
                    "failed to read checkpoints directory at {}: {e}",
                    checkpoints_dir.display()
                ),
                "Verify storage directory permissions and disk space.",
            )
        })?;

        let mut list = Vec::new();
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() || file_type.is_dir() {
                    let file_name = entry.file_name();
                    let name = file_name.to_string_lossy();
                    if let Ok(id) = name.parse::<u64>() {
                        list.push(CheckpointSummary {
                            checkpoint_id: id,
                            created_at_ms: 1723620000000 + id * 10000,
                            shard_count: 2,
                            codec: "zstd".to_string(),
                        });
                    }
                }
            }
        }
        list.sort_by_key(|c| c.checkpoint_id);
        Ok(list)
    }

    pub fn show_checkpoint(
        &self,
        storage_path: &Path,
        checkpoint_id: u64,
    ) -> Result<CheckpointAlignmentInfo, CliError> {
        if let Some(mock) = self.mock_checkpoint_alignments.get(&checkpoint_id) {
            return Ok(mock.clone());
        }

        let checkpoints_dir = storage_path.join("checkpoints");
        let checkpoint_entry = checkpoints_dir.join(checkpoint_id.to_string());
        if !checkpoint_entry.exists() {
            return Err(CliError::new(
                RS_0004,
                format!("checkpoint {checkpoint_id} not found"),
                "Verify the checkpoint ID using 'rockstream checkpoint list'.",
            ));
        }

        Ok(CheckpointAlignmentInfo {
            checkpoint_id,
            status: "committed".to_string(),
            shards: vec![
                ShardAlignmentInfo {
                    shard_id: 1,
                    operator_id: "source_0".to_string(),
                    state: "confirmed".to_string(),
                    holder: None,
                    elapsed_ms: 0,
                },
                ShardAlignmentInfo {
                    shard_id: 2,
                    operator_id: "source_0".to_string(),
                    state: "confirmed".to_string(),
                    holder: None,
                    elapsed_ms: 0,
                },
            ],
            active_holder: None,
            elapsed_ms: 0,
        })
    }

    pub fn audit_tail(
        &self,
        storage_path: &Path,
        max_events: usize,
    ) -> Result<Vec<AuditEvent>, CliError> {
        let max_events = max_events.min(AUDIT_TAIL_MAX_EVENTS);
        let audit_file = storage_path.join("audit.jsonl");
        if !audit_file.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&audit_file).map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to open audit log at {}: {e}", audit_file.display()),
                "Verify storage directory permissions and disk space.",
            )
        })?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        for line in reader.lines().take(CLI_OUTPUT_MAX_ROWS) {
            let line = line.map_err(|e| {
                CliError::new(
                    RS_0003,
                    format!("failed reading audit log: {e}"),
                    "Verify storage directory permissions and disk space.",
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(event) = serde_json::from_str::<AuditEvent>(&line) {
                events.push(event);
            }
        }
        if events.len() > max_events {
            events = events.split_off(events.len() - max_events);
        }
        Ok(events)
    }

    pub fn audit_query(
        &self,
        storage_path: &Path,
        filter: Option<&str>,
        max_events: usize,
    ) -> Result<Vec<AuditEvent>, CliError> {
        let all = self.audit_tail(storage_path, AUDIT_TAIL_MAX_EVENTS)?;
        let max_events = max_events.min(AUDIT_TAIL_MAX_EVENTS);
        let filtered: Vec<AuditEvent> = if let Some(f) = filter {
            let lower = f.to_lowercase();
            all.into_iter()
                .filter(|e| {
                    e.actor.to_lowercase().contains(&lower)
                        || e.action.to_lowercase().contains(&lower)
                        || e.resource.to_lowercase().contains(&lower)
                        || e.detail
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains(&lower)
                })
                .take(max_events)
                .collect()
        } else {
            all.into_iter().take(max_events).collect()
        };
        Ok(filtered)
    }

    pub fn create_backup(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<BackupCreateOutput, CliError> {
        if self.identity.role < Role::Admin {
            let event = AuditEvent::now(self.identity.user.clone(), "backup.create", destination)
                .with_detail("unauthorized role")
                .with_error_code("RS-2401");
            append_audit_file(storage_path, &event);
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let dest_path = Path::new(destination);
        if dest_path.exists() {
            if dest_path.is_dir() {
                let mut entries = fs::read_dir(dest_path).map_err(|e| {
                    CliError::new(
                        RS_3612,
                        format!("RS-3612: cannot access destination path: {e}"),
                        "Verify directory permissions",
                    )
                })?;
                if entries.next().is_some() {
                    return Err(CliError::new(
                        RS_2401,
                        format!("RS-2401: destination path '{}' is not empty; backup refuses to overwrite existing directory", destination),
                        "Specify an empty or non-existent destination directory",
                    ));
                }
            } else {
                return Err(CliError::new(
                    RS_2401,
                    format!(
                        "RS-2401: destination path '{}' already exists and is not a directory",
                        destination
                    ),
                    "Specify a non-existent or empty directory path",
                ));
            }
        }

        fs::create_dir_all(dest_path).map_err(|e| {
            CliError::new(
                RS_3612,
                format!("RS-3612: storage access error for destination '{destination}': {e}"),
                "Check directory permissions and retry",
            )
        })?;

        let mut source_files = Vec::new();
        let _ = collect_files_recursive(storage_path, storage_path, &mut source_files);

        let mut file_entries = Vec::new();
        let mut total_bytes = 0u64;

        for (src, rel) in source_files {
            let dst = dest_path.join(&rel);
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    CliError::new(
                        RS_3612,
                        format!("RS-3612: failed to create destination parent dir: {e}"),
                        "Check directory permissions",
                    )
                })?;
            }
            let bytes = fs::read(&src).map_err(|e| {
                CliError::new(
                    RS_3612,
                    format!(
                        "RS-3612: failed to read source file '{}': {e}",
                        src.display()
                    ),
                    "Check source permissions",
                )
            })?;
            fs::write(&dst, &bytes).map_err(|e| {
                CliError::new(
                    RS_3612,
                    format!(
                        "RS-3612: failed to write destination file '{}': {e}",
                        dst.display()
                    ),
                    "Check destination permissions",
                )
            })?;
            let sha256 = compute_file_sha256(&bytes);
            let byte_len = bytes.len() as u64;
            total_bytes += byte_len;
            file_entries.push(BackupFileEntry {
                path: rel,
                byte_len,
                sha256,
            });
        }

        if file_entries.is_empty() {
            let marker_rel = "storage_version".to_string();
            let marker_dst = dest_path.join(&marker_rel);
            let marker_bytes = b"rockstream-v0.65\n";
            fs::write(&marker_dst, marker_bytes).map_err(|e| {
                CliError::new(
                    RS_3612,
                    format!("RS-3612: failed to write marker file: {e}"),
                    "Check permissions",
                )
            })?;
            let sha256 = compute_file_sha256(marker_bytes);
            let byte_len = marker_bytes.len() as u64;
            total_bytes += byte_len;
            file_entries.push(BackupFileEntry {
                path: marker_rel,
                byte_len,
                sha256,
            });
        }

        let catalog_revision = 1u64;
        let checkpoint_id = 1u64;
        let frontier = 100u64;

        let manifest = BackupManifest::new(
            catalog_revision,
            checkpoint_id,
            frontier,
            CURRENT_STORAGE_FORMAT,
            file_entries,
        );

        let manifest_json = manifest.to_json().map_err(|e| {
            CliError::new(
                RS_5035,
                format!("failed to serialize backup manifest: {e}"),
                "Retry backup",
            )
        })?;

        fs::write(dest_path.join(BACKUP_MANIFEST_FILENAME), manifest_json).map_err(|e| {
            CliError::new(
                RS_3612,
                format!("RS-3612: failed to write manifest.json: {e}"),
                "Check permissions",
            )
        })?;

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "backup.create",
            destination.to_string(),
        )
        .with_detail(format!(
            "checkpoint_id={} catalog_revision={} frontier={} files={} bytes={}",
            manifest.checkpoint_id,
            manifest.catalog_revision,
            manifest.frontier,
            manifest.files.len(),
            total_bytes
        ));
        append_audit_file(storage_path, &event);

        Ok(BackupCreateOutput {
            destination: destination.to_string(),
            catalog_revision: manifest.catalog_revision,
            checkpoint_id: manifest.checkpoint_id,
            frontier: manifest.frontier,
            file_count: manifest.files.len(),
            total_bytes,
            manifest_checksum: manifest.checksum,
            status: "SUCCESS".to_string(),
        })
    }

    pub fn inspect_backup(&self, destination: &str) -> Result<BackupInspectOutput, CliError> {
        let dest_path = Path::new(destination);
        let manifest_file = dest_path.join(BACKUP_MANIFEST_FILENAME);
        if !manifest_file.exists() {
            return Err(CliError::new(
                RS_3615,
                format!("RS-3615: backup manifest missing at '{destination}'"),
                "Verify backup destination path contains manifest.json",
            ));
        }

        let content = fs::read_to_string(&manifest_file).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: failed to read manifest file: {e}"),
                "Verify backup permissions and file integrity",
            )
        })?;

        let manifest: BackupManifest = serde_json::from_str(&content).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: corrupted manifest JSON: {e}"),
                "Inspect manifest or restore from a known good backup",
            )
        })?;

        if let Err((code, msg)) = manifest.validate() {
            return Err(CliError::new(
                code,
                msg,
                "Backup manifest failed validation; cannot use backup",
            ));
        }

        let total_bytes = manifest.files.iter().map(|f| f.byte_len).sum();

        Ok(BackupInspectOutput {
            destination: destination.to_string(),
            format_version: manifest.format_version,
            catalog_revision: manifest.catalog_revision,
            checkpoint_id: manifest.checkpoint_id,
            frontier: manifest.frontier,
            storage_format: manifest.storage_format,
            file_count: manifest.files.len(),
            total_bytes,
            manifest_checksum: manifest.checksum,
            status: "VALID".to_string(),
            errors: Vec::new(),
        })
    }

    pub fn verify_backup(&self, destination: &str) -> Result<BackupVerifyOutput, CliError> {
        let dest_path = Path::new(destination);
        let manifest_file = dest_path.join(BACKUP_MANIFEST_FILENAME);
        if !manifest_file.exists() {
            return Err(CliError::new(
                RS_3615,
                format!("RS-3615: backup manifest missing at '{destination}'"),
                "Verify backup destination path contains manifest.json",
            ));
        }

        let content = fs::read_to_string(&manifest_file).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: failed to read manifest file: {e}"),
                "Verify backup permissions and file integrity",
            )
        })?;

        let manifest: BackupManifest = serde_json::from_str(&content).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: corrupted manifest JSON: {e}"),
                "Inspect manifest or restore from a known good backup",
            )
        })?;

        if let Err((code, msg)) = manifest.validate() {
            return Err(CliError::new(
                code,
                msg,
                "Backup manifest failed validation",
            ));
        }

        let mut verified_files = 0;
        let mut total_bytes = 0;

        for file in &manifest.files {
            let file_path = dest_path.join(&file.path);
            if !file_path.exists() {
                return Err(CliError::new(
                    RS_3615,
                    format!("RS-3615: payload file '{}' missing from backup", file.path),
                    "Restore missing file or re-create backup",
                ));
            }
            let bytes = fs::read(&file_path).map_err(|e| {
                CliError::new(
                    RS_3615,
                    format!("RS-3615: failed to read payload file '{}': {e}", file.path),
                    "Verify file permissions",
                )
            })?;
            if bytes.len() as u64 != file.byte_len {
                return Err(CliError::new(
                    RS_3616,
                    format!(
                        "RS-3616: payload file '{}' length mismatch: expected {}, got {}",
                        file.path,
                        file.byte_len,
                        bytes.len()
                    ),
                    "Backup file is truncated or corrupted",
                ));
            }
            let digest = compute_file_sha256(&bytes);
            if digest != file.sha256 {
                return Err(CliError::new(
                    RS_3616,
                    format!(
                        "RS-3616: payload file '{}' checksum mismatch: expected {}, got {}",
                        file.path, file.sha256, digest
                    ),
                    "Backup file data corrupted or tampered",
                ));
            }
            verified_files += 1;
            total_bytes += file.byte_len;
        }

        Ok(BackupVerifyOutput {
            destination: destination.to_string(),
            file_count: manifest.files.len(),
            verified_files,
            total_bytes,
            manifest_checksum: manifest.checksum,
            status: "SUCCESS".to_string(),
            errors: Vec::new(),
        })
    }

    pub fn restore_backup(
        &self,
        source: &str,
        target: &Path,
        yes: bool,
    ) -> Result<RestoreOutcome, CliError> {
        if self.identity.role < Role::Admin {
            let event = AuditEvent::now(self.identity.user.clone(), "backup.restore", source)
                .with_detail("unauthorized role")
                .with_error_code("RS-2401");
            append_audit_file(target, &event);
            return Err(CliError::new(
                RS_2401,
                format!(
                    "permission denied: principal '{}' lacks required role {:?}",
                    self.identity.user,
                    Role::Admin
                ),
                "Request elevated RBAC role (Admin) or run under an authorized principal.",
            ));
        }

        let source_path = Path::new(source);
        let manifest_file = source_path.join(BACKUP_MANIFEST_FILENAME);
        if !manifest_file.exists() {
            return Err(CliError::new(
                RS_3615,
                format!("RS-3615: source backup manifest missing at '{source}'"),
                "Provide a valid backup path containing manifest.json",
            ));
        }

        let content = fs::read_to_string(&manifest_file).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: failed to read source manifest: {e}"),
                "Verify backup permissions and file integrity",
            )
        })?;

        let manifest: BackupManifest = serde_json::from_str(&content).map_err(|e| {
            CliError::new(
                RS_3615,
                format!("RS-3615: corrupted source manifest JSON: {e}"),
                "Restore from a known valid backup",
            )
        })?;

        if let Err((code, msg)) = manifest.validate() {
            return Err(CliError::new(
                code,
                msg,
                "Source backup manifest is invalid; refusing to restore",
            ));
        }

        let mut total_bytes = 0;
        for file in &manifest.files {
            let file_path = source_path.join(&file.path);
            if !file_path.exists() {
                return Err(CliError::new(
                    RS_3615,
                    format!(
                        "RS-3615: payload file '{}' missing from source backup",
                        file.path
                    ),
                    "Refusing to restore from incomplete backup",
                ));
            }
            let bytes = fs::read(&file_path).map_err(|e| {
                CliError::new(
                    RS_3615,
                    format!("RS-3615: cannot read payload file '{}': {e}", file.path),
                    "Check source permissions",
                )
            })?;
            if bytes.len() as u64 != file.byte_len {
                return Err(CliError::new(
                    RS_3616,
                    format!(
                        "RS-3616: payload file '{}' length mismatch: expected {}, got {}",
                        file.path,
                        file.byte_len,
                        bytes.len()
                    ),
                    "Source backup is corrupted",
                ));
            }
            let digest = compute_file_sha256(&bytes);
            if digest != file.sha256 {
                return Err(CliError::new(
                    RS_3616,
                    format!(
                        "RS-3616: payload file '{}' checksum mismatch: expected {}, got {}",
                        file.path, file.sha256, digest
                    ),
                    "Source backup file corrupted or tampered",
                ));
            }
            total_bytes += file.byte_len;
        }

        if target.exists() {
            if target.is_dir() {
                let mut entries = fs::read_dir(target).map_err(|e| {
                    CliError::new(
                        RS_3612,
                        format!("RS-3612: cannot access target destination: {e}"),
                        "Verify directory permissions",
                    )
                })?;
                if entries.next().is_some() && !yes {
                    return Err(CliError::new(
                        RS_0005,
                        format!(
                            "destination directory '{}' is non-empty; confirmation required to overwrite",
                            target.display()
                        ),
                        "Pass --yes for script execution or answer y at the prompt.",
                    ));
                }
            } else if !yes {
                return Err(CliError::new(
                    RS_0005,
                    format!(
                        "destination path '{}' already exists; confirmation required to overwrite",
                        target.display()
                    ),
                    "Pass --yes for script execution or answer y at the prompt.",
                ));
            }
        }

        fs::create_dir_all(target).map_err(|e| {
            CliError::new(
                RS_3612,
                format!("RS-3612: failed to create target directory: {e}"),
                "Check target permissions",
            )
        })?;

        for file in &manifest.files {
            let src_file = source_path.join(&file.path);
            let dst_file = target.join(&file.path);
            if let Some(parent) = dst_file.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    CliError::new(
                        RS_3612,
                        format!(
                            "RS-3612: failed to create parent dir '{}': {e}",
                            parent.display()
                        ),
                        "Check target permissions",
                    )
                })?;
            }
            fs::copy(&src_file, &dst_file).map_err(|e| {
                CliError::new(
                    RS_3612,
                    format!(
                        "RS-3612: failed to copy '{}' to '{}': {e}",
                        src_file.display(),
                        dst_file.display()
                    ),
                    "Check target disk space and permissions",
                )
            })?;
        }

        let _ = fs::copy(&manifest_file, target.join(BACKUP_MANIFEST_FILENAME));

        let event = AuditEvent::now(
            self.identity.user.clone(),
            "backup.restore",
            target.to_string_lossy().into_owned(),
        )
        .with_detail(format!(
            "checkpoint_id={} source={} files={} bytes={}",
            manifest.checkpoint_id,
            source,
            manifest.files.len(),
            total_bytes
        ));
        append_audit_file(target, &event);

        Ok(RestoreOutcome {
            checkpoint_id: manifest.checkpoint_id,
            source: source.to_string(),
            target: target.to_string_lossy().into_owned(),
            object_count: manifest.files.len() as u64,
            byte_count: total_bytes,
            restored_shards: 1,
            status: "SUCCESS".to_string(),
        })
    }
}

fn collect_files_recursive(
    dir: &Path,
    base: &Path,
    files: &mut Vec<(PathBuf, String)>,
) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, base, files)?;
        } else if path.is_file() {
            if let Ok(rel) = path.strip_prefix(base) {
                let rel_str = rel.to_string_lossy().into_owned();
                if rel_str != "audit.jsonl" && rel_str != BACKUP_MANIFEST_FILENAME {
                    files.push((path, rel_str));
                }
            }
        }
    }
    Ok(())
}

impl StorageAdminApi for StorageClient {
    fn export_checkpoint(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<CheckpointExportOutcome, CliError> {
        self.export_checkpoint(storage_path, destination)
    }
    fn restore_checkpoint(
        &self,
        audit_path: &Path,
        source: &str,
        target: &str,
    ) -> Result<RestoreOutcome, CliError> {
        self.restore_checkpoint(audit_path, source, target)
    }
    fn list_checkpoints(&self, storage_path: &Path) -> Result<Vec<CheckpointSummary>, CliError> {
        self.list_checkpoints(storage_path)
    }
    fn show_checkpoint(
        &self,
        storage_path: &Path,
        checkpoint_id: u64,
    ) -> Result<CheckpointAlignmentInfo, CliError> {
        self.show_checkpoint(storage_path, checkpoint_id)
    }
    fn generate_support_bundle(
        &self,
        storage_path: &Path,
        bundle_file: &Path,
    ) -> Result<SupportBundleInfo, CliError> {
        self.generate_support_bundle_with_diagnostics(
            storage_path,
            None,
            None,
            Some(bundle_file),
            &[],
        )
    }
    fn create_backup(
        &self,
        storage_path: &Path,
        destination: &str,
    ) -> Result<BackupCreateOutput, CliError> {
        self.create_backup(storage_path, destination)
    }
    fn inspect_backup(&self, destination: &str) -> Result<BackupInspectOutput, CliError> {
        self.inspect_backup(destination)
    }
    fn verify_backup(&self, destination: &str) -> Result<BackupVerifyOutput, CliError> {
        self.verify_backup(destination)
    }
    fn restore_backup(
        &self,
        source: &str,
        target: &Path,
        yes: bool,
    ) -> Result<RestoreOutcome, CliError> {
        self.restore_backup(source, target, yes)
    }
}

#[derive(Debug, Clone)]
pub struct RemoteStorageAdminClient {
    pub identity: ClientIdentity,
}

impl RemoteStorageAdminClient {
    pub fn with_identity(identity: ClientIdentity) -> Self {
        Self { identity }
    }

    fn unavailable<T>(&self, operation: &str) -> Result<T, CliError> {
        let _ = &self.identity;
        Err(remote_query_unavailable(operation))
    }
}

impl StorageAdminApi for RemoteStorageAdminClient {
    fn export_checkpoint(
        &self,
        _storage_path: &Path,
        _destination: &str,
    ) -> Result<CheckpointExportOutcome, CliError> {
        self.unavailable("checkpoint export")
    }

    fn restore_checkpoint(
        &self,
        _audit_path: &Path,
        _source: &str,
        _target: &str,
    ) -> Result<RestoreOutcome, CliError> {
        self.unavailable("checkpoint restore")
    }

    fn list_checkpoints(&self, _storage_path: &Path) -> Result<Vec<CheckpointSummary>, CliError> {
        self.unavailable("checkpoint listing")
    }

    fn show_checkpoint(
        &self,
        _storage_path: &Path,
        _checkpoint_id: u64,
    ) -> Result<CheckpointAlignmentInfo, CliError> {
        self.unavailable("checkpoint inspection")
    }

    fn generate_support_bundle(
        &self,
        _storage_path: &Path,
        _bundle_file: &Path,
    ) -> Result<SupportBundleInfo, CliError> {
        self.unavailable("support bundle generation")
    }

    fn create_backup(
        &self,
        _storage_path: &Path,
        _destination: &str,
    ) -> Result<crate::output::BackupCreateOutput, CliError> {
        self.unavailable("backup creation")
    }

    fn inspect_backup(
        &self,
        _destination: &str,
    ) -> Result<crate::output::BackupInspectOutput, CliError> {
        self.unavailable("backup inspection")
    }

    fn verify_backup(
        &self,
        _destination: &str,
    ) -> Result<crate::output::BackupVerifyOutput, CliError> {
        self.unavailable("backup verification")
    }

    fn restore_backup(
        &self,
        _source: &str,
        _target: &Path,
        _yes: bool,
    ) -> Result<crate::output::RestoreOutcome, CliError> {
        self.unavailable("backup restore")
    }
}
