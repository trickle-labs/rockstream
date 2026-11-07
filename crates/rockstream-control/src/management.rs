#![allow(clippy::result_large_err)]

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use futures::Stream;
use rockstream_management_proto::{ensure_protocol_version, v1};
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, PermissionPreset, PolicyScope, SecurityAction,
    SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal,
    SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::diagnostic::redact_secrets;
use rockstream_types::ids::{PrincipalId, ShardId, WorkerId};
use rockstream_types::topology::WorkerInfo;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::server::TlsStream;
use tonic::transport::server::Connected;
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
use crate::support_artifact::{
    ArtifactExpiry, ArtifactStoreError, SupportArtifactStore, ARTIFACT_TTL, MAX_ARTIFACT_BYTES,
};
use crate::topology::TopologyCatalog;

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_MANAGEMENT_REQUESTS: usize = 64;

fn has_management_admin(principals: &[SecurityPrincipal]) -> bool {
    principals.iter().any(|principal| {
        principal.enabled
            && principal.preset_grants.iter().any(|grant| {
                grant.preset == PermissionPreset::Administrator
                    && grant.scope == PolicyScope::Cluster
            })
            && principal.login_methods.iter().any(|method| {
                matches!(
                    method,
                    rockstream_types::authorization::LoginMethod::Mtls { .. }
                )
            })
    })
}

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

#[derive(Serialize)]
struct SupportArtifactDigest {
    principal_id: u64,
    node_id: Option<u64>,
}

#[derive(Serialize)]
struct SupportArtifactDocument {
    format_version: u16,
    generated_at_ms: u64,
    scope: String,
    source_version: String,
    topology_revision: u64,
    active_operations: usize,
    retained_operations: usize,
    node: Option<SupportArtifactNode>,
}

#[derive(Serialize)]
struct SupportArtifactNode {
    node_id: String,
    role: String,
    healthy: bool,
    state: String,
}

struct SupportArtifactAudit<'a> {
    operation_id: AuditField<String>,
    correlation: AuditCorrelation,
    artifact_id: &'a str,
    scope: &'a PolicyScope,
    action: SecurityAuditClass,
    outcome: SecurityAuditOutcome,
    before_state: AuditField<String>,
    after_state: AuditField<String>,
}

impl<'a> SupportArtifactAudit<'a> {
    fn operation(
        artifact_id: &'a str,
        scope: &'a PolicyScope,
        action: SecurityAuditClass,
        outcome: SecurityAuditOutcome,
        before_state: AuditField<String>,
        after_state: AuditField<String>,
    ) -> Self {
        Self {
            operation_id: AuditField::Known(artifact_id.to_owned()),
            correlation: AuditCorrelation::Observed(artifact_id.to_owned()),
            artifact_id,
            scope,
            action,
            outcome,
            before_state,
            after_state,
        }
    }

    fn download_denial(artifact_id: &'a str, scope: &'a PolicyScope) -> Self {
        Self {
            operation_id: AuditField::NotApplicable,
            correlation: AuditCorrelation::Unknown,
            artifact_id,
            scope,
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    }
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
    security_catalog: Option<Arc<dyn CatalogStore>>,
    request_gate: Arc<Semaphore>,
    support_artifacts: SupportArtifactStore,
    support_generation_gate: Arc<Semaphore>,
    support_download_gate: Arc<Semaphore>,
}

#[derive(Clone)]
struct ManagementConnectionInfo {
    certificate_sha256: [u8; 32],
    principal_id: PrincipalId,
    closed: Arc<AtomicBool>,
}

impl ManagementConnectionInfo {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

struct ManagementTlsStream {
    stream: TlsStream<TcpStream>,
    info: ManagementConnectionInfo,
}

impl Connected for ManagementTlsStream {
    type ConnectInfo = ManagementConnectionInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for ManagementTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.info.is_closed() {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for ManagementTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.info.is_closed() {
            return Poll::Ready(Err(management_session_closed()));
        }
        Pin::new(&mut this.stream).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.info.is_closed() {
            return Poll::Ready(Err(management_session_closed()));
        }
        Pin::new(&mut this.stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.info.is_closed() {
            return Poll::Ready(Err(management_session_closed()));
        }
        Pin::new(&mut this.stream).poll_shutdown(context)
    }
}

fn management_session_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "management session closed")
}

fn required_audit_unavailable<T>(request: &Request<T>, message: &'static str) -> Status {
    if let Some(peer) = request.extensions().get::<ManagementConnectionInfo>() {
        peer.close();
    }
    Status::unavailable(message)
}

impl ManagementService {
    pub fn new(
        catalog: TopologyCatalog,
        shard_manager: ShardManager,
        operations: ManagementOperationStore,
        config: NodeConfig,
    ) -> Self {
        let support_artifacts = SupportArtifactStore::new(operations.object_store());
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
            security_catalog: None,
            request_gate: Arc::new(Semaphore::new(MAX_MANAGEMENT_REQUESTS)),
            support_artifacts,
            support_generation_gate: Arc::new(Semaphore::new(1)),
            support_download_gate: Arc::new(Semaphore::new(8)),
        }
    }

    pub fn with_security_catalog(mut self, catalog: Arc<dyn CatalogStore>) -> Self {
        self.security_catalog = Some(catalog);
        self
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

    pub async fn start(
        &self,
        bind_addr: &str,
        reloader: Arc<crate::tls::TlsCertificateReloader>,
    ) -> std::io::Result<ManagementServiceHandle> {
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
        let security_catalog = self.security_catalog.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "RS-2405: management API requires the durable security catalog",
            )
        })?;
        security_catalog
            .refresh()
            .await
            .map_err(|error| io::Error::other(format!("management security catalog: {error}")))?;
        let principals = security_catalog
            .list_security_principals()
            .await
            .map_err(|error| io::Error::other(format!("management security catalog: {error}")))?;
        if !has_management_admin(&principals) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RS-2405: management API requires a preprovisioned mTLS administrator",
            ));
        }

        match self.support_artifacts.initialize().await {
            Ok(()) => {
                self.support_artifacts
                    .cleanup_expired(Utc::now().timestamp_millis().max(1) as u64)
                    .await
                    .map_err(|error| {
                        io::Error::other(format!("management support artifact recovery: {error:?}"))
                    })?;
            }
            Err(ArtifactStoreError::Unsupported) => {
                self.support_artifacts.disable();
                tracing::warn!("management support artifacts disabled: object store lacks atomic conditional writes");
            }
            Err(error) => {
                return Err(io::Error::other(format!(
                    "management support artifact storage: {error:?}"
                )));
            }
        }

        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, _) = broadcast::channel(1);
        let (incoming_tx, incoming_rx) =
            mpsc::channel::<Result<ManagementTlsStream, io::Error>>(MAX_MANAGEMENT_REQUESTS);
        let mut accept_shutdown = shutdown_tx.subscribe();
        let accept_service = self.clone();
        let handshake_gate = Arc::new(Semaphore::new(MAX_MANAGEMENT_REQUESTS));
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = accept_shutdown.recv() => break,
                };
                let (stream, peer_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "management listener accept failed");
                        continue;
                    }
                };
                let permit = match handshake_gate.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let service = accept_service.clone();
                let reloader = reloader.clone();
                let incoming_tx = incoming_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let handshake = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        reloader.current_acceptor().accept(stream),
                    )
                    .await;
                    match handshake {
                        Ok(Ok(stream)) => match service.authenticate_tls_peer(&stream).await {
                            Ok(info) => {
                                let _ = incoming_tx
                                    .send(Ok(ManagementTlsStream { stream, info }))
                                    .await;
                            }
                            Err(error) => {
                                tracing::warn!(%error, %peer_addr, "management client identity rejected")
                            }
                        },
                        Ok(Err(_)) | Err(_) => {
                            if let Err(error) = service.record_authentication_failure().await {
                                tracing::error!(%error, %peer_addr, "management authentication audit failed");
                            }
                            tracing::warn!(%peer_addr, "management mTLS handshake rejected");
                        }
                    }
                });
            }
        });
        let incoming = futures::stream::unfold(incoming_rx, |mut receiver| async move {
            receiver.recv().await.map(|stream| (stream, receiver))
        });
        let mut shutdown_rx = shutdown_tx.subscribe();
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
        if self.support_artifacts.is_available() {
            let artifacts = self.support_artifacts.clone();
            let mut artifact_shutdown = shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = interval.tick() => if let Err(error) = artifacts.cleanup_expired(Utc::now().timestamp_millis().max(1) as u64).await {
                            tracing::warn!(?error, "management support artifact cleanup failed");
                        },
                        _ = artifact_shutdown.recv() => break,
                    }
                }
            });
        }
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

    async fn authenticate_tls_peer(
        &self,
        stream: &TlsStream<TcpStream>,
    ) -> Result<ManagementConnectionInfo, String> {
        let certificate = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .ok_or_else(|| "management mTLS peer certificate is missing".to_owned())?;
        let certificate_sha256: [u8; 32] = Sha256::digest(certificate.as_ref()).into();
        let catalog = self
            .security_catalog
            .as_ref()
            .ok_or_else(|| "management security catalog is unavailable".to_owned())?;
        catalog
            .refresh()
            .await
            .map_err(|error| format!("management security catalog refresh failed: {error}"))?;
        let identity = match crate::tls::extract_peer_identity(stream) {
            Ok(identity) => identity,
            Err(_) => {
                append_security_audit(
                    catalog.as_ref(),
                    authentication_audit(
                        AuditIdentity::Unverified(format!(
                            "certificate:{}",
                            hex::encode(certificate_sha256)
                        )),
                        false,
                        AuditField::Unknown,
                    ),
                )
                .await?;
                return Err("management client certificate identity is invalid".to_owned());
            }
        };
        if identity.role != rockstream_types::identity::NodeRole::Cli {
            append_security_audit(
                catalog.as_ref(),
                authentication_audit(
                    AuditIdentity::Unverified(identity.to_cn()),
                    false,
                    AuditField::Unknown,
                ),
            )
            .await?;
            return Err("management requires a CLI certificate identity".to_owned());
        }
        let principal = catalog
            .list_security_principals()
            .await
            .map_err(|error| format!("management security catalog read failed: {error}"))?
            .into_iter()
            .find(|principal| {
                principal.enabled
                    && principal.login_methods.iter().any(|method| {
                        matches!(method, rockstream_types::authorization::LoginMethod::Mtls { certificate_sha256: registered } if registered == &certificate_sha256)
                    })
            });
        let Some(principal) = principal else {
            let record = authentication_audit(
                AuditIdentity::Unverified(format!(
                    "certificate:{}",
                    hex::encode(certificate_sha256)
                )),
                false,
                AuditField::Unknown,
            );
            append_security_audit(catalog.as_ref(), record).await?;
            return Err(
                "management client certificate is not mapped to an enabled principal".into(),
            );
        };
        append_security_audit(
            catalog.as_ref(),
            authentication_audit(
                AuditIdentity::Verified(format!("principal:{}", principal.id.0)),
                true,
                AuditField::Known(principal.revision),
            ),
        )
        .await?;
        Ok(ManagementConnectionInfo {
            certificate_sha256,
            principal_id: principal.id,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn record_authentication_failure(&self) -> Result<(), String> {
        let catalog = self
            .security_catalog
            .as_ref()
            .ok_or_else(|| "management security catalog is unavailable".to_owned())?;
        append_security_audit(
            catalog.as_ref(),
            authentication_audit(AuditIdentity::Unknown, false, AuditField::Unknown),
        )
        .await
    }

    async fn authorize<T>(
        &self,
        request: &Request<T>,
        action: SecurityAction,
        scope: PolicyScope,
    ) -> Result<(PrincipalId, u64), Status> {
        self.authorize_actions(request, &[(action, scope)]).await
    }

    async fn authorize_actions<T>(
        &self,
        request: &Request<T>,
        requirements: &[(SecurityAction, PolicyScope)],
    ) -> Result<(PrincipalId, u64), Status> {
        let catalog = self
            .security_catalog
            .as_ref()
            .ok_or_else(|| Status::unavailable("management security catalog is unavailable"))?;
        let Some(peer) = request.extensions().get::<ManagementConnectionInfo>() else {
            self.record_authentication_failure().await.map_err(|_| {
                Status::unavailable("management authentication audit is unavailable")
            })?;
            return Err(Status::unauthenticated(
                "management mTLS identity is required",
            ));
        };
        catalog
            .refresh()
            .await
            .map_err(|_| Status::unavailable("management security policy is unavailable"))?;
        let principal = catalog
            .get_security_principal(peer.principal_id)
            .await
            .map_err(|_| Status::unavailable("management security policy is unavailable"))?;
        let principal = principal.filter(|principal| {
            principal.enabled
                && principal.login_methods.iter().any(|method| {
                    matches!(method, rockstream_types::authorization::LoginMethod::Mtls { certificate_sha256 } if certificate_sha256 == &peer.certificate_sha256)
                })
        });
        let Some(principal) = principal else {
            let record = authentication_audit(
                AuditIdentity::Unverified(format!("principal:{}", peer.principal_id.0)),
                false,
                AuditField::Unknown,
            );
            append_security_audit(catalog.as_ref(), record)
                .await
                .map_err(|_| {
                    required_audit_unavailable(
                        request,
                        "management authentication audit is unavailable",
                    )
                })?;
            return Err(Status::unauthenticated(
                "management identity is no longer enabled",
            ));
        };
        for (action, scope) in requirements {
            if !crate::authorization::allows(&principal, *action, scope) {
                let scope = serde_json::to_string(scope)
                    .map(|scope| redact_secrets(&scope))
                    .unwrap_or_else(|_| "unknown".to_owned());
                let record = SecurityAuditRecord {
                    format_version: SECURITY_AUDIT_FORMAT_VERSION,
                    event_id: format!("management-denial-{}", uuid::Uuid::new_v4()),
                    timestamp_ms: Utc::now().timestamp_millis().max(1) as u64,
                    actor: AuditIdentity::Verified(format!("principal:{}", principal.id.0)),
                    target: AuditField::Known(format!("{action:?}")),
                    scope: AuditField::Known(scope),
                    action: SecurityAuditClass::AuthorizationDenial,
                    outcome: SecurityAuditOutcome::Denied,
                    correlation: AuditCorrelation::Unknown,
                    operation_id: AuditField::NotApplicable,
                    policy_revision: AuditField::Known(principal.revision),
                    before_state: AuditField::NotApplicable,
                    after_state: AuditField::NotApplicable,
                };
                append_security_audit(catalog.as_ref(), record)
                    .await
                    .map_err(|_| {
                        required_audit_unavailable(
                            request,
                            "management authorization audit is unavailable",
                        )
                    })?;
                return Err(Status::permission_denied(
                    "management action is not authorized",
                ));
            }
        }
        Ok((principal.id, principal.revision))
    }

    async fn record_support_artifact_audit<T>(
        &self,
        request: &Request<T>,
        principal_id: PrincipalId,
        policy_revision: u64,
        audit: SupportArtifactAudit<'_>,
    ) -> Result<(), Status> {
        let catalog = self
            .security_catalog
            .as_ref()
            .ok_or_else(|| Status::unavailable("management security catalog is unavailable"))?;
        let scope = serde_json::to_string(audit.scope)
            .map_err(|_| Status::internal("support artifact scope serialization failed"))?;
        append_security_audit(
            catalog.as_ref(),
            SecurityAuditRecord {
                format_version: SECURITY_AUDIT_FORMAT_VERSION,
                event_id: format!("support-artifact-{}", uuid::Uuid::new_v4()),
                timestamp_ms: Utc::now().timestamp_millis().max(1) as u64,
                actor: AuditIdentity::Verified(format!("principal:{}", principal_id.0)),
                target: AuditField::Known(format!("support-artifact:{}", audit.artifact_id)),
                scope: AuditField::Known(scope),
                action: audit.action,
                outcome: audit.outcome,
                correlation: audit.correlation,
                operation_id: audit.operation_id,
                policy_revision: AuditField::Known(policy_revision),
                before_state: audit.before_state,
                after_state: audit.after_state,
            },
        )
        .await
        .map_err(|_| required_audit_unavailable(request, "support artifact audit is unavailable"))
    }

    async fn support_artifact_content(
        &self,
        node_id: Option<u64>,
        scope: String,
        generated_at_ms: u64,
    ) -> Result<Vec<u8>, Status> {
        let (active_operations, retained_operations) =
            self.operations.counts().await.map_err(store_error)?;
        let node = match node_id {
            Some(node_id) => {
                let worker = self
                    .catalog
                    .get(WorkerId(node_id))
                    .ok_or_else(|| Status::not_found("support artifact scope not found"))?;
                Some(SupportArtifactNode {
                    node_id: worker.worker_id.0.to_string(),
                    role: format!("{:?}", worker.role).to_lowercase(),
                    healthy: worker.healthy,
                    state: if worker.healthy {
                        worker.lifecycle.progress_phase().to_owned()
                    } else {
                        "unhealthy".to_owned()
                    },
                })
            }
            None => None,
        };
        let document = SupportArtifactDocument {
            format_version: 1,
            generated_at_ms,
            scope,
            source_version: format!("rockstream:{}", env!("CARGO_PKG_VERSION")),
            topology_revision: self.catalog.version(),
            active_operations,
            retained_operations,
            node,
        };
        let content = serde_json::to_vec(&document)
            .map_err(|_| Status::internal("support artifact serialization failed"))?;
        if content.len() > MAX_ARTIFACT_BYTES {
            return Err(Status::resource_exhausted(
                "support artifact exceeds the 10 MiB limit",
            ));
        }
        Ok(content)
    }

    async fn fail_support_artifact_generation(
        &self,
        request: &Request<v1::GenerateSupportArtifactRequest>,
        principal_id: PrincipalId,
        policy_revision: u64,
        artifact: (&str, &PolicyScope),
        operation: &OperationRecord,
        phase: &str,
    ) -> Result<(), Status> {
        self.record_support_artifact_audit(
            request,
            principal_id,
            policy_revision,
            SupportArtifactAudit::operation(
                artifact.0,
                artifact.1,
                SecurityAuditClass::AdminOperationCompletion,
                SecurityAuditOutcome::Failure,
                AuditField::Known("generating".to_owned()),
                AuditField::Known(phase.to_owned()),
            ),
        )
        .await?;
        self.operations
            .transition_if(
                operation.operation_id(),
                OperationStatus::Running,
                operation.phase(),
                crate::management_store::OperationUpdate {
                    status: OperationStatus::Failed,
                    updated_at_ms: Utc::now().timestamp_millis().max(1),
                    progress: Some(100),
                    phase: Some(phase.to_owned()),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .map_err(store_error)?;
        Ok(())
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
            crate::management_store::OperationKind::GenerateSupportArtifact => {
                "generate_support_artifact"
            }
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

    async fn expire_support_artifact_operation(&self, record: &OperationRecord) {
        let expiry_ms = record
            .started_at_ms()
            .saturating_add(ARTIFACT_TTL.as_millis() as i64);
        let now_ms = Utc::now().timestamp_millis().max(1);
        if now_ms < expiry_ms {
            return;
        }
        let Ok(_permit) = self.support_generation_gate.clone().try_acquire_owned() else {
            return;
        };
        let current = match self.operations.get(record.operation_id()).await {
            Ok(Some(current))
                if matches!(
                    current.status(),
                    OperationStatus::Pending | OperationStatus::Waiting
                ) =>
            {
                current
            }
            Ok(_) => return,
            Err(error) => {
                tracing::warn!(%error, operation_id = record.operation_id(), "support artifact recovery lookup failed");
                return;
            }
        };
        let Some(request) = current.request() else {
            tracing::error!(
                operation_id = current.operation_id(),
                "support artifact recovery request is missing"
            );
            return;
        };
        let Some(principal_id) = request
            .get("principal_id")
            .and_then(serde_json::Value::as_u64)
            .map(PrincipalId)
        else {
            tracing::error!(
                operation_id = current.operation_id(),
                "support artifact recovery actor is missing"
            );
            return;
        };
        let scope = match request.get("node_id").and_then(serde_json::Value::as_u64) {
            Some(node_id) => PolicyScope::Node {
                node_id: node_id.to_string(),
            },
            None => PolicyScope::Cluster,
        };
        let Some(catalog) = self.security_catalog.as_ref() else {
            tracing::error!(
                operation_id = current.operation_id(),
                "support artifact recovery audit catalog is unavailable"
            );
            return;
        };
        let audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("support-artifact-{}", uuid::Uuid::new_v4()),
            timestamp_ms: now_ms as u64,
            actor: AuditIdentity::Verified(format!("principal:{}", principal_id.0)),
            target: AuditField::Known(format!("support-artifact:{}", current.operation_id())),
            scope: serde_json::to_string(&scope)
                .map(AuditField::Known)
                .unwrap_or(AuditField::Unknown),
            action: SecurityAuditClass::AdminOperationCompletion,
            outcome: SecurityAuditOutcome::Failure,
            correlation: AuditCorrelation::Observed(current.operation_id().to_owned()),
            operation_id: AuditField::Known(current.operation_id().to_owned()),
            policy_revision: AuditField::Unknown,
            before_state: AuditField::Known(format!("{:?}", current.status())),
            after_state: AuditField::Known("expired_before_generation".to_owned()),
        };
        if let Err(error) = append_security_audit(catalog.as_ref(), audit).await {
            tracing::warn!(%error, operation_id = current.operation_id(), "support artifact expiry audit failed");
            return;
        }
        if let Err(error) = self
            .operations
            .transition_if(
                current.operation_id(),
                current.status(),
                current.phase(),
                crate::management_store::OperationUpdate {
                    status: OperationStatus::Failed,
                    updated_at_ms: now_ms,
                    progress: Some(100),
                    phase: Some("expired_before_generation".to_owned()),
                    error_code: None,
                    next_steps: vec![
                        "Use a new idempotency key to request another support artifact.".to_owned(),
                    ],
                },
            )
            .await
        {
            tracing::warn!(%error, operation_id = current.operation_id(), "support artifact expiry transition failed");
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
            crate::management_store::OperationKind::GenerateSupportArtifact => {
                self.expire_support_artifact_operation(record).await;
            }
        }
    }
}

fn authentication_audit(
    actor: AuditIdentity,
    success: bool,
    policy_revision: AuditField<u64>,
) -> SecurityAuditRecord {
    SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("management-authn-{}", uuid::Uuid::new_v4()),
        timestamp_ms: Utc::now().timestamp_millis().max(1) as u64,
        actor,
        target: AuditField::Known("management-api".to_owned()),
        scope: AuditField::NotApplicable,
        action: if success {
            SecurityAuditClass::AuthenticationSuccess
        } else {
            SecurityAuditClass::AuthenticationFailure
        },
        outcome: if success {
            SecurityAuditOutcome::Success
        } else {
            SecurityAuditOutcome::Failure
        },
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    }
}

pub(crate) async fn append_security_audit(
    store: &dyn CatalogStore,
    record: SecurityAuditRecord,
) -> Result<(), String> {
    let transaction_id = rand::random::<u64>().max(1);
    for attempt in 0..rockstream_storage::catalog::SECURITY_AUDIT_APPEND_ATTEMPTS {
        store
            .refresh()
            .await
            .map_err(|error| format!("management security audit refresh failed: {error}"))?;
        let revision = store
            .get_revision()
            .await
            .checked_add(1)
            .ok_or_else(|| "management security catalog revision exhausted".to_owned())?;
        let transaction = CatalogTxn::new(
            revision,
            transaction_id,
            vec![CatalogMutation::AppendSecurityAudit(record.clone())],
        )
        .map_err(|error| format!("management security audit transaction failed: {error}"))?;
        match store.commit_txn(transaction).await {
            Ok(_) => return Ok(()),
            Err(rockstream_storage::catalog::CatalogError::OptimisticConflict(_))
                if attempt + 1 < rockstream_storage::catalog::SECURITY_AUDIT_APPEND_ATTEMPTS =>
            {
                continue;
            }
            Err(error) => return Err(format!("management security audit commit failed: {error}")),
        }
    }
    Err("management security audit retry was exhausted".to_owned())
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
    type DownloadSupportArtifactStream =
        Pin<Box<dyn Stream<Item = Result<v1::DownloadSupportArtifactChunk, Status>> + Send>>;

    async fn get_cluster_status(
        &self,
        request: Request<v1::GetClusterStatusRequest>,
    ) -> Result<Response<v1::GetClusterStatusResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Cluster,
        )
        .await?;
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Cluster,
        )
        .await?;
        let request = request.into_inner();
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        let worker_id = request
            .get_ref()
            .node_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("node_id must be an unsigned integer"))?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Node {
                node_id: worker_id.to_string(),
            },
        )
        .await?;
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Cluster,
        )
        .await?;
        let request = request.into_inner();
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        let shard_id = request
            .get_ref()
            .shard_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("shard_id must be an unsigned integer"))?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Shard {
                shard_id: shard_id.to_string(),
            },
        )
        .await?;
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::OperationHistoryRead,
            PolicyScope::Cluster,
        )
        .await?;
        let request = request.into_inner();
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::OperationHistoryRead,
            PolicyScope::Operation {
                operation_id: request.get_ref().operation_id.clone(),
            },
        )
        .await?;
        let request = request.into_inner();
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::DiagnosticsRead,
            PolicyScope::Cluster,
        )
        .await?;
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::ClusterObserve,
            PolicyScope::Cluster,
        )
        .await?;
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
        capabilities.push("GenerateSupportArtifact".to_owned());
        capabilities.push("DownloadSupportArtifact".to_owned());
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::DiagnosticsRead,
            PolicyScope::Cluster,
        )
        .await?;
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        let worker_id = request
            .get_ref()
            .worker_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("worker_id must be an unsigned integer"))?;
        self.authorize(
            &request,
            SecurityAction::NodeDrain,
            PolicyScope::Node {
                node_id: worker_id.to_string(),
            },
        )
        .await?;
        let request = request.into_inner();
        let runtime = self
            .drain_runtime
            .as_ref()
            .ok_or_else(|| Status::unavailable("DrainWorker executor is not attached"))?;
        let accepted = self
            .operations
            .accept_idempotent(
                &request.idempotency_key,
                request.protocol_version as u16,
                &DrainWorkerDigest { worker_id },
                request.idempotency_key.clone(),
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        let shard_id = request
            .get_ref()
            .shard_id
            .parse::<u64>()
            .map_err(|_| Status::invalid_argument("shard_id must be an unsigned integer"))?;
        self.authorize(
            &request,
            SecurityAction::ShardMigrate,
            PolicyScope::Shard {
                shard_id: shard_id.to_string(),
            },
        )
        .await?;
        let request = request.into_inner();
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
                request.idempotency_key.clone(),
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        let destination = request.get_ref().destination.trim().to_owned();
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
        self.authorize(
            &request,
            SecurityAction::BackupCreate,
            PolicyScope::BackupDestination {
                destination: destination.clone(),
            },
        )
        .await?;
        let request = request.into_inner();
        let runtime = self
            .backup_runtime
            .as_ref()
            .ok_or_else(|| Status::unavailable("CreateBackup executor is not attached"))?;
        let accepted = self
            .operations
            .accept_idempotent(
                &request.idempotency_key,
                request.protocol_version as u16,
                &CreateBackupDigest {
                    destination: destination.clone(),
                },
                request.idempotency_key.clone(),
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
        Self::ensure_version(request.get_ref().protocol_version)?;
        self.authorize(
            &request,
            SecurityAction::OperationCancel,
            PolicyScope::Operation {
                operation_id: request.get_ref().operation_id.clone(),
            },
        )
        .await?;
        let request = request.into_inner();
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

    async fn generate_support_artifact(
        &self,
        request: Request<v1::GenerateSupportArtifactRequest>,
    ) -> Result<Response<v1::GenerateSupportArtifactResponse>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.get_ref().protocol_version)?;
        let key = request.get_ref().idempotency_key.clone();
        if uuid::Uuid::parse_str(&key)
            .map(|value| value.to_string())
            .ok()
            .as_deref()
            != Some(key.as_str())
        {
            return Err(Status::invalid_argument(
                "idempotency_key must be a canonical UUID",
            ));
        }
        let node_id = if request.get_ref().node_id.is_empty() {
            None
        } else {
            let value = request
                .get_ref()
                .node_id
                .parse::<u64>()
                .map_err(|_| Status::invalid_argument("node_id must be an unsigned integer"))?;
            if value.to_string() != request.get_ref().node_id {
                return Err(Status::invalid_argument(
                    "node_id must use canonical decimal form",
                ));
            }
            Some(value)
        };
        let scope = match node_id {
            Some(node_id) => PolicyScope::Node {
                node_id: node_id.to_string(),
            },
            None => PolicyScope::Cluster,
        };
        let scope_label = node_id.map_or_else(|| "cluster".to_owned(), |id| format!("node:{id}"));
        let (principal_id, policy_revision) = self
            .authorize(&request, SecurityAction::SupportGenerate, scope.clone())
            .await?;
        let _generation_permit = self
            .support_generation_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("support artifact generation is busy"))?;
        let accepted = self
            .operations
            .accept_idempotent(
                &key,
                request.get_ref().protocol_version as u16,
                &SupportArtifactDigest {
                    principal_id: principal_id.0,
                    node_id,
                },
                key.clone(),
                crate::management_store::OperationKind::GenerateSupportArtifact,
                Utc::now().timestamp_millis().max(1),
            )
            .await
            .map_err(store_error)?;
        let expires_at_ms = accepted
            .started_at_ms()
            .checked_add(ARTIFACT_TTL.as_millis() as i64)
            .filter(|value| *value > 0)
            .ok_or_else(|| Status::internal("support artifact expiry overflow"))?
            as u64;
        if accepted.status() == OperationStatus::Succeeded {
            let available = self
                .support_artifacts
                .metadata(&key, Utc::now().timestamp_millis().max(1) as u64)
                .await
                .is_ok_and(|metadata| {
                    metadata.owner == principal_id
                        && metadata.scope == scope
                        && metadata.expires_at_ms == expires_at_ms
                });
            if !available {
                self.record_support_artifact_audit(
                    &request,
                    principal_id,
                    policy_revision,
                    SupportArtifactAudit::operation(
                        &key,
                        &scope,
                        SecurityAuditClass::AdminOperationCompletion,
                        SecurityAuditOutcome::Failure,
                        AuditField::Known("available".to_owned()),
                        AuditField::Known("unavailable".to_owned()),
                    ),
                )
                .await?;
                return Err(Status::failed_precondition(
                    "support artifact expired or was cleared; use a new idempotency key",
                ));
            }
            return Ok(Response::new(v1::GenerateSupportArtifactResponse {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                opaque_id: key,
                expires_at_ms,
            }));
        }
        if matches!(
            accepted.status(),
            OperationStatus::Failed | OperationStatus::Cancelled
        ) {
            return Err(Status::failed_precondition(
                "support artifact operation is already terminal",
            ));
        }
        if Utc::now().timestamp_millis().max(1) as u64 >= expires_at_ms {
            self.record_support_artifact_audit(
                &request,
                principal_id,
                policy_revision,
                SupportArtifactAudit::operation(
                    &key,
                    &scope,
                    SecurityAuditClass::AdminOperationCompletion,
                    SecurityAuditOutcome::Failure,
                    AuditField::Known("generating".to_owned()),
                    AuditField::Known("expired_before_generation".to_owned()),
                ),
            )
            .await?;
            self.operations
                .transition_if(
                    accepted.operation_id(),
                    accepted.status(),
                    accepted.phase(),
                    crate::management_store::OperationUpdate {
                        status: OperationStatus::Failed,
                        updated_at_ms: Utc::now().timestamp_millis().max(1),
                        progress: Some(100),
                        phase: Some("expired_before_generation".to_owned()),
                        error_code: None,
                        next_steps: Vec::new(),
                    },
                )
                .await
                .map_err(store_error)?;
            return Err(Status::deadline_exceeded(
                "support artifact operation expired before generation",
            ));
        }
        let running = if accepted.status() == OperationStatus::Running {
            accepted
        } else {
            self.operations
                .transition_if(
                    accepted.operation_id(),
                    accepted.status(),
                    accepted.phase(),
                    crate::management_store::OperationUpdate {
                        status: OperationStatus::Running,
                        updated_at_ms: Utc::now().timestamp_millis().max(1),
                        progress: Some(0),
                        phase: Some("generating_support_artifact".to_owned()),
                        error_code: None,
                        next_steps: Vec::new(),
                    },
                )
                .await
                .map_err(store_error)?
        };
        self.record_support_artifact_audit(
            &request,
            principal_id,
            policy_revision,
            SupportArtifactAudit::operation(
                &key,
                &scope,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditOutcome::Success,
                AuditField::Known("pending".to_owned()),
                AuditField::Known("generating".to_owned()),
            ),
        )
        .await?;
        let now_ms = Utc::now().timestamp_millis().max(1) as u64;
        let existing = self.support_artifacts.metadata(&key, now_ms).await;
        match existing {
            Ok(metadata)
                if metadata.owner == principal_id
                    && metadata.scope == scope
                    && metadata.expires_at_ms == expires_at_ms => {}
            Ok(_) => {
                return Err(Status::permission_denied(
                    "support artifact identity is bound to another actor or scope",
                ));
            }
            Err(ArtifactStoreError::NotFound) => {
                let generated_at_ms = running.started_at_ms().max(1) as u64;
                let content = match self
                    .support_artifact_content(node_id, scope_label, generated_at_ms)
                    .await
                {
                    Ok(content) => content,
                    Err(status) => {
                        self.fail_support_artifact_generation(
                            &request,
                            principal_id,
                            policy_revision,
                            (&key, &scope),
                            &running,
                            "generation_failed",
                        )
                        .await?;
                        return Err(status);
                    }
                };
                if let Err(error) = self
                    .support_artifacts
                    .insert(
                        key.clone(),
                        principal_id,
                        scope.clone(),
                        Arc::from(content),
                        ArtifactExpiry {
                            unix_ms: expires_at_ms,
                        },
                        Utc::now().timestamp_millis().max(1) as u64,
                    )
                    .await
                {
                    let status = match error {
                        ArtifactStoreError::TooLarge | ArtifactStoreError::StorageFull => {
                            Status::resource_exhausted("support artifact storage budget is full")
                        }
                        ArtifactStoreError::Exists => {
                            Status::already_exists("support artifact identity already exists")
                        }
                        ArtifactStoreError::NotFound | ArtifactStoreError::WrongOwner => {
                            Status::internal("support artifact store rejected the new artifact")
                        }
                        ArtifactStoreError::StorageUnavailable => {
                            Status::unavailable("support artifact storage is unavailable")
                        }
                        ArtifactStoreError::Unsupported => Status::unavailable(
                            "support artifact storage requires atomic conditional writes",
                        ),
                    };
                    self.fail_support_artifact_generation(
                        &request,
                        principal_id,
                        policy_revision,
                        (&key, &scope),
                        &running,
                        "generation_failed",
                    )
                    .await?;
                    return Err(status);
                }
            }
            Err(error) => {
                self.fail_support_artifact_generation(
                    &request,
                    principal_id,
                    policy_revision,
                    (&key, &scope),
                    &running,
                    "storage_unavailable",
                )
                .await?;
                return Err(match error {
                    ArtifactStoreError::Unsupported => Status::unavailable(
                        "support artifact storage requires atomic conditional writes",
                    ),
                    _ => Status::unavailable("support artifact storage is unavailable"),
                });
            }
        }
        if let Err(status) = self
            .record_support_artifact_audit(
                &request,
                principal_id,
                policy_revision,
                SupportArtifactAudit::operation(
                    &key,
                    &scope,
                    SecurityAuditClass::AdminOperationCompletion,
                    SecurityAuditOutcome::Success,
                    AuditField::Known("generating".to_owned()),
                    AuditField::Known(format!("available_until_ms:{expires_at_ms}")),
                ),
            )
            .await
        {
            if let Err(error) = self.support_artifacts.remove(&key, principal_id).await {
                tracing::error!(?error, artifact_id = %key, "failed to remove unaudited support artifact");
                return Err(Status::unavailable(
                    "support artifact cleanup is unavailable",
                ));
            }
            return Err(status);
        }
        self.operations
            .transition_if(
                running.operation_id(),
                OperationStatus::Running,
                running.phase(),
                crate::management_store::OperationUpdate {
                    status: OperationStatus::Succeeded,
                    updated_at_ms: Utc::now().timestamp_millis().max(1),
                    progress: Some(100),
                    phase: Some(format!("available_until_ms:{expires_at_ms}")),
                    error_code: None,
                    next_steps: Vec::new(),
                },
            )
            .await
            .map_err(store_error)?;
        Ok(Response::new(v1::GenerateSupportArtifactResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: key,
            expires_at_ms,
        }))
    }

    async fn download_support_artifact(
        &self,
        request: Request<v1::DownloadSupportArtifactRequest>,
    ) -> Result<Response<Self::DownloadSupportArtifactStream>, Status> {
        let _permit = self.acquire_request()?;
        Self::ensure_version(request.get_ref().protocol_version)?;
        let artifact_id = request.get_ref().opaque_id.clone();
        if uuid::Uuid::parse_str(&artifact_id)
            .map(|value| value.to_string())
            .ok()
            .as_deref()
            != Some(artifact_id.as_str())
        {
            return Err(Status::not_found("support artifact not found or expired"));
        }
        let artifact_scope = PolicyScope::Artifact {
            opaque_id: artifact_id.clone(),
        };
        let (principal_id, policy_revision) = self
            .authorize(
                &request,
                SecurityAction::SupportDownload,
                artifact_scope.clone(),
            )
            .await?;
        let metadata = match self
            .support_artifacts
            .metadata(&artifact_id, Utc::now().timestamp_millis().max(1) as u64)
            .await
        {
            Ok(metadata) if metadata.owner == principal_id => metadata,
            Ok(_) | Err(ArtifactStoreError::NotFound) => {
                self.record_support_artifact_audit(
                    &request,
                    principal_id,
                    policy_revision,
                    SupportArtifactAudit::download_denial(&artifact_id, &artifact_scope),
                )
                .await?;
                return Err(Status::not_found("support artifact not found or expired"));
            }
            Err(_) => return Err(Status::unavailable("support artifact store is unavailable")),
        };
        let (downloaded_principal, policy_revision) = self
            .authorize_actions(
                &request,
                &[
                    (SecurityAction::SupportDownload, artifact_scope.clone()),
                    (SecurityAction::SupportGenerate, metadata.scope.clone()),
                ],
            )
            .await?;
        if downloaded_principal != principal_id || metadata.expires_at_ms == 0 {
            return Err(Status::not_found("support artifact not found or expired"));
        }
        let (content, expires_at_ms, expires_at) = match self
            .support_artifacts
            .download(
                &artifact_id,
                principal_id,
                Utc::now().timestamp_millis().max(1) as u64,
            )
            .await
        {
            Ok((content, expires_at_ms, expires_at)) if expires_at_ms == metadata.expires_at_ms => {
                (content, expires_at_ms, expires_at)
            }
            Ok(_) => return Err(Status::internal("support artifact expiry metadata changed")),
            Err(ArtifactStoreError::WrongOwner | ArtifactStoreError::NotFound) => {
                self.record_support_artifact_audit(
                    &request,
                    principal_id,
                    policy_revision,
                    SupportArtifactAudit::download_denial(&artifact_id, &artifact_scope),
                )
                .await?;
                return Err(Status::not_found("support artifact not found or expired"));
            }
            Err(_) => return Err(Status::unavailable("support artifact store is unavailable")),
        };
        let download_permit = self
            .support_download_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("support artifact download limit reached"))?;
        self.record_support_artifact_audit(
            &request,
            principal_id,
            policy_revision,
            SupportArtifactAudit {
                operation_id: AuditField::NotApplicable,
                correlation: AuditCorrelation::Unknown,
                artifact_id: &artifact_id,
                scope: &artifact_scope,
                action: SecurityAuditClass::AdminOperationStart,
                outcome: SecurityAuditOutcome::Success,
                before_state: AuditField::NotApplicable,
                after_state: AuditField::NotApplicable,
            },
        )
        .await?;
        let peer = request
            .extensions()
            .get::<ManagementConnectionInfo>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("management mTLS identity is required"))?;
        let stream_service = self.clone();
        let stream_artifact_scope = artifact_scope.clone();
        let stream_source_scope = metadata.scope.clone();
        let stream = futures::stream::unfold(
            (
                content,
                Some(0usize),
                download_permit,
                expires_at_ms,
                expires_at,
            ),
            move |(content, offset, permit, expires_at_ms, expires_at)| {
                let service = stream_service.clone();
                let peer = peer.clone();
                let artifact_scope = stream_artifact_scope.clone();
                let source_scope = stream_source_scope.clone();
                async move {
                    let offset = offset?;
                    if Instant::now() >= expires_at {
                        return Some((
                            Err(Status::not_found("support artifact not found or expired")),
                            (content, None, permit, expires_at_ms, expires_at),
                        ));
                    }
                    let mut authorization_request = Request::new(());
                    authorization_request.extensions_mut().insert(peer);
                    let authorization = service
                        .authorize_actions(
                            &authorization_request,
                            &[
                                (SecurityAction::SupportDownload, artifact_scope),
                                (SecurityAction::SupportGenerate, source_scope),
                            ],
                        )
                        .await
                        .and_then(|(authorized_principal, _)| {
                            if authorized_principal != principal_id {
                                Err(Status::unauthenticated(
                                    "management identity is no longer enabled",
                                ))
                            } else {
                                Ok(())
                            }
                        });
                    if let Err(status) = authorization {
                        return Some((
                            Err(status),
                            (content, None, permit, expires_at_ms, expires_at),
                        ));
                    }
                    if offset >= content.len() {
                        return None;
                    }
                    let end = offset.saturating_add(64 * 1024).min(content.len());
                    let chunk = v1::DownloadSupportArtifactChunk {
                        content: content[offset..end].to_vec(),
                        offset: offset as u64,
                        final_chunk: end == content.len(),
                        expires_at_ms,
                    };
                    Some((
                        Ok(chunk),
                        (content, Some(end), permit, expires_at_ms, expires_at),
                    ))
                }
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use rockstream_types::authorization::{
        LoginMethod, ScopedPresetGrant, SECURITY_PRINCIPAL_FORMAT_VERSION,
    };
    use rockstream_types::topology::WorkerMessage;

    #[test]
    fn management_admin_requires_mtls_and_admin_grant_on_same_principal() {
        let principal = |id, login_method, preset| SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(id),
            display_name: format!("principal-{id}"),
            enabled: true,
            revision: 1,
            login_methods: vec![login_method],
            preset_grants: vec![ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            }],
            scoped_grants: Vec::new(),
        };
        let split = [
            principal(
                1,
                LoginMethod::Mtls {
                    certificate_sha256: [1; 32],
                },
                PermissionPreset::Operator,
            ),
            principal(
                2,
                LoginMethod::Oidc {
                    issuer: "https://issuer.example".to_owned(),
                    subject: "admin".to_owned(),
                },
                PermissionPreset::Administrator,
            ),
        ];
        let mtls_admin = [principal(
            3,
            LoginMethod::Mtls {
                certificate_sha256: [3; 32],
            },
            PermissionPreset::Administrator,
        )];

        assert!(!has_management_admin(&split));
        assert!(has_management_admin(&mtls_admin));
    }

    #[test]
    fn unavailable_required_audit_closes_authenticated_session() {
        let peer = ManagementConnectionInfo {
            certificate_sha256: [7; 32],
            principal_id: PrincipalId(1),
            closed: Arc::new(AtomicBool::new(false)),
        };
        let mut request = Request::new(());
        request.extensions_mut().insert(peer.clone());

        let status = required_audit_unavailable(&request, "audit unavailable");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "audit unavailable");
        assert!(peer.is_closed());
    }

    async fn test_security_catalog() -> Arc<dyn CatalogStore> {
        let catalog = Arc::new(rockstream_storage::catalog::DurableCatalogStore::new(
            Arc::new(InMemory::new()),
            "management-test",
        ));
        let principal = rockstream_types::authorization::SecurityPrincipal {
            format_version: rockstream_types::authorization::SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(1),
            display_name: "management-test-operator".to_owned(),
            enabled: true,
            revision: 1,
            login_methods: vec![rockstream_types::authorization::LoginMethod::Mtls {
                certificate_sha256: [7; 32],
            }],
            preset_grants: vec![
                rockstream_types::authorization::ScopedPresetGrant {
                    preset: PermissionPreset::Administrator,
                    scope: PolicyScope::Cluster,
                },
                rockstream_types::authorization::ScopedPresetGrant {
                    preset: PermissionPreset::Operator,
                    scope: PolicyScope::Cluster,
                },
            ],
            scoped_grants: Vec::new(),
        };
        let audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-test-bootstrap".to_owned(),
            timestamp_ms: Utc::now().timestamp_millis().max(1) as u64,
            actor: AuditIdentity::Unknown,
            target: AuditField::Known("principal:1".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known("1".to_owned()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(format!(
                "revision={};policy={}",
                principal.revision,
                serde_json::to_string(&(
                    principal.enabled,
                    &principal.preset_grants,
                    &principal.scoped_grants
                ))
                .unwrap()
            )),
        };
        catalog
            .commit_txn(
                CatalogTxn::new(
                    1,
                    1,
                    vec![CatalogMutation::PutSecurityPrincipal {
                        principal,
                        expected_revision: None,
                        grantor_revision: None,
                        audit,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        catalog
    }

    async fn cluster_status(
        service: &ManagementService,
    ) -> Result<v1::GetClusterStatusResponse, Status> {
        let mut request = Request::new(v1::GetClusterStatusRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        });
        request.extensions_mut().insert(ManagementConnectionInfo {
            certificate_sha256: [7; 32],
            principal_id: PrincipalId(1),
            closed: Arc::new(AtomicBool::new(false)),
        });
        <ManagementService as v1::management_service_server::ManagementService>::get_cluster_status(
            service, request,
        )
        .await
        .map(Response::into_inner)
    }

    fn authenticated_request<T>(request: T) -> Request<T> {
        let mut request = Request::new(request);
        request.extensions_mut().insert(ManagementConnectionInfo {
            certificate_sha256: [7; 32],
            principal_id: PrincipalId(1),
            closed: Arc::new(AtomicBool::new(false)),
        });
        request
    }

    #[tokio::test]
    async fn support_artifact_api_retries_and_streams_exact_sanitized_bytes() {
        let security_catalog = test_security_catalog().await;
        let operations = ManagementOperationStore::new(Arc::new(InMemory::new()));
        let service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            operations.clone(),
            NodeConfig::default(),
        )
        .with_security_catalog(security_catalog.clone());
        let key = uuid::Uuid::new_v4();
        let request = || {
            authenticated_request(v1::GenerateSupportArtifactRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                node_id: String::new(),
                idempotency_key: key.to_string(),
            })
        };
        let first = <ManagementService as v1::management_service_server::ManagementService>::generate_support_artifact(
            &service,
            request(),
        )
        .await
        .unwrap()
        .into_inner();
        let retry = <ManagementService as v1::management_service_server::ManagementService>::generate_support_artifact(
            &service,
            request(),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(first, retry);
        assert_eq!(first.opaque_id, key.to_string());

        let operation = operations.get(&key.to_string()).await.unwrap().unwrap();
        let generated_at_ms = operation.started_at_ms() as u64;
        assert_eq!(first.expires_at_ms, generated_at_ms + 600_000);
        let expected = format!(
            "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":0,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
            env!("CARGO_PKG_VERSION")
        );
        let audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        assert_eq!(
            audit.iter().map(|record| record.action).collect::<Vec<_>>(),
            vec![
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
            ]
        );

        let response = <ManagementService as v1::management_service_server::ManagementService>::download_support_artifact(
            &service,
            authenticated_request(v1::DownloadSupportArtifactRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                opaque_id: first.opaque_id.clone(),
            }),
        )
        .await
        .unwrap();
        let mut chunks = response.into_inner();
        let chunk = chunks.next().await.unwrap().unwrap();
        assert_eq!(chunk.offset, 0);
        assert!(chunk.final_chunk);
        assert_eq!(chunk.expires_at_ms, first.expires_at_ms);
        assert_eq!(String::from_utf8(chunk.content).unwrap(), expected);
        assert!(chunks.next().await.is_none());

        let audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        assert_eq!(
            audit.iter().map(|record| record.action).collect::<Vec<_>>(),
            vec![
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
                SecurityAuditClass::AdminOperationStart,
            ]
        );

        let restarted_service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            operations.clone(),
            NodeConfig::default(),
        )
        .with_security_catalog(security_catalog.clone());
        let retry_after_restart = <ManagementService as v1::management_service_server::ManagementService>::generate_support_artifact(
            &restarted_service,
            request(),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(first, retry_after_restart);
        let audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        assert_eq!(
            audit.iter().map(|record| record.action).collect::<Vec<_>>(),
            vec![
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
                SecurityAuditClass::AdminOperationStart,
            ]
        );
    }

    #[tokio::test]
    async fn support_artifact_download_rechecks_policy_between_chunks() {
        let security_catalog = test_security_catalog().await;
        let service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            ManagementOperationStore::new(Arc::new(InMemory::new())),
            NodeConfig::default(),
        )
        .with_security_catalog(security_catalog.clone());
        let artifact_id = uuid::Uuid::new_v4().to_string();
        let scope = PolicyScope::Cluster;
        let now_ms = Utc::now().timestamp_millis().max(1) as u64;
        let expires_at_ms = now_ms + ARTIFACT_TTL.as_millis() as u64;
        let content = vec![0x5a; 128 * 1024];
        service
            .support_artifacts
            .insert(
                artifact_id.clone(),
                PrincipalId(1),
                scope.clone(),
                Arc::from(content.clone()),
                ArtifactExpiry {
                    unix_ms: expires_at_ms,
                },
                now_ms,
            )
            .await
            .unwrap();

        let mut stream = <ManagementService as v1::management_service_server::ManagementService>::download_support_artifact(
            &service,
            authenticated_request(v1::DownloadSupportArtifactRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                opaque_id: artifact_id.clone(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let first_chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(first_chunk.offset, 0);
        assert_eq!(first_chunk.expires_at_ms, expires_at_ms);
        assert!(!first_chunk.final_chunk);
        assert_eq!(first_chunk.content.as_slice(), &content[..64 * 1024]);

        let mut principal = security_catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap()
            .unwrap();
        let prior_revision = principal.revision;
        let prior_policy = serde_json::to_string(&(
            principal.enabled,
            &principal.preset_grants,
            &principal.scoped_grants,
        ))
        .unwrap();
        principal
            .preset_grants
            .retain(|grant| grant.preset != PermissionPreset::Operator);
        principal.revision += 1;
        let next_revision = security_catalog.get_revision().await + 1;
        let changed_at_ms = Utc::now().timestamp_millis().max(1) as u64;
        let grant_audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("support-artifact-grant-revocation-{artifact_id}"),
            timestamp_ms: changed_at_ms,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("principal:1".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known(next_revision.to_string()),
            policy_revision: AuditField::Known(principal.revision),
            before_state: AuditField::Known(format!(
                "revision={prior_revision};policy={prior_policy}"
            )),
            after_state: AuditField::Known(format!(
                "revision={};policy={}",
                principal.revision,
                serde_json::to_string(&(
                    principal.enabled,
                    &principal.preset_grants,
                    &principal.scoped_grants
                ))
                .unwrap()
            )),
        };
        security_catalog
            .commit_txn(
                CatalogTxn::new(
                    next_revision,
                    next_revision,
                    vec![CatalogMutation::PutSecurityPrincipal {
                        principal,
                        expected_revision: Some(prior_revision),
                        grantor_revision: Some((PrincipalId(1), prior_revision)),
                        audit: grant_audit,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let denial = stream.next().await.unwrap().unwrap_err();
        assert_eq!(denial.code(), tonic::Code::PermissionDenied);
        assert_eq!(denial.message(), "management action is not authorized");
        assert!(stream.next().await.is_none());

        let audits = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        assert_eq!(
            audits
                .iter()
                .map(|record| record.action)
                .collect::<Vec<_>>(),
            vec![
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AuthorizationDenial,
            ]
        );
        let denied = audits.last().unwrap();
        assert_eq!(
            denied.actor,
            AuditIdentity::Verified("principal:1".to_owned())
        );
        assert_eq!(
            denied.target,
            AuditField::Known("SupportDownload".to_owned())
        );
        assert_eq!(
            denied.scope,
            AuditField::Known(
                serde_json::to_string(&PolicyScope::Artifact {
                    opaque_id: artifact_id,
                })
                .unwrap()
            )
        );
        assert_eq!(denied.outcome, SecurityAuditOutcome::Denied);
        assert_eq!(
            denied.policy_revision,
            AuditField::Known(prior_revision + 1)
        );
        assert_eq!(denied.correlation, AuditCorrelation::Unknown);
        assert_eq!(denied.operation_id, AuditField::NotApplicable);
        assert_eq!(denied.before_state, AuditField::NotApplicable);
        assert_eq!(denied.after_state, AuditField::NotApplicable);
    }

    #[tokio::test]
    async fn abandoned_support_artifact_operation_expires_with_durable_audit() {
        let security_catalog = test_security_catalog().await;
        let operations = ManagementOperationStore::new(Arc::new(InMemory::new()));
        let key = uuid::Uuid::new_v4().to_string();
        let now_ms = Utc::now().timestamp_millis().max(1);
        let accepted = operations
            .accept_idempotent(
                &key,
                rockstream_management_proto::PROTOCOL_VERSION as u16,
                &SupportArtifactDigest {
                    principal_id: 1,
                    node_id: None,
                },
                key.clone(),
                crate::management_store::OperationKind::GenerateSupportArtifact,
                now_ms - ARTIFACT_TTL.as_millis() as i64 - 1,
            )
            .await
            .unwrap();
        let service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            operations.clone(),
            NodeConfig::default(),
        )
        .with_security_catalog(security_catalog.clone());

        service.expire_support_artifact_operation(&accepted).await;

        let expired = operations.get(&key).await.unwrap().unwrap();
        assert_eq!(expired.status(), OperationStatus::Failed);
        assert_eq!(expired.phase(), Some("expired_before_generation"));
        let audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        assert_eq!(
            audit.iter().map(|record| record.action).collect::<Vec<_>>(),
            vec![
                SecurityAuditClass::GrantChange,
                SecurityAuditClass::AdminOperationCompletion,
            ]
        );
        let expiry_audit = audit.last().unwrap();
        assert_eq!(
            expiry_audit.actor,
            AuditIdentity::Verified("principal:1".to_owned())
        );
        assert_eq!(
            expiry_audit.correlation,
            AuditCorrelation::Observed(key.clone())
        );
        assert_eq!(expiry_audit.policy_revision, AuditField::Unknown);
        assert_eq!(expiry_audit.outcome, SecurityAuditOutcome::Failure);
    }

    #[tokio::test]
    async fn cluster_status_reports_request_and_ack_waiter_fill_and_recovery() {
        let mut service = ManagementService::new(
            TopologyCatalog::new(),
            ShardManager::new(),
            ManagementOperationStore::new(Arc::new(InMemory::new())),
            NodeConfig::default(),
        )
        .with_security_catalog(test_security_catalog().await);
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
