//! Short-lived public data-plane client.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rockstream_types::data_plane::{DeploymentRequest, SourceDeltaRequest, WorkloadSnapshot};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::WorkloadId;
use rockstream_types::topology::{ControlMessage, WorkerMessage};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

static NEXT_OPERATOR_STATS_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub struct DataPlaneClient {
    control_addr: String,
    tls_connector: Option<TlsConnector>,
    insecure_internal_dev: bool,
}

pub type ControlDataPlaneClient = DataPlaneClient;

impl DataPlaneClient {
    pub fn new(control_addr: impl Into<String>) -> Self {
        Self {
            control_addr: control_addr.into(),
            tls_connector: None,
            insecure_internal_dev: false,
        }
    }

    pub fn with_internal_tls(mut self, config: &InternalTlsConfig) -> Result<Self, String> {
        if !config.client_auth_required || !config.is_enabled() {
            return Err("RS-2405: data-plane client requires internal mTLS".to_owned());
        }
        config.validate()?;
        self.tls_connector = Some(crate::tls::build_client_tls_connector(config)?);
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
                "RS-2405: data-plane plaintext is restricted to explicitly enabled loopback development",
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
            ControlMessage::OperationFailed { code, message, .. } => {
                io::Error::other(format!("{code}: {message}"))
            }
            other => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected control response: {other:?}"),
            ),
        }
    }

    pub async fn deploy(&self, request: DeploymentRequest) -> io::Result<()> {
        self.deploy_request(request, None).await
    }

    pub async fn deploy_delegated(
        &self,
        request: DeploymentRequest,
        actor_assertion: &str,
    ) -> io::Result<()> {
        self.deploy_request(request, Some(actor_assertion)).await
    }

    async fn deploy_request(
        &self,
        request: DeploymentRequest,
        actor_assertion: Option<&str>,
    ) -> io::Result<()> {
        let workload_id = request.workload_id;
        match self
            .request_with_assertion(WorkerMessage::DeployWorkload(request), actor_assertion)
            .await?
        {
            ControlMessage::DeploymentReady {
                workload_id: actual,
                ..
            } if actual == workload_id => Ok(()),
            response => Err(Self::unexpected(response)),
        }
    }

    pub async fn submit_delta(&self, request: SourceDeltaRequest) -> io::Result<u64> {
        self.submit_delta_request(request, None).await
    }

    pub async fn submit_delta_delegated(
        &self,
        request: SourceDeltaRequest,
        actor_assertion: &str,
    ) -> io::Result<u64> {
        self.submit_delta_request(request, Some(actor_assertion))
            .await
    }

    async fn submit_delta_request(
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
            .request_with_assertion(WorkerMessage::ReadWorkload { workload_id }, actor_assertion)
            .await?
        {
            ControlMessage::WorkloadSnapshot { snapshot }
                if snapshot.deployment.workload_id == workload_id =>
            {
                Ok(snapshot)
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
