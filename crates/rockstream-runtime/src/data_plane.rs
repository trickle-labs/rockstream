//! Short-lived public data-plane client.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rockstream_types::data_plane::{DeploymentRequest, SourceDeltaRequest, WorkloadSnapshot};
use rockstream_types::ids::WorkloadId;
use rockstream_types::topology::{ControlMessage, WorkerMessage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

static NEXT_OPERATOR_STATS_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub struct DataPlaneClient {
    control_addr: String,
}

pub type ControlDataPlaneClient = DataPlaneClient;

impl DataPlaneClient {
    pub fn new(control_addr: impl Into<String>) -> Self {
        Self {
            control_addr: control_addr.into(),
        }
    }

    async fn request(&self, request: WorkerMessage) -> io::Result<ControlMessage> {
        let addr = self
            .control_addr
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let mut stream = TcpStream::connect(addr).await?;
        let wire = serde_json::to_string(&request)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            + "\n";
        stream.write_all(wire.as_bytes()).await?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).await?;
        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "control closed without a response",
            ));
        }
        serde_json::from_str(&line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
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
        let workload_id = request.workload_id;
        match self.request(WorkerMessage::DeployWorkload(request)).await? {
            ControlMessage::DeploymentReady {
                workload_id: actual,
                ..
            } if actual == workload_id => Ok(()),
            response => Err(Self::unexpected(response)),
        }
    }

    pub async fn submit_delta(&self, request: SourceDeltaRequest) -> io::Result<u64> {
        let request_id = request.request_id.clone();
        match self
            .request(WorkerMessage::SubmitSourceDelta(request))
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
        match self
            .request(WorkerMessage::ReadWorkload { workload_id })
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
        let request_id = format!(
            "{}-{}-operator-stats-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            NEXT_OPERATOR_STATS_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
        );
        let response = tokio::time::timeout(
            Duration::from_secs(7),
            self.request(WorkerMessage::ReadOperatorStats {
                request_id: request_id.clone(),
                workload_id,
                operator_ids,
            }),
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
