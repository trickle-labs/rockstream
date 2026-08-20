//! Short-lived public data-plane client.

use std::io;

use rockstream_types::data_plane::{DeploymentRequest, SourceDeltaRequest, WorkloadSnapshot};
use rockstream_types::ids::WorkloadId;
use rockstream_types::topology::{ControlMessage, WorkerMessage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

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
}
