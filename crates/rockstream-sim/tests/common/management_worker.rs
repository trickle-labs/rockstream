use std::net::SocketAddr;
use std::time::Duration;

use rockstream_cli::transport::ManagementCliClient;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub fn run_drain_cli(management_addr: SocketAddr, worker_id: u64) -> serde_json::Value {
    let binary = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/rockstream");
    let output = std::process::Command::new(binary)
        .args([
            "--identity-role",
            "admin",
            "--management",
            &management_addr.to_string(),
            "--output",
            "json",
            "cluster",
            "workers",
            "drain",
            &worker_id.to_string(),
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "drain CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("drain CLI should return operation JSON")
}

pub async fn wait_for_operation_state(
    management_addr: SocketAddr,
    operation_id: &str,
    expected_state: &str,
) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let operation_id = operation_id.to_owned();
            let current = tokio::task::spawn_blocking(move || {
                let mut client =
                    ManagementCliClient::connect(&management_addr.to_string()).unwrap();
                serde_json::to_value(client.get_operation(&operation_id).unwrap()).unwrap()
            })
            .await
            .unwrap();
            if current["state"] == expected_state {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("operation {operation_id} did not reach {expected_state}"))
}

pub struct WorkerPeer {
    reader: BufReader<TcpStream>,
    worker_id: WorkerId,
}

impl WorkerPeer {
    pub async fn register(addr: SocketAddr, worker_id: u64) -> Self {
        let mut reader = BufReader::new(TcpStream::connect(addr).await.unwrap());
        let registration = WorkerRegistration::new(
            WorkerId(worker_id),
            NodeRole::Worker,
            format!("host-{worker_id}:7000"),
            CapacityHeadroom::FULL,
        )
        .with_capabilities(WorkerCapabilities {
            shared_shard_store_id: Some([6; 32]),
            ..Default::default()
        });
        Self::send_on(&mut reader, &WorkerMessage::Register(registration)).await;
        let worker_id = WorkerId(worker_id);
        loop {
            if matches!(Self::next_from(&mut reader).await, ControlMessage::Registered { worker_id: id } if id == worker_id)
            {
                return Self { reader, worker_id };
            }
        }
    }

    pub async fn request_shard(&mut self, shard_id: u64) -> ShardLease {
        self.request_shard_result(shard_id)
            .await
            .unwrap_or_else(|message| {
                panic!(
                    "worker {} could not request shard {shard_id}: {message}",
                    self.worker_id
                )
            })
    }

    pub async fn request_shard_result(&mut self, shard_id: u64) -> Result<ShardLease, String> {
        self.send(&WorkerMessage::RequestShard {
            worker_id: self.worker_id,
            shard_id: ShardId(shard_id),
        })
        .await;
        loop {
            match self.next().await {
                ControlMessage::ShardAssigned { lease, .. }
                    if lease.shard_id == ShardId(shard_id) =>
                {
                    return Ok(lease);
                }
                ControlMessage::OperationFailed { message, .. } => {
                    return Err(message);
                }
                _ => {}
            }
        }
    }

    pub async fn wait_for_drain(&mut self) -> ControlMessage {
        loop {
            let message = self.next().await;
            if matches!(message, ControlMessage::BeginDrain(_)) {
                return message;
            }
        }
    }

    pub async fn acknowledge_drain(&mut self) {
        self.send(&WorkerMessage::DrainAck {
            worker_id: self.worker_id,
            shards_remaining: 0,
        })
        .await;
    }

    pub async fn acknowledge_recipient(&mut self, shard_id: u64) -> ShardLease {
        let (lease, operation_id) = self.wait_for_assignment(shard_id).await;
        self.acknowledge_assignment(&lease, operation_id).await;
        lease
    }

    pub async fn wait_for_assignment(&mut self, shard_id: u64) -> (ShardLease, String) {
        loop {
            match self.next().await {
                ControlMessage::ShardAssigned {
                    lease,
                    operation_id: Some(operation_id),
                } if lease.shard_id == ShardId(shard_id) => {
                    return (lease, operation_id);
                }
                _ => {}
            }
        }
    }

    pub async fn acknowledge_assignment(&mut self, lease: &ShardLease, operation_id: String) {
        self.send(&WorkerMessage::ShardTransferAck {
            operation_id,
            stage: "recipient".to_owned(),
            worker_id: self.worker_id,
            shard_id: lease.shard_id,
            lease_token: lease.lease_token,
            success: true,
            error: None,
        })
        .await;
    }

    pub async fn next(&mut self) -> ControlMessage {
        Self::next_from(&mut self.reader).await
    }

    pub async fn send(&mut self, message: &WorkerMessage) {
        Self::send_on(&mut self.reader, message).await;
    }

    async fn send_on(reader: &mut BufReader<TcpStream>, message: &WorkerMessage) {
        let line = serde_json::to_string(message).unwrap() + "\n";
        reader.get_mut().write_all(line.as_bytes()).await.unwrap();
    }

    async fn next_from(reader: &mut BufReader<TcpStream>) -> ControlMessage {
        let mut line = String::new();
        let bytes = tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line))
            .await
            .expect("timed out waiting for control message")
            .unwrap();
        assert_ne!(
            bytes, 0,
            "control connection closed before the expected message"
        );
        serde_json::from_str(line.trim()).unwrap()
    }
}
