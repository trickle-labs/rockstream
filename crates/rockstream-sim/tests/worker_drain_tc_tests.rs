use std::net::SocketAddr;
use std::time::{Duration, Instant};

use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};

const IMAGE_NAME: &str = "rockstream-tc-test";
const IMAGE_TAG: &str = "latest";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

fn image_available() -> bool {
    std::process::Command::new("docker")
        .args(["image", "inspect", &format!("{IMAGE_NAME}:{IMAGE_TAG}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct TcCluster {
    control: ContainerAsync<GenericImage>,
    donor: ContainerAsync<GenericImage>,
    recipient: ContainerAsync<GenericImage>,
    gateway: ContainerAsync<GenericImage>,
    _control_name: String,
    control_addr: SocketAddr,
    network: String,
    _shared_dir: tempfile::TempDir,
}

impl TcCluster {
    async fn boot(test_id: &str) -> Self {
        let network = format!("rs-m46-drain-net-{test_id}");
        let shared_dir = tempfile::tempdir().unwrap();
        let control_name = format!("rs-m46-drain-control-{test_id}");
        let control = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::message_on_stdout("control service listening"))
            .with_exposed_port(ContainerPort::Tcp(8000))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=control".to_string(),
                "--daemon".to_string(),
                "--control-bind=0.0.0.0:8000".to_string(),
                "--control-shared-storage=/shared".to_string(),
            ])
            .with_container_name(control_name.clone())
            .with_network(network.clone())
            .with_mount(Mount::bind_mount(
                shared_dir.path().to_str().unwrap().to_string(),
                "/shared".to_string(),
            ))
            .start()
            .await
            .unwrap();
        let host_port = control.get_host_port_ipv4(8000).await.unwrap();
        let control_addr = format!("127.0.0.1:{host_port}").parse().unwrap();
        let donor = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=frontier".to_string(),
            ])
            .with_env_var("ROCKSTREAM_E2E_SLEEP_MS", "300000")
            .with_container_name(format!("rs-m46-drain-donor-{test_id}"))
            .with_network(network.clone())
            .start()
            .await
            .unwrap();
        let recipient = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=frontier".to_string(),
            ])
            .with_env_var("ROCKSTREAM_E2E_SLEEP_MS", "300000")
            .with_container_name(format!("rs-m46-drain-recipient-{test_id}"))
            .with_network(network.clone())
            .start()
            .await
            .unwrap();
        let gateway = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::message_on_stdout("PostgreSQL wire gateway ready"))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=gateway".to_string(),
                "--listen=0.0.0.0:5432".to_string(),
            ])
            .with_container_name(format!("rs-m46-drain-gateway-{test_id}"))
            .with_network(network.clone())
            .start()
            .await
            .unwrap();
        Self {
            control,
            donor,
            recipient,
            gateway,
            _control_name: control_name,
            control_addr,
            network,
            _shared_dir: shared_dir,
        }
    }

    async fn cleanup(self) {
        let _ = self.control.rm().await;
        let _ = self.donor.rm().await;
        let _ = self.recipient.rm().await;
        let _ = self.gateway.rm().await;
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.network])
            .status();
    }
}

async fn send(addr: SocketAddr, msg: &WorkerMessage) -> Vec<ControlMessage> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let line = serde_json::to_string(msg).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut reader = BufReader::new(stream);
    let mut out = Vec::new();
    loop {
        let mut line = String::new();
        let Ok(read) =
            tokio::time::timeout(Duration::from_millis(50), reader.read_line(&mut line)).await
        else {
            break;
        };
        let Ok(read) = read else { break };
        if read == 0 || line.trim().is_empty() {
            break;
        }
        out.push(serde_json::from_str(line.trim()).unwrap());
    }
    out
}

async fn request_shard(addr: SocketAddr, worker_id: u64, shard_id: u64) -> Option<ShardLease> {
    let replies = send(
        addr,
        &WorkerMessage::RequestShard {
            worker_id: WorkerId(worker_id),
            shard_id: ShardId(shard_id),
        },
    )
    .await;
    replies.into_iter().find_map(|reply| match reply {
        ControlMessage::ShardAssigned { lease } => Some(lease),
        _ => None,
    })
}

async fn wait_for_lease(addr: SocketAddr, worker_id: u64, shard_id: u64) -> ShardLease {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(lease) = request_shard(addr, worker_id, shard_id).await {
            return lease;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn drain_completes_zero_downtime_tc() {
    if !docker_available() || !image_available() {
        eprintln!(
            "SKIP drain_completes_zero_downtime_tc: Docker or {IMAGE_NAME}:{IMAGE_TAG} unavailable"
        );
        return;
    }
    let cluster = TcCluster::boot("drain").await;
    for worker_id in [1u64, 2u64] {
        let reg = WorkerRegistration::new(
            WorkerId(worker_id),
            NodeRole::Worker,
            format!("host-{worker_id}:7000"),
            CapacityHeadroom::FULL,
        );
        let _ = send(cluster.control_addr, &WorkerMessage::Register(reg)).await;
    }
    assert_eq!(
        request_shard(cluster.control_addr, 1, 31)
            .await
            .unwrap()
            .worker_id,
        WorkerId(1)
    );
    let binary = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/rockstream");
    let status = std::process::Command::new(binary)
        .args([
            "cluster",
            "workers",
            "drain",
            "--control",
            &cluster.control_addr.to_string(),
            "1",
            "--yes",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    for _ in 0..20 {
        let replies = send(cluster.control_addr, &WorkerMessage::ClusterStatusQuery).await;
        assert!(matches!(
            replies.first(),
            Some(ControlMessage::ClusterStatusReport { .. })
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        wait_for_lease(cluster.control_addr, 2, 31).await.worker_id,
        WorkerId(2)
    );
    cluster.cleanup().await;
}
