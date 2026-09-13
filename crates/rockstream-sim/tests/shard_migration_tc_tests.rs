use std::net::SocketAddr;

use rockstream_types::ids::WorkerId;
use testcontainers::core::{ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[path = "common/management_worker.rs"]
mod management_worker;
use management_worker::{run_drain_cli, wait_for_operation_state, WorkerPeer};

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
    donor_name: String,
    control_addr: SocketAddr,
    management_addr: SocketAddr,
    network: String,
    _shared_dir: tempfile::TempDir,
}

impl TcCluster {
    async fn boot(test_id: &str) -> Self {
        let network = format!("rs-m46-net-{test_id}");
        let shared_dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                std::fs::set_permissions(shared_dir.path(), std::fs::Permissions::from_mode(0o777));
        }
        let control_name = format!("rs-m46-control-{test_id}");
        let donor_name = format!("rs-m46-donor-{test_id}");
        let recipient_name = format!("rs-m46-recipient-{test_id}");
        let gateway_name = format!("rs-m46-gateway-{test_id}");

        let control = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::message_on_stdout("control service listening"))
            .with_exposed_port(ContainerPort::Tcp(8000))
            .with_exposed_port(ContainerPort::Tcp(9201))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=control".to_string(),
                "--daemon".to_string(),
                "--control-bind=0.0.0.0:8000".to_string(),
                "--management-addr=0.0.0.0:9201".to_string(),
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
        let management_port = control.get_host_port_ipv4(9201).await.unwrap();
        let management_addr = format!("127.0.0.1:{management_port}").parse().unwrap();

        let donor = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_cmd(vec![
                "start".to_string(),
                "--storage=/data".to_string(),
                "--role=frontier".to_string(),
            ])
            .with_env_var("ROCKSTREAM_E2E_SLEEP_MS", "300000")
            .with_container_name(donor_name.clone())
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
            .with_container_name(recipient_name)
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
            .with_container_name(gateway_name)
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
            donor_name,
            control_addr,
            management_addr,
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

#[tokio::test]
async fn live_migration_zero_loss_tc() {
    if !docker_available() || !image_available() {
        eprintln!(
            "SKIP live_migration_zero_loss_tc: Docker or {IMAGE_NAME}:{IMAGE_TAG} unavailable"
        );
        return;
    }
    let cluster = TcCluster::boot("live").await;
    let mut donor = WorkerPeer::register(cluster.control_addr, 1).await;
    let mut recipient = WorkerPeer::register(cluster.control_addr, 2).await;
    assert_eq!(donor.request_shard(11).await.worker_id, WorkerId(1));
    let accepted = run_drain_cli(cluster.management_addr, 1);
    let operation_id = accepted["operation_id"].as_str().unwrap().to_owned();
    donor.wait_for_drain().await;
    donor.acknowledge_drain().await;
    assert_eq!(
        recipient.acknowledge_recipient(11).await.worker_id,
        WorkerId(2)
    );
    let completed =
        wait_for_operation_state(cluster.management_addr, &operation_id, "succeeded").await;
    assert_eq!(completed["phase"], "completed");
    assert_eq!(completed["progress"], "100%");
    cluster.cleanup().await;
}

#[tokio::test]
async fn donor_killed_mid_dual_writing_tc() {
    if !docker_available() || !image_available() {
        eprintln!(
            "SKIP donor_killed_mid_dual_writing_tc: Docker or {IMAGE_NAME}:{IMAGE_TAG} unavailable"
        );
        return;
    }
    let cluster = TcCluster::boot("dual").await;
    let mut donor = WorkerPeer::register(cluster.control_addr, 1).await;
    let _recipient = WorkerPeer::register(cluster.control_addr, 2).await;
    assert_eq!(donor.request_shard(21).await.worker_id, WorkerId(1));
    let accepted = run_drain_cli(cluster.management_addr, 1);
    let operation_id = accepted["operation_id"].as_str().unwrap().to_owned();
    donor.wait_for_drain().await;
    let status = std::process::Command::new("docker")
        .args(["rm", "-f", &cluster.donor_name])
        .status()
        .unwrap();
    assert!(status.success());
    drop(donor);
    let pending = wait_for_operation_state(cluster.management_addr, &operation_id, "running").await;
    assert_eq!(pending["state"], "running");
    assert_eq!(pending["phase"], "waiting_for_worker_flush_ack");
    cluster.cleanup().await;
}

#[tokio::test]
async fn donor_killed_mid_cutover_tc() {
    if !docker_available() || !image_available() {
        eprintln!(
            "SKIP donor_killed_mid_cutover_tc: Docker or {IMAGE_NAME}:{IMAGE_TAG} unavailable"
        );
        return;
    }
    let cluster = TcCluster::boot("cut").await;
    let mut donor = WorkerPeer::register(cluster.control_addr, 1).await;
    let mut recipient = WorkerPeer::register(cluster.control_addr, 2).await;
    assert_eq!(donor.request_shard(22).await.worker_id, WorkerId(1));
    let accepted = run_drain_cli(cluster.management_addr, 1);
    let operation_id = accepted["operation_id"].as_str().unwrap().to_owned();
    donor.wait_for_drain().await;
    donor.acknowledge_drain().await;
    let (lease, transfer_id) = recipient.wait_for_assignment(22).await;
    assert_eq!(lease.worker_id, WorkerId(2));
    assert_eq!(
        wait_for_operation_state(cluster.management_addr, &operation_id, "running").await["state"],
        "running"
    );
    let status = std::process::Command::new("docker")
        .args(["rm", "-f", &cluster.donor_name])
        .status()
        .unwrap();
    assert!(status.success());
    drop(donor);
    recipient.acknowledge_assignment(&lease, transfer_id).await;
    let completed =
        wait_for_operation_state(cluster.management_addr, &operation_id, "succeeded").await;
    assert_eq!(completed["state"], "succeeded");
    assert_eq!(completed["phase"], "completed");
    cluster.cleanup().await;
}
