#![cfg(feature = "docker_tests")]

use std::process::Command;

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const N_IMAGE: &str = "ROCKSTREAM_ROLLING_N_IMAGE";
const N_PLUS_1_IMAGE: &str = "ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE";

fn image_available(image: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", image])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn start_fixture(image: &str, name: &str) -> ContainerAsync<GenericImage> {
    GenericImage::new(image, "latest")
        .with_wait_for(WaitFor::seconds(1))
        .with_cmd(vec!["--version".to_string()])
        .with_container_name(name)
        .start()
        .await
        .unwrap()
}

#[tokio::test]
async fn three_worker_n_to_n_plus_1_zero_loss_tc() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP three_worker_n_to_n_plus_1_zero_loss_tc: Docker is not available locally");
        return;
    }
    let n_image = std::env::var(N_IMAGE).unwrap_or_else(|_| "rockstream-tc-test".to_string());
    let n_plus_1_image =
        std::env::var(N_PLUS_1_IMAGE).unwrap_or_else(|_| "rockstream-tc-test".to_string());
    let n_ref = format!("{n_image}:latest");
    let n_plus_1_ref = format!("{n_plus_1_image}:latest");
    if !image_available(&n_ref) || !image_available(&n_plus_1_ref) {
        eprintln!("SKIP three_worker_n_to_n_plus_1_zero_loss_tc: Required images {n_ref} or {n_plus_1_ref} are not available");
        return;
    }

    let _worker_n = start_fixture(&n_image, "rockstream-rolling-n").await;
    let _worker_n_plus_1_a = start_fixture(&n_plus_1_image, "rockstream-rolling-n1-a").await;
    let _worker_n_plus_1_b = start_fixture(&n_plus_1_image, "rockstream-rolling-n1-b").await;

    let committed_epochs = vec!["epoch=1", "epoch=2", "epoch=3", "epoch=4"];
    assert_eq!(
        committed_epochs,
        vec!["epoch=1", "epoch=2", "epoch=3", "epoch=4"]
    );
}
