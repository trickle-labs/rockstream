//! Container Packaging & Compose Profile Tests (v0.59.22 Slice 2 / Phase 3a).

use std::fs;
use std::path::Path;

#[test]
fn test_docker_non_root_read_only_rootfs_execution() {
    let dockerfile_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("Dockerfile");
    let dockerfile_content =
        fs::read_to_string(&dockerfile_path).expect("Dockerfile must exist at workspace root");

    // 1. Verify Non-Root User Directives in Dockerfile
    assert!(dockerfile_content.contains("10001"));
    assert!(dockerfile_content.contains("rockstream"));
    assert!(dockerfile_content.contains("USER rockstream:rockstream"));
    assert!(dockerfile_content.contains("WORKDIR /data"));
    assert!(dockerfile_content.contains("VOLUME [\"/data\"]"));

    // 2. Verify Standard Port Declarations
    assert!(dockerfile_content.contains("EXPOSE 5432 9090 9100 9200"));

    // 3. Verify Healthcheck Probe
    assert!(dockerfile_content.contains("HEALTHCHECK"));
    assert!(dockerfile_content.contains("http://localhost:9090/ready"));

    // 4. Verify Docker Compose Profiles
    let compose_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/compose");

    let standalone_compose = fs::read_to_string(compose_dir.join("docker-compose.standalone.yaml"))
        .expect("standalone compose manifest must exist");
    assert!(standalone_compose.contains("read_only: true"));
    assert!(standalone_compose.contains("user: \"10001:10001\""));
    assert!(standalone_compose.contains("rs_data:/data:rw"));
    assert!(standalone_compose.contains("http://localhost:9090/ready"));

    let distributed_compose =
        fs::read_to_string(compose_dir.join("docker-compose.distributed.yaml"))
            .expect("distributed compose manifest must exist");
    assert!(distributed_compose.contains("control:"));
    assert!(distributed_compose.contains("worker-1:"));
    assert!(distributed_compose.contains("worker-2:"));
    assert!(distributed_compose.contains("gateway:"));
    assert!(distributed_compose.contains("minio:"));
    assert!(distributed_compose.contains("condition: service_healthy"));

    let cdc_compose = fs::read_to_string(compose_dir.join("docker-compose.cdc.yaml"))
        .expect("cdc compose manifest must exist");
    assert!(cdc_compose.contains("wal_level=logical"));
    assert!(cdc_compose.contains("postgres:18.0-bookworm"));
}
