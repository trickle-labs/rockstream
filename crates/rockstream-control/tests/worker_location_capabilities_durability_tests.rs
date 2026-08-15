mod support;

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::TopologyPersistentStore;
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};
use support::{create_minio_bucket, docker_available, minio_object_store};

const MINIO_BUCKET: &str = "rockstream-worker-location-capabilities-durability-test";

fn make_worker() -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(41),
        role: NodeRole::Worker,
        address: "127.0.0.1:7041".to_string(),
        capacity_headroom: CapacityHeadroom::new(0.67),
        location: WorkerLocation::new("host-durable-a", "az-west-1"),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: true,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
        registered_at_ms: 11,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

#[tokio::test]
async fn worker_location_and_capabilities_survive_lfs_reload() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let worker = make_worker();
    TopologyPersistentStore::new(store)
        .save_worker(&worker)
        .await
        .unwrap();

    let reloaded = TopologyPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        reloaded.load_worker(worker.worker_id).await,
        Some(worker.clone())
    );
    assert_eq!(reloaded.load_all().await.unwrap(), vec![worker]);
}

#[tokio::test]
async fn worker_location_and_capabilities_survive_minio_tc_reload() {
    if !docker_available() {
        eprintln!(
            "SKIP worker_location_and_capabilities_survive_minio_tc_reload: Docker not available"
        );
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let worker = make_worker();
    TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET))
        .save_worker(&worker)
        .await
        .unwrap();

    let reloaded = TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    assert_eq!(
        reloaded.load_worker(worker.worker_id).await,
        Some(worker.clone())
    );
    assert_eq!(reloaded.load_all().await.unwrap(), vec![worker]);
}
