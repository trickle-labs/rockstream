use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::TopologyPersistentStore;
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};

fn make_worker() -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(7),
        role: NodeRole::Worker,
        address: "127.0.0.1:7007".to_string(),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::default(),
        capabilities: WorkerCapabilities::default(),
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::draining(2, 99),
    }
}

const MINIO_BUCKET: &str = "rockstream-worker-drain-durability-test";

#[tokio::test]
async fn draining_state_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let persistent_a = TopologyPersistentStore::new(store.clone());
    let worker = make_worker();
    persistent_a.save_worker(&worker).await.unwrap();

    let persistent_b = TopologyPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        persistent_b.load_worker(worker.worker_id).await.unwrap(),
        worker
    );
}

#[tokio::test]
async fn draining_state_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP draining_state_survives_restart_minio_tc: Docker not available");
        return;
    }
    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let persistent_a = TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    let worker = make_worker();
    persistent_a.save_worker(&worker).await.unwrap();
    let persistent_b = TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    assert_eq!(
        persistent_b.load_worker(worker.worker_id).await.unwrap(),
        worker
    );
}

#[tokio::test]
async fn test_interrupted_drain_progress_survives_restart_lfs_and_minio() {
    let mut worker = make_worker();
    worker
        .lifecycle
        .advance_drain_progress(2, Some(20_000_000), Some(100_000));

    // 1. LFS
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let persistent_a = TopologyPersistentStore::new(store.clone());
    persistent_a.save_worker(&worker).await.unwrap();

    let persistent_b = TopologyPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let mut loaded = persistent_b.load_worker(worker.worker_id).await.unwrap();
    assert_eq!(loaded.lifecycle.progress_phase(), "draining");
    assert_eq!(loaded.lifecycle.shards_remaining(), Some(2));
    assert_eq!(loaded.lifecycle.bytes_remaining(), Some(20_000_000));
    assert_eq!(loaded.lifecycle.rows_remaining(), Some(100_000));

    // Advance drain after reload
    loaded
        .lifecycle
        .advance_drain_progress(1, Some(10_000_000), Some(50_000));
    assert_eq!(loaded.lifecycle.shards_remaining(), Some(1));
    loaded.lifecycle.advance_drain_progress(0, Some(0), Some(0));
    assert_eq!(loaded.lifecycle.progress_phase(), "decommissioned");
    assert_eq!(loaded.lifecycle.shards_remaining(), Some(0));

    // 2. MinIO (if docker available)
    if docker_available() {
        if let Some((_container, port)) = start_minio(MINIO_BUCKET).await {
            let persistent_minio_a =
                TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
            persistent_minio_a.save_worker(&worker).await.unwrap();
            let persistent_minio_b =
                TopologyPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
            let loaded_minio = persistent_minio_b
                .load_worker(worker.worker_id)
                .await
                .unwrap();
            assert_eq!(loaded_minio.lifecycle.progress_phase(), "draining");
            assert_eq!(loaded_minio.lifecycle.shards_remaining(), Some(2));
        }
    }
}
