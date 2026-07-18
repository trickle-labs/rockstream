mod support;

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_control::CheckpointManifestStore;
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::ids::ShardId;
use support::{create_minio_bucket, docker_available, minio_object_store};

const MINIO_BUCKET: &str = "rockstream-checkpoint-manifest-durability-test";

fn manifest(checkpoint_id: u64, shard_base: u64) -> ClusterCheckpoint {
    let checkpoint_id = CheckpointId(checkpoint_id);
    let mut manifest = ClusterCheckpoint::new(checkpoint_id);
    manifest.record_shard(
        ShardId(shard_base),
        PerShardCheckpoint::new(checkpoint_id, 10 + shard_base),
    );
    manifest.record_shard(
        ShardId(shard_base + 1),
        PerShardCheckpoint::new(checkpoint_id, 20 + shard_base),
    );
    manifest
}

#[tokio::test]
async fn zstd_checkpoint_manifests_survive_lfs_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let manifests = CheckpointManifestStore::new(store);
    let expected = manifest(7, 1);
    manifests
        .save_manifest(&expected, true, None)
        .await
        .unwrap();

    let reloaded = CheckpointManifestStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        reloaded.load_manifest(CheckpointId(7)).await,
        Some(expected)
    );
}

#[tokio::test]
async fn legacy_json_and_zstd_checkpoint_manifests_survive_minio_tc_restart() {
    if !docker_available() {
        eprintln!(
            "SKIP legacy_json_and_zstd_checkpoint_manifests_survive_minio_tc_restart: Docker not available"
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

    let zstd_manifest = manifest(8, 3);
    let legacy_manifest = manifest(9, 5);
    let first = minio_object_store(port, MINIO_BUCKET);
    let manifests = CheckpointManifestStore::new(first.clone());
    manifests
        .save_manifest(&zstd_manifest, true, None)
        .await
        .unwrap();
    first
        .put(
            &Path::from("control/checkpoints/9"),
            serde_json::to_vec(&legacy_manifest).unwrap().into(),
        )
        .await
        .unwrap();

    let reloaded = CheckpointManifestStore::new(minio_object_store(port, MINIO_BUCKET));
    assert_eq!(
        reloaded.load_manifest(CheckpointId(8)).await,
        Some(zstd_manifest)
    );
    assert_eq!(
        reloaded.load_manifest(CheckpointId(9)).await,
        Some(legacy_manifest)
    );
}
