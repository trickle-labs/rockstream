#![cfg(feature = "simulation")]

use std::sync::Arc;

use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use rockstream_control::{CheckpointExportService, CheckpointManifestStore};
use rockstream_sim::{
    buggify::buggify_disable, buggify::buggify_focus, buggify::buggify_init, SimRuntime,
};
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};

#[tokio::test]
async fn m3_export_selection_is_single_epoch_under_buggify() {
    let _runtime = SimRuntime::new(0x5601_0001);
    let source_dir = tempfile::tempdir().unwrap();
    let destination_dir = tempfile::tempdir().unwrap();
    let source: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(source_dir.path()).unwrap());
    let destination: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(destination_dir.path()).unwrap());
    let manifests = CheckpointManifestStore::new(source.clone());
    let checkpoint = ClusterCheckpoint::new(CheckpointId(11));
    manifests
        .save_manifest(&checkpoint, false, None)
        .await
        .unwrap();
    source
        .put(
            &Path::from("shard/11/manifest"),
            bytes::Bytes::from_static(b"epoch-11").into(),
        )
        .await
        .unwrap();

    buggify_init(0x5601_0001);
    buggify_focus("dr.export.after_m3_selection");
    let service = CheckpointExportService::new();
    let selected = service
        .export_latest(
            source.clone(),
            destination.clone(),
            &manifests,
            "generation-11",
            |_| vec![Path::from("shard/11/manifest")],
        )
        .await
        .unwrap_err();
    assert_eq!(
        selected.to_string(),
        "RS-5035: checkpoint export was interrupted at dr.export.after_m3_selection; next_steps: retry the same generation so validated objects can be resumed"
    );

    buggify_focus("dr.export.before_terminal_marker");
    let interrupted = service
        .export_latest(
            source.clone(),
            destination.clone(),
            &manifests,
            "generation-11",
            |_| vec![Path::from("shard/11/manifest")],
        )
        .await
        .unwrap_err();
    assert_eq!(
        interrupted.to_string(),
        "RS-5035: checkpoint export was interrupted at dr.export.before_terminal_marker; next_steps: retry the same generation so validated objects can be resumed"
    );

    buggify_disable();
    let outcome = service
        .export_latest(source, destination, &manifests, "generation-11", |_| {
            vec![Path::from("shard/11/manifest")]
        })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        rockstream_control::CheckpointExportOutcome {
            checkpoint_id: 11,
            generation: "generation-11".to_string(),
            object_count: 1,
            byte_count: 8,
            inventory_digest: "2a083be67e8c1aeeda3a6ca142bd7c0341be7bdcbbd6852d0aae9813bbc51d67"
                .to_string(),
            status: "SUCCESS".to_string(),
        }
    );
}
