mod common;

use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_connectors::{ObjectStoreSink, SinkConnector};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use common::{build_minio_store, create_minio_bucket};

#[tokio::test(flavor = "multi_thread")]
async fn conditional_idempotent_finalization_uses_real_minio() {
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, "object-store-sink-real").await;
    let store = build_minio_store(port, "object-store-sink-real");
    let expected = vec![0xAB; 3];

    let (state, keys) = tokio::task::spawn_blocking({
        let store = Arc::clone(&store);
        move || {
            let mut sink = ObjectStoreSink::new(ConnectorId(51_21), store);
            sink.set_cluster_committed(7);
            let state = sink.pre_commit(7, 3).unwrap();
            sink.commit(7, &state).unwrap();
            sink.recover(RecoveryAction::RerunCommit {
                epoch: 7,
                profile: SinkIdempotencyProfile::NativeIdempotent,
                pending_handle: vec![],
            })
            .unwrap();
            (state, ["_pending/7/part-0", "final/7/part-0"])
        }
    })
    .await
    .unwrap();

    assert_eq!(
        state,
        rockstream_types::sink::SinkState::PreCommitted {
            staged_rows: 3,
            pending_handle: b"_pending/7/part-0".to_vec(),
        }
    );
    assert_eq!(keys, ["_pending/7/part-0", "final/7/part-0"]);
    assert!(store.head(&Path::from(keys[0])).await.is_err());
    assert_eq!(
        store
            .get(&Path::from(keys[1]))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicting_existing_final_is_rejected() {
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, "object-store-sink-conflict").await;
    let store = build_minio_store(port, "object-store-sink-conflict");
    store
        .put(&Path::from("final/8/part-0"), b"other".to_vec().into())
        .await
        .unwrap();

    let error = tokio::task::spawn_blocking({
        let store = Arc::clone(&store);
        move || {
            let mut sink = ObjectStoreSink::new(ConnectorId(51_22), store);
            sink.set_cluster_committed(8);
            let state = sink.pre_commit(8, 3).unwrap();
            sink.commit(8, &state).unwrap_err().to_string()
        }
    })
    .await
    .unwrap();

    assert_eq!(
        error,
        "RS-4004: sink commit failed for epoch 8: conditional final write conflict: final bytes differ"
    );
}
