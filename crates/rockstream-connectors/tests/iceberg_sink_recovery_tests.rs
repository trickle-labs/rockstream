mod common;

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_connectors::{FaultInjectingObjectStore, IcebergSink, SinkConnector, SinkError};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::RecoveryAction;
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use common::{
    build_minio_store, create_minio_bucket, docker_available, make_cumulative_batch,
    render_batches, RNG_SEED,
};

const NUM_EPOCHS: u64 = 8;
const PARTIAL_WRITE_PROBABILITY: f64 = 0.5;

async fn run_iceberg_recovery_scenario(store: Arc<dyn ObjectStore>, base_path: &str) {
    let mut fault_store = FaultInjectingObjectStore::new(store);
    fault_store.set_partial_write_probability(PARTIAL_WRITE_PROBABILITY);
    fault_store.set_deterministic_fault_seed(RNG_SEED);
    let store = Arc::new(fault_store);
    let mut sink = IcebergSink::new(ConnectorId(144), store, base_path);
    sink.set_parquet_row_group_bytes(256);

    let mut expected_final = None;
    for epoch in 1..=NUM_EPOCHS {
        let batch = make_cumulative_batch(epoch as i64);
        expected_final = Some(batch.clone());
        sink.set_staged_batch(batch.clone());
        let state = sink.pre_commit(epoch, batch.num_rows()).await.unwrap();
        sink.set_cluster_committed(epoch);

        if let Err(SinkError::CommitFailed { .. }) = sink.commit(epoch, &state).await {
            let action = RecoveryAction::from_sink_state(&state, epoch, sink.idempotency_profile());
            let mut recovered = false;
            for _attempt in 0..12 {
                if sink.recover(action.clone()).await.is_ok() {
                    recovered = true;
                    break;
                }
            }
            assert!(
                recovered,
                "epoch {epoch}: recovery never completed successfully"
            );
        }
    }

    let observed = sink.read_latest_snapshot().await.unwrap();
    assert_eq!(
        render_batches(&observed),
        render_batches(std::slice::from_ref(expected_final.as_ref().unwrap()))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_iceberg_sink_recovery_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    run_iceberg_recovery_scenario(store, "iceberg-lfs").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_iceberg_sink_recovery_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP test_iceberg_sink_recovery_minio_tc: Docker not available");
        return;
    }

    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, "iceberg-recovery-test").await;
    let store = build_minio_store(port, "iceberg-recovery-test");
    run_iceberg_recovery_scenario(store, "iceberg-minio").await;
}
