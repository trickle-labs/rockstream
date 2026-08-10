//! Async Hygiene Tests (P1): Delta and Iceberg Sinks run cleanly on single-threaded Tokio runtimes
//! without triggering blocked-worker (block_in_place) panics or thread stalls.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use std::sync::Arc;

use rockstream_connectors::{DeltaSink, FaultInjectingObjectStore, IcebergSink, SinkConnector};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::RecoveryAction;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_batch() -> RecordBatch {
    let schema = test_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn test_delta_and_iceberg_sink_async_no_blocked_workers() {
    // Current-thread Tokio runtime will panic if `block_in_place` is called.
    // Proves P1: Delta and Iceberg sinks execute as genuine async operations.

    let mem_store = Arc::new(InMemory::new());
    let fault_store = Arc::new(FaultInjectingObjectStore::new(mem_store));
    let batch = test_batch();

    // 1. DeltaSink Test
    let mut delta_sink = DeltaSink::new(ConnectorId(101), fault_store.clone(), "test_delta");
    delta_sink.set_staged_batch(batch.clone());
    delta_sink.set_cluster_committed(1);

    let delta_state = delta_sink.pre_commit(1, batch.num_rows()).await.unwrap();
    delta_sink.commit(1, &delta_state).await.unwrap();

    // 2. IcebergSink Test
    let iceberg_mem_store = Arc::new(InMemory::new());
    let iceberg_fault_store = Arc::new(FaultInjectingObjectStore::new(iceberg_mem_store));
    let mut iceberg_sink = IcebergSink::new(
        ConnectorId(102),
        iceberg_fault_store.clone(),
        "test_iceberg",
    );
    iceberg_sink.set_staged_batch(batch.clone());
    iceberg_sink.set_cluster_committed(1);

    let iceberg_state = iceberg_sink.pre_commit(1, batch.num_rows()).await.unwrap();
    iceberg_sink.commit(1, &iceberg_state).await.unwrap();
}
