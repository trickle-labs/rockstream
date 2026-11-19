//! v0.67 Slice 3 exit tests: Direct Arrow IPC data plane ingestion & read hot path.
//!
//! Verifies:
//! 1. Worker shuffle delivers exact Arrow Z-sets to registered inlets over gRPC.
//! 2. Direct Arrow RecordBatch read hot path (`ViewReader::read_view_batches`) without TSV conversion.
//! 3. Preservation of NULLs, exact column types, signed multiplicities, and result multisets.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::memory::InMemory;
use rockstream_gateway::view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};
use rockstream_ops::sink::{write_view_directory_entry, ViewSinkOp};
use rockstream_ops::ArrowZSet;
use rockstream_runtime::exchange::proto::shuffle_service_client::ShuffleServiceClient;
use rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::{build_exchange_frame, serialize_zset};
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tonic::transport::Server;

/// Test 1: Worker shuffle delivers the full Arrow Z-set to its registered inlet.
#[tokio::test]
async fn test_worker_shuffle_delivers_exact_arrow_zset_to_registered_inlet() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let id_array = Arc::new(Int64Array::from(vec![1, 2, 2, 3])) as ArrayRef;
    let label_array = Arc::new(StringArray::from(vec![
        Some("alpha"),
        None,
        None,
        Some("gamma"),
    ])) as ArrayRef;
    let score_array = Arc::new(Float64Array::from(vec![
        Some(10.5),
        Some(20.0),
        Some(20.0),
        None,
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, label_array, score_array])
        .expect("valid record batch");
    let zset = ArrowZSet::new(batch, vec![1, 2, -1, 1]);
    let expected_payload = serialize_zset(&zset).expect("serialize expected Arrow Z-set");

    let registry = ExchangeRegistry::new();
    let (inlet_tx, mut inlet_rx) = tokio::sync::mpsc::channel(1);
    registry.register(1001, 1, inlet_tx, schema.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ShuffleServiceServer::new(ShuffleServer::new(registry)))
            .serve(addr)
            .await
            .unwrap();
    });

    // Wait for server to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = ShuffleServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to worker shuffle service");

    let frame = ShuffleFrame {
        exchange_id: 1001,
        src_shard: 2,
        target_shard: 1,
        epoch: 3,
        seq: 42,
        payload: expected_payload.to_vec(),
        row_count: zset.num_rows() as u32,
    };
    let mut request = tonic::Request::new(futures::stream::iter(vec![frame]));
    request
        .metadata_mut()
        .insert("protocol_version", "1".parse().unwrap());

    let mut response_stream = client
        .shuffle_stream(request)
        .await
        .expect("call shuffle_stream")
        .into_inner();

    let ack = response_stream
        .next()
        .await
        .expect("response stream item")
        .expect("valid ack");

    assert_eq!(ack.exchange_id, 1001);
    assert_eq!(ack.src_shard, 2);
    assert_eq!(ack.target_shard, 1);
    assert_eq!(ack.epoch, 3);
    assert_eq!(ack.seq, 42);
    assert_eq!(ack.credit_grant, 4);
    let received = inlet_rx.recv().await.expect("registered inlet delivery");
    assert_eq!(serialize_zset(&received).unwrap(), expected_payload);

    server_handle.abort();
}

#[tokio::test]
async fn test_exchange_stream_rejects_frames_without_an_execution_handler() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let frame =
        build_exchange_frame(1001, 1, 50, 3, 42, &schema, &ArrowZSet::new(batch, vec![1])).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ShuffleServiceServer::new(ShuffleServer::new(
                ExchangeRegistry::new(),
            )))
            .serve(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut client = ShuffleServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to worker shuffle service");
    let mut request = tonic::Request::new(futures::stream::iter(vec![frame]));
    request
        .metadata_mut()
        .insert("protocol_version", "1".parse().unwrap());

    let status = match client.exchange_stream(request).await {
        Err(status) => status,
        Ok(_) => panic!("ExchangeStream must not acknowledge an unexecuted frame"),
    };
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert_eq!(
        status.message(),
        "ExchangeStream has no execution handler; use ShuffleStream for worker shuffle delivery"
    );
    server_handle.abort();
}

/// Test 2 (Slice 3): Gateway read view reads directly as Arrow RecordBatches without TSV conversion.
#[tokio::test]
async fn test_gateway_read_view_direct_arrow_ipc_no_tsv() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("test-direct-arrow-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );

    // Write a multi-column compiled view through ViewSinkOp
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("val", DataType::Float64, true),
    ]));

    let id_col = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
    let name_col =
        Arc::new(StringArray::from(vec![Some("first"), Some("second"), None])) as ArrayRef;
    let val_col = Arc::new(Float64Array::from(vec![Some(1.23), None, Some(4.56)])) as ArrayRef;

    let batch = RecordBatch::try_new(schema.clone(), vec![id_col, name_col, val_col]).unwrap();
    let weights = vec![1i64, 2i64, 1i64];
    let zset = ArrowZSet::new(batch, weights);

    let sink = ViewSinkOp::new(shard_db.clone(), OperatorId(77));
    sink.write_next_epoch(&zset).await.unwrap();
    write_view_directory_entry(&shard_db, "direct_arrow_view", OperatorId(77), 3, &[0])
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let reader = rockstream_storage::ShardReader::open("test-direct-arrow-shard", store.clone())
        .await
        .unwrap();
    let view_reader = HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    };

    let batches = view_reader
        .read_view_batches("direct_arrow_view", ViewReadStrategy::HotOnly)
        .await
        .expect("read_view_batches succeeds");

    assert!(!batches.is_empty(), "expected at least one RecordBatch");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // With weights [1, 2, 1], row 20 is duplicated once -> 4 total materialized rows
    assert_eq!(
        total_rows, 4,
        "materialized rows must account for multiplicities"
    );

    let b0 = &batches[0];
    assert_eq!(b0.num_columns(), 3);
    assert_eq!(b0.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(b0.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(b0.schema().field(2).data_type(), &DataType::Float64);
}

/// Test 3 (Slice 3): Gateway ingest and read direct arrow batches integration.
#[tokio::test]
async fn test_gateway_ingest_and_read_direct_arrow_batches() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("test-combined-arrow-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_col = Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef;
    let v_col = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![k_col, v_col]).unwrap();
    let weights = vec![1i64, 1i64, 1i64];
    let zset = ArrowZSet::new(batch, weights);

    let sink = ViewSinkOp::new(shard_db.clone(), OperatorId(88));
    sink.write_next_epoch(&zset).await.unwrap();
    write_view_directory_entry(&shard_db, "combined_view", OperatorId(88), 2, &[0])
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let reader = rockstream_storage::ShardReader::open("test-combined-arrow-shard", store.clone())
        .await
        .unwrap();
    let view_reader = HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    };

    let batches = view_reader
        .read_view_batches("combined_view", ViewReadStrategy::HotOnly)
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(batches[0].num_columns(), 2);
}
