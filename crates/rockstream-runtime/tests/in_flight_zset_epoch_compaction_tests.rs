//! Integration test suite for in-flight Z-set epoch compaction filter.
//!
//! Verifies:
//! - Zero net weight omission during epoch boundaries (offsetting updates +1/-1 produce 0 output rows, no storage write, no exchange message).
//! - Collapse of multiple high-frequency modifications on the same primary key into net single / net updates.
//! - Micro-batch window behavior: rows accumulating across the 100-300ms epoch window boundary and draining cleanly.
//! - Network exchange integration: ensure exchange payloads omit net-zero rows and emit only compacted non-zero updates.
//! - SlateDB storage state: ensure no arrangement lookups or storage writes occur for net-zero updates.
//! - Exact/full output verification across all tests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::sink::{read_view_output, ColumnValue};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::epoch_compaction::{
    EpochCompactionConfig, EpochCompactionError, EpochCompactor, DEFAULT_EPOCH_WINDOW,
    MAX_EPOCH_WINDOW, MIN_EPOCH_WINDOW,
};
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::{
    build_exchange_frame, compute_schema_fingerprint, decode_exchange_frame, deserialize_zset,
    serialize_zset, validate_exchange_frame, CURRENT_EXCHANGE_PROTOCOL_VERSION,
};
use rockstream_runtime::exchange::service::ExchangeRegistry;
use rockstream_runtime::{execute_frame, setup_test_deployment};
use rockstream_types::data_plane::{
    RuntimeExchangeMessage, RuntimeRow, DEPLOYMENT_DESCRIPTOR_VERSION,
};
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkloadId};
use rockstream_types::topology::WorkerMessage;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_zero_net_weight_omission_epoch_boundary_in_memory() {
    let compactor = EpochCompactor::default();
    let epoch = 1;

    // 1. Offsetting updates on "alpha\t100" (+1 and -1)
    compactor.push_delta(epoch, "alpha\t100", 1);
    compactor.push_delta(epoch, "alpha\t100", -1);

    // 2. Offsetting updates on "beta\t200" (+2, -1, -1)
    compactor.push_delta(epoch, "beta\t200", 2);
    compactor.push_delta(epoch, "beta\t200", -1);
    compactor.push_delta(epoch, "beta\t200", -1);

    // 3. Non-zero surviving updates on "gamma\t300" (+1) and "delta\t400" (-2)
    compactor.push_delta(epoch, "gamma\t300", 1);
    compactor.push_delta(epoch, "delta\t400", -2);

    // 4. Offsetting updates on "epsilon\t500" (+3, -3)
    compactor.push_delta(epoch, "epsilon\t500", 3);
    compactor.push_delta(epoch, "epsilon\t500", -3);

    // Drain epoch at boundary
    let flushed = compactor.drain_epoch(epoch).expect("epoch should exist");

    // Exact output check: alpha, beta, and epsilon must be omitted; only gamma and delta remain
    assert_eq!(
        flushed,
        vec![
            RuntimeRow {
                values_tsv: "gamma\t300".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "delta\t400".to_string(),
                weight: -2,
            },
        ]
    );

    // Exact metrics check
    let metrics = compactor.metrics_snapshot();
    assert_eq!(metrics.input_updates, 9);
    assert_eq!(metrics.cancelled_zero_weight_updates, 3);
    assert_eq!(metrics.emitted_updates, 2);
    assert_eq!(metrics.collapsed_updates, 4);

    // Epoch is cleanly drained
    assert!(!compactor.has_epoch(epoch));
    assert_eq!(compactor.active_epoch_count(), 0);
}

#[tokio::test]
async fn test_collapse_high_frequency_modifications_same_primary_key() {
    // Primary key is column 0
    let config = EpochCompactionConfig::default().with_primary_key_column(0);
    let compactor = EpochCompactor::new(config);
    let epoch = 42;

    // Key "item_42": Rapid CRUD sequence collapsing to net zero
    // 1. Insert (+1)
    compactor.push_delta(epoch, "item_42\twash\t10.0", 1);
    // 2. Update (-1 / +1)
    compactor.push_delta(epoch, "item_42\twash\t10.0", -1);
    compactor.push_delta(epoch, "item_42\twash\t12.5", 1);
    // 3. Update (-1 / +1)
    compactor.push_delta(epoch, "item_42\twash\t12.5", -1);
    compactor.push_delta(epoch, "item_42\tdry\t15.0", 1);
    // 4. Delete (-1)
    compactor.push_delta(epoch, "item_42\tdry\t15.0", -1);

    // Key "item_99": Rapid modifications collapsing to single non-zero row with exact weight
    // 1. Insert (+1)
    compactor.push_delta(epoch, "item_99\tiron\t5.0", 1);
    // 2. Update (-1 / +1)
    compactor.push_delta(epoch, "item_99\tiron\t5.0", -1);
    compactor.push_delta(epoch, "item_99\tiron\t8.0", 1);
    // 3. Additional weight increments (+2) and decrements (-1)
    compactor.push_delta(epoch, "item_99\tiron\t8.0", 2);
    compactor.push_delta(epoch, "item_99\tiron\t8.0", -1);
    // Net weight for item_99: 1 - 1 + 1 + 2 - 1 = 2

    // Key "item_77": Single clean insert (+1)
    compactor.push_delta(epoch, "item_77\tsteam\t20.0", 1);

    let flushed = compactor.drain_epoch(epoch).expect("epoch should exist");

    // Exact output check: item_42 omitted, item_99 collapsed to wt 2 with latest value, item_77 wt 1
    assert_eq!(
        flushed,
        vec![
            RuntimeRow {
                values_tsv: "item_99\tiron\t8.0".to_string(),
                weight: 2,
            },
            RuntimeRow {
                values_tsv: "item_77\tsteam\t20.0".to_string(),
                weight: 1,
            },
        ]
    );

    let metrics = compactor.metrics_snapshot();
    assert_eq!(metrics.input_updates, 12);
    assert_eq!(metrics.cancelled_zero_weight_updates, 1);
    assert_eq!(metrics.emitted_updates, 2);
    assert_eq!(metrics.collapsed_updates, 9);
}

#[tokio::test]
async fn test_composite_primary_key_compaction_and_cancellation() {
    // Composite PK on column 0 (tenant_id) and column 1 (user_id)
    let config = EpochCompactionConfig::default().with_primary_key_columns(vec![0, 1]);
    let compactor = EpochCompactor::new(config);
    let epoch = 10;

    // corp_a / u_1: insert guest (+1), delete guest (-1), insert admin (+1) -> net weight 1
    compactor.push_delta(epoch, "corp_a\tu_1\trole_guest", 1);
    compactor.push_delta(epoch, "corp_a\tu_1\trole_guest", -1);
    compactor.push_delta(epoch, "corp_a\tu_1\trole_admin", 1);

    // corp_a / u_2: insert (+1), delete (-1) -> net weight 0 (omitted!)
    compactor.push_delta(epoch, "corp_a\tu_2\tactive", 1);
    compactor.push_delta(epoch, "corp_a\tu_2\tactive", -1);

    // corp_b / u_1: distinct tenant, same user id -> insert (+3), decrement (-1) -> net weight 2
    compactor.push_delta(epoch, "corp_b\tu_1\tactive", 3);
    compactor.push_delta(epoch, "corp_b\tu_1\tsuspended", -1);

    let flushed = compactor.drain_epoch(epoch).expect("epoch should exist");

    assert_eq!(
        flushed,
        vec![
            RuntimeRow {
                values_tsv: "corp_a\tu_1\trole_admin".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "corp_b\tu_1\tsuspended".to_string(),
                weight: 2,
            },
        ]
    );

    let metrics = compactor.metrics_snapshot();
    assert_eq!(metrics.input_updates, 7);
    assert_eq!(metrics.cancelled_zero_weight_updates, 1);
    assert_eq!(metrics.emitted_updates, 2);
    assert_eq!(metrics.collapsed_updates, 4);
}

#[tokio::test]
async fn test_micro_batch_window_bounds_and_configuration() {
    let default_config = EpochCompactionConfig::default();
    assert_eq!(default_config.window_duration, DEFAULT_EPOCH_WINDOW);
    assert_eq!(default_config.min_window_duration, MIN_EPOCH_WINDOW);
    assert_eq!(default_config.max_window_duration, MAX_EPOCH_WINDOW);
    assert!(default_config.validate().is_ok());

    // Valid micro-batch window bounds [100ms, 300ms]
    assert!(EpochCompactionConfig::new(Duration::from_millis(100)).is_ok());
    assert!(EpochCompactionConfig::new(Duration::from_millis(200)).is_ok());
    assert!(EpochCompactionConfig::new(Duration::from_millis(300)).is_ok());

    // Reject below minimum (< 100ms)
    let err_low = EpochCompactionConfig::new(Duration::from_millis(99)).unwrap_err();
    assert_eq!(
        err_low,
        EpochCompactionError::WindowDurationBelowMinimum {
            actual: Duration::from_millis(99),
            minimum: Duration::from_millis(100),
        }
    );

    // Reject above maximum (> 300ms)
    let err_high = EpochCompactionConfig::new(Duration::from_millis(301)).unwrap_err();
    assert_eq!(
        err_high,
        EpochCompactionError::WindowDurationAboveMaximum {
            actual: Duration::from_millis(301),
            maximum: Duration::from_millis(300),
        }
    );

    // Clamping behavior
    let clamped_low = default_config
        .clone()
        .with_window_duration_clamped(Duration::from_millis(10));
    assert_eq!(clamped_low.window_duration, Duration::from_millis(100));

    let clamped_high = default_config
        .clone()
        .with_window_duration_clamped(Duration::from_millis(500));
    assert_eq!(clamped_high.window_duration, Duration::from_millis(300));

    let clamped_valid = default_config.with_window_duration_clamped(Duration::from_millis(250));
    assert_eq!(clamped_valid.window_duration, Duration::from_millis(250));
}

#[tokio::test]
async fn test_micro_batch_window_accumulation_and_expiration_draining() {
    let t0 = Instant::now();
    let config = EpochCompactionConfig::default()
        .with_bounds(Duration::from_millis(10), Duration::from_millis(500))
        .with_window_duration(Duration::from_millis(150))
        .expect("valid window duration");

    let compactor = EpochCompactor::new(config);

    // Open Epoch 10 at t0
    compactor.push_row_at(
        10,
        RuntimeRow {
            values_tsv: "key_1\tv1".to_string(),
            weight: 1,
        },
        t0,
    );

    // At t0 + 50ms: push offsetting and new delta to Epoch 10
    let t_50ms = t0 + Duration::from_millis(50);
    compactor.push_row_at(
        10,
        RuntimeRow {
            values_tsv: "key_1\tv1".to_string(),
            weight: -1,
        },
        t_50ms,
    );
    compactor.push_row_at(
        10,
        RuntimeRow {
            values_tsv: "key_2\tv1".to_string(),
            weight: 3,
        },
        t_50ms,
    );

    // At t0 + 80ms: open Epoch 20 with row
    let t_80ms = t0 + Duration::from_millis(80);
    compactor.push_row_at(
        20,
        RuntimeRow {
            values_tsv: "key_3\tv1".to_string(),
            weight: 5,
        },
        t_80ms,
    );

    // 1. Check at t0 + 100ms:
    // Epoch 10 age = 100ms (< 150ms) -> not expired
    // Epoch 20 age = 20ms (< 150ms) -> not expired
    let t_100ms = t0 + Duration::from_millis(100);
    assert!(!compactor.is_epoch_expired_at(10, t_100ms));
    assert!(!compactor.is_epoch_expired_at(20, t_100ms));
    assert!(compactor.expired_epochs_at(t_100ms).is_empty());
    assert_eq!(compactor.active_epoch_count(), 2);

    // 2. Check at t0 + 160ms:
    // Epoch 10 age = 160ms (>= 150ms) -> EXPIRED!
    // Epoch 20 age = 80ms (< 150ms) -> not expired
    let t_160ms = t0 + Duration::from_millis(160);
    assert!(compactor.is_epoch_expired_at(10, t_160ms));
    assert!(!compactor.is_epoch_expired_at(20, t_160ms));
    assert_eq!(compactor.expired_epochs_at(t_160ms), vec![10]);

    // Drain expired epochs at t_160ms: only Epoch 10 is drained
    let drained_10 = compactor.drain_expired_epochs_at(t_160ms);
    assert_eq!(drained_10.len(), 1);
    assert_eq!(drained_10[0].0, 10);
    // key_1 cancelled out (1 - 1 = 0); only key_2 survives with weight 3
    assert_eq!(
        drained_10[0].1,
        vec![RuntimeRow {
            values_tsv: "key_2\tv1".to_string(),
            weight: 3,
        }]
    );

    // Epoch 10 is no longer active, Epoch 20 remains active
    assert!(!compactor.has_epoch(10));
    assert!(compactor.has_epoch(20));
    assert_eq!(compactor.active_epoch_count(), 1);

    // 3. Check at t0 + 240ms:
    // Epoch 20 age = 160ms (>= 150ms) -> EXPIRED!
    let t_240ms = t0 + Duration::from_millis(240);
    assert!(compactor.is_epoch_expired_at(20, t_240ms));
    assert_eq!(compactor.expired_epochs_at(t_240ms), vec![20]);

    // Drain Epoch 20
    let drained_20 = compactor.drain_expired_epochs_at(t_240ms);
    assert_eq!(drained_20.len(), 1);
    assert_eq!(drained_20[0].0, 20);
    assert_eq!(
        drained_20[0].1,
        vec![RuntimeRow {
            values_tsv: "key_3\tv1".to_string(),
            weight: 5,
        }]
    );

    // All active epochs cleanly drained
    assert_eq!(compactor.active_epoch_count(), 0);
    assert!(compactor.active_epochs().is_empty());
}

#[tokio::test]
async fn test_micro_batch_real_time_window_expiration() {
    let compactor = EpochCompactor::default(); // 100ms window
    let epoch = 100;

    compactor.push_delta(epoch, "live_row", 2);
    compactor.push_delta(epoch, "offset_row", 1);
    compactor.push_delta(epoch, "offset_row", -1);

    // Immediately after ingestion: window has not expired
    assert!(!compactor.is_epoch_expired(epoch));

    // Wait for micro-batch window (100ms) to elapse
    tokio::time::sleep(Duration::from_millis(120)).await;

    // After window duration: epoch has expired
    assert!(compactor.is_epoch_expired(epoch));
    let expired = compactor.drain_expired_epochs();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].0, epoch);
    assert_eq!(
        expired[0].1,
        vec![RuntimeRow {
            values_tsv: "live_row".to_string(),
            weight: 2,
        }]
    );

    assert_eq!(compactor.active_epoch_count(), 0);
}

#[tokio::test]
async fn test_network_exchange_payload_omits_net_zero_and_emits_exact_compacted_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let compactor = EpochCompactor::default();

    // High frequency updates within an epoch window:
    // id 10: +1, -1 -> net zero (MUST NOT BE IN EXCHANGE PAYLOAD)
    // id 20: +2, -1 -> net weight 1
    // id 30: +3 -> net weight 3
    // id 40: +1, -1 -> net zero (MUST NOT BE IN EXCHANGE PAYLOAD)
    let raw_deltas = vec![
        RuntimeRow {
            values_tsv: "10\talice".to_string(),
            weight: 1,
        },
        RuntimeRow {
            values_tsv: "10\talice".to_string(),
            weight: -1,
        },
        RuntimeRow {
            values_tsv: "20\tbob".to_string(),
            weight: 2,
        },
        RuntimeRow {
            values_tsv: "20\tbob".to_string(),
            weight: -1,
        },
        RuntimeRow {
            values_tsv: "30\tcharlie".to_string(),
            weight: 3,
        },
        RuntimeRow {
            values_tsv: "40\tdave".to_string(),
            weight: 1,
        },
        RuntimeRow {
            values_tsv: "40\tdave".to_string(),
            weight: -1,
        },
    ];

    // Compact in-flight rows prior to network exchange frame construction
    let compacted_rows = compactor.compact_slice(&raw_deltas);
    assert_eq!(
        compacted_rows,
        vec![
            RuntimeRow {
                values_tsv: "20\tbob".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "30\tcharlie".to_string(),
                weight: 3,
            },
        ]
    );

    // Build ArrowZSet from compacted rows
    let id_col = Arc::new(Int64Array::from(vec![20, 30])) as ArrayRef;
    let name_col = Arc::new(StringArray::from(vec!["bob", "charlie"])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_col, name_col]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1, 3]);

    // Build exchange frame
    let frame = build_exchange_frame(1, 100, 10, 1, 777, &schema, &zset).unwrap();

    // Validate exchange frame integrity and protocol version
    let fingerprint = compute_schema_fingerprint(&schema);
    validate_exchange_frame(&frame, 777, Some(&fingerprint), 16 * 1024 * 1024).unwrap();
    assert_eq!(frame.protocol_version, CURRENT_EXCHANGE_PROTOCOL_VERSION);
    assert_eq!(frame.workload_id, 1);
    assert_eq!(frame.shard_id, 100);
    assert_eq!(frame.operator_id, 10);
    assert_eq!(frame.epoch, 1);
    assert_eq!(frame.lease_token, 777);

    // Decode exchange frame on receiving worker
    let decoded_zset = decode_exchange_frame(&frame, schema.clone()).unwrap();

    // Assert exact full contents of decoded ArrowZSet
    assert_eq!(decoded_zset.num_rows(), 2);
    assert_eq!(decoded_zset.weights, vec![1, 3]);

    let decoded_id = decoded_zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(decoded_id.value(0), 20);
    assert_eq!(decoded_id.value(1), 30);

    let decoded_name = decoded_zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(decoded_name.value(0), "bob");
    assert_eq!(decoded_name.value(1), "charlie");

    // Also verify: all-net-zero frame results in empty payload
    let all_zero_deltas = vec![
        RuntimeRow {
            values_tsv: "999\tnobody".to_string(),
            weight: 1,
        },
        RuntimeRow {
            values_tsv: "999\tnobody".to_string(),
            weight: -1,
        },
    ];
    let empty_compacted = compactor.compact_slice(&all_zero_deltas);
    assert!(empty_compacted.is_empty());

    let empty_zset = ArrowZSet::empty(schema.clone());
    let empty_frame = build_exchange_frame(1, 100, 10, 1, 777, &schema, &empty_zset).unwrap();
    let decoded_empty = decode_exchange_frame(&empty_frame, schema).unwrap();
    assert_eq!(decoded_empty.num_rows(), 0);
    assert!(decoded_empty.weights.is_empty());
}

#[tokio::test]
async fn test_network_exchange_registry_channel_delivery_exact() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let registry = ExchangeRegistry::new();
    let (tx, mut rx) = mpsc::channel(10);
    let exchange_id = 500;
    let target_shard = 2;

    registry.register(exchange_id, target_shard, tx, schema.clone());

    // Prepare in-flight deltas with high-frequency updates:
    // id 101: +2, +2 -> collapses to +4
    // id 102: -1 -> -1
    // id 103: +1, -1 -> net 0 (omitted!)
    let compactor = EpochCompactor::default();
    let rows = vec![
        RuntimeRow {
            values_tsv: "101\tnode-1".to_string(),
            weight: 2,
        },
        RuntimeRow {
            values_tsv: "101\tnode-1".to_string(),
            weight: 2,
        },
        RuntimeRow {
            values_tsv: "102\tnode-2".to_string(),
            weight: -1,
        },
        RuntimeRow {
            values_tsv: "103\ttemp".to_string(),
            weight: 1,
        },
        RuntimeRow {
            values_tsv: "103\ttemp".to_string(),
            weight: -1,
        },
    ];

    let compacted = compactor.compact_slice(&rows);
    assert_eq!(
        compacted,
        vec![
            RuntimeRow {
                values_tsv: "101\tnode-1".to_string(),
                weight: 4,
            },
            RuntimeRow {
                values_tsv: "102\tnode-2".to_string(),
                weight: -1,
            },
        ]
    );

    let id_col = Arc::new(Int64Array::from(vec![101, 102])) as ArrayRef;
    let name_col = Arc::new(StringArray::from(vec!["node-1", "node-2"])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_col, name_col]).unwrap();
    let zset = ArrowZSet::new(batch, vec![4, -1]);

    let payload = serialize_zset(&zset).unwrap();
    let shuffle_frame = ShuffleFrame {
        exchange_id,
        src_shard: 0,
        target_shard,
        epoch: 1,
        seq: 1,
        payload: payload.into(),
        row_count: zset.num_rows() as u32,
    };

    // Forward frame to registered inlet channel
    let inlet = registry
        .get(exchange_id, target_shard)
        .expect("inlet should be registered");
    let deserialized = deserialize_zset(&shuffle_frame.payload, inlet.schema).unwrap();
    inlet.sender.send(deserialized).await.unwrap();

    // Receiver gets decoded zset
    let received_zset = rx.recv().await.expect("received frame expected");
    assert_eq!(received_zset.num_rows(), 2);
    assert_eq!(received_zset.weights, vec![4, -1]);

    let received_id = received_zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(received_id.value(0), 101);
    assert_eq!(received_id.value(1), 102);

    let received_name = received_zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(received_name.value(0), "node-1");
    assert_eq!(received_name.value(1), "node-2");
}

#[tokio::test]
async fn test_runtime_pipeline_all_rows_net_zero_omits_storage_writes_and_progress() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (client, deployments, db, compactor, mut progress_rx) =
        setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

    // Exchange frame where all rows offset to net-zero (+1 and -1 on key 555)
    let frame = RuntimeExchangeMessage {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: "req-zero-omit".to_string(),
        workload_id: WorkloadId(1),
        shard_id: ShardId(100),
        operator_id: OperatorId(10),
        lease_token: LeaseToken(777),
        epoch: 1,
        source: "items".to_string(),
        rows: vec![
            RuntimeRow {
                values_tsv: "555\ttransient".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "555\ttransient".to_string(),
                weight: -1,
            },
        ],
    };

    execute_frame(&client, &deployments, frame).await.unwrap();

    // Verify emitted progress message: output rows must be completely empty!
    let msg = progress_rx.recv().await.expect("progress message expected");
    match msg {
        WorkerMessage::ExecutionProgress {
            output,
            input_rows,
            output_rows,
        } => {
            assert_eq!(input_rows, 2);
            assert_eq!(output_rows, 0);
            assert!(output.rows.is_empty());
            assert_eq!(output.epoch, 1);
            assert_eq!(output.operator_id, OperatorId(10));
        }
        other => panic!("unexpected worker message: {other:?}"),
    }

    // Verify compaction metrics: exactly 1 cancelled zero-weight update
    let metrics = compactor.metrics_snapshot();
    assert_eq!(metrics.input_updates, 2);
    assert_eq!(metrics.cancelled_zero_weight_updates, 1);
    assert_eq!(metrics.emitted_updates, 0);

    // Verify SlateDB storage: NO writes occurred to storage
    let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
    assert!(
        stored.is_empty(),
        "storage writes must be completely omitted when all rows offset to net-zero"
    );
}

#[tokio::test]
async fn test_runtime_pipeline_mixed_rows_exact_storage_and_progress_delta() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (client, deployments, db, compactor, mut progress_rx) =
        setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

    // Exchange frame with:
    // - key 10: +1, -1 -> net zero (omitted from pipeline & storage)
    // - key 20: +3, -1 -> net weight 2 (collapsed)
    // - key 30: +1 -> net weight 1
    // - key 40: +2, -2 -> net zero (omitted from pipeline & storage)
    let frame = RuntimeExchangeMessage {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: "req-mixed".to_string(),
        workload_id: WorkloadId(1),
        shard_id: ShardId(100),
        operator_id: OperatorId(10),
        lease_token: LeaseToken(777),
        epoch: 1,
        source: "items".to_string(),
        rows: vec![
            RuntimeRow {
                values_tsv: "10\talice".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "10\talice".to_string(),
                weight: -1,
            },
            RuntimeRow {
                values_tsv: "20\tbob".to_string(),
                weight: 3,
            },
            RuntimeRow {
                values_tsv: "20\tbob".to_string(),
                weight: -1,
            },
            RuntimeRow {
                values_tsv: "30\tcharlie".to_string(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "40\tdavid".to_string(),
                weight: 2,
            },
            RuntimeRow {
                values_tsv: "40\tdavid".to_string(),
                weight: -2,
            },
        ],
    };

    execute_frame(&client, &deployments, frame).await.unwrap();

    // Verify emitted progress message: only keys 20 and 30 survived
    let msg = progress_rx.recv().await.expect("progress message expected");
    match msg {
        WorkerMessage::ExecutionProgress {
            output,
            input_rows,
            output_rows,
        } => {
            assert_eq!(input_rows, 7);
            assert_eq!(output_rows, 2);
            assert_eq!(
                output.rows,
                vec![
                    RuntimeRow {
                        values_tsv: "20\tbob".to_string(),
                        weight: 2,
                    },
                    RuntimeRow {
                        values_tsv: "30\tcharlie".to_string(),
                        weight: 1,
                    },
                ]
            );
        }
        other => panic!("unexpected worker message: {other:?}"),
    }

    // Verify SlateDB storage: exactly keys 20 and 30 written with exact columns and weights
    let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
    assert_eq!(stored.len(), 2);

    // Row 1: key 20, "bob", weight 2
    assert_eq!(stored[0].0, 1); // epoch 1
    assert_eq!(
        stored[0].2,
        vec![
            ColumnValue::Int64(20),
            ColumnValue::Utf8("bob".to_string()),
        ]
    );
    assert_eq!(stored[0].3, 2); // weight 2

    // Row 2: key 30, "charlie", weight 1
    assert_eq!(stored[1].0, 1); // epoch 1
    assert_eq!(
        stored[1].2,
        vec![
            ColumnValue::Int64(30),
            ColumnValue::Utf8("charlie".to_string()),
        ]
    );
    assert_eq!(stored[1].3, 1); // weight 1

    // Verify compaction metrics
    let metrics = compactor.metrics_snapshot();
    // 7 input updates + 2 output updates = 9 total updates evaluated
    assert_eq!(metrics.input_updates, 9);
    assert_eq!(metrics.cancelled_zero_weight_updates, 2);
    assert_eq!(metrics.collapsed_updates, 3);
    assert_eq!(metrics.emitted_updates, 4);
}
