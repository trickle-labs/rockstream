use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcWireFormat, PgLsn, PgOutputConfig, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
    SourceConnector, POSTGRES_CDC_MAX_WAL_LAG_BYTES,
};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::NoTls;

fn source() -> PostgresCdcSource {
    PostgresCdcSource::new(
        ConnectorId(515),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        CdcWireFormat::PgOutput,
    )
}

async fn do_worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output() {
    let mut first_worker = source();
    first_worker
        .decode_and_enqueue(b"B|0/10|9|I|one|1")
        .unwrap();
    first_worker
        .decode_and_enqueue(b"B|0/20|9|U|one|1|2")
        .unwrap();
    let first = first_worker
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024, 1, None)
        .await
        .unwrap();
    first_worker
        .commit_offset(1, first.new_offset.clone())
        .await
        .unwrap();
    assert_eq!(
        first_worker.last_committed_lsn(),
        Some(PgLsn::parse("0/10").unwrap())
    );

    let mut recovered_worker = source();
    recovered_worker
        .decode_and_enqueue(b"B|0/10|9|I|one|1")
        .unwrap();
    recovered_worker
        .decode_and_enqueue(b"B|0/20|9|U|one|1|2")
        .unwrap();
    let resumed = recovered_worker
        .poll_delta(first.new_offset, 1024, 2, None)
        .await
        .unwrap();
    let (batch, weights) = split_weight_column(&resumed.batches[0]).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(
        (ids, weights, resumed.new_offset.as_bytes().to_vec()),
        (
            vec![1, 2],
            vec![-1, 1],
            PgLsn::parse("0/20")
                .unwrap()
                .to_offset_token()
                .as_bytes()
                .to_vec()
        )
    );
}

#[tokio::test]
async fn worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output() {
    do_worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output().await;
}

#[tokio::test]
async fn queued_pgoutput_lsn_restart_zero_duplicates() {
    do_worker_restart_resumes_committed_lsn_with_exact_keyed_cdc_output().await;
}

#[tokio::test]
async fn pgoutput_snapshot_matches_initial_table_state() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source =
        PostgresCdcSource::new(ConnectorId(515), schema.clone(), CdcWireFormat::PgOutput);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let batch_with_weights =
        rockstream_types::arrow_batch::append_weight_column(batch, &[1, 1]).unwrap();
    source.set_snapshot_batches(vec![batch_with_weights.clone()]);

    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let stream = source.start_snapshot(&fence, None, None).await.unwrap();
    let snapshot_records: Vec<_> = stream.collect();
    assert_eq!(snapshot_records.len(), 1);
    assert_eq!(snapshot_records[0].batch.num_rows(), 2);
}

#[tokio::test]
async fn wal2json_snapshot_matches_initial_table_state() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source =
        PostgresCdcSource::new(ConnectorId(516), schema.clone(), CdcWireFormat::Wal2Json);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let batch_with_weights =
        rockstream_types::arrow_batch::append_weight_column(batch, &[1, 1]).unwrap();
    source.set_snapshot_batches(vec![batch_with_weights.clone()]);

    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let stream = source.start_snapshot(&fence, None, None).await.unwrap();
    let snapshot_records: Vec<_> = stream.collect();
    assert_eq!(snapshot_records.len(), 1);
    assert_eq!(snapshot_records[0].batch.num_rows(), 2);
}

#[tokio::test]
async fn pgoutput_testcontainer_fence_snapshot_live_update_and_commit_are_exact() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC proof"
    );
    let container = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (admin, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    admin
        .batch_execute(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY); \
             ALTER TABLE orders REPLICA IDENTITY FULL; \
             CREATE PUBLICATION orders_pub FOR TABLE orders; \
             INSERT INTO orders VALUES (1);",
        )
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let config = PgOutputConfig {
        host: host.to_string(),
        port,
        database: "postgres".to_string(),
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        slot: "rockstream_pgoutput_test".to_string(),
        publication: "orders_pub".to_string(),
        table: "orders".to_string(),
    };
    let mut source = PostgresCdcSource::connect_pgoutput(ConnectorId(5_201), schema, config)
        .await
        .unwrap();
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let snapshot = source
        .start_snapshot(&fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    let (snapshot_rows, snapshot_weights) = split_weight_column(&snapshot[0].batch).unwrap();
    assert_eq!(
        (
            snapshot_rows
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            snapshot_weights,
        ),
        (vec![1], vec![1])
    );

    admin
        .batch_execute("INSERT INTO orders VALUES (2); UPDATE orders SET id = 3 WHERE id = 1;")
        .await
        .unwrap();
    let delta = source
        .poll_delta(fence.live.clone(), 4_096, 16, None)
        .await
        .unwrap();
    let (delta_rows, delta_weights) = split_weight_column(&delta.batches[0]).unwrap();
    assert_eq!(
        (
            delta_rows
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
            delta_weights,
        ),
        (vec![2, 1, 3], vec![1, -1, 1])
    );
    source
        .commit_offset(7, delta.new_offset.clone())
        .await
        .unwrap();
    assert_eq!(
        source.last_committed_lsn(),
        Some(PgLsn::from_offset_token(&delta.new_offset).unwrap())
    );
    assert!(source
        .poll_delta(delta.new_offset, 4_096, 16, None)
        .await
        .unwrap()
        .batches
        .is_empty());
}

#[test]
fn invalidated_slot_resnapshots_and_slow_subscriber_pauses_before_retention_growth() {
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::SlotInvalidated);
    assert_eq!(
        source.status(),
        &PostgresCdcStatus::Blocked {
            code: "RS-4011",
            reason: "replication slot was invalidated. Next steps: repair PostgreSQL replication settings, then resume the source".to_string(),
        }
    );
    source.begin_resnapshot().unwrap();
    assert_eq!(
        source.status(),
        &PostgresCdcStatus::Resnapshotting { attempt: 1 }
    );
    source.complete_resnapshot();
    source.set_wal_lag_bytes(POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert_eq!(
        (source.wal_lag_bytes(), source.replication_read_paused()),
        (POSTGRES_CDC_MAX_WAL_LAG_BYTES, true)
    );
}

#[tokio::test]
async fn test_snapshot_wal_fence_exact_multiset_handoff() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut source =
        PostgresCdcSource::new(ConnectorId(5_300), schema.clone(), CdcWireFormat::PgOutput);

    // Initial snapshot: keys 1, 2, 3
    let snapshot_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let snapshot_batch_weighted =
        rockstream_types::arrow_batch::append_weight_column(snapshot_batch, &[1, 1, 1]).unwrap();
    source.set_snapshot_batches(vec![snapshot_batch_weighted]);

    // Enqueue pre-fence and at-fence events to simulate WAL prior to cutover
    source.decode_and_enqueue(b"B|0/40|9|I|orders|99").unwrap();
    source.decode_and_enqueue(b"B|0/50|9|I|orders|98").unwrap();

    // Capture fence at consistent point LSN 0/50
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    assert_eq!(fence.live, PgLsn::parse("0/50").unwrap().to_offset_token());

    // Stream snapshot
    let snapshot_stream = source.start_snapshot(&fence, None, None).await.unwrap();
    let snapshot_records = snapshot_stream.collect::<Vec<_>>();
    assert_eq!(snapshot_records.len(), 1);
    let (snap_batch, snap_weights) = split_weight_column(&snapshot_records[0].batch).unwrap();
    let snap_ids = snap_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(snap_ids, vec![1, 2, 3]);
    assert_eq!(snap_weights, vec![1, 1, 1]);

    // Enqueue post-fence events:
    // Insert 4
    source.decode_and_enqueue(b"B|0/60|9|I|orders|4").unwrap();
    // Update 1 to 10 (retract 1, insert 10)
    source
        .decode_and_enqueue(b"B|0/70|9|U|orders|1|10")
        .unwrap();

    // Poll delta starting strictly after fence (0/50)
    let delta = source
        .poll_delta(fence.live.clone(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    assert!(!delta.batches.is_empty(), "expected post-fence deltas");

    let (delta_batch, delta_weights) = split_weight_column(&delta.batches[0]).unwrap();
    let delta_ids = delta_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();

    // Pre-fence events 99 (at 0/40) and 98 (at 0/50) must be discarded!
    assert!(
        !delta_ids.contains(&99),
        "event before fence must be discarded"
    );
    assert!(!delta_ids.contains(&98), "event at fence must be discarded");
    assert_eq!(delta_ids, vec![4, 1, 10]);
    assert_eq!(delta_weights, vec![1, -1, 1]);

    // Compute final maintained multiset:
    // Snapshot: {1: 1, 2: 1, 3: 1}
    // Delta: {4: +1, 1: -1, 10: +1}
    // Result: {1: 0, 2: 1, 3: 1, 4: 1, 10: 1}
    let mut counts: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for (id, w) in snap_ids.iter().zip(snap_weights.iter()) {
        *counts.entry(*id).or_default() += *w;
    }
    for (id, w) in delta_ids.iter().zip(delta_weights.iter()) {
        *counts.entry(*id).or_default() += *w;
    }

    assert_eq!(
        counts.get(&1).copied().unwrap_or(0),
        0,
        "id 1 must be retracted"
    );
    assert_eq!(counts.get(&2).copied().unwrap_or(0), 1, "id 2 must exist");
    assert_eq!(counts.get(&3).copied().unwrap_or(0), 1, "id 3 must exist");
    assert_eq!(counts.get(&4).copied().unwrap_or(0), 1, "id 4 must exist");
    assert_eq!(counts.get(&10).copied().unwrap_or(0), 1, "id 10 must exist");

    // Commit delta offset (0/70)
    source
        .commit_offset(1, delta.new_offset.clone())
        .await
        .unwrap();
    assert_eq!(
        source.last_committed_lsn(),
        Some(PgLsn::parse("0/70").unwrap())
    );

    // Crash recovery check: recreate worker from committed offset
    let mut recovered =
        PostgresCdcSource::new(ConnectorId(5_300), schema.clone(), CdcWireFormat::PgOutput);
    // Replay same stream including duplicate events
    recovered
        .decode_and_enqueue(b"B|0/60|9|I|orders|4")
        .unwrap();
    recovered
        .decode_and_enqueue(b"B|0/70|9|U|orders|1|10")
        .unwrap();
    // Later event at 0/80
    recovered
        .decode_and_enqueue(b"B|0/80|9|I|orders|5")
        .unwrap();

    let recovered_delta = recovered
        .poll_delta(delta.new_offset.clone(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    let (rec_batch, rec_weights) = split_weight_column(&recovered_delta.batches[0]).unwrap();
    let rec_ids = rec_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    // Zero duplicate rows: only row 5 at 0/80 is emitted!
    assert_eq!(rec_ids, vec![5]);
    assert_eq!(rec_weights, vec![1]);
}
