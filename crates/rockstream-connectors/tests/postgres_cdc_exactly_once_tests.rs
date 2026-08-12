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
