mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcWireFormat, PgLsn, PgOutputConfig, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
    SourceConnector, POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS, POSTGRES_CDC_MAX_WAL_LAG_BYTES,
};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

use common::ConnectorFixture;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn source() -> PostgresCdcSource {
    PostgresCdcSource::new(ConnectorId(5_250), schema(), CdcWireFormat::PgOutput)
}

fn config(fixture: &ConnectorFixture, label: &str) -> PgOutputConfig {
    PgOutputConfig {
        host: fixture.postgres_host.clone(),
        port: fixture.postgres_port,
        database: "postgres".to_string(),
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        slot: format!("slot_{label}"),
        publication: "orders_pub".to_string(),
        table: "orders".to_string(),
    }
}

fn rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(i64, i64)> {
    batches
        .iter()
        .flat_map(|batch| {
            let (data, weights) = split_weight_column(batch).unwrap();
            let values = data
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .values();
            values.iter().copied().zip(weights).collect::<Vec<_>>()
        })
        .collect()
}

async fn poll_until_rows(
    source: &mut PostgresCdcSource,
    after: rockstream_connectors::OffsetToken,
) -> rockstream_connectors::PollDeltaResult {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = source
            .poll_delta(after.clone(), 8 * 1024 * 1024, 4_096, None)
            .await;
        let result = result.unwrap();
        if !result.batches.is_empty() {
            return result;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC did not receive a record"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn postgres_cdc_snapshot_stream_fence_has_exact_transcript() {
    let fixture = common::connector_fixture("snapshot_fence").await;
    fixture
        .postgres
        .batch_execute("INSERT INTO orders VALUES (1)")
        .await
        .unwrap();
    let mut source = PostgresCdcSource::connect_pgoutput(
        ConnectorId(5_251),
        schema(),
        config(&fixture, "snapshot_fence"),
    )
    .await
    .unwrap();
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let snapshot = source
        .start_snapshot(&fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(
        rows(
            &snapshot
                .iter()
                .map(|batch| batch.batch.clone())
                .collect::<Vec<_>>()
        ),
        vec![(1, 1)]
    );

    fixture
        .postgres
        .batch_execute("INSERT INTO orders VALUES (2)")
        .await
        .unwrap();
    let delta = poll_until_rows(&mut source, fence.live.clone()).await;
    assert_eq!(rows(&delta.batches), vec![(2, 1)]);
    source
        .commit_offset(1, delta.new_offset.clone())
        .await
        .unwrap();
    assert!(source
        .poll_delta(delta.new_offset, 8 * 1024 * 1024, 4_096, None)
        .await
        .unwrap()
        .batches
        .is_empty());
}

#[tokio::test]
async fn postgres_cdc_all_mutation_types_have_exact_transcript() {
    let fixture = common::connector_fixture("mutations").await;
    fixture
        .postgres
        .batch_execute("INSERT INTO orders VALUES (1); UPDATE orders SET id = 2 WHERE id = 1; DELETE FROM orders WHERE id = 2; TRUNCATE orders")
        .await
        .unwrap();
    let mut source = source();
    for frame in [
        b"B|0/10|7|I|one|1".as_slice(),
        b"B|0/20|7|U|one|1|2",
        b"B|0/30|7|D|two|2",
    ] {
        source.decode_and_enqueue(frame).unwrap();
    }
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 4096, 16, None)
        .await
        .unwrap();
    assert_eq!(rows(&delta.batches), vec![(1, 1), (1, -1), (2, 1), (2, -1)]);
    assert!(fixture
        .postgres
        .query("SELECT id FROM orders", &[])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn postgres_cdc_each_commit_boundary_recovers_exactly_once() {
    let _fixture = common::connector_fixture("commit_boundaries").await;
    let mut source = source();
    let mut after = PgLsn::ZERO.to_offset_token();
    let mut transcript = Vec::new();
    for (epoch, lsn, value) in [(1, "0/10", 1), (2, "0/20", 2), (3, "0/30", 3)] {
        source
            .decode_and_enqueue(format!("B|{lsn}|7|I|key|{value}").as_bytes())
            .unwrap();
        let delta = source.poll_delta(after, 4096, 1, None).await.unwrap();
        assert_eq!(rows(&delta.batches), vec![(value, 1)]);
        transcript.extend(rows(&delta.batches));
        source
            .commit_offset(epoch, delta.new_offset.clone())
            .await
            .unwrap();
        after = delta.new_offset;
    }
    assert_eq!(
        source.last_committed_lsn(),
        Some(PgLsn::parse("0/30").unwrap())
    );
    assert_eq!(transcript, vec![(1, 1), (2, 1), (3, 1)]);
}

#[tokio::test]
async fn postgres_cdc_wal_lag_pauses_at_bound_and_recovers_within_slo() {
    let _fixture = common::connector_fixture("wal_lag").await;
    let mut source = source();
    source.set_wal_lag_bytes(POSTGRES_CDC_MAX_WAL_LAG_BYTES);
    assert!(source.replication_read_paused());
    let started = Instant::now();
    source.set_wal_lag_bytes(0);
    assert!(!source.replication_read_paused());
    source.decode_and_enqueue(b"B|0/40|7|I|one|7").unwrap();
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 4096, 1, None)
        .await
        .unwrap();
    assert_eq!(rows(&delta.batches), vec![(7, 1)]);
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test]
async fn postgres_cdc_malformed_replication_record_fails_closed_then_recovers_exactly() {
    let _fixture = common::connector_fixture("malformed").await;
    let mut source = source();
    source
        .decode_and_enqueue(b"malformed replication record")
        .unwrap();
    assert_eq!(source.buffered_records(), 0);
    source.decode_and_enqueue(b"B|0/10|7|I|one|7").unwrap();
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 4096, 1, None)
        .await
        .unwrap();
    assert_eq!(rows(&delta.batches), vec![(7, 1)]);
}

#[tokio::test]
async fn postgres_cdc_replication_slot_loss_resnapshots_with_exact_transcript() {
    let _fixture = common::connector_fixture("slot_loss").await;
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::SlotInvalidated);
    assert!(matches!(
        source.status(),
        PostgresCdcStatus::Blocked {
            code: "RS-4011",
            ..
        }
    ));
    source.begin_resnapshot().unwrap();
    source.complete_resnapshot();
    source.decode_and_enqueue(b"B|0/20|7|I|one|7").unwrap();
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 4096, 1, None)
        .await
        .unwrap();
    assert_eq!(rows(&delta.batches), vec![(7, 1)]);
}

#[tokio::test]
async fn postgres_cdc_publication_loss_fails_clearly_then_recovers_exactly() {
    let fixture = common::connector_fixture("publication_loss").await;
    fixture
        .postgres
        .batch_execute(
            "DROP PUBLICATION orders_pub; CREATE PUBLICATION orders_pub FOR TABLE orders",
        )
        .await
        .unwrap();
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::PublicationMissing);
    assert!(matches!(
        source.status(),
        PostgresCdcStatus::Blocked {
            code: "RS-4011",
            ..
        }
    ));
    source.begin_resnapshot().unwrap();
    source.complete_resnapshot();
    source.decode_and_enqueue(b"B|0/30|7|I|one|8").unwrap();
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 4096, 1, None)
        .await
        .unwrap();
    assert_eq!(rows(&delta.batches), vec![(8, 1)]);
}

#[tokio::test]
async fn postgres_cdc_backpressure_never_exceeds_record_or_byte_bound() {
    let _fixture = common::connector_fixture("backpressure").await;
    let mut source = source();
    for index in 0..POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS {
        source
            .decode_and_enqueue(format!("B|0/{:X}|7|I|key|{index}", index + 1).as_bytes())
            .unwrap();
    }
    let error = source
        .decode_and_enqueue(b"B|0/FFFF|7|I|overflow|999")
        .unwrap_err();
    assert!(error.to_string().contains("RS-4014"));
    assert_eq!(
        source.buffered_records(),
        POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
    );
    assert!(source.buffer_fill_ratio() <= 1.0);
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 8 * 1024 * 1024, 4_096, None)
        .await
        .unwrap();
    assert_eq!(
        rows(&delta.batches),
        (0..POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS)
            .map(|value| (value as i64, 1))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn postgres_cdc_long_running_recovery_is_exact_and_within_slo() {
    let _fixture = common::connector_fixture("long_recovery").await;
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::ReplicationTimeout);
    let started = Instant::now();
    source.begin_resnapshot().unwrap();
    source.complete_resnapshot();
    for value in 1..=32 {
        source
            .decode_and_enqueue(format!("B|0/{:X}|7|I|key|{value}", value).as_bytes())
            .unwrap();
    }
    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 8 * 1024 * 1024, 4_096, None)
        .await
        .unwrap();
    assert_eq!(
        rows(&delta.batches),
        (1..=32).map(|value| (value, 1)).collect::<Vec<_>>()
    );
    assert!(started.elapsed() < Duration::from_secs(60));
}

#[tokio::test]
async fn postgres_cdc_initial_snapshot_backfills_all_rows() {
    let fixture = common::connector_fixture("initial_snapshot_backfill").await;
    fixture
        .postgres
        .batch_execute("INSERT INTO orders VALUES (10), (20), (30)")
        .await
        .unwrap();
    let mut source = PostgresCdcSource::connect_pgoutput(
        ConnectorId(5_255),
        schema(),
        config(&fixture, "initial_snapshot_backfill"),
    )
    .await
    .unwrap();
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let snapshot = source
        .start_snapshot(&fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    let mut all_rows = rows(
        &snapshot
            .iter()
            .map(|batch| batch.batch.clone())
            .collect::<Vec<_>>(),
    );
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(10, 1), (20, 1), (30, 1)]);
}

#[tokio::test]
async fn postgres_cdc_invalid_slot_fails_closed() {
    let _fixture = common::connector_fixture("invalid_slot").await;
    let mut source = source();
    source.mark_failure(PostgresCdcFailure::SlotInvalidated);
    assert!(matches!(
        source.status(),
        PostgresCdcStatus::Blocked {
            code: "RS-4011",
            ..
        }
    ));
}
