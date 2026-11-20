//! PostgreSQL-backed regression for #349: validate the actual pgoutput event
//! poll, independent SQL transcript sizes, and the source/slot ACK barrier.
mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    PgLsn, PgOutputColumn, PgOutputConfig, PgOutputEvent, PgOutputRelationMetadata,
    PostgresCdcSource, SourceConnector, POSTGRES_CDC_MAX_TRANSACTION_BYTES,
};
use rockstream_types::ids::ConnectorId;
use tokio_postgres::Client;

use common::ConnectorFixture;

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn text_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn config(fixture: &ConnectorFixture, slot: &str, table: &str) -> PgOutputConfig {
    PgOutputConfig {
        host: fixture.postgres_host.clone(),
        port: fixture.postgres_port,
        database: "postgres".into(),
        user: "postgres".into(),
        password: Some("postgres".into()),
        slot: slot.into(),
        publication: "orders_pub".into(),
        table: table.into(),
    }
}

/// Expected values are computed by PostgreSQL separately from the source's
/// decoder. These aggregate SQL queries return only counts, total bytes,
/// maximum frame length and the COMMIT end LSN, not the whole transcript.
async fn pg_transcript_sizes(client: &Client, slot: &str) -> (u64, u64, usize, PgLsn) {
    let row = client
        .query_one(
            "SELECT count(*), coalesce(sum(octet_length(data)), 0)::bigint, coalesce(max(octet_length(data)), 0)::integer FROM pg_logical_slot_peek_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', 'orders_pub')",
            &[&slot],
        )
        .await
        .unwrap();
    let count: i64 = row.get(0);
    let bytes: i64 = row.get(1);
    let max_frame: i32 = row.get(2);
    let row = client
        .query_one(
            "SELECT substring(data FROM 11 FOR 8) FROM pg_logical_slot_peek_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', 'orders_pub') WHERE get_byte(data, 0) = 67",
            &[&slot],
        )
        .await
        .unwrap();
    let end_lsn: Vec<u8> = row.get(0);
    (
        count as u64,
        bytes as u64,
        max_frame as usize,
        PgLsn(u64::from_be_bytes(end_lsn.try_into().unwrap())),
    )
}

async fn first_transaction(source: &mut PostgresCdcSource) -> Vec<PgOutputEvent> {
    tokio::time::timeout(Duration::from_secs(45), async {
        let mut events = Vec::new();
        loop {
            match source.poll_pgoutput_event(1).await.unwrap() {
                Some(event) => {
                    let is_commit = matches!(event, PgOutputEvent::Commit { .. });
                    events.push(event);
                    if is_commit {
                        return events;
                    }
                }
                None => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("pgoutput transaction did not complete")
}

#[tokio::test]
async fn pgoutput_bounded_copy_preserves_complete_ordered_thousand_row_transcript_and_ack() {
    let fixture = common::connector_fixture("bounded_copy_order").await;
    let mut source = PostgresCdcSource::connect_pgoutput(
        ConnectorId(34901),
        id_schema(),
        config(&fixture, "slot_bounded_copy_order", "orders"),
        false,
    )
    .await
    .unwrap();
    source.capture_snapshot_delta_fence(None).await.unwrap();

    fixture.postgres.batch_execute("BEGIN").await.unwrap();
    let xid = fixture
        .postgres
        .query_one("SELECT txid_current()", &[])
        .await
        .unwrap()
        .get::<_, i64>(0) as u32;
    fixture
        .postgres
        .batch_execute("INSERT INTO orders SELECT generate_series(1, 2500); COMMIT")
        .await
        .unwrap();
    let (frame_count, byte_count, max_frame, end_lsn) =
        pg_transcript_sizes(&fixture.postgres, "slot_bounded_copy_order").await;
    let relation_id = fixture
        .postgres
        .query_one("SELECT 'orders'::regclass::oid::bigint", &[])
        .await
        .unwrap()
        .get::<_, i64>(0) as u32;

    let mut expected = vec![
        PgOutputEvent::Begin { xid },
        PgOutputEvent::Relation {
            xid,
            relation: PgOutputRelationMetadata {
                relation_id,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: b'f',
                columns: vec![PgOutputColumn {
                    flags: 1,
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                }],
            },
        },
    ];
    expected.extend((1..=2500).map(|value| PgOutputEvent::Insert {
        xid,
        relation_id,
        new_values: vec![Some(value.to_string())],
    }));
    expected.push(PgOutputEvent::Commit {
        xid,
        commit_lsn: end_lsn,
    });

    let actual = first_transaction(&mut source).await;
    assert_eq!(actual, expected, "full ordered pgoutput transcript");
    let transport = source.pgoutput_transport_stats();
    assert_eq!(transport.peek_queries, 1, "never replay a transaction prefix");
    assert_eq!(transport.frames_received, frame_count);
    assert_eq!(transport.payload_bytes_received, byte_count);
    assert_eq!(frame_count, expected.len() as u64);
    assert!(transport.peak_chunk_bytes <= 8 * 1024);
    assert_eq!(
        transport.peak_frame_buffered_bytes,
        max_frame,
        "peak buffered payload must match PostgreSQL's measured largest frame"
    );
    assert!(transport.peak_frame_buffered_bytes <= POSTGRES_CDC_MAX_TRANSACTION_BYTES);
    assert!(transport.peak_transaction_payload_bytes <= POSTGRES_CDC_MAX_TRANSACTION_BYTES);

    // Only after the consumer commits does the source consume the slot.
    let before_ack = source.confirmed_flush_lsn().await.unwrap();
    source
        .commit_offset(1, end_lsn.to_offset_token())
        .await
        .unwrap();
    assert!(source.confirmed_flush_lsn().await.unwrap() > before_ack);
    assert_eq!(source.last_committed_lsn(), Some(end_lsn));
    assert_eq!(source.quad_lsn_progress().confirmed_flush_lsn, end_lsn);
    let remaining: i64 = fixture
        .postgres
        .query_one(
            "SELECT count(*) FROM pg_logical_slot_peek_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', 'orders_pub')",
            &[&"slot_bounded_copy_order"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0, "the committed transaction was acknowledged exactly once");
}

#[tokio::test]
async fn pgoutput_copy_rejects_oversized_frame_before_buffer_or_checkpoint_advances() {
    let fixture = common::connector_fixture("bounded_copy_overflow").await;
    fixture
        .postgres
        .batch_execute(
            "CREATE TABLE big_orders (id BIGINT PRIMARY KEY, payload TEXT NOT NULL); \
             ALTER TABLE big_orders REPLICA IDENTITY FULL; \
             ALTER PUBLICATION orders_pub ADD TABLE big_orders",
        )
        .await
        .unwrap();
    let mut source = PostgresCdcSource::connect_pgoutput(
        ConnectorId(34902),
        text_schema(),
        config(&fixture, "slot_bounded_copy_overflow", "big_orders"),
        false,
    )
    .await
    .unwrap();
    source.capture_snapshot_delta_fence(None).await.unwrap();
    let before_ack = source.confirmed_flush_lsn().await.unwrap();

    // TOAST compression does not shrink pgoutput's decoded text frame.
    fixture
        .postgres
        .execute(
            "INSERT INTO big_orders VALUES (1, repeat('x', $1))",
            &[&((POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1024) as i32)],
        )
        .await
        .unwrap();
    let (full_frames, full_bytes, max_frame, end_lsn) =
        pg_transcript_sizes(&fixture.postgres, "slot_bounded_copy_overflow").await;
    assert!(full_bytes > POSTGRES_CDC_MAX_TRANSACTION_BYTES as u64);
    assert!(max_frame > POSTGRES_CDC_MAX_TRANSACTION_BYTES);

    let (received, error) = tokio::time::timeout(Duration::from_secs(45), async {
        let mut received = Vec::new();
        loop {
            match source.poll_pgoutput_event(1).await {
                Ok(Some(event)) => {
                    assert!(!matches!(event, PgOutputEvent::Commit { .. }));
                    received.push(event);
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(25)).await,
                Err(error) => return (received, error),
            }
        }
    })
    .await
    .expect("oversized pgoutput transaction did not reject");
    assert_eq!(received.len(), 2, "BEGIN and Relation precede oversized INSERT");
    assert!(matches!(received[0], PgOutputEvent::Begin { .. }));
    assert!(matches!(received[1], PgOutputEvent::Relation { .. }));
    assert!(error.to_string().contains("RS-4014"), "{error}");

    let transport = source.pgoutput_transport_stats();
    assert_eq!(transport.peek_queries, 1);
    assert_eq!(transport.frames_received, 2);
    assert!(transport.frames_received < full_frames);
    assert!(transport.payload_bytes_received < full_bytes);
    assert!(transport.peak_chunk_bytes <= 8 * 1024);
    assert!(transport.peak_frame_buffered_bytes <= POSTGRES_CDC_MAX_TRANSACTION_BYTES);
    assert!(
        transport.peak_frame_buffered_bytes < 1024,
        "oversized frame was rejected from its header"
    );
    assert_eq!(source.last_committed_lsn(), None);
    assert_eq!(source.quad_lsn_progress().durable_lsn, PgLsn::ZERO);
    assert_eq!(source.confirmed_flush_lsn().await.unwrap(), before_ack);
    assert!(source.replication_read_paused());

    // An erroneous retry or direct attempt to ACK cannot skip the rejected
    // frame, and must not issue another peek or GET on the PostgreSQL slot.
    assert!(source.poll_pgoutput_event(1).await.unwrap_err().to_string().contains("RS-4014"));
    assert!(source.commit_offset(1, end_lsn.to_offset_token()).await.is_err());
    assert_eq!(source.pgoutput_transport_stats().peek_queries, 1);
    assert_eq!(source.confirmed_flush_lsn().await.unwrap(), before_ack);

    // Operator recovery: repair the upstream row, then take a fresh
    // snapshot with a replacement slot (never reuse the rejected checkpoint).
    source.close_pgoutput();
    fixture
        .postgres
        .batch_execute("UPDATE big_orders SET payload = 'repaired'")
        .await
        .unwrap();
    let mut recovered = PostgresCdcSource::connect_pgoutput(
        ConnectorId(34903),
        text_schema(),
        config(&fixture, "slot_bounded_copy_recovered", "big_orders"),
        false,
    )
    .await
    .unwrap();
    let fence = recovered.capture_snapshot_delta_fence(None).await.unwrap();
    let batches = recovered
        .start_snapshot(&fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    let data = &batches[0].batch;
    let ids = data.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    let payloads = data.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(ids.values().as_ref(), &[1]);
    assert_eq!(payloads.value(0), "repaired");
}
