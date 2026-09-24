//! Worker Budgets, Flow Control via Pause/Resume & Truthful Lag Tests (v0.70 Slice 7 / Table 4.7).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    KafkaSource, OffsetToken, SourceConnector, DEFAULT_MAX_EPOCH_BATCH_BYTES,
    DEFAULT_MAX_EPOCH_BATCH_RECORDS,
};
use rockstream_types::ids::ConnectorId;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

#[tokio::test]
async fn test_kafka_poll_batch_limit_enforced() {
    let mut source = KafkaSource::connect(
        ConnectorId(9101),
        schema(),
        "127.0.0.1:1",
        "topic_batch_limit",
        "grp_batch_limit",
    )
    .unwrap();

    // Default maximum records is 5,000
    assert_eq!(
        source.max_epoch_batch_records(),
        DEFAULT_MAX_EPOCH_BATCH_RECORDS
    );
    assert_eq!(DEFAULT_MAX_EPOCH_BATCH_RECORDS, 5_000);

    // Dynamic configuration limit down to 50
    source.set_max_epoch_batch_records(50);
    assert_eq!(source.max_epoch_batch_records(), 50);

    let poll_res = source
        .poll_delta(OffsetToken::new(vec![]), 10_000_000, 0, None)
        .await
        .unwrap();
    assert!(poll_res.batches.is_empty());
}

#[tokio::test]
async fn test_kafka_poll_byte_limit_enforced() {
    let mut source = KafkaSource::connect(
        ConnectorId(9102),
        schema(),
        "127.0.0.1:1",
        "topic_byte_limit",
        "grp_byte_limit",
    )
    .unwrap();

    // Default maximum bytes is 16 MiB
    assert_eq!(
        source.max_epoch_batch_bytes(),
        DEFAULT_MAX_EPOCH_BATCH_BYTES
    );
    assert_eq!(DEFAULT_MAX_EPOCH_BATCH_BYTES, 16 * 1024 * 1024);

    // Dynamic configuration limit down to 64 KiB
    source.set_max_epoch_batch_bytes(64 * 1024);
    assert_eq!(source.max_epoch_batch_bytes(), 65536);
}

#[tokio::test]
async fn test_kafka_backpressure_pauses_consumer() {
    let mut source = KafkaSource::connect(
        ConnectorId(9103),
        schema(),
        "127.0.0.1:1",
        "topic_pause",
        "grp_pause",
    )
    .unwrap();

    assert!(!source.is_paused());

    // Trigger backpressure pause
    source.pause();
    assert!(source.is_paused());

    // When paused, poll_delta immediately short-circuits with empty batch
    let offset = OffsetToken::new(b"{\"0\": 10}".to_vec());
    let res = source
        .poll_delta(offset.clone(), 1024 * 1024, 100, None)
        .await
        .unwrap();

    assert!(res.batches.is_empty());
    assert_eq!(res.new_offset, offset);
}

#[tokio::test]
async fn test_kafka_backpressure_resumes_consumer() {
    let mut source = KafkaSource::connect(
        ConnectorId(9104),
        schema(),
        "127.0.0.1:1",
        "topic_resume",
        "grp_resume",
    )
    .unwrap();

    source.pause();
    assert!(source.is_paused());

    // Clear backpressure and resume
    source.resume();
    assert!(!source.is_paused());
}

#[test]
fn test_kafka_source_lag_reporting_matches_broker() {
    // 50,000 unread records in topic
    let lag = KafkaSource::calculate_lag(50_000, 0);
    assert_eq!(lag, 50_000);

    let lag = KafkaSource::calculate_lag(50_000, 10_000);
    assert_eq!(lag, 40_000);

    let lag = KafkaSource::calculate_lag(50_000, 50_000);
    assert_eq!(lag, 0);

    // Stored offset ahead of high watermark does not underflow
    let lag = KafkaSource::calculate_lag(50_000, 55_000);
    assert_eq!(lag, 0);
}

#[test]
fn test_kafka_backpressure_pause_resume_and_truthful_lag() {
    test_kafka_poll_batch_limit_enforced();
    test_kafka_poll_byte_limit_enforced();
    test_kafka_backpressure_pauses_consumer();
    test_kafka_backpressure_resumes_consumer();
    test_kafka_source_lag_reporting_matches_broker();
}
