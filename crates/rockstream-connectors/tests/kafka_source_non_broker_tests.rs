use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{KafkaSource, OffsetToken, SourceConnector};
use rockstream_types::ids::ConnectorId;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

#[test]
fn incomplete_kafka_configuration_is_rejected_before_client_creation() {
    let error = match KafkaSource::connect(ConnectorId(5127), schema(), "", "topic", "group") {
        Ok(_) => panic!("incomplete Kafka configuration must fail"),
        Err(error) => error,
    };

    assert_eq!(
        error.to_string(),
        "RS-4001: source I/O error: Kafka configuration is incomplete. Next steps: provide bootstrap servers, topic, and group id"
    );
}

#[tokio::test]
async fn broker_free_fast_paths_preserve_offsets_and_reject_invalid_commits() {
    let mut source =
        KafkaSource::connect(ConnectorId(5128), schema(), "127.0.0.1:1", "topic", "group").unwrap();
    let token = OffsetToken::new(serde_json::to_vec(&BTreeMap::from([(3, 7)])).unwrap());
    let zero_bytes = source.poll_delta(token.clone(), 0, 1, None).await.unwrap();
    let zero_credits = source
        .poll_delta(token.clone(), 1024, 0, None)
        .await
        .unwrap();
    let invalid_commit = source
        .commit_offset(4, OffsetToken::new(br#"{"2147483648":0}"#.to_vec()))
        .await
        .unwrap_err();

    assert_eq!(
        (
            source.discover_schema().unwrap().fields().len(),
            source.start_snapshot(0, None).await.unwrap().count(),
            source.get_partition_offset(&OffsetToken::new(vec![]), 3),
            source.get_partition_offset(&token, 3),
            source.get_partition_offset(&token, 4),
            source.get_partition_offset(&OffsetToken::new(b"not-json".to_vec()), 3),
            (
                zero_bytes.batches.len(),
                zero_bytes.new_offset,
                zero_bytes.watermark,
            ),
            (
                zero_credits.batches.len(),
                zero_credits.new_offset,
                zero_credits.watermark,
            ),
            invalid_commit.to_string(),
            source.last_committed(),
            source.assigned_partition_count(),
            source.last_poll_fill_level(),
        ),
        (
            1,
            0,
            Some(0),
            Some(7),
            Some(0),
            None,
            (0, token.clone(), None),
            (0, token, None),
            "RS-4001: source commit offset failed for epoch 4: Kafka partition exceeds i32. Next steps: commit a valid source token".to_string(),
            None,
            0,
            0,
        )
    );
}
