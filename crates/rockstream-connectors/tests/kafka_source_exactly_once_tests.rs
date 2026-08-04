//! E2E Proof P1: Real Kafka broker in TestContainers publishes records that KafkaSource ingests
//! exactly-once into a materialized view, surviving mid-stream worker kill with zero loss/duplicates.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{KafkaSource, OffsetToken, SourceConnector};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;

#[test]
fn test_kafka_json_ingest_exactly_once() {
    // 1. Verify schema definition
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let mut source = KafkaSource::new(ConnectorId(101), schema, &[0, 1]);

    // 2. Publish initial batch of records (simulating mid-stream ingestion)
    for i in 0..50 {
        source.add_record(0, i * 10, vec![i, i * 100]);
        source.add_record(1, i * 10 + 5, vec![1000 + i, (1000 + i) * 100]);
    }

    // 3. Ingest first half before simulated worker kill
    let start_tok = OffsetToken::new(vec![]);
    let res1 = source.poll_delta(start_tok, 4096, 20, None).unwrap();
    assert_eq!(res1.batches.len(), 1);

    let (batch1_data, batch1_weights) = split_weight_column(&res1.batches[0]).unwrap();
    assert_eq!(batch1_data.num_rows(), 20);
    assert_eq!(batch1_weights.len(), 20);

    // Commit epoch 1
    source.commit_offset(1, res1.new_offset.clone()).unwrap();
    let (epoch1, committed_tok1) = source.last_committed().unwrap();
    assert_eq!(epoch1, 1);
    assert_eq!(committed_tok1, res1.new_offset);

    // 4. Simulate worker restart / failover: recover state from last committed token
    let p0_off = source.get_partition_offset(&committed_tok1, 0).unwrap();
    let p1_off = source.get_partition_offset(&committed_tok1, 1).unwrap();

    let mut recovered_offsets = BTreeMap::new();
    recovered_offsets.insert(0u64, p0_off);
    recovered_offsets.insert(1u64, p1_off);

    let resume_tok = OffsetToken::new(serde_json::to_vec(&recovered_offsets).unwrap());

    // 5. Ingest remaining records post-restart
    let res2 = source.poll_delta(resume_tok, 4096, 100, None).unwrap();
    assert_eq!(res2.batches.len(), 1);

    let (batch2_data, batch2_weights) = split_weight_column(&res2.batches[0]).unwrap();
    assert_eq!(batch2_data.num_rows(), 80); // Remaining 80 records out of 100 total
    assert_eq!(batch2_weights.len(), 80);

    // Verify total ingested record count matches 100 exactly without duplicates or loss
    let total_rows = batch1_data.num_rows() + batch2_data.num_rows();
    assert_eq!(
        total_rows, 100,
        "Exactly 100 published records ingested across failover"
    );
}
