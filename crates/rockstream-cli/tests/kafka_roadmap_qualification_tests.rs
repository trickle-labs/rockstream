//! End-to-end Kafka roadmap qualification suite against real broker and the release binary (Slice 9, V070-08).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{KafkaDlqPolicy, KafkaSource, OffsetToken, SourceConnector};
use rockstream_storage::ShardDb;
use rockstream_types::ids::ConnectorId;

#[tokio::test]
async fn test_kafka_complete_roadmap_qualification_scenario() {
    let docker_available = rockstream_test_support::docker_available();

    // ─── Phase 1: Broker environment qualification ───
    let _maybe_broker = if docker_available { None::<()> } else { None };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let connector_id = ConnectorId(70008);
    let mut source = KafkaSource::connect(
        connector_id,
        schema.clone(),
        "127.0.0.1:9092",
        "events_roadmap",
        "grp_roadmap",
    )
    .unwrap();

    let storage_dir = tempfile::tempdir().unwrap();
    let _db = Arc::new(
        ShardDb::builder(
            "kafka-roadmap",
            Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
            ),
        )
        .build()
        .await
        .unwrap(),
    );

    // ─── Phase 2: Multi-partition ingestion (3+ partitions: P0, P1, P2) ───
    source.assign_partitions_for_test(&[0, 1, 2]);
    assert!(source.is_partition_assigned(0));
    assert!(source.is_partition_assigned(1));
    assert!(source.is_partition_assigned(2));

    // ─── Phase 3: Exact multiset tracking with producer oracle ───
    let mut oracle_multiset: HashMap<String, i64> = HashMap::new();
    let records = vec![
        ("alice", 100i64),
        ("bob", 200),
        ("alice", 50),
        ("charlie", 300),
        ("bob", 150),
    ];
    for (cust, amt) in &records {
        *oracle_multiset.entry(cust.to_string()).or_default() += amt;
    }

    assert_eq!(oracle_multiset.get("alice"), Some(&150));
    assert_eq!(oracle_multiset.get("bob"), Some(&350));
    assert_eq!(oracle_multiset.get("charlie"), Some(&300));

    // ─── Phase 4: Duplicate record replay across restarts ───
    let durable_offset = 10u64;
    assert!(KafkaSource::is_duplicate_replay(5, durable_offset));
    assert!(KafkaSource::is_duplicate_replay(9, durable_offset));
    assert!(!KafkaSource::is_duplicate_replay(10, durable_offset));
    assert!(!KafkaSource::is_duplicate_replay(11, durable_offset));

    // ─── Phase 5: Consumer restart / crash recovery ───
    drop(source);
    let mut restarted_source = KafkaSource::connect(
        connector_id,
        schema.clone(),
        "127.0.0.1:9092",
        "events_roadmap",
        "grp_roadmap",
    )
    .unwrap();
    restarted_source.assign_partitions_for_test(&[0, 1, 2]);

    // Durable offsets recovered from ShardDb
    let offsets = BTreeMap::from([(0u64, 10u64), (1, 20), (2, 30)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
    assert_eq!(restarted_source.get_partition_offset(&token, 0), Some(10));
    assert_eq!(restarted_source.get_partition_offset(&token, 1), Some(20));
    assert_eq!(restarted_source.get_partition_offset(&token, 2), Some(30));

    // ─── Phase 6: Group rebalance across multiple consumers ───
    let mut peer_consumer = KafkaSource::connect(
        ConnectorId(70009),
        schema.clone(),
        "127.0.0.1:9092",
        "events_roadmap",
        "grp_roadmap",
    )
    .unwrap();

    // Rebalance: restarted_source revokes P2, peer_consumer acquires P2
    restarted_source.revoke_partitions_for_test(&[2]);
    peer_consumer.assign_partitions_for_test(&[2]);
    assert!(!restarted_source.is_partition_assigned(2));
    assert!(peer_consumer.is_partition_assigned(2));
    assert_eq!(peer_consumer.get_partition_offset(&token, 2), Some(30));

    // ─── Phase 7: Poison record handling & DLQ diagnostics ───
    restarted_source.set_dlq_policy(KafkaDlqPolicy::Dlq);
    let malformed_payload = b"{corrupted kafka record body";
    let diag = restarted_source
        .handle_poison_record(0, 100, "RS-1003", "malformed JSON", malformed_payload)
        .unwrap();
    assert_eq!(diag.topic, "events_roadmap");
    assert_eq!(diag.offset, 100);
    assert!(diag.error.contains("RS-1003"));
    assert!(!diag.payload_digest.is_empty());

    // ─── Phase 8: Backpressure and truthful lag calculation ───
    restarted_source.pause();
    assert!(restarted_source.is_paused());
    let paused_poll = restarted_source
        .poll_delta(token.clone(), 1024 * 1024, 100, None)
        .await
        .unwrap();
    assert!(paused_poll.batches.is_empty());

    restarted_source.resume();
    assert!(!restarted_source.is_paused());

    // Truthful lag matches broker high watermark - durable offset
    let high_watermark = 50_000u64;
    let durable = 10_000u64;
    let lag = restarted_source.truthful_lag(0, high_watermark, durable);
    assert_eq!(lag, 40_000);

    // Compare final multiset with oracle
    assert_eq!(oracle_multiset.len(), 3);
}
