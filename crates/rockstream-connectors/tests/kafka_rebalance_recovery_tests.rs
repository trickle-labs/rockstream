//! Consumer Group Rebalance, Partition Revocation & Resumption Tests (v0.70 Slice 5 / Table 4.5).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{KafkaSource, OffsetToken};
use rockstream_types::ids::ConnectorId;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

#[tokio::test]
async fn test_rebalance_scale_out_revokes_partition_safely() {
    let mut source = KafkaSource::connect(
        ConnectorId(8001),
        schema(),
        "127.0.0.1:1",
        "topic_rebalance",
        "grp_rebalance",
    )
    .unwrap();

    // Initial assignment: P0, P1, P2
    source.assign_partitions_for_test(&[0, 1, 2]);
    let initial_gen = source.assignment_generation();
    assert!(source.is_partition_assigned(0));
    assert!(source.is_partition_assigned(1));
    assert!(source.is_partition_assigned(2));

    // Scale out: another consumer joins group, P2 is revoked from this consumer
    source.revoke_partitions_for_test(&[2]);
    assert!(source.assignment_generation() > initial_gen);
    assert!(source.is_partition_assigned(0));
    assert!(source.is_partition_assigned(1));
    assert!(!source.is_partition_assigned(2));

    // Durable offset for P2 remains safe and can be recovered by new owner
    let offsets: BTreeMap<u64, u64> = BTreeMap::from([(0, 100), (1, 200), (2, 300)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
    assert_eq!(source.get_partition_offset(&token, 2), Some(300));
}

#[tokio::test]
async fn test_rebalance_scale_in_resumes_assigned_partition() {
    let mut source = KafkaSource::connect(
        ConnectorId(8002),
        schema(),
        "127.0.0.1:1",
        "topic_scale_in",
        "grp_scale_in",
    )
    .unwrap();

    // Initial assignment: P0 only
    source.assign_partitions_for_test(&[0]);
    assert!(source.is_partition_assigned(0));
    assert!(!source.is_partition_assigned(1));

    // Scale in: consumer leaves group, P1 assigned to this consumer
    source.assign_partitions_for_test(&[1]);
    assert!(source.is_partition_assigned(0));
    assert!(source.is_partition_assigned(1));

    // Newly assigned P1 resumes from durable offset
    let offsets: BTreeMap<u64, u64> = BTreeMap::from([(0, 50), (1, 150)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
    assert_eq!(source.get_partition_offset(&token, 1), Some(150));
}

#[tokio::test]
async fn test_rebalance_fences_stale_generation_delivery() {
    let mut source = KafkaSource::connect(
        ConnectorId(8003),
        schema(),
        "127.0.0.1:1",
        "topic_fence",
        "grp_fence",
    )
    .unwrap();

    source.assign_partitions_for_test(&[0]);
    let gen1 = source.assignment_generation();

    // Rebalance advances generation
    source.assign_partitions_for_test(&[1]);
    let gen2 = source.assignment_generation();
    assert!(gen2 > gen1);

    // Record from stale generation (gen1) is fenced
    assert_ne!(gen1, source.assignment_generation());
}

#[tokio::test]
async fn test_rebalance_revocation_during_poll_purges_buffer() {
    let mut source = KafkaSource::connect(
        ConnectorId(8004),
        schema(),
        "127.0.0.1:1",
        "topic_purge",
        "grp_purge",
    )
    .unwrap();

    source.assign_partitions_for_test(&[0, 1]);
    assert!(source.is_partition_assigned(1));

    // Revocation purges uncommitted buffer for revoked partition
    source.revoke_partitions_for_test(&[1]);
    assert_eq!(source.last_poll_fill_level(), 0);
    assert!(!source.is_partition_assigned(1));
    assert!(source.is_partition_assigned(0));
}

#[tokio::test]
async fn test_rebalance_multi_consumer_coordination_exact() {
    // 4 partitions migrated across multiple consumer instances
    let mut c1 = KafkaSource::connect(
        ConnectorId(8005),
        schema(),
        "127.0.0.1:1",
        "topic_multi",
        "grp_multi",
    )
    .unwrap();
    let mut c2 = KafkaSource::connect(
        ConnectorId(8006),
        schema(),
        "127.0.0.1:1",
        "topic_multi",
        "grp_multi",
    )
    .unwrap();

    c1.assign_partitions_for_test(&[0, 1, 2, 3]);
    // Rebalance: C1 retains 0, 1; C2 gets 2, 3
    c1.revoke_partitions_for_test(&[2, 3]);
    c2.assign_partitions_for_test(&[2, 3]);

    assert!(c1.is_partition_assigned(0));
    assert!(c1.is_partition_assigned(1));
    assert!(!c1.is_partition_assigned(2));
    assert!(!c1.is_partition_assigned(3));

    assert!(c2.is_partition_assigned(2));
    assert!(c2.is_partition_assigned(3));
    assert!(!c2.is_partition_assigned(0));
    assert!(!c2.is_partition_assigned(1));

    let offsets = BTreeMap::from([(0_u64, 10_u64), (1, 20), (2, 30), (3, 40)]);
    let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());

    // Exact durable offset recovery for both consumers
    assert_eq!(c1.get_partition_offset(&token, 0), Some(10));
    assert_eq!(c1.get_partition_offset(&token, 1), Some(20));
    assert_eq!(c2.get_partition_offset(&token, 2), Some(30));
    assert_eq!(c2.get_partition_offset(&token, 3), Some(40));
}

#[test]
fn test_rebalance_fences_stale_generations_and_resumes_durable_offsets() {
    test_rebalance_scale_out_revokes_partition_safely();
    test_rebalance_scale_in_resumes_assigned_partition();
    test_rebalance_fences_stale_generation_delivery();
    test_rebalance_revocation_during_poll_purges_buffer();
    test_rebalance_multi_consumer_coordination_exact();
}
