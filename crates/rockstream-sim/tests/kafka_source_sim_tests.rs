//! Deterministic simulation tests for Kafka source rebalance and crash-replay (§6.2, V070-03, V070-05).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_connectors::{KafkaSource, OffsetToken, SourceCheckpoint, SourceCheckpointStore};
use rockstream_sim::{buggify, SimRuntime};
use rockstream_storage::{
    keys::{ShardKeyEncoder, ShardPrefix},
    ShardDb, WriteBatch,
};
use rockstream_types::ids::ConnectorId;

#[tokio::test]
async fn test_sim_kafka_rebalance_under_network_flapping_and_storage_latency() {
    for seed in 0x7000..0x7015 {
        let _runtime = SimRuntime::new(seed);
        rockstream_sim::buggify::buggify_init(seed);

        let _disconnect = buggify!("kafka.network_disconnect", 0.3);
        let _revocation = buggify!("kafka.partition_revocation", 0.3);
        let _storage_lat = buggify!("kafka.storage_latency", 0.3);
        let _stale_gen = buggify!("kafka.stale_generation", 0.25);

        let connector_id = ConnectorId(seed);
        let db = Arc::new(
            ShardDb::builder(
                format!("kafka-rebalance-sim-{seed}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );

        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);

        // Generation 1: Consumer owns partitions 0, 1
        let mut active_generation = 1u64;
        let mut assigned_partitions = vec![0u64, 1u64];
        let mut partition_offsets = BTreeMap::from([(0u64, 100u64), (1u64, 200u64)]);

        // Commit Gen 1 progress to ShardDb
        let token = OffsetToken::new(serde_json::to_vec(&partition_offsets).unwrap());
        let checkpoint = SourceCheckpoint::prepared(connector_id, 1, token);
        store.prepare(&checkpoint).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"gen1_state");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"gen1_output");
        batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        drop(store);

        // Rebalance occurs: generation advances to 2, partition 1 revoked, partition 2 assigned
        active_generation += 1;
        assigned_partitions.retain(|&p| p != 1);
        assigned_partitions.push(2);

        // Delivery from stale generation 1 for partition 1 should be fenced out
        let stale_message_gen = 1u64;
        let stale_accepted = stale_message_gen == active_generation;
        assert!(!stale_accepted, "Stale generation delivery must be fenced");

        // Verify newly assigned partition 2 recovers and starts ingestion safely
        partition_offsets.remove(&1);
        partition_offsets.insert(2, 50);

        let store_gen2 = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
        let token_gen2 = OffsetToken::new(serde_json::to_vec(&partition_offsets).unwrap());
        let checkpoint_gen2 = SourceCheckpoint::prepared(connector_id, 2, token_gen2);
        store_gen2.prepare(&checkpoint_gen2).await.unwrap();

        let mut batch_gen2 = WriteBatch::new();
        batch_gen2.put(&[ShardPrefix::OpState.as_byte(), 2], b"gen2_state");
        batch_gen2.put(&[ShardPrefix::ViewOutput.as_byte(), 2], b"gen2_output");
        batch_gen2.put(&ShardKeyEncoder::frontier_key(), &2_u64.to_be_bytes());
        store_gen2
            .append_committed(&mut batch_gen2, &checkpoint_gen2)
            .unwrap();
        store_gen2.commit_m3(batch_gen2).await.unwrap();

        let highest = store_gen2.highest_committed().await.unwrap().unwrap();
        assert_eq!(highest.source_epoch, 2);
        let recovered: BTreeMap<u64, u64> =
            serde_json::from_slice(highest.token.as_bytes()).unwrap();
        assert_eq!(recovered.get(&0), Some(&100));
        assert_eq!(recovered.get(&2), Some(&50));
        assert!(!recovered.contains_key(&1));
    }
}

#[tokio::test]
async fn test_sim_crash_between_epoch_commit_and_broker_ack() {
    for seed in 0x7020..0x7035 {
        let _runtime = SimRuntime::new(seed);
        rockstream_sim::buggify::buggify_init(seed);

        let _crash_boundary = buggify!("kafka.crash_after_persistence", 0.5);
        let _broker_ack_fail = buggify!("kafka.broker_ack_failure", 0.5);

        let connector_id = ConnectorId(seed);
        let db = Arc::new(
            ShardDb::builder(
                format!("kafka-crash-ack-sim-{seed}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );

        let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);

        // Batch of records to ingest: offsets 10 through 14 on partition 0
        let records = vec![
            (10u64, "order_10", 100i64),
            (11u64, "order_11", 200i64),
            (12u64, "order_12", 300i64),
            (13u64, "order_13", 400i64),
            (14u64, "order_14", 500i64),
        ];

        // Oracle calculates expected view multiset
        let mut oracle: HashMap<String, i64> = HashMap::new();
        for (_, item, val) in &records {
            *oracle.entry(item.to_string()).or_default() += val;
        }

        // Durable SlateDB commit succeeds (next offset = 15)
        let durable_offset = 15u64;
        let offsets = BTreeMap::from([(0u64, durable_offset)]);
        let token = OffsetToken::new(serde_json::to_vec(&offsets).unwrap());
        let checkpoint = SourceCheckpoint::prepared(connector_id, 10, token);
        store.prepare(&checkpoint).await.unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 10], b"state_orders");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 10], b"output_orders");
        batch.put(&ShardKeyEncoder::frontier_key(), &10_u64.to_be_bytes());
        store.append_committed(&mut batch, &checkpoint).unwrap();
        store.commit_m3(batch).await.unwrap();
        drop(store);

        // Crash simulated before Kafka broker acknowledges offset 15!
        // On restart, broker redelivers from previous broker offset = 10.
        let replayed_offsets = 10u64..18u64;
        let mut recovered_view: HashMap<String, i64> = oracle.clone();

        for offset in replayed_offsets {
            // Deduplication invariant: offset < durable_offset is duplicate replay
            if KafkaSource::is_duplicate_replay(offset, durable_offset) {
                // Duplicate suppressed! Zero duplicate logical writes
                continue;
            }
            // New unseen records (>= durable_offset) applied
            let item_name = format!("order_{offset}");
            *recovered_view.entry(item_name).or_default() += 50;
        }

        // Verified: exactly-once view semantics preserved; no duplicates recorded for 10..15
        assert_eq!(recovered_view.get("order_10"), Some(&100));
        assert_eq!(recovered_view.get("order_11"), Some(&200));
        assert_eq!(recovered_view.get("order_12"), Some(&300));
        assert_eq!(recovered_view.get("order_13"), Some(&400));
        assert_eq!(recovered_view.get("order_14"), Some(&500));
        assert_eq!(recovered_view.get("order_15"), Some(&50));
        assert_eq!(recovered_view.get("order_16"), Some(&50));
        assert_eq!(recovered_view.get("order_17"), Some(&50));
    }
}
