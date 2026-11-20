use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio_postgres::NoTls;

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn query_view(client: &tokio_postgres::Client, name: &str) -> Vec<(String, String)> {
    client
        .query(
            &format!("SELECT customer, amount FROM {name} ORDER BY customer"),
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn two_views_with_one_group_each_receive_every_partition() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the Kafka multi-view proof"
    );
    let broker = apache::Kafka::default().start().await.unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "gateway-kafka-multi-view";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 2, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_string())]
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-kafka-multi-view",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE orders (id BIGINT, customer TEXT, amount BIGINT)",
        &format!(
            "CREATE SOURCE orders TYPE kafka (bootstrap.servers='{bootstrap}', topic='{topic}', group_id='shared-orders-group') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW first_view AS SELECT customer, SUM(amount) AS amount FROM orders GROUP BY customer",
        "CREATE MATERIALIZED VIEW second_view AS SELECT customer, SUM(amount) AS amount FROM orders GROUP BY customer",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    for (partition, customer, amount) in [(0, "partition-a", 10), (1, "partition-b", 20)] {
        let payload = serde_json::json!({
            "timestamp": partition + 1,
            "values": [partition + 1, customer, amount],
            "weight": 1
        })
        .to_string();
        producer
            .send(
                FutureRecord::<(), _>::to(topic)
                    .partition(partition)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let expected = vec![
        ("partition-a".to_string(), "10".to_string()),
        ("partition-b".to_string(), "20".to_string()),
    ];
    let deadline = Instant::now() + Duration::from_secs(20);
    let (first, second) = loop {
        let first = query_view(&client, "first_view").await;
        let second = query_view(&client, "second_view").await;
        if first == expected && second == expected {
            break (first, second);
        }
        assert!(
            Instant::now() < deadline,
            "views did not both receive the complete Kafka input: first={first:?}, second={second:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    drop(client);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn real_broker_ack_outage_after_m3_recovers_with_no_new_input_in_same_process() {
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_connectors::{
        BackfillCursor, BackfillLifecycle, BackfillPhase, KafkaSource, OffsetToken,
        SnapshotDeltaFence, SourceCheckpoint, SourceCheckpointStore, SourceRuntimeCoordinator,
    };
    use rockstream_storage::{
        keys::{ShardKeyEncoder, ShardPrefix},
        WriteBatch,
    };
    use rockstream_types::ids::ConnectorId;
    use std::process::Command;

    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the real broker ACK outage proof",
    );
    let broker = apache::Kafka::default().start().await.unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "gateway-kafka-ack-outage";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_string())]
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    let connector_id = ConnectorId(5132);
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-kafka-ack-outage",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&shard_db), 0, connector_id);
    let source = KafkaSource::connect(
        connector_id,
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        &bootstrap,
        topic,
        "gateway-kafka-ack-outage-group",
    )
    .unwrap();
    let mut runtime = SourceRuntimeCoordinator::new(
        source,
        connector_id,
        OffsetToken::new(vec![]),
        store.clone(),
    );
    runtime.recover().await.unwrap();
    let owner = runtime.acquire_owner("gateway-kafka-live").unwrap();
    producer
        .send(
            FutureRecord::<(), _>::to(topic)
                .payload(r#"{"timestamp":1,"values":[1],"weight":1}"#),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    let token = loop {
        let delta = runtime.poll_delta(64 * 1024, 16).await.unwrap();
        if !delta.batches.is_empty() {
            break delta.new_offset;
        }
        assert!(Instant::now() < deadline, "Kafka source never received first input");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    fn m3(epoch: u64) -> WriteBatch {
        let mut batch = WriteBatch::new();
        batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"pipeline-state");
        batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"materialized-once");
        batch.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
        batch
    }

    fn lifecycle(epoch: u64, token: &OffsetToken) -> BackfillLifecycle {
        BackfillLifecycle::new(
            BackfillPhase::Running,
            BackfillCursor::new(
                "kafka_ack_view",
                0,
                token.as_bytes().to_vec(),
                SnapshotDeltaFence::new(
                    OffsetToken::new(vec![]),
                    OffsetToken::new(vec![]),
                ),
                epoch,
            ),
            0,
            2,
            0,
            Some(epoch),
        )
    }

    // Blackhole the *actual* broker before the M3 + ACK boundary. The
    // checkpoint and view output are persisted locally, but the broker cannot
    // accept the synchronous offset commit. Unlike a process restart, the
    // same coordinator must recover and release its pending in-flight slot.
    let container_id = broker.id().to_string();
    fn docker_action(action: &str, id: &str) {
        let status = Command::new("docker").args([action, id]).status().unwrap();
        assert!(status.success(), "docker {action} {id} failed: {status}");
    }
    struct UnpauseOnDrop {
        container_id: String,
        paused: bool,
    }
    impl Drop for UnpauseOnDrop {
        fn drop(&mut self) {
            if self.paused {
                let _ = Command::new("docker")
                    .args(["unpause", &self.container_id])
                    .status();
            }
        }
    }
    docker_action("pause", &container_id);
    let mut unpause = UnpauseOnDrop {
        container_id,
        paused: true,
    };
    let durable_lifecycle = lifecycle(1, &token);
    let failure = runtime
        .commit_backfill_epoch(&owner, 1, token.clone(), durable_lifecycle.clone(), m3(1))
        .await
        .unwrap_err();
    assert!(failure.to_string().contains("RS-4016"), "{failure}");
    assert!(runtime.has_pending_ack());
    assert_eq!(
        store.backfill_lifecycle("kafka_ack_view").await.unwrap(),
        Some(durable_lifecycle),
    );
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(SourceCheckpoint::prepared(connector_id, 1, token.clone()).committed()),
    );
    assert_eq!(
        shard_db
            .get(&[ShardPrefix::ViewOutput.as_byte(), 1])
            .await
            .unwrap()
            .as_deref(),
        Some(b"materialized-once".as_slice()),
    );
    assert!(runtime.poll_delta(1024, 1).await.is_err());
    docker_action("unpause", &unpause.container_id);
    unpause.paused = false;

    let retry_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match runtime.acknowledge_recovered(&owner).await {
            Ok(()) => break,
            Err(error) => {
                assert!(Instant::now() < retry_deadline, "Kafka ACK did not recover: {error}");
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    assert_eq!(runtime.blocked_reason(), None);
    assert!(!runtime.has_pending_ack());
    assert_eq!(
        runtime.metrics().await.unwrap().source_runtime_in_flight_epochs,
        0
    );
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(SourceCheckpoint::prepared(connector_id, 1, token).committed())
    );

    // Do not restart either the gateway process or the coordinator; verify
    // subsequent real Kafka input can still commit after the idle recovery.
    producer
        .send(
            FutureRecord::<(), _>::to(topic)
                .payload(r#"{"timestamp":2,"values":[2],"weight":1}"#),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(25);
    let next_token = loop {
        let delta = runtime.poll_delta(64 * 1024, 16).await.unwrap();
        if !delta.batches.is_empty() {
            break delta.new_offset;
        }
        assert!(Instant::now() < deadline, "Kafka source did not resume after ACK retry");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    runtime
        .commit_backfill_epoch(
            &owner,
            2,
            next_token.clone(),
            lifecycle(2, &next_token),
            m3(2),
        )
        .await
        .unwrap();
    assert_eq!(
        runtime.into_inner().last_committed(),
        Some((2, next_token)),
    );
}
