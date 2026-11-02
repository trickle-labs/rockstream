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
