//! Pgwire reachability tests for retained Kafka and PostgreSQL CDC sources.

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
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
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

async fn read_order_total(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query("SELECT amount FROM order_total", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

#[tokio::test]
async fn postgres_cdc_source_ddl_rejects_invalid_options_and_redacts_credentials() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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
    client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='orders_pub', slot='orders_slot') FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    let exact = rows
        .iter()
        .map(|row| {
            (0..6)
                .map(|index| row.get::<_, String>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exact,
        vec![vec![
            "orders".to_string(),
            "postgres_cdc".to_string(),
            "pgoutput".to_string(),
            "OK".to_string(),
            "0".to_string(),
            "0".to_string(),
        ]]
    );

    for sql in [
        "CREATE SOURCE wrong_format TYPE postgres_cdc (credential_ref='vault://pg/orders', publication='p', slot='s') FORMAT json;",
        "CREATE SOURCE missing_ref TYPE postgres_cdc (publication='p', slot='s') FORMAT pgoutput;",
    ] {
        let error = client.execute(sql, &[]).await.unwrap_err();
        let message = error.as_db_error().map(|error| error.message()).unwrap_or("");
        assert!(message.contains("RS-4008"), "unexpected error: {message}");
        assert!(message.contains("Next steps:"), "unexpected error: {message}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_backfill_and_live_updates_reach_pgwire() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the Kafka source backfill proof"
    );
    let broker = apache::Kafka::default().start().await.unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let topic = "gateway-source-v0521";
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
    for (timestamp, values) in [
        (1_i64, serde_json::json!([1, "a", 10])),
        (1_i64, serde_json::json!([2, "a", 20])),
    ] {
        producer
            .send(
                FutureRecord::<(), _>::to(topic).payload(
                    &serde_json::json!({"timestamp": timestamp, "values": values, "weight": 1})
                        .to_string(),
                ),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-kafka-backfill-v0521",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
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
            "CREATE SOURCE orders TYPE kafka (bootstrap.servers='{bootstrap}', topic='{topic}', group.id='gateway-source-v0521') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW order_rows AS SELECT customer, SUM(amount) AS amount FROM orders GROUP BY customer",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    let initial = loop {
        let rows = client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "30".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "Kafka initial replay did not publish"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(initial, vec![("a".to_string(), "30".to_string())]);

    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(
                &serde_json::json!({"timestamp": 2, "values": [3, "a", 5], "weight": 1})
                    .to_string(),
            ),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let live = loop {
        let rows = client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "35".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "Kafka worker did not ingest later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(live, vec![("a".to_string(), "35".to_string())]);
    let expected_status = vec![vec![
        Some("order_rows".to_string()),
        Some("RUNNING".to_string()),
        Some("3".to_string()),
        Some("0".to_string()),
        Some("0".to_string()),
        Some("ADMITTED".to_string()),
        None,
    ]];
    let status_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = client
            .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (0..7)
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if status == expected_status {
            break;
        }
        assert!(
            Instant::now() < status_deadline,
            "Kafka backfill status did not reach the final cursor"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let persisted_table = catalog.get_table("orders").unwrap();
    let persisted_source = catalog.get_source("orders").unwrap();
    let persisted_view = catalog.get_view("order_rows").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["orders".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    let recovered = restarted_client
        .query(
            "SELECT customer, amount FROM order_rows ORDER BY customer",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(recovered, vec![("a".to_string(), "35".to_string())]);
    let recovered_status = restarted_client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (0..7)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recovered_status,
        vec![vec![
            Some("order_rows".to_string()),
            Some("RUNNING".to_string()),
            Some("3".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
            Some("ADMITTED".to_string()),
            None,
        ]]
    );
    producer
        .send(
            FutureRecord::<(), _>::to(topic).payload(
                &serde_json::json!({"timestamp": 3, "values": [4, "a", 7], "weight": 1})
                    .to_string(),
            ),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let post_restart_live = loop {
        let rows = restarted_client
            .query(
                "SELECT customer, amount FROM order_rows ORDER BY customer",
                &[],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        if rows == vec![("a".to_string(), "42".to_string())] {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "restarted Kafka worker did not ingest later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(post_restart_live, vec![("a".to_string(), "42".to_string())]);
    drop(restarted_client);
    restarted_handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn postgres_cdc_pgoutput_backfill_live_update_and_restart_reach_pgwire() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC source proof"
    );
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = postgres.get_host().await.unwrap();
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let (upstream, upstream_connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = upstream_connection.await;
    });
    upstream
        .batch_execute(concat!(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT); ",
            "ALTER TABLE orders REPLICA IDENTITY FULL; ",
            "CREATE PUBLICATION orders_pub FOR TABLE orders; ",
            "INSERT INTO orders VALUES (1, 10), (2, 20);",
        ))
        .await
        .unwrap();

    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "gateway-postgres-cdc-v0521",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&shard_db),
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
        "CREATE TABLE orders (id BIGINT, amount BIGINT)",
        &format!(
            "CREATE SOURCE orders TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='orders_pub', slot='gateway_orders_slot', table='orders') FORMAT pgoutput"
        ),
        "CREATE MATERIALIZED VIEW order_total AS SELECT SUM(amount) AS amount FROM orders",
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    assert_eq!(read_order_total(&client).await, vec!["30".to_string()]);
    upstream
        .batch_execute(
            "INSERT INTO orders VALUES (3, 5); UPDATE orders SET amount = 7 WHERE id = 1;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if read_order_total(&client).await == vec!["32".to_string()] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC worker did not apply live changes"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let persisted_table = catalog.get_table("orders").unwrap();
    let persisted_source = catalog.get_source("orders").unwrap();
    let persisted_view = catalog.get_view("order_total").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["orders".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let (restarted_client, restarted_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            restarted_address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = restarted_connection.await;
    });
    assert_eq!(
        read_order_total(&restarted_client).await,
        vec!["32".to_string()]
    );
    upstream
        .execute("INSERT INTO orders VALUES (4, 11)", &[])
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if read_order_total(&restarted_client).await == vec!["43".to_string()] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted PostgreSQL CDC worker did not apply the later record"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_source_ddl_complete_valid_options() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                port='5432',
                database='pg',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "orders");
}

#[tokio::test]
async fn test_source_ddl_rejects_inline_password() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                password='secret',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
}

#[tokio::test]
async fn test_source_ddl_rejects_missing_publication() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(
        msg.contains("publication"),
        "expected publication in error, got: {msg}"
    );
}

#[tokio::test]
async fn test_source_ddl_rejects_missing_slot() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                publication='pub1',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("slot"), "expected slot in error, got: {msg}");
}

#[tokio::test]
async fn test_source_ddl_rejects_missing_credential() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
}

#[tokio::test]
async fn test_source_ddl_rejects_invalid_snapshot_policy() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='invalid_policy'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(
        msg.contains("snapshot_policy"),
        "expected snapshot_policy in message, got: {msg}"
    );
}

#[tokio::test]
async fn test_source_ddl_rejects_invalid_schema_policy() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE orders TYPE postgres_cdc (
                host='localhost',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='unknown_policy',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(
        msg.contains("schema_policy"),
        "expected schema_policy in message, got: {msg}"
    );
}

#[tokio::test]
async fn test_source_ddl_rejects_physical_slot_collision() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    client
        .execute(
            "CREATE SOURCE src1 TYPE postgres_cdc (
                host='127.0.0.1',
                port='5432',
                database='postgres',
                publication='pub1',
                slot='shared_slot',
                table='orders1',
                credential_ref='vault://pg/key'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();

    let err = client
        .execute(
            "CREATE SOURCE src2 TYPE postgres_cdc (
                host='127.0.0.1',
                port='5432',
                database='postgres',
                publication='pub2',
                slot='shared_slot',
                table='orders2',
                credential_ref='vault://pg/key'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4001") || msg.contains("RS-4013"),
        "expected RS-4001 or RS-4013 for slot collision, got: {msg}"
    );
    assert!(
        msg.contains("physical pgoutput slot is already owned"),
        "got: {msg}"
    );
}

#[test]
fn test_source_catalog_rejects_unsupported_version() {
    use rockstream_storage::catalog::SourceRecordV1;
    let valid = serde_json::json!({
        "version": 1,
        "id": 100,
        "name": "orders",
        "connector_type": "postgres_cdc",
        "table_name": "orders",
        "options": {},
        "format": "pgoutput"
    });
    let valid_bytes = serde_json::to_vec(&valid).unwrap();
    let decoded = SourceRecordV1::decode_versioned(&valid_bytes).unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.name, "orders");

    let invalid = serde_json::json!({
        "version": 99,
        "id": 100,
        "name": "orders",
        "connector_type": "postgres_cdc",
        "table_name": "orders",
        "options": {},
        "format": "pgoutput"
    });
    let invalid_bytes = serde_json::to_vec(&invalid).unwrap();
    let err = SourceRecordV1::decode_versioned(&invalid_bytes).unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2025"),
        "expected RS-2025, got: {err_str}"
    );
    assert!(
        err_str.contains("version 99 is not supported"),
        "got: {err_str}"
    );
}

#[tokio::test]
async fn test_canonical_source_ddl_seven_fields_and_credential_rejection() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    // 1. Valid source with all seven fields
    client
        .execute(
            "CREATE SOURCE s_valid TYPE postgres_cdc (
                host='localhost',
                port='5432',
                database='pg',
                publication='pub1',
                slot='slot1',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();

    // 2. Reject inline password
    let err = client
        .execute(
            "CREATE SOURCE s_inline TYPE postgres_cdc (
                host='localhost',
                publication='pub2',
                slot='slot2',
                table='orders',
                schema_policy='evolve',
                password='secret',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for inline password, got: {msg}"
    );

    // 3. Reject missing publication
    let err = client
        .execute(
            "CREATE SOURCE s_nopub TYPE postgres_cdc (
                host='localhost',
                slot='slot3',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for missing pub, got: {msg}"
    );

    // 4. Reject missing slot
    let err = client
        .execute(
            "CREATE SOURCE s_noslot TYPE postgres_cdc (
                host='localhost',
                publication='pub4',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for missing slot, got: {msg}"
    );

    // 5. Reject missing credential
    let err = client
        .execute(
            "CREATE SOURCE s_nocred TYPE postgres_cdc (
                host='localhost',
                publication='pub5',
                slot='slot5',
                table='orders',
                schema_policy='evolve',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for missing cred, got: {msg}"
    );

    // 6. Reject invalid snapshot policy
    let err = client
        .execute(
            "CREATE SOURCE s_badsnap TYPE postgres_cdc (
                host='localhost',
                publication='pub6',
                slot='slot6',
                table='orders',
                schema_policy='evolve',
                credential_ref='vault://pg/key',
                snapshot_policy='invalid_policy'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for invalid snapshot_policy, got: {msg}"
    );

    // 7. Reject invalid schema policy
    let err = client
        .execute(
            "CREATE SOURCE s_badschema TYPE postgres_cdc (
                host='localhost',
                publication='pub7',
                slot='slot7',
                table='orders',
                schema_policy='unknown_policy',
                credential_ref='vault://pg/key',
                snapshot_policy='initial'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4008"),
        "expected RS-4008 for invalid schema_policy, got: {msg}"
    );
}

#[tokio::test]
async fn test_source_ddl_persists_versioned_record_and_rejects_slot_collision() {
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "test-versioned-source-shard",
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
        Arc::clone(&shard_db),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    client
        .execute(
            "CREATE SOURCE my_pg_src TYPE postgres_cdc (
                host='127.0.0.1',
                port='5432',
                database='testdb',
                publication='test_pub',
                slot='test_slot',
                table='orders',
                credential_ref='vault://pg/key'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap();

    let persisted = shard_db
        .get(b"catalog:source:v1:my_pg_src")
        .await
        .unwrap()
        .expect("persisted source record must exist in shard_db");
    let record = rockstream_storage::catalog::SourceRecordV1::decode_versioned(&persisted).unwrap();
    assert_eq!(record.version, 1);
    assert_eq!(record.name, "my_pg_src");
    assert_eq!(record.connector_type, "postgres_cdc");
    assert_eq!(record.format, "pgoutput");

    // Second source targeting same physical slot should be rejected
    let err = client
        .execute(
            "CREATE SOURCE another_pg_src TYPE postgres_cdc (
                host='127.0.0.1',
                port='5432',
                database='testdb',
                publication='other_pub',
                slot='test_slot',
                table='orders2',
                credential_ref='vault://pg/key'
            ) FORMAT pgoutput;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        msg.contains("RS-4001") || msg.contains("RS-4013"),
        "got: {msg}"
    );
}

#[tokio::test]
async fn test_kafka_source_ddl_complete_valid_options() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='strict',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "events");
    assert_eq!(rows[0].get::<_, String>(1), "kafka");
    assert_eq!(rows[0].get::<_, String>(2), "json");
}

#[tokio::test]
async fn test_kafka_source_ddl_dot_notation_aliases() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    client
        .execute(
            "CREATE SECRET kafka_secret (TYPE = 'sasl_plain', username = 'u', password = 'p');",
            &[],
        )
        .await
        .unwrap();

    client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap.servers='localhost:9092',
                topic='events',
                group.id='grp1',
                offset_policy='latest',
                schema_policy='strict',
                secret='kafka_secret'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW SOURCES;", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "events");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_inline_password() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='strict',
                password='secret'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("inline credential"), "got: {msg}");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_inline_token() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='strict',
                token='secret_token'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("inline credential"), "got: {msg}");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_missing_bootstrap() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                topic='events',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='strict',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("bootstrap"), "got: {msg}");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_missing_topic() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='strict',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("topic"), "got: {msg}");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_invalid_offset_policy() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                group_id='grp1',
                offset_policy='invalid_policy',
                schema_policy='strict',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("offset_policy"), "got: {msg}");
}

#[tokio::test]
async fn test_kafka_source_ddl_rejects_invalid_schema_policy() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    let err = client
        .execute(
            "CREATE SOURCE events TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                group_id='grp1',
                offset_policy='earliest',
                schema_policy='unknown_policy',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-4008"), "expected RS-4008, got: {msg}");
    assert!(msg.contains("schema_policy"), "got: {msg}");
}

#[test]
fn test_kafka_catalog_rejects_unsupported_version() {
    use rockstream_storage::catalog::SourceRecordV1;
    let invalid = serde_json::json!({
        "version": 99,
        "id": 101,
        "name": "kafka_events",
        "connector_type": "kafka",
        "table_name": "events",
        "options": {
            "bootstrap_servers": "localhost:9092",
            "topic": "events"
        },
        "format": "json"
    });
    let invalid_bytes = serde_json::to_vec(&invalid).unwrap();
    let err = SourceRecordV1::decode_versioned(&invalid_bytes).unwrap_err();
    let err_str = err.to_string();
    assert!(err_str.contains("RS-2025"), "got: {err_str}");
    assert!(
        err_str.contains("version 99 is not supported"),
        "got: {err_str}"
    );
}

#[tokio::test]
async fn test_canonical_kafka_source_ddl_and_credential_rejection() {
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder(
            "test-versioned-kafka-shard",
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
        Arc::clone(&shard_db),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
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

    // 1. Valid Kafka source persists in catalog
    client
        .execute(
            "CREATE SOURCE canonical_kafka TYPE kafka (
                bootstrap_servers='127.0.0.1:9092',
                topic='canonical_events',
                group_id='grp_canon',
                offset_policy='earliest',
                schema_policy='strict',
                credential_ref='vault://kafka/key'
            ) FORMAT json;",
            &[],
        )
        .await
        .unwrap();

    let persisted = shard_db
        .get(b"catalog:source:v1:canonical_kafka")
        .await
        .unwrap()
        .expect("persisted kafka source record must exist in shard_db");
    let record = rockstream_storage::catalog::SourceRecordV1::decode_versioned(&persisted).unwrap();
    assert_eq!(record.version, 1);
    assert_eq!(record.name, "canonical_kafka");
    assert_eq!(record.connector_type, "kafka");
    assert_eq!(record.format, "json");

    // 2. Reject inline credentials
    for (name, opt) in [
        ("s_pass", "password='secret'"),
        ("s_tok", "token='tok123'"),
        ("s_key", "api_key='key123'"),
        ("s_auth", "authorization='Bearer xyz'"),
    ] {
        let sql = format!(
            "CREATE SOURCE {name} TYPE kafka (
                bootstrap_servers='localhost:9092',
                topic='events',
                {opt}
            ) FORMAT json;"
        );
        let err = client.execute(&sql, &[]).await.unwrap_err();
        let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
        assert!(
            msg.contains("RS-4008"),
            "expected RS-4008 for {opt}, got: {msg}"
        );
    }
}
