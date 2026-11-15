//! Real PostgreSQL regressions for partition identity and retention boundaries.
//!
//! These tests deliberately use PG16 because publication root identity is a
//! prerequisite of correctly routing a partitioned source into one RockStream
//! imported parent table.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{PgOutputConfig, PostgresCdcSource, SourceConnector};
use rockstream_types::ids::ConnectorId;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::NoTls;

#[tokio::test]
async fn partitioned_pgoutput_requires_root_identity_and_truncate_fails_closed() {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for PostgreSQL partitioned-source regression"
    );
    let postgres = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = postgres.get_host().await.unwrap().to_string();
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (admin, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    admin
        .batch_execute(
            "CREATE TABLE events (id BIGINT NOT NULL, event_hour BIGINT NOT NULL) \
             PARTITION BY RANGE (event_hour); \
             CREATE TABLE events_old PARTITION OF events FOR VALUES FROM (0) TO (10); \
             CREATE TABLE events_recent PARTITION OF events FOR VALUES FROM (10) TO (20); \
             ALTER TABLE events REPLICA IDENTITY FULL; \
             CREATE PUBLICATION events_pub FOR TABLE events \
             WITH (publish_via_partition_root = false); \
             INSERT INTO events VALUES (1, 1), (2, 11);",
        )
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_hour", DataType::Int64, false),
    ]));
    let mut source = PostgresCdcSource::configured_pgoutput(
        ConnectorId(338),
        schema,
        PgOutputConfig {
            host,
            port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: Some("postgres".to_string()),
            slot: "rockstream_partition_retention".to_string(),
            publication: "events_pub".to_string(),
            table: "public.events".to_string(),
        },
    )
    .unwrap();

    // Without publication-root identity, the CDC decoder would receive child
    // relation IDs that do not match the root imported source table.
    let err = source.open_pgoutput(false).await.unwrap_err().to_string();
    assert!(err.contains("RS-4013"), "{err}");
    assert!(err.contains("publish_via_partition_root"), "{err}");

    admin
        .batch_execute("ALTER PUBLICATION events_pub SET (publish_via_partition_root = true)")
        .await
        .unwrap();
    source.open_pgoutput(false).await.unwrap();
    let first_fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let first = source
        .start_snapshot(&first_fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(
        first.iter().map(|batch| batch.batch.num_rows()).sum::<usize>(),
        2
    );

    // DETACH + DROP is DDL; no per-row DELETE is sent over pgoutput. A new
    // upstream snapshot no longer has the historical row, regardless of
    // what an existing durable RockStream materialization already contains.
    admin
        .batch_execute(
            "ALTER TABLE events DETACH PARTITION events_old; DROP TABLE events_old;",
        )
        .await
        .unwrap();
    let pruned_fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let pruned = source
        .start_snapshot(&pruned_fence, None, None)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(
        pruned.iter().map(|batch| batch.batch.num_rows()).sum::<usize>(),
        1
    );

    // In contrast to DDL pruning, a published TRUNCATE has an explicit
    // message. It must block before its LSN is acknowledged, rather than
    // silently behaving like an empty delta.
    admin.batch_execute("TRUNCATE events").await.unwrap();
    let err = source
        .poll_delta(pruned_fence.live, 8 * 1024 * 1024, 4_096, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("RS-4015"), "{err}");
    assert!(source.last_committed_lsn().is_none());

    // Losing an existing slot must not turn a checkpointed source into a
    // fresh bootstrap: that would make already-pruned historical rows vanish.
    source.close_pgoutput();
    admin
        .query_one(
            "SELECT pg_drop_replication_slot('rockstream_partition_retention')",
            &[],
        )
        .await
        .unwrap();
    let err = source.open_pgoutput(true).await.unwrap_err().to_string();
    assert!(err.contains("RS-4011"), "{err}");
    assert!(err.contains("checkpointed pgoutput slot"), "{err}");
}
