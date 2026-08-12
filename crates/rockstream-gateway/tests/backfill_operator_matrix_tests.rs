//! Raw-pgwire coverage matrix for materialized-view backfill operators.

#![allow(dead_code, unused_macros)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
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
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn result_string_rows(client: &tokio_postgres::Client) -> Vec<Vec<String>> {
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn result_optional_rows(client: &tokio_postgres::Client) -> Vec<Vec<Option<String>>> {
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn aggregate_rows(
    key_type: &str,
    value_type: &str,
    function: &str,
    values: &str,
) -> Vec<Vec<String>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(
            format!("backfill-matrix-{key_type}-{value_type}-{function}"),
            store,
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE input (id BIGINT, k {key_type}, v {value_type})"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!("INSERT INTO input (id, k, v) VALUES {values}"))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "CREATE MATERIALIZED VIEW result AS SELECT k, {function}(v) AS value FROM input GROUP BY k"
        ))
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn pgoutput_aggregate_rows(
    key_type: &str,
    value_type: &str,
    function: &str,
    values: &str,
) -> Vec<Vec<String>> {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC matrix proof"
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
        .batch_execute(&format!(
            "CREATE TABLE input (id BIGINT PRIMARY KEY, k {key_type}, v {value_type}); \
             ALTER TABLE input REPLICA IDENTITY FULL; \
             CREATE PUBLICATION input_pub FOR TABLE input; \
             INSERT INTO input (id, k, v) VALUES {values};"
        ))
        .await
        .unwrap();

    let dir = TempDir::new().unwrap();
    let state = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-pgoutput-aggregate-{function}"), state)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let connect = |address: std::net::SocketAddr| async move {
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
    };
    let client = connect(address).await;
    for sql in [
        &format!("CREATE TABLE input (id BIGINT, k {key_type}, v {value_type})"),
        &format!(
            "CREATE SOURCE input TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='input_pub', slot='matrix_{}_slot', table='input') FORMAT pgoutput",
            function.to_ascii_lowercase()
        ),
        &format!(
            "CREATE MATERIALIZED VIEW result AS SELECT k, {function}(v) AS value FROM input GROUP BY k"
        ),
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let expected = result_string_rows(&client).await;
    upstream
        .batch_execute("INSERT INTO input VALUES (99, 99, 11); DELETE FROM input WHERE id = 99;")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = client
            .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW result", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (0..7)
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if result_string_rows(&client).await == expected
            && status
                == vec![vec![
                    Some("result".to_string()),
                    Some("RUNNING".to_string()),
                    Some("3".to_string()),
                    Some("0".to_string()),
                    Some("3".to_string()),
                    Some("ADMITTED".to_string()),
                    None,
                ]]
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC live aggregate did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let persisted_table = catalog.get_table("input").unwrap();
    let persisted_source = catalog.get_source("input").unwrap();
    let persisted_view = catalog.get_view("result").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(persisted_table));
    assert!(recovered_catalog.add_source(persisted_source));
    recovered_catalog.add_view_with_deps(persisted_view, vec!["input".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let restarted_client = connect(restarted_address).await;
    assert_eq!(result_string_rows(&restarted_client).await, expected);
    expected
}

macro_rules! aggregate_cell {
    ($name:ident, $key:expr, $value:expr, $function:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(
                aggregate_rows($key, $value, $function, $values).await,
                $expected
            );
        }
    };
}

macro_rules! pgoutput_aggregate_cell {
    ($name:ident, $key:expr, $value:expr, $function:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(
                pgoutput_aggregate_rows($key, $value, $function, $values).await,
                $expected
            );
        }
    };
}

pgoutput_aggregate_cell!(
    backfill_aggregate_i32_i64_sum_exact_oracle,
    "INT",
    "BIGINT",
    "SUM",
    "(1, 1, 10), (2, 1, 5), (3, 2, 3)",
    vec![
        vec!["1".to_string(), "15".to_string()],
        vec!["2".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i32_i64_count_exact_oracle,
    "INT",
    "BIGINT",
    "COUNT",
    "(1, 1, 10), (2, 1, 5), (3, 2, 3)",
    vec![
        vec!["1".to_string(), "2".to_string()],
        vec!["2".to_string(), "1".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i32_i64_avg_exact_oracle,
    "INT",
    "BIGINT",
    "AVG",
    "(1, 1, 10), (2, 1, 5), (3, 2, 3)",
    vec![
        vec!["1".to_string(), "7.5".to_string()],
        vec!["2".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i32_i64_min_exact_oracle,
    "INT",
    "BIGINT",
    "MIN",
    "(1, 1, 10), (2, 1, 5), (3, 2, 3)",
    vec![
        vec!["1".to_string(), "5".to_string()],
        vec!["2".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i32_i64_max_exact_oracle,
    "INT",
    "BIGINT",
    "MAX",
    "(1, 1, 10), (2, 1, 5), (3, 2, 3)",
    vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_text_i64_sum_exact_oracle,
    "TEXT",
    "BIGINT",
    "SUM",
    "(1, 'a', 10), (2, 'a', 5), (3, 'b', 3)",
    vec![
        vec!["a".to_string(), "15".to_string()],
        vec!["b".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_text_i64_count_exact_oracle,
    "TEXT",
    "BIGINT",
    "COUNT",
    "(1, 'a', 10), (2, 'a', 5), (3, 'b', 3)",
    vec![
        vec!["a".to_string(), "2".to_string()],
        vec!["b".to_string(), "1".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_text_i64_avg_exact_oracle,
    "TEXT",
    "BIGINT",
    "AVG",
    "(1, 'a', 10), (2, 'a', 5), (3, 'b', 3)",
    vec![
        vec!["a".to_string(), "7.5".to_string()],
        vec!["b".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_text_i64_min_exact_oracle,
    "TEXT",
    "BIGINT",
    "MIN",
    "(1, 'a', 10), (2, 'a', 5), (3, 'b', 3)",
    vec![
        vec!["a".to_string(), "5".to_string()],
        vec!["b".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_text_i64_max_exact_oracle,
    "TEXT",
    "BIGINT",
    "MAX",
    "(1, 'a', 10), (2, 'a', 5), (3, 'b', 3)",
    vec![
        vec!["a".to_string(), "10".to_string()],
        vec!["b".to_string(), "3".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i64_decimal_sum_exact_oracle,
    "BIGINT",
    "DECIMAL(12,2)",
    "SUM",
    "(1, 1, 10.25), (2, 1, 5.50), (3, 2, 3.75)",
    vec![
        vec!["1".to_string(), "15.75".to_string()],
        vec!["2".to_string(), "3.75".to_string()]
    ]
);
pgoutput_aggregate_cell!(
    backfill_aggregate_i64_decimal_avg_exact_oracle,
    "BIGINT",
    "DECIMAL(12,2)",
    "AVG",
    "(1, 1, 10.25), (2, 1, 5.50), (3, 2, 3.75)",
    vec![
        vec!["1".to_string(), "7.875".to_string()],
        vec!["2".to_string(), "3.75".to_string()]
    ]
);

async fn join_rows(
    join_kind: &str,
    key_type: &str,
    left_values: &str,
    right_values: &str,
) -> Vec<Vec<Option<String>>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-matrix-{join_kind}-{key_type}"), store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE left_input (id BIGINT, k {key_type}, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "CREATE TABLE right_input (id BIGINT, k {key_type}, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO left_input (id, k, v) VALUES {left_values}"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO right_input (id, k, v) VALUES {right_values}"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "CREATE MATERIALIZED VIEW result AS SELECT l.k, l.v AS left_value, r.v AS right_value FROM left_input l {join_kind} JOIN right_input r ON l.k = r.k"
        ))
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn pgoutput_join_rows(
    join_kind: &str,
    key_type: &str,
    left_values: &str,
    right_values: &str,
) -> Vec<Vec<Option<String>>> {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC matrix proof"
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
        .batch_execute(&format!(
            "CREATE TABLE left_input (id BIGINT PRIMARY KEY, k {key_type}, v BIGINT); \
             CREATE TABLE right_input (id BIGINT PRIMARY KEY, k {key_type}, v BIGINT); \
             ALTER TABLE left_input REPLICA IDENTITY FULL; \
             ALTER TABLE right_input REPLICA IDENTITY FULL; \
             CREATE PUBLICATION left_pub FOR TABLE left_input; \
             CREATE PUBLICATION right_pub FOR TABLE right_input; \
             INSERT INTO left_input (id, k, v) VALUES {left_values}; \
             INSERT INTO right_input (id, k, v) VALUES {right_values};"
        ))
        .await
        .unwrap();
    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            format!("backfill-pgoutput-join-{}", join_kind.to_ascii_lowercase()),
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let connect = |address: std::net::SocketAddr| async move {
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
    };
    let client = connect(address).await;
    for sql in [
        &format!("CREATE TABLE left_input (id BIGINT, k {key_type}, v BIGINT)"),
        &format!("CREATE TABLE right_input (id BIGINT, k {key_type}, v BIGINT)"),
        &format!(
            "CREATE SOURCE left_input TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='left_pub', slot='matrix_{}_left_slot', table='left_input') FORMAT pgoutput",
            join_kind.to_ascii_lowercase()
        ),
        &format!(
            "CREATE SOURCE right_input TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='right_pub', slot='matrix_{}_right_slot', table='right_input') FORMAT pgoutput",
            join_kind.to_ascii_lowercase()
        ),
        &format!(
            "CREATE MATERIALIZED VIEW result AS SELECT l.k, l.v AS left_value, r.v AS right_value FROM left_input l {join_kind} JOIN right_input r ON l.k = r.k"
        ),
    ] {
        client.execute(sql, &[]).await.unwrap();
    }
    let expected = result_optional_rows(&client).await;
    upstream
        .batch_execute(
            "INSERT INTO left_input VALUES (99, 99, 11); DELETE FROM left_input WHERE id = 99;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = client
            .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW result", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (0..7)
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if result_optional_rows(&client).await == expected
            && status
                == vec![vec![
                    Some("result".to_string()),
                    Some("RUNNING".to_string()),
                    Some("left_input:3,right_input:2".to_string()),
                    Some("0".to_string()),
                    Some("4".to_string()),
                    Some("ADMITTED".to_string()),
                    None,
                ]]
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC live join did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let left_table = catalog.get_table("left_input").unwrap();
    let right_table = catalog.get_table("right_input").unwrap();
    let left_source = catalog.get_source("left_input").unwrap();
    let right_source = catalog.get_source("right_input").unwrap();
    let view = catalog.get_view("result").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovered_catalog = Arc::new(CatalogStubs::new());
    assert!(recovered_catalog.add_table(left_table));
    assert!(recovered_catalog.add_table(right_table));
    assert!(recovered_catalog.add_source(left_source));
    assert!(recovered_catalog.add_source(right_source));
    recovered_catalog.add_view_with_deps(
        view,
        vec!["left_input".to_string(), "right_input".to_string()],
    );
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered_catalog,
        Arc::new(NoopViewReader),
        db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let restarted_client = connect(restarted_address).await;
    assert_eq!(result_optional_rows(&restarted_client).await, expected);
    expected
}

macro_rules! join_cell {
    ($name:ident, $kind:expr, $key:expr, $left:expr, $right:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(join_rows($kind, $key, $left, $right).await, $expected);
        }
    };
}

macro_rules! pgoutput_join_cell {
    ($name:ident, $kind:expr, $key:expr, $left:expr, $right:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(
                pgoutput_join_rows($kind, $key, $left, $right).await,
                $expected
            );
        }
    };
}

pgoutput_join_cell!(
    backfill_inner_join_i64_exact_oracle,
    "INNER",
    "BIGINT",
    "(1, 1, 10), (2, 2, 20)",
    "(3, 1, 100), (4, 3, 300)",
    vec![vec![
        Some("1".to_string()),
        Some("10".to_string()),
        Some("100".to_string())
    ]]
);
pgoutput_join_cell!(
    backfill_left_join_i64_exact_oracle,
    "LEFT",
    "BIGINT",
    "(1, 1, 10), (2, 2, 20)",
    "(3, 1, 100), (4, 3, 300)",
    vec![
        vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ],
        vec![Some("2".to_string()), Some("20".to_string()), None]
    ]
);
pgoutput_join_cell!(
    backfill_right_join_i64_exact_oracle,
    "RIGHT",
    "BIGINT",
    "(1, 1, 10), (2, 2, 20)",
    "(3, 1, 100), (4, 3, 300)",
    vec![
        vec![None, None, Some("300".to_string())],
        vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ]
    ]
);
pgoutput_join_cell!(
    backfill_full_join_i64_exact_oracle,
    "FULL",
    "BIGINT",
    "(1, 1, 10), (2, 2, 20)",
    "(3, 1, 100), (4, 3, 300)",
    vec![
        vec![None, None, Some("300".to_string())],
        vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ],
        vec![Some("2".to_string()), Some("20".to_string()), None]
    ]
);
pgoutput_join_cell!(
    backfill_inner_join_text_exact_oracle,
    "INNER",
    "TEXT",
    "(1, 'a', 10), (2, 'b', 20)",
    "(3, 'a', 100), (4, 'c', 300)",
    vec![vec![
        Some("a".to_string()),
        Some("10".to_string()),
        Some("100".to_string())
    ]]
);
pgoutput_join_cell!(
    backfill_left_join_text_exact_oracle,
    "LEFT",
    "TEXT",
    "(1, 'a', 10), (2, 'b', 20)",
    "(3, 'a', 100), (4, 'c', 300)",
    vec![
        vec![
            Some("a".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ],
        vec![Some("b".to_string()), Some("20".to_string()), None]
    ]
);
pgoutput_join_cell!(
    backfill_right_join_text_exact_oracle,
    "RIGHT",
    "TEXT",
    "(1, 'a', 10), (2, 'b', 20)",
    "(3, 'a', 100), (4, 'c', 300)",
    vec![
        vec![None, None, Some("300".to_string())],
        vec![
            Some("a".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ]
    ]
);
pgoutput_join_cell!(
    backfill_full_join_text_exact_oracle,
    "FULL",
    "TEXT",
    "(1, 'a', 10), (2, 'b', 20)",
    "(3, 'a', 100), (4, 'c', 300)",
    vec![
        vec![None, None, Some("300".to_string())],
        vec![
            Some("a".to_string()),
            Some("10".to_string()),
            Some("100".to_string())
        ],
        vec![Some("b".to_string()), Some("20".to_string()), None]
    ]
);

async fn pgoutput_window_rows(
    label: &str,
    key_type: &str,
    view_sql: &str,
    values: &str,
    estimated_rows: usize,
) -> Vec<Vec<Option<String>>> {
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL CDC matrix proof"
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
        .batch_execute(&format!(
        "CREATE TABLE input (id BIGINT PRIMARY KEY, k {key_type}, date_time BIGINT, v BIGINT); \
         ALTER TABLE input REPLICA IDENTITY FULL; CREATE PUBLICATION input_pub FOR TABLE input; \
         INSERT INTO input (id, k, date_time, v) VALUES {values};"
    ))
        .await
        .unwrap();
    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            format!("backfill-pgoutput-window-{label}"),
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let connect = |address: std::net::SocketAddr| async move {
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
    };
    let client = connect(address).await;
    for sql in [
        &format!("CREATE TABLE input (id BIGINT, k {key_type}, date_time BIGINT, v BIGINT)"),
        &format!("CREATE SOURCE input TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='input_pub', slot='matrix_{label}_slot', table='input') FORMAT pgoutput"),
        &format!("CREATE MATERIALIZED VIEW result AS {view_sql}"),
    ] { client.execute(sql, &[]).await.unwrap(); }
    let expected = result_optional_rows(&client).await;
    upstream
        .batch_execute("INSERT INTO input VALUES (99, 99, 0, 11); DELETE FROM input WHERE id = 99;")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = client
            .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW result", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (0..7)
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if result_optional_rows(&client).await == expected
            && status
                == vec![vec![
                    Some("result".to_string()),
                    Some("RUNNING".to_string()),
                    Some("3".to_string()),
                    Some("0".to_string()),
                    Some(estimated_rows.to_string()),
                    Some("ADMITTED".to_string()),
                    None,
                ]]
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "PostgreSQL CDC live window did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let table = catalog.get_table("input").unwrap();
    let source = catalog.get_source("input").unwrap();
    let view = catalog.get_view("result").unwrap();
    drop(client);
    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovered = Arc::new(CatalogStubs::new());
    assert!(recovered.add_table(table));
    assert!(recovered.add_source(source));
    recovered.add_view_with_deps(view, vec!["input".to_string()]);
    let restarted = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        recovered,
        Arc::new(NoopViewReader),
        db,
    );
    let (restarted_address, _restarted_handle) = restarted.serve_background().await.unwrap();
    let restarted_client = connect(restarted_address).await;
    assert_eq!(result_optional_rows(&restarted_client).await, expected);
    expected
}

macro_rules! pgoutput_window_cell {
    ($name:ident, $label:expr, $key:expr, $query:expr, $values:expr, $rows:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(
                pgoutput_window_rows($label, $key, $query, $values, $rows).await,
                $expected
            );
        }
    };
}

async fn tumble_rows(key_type: &str, values: &str) -> Vec<Vec<Option<String>>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-matrix-tumble-{key_type}"), store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE input (id BIGINT, k {key_type}, date_time BIGINT, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO input (id, k, date_time, v) VALUES {values}"
        ))
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW result AS SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT) AS window_start, k, COUNT(v) AS value FROM input GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)), k",
        )
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

macro_rules! tumble_cell {
    ($name:ident, $key:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(tumble_rows($key, $values).await, $expected);
        }
    };
}

pgoutput_window_cell!(
    backfill_tumble_i64_exact_oracle,
    "tumble_i64",
    "BIGINT",
    "SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT) AS window_start, k, COUNT(v) AS value FROM input GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)), k",
    "(1, 1, 0, 10), (2, 1, 5, 20), (3, 1, 11, 30)",
    3,
    vec![
        vec![
            Some("0".to_string()),
            Some("1".to_string()),
            Some("2".to_string())
        ],
        vec![
            Some("10000000000".to_string()),
            Some("1".to_string()),
            Some("1".to_string())
        ]
    ]
);
pgoutput_window_cell!(
    backfill_tumble_text_exact_oracle,
    "tumble_text",
    "TEXT",
    "SELECT CAST(date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)) AS BIGINT) AS window_start, k, COUNT(v) AS value FROM input GROUP BY date_bin(INTERVAL '10 seconds', CAST(date_time AS TIMESTAMP)), k",
    "(1, 'a', 0, 10), (2, 'a', 5, 20), (3, 'a', 11, 30)",
    3,
    vec![
        vec![
            Some("0".to_string()),
            Some("a".to_string()),
            Some("2".to_string())
        ],
        vec![
            Some("10000000000".to_string()),
            Some("a".to_string()),
            Some("1".to_string())
        ]
    ]
);

async fn row_number_rows(key_type: &str, values: &str) -> Vec<Vec<Option<String>>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-matrix-row-number-{key_type}"), store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE input (id BIGINT, k {key_type}, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!("INSERT INTO input (id, k, v) VALUES {values}"))
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW result AS SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM input",
        )
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

macro_rules! row_number_cell {
    ($name:ident, $key:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(row_number_rows($key, $values).await, $expected);
        }
    };
}

pgoutput_window_cell!(
    backfill_row_number_i64_exact_oracle,
    "row_number_i64",
    "BIGINT",
    "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM input",
    "(1, 1, 0, 20), (2, 1, 0, 10), (3, 2, 0, 5)",
    3,
    vec![
        vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("1".to_string())
        ],
        vec![
            Some("1".to_string()),
            Some("20".to_string()),
            Some("2".to_string())
        ],
        vec![
            Some("2".to_string()),
            Some("5".to_string()),
            Some("1".to_string())
        ]
    ]
);
pgoutput_window_cell!(
    backfill_row_number_text_exact_oracle,
    "row_number_text",
    "TEXT",
    "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM input",
    "(1, 'a', 0, 20), (2, 'a', 0, 10), (3, 'b', 0, 5)",
    3,
    vec![
        vec![
            Some("a".to_string()),
            Some("10".to_string()),
            Some("1".to_string())
        ],
        vec![
            Some("a".to_string()),
            Some("20".to_string()),
            Some("2".to_string())
        ],
        vec![
            Some("b".to_string()),
            Some("5".to_string()),
            Some("1".to_string())
        ]
    ]
);

async fn hop_rows(key_type: &str, values: &str) -> Vec<Vec<Option<String>>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-matrix-hop-{key_type}"), store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE input (id BIGINT, k {key_type}, date_time BIGINT, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO input (id, k, date_time, v) VALUES {values}"
        ))
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW result AS SELECT k, COUNT(v) AS value FROM input CROSS JOIN generate_series(0, 1) AS slide(slide_idx) GROUP BY k, date_bin(INTERVAL '10 seconds', CAST(date_time - slide_idx * 5000 AS TIMESTAMP))",
        )
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

macro_rules! hop_cell {
    ($name:ident, $key:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(hop_rows($key, $values).await, $expected);
        }
    };
}

pgoutput_window_cell!(
    backfill_hop_i64_exact_oracle,
    "hop_i64",
    "BIGINT",
    "SELECT k, COUNT(v) AS value FROM input CROSS JOIN generate_series(0, 1) AS slide(slide_idx) GROUP BY k, date_bin(INTERVAL '10 seconds', CAST(date_time - slide_idx * 5000 AS TIMESTAMP))",
    "(1, 1, 6, 10), (2, 1, 11, 20)",
    2,
    vec![
        vec![Some("1".to_string()), Some("2".to_string())],
        vec![Some("1".to_string()), Some("2".to_string())]
    ]
);
pgoutput_window_cell!(
    backfill_hop_text_exact_oracle,
    "hop_text",
    "TEXT",
    "SELECT k, COUNT(v) AS value FROM input CROSS JOIN generate_series(0, 1) AS slide(slide_idx) GROUP BY k, date_bin(INTERVAL '10 seconds', CAST(date_time - slide_idx * 5000 AS TIMESTAMP))",
    "(1, 'a', 6, 10), (2, 'a', 11, 20)",
    2,
    vec![
        vec![Some("a".to_string()), Some("2".to_string())],
        vec![Some("a".to_string()), Some("2".to_string())]
    ]
);

async fn session_rows(key_type: &str, values: &str) -> Vec<Vec<Option<String>>> {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder(format!("backfill-matrix-session-{key_type}"), store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        db,
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE TABLE input (id BIGINT, k {key_type}, date_time BIGINT, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .simple_query(&format!(
            "INSERT INTO input (id, k, date_time, v) VALUES {values}"
        ))
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW result AS SELECT k, COUNT(*) AS bid_count, MIN(date_time) AS starttime, MAX(date_time) AS endtime FROM input GROUP BY k, SESSION(date_time, INTERVAL '10 seconds')",
        )
        .await
        .unwrap();
    let mut rows = client
        .simple_query("SELECT * FROM result")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

macro_rules! session_cell {
    ($name:ident, $key:expr, $values:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            assert_eq!(session_rows($key, $values).await, $expected);
        }
    };
}

pgoutput_window_cell!(
    backfill_session_i64_exact_oracle,
    "session_i64",
    "BIGINT",
    "SELECT k, COUNT(*) AS bid_count, MIN(date_time) AS starttime, MAX(date_time) AS endtime FROM input GROUP BY k, SESSION(date_time, INTERVAL '10 seconds')",
    "(1, 1, 0, 10), (2, 1, 5000, 20), (3, 1, 20000, 30)",
    3,
    vec![
        vec![
            Some("1".to_string()),
            Some("1".to_string()),
            Some("20000".to_string()),
            Some("20000".to_string())
        ],
        vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("0".to_string()),
            Some("5000".to_string())
        ]
    ]
);
pgoutput_window_cell!(
    backfill_session_text_exact_oracle,
    "session_text",
    "TEXT",
    "SELECT k, COUNT(*) AS bid_count, MIN(date_time) AS starttime, MAX(date_time) AS endtime FROM input GROUP BY k, SESSION(date_time, INTERVAL '10 seconds')",
    "(1, 'a', 0, 10), (2, 'a', 5000, 20), (3, 'a', 20000, 30)",
    3,
    vec![
        vec![
            Some("a".to_string()),
            Some("1".to_string()),
            Some("20000".to_string()),
            Some("20000".to_string())
        ],
        vec![
            Some("a".to_string()),
            Some("2".to_string()),
            Some("0".to_string()),
            Some("5000".to_string())
        ]
    ]
);
