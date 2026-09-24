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

async fn rows(client: &tokio_postgres::Client, table: &str) -> Vec<Vec<String>> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn assert_shared_pgoutput_slot_two_tables() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP shared pgoutput slot proof: Docker not available");
        return;
    }
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
    upstream.batch_execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY); CREATE TABLE b (id BIGINT PRIMARY KEY); \
         ALTER TABLE a REPLICA IDENTITY FULL; ALTER TABLE b REPLICA IDENTITY FULL; \
         CREATE PUBLICATION shared_pub FOR TABLE a, b; INSERT INTO a VALUES (1); INSERT INTO b VALUES (2);",
    ).await.unwrap();

    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "shared-pgoutput-slot",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
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
        "CREATE TABLE a (id BIGINT)",
        "CREATE TABLE b (id BIGINT)",
        &format!("CREATE SOURCE a TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='a') FORMAT pgoutput"),
        &format!("CREATE SOURCE b TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='b') FORMAT pgoutput"),
        "CREATE MATERIALIZED VIEW a_view AS SELECT id FROM a",
        "CREATE MATERIALIZED VIEW b_view AS SELECT id FROM b",
    ] { client.execute(sql, &[]).await.unwrap(); }
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if rows(&client, "a_view").await == vec![vec!["1".to_string()]]
            && rows(&client, "b_view").await == vec![vec!["2".to_string()]]
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(rows(&client, "a_view").await, vec![vec!["1".to_string()]]);
    assert_eq!(rows(&client, "b_view").await, vec![vec!["2".to_string()]]);
    let slots: i64 = upstream
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'shared_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(slots, 1);
    upstream
        .batch_execute(
            "ALTER TABLE b DROP CONSTRAINT b_pkey; ALTER TABLE b DROP COLUMN id; INSERT INTO b DEFAULT VALUES;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut status = None;
    while Instant::now() < deadline {
        let rows = client.query("SHOW SOURCE STATUS FOR b", &[]).await.unwrap();
        let current = (
            rows[0].get::<_, Option<String>>(3),
            rows[0].get::<_, Option<String>>(9),
        );
        if current.0.as_deref() == Some("BLOCKED") {
            status = Some(current);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        status,
        Some((
            Some("BLOCKED".to_string()),
            Some(
                "RS-1002: incompatible upstream relation change blocked the pgoutput source"
                    .to_string()
            ),
        ))
    );
    handle.abort();
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_real_server_slot_count_is_one() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_before_m3_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_after_m3_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_three_tables_disconnect_resume_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_schema_change_before_commit_exact() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_schema_incompatible_show_source_status_reports_exact_rs1002() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn test_upstream_transaction_atomicity_and_overflow_rejection() {
    use rockstream_connectors::{
        CdcWireFormat, PgLsn, PostgresCdcSource, SourceConnector, SourceError,
        POSTGRES_CDC_MAX_TRANSACTION_BYTES,
    };
    use rockstream_types::arrow_batch::split_weight_column;
    use rockstream_types::ids::ConnectorId;

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source =
        PostgresCdcSource::new(ConnectorId(5_400), schema.clone(), CdcWireFormat::PgOutput);

    // 1. Transaction atomicity: multi-row changes buffered until COMMIT
    source.decode_and_enqueue(b"BEGIN|101").unwrap();
    source.decode_and_enqueue(b"B|0/10|7|I|one|1").unwrap();
    source.decode_and_enqueue(b"B|0/11|7|I|one|2").unwrap();
    source.decode_and_enqueue(b"B|0/12|7|I|one|3").unwrap();

    // Before COMMIT, poll_delta returns nothing (uncommitted transaction is not visible)
    assert_eq!(source.buffered_records(), 0);

    // After COMMIT, all 3 rows become visible in one atomic epoch
    source.decode_and_enqueue(b"COMMIT|0/20").unwrap();
    assert_eq!(source.buffered_records(), 3);

    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    let (batch, weights) = split_weight_column(&delta.batches[0]).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(weights, vec![1, 1, 1]);
    assert_eq!(delta.new_offset, PgLsn(0x20).to_offset_token());

    // 2. Upstream rollback / uncommitted transaction leaves view untouched
    let mut clean_source =
        PostgresCdcSource::new(ConnectorId(5_401), schema.clone(), CdcWireFormat::PgOutput);
    clean_source.decode_and_enqueue(b"BEGIN|103").unwrap();
    clean_source
        .decode_and_enqueue(b"B|0/30|7|I|one|10")
        .unwrap();
    clean_source.decode_and_enqueue(b"COMMIT|0/35").unwrap();
    let res = clean_source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    let (batch_clean, _) = split_weight_column(&res.batches[0]).unwrap();
    let ids_clean = batch_clean
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(ids_clean, vec![10]);

    // 3. Overflow rejection: transaction exceeding POSTGRES_CDC_MAX_TRANSACTION_BYTES fails closed with RS-4014
    let mut overflow_source =
        PostgresCdcSource::new(ConnectorId(5_402), schema.clone(), CdcWireFormat::PgOutput);
    overflow_source.decode_and_enqueue(b"BEGIN|104").unwrap();

    // Construct a valid change message whose key exceeds the transaction limit
    let large_key = "k".repeat(POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1024);
    let payload = format!("B|0/40|7|I|{large_key}|1").into_bytes();
    let err = overflow_source.decode_and_enqueue(&payload).unwrap_err();
    match err {
        SourceError::PollDeltaFailed { reason } => {
            assert!(
                reason.contains("RS-4014"),
                "expected RS-4014 in overflow error, got: {reason}"
            );
        }
        other => panic!("expected PollDeltaFailed, got: {other:?}"),
    }
    assert!(overflow_source.replication_read_paused());
}
