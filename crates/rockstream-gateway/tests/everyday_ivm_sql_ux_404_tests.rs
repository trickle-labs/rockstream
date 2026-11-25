//! Issue #404 — Complete everyday IVM SQL UX:
//! - R1: Core ORDER BY and LIMIT (materialized Top-K views + SELECT ORDER BY/LIMIT)
//! - R2: SQL-reachable HOP and SESSION windows with shared time-slice reuse
//! - R3: Zero-downtime view evolution (`CREATE OR REPLACE MATERIALIZED VIEW`)
//!   and backfill visibility (`SHOW BACKFILL STATUS`, `rockstream_catalog.backfill_status`)
//! - R4: Core SQL-wired Kafka sink (`CREATE SINK ... FROM <view> INTO KAFKA (...)`)
//!   with chunked snapshots and durable catalog recovery
//! - R5: Clear, actionable RS-* errors for unsupported SQL

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_ops::topk::{TopKOp, TOPK_BUFFER_LIMIT};
use rockstream_storage::ShardDb;
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use rockstream_types::SharedWindowSpec;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

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

async fn start_gateway_in(
    dir: &TempDir,
    catalog: Arc<CatalogStubs>,
) -> (
    tokio_postgres::Client,
    rockstream_gateway::server::SharedWindowHandle,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
) {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("everyday-ivm-404", store)
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
    let shared_handle = server.shared_window_handle();
    let (addr, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, shared_handle, handle, shard_db)
}

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\nerror: {e}"))
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(ToOwned::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn exec_tx(client: &tokio_postgres::Client, idempotency_key: &str, stmts: &[&str]) {
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{idempotency_key}'"
        ))
        .await
        .unwrap();
    for stmt in stmts {
        client
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("TX stmt failed: {stmt}\nerror: {e}"));
    }
    client.simple_query("COMMIT").await.unwrap();
}

// ─── R1: Core ORDER BY and LIMIT (five-behavior semantic ledger proofs) ──────

#[tokio::test]
async fn core_limit_order_by_incremental() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, _, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE scores (id BIGINT, score BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW top3_scores AS \
             SELECT id, score FROM scores ORDER BY score DESC NULLS LAST, id ASC LIMIT 3",
        )
        .await
        .unwrap();

    // Mixed inserts with ties and NULLs.
    exec_tx(
        &client,
        "topk-inc-1",
        &[
            "INSERT INTO scores (id, score) VALUES (1, 100)",
            "INSERT INTO scores (id, score) VALUES (2, 200)",
            "INSERT INTO scores (id, score) VALUES (3, 200)",
            "INSERT INTO scores (id, score) VALUES (4, NULL)",
            "INSERT INTO scores (id, score) VALUES (5, 150)",
        ],
    )
    .await;

    // Exact full ordered output must match batch oracle:
    // (2, 200), (3, 200), (5, 150)
    let rows = simple_rows(
        &client,
        "SELECT id, score FROM top3_scores ORDER BY score DESC NULLS LAST, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("2".into()), Some("200".into())],
            vec![Some("3".into()), Some("200".into())],
            vec![Some("5".into()), Some("150".into())],
        ]
    );

    // Update and delete: delete id=2 (score 200), update id=1 to 250 ->
    // new top 3 must be (1, 250), (3, 200), (5, 150).
    exec_tx(
        &client,
        "topk-inc-2",
        &[
            "DELETE FROM scores WHERE id = 2",
            "UPDATE scores SET score = 250 WHERE id = 1",
        ],
    )
    .await;
    let rows = simple_rows(
        &client,
        "SELECT id, score FROM top3_scores ORDER BY score DESC NULLS LAST, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("250".into())],
            vec![Some("3".into()), Some("200".into())],
            vec![Some("5".into()), Some("150".into())],
        ]
    );

    // Delete all non-null high scores to verify NULLs surface cleanly when in top-K.
    exec_tx(
        &client,
        "topk-inc-3",
        &[
            "DELETE FROM scores WHERE id = 1",
            "DELETE FROM scores WHERE id = 3",
        ],
    )
    .await;
    let rows = simple_rows(
        &client,
        "SELECT id, score FROM top3_scores ORDER BY score DESC NULLS LAST, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("5".into()), Some("150".into())],
            vec![Some("4".into()), None],
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn core_limit_order_by_backfill() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, _, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE items (id BIGINT, price BIGINT)")
        .await
        .unwrap();
    exec_tx(
        &client,
        "topk-bf-1",
        &[
            "INSERT INTO items (id, price) VALUES (10, 50)",
            "INSERT INTO items (id, price) VALUES (11, 20)",
            "INSERT INTO items (id, price) VALUES (12, NULL)",
            "INSERT INTO items (id, price) VALUES (13, 20)",
            "INSERT INTO items (id, price) VALUES (14, 10)",
        ],
    )
    .await;

    // Backfill over pre-existing snapshot with ASC NULLS FIRST LIMIT 3.
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cheapest3 AS \
             SELECT id, price FROM items ORDER BY price ASC NULLS FIRST, id ASC LIMIT 3",
        )
        .await
        .unwrap();

    let rows = simple_rows(
        &client,
        "SELECT id, price FROM cheapest3 ORDER BY price ASC NULLS FIRST, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("12".into()), None],
            vec![Some("14".into()), Some("10".into())],
            vec![Some("11".into()), Some("20".into())],
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn core_limit_order_by_checkpoint_recovery() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    {
        let (client, _, handle, shard_db) = start_gateway_in(&dir, Arc::clone(&catalog)).await;
        client
            .simple_query("CREATE TABLE leaderboard (id BIGINT, pts BIGINT)")
            .await
            .unwrap();
        client
            .simple_query(
                "CREATE MATERIALIZED VIEW top2_lb AS \
                 SELECT id, pts FROM leaderboard ORDER BY pts DESC NULLS LAST, id ASC LIMIT 2",
            )
            .await
            .unwrap();
        exec_tx(
            &client,
            "topk-rec-1",
            &[
                "INSERT INTO leaderboard (id, pts) VALUES (1, 10)",
                "INSERT INTO leaderboard (id, pts) VALUES (2, 30)",
                "INSERT INTO leaderboard (id, pts) VALUES (3, 20)",
            ],
        )
        .await;
        shard_db.flush().await.unwrap();
        handle.abort();
        let _ = handle.await;
        drop(client);
        drop(shard_db);
    }

    // Reopen ShardDb + gateway and recover compiled views.
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("everyday-ivm-404", store)
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
    let (addr, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Exact recovered output before new deltas.
    let rows = simple_rows(
        &client,
        "SELECT id, pts FROM top2_lb ORDER BY pts DESC NULLS LAST, id ASC LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("2".into()), Some("30".into())],
            vec![Some("3".into()), Some("20".into())],
        ]
    );

    // Apply post-recovery delete of id=2 -> id=1 (10) must be refilled from recovered TopK state.
    exec_tx(
        &client,
        "topk-rec-2",
        &["DELETE FROM leaderboard WHERE id = 2"],
    )
    .await;
    let rows = simple_rows(
        &client,
        "SELECT id, pts FROM top2_lb ORDER BY pts DESC NULLS LAST, id ASC LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Some("3".into()), Some("20".into())],
            vec![Some("1".into()), Some("10".into())],
        ]
    );

    handle.abort();
}

#[test]
fn core_limit_order_by_state_growth() {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rockstream_ops::ArrowZSet;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, true),
    ]));
    let op = TopKOp::new(Arc::clone(&schema), 3, 1, vec![]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30), Some(20)])) as ArrayRef,
        ],
    )
    .unwrap();
    let _ = op
        .process_epoch(ArrowZSet::new(batch, vec![1, 1, 1, 1]), 1)
        .unwrap();
    assert_eq!(op.fill_level(), 4);
    assert!(op.state_bytes() > 0);
    assert_eq!(TOPK_BUFFER_LIMIT, 100_000);
}

#[tokio::test]
async fn core_limit_order_by_failure() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, _, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE scores (id BIGINT, score BIGINT)")
        .await
        .unwrap();

    // Unbounded ORDER BY without LIMIT in a materialized view must fail closed with RS-1019.
    let err = client
        .simple_query(
            "CREATE MATERIALIZED VIEW unbounded_sort AS \
             SELECT id, score FROM scores ORDER BY score DESC",
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().unwrap().message();
    assert!(
        msg.contains("RS-1019") && msg.contains("order_by_without_limit"),
        "expected RS-1019 order_by_without_limit error, got: {msg}"
    );

    // LIMIT with OFFSET > 0 must fail closed with RS-1019.
    let err = client
        .simple_query(
            "CREATE MATERIALIZED VIEW offset_sort AS \
             SELECT id, score FROM scores ORDER BY score DESC LIMIT 3 OFFSET 1",
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().unwrap().message();
    assert!(
        msg.contains("RS-1019") && msg.contains("limit_offset_unsupported"),
        "expected RS-1019 limit_offset_unsupported error, got: {msg}"
    );

    handle.abort();
}

// ─── R2: SQL-reachable HOP and SESSION windows + shared slice reuse ──────────

#[tokio::test]
async fn hop_and_session_windows_e2e_and_restart_recovery() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    {
        let (client, _, handle, shard_db) = start_gateway_in(&dir, Arc::clone(&catalog)).await;
        client
            .simple_query("CREATE TABLE events (user_id BIGINT, ts BIGINT, amount BIGINT)")
            .await
            .unwrap();
        client
            .simple_query(
                "CREATE MATERIALIZED VIEW hop_mv AS \
                 SELECT HOP(ts, INTERVAL '5 seconds', INTERVAL '10 seconds') AS w, \
                        COUNT(*) AS cnt, SUM(amount) AS total \
                 FROM events \
                 GROUP BY HOP(ts, INTERVAL '5 seconds', INTERVAL '10 seconds')",
            )
            .await
            .unwrap();
        client
            .simple_query(
                "CREATE MATERIALIZED VIEW sess_mv AS \
                 SELECT user_id, SESSION(ts, INTERVAL '10 seconds') AS s, \
                        COUNT(*) AS cnt, SUM(amount) AS total \
                 FROM events \
                 GROUP BY user_id, SESSION(ts, INTERVAL '10 seconds')",
            )
            .await
            .unwrap();

        // Insert in-order and out-of-order events.
        exec_tx(
            &client,
            "win-1",
            &[
                "INSERT INTO events (user_id, ts, amount) VALUES (1, 10000, 10)",
                "INSERT INTO events (user_id, ts, amount) VALUES (1, 14000, 20)",
                "INSERT INTO events (user_id, ts, amount) VALUES (1, 12000, 5)",
            ],
        )
        .await;

        // Verify HOP(ts, 5s, 10s):
        // ts=10000 -> windows [5000, 15000) and [10000, 20000)
        // ts=12000 -> windows [5000, 15000) and [10000, 20000)
        // ts=14000 -> windows [5000, 15000) and [10000, 20000)
        let hop_rows =
            simple_rows(&client, "SELECT w, cnt, total FROM hop_mv ORDER BY w ASC").await;
        assert_eq!(
            hop_rows,
            vec![
                vec![Some("5000".into()), Some("3".into()), Some("35".into())],
                vec![Some("10000".into()), Some("3".into()), Some("35".into())],
            ]
        );

        // Verify SESSION(ts, 10s): all 3 events for user 1 are within 10s -> 1 session starting at 10000.
        let sess_rows = simple_rows(
            &client,
            "SELECT user_id, s, cnt, total FROM sess_mv ORDER BY user_id ASC, s ASC",
        )
        .await;
        assert_eq!(
            sess_rows,
            vec![vec![
                Some("1".into()),
                Some("10000".into()),
                Some("3".into()),
                Some("35".into()),
            ]]
        );

        shard_db.flush().await.unwrap();
        handle.abort();
        let _ = handle.await;
        drop(client);
        drop(shard_db);
    }

    // Reopen and verify durable state + late/new session events.
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("everyday-ivm-404", store)
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
    let (addr, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Commit a new session after the 10s gap (ts=40000) to advance the watermark,
    // then inject an out-of-order / late event (ts=1000).
    // - For HOP: ts=1000 is behind watermark 40000 (1000 + 10000 <= 40000), so it is dropped.
    // - For SESSION: without an explicit upstream frontier watermark, ts=1000 is within
    //   10s of the 10000 session and merges into [1000, 14000] (exact batch oracle semantics).
    exec_tx(
        &client,
        "win-2",
        &["INSERT INTO events (user_id, ts, amount) VALUES (1, 40000, 100)"],
    )
    .await;
    exec_tx(
        &client,
        "win-3",
        &["INSERT INTO events (user_id, ts, amount) VALUES (1, 1000, 999)"],
    )
    .await;

    let hop_rows_after =
        simple_rows(&client, "SELECT w, cnt, total FROM hop_mv ORDER BY w ASC").await;
    assert_eq!(
        hop_rows_after,
        vec![
            vec![Some("5000".into()), Some("3".into()), Some("35".into())],
            vec![Some("10000".into()), Some("3".into()), Some("35".into())],
            vec![Some("35000".into()), Some("1".into()), Some("100".into())],
            vec![Some("40000".into()), Some("1".into()), Some("100".into())],
        ]
    );

    let sess_rows = simple_rows(
        &client,
        "SELECT user_id, s, cnt, total FROM sess_mv ORDER BY user_id ASC, s ASC",
    )
    .await;
    assert_eq!(
        sess_rows,
        vec![
            vec![
                Some("1".into()),
                Some("1000".into()),
                Some("4".into()),
                Some("1034".into()),
            ],
            vec![
                Some("1".into()),
                Some("40000".into()),
                Some("1".into()),
                Some("100".into()),
            ],
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn correlated_hop_windows_reuse_shared_slices() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, shared_handle, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE telemetry (device_id BIGINT, ts BIGINT, val BIGINT)")
        .await
        .unwrap();

    // Create 10 correlated HOP windows over `telemetry(ts)` sharing a 5-second slide.
    for i in 1..=10 {
        let width_sec = i * 5;
        let sql = format!(
            "CREATE MATERIALIZED VIEW hop_w{i} AS \
             SELECT HOP(ts, INTERVAL '5 seconds', INTERVAL '{width_sec} seconds') AS w, \
                    SUM(val) AS total \
             FROM telemetry \
             GROUP BY HOP(ts, INTERVAL '5 seconds', INTERVAL '{width_sec} seconds')"
        );
        client.simple_query(&sql).await.unwrap();
    }

    let expected_spec = SharedWindowSpec::new(
        "telemetry",
        "default",
        "ts",
        5_000,
        None,
        MergeLawId(2),
        MergeLawVersion(1),
    )
    .unwrap();
    assert_eq!(
        shared_handle.consumer_count(&expected_spec),
        Some(10),
        "all 10 correlated HOP windows must share one physical slice spec"
    );

    handle.abort();
}

// ─── R3: Zero-downtime view replacement + backfill visibility ────────────────

#[tokio::test]
async fn zero_downtime_view_replacement_and_backfill_visibility() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, _, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, customer BIGINT, amount BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW order_totals AS \
             SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer",
        )
        .await
        .unwrap();
    exec_tx(
        &client,
        "rep-1",
        &[
            "INSERT INTO orders (id, customer, amount) VALUES (1, 10, 100)",
            "INSERT INTO orders (id, customer, amount) VALUES (2, 10, 50)",
            "INSERT INTO orders (id, customer, amount) VALUES (3, 20, 75)",
        ],
    )
    .await;

    let before = simple_rows(
        &client,
        "SELECT customer, total FROM order_totals ORDER BY customer ASC",
    )
    .await;
    assert_eq!(
        before,
        vec![
            vec![Some("10".into()), Some("150".into())],
            vec![Some("20".into()), Some("75".into())],
        ]
    );

    // Failed replacement must fail closed with RS-1019 and leave live view intact.
    let err = client
        .simple_query(
            "CREATE OR REPLACE MATERIALIZED VIEW order_totals AS \
             SELECT customer, SUM(amount) AS total FROM missing_table GROUP BY customer",
        )
        .await
        .unwrap_err();
    assert!(
        err.as_db_error().unwrap().message().contains("RS-1019"),
        "expected RS-1019 on invalid replacement"
    );
    let still_live = simple_rows(
        &client,
        "SELECT customer, total FROM order_totals ORDER BY customer ASC",
    )
    .await;
    assert_eq!(still_live, before);

    // Inline atomic zero-downtime replacement (filter amount >= 60).
    client
        .simple_query(
            "CREATE OR REPLACE MATERIALIZED VIEW order_totals AS \
             SELECT customer, SUM(amount) AS total FROM orders WHERE amount >= 60 GROUP BY customer",
        )
        .await
        .unwrap();

    let after = simple_rows(
        &client,
        "SELECT customer, total FROM order_totals ORDER BY customer ASC",
    )
    .await;
    assert_eq!(
        after,
        vec![
            vec![Some("10".into()), Some("100".into())],
            vec![Some("20".into()), Some("75".into())],
        ]
    );

    // New streaming commit after cutover must apply to the replaced definition with zero missed epochs.
    exec_tx(
        &client,
        "rep-2",
        &[
            "INSERT INTO orders (id, customer, amount) VALUES (4, 10, 20)",
            "INSERT INTO orders (id, customer, amount) VALUES (5, 10, 80)",
        ],
    )
    .await;
    let post_delta = simple_rows(
        &client,
        "SELECT customer, total FROM order_totals ORDER BY customer ASC",
    )
    .await;
    assert_eq!(
        post_delta,
        vec![
            vec![Some("10".into()), Some("180".into())],
            vec![Some("20".into()), Some("75".into())],
        ]
    );

    // Verify SHOW BACKFILL STATUS (both single-view and all-views) and rockstream_catalog.backfill_status.
    let single_status = simple_rows(
        &client,
        "SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_totals",
    )
    .await;
    assert_eq!(
        single_status,
        vec![vec![
            Some("order_totals".into()),
            Some("RUNNING".into()),
            None,
            Some("0".into()),
            Some("0".into()),
            Some("ADMITTED".into()),
            None,
        ]]
    );
    let all_status = simple_rows(&client, "SHOW BACKFILL STATUS").await;
    assert_eq!(all_status, single_status);
    let catalog_status =
        simple_rows(&client, "SELECT * FROM rockstream_catalog.backfill_status").await;
    assert_eq!(catalog_status, single_status);

    // Optional ASYNC replacement must keep the view queryable and converge to the new definition.
    client
        .simple_query(
            "CREATE OR REPLACE MATERIALIZED VIEW order_totals WITH (ASYNC = true) AS \
             SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer",
        )
        .await
        .unwrap();

    // Poll briefly until async cutover finishes (every intermediate query must succeed!).
    let expected_final = vec![
        vec![Some("10".into()), Some("250".into())],
        vec![Some("20".into()), Some("75".into())],
    ];
    let mut converged = false;
    for _ in 0..50 {
        let rows = simple_rows(
            &client,
            "SELECT customer, total FROM order_totals ORDER BY customer ASC",
        )
        .await;
        if rows == expected_final {
            converged = true;
            break;
        }
        // Must see either the old live result or the new live result — never an error!
        assert_eq!(rows, post_delta);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        converged,
        "async replacement view did not converge to expected output"
    );

    handle.abort();
}

// ─── R4: SQL-wired Kafka sink DDL validation & error contract ────────────────

#[tokio::test]
async fn sql_create_sink_kafka_validation_and_error_contract() {
    let dir = TempDir::new().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let (client, _, handle, _) = start_gateway_in(&dir, catalog).await;

    client
        .simple_query("CREATE TABLE sales (id BIGINT, amount BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW sales_mv AS SELECT id, amount FROM sales")
        .await
        .unwrap();

    // Missing topic -> actionable RS-4007 error.
    let err = client
        .simple_query("CREATE SINK s1 FROM sales_mv INTO KAFKA (brokers = '127.0.0.1:9092')")
        .await
        .unwrap_err();
    let msg = err.as_db_error().unwrap().message();
    assert!(
        msg.contains("RS-4007") && msg.contains("topic"),
        "expected RS-4007 missing topic error, got: {msg}"
    );

    // Unknown option -> actionable RS-4007 error.
    let err = client
        .simple_query(
            "CREATE SINK s2 FROM sales_mv INTO KAFKA (brokers = '127.0.0.1:9092', topic = 't', bogus = 'x')",
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().unwrap().message();
    assert!(
        msg.contains("RS-4007") && msg.contains("bogus"),
        "expected RS-4007 unknown option error, got: {msg}"
    );

    // Unknown view -> actionable RS-4007 error.
    let err = client
        .simple_query(
            "CREATE SINK s3 FROM no_such_view INTO KAFKA (brokers = '127.0.0.1:9092', topic = 't')",
        )
        .await
        .unwrap_err();
    let msg = err.as_db_error().unwrap().message();
    assert!(
        msg.contains("RS-4007") && msg.contains("no_such_view"),
        "expected RS-4007 unknown view error, got: {msg}"
    );

    // A secret binding must identify the producer's SASL transport.
    let err = client
        .simple_query(
            "CREATE SINK s4 FROM sales_mv INTO KAFKA (brokers = '127.0.0.1:9092', topic = 't', secret = 'kafka_auth')",
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.as_db_error().unwrap().message(),
        "[RS-4007] Kafka sink secret requires security.protocol. Next steps: provide security.protocol = 'SASL_SSL' or 'SASL_PLAINTEXT' with secret = '<name>'."
    );

    // Ensure removed connectors still fail closed with RS-4017 (R6 scope guardrail).
    let err = client
        .simple_query(
            "CREATE SINK s5 FOR VIEW sales_mv TO PARQUET 's3://bucket/out' WITH (catalog = filesystem)",
        )
        .await
        .unwrap_err();
    assert!(
        err.as_db_error().unwrap().message().contains("RS-4017"),
        "removed connector must still fail closed with RS-4017"
    );

    // Keep unused BTreeMap import warning-free for future oracle helpers.
    let _oracle: BTreeMap<i64, i64> = BTreeMap::new();

    handle.abort();
}
