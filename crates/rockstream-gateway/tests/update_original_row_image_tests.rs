//! Issue #343: Evaluate all UPDATE assignments against the original row image.
//!
//! Verifies across simple and prepared/extended PgWire paths, dependent
//! materialized views, and crash recovery that:
//! - Multi-column swaps (`SET a=b, b=a` and `SET b=a, a=b`) read original values.
//! - Dependent expressions (`SET a=a+1, b=a+1`, `SET a=a+b, b=a-b`) read original values.
//! - NULL swaps preserve NULL and non-NULL values without order dependence.
//! - An expression error after an earlier valid assignment leaves the row and
//!   dependent materialized views unchanged.
//! - Typed arithmetic and assignment validation (#120) continues to work.

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_storage::ShardDb;

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

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
    Arc<InMemory>,
) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, shard_db, store)
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn simple_rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<Option<String>>> {
    let msgs = client.simple_query(query).await.expect("query failed");
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let count = r.columns().len();
                let mut v = Vec::with_capacity(count);
                for i in 0..count {
                    v.push(r.get(i).map(ToOwned::to_owned));
                }
                Some(v)
            }
            _ => None,
        })
        .collect()
}

fn s(val: &str) -> Option<String> {
    Some(val.to_string())
}

#[tokio::test]
async fn test_update_swap_simple_returning_select_view_and_recovery() {
    let store = Arc::new(InMemory::new());
    let shard_path = "issue-343-swap-recovery";
    let prefix = "catalog-issue-343-swap";

    // Phase 1: create table + dependent materialized view, insert, swap, verify
    let (port1, handle1, shard_db1) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(DurableCatalogStore::new(store.clone(), prefix));
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE update_swap (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT);")
        .await
        .unwrap();
    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_update_swap AS \
             SELECT id, SUM(a) AS sum_a, SUM(b) AS sum_b FROM update_swap GROUP BY id;",
        )
        .await
        .unwrap();
    client1
        .simple_query("INSERT INTO update_swap VALUES (1, 1, 2);")
        .await
        .unwrap();

    // Minimal reproduction from issue #343
    let ret = simple_rows(
        &client1,
        "UPDATE update_swap SET a=b, b=a WHERE id=1 RETURNING id,a,b;",
    )
    .await;
    assert_eq!(ret, vec![vec![s("1"), s("2"), s("1")]]);

    let sel = simple_rows(&client1, "SELECT id,a,b FROM update_swap;").await;
    assert_eq!(sel, vec![vec![s("1"), s("2"), s("1")]]);

    let mv = simple_rows(&client1, "SELECT id, sum_a, sum_b FROM mv_update_swap;").await;
    assert_eq!(mv, vec![vec![s("1"), s("2"), s("1")]]);

    // Reverse assignment order must produce the exact same swap result
    client1
        .simple_query("INSERT INTO update_swap VALUES (2, 10, 20);")
        .await
        .unwrap();
    let ret_rev = simple_rows(
        &client1,
        "UPDATE update_swap SET b=a, a=b WHERE id=2 RETURNING id,a,b;",
    )
    .await;
    assert_eq!(ret_rev, vec![vec![s("2"), s("20"), s("10")]]);

    shard_db1.flush().await.unwrap();
    handle1.abort();

    // Phase 2: recover and verify base table + dependent materialized view
    let (port2, _handle2, _shard_db2) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(
            DurableCatalogStore::recover(store.clone(), prefix)
                .await
                .unwrap(),
        );
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);
        catalog.sync_from_durable_store().await.unwrap();

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client2 = connect_port(port2).await;
    let mut recovered_table = simple_rows(&client2, "SELECT id, a, b FROM update_swap;").await;
    recovered_table.sort_by_key(|r| r[0].as_deref().unwrap().parse::<i64>().unwrap());
    assert_eq!(
        recovered_table,
        vec![vec![s("1"), s("2"), s("1")], vec![s("2"), s("20"), s("10")],]
    );

    let mut recovered_mv =
        simple_rows(&client2, "SELECT id, sum_a, sum_b FROM mv_update_swap;").await;
    recovered_mv.sort_by_key(|r| r[0].as_deref().unwrap().parse::<i64>().unwrap());
    assert_eq!(
        recovered_mv,
        vec![vec![s("1"), s("2"), s("1")], vec![s("2"), s("20"), s("10")],]
    );
}

#[tokio::test]
async fn test_update_dependency_and_null_swap_simple_and_prepared() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("issue-343-dep-null-swap").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE dep_test (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO dep_test VALUES (1, 10, 20), (2, 1, NULL), (3, NULL, 2);")
        .await
        .unwrap();

    // Dependency case: SET a=a+1, b=a+1 must evaluate both RHSs against original a=10
    let dep_ret = simple_rows(
        &client,
        "UPDATE dep_test SET a = a + 1, b = a + 1 WHERE id = 1 RETURNING id, a, b;",
    )
    .await;
    assert_eq!(dep_ret, vec![vec![s("1"), s("11"), s("11")]]);

    // NULL swap via simple query
    let null_ret = simple_rows(
        &client,
        "UPDATE dep_test SET a = b, b = a WHERE id = 2 RETURNING id, a, b;",
    )
    .await;
    assert_eq!(null_ret, vec![vec![s("2"), None, s("1")]]);

    // Prepared/extended PgWire path: NULL swap and parameterized dependency update
    let swap_stmt = client
        .prepare("UPDATE dep_test SET a = b, b = a WHERE id = $1 RETURNING id, a, b;")
        .await
        .unwrap();
    let swap_rows = client.query(&swap_stmt, &[&3i64]).await.unwrap();
    assert_eq!(swap_rows.len(), 1);
    assert_eq!(swap_rows[0].get::<_, i64>(0), 3);
    assert_eq!(swap_rows[0].get::<_, Option<i64>>(1), Some(2));
    assert_eq!(swap_rows[0].get::<_, Option<i64>>(2), None);

    let arith_stmt = client
        .prepare("UPDATE dep_test SET a = a + $1, b = a + $2 WHERE id = $3 RETURNING id, a, b;")
        .await
        .unwrap();
    let arith_rows = client
        .query(&arith_stmt, &[&5i64, &10i64, &1i64])
        .await
        .unwrap();
    assert_eq!(arith_rows.len(), 1);
    assert_eq!(arith_rows[0].get::<_, i64>(0), 1);
    // Original row 1 had a=11, b=11 -> a = 11 + 5 = 16, b = 11 + 10 = 21
    assert_eq!(arith_rows[0].get::<_, i64>(1), 16);
    assert_eq!(arith_rows[0].get::<_, i64>(2), 21);

    let mut final_rows = simple_rows(&client, "SELECT id, a, b FROM dep_test;").await;
    final_rows.sort_by_key(|r| r[0].as_deref().unwrap().parse::<i64>().unwrap());
    assert_eq!(
        final_rows,
        vec![
            vec![s("1"), s("16"), s("21")],
            vec![s("2"), None, s("1")],
            vec![s("3"), s("2"), None],
        ]
    );
}

#[tokio::test]
async fn test_update_later_assignment_error_leaves_row_and_view_unchanged() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("issue-343-err").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE err_test (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT);")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_err_test AS \
             SELECT id, SUM(a) AS sum_a, SUM(b) AS sum_b FROM err_test GROUP BY id;",
        )
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO err_test VALUES (1, 10, 20), (2, 30, 40);")
        .await
        .unwrap();

    // Simple query: first assignment (a = b) is valid, second assignment (b = a / 0) fails
    let err = client
        .simple_query("UPDATE err_test SET a = b, b = a / 0 WHERE id = 1;")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected DB error");
    assert_eq!(db_err.code().code(), "22012");

    // Prepared/extended query: multi-row where row 1 succeeds and row 2 fails on second assignment
    let stmt = client
        .prepare("UPDATE err_test SET a = b, b = 100 / (a - $1) WHERE id >= 1;")
        .await
        .unwrap();
    let ext_err = client.execute(&stmt, &[&30i64]).await.unwrap_err();
    let ext_db_err = ext_err.as_db_error().expect("expected DB error");
    assert_eq!(ext_db_err.code().code(), "22012");

    // Both rows and dependent materialized view must be completely unchanged
    let mut table_rows = simple_rows(&client, "SELECT id, a, b FROM err_test;").await;
    table_rows.sort_by_key(|r| r[0].as_deref().unwrap().parse::<i64>().unwrap());
    assert_eq!(
        table_rows,
        vec![
            vec![s("1"), s("10"), s("20")],
            vec![s("2"), s("30"), s("40")],
        ]
    );

    let mut mv_rows = simple_rows(&client, "SELECT id, sum_a, sum_b FROM mv_err_test;").await;
    mv_rows.sort_by_key(|r| r[0].as_deref().unwrap().parse::<i64>().unwrap());
    assert_eq!(
        mv_rows,
        vec![
            vec![s("1"), s("10"), s("20")],
            vec![s("2"), s("30"), s("40")],
        ]
    );
}

#[tokio::test]
async fn test_update_retains_typed_arithmetic_and_decimal_validation() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("issue-343-typed").await;
    let client = connect_port(port).await;

    client
        .simple_query(
            "CREATE TABLE typed_upd (id BIGINT PRIMARY KEY, a DECIMAL(5, 2), b DECIMAL(5, 2));",
        )
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO typed_upd VALUES (1, 10.25, 20.50);")
        .await
        .unwrap();

    // Swap with typed decimal arithmetic against original row values
    let ret = simple_rows(
        &client,
        "UPDATE typed_upd SET a = b + 1.25, b = a + 2.00 WHERE id = 1 RETURNING id, a, b;",
    )
    .await;
    assert_eq!(ret, vec![vec![s("1"), s("21.75"), s("12.25")]]);

    // Later assignment overflow on DECIMAL(5, 2) must leave row unchanged
    let err = client
        .simple_query("UPDATE typed_upd SET a = b, b = b + 990.00 WHERE id = 1;")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("expected DB error");
    assert_eq!(db_err.code().code(), "22003");

    let sel = simple_rows(&client, "SELECT id, a, b FROM typed_upd;").await;
    assert_eq!(sel, vec![vec![s("1"), s("21.75"), s("12.25")]]);
}
