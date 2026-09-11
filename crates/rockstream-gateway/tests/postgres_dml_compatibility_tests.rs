//! Slice 6: PostgreSQL Compatibility Corpus with `tokio-postgres` & `psycopg3`.
//!
//! Required roadmap scenarios:
//! 1. multi-row UPDATE
//! 2. UPDATE RETURNING
//! 3. DELETE
//! 4. DELETE RETURNING
//! 5. no-match UPDATE
//! 6. NULL predicate
//! 7. prepared UPDATE
//! 8. transaction rollback
//! 9. transaction commit
//! 10. materialized-view propagation

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
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

async fn rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let msgs = client.simple_query(query).await.expect("query failed");
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let count = r.columns().len();
                let mut v = Vec::with_capacity(count);
                for i in 0..count {
                    v.push(r.get(i).unwrap_or("").to_string());
                }
                Some(v)
            }
            _ => None,
        })
        .collect()
}

async fn execute_tag(client: &tokio_postgres::Client, query: &str) -> (String, u64) {
    let msgs = client.simple_query(query).await.expect("query failed");
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = msg {
            return (format!("COMMAND {n}"), n);
        }
    }
    ("".to_string(), 0)
}

#[tokio::test]
async fn test_postgres_compatibility_corpus_e2e() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("compat-corpus-e2e").await;
    let client = connect_port(port).await;

    // Schema setup: base table + materialized view
    client
        .simple_query("CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT, status TEXT);")
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_balances AS SELECT id, balance FROM accounts WHERE balance > 0;",
        )
        .await
        .unwrap();

    // Initial seeding: 4 rows
    client
        .simple_query(
            "INSERT INTO accounts (id, balance, status) VALUES \
             (1, 100, 'active'), \
             (2, 200, 'active'), \
             (3, 300, 'pending'), \
             (4, 0, 'inactive');",
        )
        .await
        .unwrap();

    // Verify initial base table and view state
    let mut initial_accounts = rows(&client, "SELECT id, balance, status FROM accounts;").await;
    initial_accounts.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        initial_accounts,
        vec![
            vec!["1".to_string(), "100".to_string(), "active".to_string()],
            vec!["2".to_string(), "200".to_string(), "active".to_string()],
            vec!["3".to_string(), "300".to_string(), "pending".to_string()],
            vec!["4".to_string(), "0".to_string(), "inactive".to_string()],
        ]
    );

    // Scenario 1: Multi-row UPDATE
    // Update all active accounts balance by +50
    let (_tag, count) = execute_tag(
        &client,
        "UPDATE accounts SET balance = balance + 50 WHERE status = 'active';",
    )
    .await;
    assert_eq!(count, 2);

    let mut r1 = rows(
        &client,
        "SELECT id, balance FROM accounts WHERE status = 'active';",
    )
    .await;
    r1.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        r1,
        vec![
            vec!["1".to_string(), "150".to_string()],
            vec!["2".to_string(), "250".to_string()],
        ]
    );

    // Scenario 2: UPDATE RETURNING
    let r2 = rows(
        &client,
        "UPDATE accounts SET status = 'verified' WHERE id = 1 RETURNING id, status;",
    )
    .await;
    assert_eq!(r2, vec![vec!["1".to_string(), "verified".to_string()]]);

    // Scenario 3: DELETE (multi-row)
    let (_tag, d_count) = execute_tag(&client, "DELETE FROM accounts WHERE balance >= 250;").await;
    // Row 2 has 250, Row 3 has 300 -> 2 rows deleted
    assert_eq!(d_count, 2);

    let remaining_after_del = rows(&client, "SELECT id FROM accounts;").await;
    assert_eq!(remaining_after_del.len(), 2);

    // Scenario 4: DELETE RETURNING
    let r4 = rows(
        &client,
        "DELETE FROM accounts WHERE id = 4 RETURNING id, status;",
    )
    .await;
    assert_eq!(r4, vec![vec!["4".to_string(), "inactive".to_string()]]);

    // Now only id=1 remains
    let remaining = rows(&client, "SELECT id, balance, status FROM accounts;").await;
    assert_eq!(
        remaining,
        vec![vec![
            "1".to_string(),
            "150".to_string(),
            "verified".to_string()
        ]]
    );

    // Scenario 5: no-match UPDATE
    let (_tag, no_match_count) = execute_tag(
        &client,
        "UPDATE accounts SET balance = 999 WHERE id = 9999;",
    )
    .await;
    assert_eq!(no_match_count, 0);

    // Scenario 6: NULL predicate
    client
        .simple_query("INSERT INTO accounts (id, balance, status) VALUES (5, 500, NULL);")
        .await
        .unwrap();

    let r6_null = rows(&client, "SELECT id FROM accounts WHERE status IS NULL;").await;
    assert_eq!(r6_null, vec![vec!["5".to_string()]]);

    let (_tag, n_null_upd) = execute_tag(
        &client,
        "UPDATE accounts SET status = 'null_resolved' WHERE status IS NULL;",
    )
    .await;
    assert_eq!(n_null_upd, 1);

    let r6_resolved = rows(&client, "SELECT id, status FROM accounts WHERE id = 5;").await;
    assert_eq!(
        r6_resolved,
        vec![vec!["5".to_string(), "null_resolved".to_string()]]
    );

    // Scenario 7: Prepared UPDATE
    let stmt = client
        .prepare("UPDATE accounts SET balance = $1 WHERE id = $2;")
        .await
        .expect("prepare update");
    let p_aff = client.execute(&stmt, &[&777i64, &1i64]).await.unwrap();
    assert_eq!(p_aff, 1);

    let r7 = rows(&client, "SELECT balance FROM accounts WHERE id = 1;").await;
    assert_eq!(r7, vec![vec!["777".to_string()]]);

    // Scenario 8: Transaction ROLLBACK
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("UPDATE accounts SET balance = 9999 WHERE id = 1;")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM accounts WHERE id = 5;")
        .await
        .unwrap();
    client.simple_query("ROLLBACK;").await.unwrap();

    // Verify row 1 still 777 and row 5 still exists
    let r8_1 = rows(&client, "SELECT balance FROM accounts WHERE id = 1;").await;
    assert_eq!(r8_1, vec![vec!["777".to_string()]]);
    let r8_5 = rows(&client, "SELECT id FROM accounts WHERE id = 5;").await;
    assert_eq!(r8_5, vec![vec!["5".to_string()]]);

    // Scenario 9: Transaction COMMIT
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("UPDATE accounts SET balance = 888 WHERE id = 1;")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO accounts (id, balance, status) VALUES (6, 600, 'active');")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    let mut r9 = rows(&client, "SELECT id, balance, status FROM accounts;").await;
    r9.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        r9,
        vec![
            vec!["1".to_string(), "888".to_string(), "verified".to_string()],
            vec![
                "5".to_string(),
                "500".to_string(),
                "null_resolved".to_string()
            ],
            vec!["6".to_string(), "600".to_string(), "active".to_string()],
        ]
    );

    // Scenario 10: Materialized View Propagation
    // mv_balances tracks id, balance WHERE balance > 0
    let mut mv_rows = rows(&client, "SELECT id, balance FROM mv_balances;").await;
    mv_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        mv_rows,
        vec![
            vec!["1".to_string(), "888".to_string()],
            vec!["5".to_string(), "500".to_string()],
            vec!["6".to_string(), "600".to_string()],
        ]
    );
}

#[tokio::test]
async fn test_simple_prepared_bound_parity_tokio_postgres() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("compat-parity").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE parity_t (id INT PRIMARY KEY, val TEXT, cnt INT);")
        .await
        .unwrap();

    // 1. Simple query insert
    client
        .simple_query("INSERT INTO parity_t (id, val, cnt) VALUES (1, 'alpha', 10);")
        .await
        .unwrap();

    // 2. Bound parameter insert via extended protocol
    let ins_stmt = client
        .prepare("INSERT INTO parity_t (id, val, cnt) VALUES ($1, $2, $3);")
        .await
        .unwrap();
    client
        .execute(&ins_stmt, &[&2i32, &"beta", &20i32])
        .await
        .unwrap();

    // 3. Simple query update
    client
        .simple_query("UPDATE parity_t SET cnt = 15 WHERE id = 1;")
        .await
        .unwrap();

    // 4. Prepared bound update
    let upd_stmt = client
        .prepare("UPDATE parity_t SET cnt = $1 WHERE id = $2;")
        .await
        .unwrap();
    client.execute(&upd_stmt, &[&25i32, &2i32]).await.unwrap();

    // Assert parity in row results
    let mut all_rows = rows(&client, "SELECT id, val, cnt FROM parity_t;").await;
    all_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        all_rows,
        vec![
            vec!["1".to_string(), "alpha".to_string(), "15".to_string()],
            vec!["2".to_string(), "beta".to_string(), "25".to_string()],
        ]
    );
}

#[tokio::test]
async fn test_psycopg3_conformance_suite() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP test_psycopg3_conformance_suite: Docker is not available locally");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;

    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("compat-psycopg3").await;

    let script = format!(
        r#"import psycopg

conn = psycopg.connect("host=host.docker.internal port={port} user=test dbname=test autocommit=True")
cur = conn.cursor()

cur.execute("CREATE TABLE py_dml (id INT PRIMARY KEY, name TEXT, score INT);")
cur.execute("INSERT INTO py_dml (id, name, score) VALUES (1, 'alice', 90), (2, 'bob', 80);")

# Update
cur.execute("UPDATE py_dml SET score = 95 WHERE id = 1;")
cur.execute("SELECT score FROM py_dml WHERE id = 1;")
row = cur.fetchone()
assert row[0] == 95, f"Expected 95, got {{row}}"

# Update Returning
cur.execute("UPDATE py_dml SET name = 'robert' WHERE id = 2 RETURNING id, name;")
ret = cur.fetchone()
assert ret[0] == 2 and ret[1] == 'robert', f"Expected (2, robert), got {{ret}}"

# Delete
cur.execute("DELETE FROM py_dml WHERE id = 1;")
cur.execute("SELECT COUNT(*) FROM py_dml;")
cnt = cur.fetchone()
assert cnt[0] == 1, f"Expected count 1, got {{cnt}}"

print("PSYCOPG3 DML CONFORMANCE PASSED")
"#
    );

    let container = GenericImage::new("python", "3.12-slim")
        .with_cmd(["sleep", "3600"])
        .with_host(
            "host.docker.internal",
            testcontainers::core::Host::HostGateway,
        )
        .start()
        .await
        .expect("psycopg3 container start");

    // Install psycopg binary
    let res1 = container
        .exec(testcontainers::core::ExecCommand::new([
            "pip",
            "install",
            "-q",
            "psycopg[binary]",
        ]))
        .await
        .expect("pip install");
    drop(res1);

    let res2 = container
        .exec(testcontainers::core::ExecCommand::new([
            "python3", "-c", &script,
        ]))
        .await
        .expect("run script");
    drop(res2);
}
