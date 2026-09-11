//! Matrix B & Slice 5 tests: DML Statement × Execution Protocol Parity,
//! Parameter Binding, and RETURNING Semantics across Simple and Extended Query Protocols.

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

#[tokio::test]
async fn test_insert_simple_protocol() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-ins-simple").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    let msgs = client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'alpha');")
        .await
        .unwrap();

    let tag = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(*tag),
        _ => None,
    });
    assert_eq!(tag, Some(1), "expected INSERT 0 1 command completion tag");

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "alpha".to_string()]]);
}

#[tokio::test]
async fn test_insert_extended_binary_params() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-ins-ext").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    let n = client
        .execute(
            "INSERT INTO t (id, val) VALUES ($1, $2);",
            &[&10i64, &"bravo"],
        )
        .await
        .unwrap();
    assert_eq!(n, 1, "expected 1 row affected");

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert_eq!(r, vec![vec!["10".to_string(), "bravo".to_string()]]);
}

#[tokio::test]
async fn test_insert_returning_extended() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-ins-ret").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    let ret_rows = client
        .query(
            "INSERT INTO t (id, val) VALUES ($1, $2) RETURNING *;",
            &[&42i64, &"charlie"],
        )
        .await
        .unwrap();

    assert_eq!(ret_rows.len(), 1);
    let id: i64 = ret_rows[0].get(0);
    let val: &str = ret_rows[0].get(1);
    assert_eq!(id, 42);
    assert_eq!(val, "charlie");
}

#[tokio::test]
async fn test_update_single_row_simple() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-upd-single-simple").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial');")
        .await
        .unwrap();

    let msgs = client
        .simple_query("UPDATE t SET val = 'updated' WHERE id = 1;")
        .await
        .unwrap();

    let tag = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(*tag),
        _ => None,
    });
    assert_eq!(tag, Some(1), "expected UPDATE 1");

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "updated".to_string()]]);
}

#[tokio::test]
async fn test_update_single_row_extended() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-upd-single-ext").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'v1');")
        .await
        .unwrap();

    let n = client
        .execute("UPDATE t SET val = $1 WHERE id = $2;", &[&"v2", &1i64])
        .await
        .unwrap();
    assert_eq!(n, 1);

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "v2".to_string()]]);
}

#[tokio::test]
async fn test_update_multi_row_simple() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-upd-multi-simple").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    for i in 1..=5 {
        client
            .simple_query(&format!("INSERT INTO t (id, val) VALUES ({i}, 'val_{i}');"))
            .await
            .unwrap();
    }

    let msgs = client
        .simple_query("UPDATE t SET val = 'all_same' WHERE id > 0;")
        .await
        .unwrap();

    let tag = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(*tag),
        _ => None,
    });
    assert_eq!(tag, Some(5), "expected UPDATE 5");

    let mut r = rows(&client, "SELECT id, val FROM t;").await;
    r.sort_by_key(|row| row[0].parse::<i64>().unwrap());
    assert_eq!(r.len(), 5);
    for row in r {
        assert_eq!(row[1], "all_same");
    }
}

#[tokio::test]
async fn test_update_returning_multi_row() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-upd-ret-multi").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, score BIGINT);")
        .await
        .unwrap();
    for i in 1..=5 {
        client
            .simple_query(&format!("INSERT INTO t (id, score) VALUES ({i}, {i}0);"))
            .await
            .unwrap();
    }

    let ret_rows = client
        .query(
            "UPDATE t SET score = score + $1 WHERE id > 0 RETURNING id, score;",
            &[&5i64],
        )
        .await
        .unwrap();

    assert_eq!(ret_rows.len(), 5, "expected 5 returning rows");
    let mut results: Vec<(i64, i64)> = ret_rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect();
    results.sort_by_key(|r| r.0);
    assert_eq!(results, vec![(1, 15), (2, 25), (3, 35), (4, 45), (5, 55)]);
}

#[tokio::test]
async fn test_update_no_match_simple() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-upd-nomatch-simple").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial');")
        .await
        .unwrap();

    let msgs = client
        .simple_query("UPDATE t SET val = 'new' WHERE id = 999;")
        .await
        .unwrap();

    let tag = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(*tag),
        _ => None,
    });
    assert_eq!(tag, Some(0), "expected UPDATE 0");
}

#[tokio::test]
async fn test_update_no_match_returning() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-upd-nomatch-ret").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial');")
        .await
        .unwrap();

    let ret_rows = client
        .query(
            "UPDATE t SET val = $1 WHERE id = $2 RETURNING *;",
            &[&"new", &999i64],
        )
        .await
        .unwrap();

    assert_eq!(
        ret_rows.len(),
        0,
        "expected 0 rows for non-matching UPDATE RETURNING"
    );
}

#[tokio::test]
async fn test_delete_single_row_simple() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-del-single-simple").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'alpha');")
        .await
        .unwrap();

    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1;")
        .await
        .unwrap();

    let tag = msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::CommandComplete(tag) => Some(*tag),
        _ => None,
    });
    assert_eq!(tag, Some(1), "expected DELETE 1");

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert!(r.is_empty(), "table should be empty");
}

#[tokio::test]
async fn test_delete_multi_row_extended() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-del-multi-ext").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    for i in 1..=4 {
        client
            .simple_query(&format!("INSERT INTO t (id, val) VALUES ({i}, 'del_{i}');"))
            .await
            .unwrap();
    }

    let n = client
        .execute("DELETE FROM t WHERE id <= $1;", &[&4i64])
        .await
        .unwrap();
    assert_eq!(n, 4, "expected DELETE 4");

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert!(r.is_empty(), "all 4 rows should be deleted");
}

#[tokio::test]
async fn test_delete_returning_pre_image() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-del-ret-pre").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (10, 'first'), (20, 'second');")
        .await
        .unwrap();

    let msgs = client
        .simple_query("DELETE FROM t WHERE id > 0 RETURNING *;")
        .await
        .unwrap();

    let mut ret_rows: Vec<Vec<String>> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(vec![
                r.get(0).unwrap().to_string(),
                r.get(1).unwrap().to_string(),
            ]),
            _ => None,
        })
        .collect();
    ret_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());

    assert_eq!(
        ret_rows,
        vec![
            vec!["10".to_string(), "first".to_string()],
            vec!["20".to_string(), "second".to_string()],
        ],
        "DELETE RETURNING should return exact pre-image row values"
    );

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert!(r.is_empty());
}

#[tokio::test]
async fn test_delete_no_match_returning() {
    let (port, _handle, _shard_db, _store) =
        start_gateway_with_shard("parity-del-nomatch-ret").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'keep');")
        .await
        .unwrap();

    let ret_rows = client
        .query("DELETE FROM t WHERE id = $1 RETURNING id;", &[&999i64])
        .await
        .unwrap();
    assert_eq!(ret_rows.len(), 0);

    let r = rows(&client, "SELECT id, val FROM t;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "keep".to_string()]]);
}

#[tokio::test]
async fn test_prepared_update_execution() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-prepared-upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial_1'), (2, 'initial_2');")
        .await
        .unwrap();

    let stmt = client
        .prepare("UPDATE t SET val = $1 WHERE id = $2;")
        .await
        .expect("prepare statement");

    let n1 = client.execute(&stmt, &[&"p1", &1i64]).await.unwrap();
    assert_eq!(n1, 1);

    let n2 = client.execute(&stmt, &[&"p2", &2i64]).await.unwrap();
    assert_eq!(n2, 1);

    let mut r = rows(&client, "SELECT id, val FROM t;").await;
    r.sort_by_key(|row| row[0].parse::<i64>().unwrap());
    assert_eq!(
        r,
        vec![
            vec!["1".to_string(), "p1".to_string()],
            vec!["2".to_string(), "p2".to_string()],
        ]
    );
}

#[tokio::test]
async fn test_extended_protocol_parameter_binding_and_returning_parity() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-full-run").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT, count BIGINT);")
        .await
        .unwrap();

    // Insert via extended
    client
        .execute(
            "INSERT INTO t (id, val, count) VALUES ($1, $2, $3);",
            &[&100i64, &"item_100", &5i64],
        )
        .await
        .unwrap();

    // Update with arithmetic and returning
    let res = client
        .query(
            "UPDATE t SET val = $1, count = count + $2 WHERE id = $3 RETURNING val, count;",
            &[&"item_updated", &10i64, &100i64],
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    let v: &str = res[0].get(0);
    let c: i64 = res[0].get(1);
    assert_eq!(v, "item_updated");
    assert_eq!(c, 15);
}

#[tokio::test]
async fn test_no_textual_prefix_dispatch_used() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("parity-prefix-bypass").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    // 1. DML with leading SQL comments
    client
        .simple_query("/* leading comment */ INSERT INTO t (id, val) VALUES (1, 'from_comment');")
        .await
        .expect("INSERT with leading comment must succeed");

    // 2. Mixed case and leading whitespace/newlines
    client
        .simple_query("\n\t  UpDaTe t SET val = 'mixed_case' WHERE id = 1;")
        .await
        .expect("UPDATE with leading whitespace and mixed case must succeed");

    let r = rows(&client, "SELECT val FROM t WHERE id = 1;").await;
    assert_eq!(r, vec![vec!["mixed_case".to_string()]]);

    // 3. DELETE with block comment
    client
        .simple_query("/* before delete */ DELETE FROM t WHERE id = 1;")
        .await
        .expect("DELETE with leading block comment must succeed");

    let r_empty = rows(&client, "SELECT val FROM t WHERE id = 1;").await;
    assert!(r_empty.is_empty(), "row must be deleted");
}
