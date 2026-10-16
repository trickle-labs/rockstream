use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{BatchOp, ShardDb};
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

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

const MINIO_BUCKET: &str = "rockstream-delete-durability-test";

#[derive(Debug, PartialEq)]
struct QueryReply {
    columns: Vec<(String, u32, i32)>,
    rows: Vec<Vec<Option<String>>>,
    tags: Vec<String>,
    error: Option<(String, String)>,
}

fn reply(columns: &[(&str, u32)], rows: &[&[Option<&str>]], tag: &str) -> QueryReply {
    QueryReply {
        columns: columns
            .iter()
            .map(|(name, oid)| (name.to_string(), *oid, 0))
            .collect(),
        rows: rows
            .iter()
            .map(|row| row.iter().map(|v| v.map(str::to_string)).collect())
            .collect(),
        tags: vec![tag.to_string()],
        error: None,
    }
}

// Preserve raw command tags and SQL NULLs, which tokio-postgres's simple-query API discards.
struct WireClient(tokio::net::TcpStream);

fn wire_string(bytes: &mut &[u8]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap();
    let value = std::str::from_utf8(&bytes[..end]).unwrap().to_string();
    *bytes = &bytes[end + 1..];
    value
}

impl WireClient {
    async fn connect(port: u16) -> Self {
        use tokio::io::AsyncWriteExt;
        let mut client = Self(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap(),
        );
        let params = b"user\0test\0database\0test\0\0";
        client.0.write_u32((8 + params.len()) as u32).await.unwrap();
        client.0.write_u32(196608).await.unwrap();
        client.0.write_all(params).await.unwrap();
        loop {
            let (tag, body) = client.message().await;
            assert_ne!(tag, b'E', "startup failed: {body:?}");
            if tag == b'Z' {
                break;
            }
        }
        client
    }

    async fn message(&mut self) -> (u8, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        let tag = self.0.read_u8().await.unwrap();
        let len = self.0.read_u32().await.unwrap();
        let mut body = vec![0; len as usize - 4];
        self.0.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn query(&mut self, sql: &str) -> QueryReply {
        use bytes::Buf;
        use tokio::io::AsyncWriteExt;
        self.0.write_u8(b'Q').await.unwrap();
        self.0.write_u32((sql.len() + 5) as u32).await.unwrap();
        self.0.write_all(sql.as_bytes()).await.unwrap();
        self.0.write_u8(0).await.unwrap();
        let mut result = QueryReply {
            columns: vec![],
            rows: vec![],
            tags: vec![],
            error: None,
        };
        loop {
            let (tag, body) =
                tokio::time::timeout(std::time::Duration::from_secs(30), self.message())
                    .await
                    .unwrap_or_else(|_| panic!("query timed out: {sql}"));
            let mut body = body.as_slice();
            match tag {
                b'T' => {
                    for _ in 0..body.get_i16() {
                        let name = wire_string(&mut body);
                        assert_eq!((body.get_u32(), body.get_i16()), (0, 0));
                        let oid = body.get_u32();
                        let _size = body.get_i16();
                        let modifier = body.get_i32();
                        assert_eq!(body.get_i16(), 0);
                        result.columns.push((name, oid, modifier));
                    }
                }
                b'D' => {
                    let mut row = vec![];
                    for _ in 0..body.get_i16() {
                        let len = body.get_i32();
                        row.push(if len == -1 {
                            None
                        } else {
                            let value = std::str::from_utf8(&body[..len as usize])
                                .unwrap()
                                .to_string();
                            body.advance(len as usize);
                            Some(value)
                        });
                    }
                    result.rows.push(row);
                }
                b'C' => result.tags.push(wire_string(&mut body)),
                b'E' => {
                    let (mut code, mut message) = (String::new(), String::new());
                    while body[0] != 0 {
                        let field = body.get_u8();
                        let value = wire_string(&mut body);
                        match field {
                            b'C' => code = value,
                            b'M' => message = value,
                            _ => {}
                        }
                    }
                    result.error = Some((code, message));
                }
                b'Z' => break,
                _ => panic!("unexpected query message: {tag}"),
            }
        }
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_dml_types_preserve_exact_rows_through_rollback_and_reopen_lfs() {
    use rockstream_storage::catalog::DurableCatalogStore;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    catalog.set_durable_store(Arc::new(DurableCatalogStore::new(
        store.clone(),
        "typed-catalog",
    )));
    let (port, handle, db) = start_gateway("typed-dml", store.clone(), catalog.clone()).await;
    let mut client = WireClient::connect(port).await;
    let text_schema = [("id", 20), ("v", 25)];
    let original: &[&[Option<&str>]] = &[
        &[Some("1"), Some("01")],
        &[Some("2"), Some("1")],
        &[Some("3"), Some("true")],
        &[Some("4"), Some("t")],
    ];
    assert_eq!(
        client
            .query("CREATE TABLE typed_text (id BIGINT PRIMARY KEY, v TEXT)")
            .await,
        reply(&[], &[], "CREATE TABLE 0")
    );
    assert_eq!(
        client
            .query("INSERT INTO typed_text VALUES (1,'01'),(2,'1'),(3,'true'),(4,'t')")
            .await,
        reply(&[], &[], "INSERT 0 4")
    );
    assert_eq!(
        client
            .query("DELETE FROM typed_text WHERE v = '1' RETURNING *")
            .await,
        reply(&text_schema, &[&[Some("2"), Some("1")]], "DELETE 1")
    );
    assert_eq!(
        client.query("SELECT * FROM typed_text ORDER BY id").await,
        reply(
            &text_schema,
            &[original[0], original[2], original[3]],
            "SELECT 3"
        )
    );
    assert_eq!(
        client.query("INSERT INTO typed_text VALUES (2,'1')").await,
        reply(&[], &[], "INSERT 0 1")
    );
    assert_eq!(
        client.query("SELECT * FROM typed_text ORDER BY id").await,
        reply(&text_schema, original, "SELECT 4")
    );

    assert_eq!(client.query("BEGIN").await, reply(&[], &[], "BEGIN"));
    assert_eq!(
        client
            .query("DELETE FROM typed_text WHERE v = 'true' RETURNING *")
            .await,
        reply(&text_schema, &[original[2]], "DELETE 1")
    );
    assert_eq!(
        client
            .query("UPDATE typed_text SET id = id RETURNING *")
            .await,
        reply(
            &text_schema,
            &[original[0], original[1], original[3]],
            "UPDATE 3"
        )
    );
    assert_eq!(client.query("ROLLBACK").await, reply(&[], &[], "ROLLBACK"));
    assert_eq!(
        client.query("SELECT * FROM typed_text ORDER BY id").await,
        reply(&text_schema, original, "SELECT 4")
    );

    assert_eq!(client.query("BEGIN").await, reply(&[], &[], "BEGIN"));
    assert_eq!(
        client
            .query("UPDATE typed_text SET v = 'changed' WHERE v = 'true' RETURNING *")
            .await,
        reply(&text_schema, &[&[Some("3"), Some("changed")]], "UPDATE 1")
    );
    assert_eq!(
        client
            .query("UPDATE typed_text SET v = 'one' WHERE v = '1' RETURNING *")
            .await,
        reply(&text_schema, &[&[Some("2"), Some("one")]], "UPDATE 1")
    );
    assert_eq!(
        client
            .query("UPDATE typed_text SET id = id RETURNING *")
            .await,
        reply(
            &text_schema,
            &[
                original[0],
                &[Some("2"), Some("one")],
                &[Some("3"), Some("changed")],
                original[3]
            ],
            "UPDATE 4"
        )
    );
    assert_eq!(client.query("ROLLBACK").await, reply(&[], &[], "ROLLBACK"));
    assert_eq!(
        client.query("INSERT INTO typed_text VALUES (5,NULL)").await,
        reply(&[], &[], "INSERT 0 1")
    );
    assert_eq!(
        client
            .query("DELETE FROM typed_text WHERE v = NULL RETURNING *")
            .await,
        reply(&text_schema, &[], "DELETE 0")
    );
    assert_eq!(
        client
            .query("UPDATE typed_text SET v = 'bad' WHERE v = NULL RETURNING *")
            .await,
        reply(&text_schema, &[], "UPDATE 0")
    );
    assert_eq!(client.query("BEGIN").await, reply(&[], &[], "BEGIN"));
    assert_eq!(
        client
            .query("UPDATE typed_text SET v = 'null changed' WHERE v IS NULL RETURNING *")
            .await,
        reply(
            &text_schema,
            &[&[Some("5"), Some("null changed")]],
            "UPDATE 1"
        )
    );
    assert_eq!(client.query("ROLLBACK").await, reply(&[], &[], "ROLLBACK"));
    assert_eq!(
        client
            .query("DELETE FROM typed_text WHERE v = '1' RETURNING *")
            .await,
        reply(&text_schema, &[original[1]], "DELETE 1")
    );
    assert_eq!(
        client
            .query("UPDATE typed_text SET v = 'changed' WHERE v = 'true' RETURNING *")
            .await,
        reply(&text_schema, &[&[Some("3"), Some("changed")]], "UPDATE 1")
    );
    assert_eq!(
        client
            .query("DELETE FROM typed_text WHERE v IS NULL RETURNING *")
            .await,
        reply(&text_schema, &[&[Some("5"), None]], "DELETE 1")
    );
    let final_text = reply(
        &text_schema,
        &[original[0], &[Some("3"), Some("changed")], original[3]],
        "SELECT 3",
    );
    assert_eq!(
        client.query("SELECT * FROM typed_text ORDER BY id").await,
        final_text
    );

    let double_schema = [("id", 20), ("v", 701)];
    assert_eq!(
        client
            .query("CREATE TABLE typed_double (id BIGINT PRIMARY KEY, v DOUBLE PRECISION)")
            .await,
        reply(&[], &[], "CREATE TABLE 0")
    );
    assert_eq!(
        client
            .query("INSERT INTO typed_double VALUES (1,0),(2,1e-17)")
            .await,
        reply(&[], &[], "INSERT 0 2")
    );
    assert_eq!(client.query("BEGIN").await, reply(&[], &[], "BEGIN"));
    assert_eq!(
        client
            .query("UPDATE typed_double SET v = 1 WHERE v = 0 RETURNING *")
            .await,
        reply(&double_schema, &[&[Some("1"), Some("1")]], "UPDATE 1")
    );
    assert_eq!(client.query("ROLLBACK").await, reply(&[], &[], "ROLLBACK"));
    assert_eq!(
        client
            .query("DELETE FROM typed_double WHERE v = 0 RETURNING *")
            .await,
        reply(&double_schema, &[&[Some("1"), Some("0")]], "DELETE 1")
    );
    let final_double = reply(
        &double_schema,
        &[&[Some("2"), Some("0.00000000000000001")]],
        "SELECT 1",
    );
    assert_eq!(
        client.query("SELECT * FROM typed_double ORDER BY id").await,
        final_double
    );

    let decimal_schema = [("id", 20), ("v", 1700)];
    assert_eq!(
        client
            .query("CREATE TABLE typed_decimal (id BIGINT PRIMARY KEY, v DECIMAL(20,2))")
            .await,
        reply(&[], &[], "CREATE TABLE 0")
    );
    assert_eq!(client.query("INSERT INTO typed_decimal VALUES (1,9007199254740993.00),(2,999999999999999999.00)").await, reply(&[], &[], "INSERT 0 2"));
    let original_decimal = reply(
        &decimal_schema,
        &[
            &[Some("1"), Some("9007199254740993.00")],
            &[Some("2"), Some("999999999999999999.00")],
        ],
        "SELECT 2",
    );
    let overflow = client
        .query("UPDATE typed_decimal SET v = v + 1 RETURNING *")
        .await;
    assert_eq!(overflow, QueryReply { columns: vec![], rows: vec![], tags: vec![], error: Some(("22003".to_string(), "[RS-1012] SQL parse error: [RS-1016] Arrow error: Invalid argument error: 1000000000000000000.00 is too large to store in a Decimal128 of precision 20. Max is 999999999999999999.99".to_string())) });
    assert_eq!(
        client
            .query("SELECT * FROM typed_decimal ORDER BY id")
            .await,
        original_decimal
    );
    assert_eq!(
        client
            .query("UPDATE typed_decimal SET v = v + 1 WHERE v = 9007199254740993.00 RETURNING *")
            .await,
        reply(
            &decimal_schema,
            &[&[Some("1"), Some("9007199254740994.00")]],
            "UPDATE 1"
        )
    );
    let final_decimal = reply(
        &decimal_schema,
        &[
            &[Some("1"), Some("9007199254740994.00")],
            &[Some("2"), Some("999999999999999999.00")],
        ],
        "SELECT 2",
    );
    assert_eq!(
        client
            .query("SELECT * FROM typed_decimal ORDER BY id")
            .await,
        final_decimal
    );
    assert_eq!(
        client
            .query("CREATE TABLE wide_decimal (id BIGINT PRIMARY KEY, v DECIMAL(38,2))")
            .await,
        reply(&[], &[], "CREATE TABLE 0")
    );
    assert_eq!(
        client
            .query("INSERT INTO wide_decimal VALUES (1,99999999999999999999999999999999999.00)")
            .await,
        reply(&[], &[], "INSERT 0 1")
    );
    assert_eq!(
        client
            .query("UPDATE wide_decimal SET v = v + 1 RETURNING *")
            .await,
        reply(
            &decimal_schema,
            &[&[Some("1"), Some("100000000000000000000000000000000000.00")]],
            "UPDATE 1"
        )
    );
    let final_wide = reply(
        &decimal_schema,
        &[&[Some("1"), Some("100000000000000000000000000000000000.00")]],
        "SELECT 1",
    );
    assert_eq!(
        client.query("SELECT * FROM wide_decimal ORDER BY id").await,
        final_wide
    );
    for declaration in [
        "DECIMAL(0,0)",
        "DECIMAL(39,0)",
        "DECIMAL(5,6)",
        "DECIMAL(999,0)",
    ] {
        assert_eq!(
            client
                .query(&format!("CREATE TABLE invalid_decimal (v {declaration})"))
                .await,
            QueryReply {
                columns: vec![],
                rows: vec![],
                tags: vec![],
                error: Some((
                    "42601".to_string(),
                    "[RS-1012] Invalid decimal precision or scale".to_string()
                ))
            }
        );
        assert!(catalog.get_table("invalid_decimal").is_none());
    }

    let tables: Vec<_> = [
        "typed_text",
        "typed_double",
        "typed_decimal",
        "wide_decimal",
    ]
    .map(|name| catalog.get_table(name).unwrap())
    .into();
    db.flush().await.unwrap();
    drop(client);
    handle.abort();
    let _ = handle.await;
    drop(db);
    drop(catalog);
    let recovered_catalog = Arc::new(CatalogStubs::new());
    recovered_catalog.set_durable_store(Arc::new(
        DurableCatalogStore::recover(store.clone(), "typed-catalog")
            .await
            .unwrap(),
    ));
    recovered_catalog.sync_from_durable_store().await.unwrap();
    for expected in tables {
        let actual = recovered_catalog.get_table(&expected.name).unwrap();
        assert_eq!(
            (actual.name, actual.columns, actual.pk_cols),
            (expected.name, expected.columns, expected.pk_cols)
        );
    }
    let (port, handle, db) = start_gateway("typed-dml", store, recovered_catalog).await;
    let mut client = WireClient::connect(port).await;
    assert_eq!(
        client.query("SELECT * FROM typed_text ORDER BY id").await,
        final_text
    );
    assert_eq!(
        client.query("SELECT * FROM typed_double ORDER BY id").await,
        final_double
    );
    assert_eq!(
        client
            .query("SELECT * FROM typed_decimal ORDER BY id")
            .await,
        final_decimal
    );
    assert_eq!(
        client.query("SELECT * FROM wide_decimal ORDER BY id").await,
        final_wide
    );
    db.flush().await.unwrap();
    handle.abort();
}

async fn run_delete_restart_case(store: Arc<dyn ObjectStore>, shard_path: &str) {
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'delete-seed'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, a) VALUES (1, 'A1')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'delete-change'")
        .await
        .unwrap();
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1, a = 'A1' RETURNING *")
        .await
        .unwrap();
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();
    let msgs2 = client2.simple_query("SELECT * FROM t").await.unwrap();
    let rows2: Vec<_> = msgs2
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert!(rows2.is_empty());
    assert!(matches!(
        BatchOp::Delete { key: vec![] },
        BatchOp::Delete { .. }
    ));
}

#[tokio::test]
async fn pre_image_capture_replays_correctly_after_crash_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    run_delete_restart_case(store, "delete-rmw-lfs").await;
}

#[tokio::test]
async fn pre_image_capture_replays_correctly_after_crash_minio_tc() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP pre_image_capture_replays_correctly_after_crash_minio_tc: Docker not available"
            );
            return;
        }
    };
    run_delete_restart_case(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        )),
        "delete-rmw-minio",
    )
    .await;
}
