//! Regression tests for issue #345: unambiguous typed primary-key encoding,
//! type normalization, UPDATE key changes, simple/extended query paths, durable
//! storage reopen, and legacy textual key reconciliation.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
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

fn assert_unique_violation(err: &tokio_postgres::Error) {
    let db_err = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected DbError, got {err:?}"));
    assert_eq!(
        db_err.code().code(),
        "23505",
        "unexpected error: {db_err:?}"
    );
}

#[tokio::test]
async fn composite_text_keys_with_delimiters_do_not_collide() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, _shard_db) = start_gateway("pk-composite-delimiters", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE composite_keys (a TEXT, b TEXT, PRIMARY KEY (a, b))")
        .await
        .unwrap();

    // Case A from issue #345
    client
        .simple_query("INSERT INTO composite_keys VALUES ('x|b=y', 'z')")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO composite_keys VALUES ('x', 'y|b=z')")
        .await
        .unwrap();

    // Delimiters in both components, including codec markers and colons/semicolons
    client
        .execute(
            "INSERT INTO composite_keys VALUES ($1, $2)",
            &[&"a:b;c=d|e", &"V1:x;N;"],
        )
        .await
        .unwrap();

    // True duplicate via simple query and extended query must fail with 23505
    let dup_simple = client
        .simple_query("INSERT INTO composite_keys VALUES ('x|b=y', 'z')")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_simple);

    let dup_ext = client
        .execute(
            "INSERT INTO composite_keys VALUES ($1, $2)",
            &[&"x", &"y|b=z"],
        )
        .await
        .unwrap_err();
    assert_unique_violation(&dup_ext);

    let rows = client
        .query("SELECT a, b FROM composite_keys ORDER BY a, b", &[])
        .await
        .unwrap();
    let actual: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("a:b;c=d|e".to_string(), "V1:x;N;".to_string()),
            ("x".to_string(), "y|b=z".to_string()),
            ("x|b=y".to_string(), "z".to_string()),
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn numerically_equivalent_keys_share_identity_across_simple_and_extended_queries() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, _shard_db) = start_gateway("pk-numeric-norm", store, catalog).await;
    let client = connect_port(port).await;

    // Case B from issue #345
    client
        .simple_query("CREATE TABLE numeric_keys (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO numeric_keys VALUES (1, 10)")
        .await
        .unwrap();

    let dup_zero_padded = client
        .simple_query("INSERT INTO numeric_keys VALUES ('01', 20)")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_zero_padded);

    let dup_ext = client
        .execute(
            "INSERT INTO numeric_keys VALUES ($1, $2)",
            &[&1_i64, &30_i64],
        )
        .await
        .unwrap_err();
    assert_unique_violation(&dup_ext);

    // Invalid literal for BIGINT PK must be rejected
    let invalid_pk = client
        .simple_query("INSERT INTO numeric_keys VALUES ('not_a_number', 40)")
        .await
        .unwrap_err();
    assert!(invalid_pk.as_db_error().is_some());

    let rows = client
        .query("SELECT id, v FROM numeric_keys ORDER BY id", &[])
        .await
        .unwrap();
    let actual: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect();
    assert_eq!(actual, vec![(1, 10)]);

    // Also verify FLOAT8 (-0.0 vs 0.0, 1.0 vs 1.00 vs 1e0) and DECIMAL(10, 2)
    client
        .simple_query("CREATE TABLE float_keys (k DOUBLE PRECISION PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO float_keys VALUES (-0.0, 'zero')")
        .await
        .unwrap();
    let dup_zero = client
        .simple_query("INSERT INTO float_keys VALUES ('0.00', 'zero2')")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_zero);

    client
        .simple_query("INSERT INTO float_keys VALUES (1.5, 'one_half')")
        .await
        .unwrap();
    let dup_sci = client
        .simple_query("INSERT INTO float_keys VALUES ('15e-1', 'one_half_2')")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_sci);

    handle.abort();
}

#[tokio::test]
async fn update_key_changes_enforce_typed_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, handle, _shard_db) = start_gateway("pk-update-changes", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE upd_keys (id BIGINT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO upd_keys VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();

    // Updating PK to another spelling of its own value must succeed in-place
    client
        .simple_query("UPDATE upd_keys SET id = '01', v = 'one_updated' WHERE id = 1")
        .await
        .unwrap();

    // Updating PK to collide with row 2 via '02' must fail with 23505
    let conflict = client
        .simple_query("UPDATE upd_keys SET id = '02' WHERE id = 1")
        .await
        .unwrap_err();
    assert_unique_violation(&conflict);

    // Updating PK to a free key (3) must succeed and free key 1
    client
        .simple_query("UPDATE upd_keys SET id = 3 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO upd_keys VALUES (1, 'reused_one')")
        .await
        .unwrap();

    let rows = client
        .query("SELECT id, v FROM upd_keys ORDER BY id", &[])
        .await
        .unwrap();
    let actual: Vec<(i64, String)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(
        actual,
        vec![
            (1, "reused_one".to_string()),
            (2, "two".to_string()),
            (3, "one_updated".to_string()),
        ]
    );

    handle.abort();
}

#[tokio::test]
async fn durable_reopen_preserves_uniqueness_and_complete_contents() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "pk-durable-reopen";

    {
        let (port, handle, shard_db) =
            start_gateway(shard_path, store.clone(), catalog.clone()).await;
        let client = connect_port(port).await;

        client
            .simple_query("CREATE TABLE composite_keys (a TEXT, b TEXT, PRIMARY KEY (a, b))")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO composite_keys VALUES ('x|b=y', 'z'), ('x', 'y|b=z')")
            .await
            .unwrap();

        client
            .simple_query("CREATE TABLE numeric_keys (id BIGINT PRIMARY KEY, v BIGINT)")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO numeric_keys VALUES (1, 10)")
            .await
            .unwrap();

        drop(client);
        handle.abort();
        (*shard_db).clone().close().await.unwrap();
    }

    // Reopen durable storage with a fresh GatewayServer
    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;

    let comp_rows = client
        .query("SELECT a, b FROM composite_keys ORDER BY a, b", &[])
        .await
        .unwrap();
    let comp_actual: Vec<(String, String)> = comp_rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(
        comp_actual,
        vec![
            ("x".to_string(), "y|b=z".to_string()),
            ("x|b=y".to_string(), "z".to_string()),
        ]
    );

    let num_rows = client
        .query("SELECT id, v FROM numeric_keys ORDER BY id", &[])
        .await
        .unwrap();
    let num_actual: Vec<(i64, i64)> = num_rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect();
    assert_eq!(num_actual, vec![(1, 10)]);

    // Uniqueness must still be enforced after reopen
    let dup_comp = client
        .simple_query("INSERT INTO composite_keys VALUES ('x|b=y', 'z')")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_comp);

    let dup_num = client
        .simple_query("INSERT INTO numeric_keys VALUES ('01', 99)")
        .await
        .unwrap_err();
    assert_unique_violation(&dup_num);

    drop(client);
    handle.abort();
    (*shard_db).clone().close().await.unwrap();
}

#[tokio::test]
async fn legacy_persisted_textual_keys_are_reconciled_and_conflicts_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "pk-legacy-reconcile";

    // Pre-populate ShardDb with legacy unescaped keys
    {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        shard_db
            .put(b"view_output/legacy_tbl/id=1", b"1\talice")
            .await
            .unwrap();
        shard_db
            .put(b"view_output/legacy_tbl/id=2", b"2\tbob")
            .await
            .unwrap();
        (*shard_db).clone().close().await.unwrap();
    }

    catalog.add_table(CatalogTable {
        name: "legacy_tbl".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
        pk_cols: vec!["id".to_string()],
    });

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;

    // Legacy rows must remain readable
    let rows = client
        .query("SELECT id, name FROM legacy_tbl ORDER BY id", &[])
        .await
        .unwrap();
    let actual: Vec<(i64, String)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(
        actual,
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );

    // Inserting '01' into legacy_tbl must reconcile legacy id=1 and reject as duplicate
    let dup = client
        .simple_query("INSERT INTO legacy_tbl VALUES ('01', 'mallory')")
        .await
        .unwrap_err();
    assert_unique_violation(&dup);

    // Updating and deleting legacy rows must work
    client
        .simple_query("UPDATE legacy_tbl SET name = 'alice2' WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM legacy_tbl WHERE id = 2")
        .await
        .unwrap();

    let after = client
        .query("SELECT id, name FROM legacy_tbl ORDER BY id", &[])
        .await
        .unwrap();
    let after_actual: Vec<(i64, String)> = after
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
        .collect();
    assert_eq!(after_actual, vec![(1, "alice2".to_string())]);

    // Legacy keys must have been migrated to canonical __pk1: keys
    assert!(shard_db
        .get(b"view_output/legacy_tbl/id=1")
        .await
        .unwrap()
        .is_none());
    assert!(shard_db
        .get(b"view_output/legacy_tbl/__pk1:V1:1;")
        .await
        .unwrap()
        .is_some());

    drop(client);
    handle.abort();
    (*shard_db).clone().close().await.unwrap();

    // Conflicting legacy keys (id=1 and id=01) must fail closed on startup recovery
    let conflict_dir = tempfile::tempdir().unwrap();
    let conflict_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(conflict_dir.path()).unwrap());
    let conflict_shard = "pk-legacy-conflict";
    {
        let db = Arc::new(
            ShardDb::builder(conflict_shard, conflict_store.clone())
                .build()
                .await
                .unwrap(),
        );
        db.put(b"view_output/conflict_tbl/id=1", b"1\tfirst")
            .await
            .unwrap();
        db.put(b"view_output/conflict_tbl/id=01", b"01\tsecond")
            .await
            .unwrap();
        (*db).clone().close().await.unwrap();
    }
    let conflict_catalog = Arc::new(CatalogStubs::new());
    conflict_catalog.add_table(CatalogTable {
        name: "conflict_tbl".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
        pk_cols: vec!["id".to_string()],
    });
    let conflict_db = Arc::new(
        ShardDb::builder(conflict_shard, conflict_store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        conflict_catalog,
        Arc::new(NoopViewReader),
        conflict_db.clone(),
    );
    let startup_err = server.serve_background().await.unwrap_err().to_string();
    assert!(
        startup_err.contains("RS-2057"),
        "expected RS-2057 conflict error, got: {startup_err}"
    );
    (*conflict_db).clone().close().await.unwrap();
}
