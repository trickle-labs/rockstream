use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions,
    PutPayload, PutResult,
};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::{CatalogStore, DurableCatalogStore};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

#[derive(Debug)]
struct FailCatalogLogWrites {
    inner: InMemory,
    fail: AtomicBool,
    failed_appends: AtomicUsize,
}

impl std::fmt::Display for FailCatalogLogWrites {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FailCatalogLogWrites")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailCatalogLogWrites {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail.load(Ordering::SeqCst) && location.as_ref().contains("catalog/log/") {
            self.failed_appends.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "FailCatalogLogWrites",
                source: "injected catalog log append failure".into(),
            });
        }
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

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

async fn start_gateway(store: Arc<dyn ObjectStore>) -> (u16, tokio::task::JoinHandle<()>) {
    let shard_db = Arc::new(
        ShardDb::builder("catalog-write-failure", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let durable = Arc::new(
        DurableCatalogStore::recover(store, "catalog-write-failure")
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    catalog.set_durable_store(durable);
    catalog.sync_from_durable_store().await.unwrap();

    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (address, handle) = server.serve_background().await.unwrap();
    (address.port(), handle)
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn public_tables(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn table_columns(client: &tokio_postgres::Client) -> Vec<(String, String)> {
    client
        .query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'durable_rows'", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn durable_table(
    store: Arc<dyn ObjectStore>,
) -> Option<rockstream_storage::catalog::CatalogTable> {
    DurableCatalogStore::recover(store, "catalog-write-failure")
        .await
        .unwrap()
        .get_table_by_name("durable_rows")
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_drop_preserves_table_until_durable_deletion() {
    let store = Arc::new(FailCatalogLogWrites {
        inner: InMemory::new(),
        fail: AtomicBool::new(false),
        failed_appends: AtomicUsize::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let (port, handle) = start_gateway(object_store.clone()).await;
    let client = connect(port).await;
    client
        .simple_query("CREATE TABLE durable_rows (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .unwrap();
    let original = durable_table(object_store.clone()).await.unwrap();
    let columns = table_columns(&client).await;
    assert_eq!(
        columns,
        vec![
            ("id".into(), "bigint".into()),
            ("label".into(), "text".into())
        ]
    );
    store.fail.store(true, Ordering::SeqCst);
    let result = client.simple_query("DROP TABLE durable_rows").await;
    assert_eq!(store.failed_appends.load(Ordering::SeqCst), 1);
    assert!(
        result.is_err(),
        "DROP TABLE must report the failed append: {result:?}"
    );
    assert_eq!(public_tables(&client).await, vec!["durable_rows"]);
    assert_eq!(table_columns(&client).await, columns);
    assert_eq!(
        durable_table(object_store.clone()).await,
        Some(original.clone())
    );
    drop(client);
    handle.abort();

    let (port, handle) = start_gateway(object_store.clone()).await;
    let client = connect(port).await;
    assert_eq!(public_tables(&client).await, vec!["durable_rows"]);
    assert_eq!(table_columns(&client).await, columns);
    assert_eq!(durable_table(object_store.clone()).await, Some(original));
    store.fail.store(false, Ordering::SeqCst);
    client
        .simple_query("DROP TABLE durable_rows")
        .await
        .unwrap();
    assert_eq!(public_tables(&client).await, Vec::<String>::new());
    assert_eq!(table_columns(&client).await, Vec::<(String, String)>::new());
    assert_eq!(durable_table(object_store.clone()).await, None);
    drop(client);
    handle.abort();

    let (port, handle) = start_gateway(object_store.clone()).await;
    let client = connect(port).await;
    assert_eq!(public_tables(&client).await, Vec::<String>::new());
    assert_eq!(table_columns(&client).await, Vec::<(String, String)>::new());
    assert_eq!(durable_table(object_store).await, None);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_catalog_log_append_rejects_create_table_without_partial_publication() {
    let store = Arc::new(FailCatalogLogWrites {
        inner: InMemory::new(),
        fail: AtomicBool::new(false),
        failed_appends: AtomicUsize::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let (port, handle) = start_gateway(object_store.clone()).await;
    let client = connect(port).await;
    store.fail.store(true, Ordering::SeqCst);
    let create_result = client
        .simple_query("CREATE TABLE uncommitted_rows (id BIGINT)")
        .await;
    let visible_before_restart = public_tables(&client).await;
    drop(client);
    handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (port, handle) = start_gateway(object_store.clone()).await;
    let client = connect(port).await;
    let visible_after_restart = public_tables(&client).await;
    store.fail.store(false, Ordering::SeqCst);
    let success_result = client
        .simple_query("CREATE TABLE durable_rows (id BIGINT)")
        .await;
    let visible_after_success = public_tables(&client).await;
    drop(client);
    handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (port, handle) = start_gateway(object_store).await;
    let client = connect(port).await;
    let visible_after_success_restart = public_tables(&client).await;
    assert_eq!(
        (
            create_result.is_err(),
            store.failed_appends.load(Ordering::SeqCst),
            visible_before_restart,
            visible_after_restart,
            success_result.is_ok(),
            visible_after_success,
            visible_after_success_restart
        ),
        (
            true,
            1,
            vec![],
            vec![],
            true,
            vec!["durable_rows".to_string()],
            vec!["durable_rows".to_string()]
        ),
        "failed DDL must stay absent and successful DDL must survive restart; failed={create_result:?}, successful={success_result:?}"
    );
    drop(client);
    handle.abort();
}
