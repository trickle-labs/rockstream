//! PGWire SQL tests for `SELECT rockstream_version()` candidate identity function (v0.59.10 OBS-02).

use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::candidate_identity::CandidateIdentity;

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

async fn start_gateway(catalog: CatalogStubs) -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
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

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .expect("query failed")
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(str::to_string));
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_select_rockstream_version_columns() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let id = CandidateIdentity::current();
    let rows = simple_rows(&client, "SELECT rockstream_version();").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 4);

    assert_eq!(
        row[0].as_deref(),
        Some(id.semantic_version.as_str()),
        "product_version must match"
    );
    assert_eq!(
        row[1].as_deref(),
        Some(id.candidate_id().as_str()),
        "candidate_id must match"
    );
    assert_eq!(
        row[2].as_deref(),
        Some(id.commit_sha.as_str()),
        "source_sha must match"
    );
    assert_eq!(
        row[3].as_deref(),
        Some(id.lockfile_digest.as_str()),
        "artifact_digest must match"
    );
}

#[tokio::test]
async fn test_select_rockstream_version_extended_protocol() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let id = CandidateIdentity::current();
    let rows = client
        .query("SELECT rockstream_version()", &[])
        .await
        .expect("extended query protocol failed");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 4);

    let product_version: &str = row.get(0);
    let candidate_id: &str = row.get(1);
    let source_sha: &str = row.get(2);
    let artifact_digest: &str = row.get(3);

    assert_eq!(product_version, id.semantic_version);
    assert_eq!(candidate_id, id.candidate_id());
    assert_eq!(source_sha, id.commit_sha);
    assert_eq!(artifact_digest, id.lockfile_digest);
}

#[tokio::test]
async fn test_select_rockstream_version_reachability() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Both simple and lower/upper case queries work
    let rows = simple_rows(&client, "select ROCKSTREAM_VERSION()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
}
