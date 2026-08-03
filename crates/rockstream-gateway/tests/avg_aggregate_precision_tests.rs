//! v0.51.6 Slice 4 — `AVG` returns a genuine floating-point mean over the
//! wire, not a truncated-toward-zero integer.
//!
//! `SELECT ... AVG(qty) ... GROUP BY grp` covers three cases in one
//! materialized view: a positive fractional mean, a negative fractional
//! mean, and an exact-integer mean (regression: small-integer means must
//! still come back exactly right, not off due to floating-point rounding).

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tempfile::TempDir;
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
) -> (u16, tokio::task::JoinHandle<()>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle)
}

/// `SELECT grp, AVG(qty) FROM t GROUP BY grp` over rows whose true mean is
/// fractional (positive and negative) and an exact-integer control case
/// returns the correct `f64` value through the wire protocol, not a
/// truncated integer.
#[tokio::test]
async fn avg_aggregate_returns_true_fractional_mean_over_wire() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle) = start_gateway("avg-aggregate-precision", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, grp BIGINT, qty BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW avg_by_grp AS SELECT grp, AVG(qty) as avg_qty FROM t GROUP BY grp",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    // grp 1: {1, 2, 4} -> mean 7/3, a positive fractional mean.
    for (id, qty) in [(1, 1), (2, 2), (3, 4)] {
        client
            .simple_query(&format!(
                "INSERT INTO t (id, grp, qty) VALUES ({id}, 1, {qty})"
            ))
            .await
            .expect("INSERT failed");
    }
    // grp 2: {-10, -9, -7} -> mean -26/3, a negative fractional mean.
    for (id, qty) in [(4, -10), (5, -9), (6, -7)] {
        client
            .simple_query(&format!(
                "INSERT INTO t (id, grp, qty) VALUES ({id}, 2, {qty})"
            ))
            .await
            .expect("INSERT failed");
    }
    // grp 3: {2, 4, 6} -> mean 4.0, an exact-integer mean (regression: must
    // still be exactly correct, not off due to floating-point rounding).
    for (id, qty) in [(7, 2), (8, 4), (9, 6)] {
        client
            .simple_query(&format!(
                "INSERT INTO t (id, grp, qty) VALUES ({id}, 3, {qty})"
            ))
            .await
            .expect("INSERT failed");
    }
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    let msgs = client
        .simple_query("SELECT grp, avg_qty FROM avg_by_grp")
        .await
        .expect("SELECT failed");
    let mut got: HashMap<i64, f64> = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let grp: i64 = row.get(0).unwrap().parse().unwrap();
            let avg_qty: f64 = row.get(1).unwrap().parse().unwrap();
            got.insert(grp, avg_qty);
        }
    }

    assert_eq!(got.len(), 3, "expected exactly 3 groups, got {got:?}");
    let expect = |grp: i64, expected: f64| {
        let actual = got[&grp];
        assert!(
            (actual - expected).abs() < 1e-9,
            "grp={grp}: expected avg {expected}, got {actual}"
        );
    };
    expect(1, 7.0 / 3.0);
    expect(2, -26.0 / 3.0);
    expect(3, 4.0);
}
