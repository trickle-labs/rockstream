//! v0.51.4 Slice 5 exit test: `compile_plan` compiles `COUNT(DISTINCT ...)`
//! (`PlanNode::Aggregate` with `AggregateExpr::distinct = true`, composed
//! with `PlanNode::Distinct`/`DistinctOp` beneath `AggregateOp` — the
//! q15/q16 Nexmark shape) through the live, gateway-submitted SQL path,
//! wired via `StatefulPipeline`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use object_store::local::LocalFileSystem;
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

async fn read_view_state(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let category: i64 = row.get(0).unwrap().parse().unwrap();
            let count: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(category, count);
        }
    }
    state
}

fn oracle_state(rows: &HashMap<i64, (i64, i64)>) -> HashMap<i64, i64> {
    let mut by_category: HashMap<i64, HashSet<i64>> = HashMap::new();
    for (category, bidder) in rows.values() {
        by_category.entry(*category).or_default().insert(*bidder);
    }
    by_category
        .into_iter()
        .map(|(k, v)| (k, v.len() as i64))
        .collect()
}

#[tokio::test]
async fn compiled_count_distinct_view_matches_batch_oracle() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("live-exec-distinct", store)
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let handler = server.handler().clone();
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, bidder BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW distinct_bidders AS \
             SELECT category, COUNT(DISTINCT bidder) FROM bid GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(
        handler.has_compiled_view("distinct_bidders"),
        "distinct_bidders (q15/q16-shaped) should be compiled through compile_plan"
    );
    assert!(
        catalog
            .get_view("distinct_bidders")
            .expect("view registered")
            .op_id
            .is_some(),
        "CatalogView.op_id should be Some(_) for a compiled COUNT(DISTINCT ...) view"
    );

    let mut rows: HashMap<i64, (i64, i64)> = HashMap::new(); // id -> (category, bidder)

    // Commit 1: bidder 100 bids twice in category 1 (should count once),
    // bidder 200 bids once in category 1, bidder 300 bids in category 2.
    client
        .simple_query(
            "INSERT INTO bid (id, category, bidder) VALUES \
             (1, 1, 100), (2, 1, 100), (3, 1, 200), (4, 2, 300)",
        )
        .await
        .expect("INSERT should succeed");
    rows.insert(1, (1, 100));
    rows.insert(2, (1, 100));
    rows.insert(3, (1, 200));
    rows.insert(4, (2, 300));
    assert_eq!(
        read_view_state(&client, "distinct_bidders").await,
        oracle_state(&rows),
        "after insert commit (category 1 has 2 distinct bidders, category 2 has 1)"
    );

    // Commit 2: delete one of the duplicate bidder-100 rows — distinct count
    // for category 1 must stay at 2 (bidder 100 still bid once more). WHERE
    // lists every column's current value (row-key lookup is keyed by the
    // full row, not just `id`).
    client
        .simple_query("DELETE FROM bid WHERE id = 2, category = 1, bidder = 100")
        .await
        .expect("DELETE should succeed");
    rows.remove(&2);
    assert_eq!(
        read_view_state(&client, "distinct_bidders").await,
        oracle_state(&rows),
        "after deleting one duplicate row"
    );

    // Commit 3: delete the last remaining bid from bidder 100 — distinct
    // count for category 1 must drop to 1.
    client
        .simple_query("DELETE FROM bid WHERE id = 1, category = 1, bidder = 100")
        .await
        .expect("DELETE should succeed");
    rows.remove(&1);
    assert_eq!(
        read_view_state(&client, "distinct_bidders").await,
        oracle_state(&rows),
        "after deleting bidder 100's last bid"
    );
}
