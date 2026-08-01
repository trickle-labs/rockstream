//! v0.51.4 Slice 1 exit test: `compile_plan` compiles `PlanNode::Aggregate`
//! (a `CREATE MATERIALIZED VIEW ... SELECT category, SUM(price) FROM bid
//! GROUP BY category`-shaped view, no join) through the live, gateway-
//! submitted SQL path into `AggregateOp`, wired via the new
//! `StatefulPipeline` (`live_exec.rs`). Driven over pgwire across
//! insert/update/delete commits, compared against a from-scratch batch
//! recompute (the oracle) after each commit.

use std::collections::HashMap;
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

/// Query `view` via `SELECT * FROM view` and parse rows into `(category, sum)`.
async fn read_view_state(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let category: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(category, sum);
        }
    }
    state
}

/// Batch oracle: recompute `SUM(price) GROUP BY category` from scratch over
/// `rows` (id -> (category, price)).
fn oracle_state(rows: &HashMap<i64, (i64, i64)>) -> HashMap<i64, i64> {
    let mut state: HashMap<i64, i64> = HashMap::new();
    for (category, price) in rows.values() {
        *state.entry(*category).or_insert(0) += price;
    }
    state
}

#[tokio::test]
async fn compiled_aggregate_view_matches_batch_oracle_across_commits() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("live-exec-aggregate", store)
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
        .simple_query("CREATE TABLE bid (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW cat_sum AS SELECT category, SUM(price) FROM bid GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(
        handler.has_compiled_view("cat_sum"),
        "cat_sum should be compiled through compile_plan (op_id Some), not the DataFusion materializer"
    );
    let op_id = catalog.get_view("cat_sum").expect("view registered").op_id;
    assert!(
        op_id.is_some(),
        "CatalogView.op_id should be Some(_) for a compiled Aggregate view"
    );

    let mut rows: HashMap<i64, (i64, i64)> = HashMap::new(); // id -> (category, price)

    // Commit 1: three inserts across two categories.
    client
        .simple_query(
            "INSERT INTO bid (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50)",
        )
        .await
        .expect("INSERT should succeed");
    rows.insert(1, (10, 100));
    rows.insert(2, (10, 200));
    rows.insert(3, (20, 50));
    assert_eq!(
        read_view_state(&client, "cat_sum").await,
        oracle_state(&rows),
        "after insert commit"
    );

    // Commit 2: update one row's price (same group).
    // WHERE lists every column's current value (this codebase's row-key
    // lookup is keyed by the full row, not just the declared PK — matching
    // the pattern used by e.g. `gateway_proof_tests.rs`'s UPDATE tests).
    client
        .simple_query("UPDATE bid SET price = 500 WHERE id = 1, category = 10, price = 100")
        .await
        .expect("UPDATE should succeed");
    rows.insert(1, (10, 500));
    assert_eq!(
        read_view_state(&client, "cat_sum").await,
        oracle_state(&rows),
        "after update commit"
    );

    // Commit 3: delete a row, removing a group entirely.
    // WHERE lists every column's current value — see the UPDATE comment
    // above for why (row-key lookup is keyed by the full row).
    client
        .simple_query("DELETE FROM bid WHERE id = 3, category = 20, price = 50")
        .await
        .expect("DELETE should succeed");
    rows.remove(&3);
    assert_eq!(
        read_view_state(&client, "cat_sum").await,
        oracle_state(&rows),
        "after delete commit"
    );

    // Commit 4: insert a row that moves a category's total again, to prove
    // the arrangement keeps accumulating correctly across multiple commits.
    client
        .simple_query("INSERT INTO bid (id, category, price) VALUES (4, 10, 25)")
        .await
        .expect("INSERT should succeed");
    rows.insert(4, (10, 25));
    assert_eq!(
        read_view_state(&client, "cat_sum").await,
        oracle_state(&rows),
        "after final insert commit"
    );
}
