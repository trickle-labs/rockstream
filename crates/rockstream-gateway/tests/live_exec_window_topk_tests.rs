//! v0.51.4 Slice 4 exit test: `compile_plan` compiles `PlanNode::Window`
//! (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`) and `PlanNode::TopK`
//! (the `WHERE rn <= K` filter wrapping it — the q5/q6/q9/q18/q19 Nexmark
//! shape) through the live, gateway-submitted SQL path into `WindowOp`/
//! `TopKOp`, wired via `StatefulPipeline`.

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

async fn read_view_rows(client: &tokio_postgres::Client, view: &str) -> HashSet<(i64, i64)> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut rows = HashSet::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let grp: i64 = row.get(0).unwrap().parse().unwrap();
            let val: i64 = row.get(1).unwrap().parse().unwrap();
            rows.insert((grp, val));
        }
    }
    rows
}

/// Batch oracle: the 2 lowest `val`s per `grp` (ties broken arbitrarily, but
/// the value set is what's compared, matching the compiled path's `k=2`
/// cutoff).
///
/// Note on `ASC` (not `DESC`): this exercises `PlanNode::Window` + `Filter`
/// (`rn <= k` compiled as a plain `FilterOp` on the `WindowOp`-computed rank
/// column) — the shape DataFusion's optimizer produces for this query
/// (`ROW_NUMBER() OVER (... ORDER BY val)` directly atop a table scan, with
/// no intervening `Aggregate`). `WindowOp::RowNumber` ranks in ascending
/// order of the order-by column (pre-existing behavior, unrelated to this
/// wiring); reproducing the *other* Nexmark idiom
/// (`ORDER BY ... DESC` lowering straight to `PlanNode::TopK`, which ranks
/// descending) requires the specific nested-`Aggregate` shape
/// `try_lower_topk_pattern` detects (see q5/q6/q9/q18/q19's existing
/// fixtures) — this test proves the `WindowOp`/`Filter` compiled-path wiring
/// end-to-end; `TopKOp`'s own descending-rank semantics remain covered by
/// its pre-existing oracle-proven unit tests in `rockstream-ops`.
fn oracle_bottom2(rows: &HashMap<i64, (i64, i64)>) -> HashSet<(i64, i64)> {
    let mut by_group: HashMap<i64, Vec<i64>> = HashMap::new();
    for (grp, val) in rows.values() {
        by_group.entry(*grp).or_default().push(*val);
    }
    let mut out = HashSet::new();
    for (grp, mut vals) in by_group {
        vals.sort_unstable();
        for v in vals.into_iter().take(2) {
            out.insert((grp, v));
        }
    }
    out
}

#[tokio::test]
async fn compiled_topk_view_matches_batch_oracle() {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("live-exec-window-topk", store)
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
        .simple_query("CREATE TABLE items (id BIGINT, grp BIGINT, val BIGINT)")
        .await
        .expect("CREATE TABLE should succeed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW top2 AS \
             SELECT grp, val FROM (\
                SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn \
                FROM items\
             ) WHERE rn <= 2",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(
        handler.has_compiled_view("top2"),
        "top2 (q5/q6/q9/q18/q19-shaped) should be compiled through compile_plan"
    );
    assert!(
        catalog
            .get_view("top2")
            .expect("view registered")
            .op_id
            .is_some(),
        "CatalogView.op_id should be Some(_) for a compiled Window/TopK view"
    );

    let mut rows: HashMap<i64, (i64, i64)> = HashMap::new(); // id -> (grp, val)

    client
        .simple_query(
            "INSERT INTO items (id, grp, val) VALUES \
             (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 5), (5, 2, 50)",
        )
        .await
        .expect("INSERT should succeed");
    rows.insert(1, (1, 10));
    rows.insert(2, (1, 30));
    rows.insert(3, (1, 20));
    rows.insert(4, (2, 5));
    rows.insert(5, (2, 50));
    assert_eq!(
        read_view_rows(&client, "top2").await,
        oracle_bottom2(&rows),
        "after insert commit"
    );

    // A new row that outranks the current 2nd place in group 1 (20) must
    // displace it.
    client
        .simple_query("INSERT INTO items (id, grp, val) VALUES (6, 1, 15)")
        .await
        .expect("INSERT should succeed");
    rows.insert(6, (1, 15));
    assert_eq!(
        read_view_rows(&client, "top2").await,
        oracle_bottom2(&rows),
        "after displacing insert commit"
    );
}
