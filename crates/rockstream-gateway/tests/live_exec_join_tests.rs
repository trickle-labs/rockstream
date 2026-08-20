//! v0.51.4 Slice 3 exit test: `compile_plan` compiles an `InnerJoin`-shaped
//! view (a `CREATE MATERIALIZED VIEW ... SELECT ... FROM a JOIN b ON
//! a.k = b.k`-shaped view) through the live, gateway-submitted SQL path
//! into `JoinOp`, wired via the two-input `JoinPipeline` (`live_exec.rs`).
//! Driven over pgwire, compared against a from-scratch batch recompute (the
//! oracle) after each commit — asserting correctness when a commit lands on
//! the left source only, the right source only, and both interleaved.

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

/// Query `view` via `SELECT * FROM view` and parse rows into a multiset
/// (row -> count) ked by `(a_id, a_k, b_id, b_val)`.
async fn read_view_state(
    client: &tokio_postgres::Client,
    view: &str,
) -> HashMap<(i64, i64, i64, i64), i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state: HashMap<(i64, i64, i64, i64), i64> = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let a_id: i64 = row.get(0).unwrap().parse().unwrap();
            let a_k: i64 = row.get(1).unwrap().parse().unwrap();
            let b_id: i64 = row.get(2).unwrap().parse().unwrap();
            let b_val: i64 = row.get(3).unwrap().parse().unwrap();
            *state.entry((a_id, a_k, b_id, b_val)).or_insert(0) += 1;
        }
    }
    state
}

async fn read_factorized_view_state(client: &tokio_postgres::Client) -> Vec<(i64, i64)> {
    let mut rows = client
        .simple_query("SELECT * FROM factorized_sum")
        .await
        .expect("SELECT should succeed")
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().parse().unwrap(),
                row.get(1).unwrap().parse().unwrap(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

/// Batch oracle: recompute the equi-join `a.k = b.k` from scratch over
/// `a_rows` (id -> k) and `b_rows` (id -> (k, val)).
fn oracle_state(
    a_rows: &HashMap<i64, i64>,
    b_rows: &HashMap<i64, (i64, i64)>,
) -> HashMap<(i64, i64, i64, i64), i64> {
    let mut state: HashMap<(i64, i64, i64, i64), i64> = HashMap::new();
    for (a_id, a_k) in a_rows {
        for (b_id, (b_k, b_val)) in b_rows {
            if a_k == b_k {
                *state.entry((*a_id, *a_k, *b_id, *b_val)).or_insert(0) += 1;
            }
        }
    }
    state
}

/// Returns (in order): the temp-dir guard (must be kept alive by the
/// caller for the test's duration — dropping it deletes the on-disk
/// SlateDB files out from under the still-running server), the server's
/// background-task handle (same lifetime requirement), the catalog, the
/// pgwire client, and the gateway handler.
async fn setup() -> (
    TempDir,
    tokio::task::JoinHandle<()>,
    Arc<CatalogStubs>,
    tokio_postgres::Client,
    Arc<rockstream_gateway::server::GatewayHandler>,
) {
    let catalog = Arc::new(CatalogStubs::new());
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_db = Arc::new(
        ShardDb::builder("live-exec-join", store)
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
    let (addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE a (id BIGINT, k BIGINT)")
        .await
        .expect("CREATE TABLE a should succeed");
    client
        .simple_query("CREATE TABLE b (id BIGINT, k BIGINT, val BIGINT)")
        .await
        .expect("CREATE TABLE b should succeed");
    client
        .simple_query(
            "CREATE VIEW a_join_b AS SELECT a.id, a.k, b.id, b.val FROM a JOIN b ON a.k = b.k",
        )
        .await
        .expect("CREATE VIEW should succeed");
    assert!(
        handler.has_compiled_view("a_join_b"),
        "a_join_b should be compiled through compile_plan (op_id Some), not the DataFusion materializer"
    );

    (dir, handle, catalog, client, handler)
}

#[tokio::test]
async fn compiled_join_view_matches_batch_oracle_when_either_side_commits() {
    let (_dir, _handle, _catalog, client, _handler) = setup().await;

    let mut a_rows: HashMap<i64, i64> = HashMap::new(); // id -> k
    let mut b_rows: HashMap<i64, (i64, i64)> = HashMap::new(); // id -> (k, val)

    // Commit 1: left source only.
    client
        .simple_query("INSERT INTO a (id, k) VALUES (1, 100), (2, 200)")
        .await
        .expect("INSERT INTO a should succeed");
    a_rows.insert(1, 100);
    a_rows.insert(2, 200);
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after left-only insert commit (no matches yet)"
    );

    // Commit 2: right source only, some rows match, some don't.
    client
        .simple_query("INSERT INTO b (id, k, val) VALUES (10, 100, 1000), (11, 300, 3000)")
        .await
        .expect("INSERT INTO b should succeed");
    b_rows.insert(10, (100, 1000));
    b_rows.insert(11, (300, 3000));
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after right-only insert commit"
    );

    // Commit 3: left source again, adding a row that matches an existing
    // right row.
    client
        .simple_query("INSERT INTO a (id, k) VALUES (3, 300)")
        .await
        .expect("INSERT INTO a should succeed");
    a_rows.insert(3, 300);
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after second left-only insert commit"
    );

    // Commit 4: delete a left row that had a match, removing its join rows.
    client
        .simple_query("DELETE FROM a WHERE id = 1, k = 100")
        .await
        .expect("DELETE FROM a should succeed");
    a_rows.remove(&1);
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after left-side delete commit"
    );

    // Commit 5: delete a right row, removing its join rows too.
    client
        .simple_query("DELETE FROM b WHERE id = 11, k = 300, val = 3000")
        .await
        .expect("DELETE FROM b should succeed");
    b_rows.remove(&11);
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after right-side delete commit"
    );
}

#[tokio::test]
async fn factorized_join_aggregate_is_reachable_over_simple_and_extended_pgwire() {
    let (_dir, _handle, _catalog, client, handler) = setup().await;
    let created = client
        .simple_query(
            "CREATE MATERIALIZED VIEW factorized_sum AS \
             SELECT a.k, SUM(b.val) FROM a JOIN b ON a.k = b.k GROUP BY a.k",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    assert!(created.iter().any(|message| matches!(
        message,
        tokio_postgres::SimpleQueryMessage::CommandComplete(_)
    )));
    assert!(handler.has_compiled_view("factorized_sum"));

    client
        .execute("INSERT INTO a (id, k) VALUES (1, 10)", &[])
        .await
        .expect("extended INSERT should succeed");
    client
        .execute("INSERT INTO b (id, k, val) VALUES (2, 10, 7)", &[])
        .await
        .expect("extended INSERT should succeed");
    assert_eq!(read_factorized_view_state(&client).await, vec![(10, 7)]);

    client
        .simple_query("DELETE FROM b WHERE id = 2, k = 10, val = 7")
        .await
        .expect("simple DELETE should succeed");
    assert_eq!(
        read_factorized_view_state(&client).await,
        Vec::<(i64, i64)>::new()
    );
}

#[tokio::test]
async fn compiled_join_view_matches_batch_oracle_when_both_sides_commit_together() {
    let (_dir, _handle, _catalog, client, _handler) = setup().await;

    let mut a_rows: HashMap<i64, i64> = HashMap::new();
    let mut b_rows: HashMap<i64, (i64, i64)> = HashMap::new();

    // Seed both sides so a single interleaved commit has existing state on
    // both arrangements to join against (Δ(L⋈R) = ΔL⋈R₀ + L₀⋈ΔR + ΔL⋈ΔR).
    client
        .simple_query("INSERT INTO a (id, k) VALUES (1, 100)")
        .await
        .expect("seed INSERT INTO a should succeed");
    a_rows.insert(1, 100);
    client
        .simple_query("INSERT INTO b (id, k, val) VALUES (10, 100, 1000)")
        .await
        .expect("seed INSERT INTO b should succeed");
    b_rows.insert(10, (100, 1000));
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after seeding both sides"
    );

    // A single multi-statement commit that touches both `a` and `b` in the
    // same `WriteBatch` — exercises the ΔL⋈ΔR cross term directly.
    client
        .simple_query(
            "BEGIN; INSERT INTO a (id, k) VALUES (2, 200); INSERT INTO b (id, k, val) VALUES (20, 200, 2000); COMMIT;",
        )
        .await
        .expect("interleaved multi-statement commit should succeed");
    a_rows.insert(2, 200);
    b_rows.insert(20, (200, 2000));
    assert_eq!(
        read_view_state(&client, "a_join_b").await,
        oracle_state(&a_rows, &b_rows),
        "after a single commit touching both sides"
    );
}
