use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, QueryTimeScatterBudget, QueryTimeShardTopology,
};
use rockstream_storage::{ShardDb, ShardReader, WriteBatch};
use tokio_postgres::{Client, NoTls};

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

async fn seed_shard(db: &ShardDb, start: usize, count: usize, payload: &str) {
    let mut next = start;
    let end = start + count;
    while next < end {
        let mut batch = WriteBatch::new();
        for id in next..end.min(next + 5_000) {
            batch.put(
                format!("view_output/sales/{id:012}").as_bytes(),
                format!("{id}\t{payload}").as_bytes(),
            );
        }
        db.write_batch(batch).await.unwrap();
        next = (next + 5_000).min(end);
    }
    db.flush().await.unwrap();
}

async fn start_gateway(total_rows: usize, payload: &str, budget: QueryTimeScatterBudget) -> Client {
    let store = Arc::new(InMemory::new());
    let mut shards = Vec::new();
    let base_rows = total_rows / 3;
    let mut start = 1usize;
    for shard in 0..3 {
        let count = if shard == 2 {
            total_rows - base_rows * 2
        } else {
            base_rows
        };
        let path = format!("query-time-large-{total_rows}-{shard}");
        let db = Arc::new(
            ShardDb::builder(path.clone(), store.clone())
                .build()
                .await
                .unwrap(),
        );
        seed_shard(&db, start, count, payload).await;
        start += count;
        shards.push((path, db));
    }
    let readers = futures::future::try_join_all(
        shards
            .iter()
            .map(|(path, _)| ShardReader::open(path.clone(), store.clone())),
    )
    .await
    .unwrap()
    .into_iter()
    .map(Arc::new)
    .collect();
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_table(CatalogTable {
        name: "sales".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "payload".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    }));
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shards[0].1.clone(),
        QueryTimeShardTopology::with_query_time_scatter_budget(readers, 0, budget),
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn aggregate_row(client: &Client) -> Vec<String> {
    client
        .simple_query("SELECT SUM(id) AS id_sum, COUNT(*) AS row_count FROM sales")
        .await
        .unwrap()
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(vec![
                row.get("id_sum").unwrap().to_string(),
                row.get("row_count").unwrap().to_string(),
            ]),
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn large_relation_over_old_row_cap_aggregate_matches_oracle() {
    const ROWS: usize = 1_000_001;
    let client = start_gateway(ROWS, "x", QueryTimeScatterBudget::default()).await;
    assert_eq!(
        aggregate_row(&client).await,
        vec!["500001500001".into(), ROWS.to_string()],
    );
}

#[tokio::test]
async fn large_relation_over_old_byte_cap_aggregate_matches_oracle() {
    const ROWS: usize = 65_537;
    let payload = "x".repeat(1_024);
    let client = start_gateway(ROWS, &payload, QueryTimeScatterBudget::default()).await;
    assert_eq!(
        aggregate_row(&client).await,
        vec!["2147581953".into(), ROWS.to_string()],
    );
}

#[tokio::test]
async fn pathological_query_budget_returns_documented_error_without_truncation() {
    let client = start_gateway(
        3,
        "x",
        QueryTimeScatterBudget {
            row_limit: 2,
            byte_limit: 1_024,
        },
    )
    .await;
    let error = client
        .simple_query("SELECT SUM(id) AS id_sum, COUNT(*) AS row_count FROM sales")
        .await
        .unwrap_err();
    let message = error.as_db_error().expect("pgwire ErrorResponse").message();
    assert!(message.contains("RS-2029"), "unexpected error: {message}");
    assert!(
        message.contains("byte_limit=1024, relation=sales, row_limit=2"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("next_steps"),
        "unexpected error: {message}"
    );
}
