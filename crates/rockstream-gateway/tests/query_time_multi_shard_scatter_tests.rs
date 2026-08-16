use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    query_time_scatter_fill_levels, query_time_scatter_peak_fill_levels,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, QueryTimeShardTopology,
    QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES, QUERY_TIME_SCATTER_MAX_IN_FLIGHT_BYTES,
    QUERY_TIME_SCATTER_MAX_IN_FLIGHT_ROWS,
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

fn table(name: &str, columns: &[(&str, &str)]) -> CatalogTable {
    CatalogTable {
        name: name.to_string(),
        columns: columns
            .iter()
            .map(|(name, data_type)| CatalogColumn {
                name: (*name).to_string(),
                data_type: (*data_type).to_string(),
            })
            .collect(),
    }
}

async fn start_gateway(
    tables: Vec<CatalogTable>,
    shard_rows: Vec<Vec<(&str, &str, &str)>>,
) -> Client {
    let store = Arc::new(InMemory::new());
    start_gateway_with_store(tables, shard_rows, store, "query-time-scatter").await
}

async fn start_gateway_with_store(
    tables: Vec<CatalogTable>,
    shard_rows: Vec<Vec<(&str, &str, &str)>>,
    store: Arc<dyn ObjectStore>,
    path_prefix: &str,
) -> Client {
    let mut shards = Vec::new();
    for (shard, rows) in shard_rows.into_iter().enumerate() {
        let path = format!("{path_prefix}-{shard}");
        let db = Arc::new(
            ShardDb::builder(path.clone(), store.clone())
                .build()
                .await
                .unwrap(),
        );
        for (relation, key, value) in rows {
            db.put(
                format!("view_output/{relation}/{key}").as_bytes(),
                value.as_bytes(),
            )
            .await
            .unwrap();
        }
        db.flush().await.unwrap();
        shards.push((path, db));
    }
    start_gateway_from_shards(tables, shards).await
}

async fn start_gateway_from_shards(
    tables: Vec<CatalogTable>,
    shards: Vec<(String, Arc<ShardDb>)>,
) -> Client {
    let store = shards[0].1.object_store();
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
    for relation in tables {
        assert!(catalog.add_table(relation));
    }
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shards[0].1.clone(),
        QueryTimeShardTopology::new(readers, 0),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn query_rows(client: &Client, sql: &str, columns: &[&str]) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                columns
                    .iter()
                    .map(|column| row.get(column).unwrap().to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn multi_shard_where_bigint_matches_single_shard_oracle() {
    let client = start_gateway(
        vec![table("sales", &[("id", "Int64"), ("region", "Utf8")])],
        vec![
            vec![("sales", "00", "1\twest")],
            vec![("sales", "01", "2\teast")],
            vec![("sales", "02", "3\tnorth")],
        ],
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT id, region FROM sales WHERE id >= 2 ORDER BY id",
            &["id", "region"],
        )
        .await,
        vec![
            vec![String::from("2"), String::from("east")],
            vec![String::from("3"), String::from("north")]
        ],
    );
}

#[tokio::test]
async fn multi_shard_where_text_matches_single_shard_oracle() {
    let client = start_gateway(
        vec![table("sales", &[("id", "Int64"), ("region", "Utf8")])],
        vec![
            vec![("sales", "00", "1\twest")],
            vec![("sales", "01", "2\teast")],
            vec![("sales", "02", "3\twest")],
        ],
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT id, region FROM sales WHERE region = 'west' ORDER BY id",
            &["id", "region"],
        )
        .await,
        vec![
            vec![String::from("1"), String::from("west")],
            vec![String::from("3"), String::from("west")]
        ],
    );
}

#[tokio::test]
async fn multi_shard_where_float_matches_single_shard_oracle() {
    let client = start_gateway(
        vec![table("metrics", &[("score", "Float64"), ("label", "Utf8")])],
        vec![
            vec![("metrics", "00", "1.25\tlow")],
            vec![("metrics", "01", "2.5\tmid")],
            vec![("metrics", "02", "3.75\thigh")],
        ],
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT score, label FROM metrics WHERE score >= 2.5 ORDER BY score",
            &["score", "label"],
        )
        .await,
        vec![
            vec![String::from("2.5"), String::from("mid")],
            vec![String::from("3.75"), String::from("high")]
        ],
    );
}

async fn assert_join(key_type: &str, rows: Vec<Vec<(&str, &str, &str)>>) {
    let client = start_gateway(
        vec![
            table("left_rows", &[("key", key_type), ("value", "Utf8")]),
            table("right_rows", &[("key", key_type), ("label", "Utf8")]),
        ],
        rows,
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT l.value, r.label FROM left_rows l JOIN right_rows r ON l.key = r.key ORDER BY l.value",
            &["value", "label"],
        )
        .await,
        vec![
            vec![String::from("a"), String::from("first")],
            vec![String::from("c"), String::from("third")],
        ],
    );
}

#[tokio::test]
async fn multi_shard_inner_join_bigint_matches_single_shard_oracle() {
    assert_join(
        "Int64",
        vec![
            vec![
                ("left_rows", "00", "1\ta"),
                ("right_rows", "00", "3\tthird"),
            ],
            vec![("right_rows", "01", "1\tfirst")],
            vec![("left_rows", "02", "3\tc")],
        ],
    )
    .await;
}

#[tokio::test]
async fn multi_shard_inner_join_text_matches_single_shard_oracle() {
    assert_join(
        "Utf8",
        vec![
            vec![
                ("left_rows", "00", "one\ta"),
                ("right_rows", "00", "three\tthird"),
            ],
            vec![("right_rows", "01", "one\tfirst")],
            vec![("left_rows", "02", "three\tc")],
        ],
    )
    .await;
}

#[tokio::test]
async fn multi_shard_inner_join_float_matches_single_shard_oracle() {
    assert_join(
        "Float64",
        vec![
            vec![
                ("left_rows", "00", "1.5\ta"),
                ("right_rows", "00", "3.5\tthird"),
            ],
            vec![("right_rows", "01", "1.5\tfirst")],
            vec![("left_rows", "02", "3.5\tc")],
        ],
    )
    .await;
}

async fn assert_group_by(
    key_type: &str,
    amount_type: &str,
    rows: Vec<Vec<(&str, &str, &str)>>,
    expected: Vec<Vec<String>>,
) {
    let client = start_gateway(
        vec![table(
            "sales",
            &[("key", key_type), ("amount", amount_type)],
        )],
        rows,
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT key, SUM(amount) AS total, COUNT(*) AS count FROM sales GROUP BY key ORDER BY key",
            &["key", "total", "count"],
        )
        .await,
        expected,
    );
}

#[tokio::test]
async fn multi_shard_group_by_bigint_sum_count_matches_single_shard_oracle() {
    assert_group_by(
        "Int64",
        "Int64",
        vec![
            vec![("sales", "00", "1\t2")],
            vec![("sales", "01", "2\t5")],
            vec![("sales", "02", "1\t3")],
        ],
        vec![
            vec!["1".into(), "5".into(), "2".into()],
            vec!["2".into(), "5".into(), "1".into()],
        ],
    )
    .await;
}

#[tokio::test]
async fn multi_shard_group_by_text_key_sum_count_matches_single_shard_oracle() {
    assert_group_by(
        "Utf8",
        "Int64",
        vec![
            vec![("sales", "00", "east\t2")],
            vec![("sales", "01", "west\t5")],
            vec![("sales", "02", "east\t3")],
        ],
        vec![
            vec!["east".into(), "5".into(), "2".into()],
            vec!["west".into(), "5".into(), "1".into()],
        ],
    )
    .await;
}

#[tokio::test]
async fn multi_shard_group_by_float_sum_count_matches_single_shard_oracle() {
    assert_group_by(
        "Float64",
        "Float64",
        vec![
            vec![("sales", "00", "1.5\t2.5")],
            vec![("sales", "01", "3.5\t5.5")],
            vec![("sales", "02", "1.5\t3.5")],
        ],
        vec![
            vec!["1.5".into(), "6".into(), "2".into()],
            vec!["3.5".into(), "5.5".into(), "1".into()],
        ],
    )
    .await;
}

#[tokio::test]
async fn query_time_scatter_batches_never_exceed_row_or_byte_budget() {
    let store = Arc::new(InMemory::new());
    let mut shards = Vec::new();
    for shard in 0..2 {
        let path = format!("query-time-bounded-{shard}");
        let db = Arc::new(
            ShardDb::builder(path.clone(), store.clone())
                .build()
                .await
                .unwrap(),
        );
        let mut batch = WriteBatch::new();
        for offset in 0..10_000usize {
            let id = shard * 10_000 + offset + 1;
            batch.put(
                format!("view_output/sales/{id:08}").as_bytes(),
                format!("{id}\t1").as_bytes(),
            );
        }
        db.write_batch(batch).await.unwrap();
        db.flush().await.unwrap();
        shards.push((path, db));
    }

    let client = start_gateway_from_shards(
        vec![table("sales", &[("id", "Int64"), ("amount", "Int64")])],
        shards,
    )
    .await;
    assert_eq!(
        query_rows(
            &client,
            "SELECT SUM(id) AS id_sum, COUNT(*) AS row_count FROM sales",
            &["id_sum", "row_count"],
        )
        .await,
        vec![vec![String::from("200010000"), String::from("20000")]],
    );
    assert_eq!(
        query_time_scatter_fill_levels(),
        rockstream_gateway::QueryTimeScatterFillLevels {
            rows: 0,
            bytes: 0,
            batches: 0,
        },
    );
    let peak = query_time_scatter_peak_fill_levels();
    assert!(peak.rows <= QUERY_TIME_SCATTER_MAX_IN_FLIGHT_ROWS);
    assert!(peak.bytes <= QUERY_TIME_SCATTER_MAX_IN_FLIGHT_BYTES);
    assert!(peak.batches <= QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES);
}

#[tokio::test]
async fn query_time_invalid_sql_returns_actionable_rs_error() {
    let client = start_gateway(
        vec![table("sales", &[("id", "Int64")])],
        vec![vec![("sales", "00", "1")]],
    )
    .await;
    let error = client
        .simple_query("SELECT unknown_column FROM sales WHERE id = 1")
        .await
        .unwrap_err();
    let message = error.as_db_error().expect("pgwire ErrorResponse").message();
    assert!(message.contains("RS-2026"), "unexpected error: {message}");
    assert!(
        message.contains("next_steps"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn multi_shard_scatter_lfs_where_join_group_by_exact_oracle() {
    use object_store::local::LocalFileSystem;

    let directory = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let client = start_gateway_with_store(
        vec![
            table(
                "sales",
                &[("id", "Int64"), ("region", "Utf8"), ("amount", "Int64")],
            ),
            table("labels", &[("id", "Int64"), ("label", "Utf8")]),
        ],
        vec![
            vec![("sales", "00", "1\twest\t2"), ("labels", "00", "3\tthird")],
            vec![("sales", "01", "2\teast\t5"), ("labels", "01", "1\tfirst")],
            vec![("sales", "02", "3\twest\t3")],
        ],
        store,
        "query-time-scatter-lfs",
    )
    .await;

    assert_eq!(
        query_rows(
            &client,
            "SELECT id, region FROM sales WHERE region = 'west' ORDER BY id",
            &["id", "region"],
        )
        .await,
        vec![
            vec![String::from("1"), String::from("west")],
            vec![String::from("3"), String::from("west")]
        ],
    );
    assert_eq!(
        query_rows(
            &client,
            "SELECT s.id, l.label FROM sales s JOIN labels l ON s.id = l.id ORDER BY s.id",
            &["id", "label"],
        )
        .await,
        vec![
            vec![String::from("1"), String::from("first")],
            vec![String::from("3"), String::from("third")]
        ],
    );
    assert_eq!(
        query_rows(
            &client,
            "SELECT region, SUM(amount) AS total, COUNT(*) AS count FROM sales GROUP BY region ORDER BY region",
            &["region", "total", "count"],
        )
        .await,
        vec![
            vec![String::from("east"), String::from("5"), String::from("1")],
            vec![String::from("west"), String::from("5"), String::from("2")],
        ],
    );
}

#[tokio::test]
async fn empty_scatter_topology_returns_actionable_error() {
    let store = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("query-time-empty-topology", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_table(table("sales", &[("id", "Int64")])));
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard,
        QueryTimeShardTopology::new(Vec::new(), 0),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let error = client
        .simple_query("SELECT id FROM sales WHERE id = 1")
        .await
        .unwrap_err();
    let message = error.as_db_error().expect("pgwire ErrorResponse").message();
    assert!(message.contains("RS-2028"), "unexpected error: {message}");
    assert!(
        message.contains("next_steps"),
        "unexpected error: {message}"
    );
}
