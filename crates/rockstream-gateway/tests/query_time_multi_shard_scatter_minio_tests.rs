#![cfg(feature = "testcontainers")]

use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, QueryTimeShardTopology,
};
use rockstream_storage::{ShardDb, ShardReader};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

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
        pk_cols: vec![],
    }
}

async fn start_gateway(store: Arc<dyn ObjectStore>, path_prefix: &str) -> Client {
    let mut shards = Vec::new();
    for (shard, rows) in [
        vec![("sales", "00", "1\twest\t2"), ("labels", "00", "3\tthird")],
        vec![("sales", "01", "2\teast\t5"), ("labels", "01", "1\tfirst")],
        vec![("sales", "02", "3\twest\t3")],
    ]
    .into_iter()
    .enumerate()
    {
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
    assert!(catalog.add_table(table(
        "sales",
        &[("id", "Int64"), ("region", "Utf8"), ("amount", "Int64")],
    )));
    assert!(catalog.add_table(table("labels", &[("id", "Int64"), ("label", "Utf8")],)));
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shards[0].1.clone(),
        QueryTimeShardTopology::new(readers, 0),
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
async fn multi_shard_scatter_minio_where_join_group_by_exact_oracle() {
    let bucket = format!("query-time-scatter-{}", Uuid::new_v4().simple());
    let (_container, port) = match rockstream_test_support::minio::start_minio(&bucket).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP multi_shard_scatter_minio_where_join_group_by_exact_oracle: Docker not available");
            return;
        }
    };
    let client = start_gateway(
        Arc::new(rockstream_test_support::minio::minio_object_store(
            port, &bucket,
        )),
        &bucket,
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
            vec![String::from("west"), String::from("5"), String::from("2")]
        ],
    );
}
