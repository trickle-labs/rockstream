#![cfg(feature = "simulation")]

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, QueryTimeShardTopology,
};
use rockstream_sim::{buggify, buggify::buggify_init, SimRuntime};
use rockstream_storage::{ShardDb, ShardReader};
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
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway(
    readers: Vec<Arc<ShardReader>>,
    local: Arc<ShardDb>,
) -> tokio_postgres::Client {
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_table(CatalogTable {
        name: "sales".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "region".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    }));
    let topology = QueryTimeShardTopology::new(readers, 17);
    assert_eq!(topology.pinned_frontier(), 17);
    assert_eq!(topology.reader_count(), 3);
    let server = GatewayServer::with_shard_db_and_query_time_shard_topology(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        local,
        topology,
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

#[tokio::test]
async fn scatter_frontier_buggify_preserves_complete_exact_result() {
    for seed in [0x51_13_0001u64, 0x51_13_0002, 0x51_13_0003] {
        let runtime = SimRuntime::new(seed);
        buggify_init(seed);
        assert!(buggify!("query_time_scatter.before_reader_selection", 1.0));
        assert!(buggify!("query_time_scatter.between_shard_batches", 1.0));
        assert!(buggify!(
            "query_time_scatter.before_frontier_validation",
            1.0
        ));

        let store = Arc::new(InMemory::new());
        let mut shards = Vec::new();
        for (shard, row) in [(0, "1\twest"), (1, "2\teast"), (2, "3\tnorth")] {
            let path = format!("query-time-sim-{seed}-{shard}");
            let db = Arc::new(
                ShardDb::builder(path.clone(), store.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            db.put(
                format!("view_output/sales/{shard:02}").as_bytes(),
                row.as_bytes(),
            )
            .await
            .unwrap();
            db.flush().await.unwrap();
            shards.push((path, db));
        }
        let mut readers = futures::future::try_join_all(
            shards
                .iter()
                .map(|(path, _)| ShardReader::open(path.clone(), store.clone())),
        )
        .await
        .unwrap()
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        if runtime.random_bool(0.5) {
            readers.reverse();
        }
        let client = start_gateway(readers, shards[0].1.clone()).await;
        let rows: Vec<Vec<String>> = client
            .simple_query("SELECT id, region FROM sales WHERE id >= 1 ORDER BY id")
            .await
            .unwrap()
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(vec![
                    row.get("id").unwrap().to_string(),
                    row.get("region").unwrap().to_string(),
                ]),
                _ => None,
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                vec![String::from("1"), String::from("west")],
                vec![String::from("2"), String::from("east")],
                vec![String::from("3"), String::from("north")],
            ],
            "seed={seed}: replay with shard-order perturbation lost or duplicated a row"
        );
    }
}
