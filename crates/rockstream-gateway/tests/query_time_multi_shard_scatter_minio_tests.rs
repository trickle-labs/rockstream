#![cfg(feature = "testcontainers")]

use std::sync::Arc;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer, QueryTimeShardTopology,
};
use rockstream_storage::{ShardDb, ShardReader};
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

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

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

async fn create_bucket(port: u16, bucket: &str) {
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
    let host = format!("127.0.0.1:{port}");
    let empty_hash = format!("{:x}", Sha256::digest(b""));
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{:x}",
        Sha256::digest(canonical.as_bytes())
    );
    let date_key = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, b"us-east-1");
    let service_key = hmac_sha256(&region_key, b"s3");
    let signing_key = hmac_sha256(&service_key, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", host)
        .header("X-Amz-Content-Sha256", empty_hash)
        .header("X-Amz-Date", datetime)
        .header("Authorization", authorization)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "bucket response: {response:?}"
    );
}

fn minio_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    )
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
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let bucket = format!("query-time-scatter-{}", Uuid::new_v4().simple());
    create_bucket(port, &bucket).await;
    let client = start_gateway(minio_store(port, &bucket), &bucket).await;

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
