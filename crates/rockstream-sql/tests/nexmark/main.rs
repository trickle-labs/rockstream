use async_trait::async_trait;
use std::sync::Arc;
use tokio_postgres::NoTls;

use object_store::memory::InMemory;
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_gateway::error::GatewayError;
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_storage::ShardDb;

struct NoopViewReader;

#[async_trait]
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
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

#[tokio::test]
async fn test_nexmark_schema_creation_and_catalog() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-test-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Define SQL DDL queries
    let create_person_ddl = "CREATE TABLE person (
        id BIGINT,
        name VARCHAR,
        email_address VARCHAR,
        credit_card VARCHAR,
        city VARCHAR,
        state VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    let create_auction_ddl = "CREATE TABLE auction (
        id BIGINT,
        item_name VARCHAR,
        description VARCHAR,
        initial_bid BIGINT,
        reserve BIGINT,
        date_time BIGINT,
        expires BIGINT,
        seller BIGINT,
        category BIGINT,
        extra VARCHAR
    )";

    let create_bid_ddl = "CREATE TABLE bid (
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel VARCHAR,
        url VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    // Run CREATE TABLE statements
    client
        .simple_query(create_person_ddl)
        .await
        .expect("CREATE TABLE person failed");
    client
        .simple_query(create_auction_ddl)
        .await
        .expect("CREATE TABLE auction failed");
    client
        .simple_query(create_bid_ddl)
        .await
        .expect("CREATE TABLE bid failed");

    // Verify they are queryable in information_schema.tables
    let info_schema_rows = client
        .simple_query("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'")
        .await
        .expect("query information_schema.tables failed");

    let mut found_tables = std::collections::HashSet::new();
    for row in info_schema_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = row {
            let table_name = r.get("table_name").unwrap_or("");
            let table_type = r.get("table_type").unwrap_or("");
            if ["person", "auction", "bid"].contains(&table_name) {
                assert_eq!(
                    table_type, "BASE TABLE",
                    "expected BASE TABLE type for table {}",
                    table_name
                );
                found_tables.insert(table_name.to_string());
            }
        }
    }
    assert!(
        found_tables.contains("person"),
        "person table not found in information_schema.tables"
    );
    assert!(
        found_tables.contains("auction"),
        "auction table not found in information_schema.tables"
    );
    assert!(
        found_tables.contains("bid"),
        "bid table not found in information_schema.tables"
    );

    // Verify they are queryable in pg_catalog.pg_tables
    let pg_catalog_rows = client
        .simple_query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
        .await
        .expect("query pg_catalog.pg_tables failed");

    let mut found_pg_tables = std::collections::HashSet::new();
    for row in pg_catalog_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = row {
            let tablename = r.get("tablename").unwrap_or("");
            if ["person", "auction", "bid"].contains(&tablename) {
                found_pg_tables.insert(tablename.to_string());
            }
        }
    }
    assert!(
        found_pg_tables.contains("person"),
        "person table not found in pg_tables"
    );
    assert!(
        found_pg_tables.contains("auction"),
        "auction table not found in pg_tables"
    );
    assert!(
        found_pg_tables.contains("bid"),
        "bid table not found in pg_tables"
    );
}

#[tokio::test]
async fn test_nexmark_ingestion_lfs() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-ingest-lfs-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Define SQL DDL queries
    let create_person_ddl = "CREATE TABLE person (
        id BIGINT,
        name VARCHAR,
        email_address VARCHAR,
        credit_card VARCHAR,
        city VARCHAR,
        state VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    let create_auction_ddl = "CREATE TABLE auction (
        id BIGINT,
        item_name VARCHAR,
        description VARCHAR,
        initial_bid BIGINT,
        reserve BIGINT,
        date_time BIGINT,
        expires BIGINT,
        seller BIGINT,
        category BIGINT,
        extra VARCHAR
    )";

    let create_bid_ddl = "CREATE TABLE bid (
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel VARCHAR,
        url VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    // Run CREATE TABLE statements
    client
        .simple_query(create_person_ddl)
        .await
        .expect("CREATE TABLE person failed");
    client
        .simple_query(create_auction_ddl)
        .await
        .expect("CREATE TABLE auction failed");
    client
        .simple_query(create_bid_ddl)
        .await
        .expect("CREATE TABLE bid failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut expected_persons = 0;
    let mut expected_auctions = 0;
    let mut expected_bids = 0;
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(_) => expected_persons += 1,
            rockstream_sim::NexmarkEvent::Auction(_) => expected_auctions += 1,
            rockstream_sim::NexmarkEvent::Bid(_) => expected_bids += 1,
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-lfs-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Verify row counts in each table via SELECT COUNT(*)
    let person_rows = client
        .simple_query("SELECT COUNT(*) FROM person")
        .await
        .expect("SELECT COUNT(*) FROM person failed");
    let person_count = person_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(person_count, expected_persons, "person row count mismatch");

    let auction_rows = client
        .simple_query("SELECT COUNT(*) FROM auction")
        .await
        .expect("SELECT COUNT(*) FROM auction failed");
    let auction_count = auction_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(
        auction_count, expected_auctions,
        "auction row count mismatch"
    );

    let bid_rows = client
        .simple_query("SELECT COUNT(*) FROM bid")
        .await
        .expect("SELECT COUNT(*) FROM bid failed");
    let bid_count = bid_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(bid_count, expected_bids, "bid row count mismatch");
}

#[tokio::test]
#[cfg(feature = "testcontainers")]
async fn test_nexmark_ingestion_minio() {
    use object_store::aws::AmazonS3Builder;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    let minio = match MinIO::default().start().await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Docker is not available, skipping MinIO test: {:?}", e);
            return;
        }
    };
    let host = minio.get_host().await.expect("host");
    let port = minio.get_host_port_ipv4(9000).await.expect("port");
    create_minio_bucket(port, "testbucket").await;

    let store = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{host}:{port}"))
            .with_bucket_name("testbucket")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("S3 builder"),
    );

    let shard_db = Arc::new(
        ShardDb::builder("nexmark-ingest-minio-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Define SQL DDL queries
    let create_person_ddl = "CREATE TABLE person (
        id BIGINT,
        name VARCHAR,
        email_address VARCHAR,
        credit_card VARCHAR,
        city VARCHAR,
        state VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    let create_auction_ddl = "CREATE TABLE auction (
        id BIGINT,
        item_name VARCHAR,
        description VARCHAR,
        initial_bid BIGINT,
        reserve BIGINT,
        date_time BIGINT,
        expires BIGINT,
        seller BIGINT,
        category BIGINT,
        extra VARCHAR
    )";

    let create_bid_ddl = "CREATE TABLE bid (
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel VARCHAR,
        url VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )";

    // Run CREATE TABLE statements
    client
        .simple_query(create_person_ddl)
        .await
        .expect("CREATE TABLE person failed");
    client
        .simple_query(create_auction_ddl)
        .await
        .expect("CREATE TABLE auction failed");
    client
        .simple_query(create_bid_ddl)
        .await
        .expect("CREATE TABLE bid failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut expected_persons = 0;
    let mut expected_auctions = 0;
    let mut expected_bids = 0;
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(_) => expected_persons += 1,
            rockstream_sim::NexmarkEvent::Auction(_) => expected_auctions += 1,
            rockstream_sim::NexmarkEvent::Bid(_) => expected_bids += 1,
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-minio-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Verify row counts in each table via SELECT COUNT(*)
    let person_rows = client
        .simple_query("SELECT COUNT(*) FROM person")
        .await
        .expect("SELECT COUNT(*) FROM person failed");
    let person_count = person_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(person_count, expected_persons, "person row count mismatch");

    let auction_rows = client
        .simple_query("SELECT COUNT(*) FROM auction")
        .await
        .expect("SELECT COUNT(*) FROM auction failed");
    let auction_count = auction_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(
        auction_count, expected_auctions,
        "auction row count mismatch"
    );

    let bid_rows = client
        .simple_query("SELECT COUNT(*) FROM bid")
        .await
        .expect("SELECT COUNT(*) FROM bid failed");
    let bid_count = bid_rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count() as u64;
    assert_eq!(bid_count, expected_bids, "bid row count mismatch");
}

#[cfg(feature = "testcontainers")]
const MINIO_USER: &str = "minioadmin";
#[cfg(feature = "testcontainers")]
const MINIO_PASS: &str = "minioadmin";

#[cfg(feature = "testcontainers")]
fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(data))
}

#[cfg(feature = "testcontainers")]
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(feature = "testcontainers")]
fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let dpm: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = days + 1;
    month += 1;
    (year, month, day, h, m, s)
}

#[cfg(feature = "testcontainers")]
async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&signing_key, sts.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    );
    let resp = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &auth)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT request failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}
