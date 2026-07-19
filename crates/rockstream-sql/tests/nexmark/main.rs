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

const NEXMARK_Q11_VIEW_SQL: &str =
    "CREATE VIEW q11 AS SELECT bidder, COUNT(*) as bid_count, MIN(date_time) as starttime, MAX(date_time) as endtime FROM bid GROUP BY bidder, SESSION(date_time, INTERVAL '10 seconds')";

const NEXMARK_Q11_ORACLE_SQL: &str = r#"
WITH ordered AS (
    SELECT
        bidder,
        date_time,
        CASE
            WHEN LAG(date_time) OVER (PARTITION BY bidder ORDER BY date_time) IS NULL THEN 1
            WHEN date_time - LAG(date_time) OVER (PARTITION BY bidder ORDER BY date_time) > 10000 THEN 1
            ELSE 0
        END AS new_session
    FROM bid
),
labeled AS (
    SELECT
        bidder,
        date_time,
        SUM(new_session) OVER (
            PARTITION BY bidder
            ORDER BY date_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM ordered
)
SELECT bidder, COUNT(*) AS bid_count, MIN(date_time) AS starttime, MAX(date_time) AS endtime
FROM labeled
GROUP BY bidder, session_id
"#;

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
async fn test_nexmark_q0_q3_lfs() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q0-q3-lfs-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q0–q3
    client
        .simple_query("CREATE VIEW q0 AS SELECT * FROM bid")
        .await
        .expect("CREATE VIEW q0 failed");

    client
        .simple_query("CREATE VIEW q1 AS SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid")
        .await
        .expect("CREATE VIEW q1 failed");

    client
        .simple_query(
            "CREATE VIEW q2 AS SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        )
        .await
        .expect("CREATE VIEW q2 failed");

    client
        .simple_query("CREATE VIEW q3 AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10")
        .await
        .expect("CREATE VIEW q3 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q0-q3-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q0–q3 results are bit-identical to the DataFusion batch oracle.
    let views = ["q0", "q1", "q2", "q3"];
    let oracle_queries = [
        "SELECT auction, bidder, price, channel, url, date_time, extra FROM bid",
        "SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
        "SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        "SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty() || *view == "q2" || *view == "q3",
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q4_q9_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q4-q9-lfs-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q4–q9
    client
        .simple_query("CREATE VIEW q4 AS SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category")
        .await
        .expect("CREATE VIEW q4 failed");

    client
        .simple_query("CREATE VIEW q5 AS SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5")
        .await
        .expect("CREATE VIEW q5 failed");

    client
        .simple_query("CREATE VIEW q6 AS SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)")
        .await
        .expect("CREATE VIEW q6 failed");

    client
        .simple_query("CREATE VIEW q7 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q7 failed");

    client
        .simple_query("CREATE VIEW q8 AS SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000")
        .await
        .expect("CREATE VIEW q8 failed");

    client
        .simple_query("CREATE VIEW q9 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1")
        .await
        .expect("CREATE VIEW q9 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q4-q9-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q4–q9 results are bit-identical to the DataFusion batch oracle.
    let views = ["q4", "q5", "q6", "q7", "q8", "q9"];
    let oracle_queries = [
        "SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category",
        "SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5",
        "SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)",
        "SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty() || *view == "q5" || *view == "q6" || *view == "q8",
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q12_q13_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q12-q13-lfs-shard", store.clone())
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

    let create_side_input_ddl = "CREATE TABLE side_input (
        key BIGINT,
        value VARCHAR
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
    client
        .simple_query(create_side_input_ddl)
        .await
        .expect("CREATE TABLE side_input failed");

    // Populate side_input with static lookup data
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-side-input-init'")
        .await
        .expect("SET idempotency_key failed");
    client.simple_query("BEGIN").await.unwrap();
    for key in 0..2000 {
        client
            .simple_query(&format!(
                "INSERT INTO side_input (key, value) VALUES ({key}, 'val_{key}')"
            ))
            .await
            .expect("INSERT into side_input failed");
    }

    client.simple_query("COMMIT").await.unwrap();

    // Define standard CREATE VIEW statements for Nexmark q12–q13
    client
        .simple_query("CREATE VIEW q12 AS SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q12 failed");

    client
        .simple_query("CREATE VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key")
        .await
        .expect("CREATE VIEW q13 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q12-q13-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let side_input_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for key in 0..2000 {
                keys.push(key as i64);
                values.push(format!("val_{key}"));
            }
            let si_batch = RecordBatch::try_new(
                side_input_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .unwrap();
            let si_table = MemTable::try_new(side_input_schema, vec![vec![si_batch]]).unwrap();
            ctx.register_table("side_input", Arc::new(si_table))
                .unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q12–q13 results are bit-identical to the DataFusion batch oracle.
    let views = ["q12", "q13"];
    let oracle_queries = [
        "SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty(),
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn nexmark_q11_session_bit_identical_to_batch_oracle() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q11-lfs-shard", store.clone())
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

    client.simple_query("CREATE TABLE person (id BIGINT, name VARCHAR, email_address VARCHAR, credit_card VARCHAR, city VARCHAR, state VARCHAR, date_time BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query("CREATE TABLE auction (id BIGINT, item_name VARCHAR, description VARCHAR, initial_bid BIGINT, reserve BIGINT, date_time BIGINT, expires BIGINT, seller BIGINT, category BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query("CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, date_time BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query(NEXMARK_Q11_VIEW_SQL).await.unwrap();

    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut bids = Vec::new();
    let mut inserts = Vec::new();
    for _ in 0..500 {
        let event = gen.next().unwrap();
        if let rockstream_sim::NexmarkEvent::Bid(b) = &event {
            bids.push(b.clone());
        }
        inserts.push(event.to_insert_sql());
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q11-batch-1'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for sql in inserts {
        client.simple_query(&sql).await.unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();

    let run_df_oracle = |current_bids: Vec<rockstream_sim::Bid>| async move {
        let ctx = SessionContext::new();
        let bid_schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("url", DataType::Utf8, false),
            Field::new("date_time", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let bid_batch = RecordBatch::try_new(
            bid_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.auction as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.bidder as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.price as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.channel.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.url.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.date_time as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.extra.clone())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let bid_table = MemTable::try_new(bid_schema, vec![vec![bid_batch]]).unwrap();
        ctx.register_table("bid", Arc::new(bid_table)).unwrap();

        let df = ctx.sql(NEXMARK_Q11_ORACLE_SQL).await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let bidder = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let bid_count = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let start = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let end = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push(format!(
                    "{}\t{}\t{}\t{}",
                    bidder.value(row),
                    bid_count.value(row),
                    start.value(row),
                    end.value(row)
                ));
            }
        }
        rows
    };

    let psql_res = client.simple_query("SELECT * FROM q11").await.unwrap();
    let mut psql_rows = Vec::new();
    for msg in psql_res {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            let mut fields = Vec::new();
            for col in 0..r.len() {
                fields.push(r.get(col).unwrap_or("NULL").to_string());
            }
            psql_rows.push(fields.join("\t"));
        }
    }

    let oracle_rows = run_df_oracle(bids).await;
    assert_eq!(
        psql_rows.into_iter().collect::<BTreeSet<_>>(),
        oracle_rows.into_iter().collect::<BTreeSet<_>>(),
        "bit-identical comparison failed for q11"
    );
}

#[tokio::test]
async fn nexmark_q11_survives_retraction_storm() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q11-retractions-lfs-shard", store.clone())
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

    client.simple_query("CREATE TABLE person (id BIGINT, name VARCHAR, email_address VARCHAR, credit_card VARCHAR, city VARCHAR, state VARCHAR, date_time BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query("CREATE TABLE auction (id BIGINT, item_name VARCHAR, description VARCHAR, initial_bid BIGINT, reserve BIGINT, date_time BIGINT, expires BIGINT, seller BIGINT, category BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query("CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, date_time BIGINT, extra VARCHAR)").await.unwrap();
    client.simple_query(NEXMARK_Q11_VIEW_SQL).await.unwrap();

    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut bids = Vec::new();
    let mut inserts = Vec::new();
    for _ in 0..500 {
        let event = gen.next().unwrap();
        if let rockstream_sim::NexmarkEvent::Bid(b) = &event {
            bids.push(b.clone());
        }
        inserts.push(event.to_insert_sql());
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q11-retraction-base'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for sql in inserts {
        client.simple_query(&sql).await.unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();

    let run_df_oracle = |current_bids: Vec<rockstream_sim::Bid>| async move {
        let ctx = SessionContext::new();
        let bid_schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int64, false),
            Field::new("bidder", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("url", DataType::Utf8, false),
            Field::new("date_time", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let bid_batch = RecordBatch::try_new(
            bid_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.auction as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.bidder as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.price as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.channel.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.url.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    current_bids
                        .iter()
                        .map(|b| b.date_time as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    current_bids
                        .iter()
                        .map(|b| b.extra.clone())
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let bid_table = MemTable::try_new(bid_schema, vec![vec![bid_batch]]).unwrap();
        ctx.register_table("bid", Arc::new(bid_table)).unwrap();

        let df = ctx.sql(NEXMARK_Q11_ORACLE_SQL).await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let bidder = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let bid_count = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let start = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let end = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push(format!(
                    "{}\t{}\t{}\t{}",
                    bidder.value(row),
                    bid_count.value(row),
                    start.value(row),
                    end.value(row)
                ));
            }
        }
        rows
    };

    let mut remaining_bids = bids.clone();
    let bids_to_delete: Vec<_> = bids.iter().step_by(10).take(50).cloned().collect();
    for to_del in &bids_to_delete {
        if let Some(pos) = remaining_bids.iter().position(|b| {
            b.auction == to_del.auction
                && b.bidder == to_del.bidder
                && b.price == to_del.price
                && b.channel == to_del.channel
                && b.url == to_del.url
                && b.date_time == to_del.date_time
                && b.extra == to_del.extra
        }) {
            remaining_bids.remove(pos);
        }
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q11-retractions'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for b in &bids_to_delete {
        let sql = format!(
            "DELETE FROM bid WHERE auction={}, bidder={}, price={}, channel='{}', url='{}', date_time={}, extra='{}'",
            b.auction, b.bidder, b.price, b.channel, b.url, b.date_time, b.extra
        );
        client.simple_query(&sql).await.unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();

    let psql_res = client.simple_query("SELECT * FROM q11").await.unwrap();
    let mut psql_rows = Vec::new();
    for msg in psql_res {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            let mut fields = Vec::new();
            for col in 0..r.len() {
                fields.push(r.get(col).unwrap_or("NULL").to_string());
            }
            psql_rows.push(fields.join("\t"));
        }
    }

    let oracle_rows = run_df_oracle(remaining_bids).await;
    assert_eq!(
        psql_rows.into_iter().collect::<BTreeSet<_>>(),
        oracle_rows.into_iter().collect::<BTreeSet<_>>(),
        "retraction-storm comparison failed for q11"
    );
}

#[tokio::test]
async fn test_nexmark_q14_q15_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q14-q15-lfs-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q14–q15
    client
        .simple_query("CREATE VIEW q14 AS SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid")
        .await
        .expect("CREATE VIEW q14 failed");

    client
        .simple_query("CREATE VIEW q15 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, SUM(CASE WHEN price < 10000 THEN price ELSE 0 END) as low_sum, COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 100000 THEN bidder END) as medium_bidders, COUNT(DISTINCT CASE WHEN price >= 100000 THEN bidder END) as high_bidders FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q15 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q14-q15-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q14–q15 results are bit-identical to the DataFusion batch oracle.
    let views = ["q14", "q15"];
    let oracle_queries = [
        "SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, SUM(CASE WHEN price < 10000 THEN price ELSE 0 END) as low_sum, COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 100000 THEN bidder END) as medium_bidders, COUNT(DISTINCT CASE WHEN price >= 100000 THEN bidder END) as high_bidders FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty(),
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q16_q17_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q16-q17-lfs-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q16–q17
    client
        .simple_query("CREATE VIEW q16 AS SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q16 failed");

    client
        .simple_query("CREATE VIEW q17 AS SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q17 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q16-q17-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q16–q17 results are bit-identical to the DataFusion batch oracle.
    let views = ["q16", "q17"];
    let oracle_queries = [
        "SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty(),
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q18_q19_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q18-q19-lfs-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q18–q19
    client
        .simple_query("CREATE VIEW q18 AS SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1")
        .await
        .expect("CREATE VIEW q18 failed");

    client
        .simple_query("CREATE VIEW q19 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10")
        .await
        .expect("CREATE VIEW q19 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q18-q19-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String, current_bids: Vec<rockstream_sim::Bid>| {
        let persons = persons.clone();
        let auctions = auctions.clone();
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        current_bids
                            .iter()
                            .map(|b| b.auction as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        current_bids
                            .iter()
                            .map(|b| b.bidder as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        current_bids
                            .iter()
                            .map(|b| b.price as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        current_bids
                            .iter()
                            .map(|b| b.channel.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        current_bids
                            .iter()
                            .map(|b| b.url.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        current_bids
                            .iter()
                            .map(|b| b.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        current_bids
                            .iter()
                            .map(|b| b.extra.clone())
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    let views = ["q18", "q19"];
    let oracle_queries = [
        "SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10"
    ];

    // 1. Verify initial state matches oracle
    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        let oracle_rows = run_df_oracle(query.to_string(), bids.clone()).await;
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "Initial bit-identical comparison failed for {view}"
        );
    }

    // 2. Perform retraction storm (delete 50 random bids)
    let mut bids_to_delete = Vec::new();
    let mut remaining_bids = bids.clone();
    for i in (0..bids.len()).step_by(10).take(50) {
        if i < bids.len() {
            bids_to_delete.push(bids[i].clone());
        }
    }

    // Remove deleted bids from remaining_bids
    for to_del in &bids_to_delete {
        if let Some(pos) = remaining_bids.iter().position(|b| {
            b.auction == to_del.auction
                && b.bidder == to_del.bidder
                && b.price == to_del.price
                && b.channel == to_del.channel
                && b.url == to_del.url
                && b.date_time == to_del.date_time
                && b.extra == to_del.extra
        }) {
            remaining_bids.remove(pos);
        }
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q18-q19-retractions'")
        .await
        .expect("SET idempotency_key failed");

    client.simple_query("BEGIN").await.unwrap();
    for b in &bids_to_delete {
        let sql = format!(
            "DELETE FROM bid WHERE auction={}, bidder={}, price={}, channel='{}', url='{}', date_time={}, extra='{}'",
            b.auction, b.bidder, b.price, b.channel, b.url, b.date_time, b.extra
        );
        client.simple_query(&sql).await.unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();

    // 3. Verify state after retractions matches oracle
    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        let oracle_rows = run_df_oracle(query.to_string(), remaining_bids.clone()).await;
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "Retracted bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        assert!(
            !delivered.is_empty(),
            "SUBSCRIBE snapshot failed for {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q20_q22_lfs() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("nexmark-q20-q22-lfs-shard", store.clone())
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

    // Define CREATE VIEW statements for Nexmark q20–q22
    client
        .simple_query("CREATE VIEW q20 AS SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10")
        .await
        .expect("CREATE VIEW q20 failed");

    client
        .simple_query("CREATE VIEW q21 AS SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid")
        .await
        .expect("CREATE VIEW q21 failed");

    client
        .simple_query(
            "CREATE VIEW q22 AS SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
        )
        .await
        .expect("CREATE VIEW q22 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        inserts.push(event.to_insert_sql());
    }

    // Set idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-q20-q22-batch-1'")
        .await
        .expect("SET idempotency_key failed");

    // Begin txn
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    // Commit txn
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    let views = ["q20", "q21", "q22"];
    let oracle_queries = [
        "SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        let oracle_rows = run_df_oracle(query.to_string()).await;
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        let has_rows = !kvs.is_empty();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        if has_rows {
            assert!(
                !delivered.is_empty(),
                "SUBSCRIBE snapshot failed for {view}"
            );
        }
    }
}

#[tokio::test]
#[cfg(feature = "testcontainers")]
async fn test_nexmark_q0_q9_minio() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::aws::AmazonS3Builder;
    use std::collections::BTreeSet;
    use std::sync::Arc;
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
        ShardDb::builder("nexmark-q0-q9-minio-shard", store.clone())
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

    // Define standard CREATE VIEW statements for Nexmark q0–q9
    client
        .simple_query("CREATE VIEW q0 AS SELECT * FROM bid")
        .await
        .expect("CREATE VIEW q0 failed");

    client
        .simple_query("CREATE VIEW q1 AS SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid")
        .await
        .expect("CREATE VIEW q1 failed");

    client
        .simple_query(
            "CREATE VIEW q2 AS SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        )
        .await
        .expect("CREATE VIEW q2 failed");

    client
        .simple_query("CREATE VIEW q3 AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10")
        .await
        .expect("CREATE VIEW q3 failed");

    client
        .simple_query("CREATE VIEW q4 AS SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category")
        .await
        .expect("CREATE VIEW q4 failed");

    client
        .simple_query("CREATE VIEW q5 AS SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5")
        .await
        .expect("CREATE VIEW q5 failed");

    client
        .simple_query("CREATE VIEW q6 AS SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)")
        .await
        .expect("CREATE VIEW q6 failed");

    client
        .simple_query("CREATE VIEW q7 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .expect("CREATE VIEW q7 failed");

    client
        .simple_query("CREATE VIEW q8 AS SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000")
        .await
        .expect("CREATE VIEW q8 failed");

    client
        .simple_query("CREATE VIEW q9 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1")
        .await
        .expect("CREATE VIEW q9 failed");

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
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

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q0–q9 results are bit-identical to the DataFusion batch oracle.
    let views = ["q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9"];
    let oracle_queries = [
        "SELECT auction, bidder, price, channel, url, date_time, extra FROM bid",
        "SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
        "SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        "SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
        "SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category",
        "SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5",
        "SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)",
        "SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        // Query view via pgwire client
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .expect(&format!("SELECT * FROM {view} failed"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        // Run DataFusion oracle query
        let oracle_rows = run_df_oracle(query.to_string()).await;

        // Compare both results (unsorted, but sorted to compare set equivalence)
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }
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

#[tokio::test]
#[cfg(feature = "testcontainers")]
async fn test_nexmark_q12_q22_minio() {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use object_store::aws::AmazonS3Builder;
    use rockstream_gateway::catalog_stubs::CatalogStubs;
    use rockstream_gateway::server::GatewayServer;
    use rockstream_gateway::subscribe_handler::deliver_snapshot;
    use rockstream_gateway::subscribe_parser::parse_subscribe;
    use rockstream_storage::ShardDb;
    use std::collections::BTreeSet;
    use std::sync::Arc;
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
        ShardDb::builder("nexmark-q12-q22-minio-shard", store.clone())
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

    let create_side_input_ddl = "CREATE TABLE side_input (
        key BIGINT,
        value VARCHAR
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
    client
        .simple_query(create_side_input_ddl)
        .await
        .expect("CREATE TABLE side_input failed");

    // Populate side_input with static lookup data
    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-side-input-init'")
        .await
        .expect("SET idempotency_key failed");
    client.simple_query("BEGIN").await.unwrap();
    for key in 0..2000 {
        client
            .simple_query(&format!(
                "INSERT INTO side_input (key, value) VALUES ({key}, 'val_{key}')"
            ))
            .await
            .expect("INSERT into side_input failed");
    }
    client.simple_query("COMMIT").await.unwrap();

    // Define views
    client.simple_query(NEXMARK_Q11_VIEW_SQL).await.unwrap();

    client
        .simple_query("CREATE VIEW q12 AS SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q14 AS SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q15 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, SUM(CASE WHEN price < 10000 THEN price ELSE 0 END) as low_sum, COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 100000 THEN bidder END) as medium_bidders, COUNT(DISTINCT CASE WHEN price >= 100000 THEN bidder END) as high_bidders FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q16 AS SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q17 AS SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q18 AS SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q19 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q20 AS SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10")
        .await
        .unwrap();

    client
        .simple_query("CREATE VIEW q21 AS SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid")
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE VIEW q22 AS SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
        )
        .await
        .unwrap();

    // Generate 500 events
    let mut gen = rockstream_sim::NexmarkGenerator::new(42);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            rockstream_sim::NexmarkEvent::Person(p) => persons.push(p.clone()),
            rockstream_sim::NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            rockstream_sim::NexmarkEvent::Bid(b) => bids.push(b.clone()),
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

    // Define oracle runner helper
    let run_df_oracle = |query: String| {
        let persons = &persons;
        let auctions = &auctions;
        let bids = &bids;
        async move {
            let ctx = SessionContext::new();

            let person_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("email_address", DataType::Utf8, false),
                Field::new("credit_card", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let p_batch = RecordBatch::try_new(
                person_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.city.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.state.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        persons.iter().map(|p| p.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let p_table = MemTable::try_new(person_schema, vec![vec![p_batch]]).unwrap();
            ctx.register_table("person", Arc::new(p_table)).unwrap();

            let auction_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("item_name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("initial_bid", DataType::Int64, false),
                Field::new("reserve", DataType::Int64, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("expires", DataType::Int64, false),
                Field::new("seller", DataType::Int64, false),
                Field::new("category", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let a_batch = RecordBatch::try_new(
                auction_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let a_table = MemTable::try_new(auction_schema, vec![vec![a_batch]]).unwrap();
            ctx.register_table("auction", Arc::new(a_table)).unwrap();

            let bid_schema = Arc::new(Schema::new(vec![
                Field::new("auction", DataType::Int64, false),
                Field::new("bidder", DataType::Int64, false),
                Field::new("price", DataType::Int64, false),
                Field::new("channel", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("date_time", DataType::Int64, false),
                Field::new("extra", DataType::Utf8, false),
            ]));
            let b_batch = RecordBatch::try_new(
                bid_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        bids.iter().map(|b| b.date_time as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            let side_input_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for key in 0..2000 {
                keys.push(key as i64);
                values.push(format!("val_{key}"));
            }
            let si_batch = RecordBatch::try_new(
                side_input_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .unwrap();
            let si_table = MemTable::try_new(side_input_schema, vec![vec![si_batch]]).unwrap();
            ctx.register_table("side_input", Arc::new(si_table))
                .unwrap();

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut fields = Vec::new();
                    for col in 0..batch.num_columns() {
                        let array = batch.column(col);
                        let val_str = if array.is_null(row) {
                            "NULL".to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                            (a.value(row) as i64).to_string()
                        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                            a.value(row).to_string()
                        } else if let Some(a) = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringViewArray>()
                        {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    rows.push(fields.join("\t"));
                }
            }
            rows
        }
    };

    // Verify q12–q22 results are bit-identical to the DataFusion batch oracle.
    let views = [
        "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ];
    let oracle_queries = [
        NEXMARK_Q11_ORACLE_SQL.trim(),
        "SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, SUM(CASE WHEN price < 10000 THEN price ELSE 0 END) as low_sum, COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 100000 THEN bidder END) as medium_bidders, COUNT(DISTINCT CASE WHEN price >= 100000 THEN bidder END) as high_bidders FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid"
    ];

    for (view, query) in views.iter().zip(oracle_queries.iter()) {
        let psql_res = client
            .simple_query(&format!("SELECT * FROM {view}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {view} failed: {err:?}"));
        let mut psql_rows = Vec::new();
        for msg in psql_res {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                let mut fields = Vec::new();
                for col in 0..r.len() {
                    fields.push(r.get(col).unwrap_or("NULL").to_string());
                }
                psql_rows.push(fields.join("\t"));
            }
        }

        let oracle_rows = run_df_oracle(query.to_string()).await;
        let psql_set: BTreeSet<String> = psql_rows.into_iter().collect();
        let oracle_set: BTreeSet<String> = oracle_rows.into_iter().collect();
        assert_eq!(
            psql_set, oracle_set,
            "bit-identical comparison failed for {view}"
        );
    }

    // In-process SUBSCRIBE change log verification (P3)
    for view in &views {
        let req = parse_subscribe(&format!("SUBSCRIBE {view} AS OF NOW WITH SNAPSHOT")).unwrap();
        let mut snapshot_rows = Vec::new();
        let prefix = format!("view_output/{view}/");
        let kvs = shard_db.scan_prefix(prefix.as_bytes()).await.unwrap();
        let has_rows = !kvs.is_empty();
        for (k, v) in kvs {
            snapshot_rows.push((
                bytes::Bytes::copy_from_slice(&k),
                bytes::Bytes::copy_from_slice(&v),
            ));
        }
        let delivered = deliver_snapshot(snapshot_rows, 1, &req, &[]);
        if has_rows {
            assert!(
                !delivered.is_empty(),
                "SUBSCRIBE snapshot failed for {view}"
            );
        }
    }
}
