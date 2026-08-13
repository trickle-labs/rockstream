use async_trait::async_trait;
use std::sync::Arc;
use tokio_postgres::NoTls;

use arrow::array::{
    Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_gateway::error::GatewayError;
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_sim::{Auction, Bid, NexmarkEvent, NexmarkGenerator, Person};
use rockstream_storage::ShardDb;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::{core::WaitFor, Image};

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

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

async fn run_retraction_test(
    shard_name: &str,
    views: &[&str],
    view_ddls: &[&str],
    oracle_queries: &[&str],
    seed: u64,
) {
    let store = Arc::new(InMemory::new());
    run_retraction_test_with_store(shard_name, views, view_ddls, oracle_queries, seed, store).await;
}

async fn run_retraction_test_with_store(
    shard_name: &str,
    views: &[&str],
    view_ddls: &[&str],
    oracle_queries: &[&str],
    seed: u64,
    store: Arc<dyn ObjectStore>,
) {
    let shard_db = Arc::new(ShardDb::builder(shard_name, store).build().await.unwrap());

    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // 1. Register base tables
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

    // Register side input if needed
    if views.contains(&"q13") {
        client
            .simple_query("CREATE TABLE side_input (key BIGINT, value VARCHAR)")
            .await
            .expect("CREATE TABLE side_input failed");
        for key in 0..2000 {
            client
                .simple_query(&format!(
                    "INSERT INTO side_input (key, value) VALUES ({key}, 'val_{key}')"
                ))
                .await
                .expect("INSERT side_input failed");
        }
    }

    // 2. Register views
    for ddl in view_ddls {
        client.simple_query(ddl).await.expect("CREATE VIEW failed");
    }

    // 3. Ingest initial 500 events
    let mut gen = NexmarkGenerator::new(seed);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();
    let mut initial_inserts = Vec::new();

    for _ in 0..500 {
        let event = gen.next().unwrap();
        match &event {
            NexmarkEvent::Person(p) => persons.push(p.clone()),
            NexmarkEvent::Auction(a) => auctions.push(a.clone()),
            NexmarkEvent::Bid(b) => bids.push(b.clone()),
        }
        initial_inserts.push(event.to_insert_sql());
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-retraction-initial'")
        .await
        .expect("SET idempotency_key failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in initial_inserts {
        client.simple_query(&sql).await.expect("INSERT failed");
    }
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // Helper to run DataFusion batch oracle
    let run_df_oracle = |query: &str,
                         cur_persons: &[Person],
                         cur_auctions: &[Auction],
                         cur_bids: &[Bid]| {
        let cur_persons = cur_persons.to_vec();
        let cur_auctions = cur_auctions.to_vec();
        let cur_bids = cur_bids.to_vec();
        let query = query.to_string();
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
                        cur_persons.iter().map(|p| p.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.email_address.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.credit_card.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.city.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.state.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_persons
                            .iter()
                            .map(|p| p.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_persons
                            .iter()
                            .map(|p| p.extra.clone())
                            .collect::<Vec<_>>(),
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
                        cur_auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.item_name.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.description.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.initial_bid as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.reserve as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.expires as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.seller as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.category as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_auctions
                            .iter()
                            .map(|a| a.extra.clone())
                            .collect::<Vec<_>>(),
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
                        cur_bids
                            .iter()
                            .map(|b| b.auction as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_bids.iter().map(|b| b.bidder as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_bids.iter().map(|b| b.price as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_bids
                            .iter()
                            .map(|b| b.channel.clone())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        cur_bids
                            .iter()
                            .map(|b| b.date_time as i64)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        cur_bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let b_table = MemTable::try_new(bid_schema, vec![vec![b_batch]]).unwrap();
            ctx.register_table("bid", Arc::new(b_table)).unwrap();

            if query.contains("side_input") {
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
            }

            let df = ctx.sql(&query).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut df_rows = Vec::new();
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
                        } else if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
                            a.value(row).to_string()
                        } else {
                            format!("{:?}", array)
                        };
                        fields.push(val_str);
                    }
                    df_rows.push(fields.join("\t"));
                }
            }
            df_rows
        }
    };

    // 4. Verify initial incremental == batch
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

        let mut oracle_rows = run_df_oracle(query, &persons, &auctions, &bids).await;
        psql_rows.sort();
        oracle_rows.sort();
        assert_eq!(
            psql_rows, oracle_rows,
            "Initial verification failed for view {view}"
        );
    }

    // 5. Apply a mixture of updates/deletes/inserts in transaction
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::from_seed([0u8; 32]);
    let mut dml_statements = Vec::new();

    // Generate exactly 50 modifications
    for _ in 0..50 {
        let op_type = rng.gen_range(0..10);
        if op_type < 4 {
            // Delete (40% chance)
            let tbl = rng.gen_range(0..3);
            match tbl {
                0 if !persons.is_empty() => {
                    let idx = rng.gen_range(0..persons.len());
                    let p = persons.remove(idx);
                    dml_statements.push(format!(
                        "DELETE FROM person WHERE id={}, name='{}', email_address='{}', credit_card='{}', city='{}', state='{}', date_time={}, extra='{}'",
                        p.id, esc(&p.name), esc(&p.email_address), esc(&p.credit_card), esc(&p.city), esc(&p.state), p.date_time, esc(&p.extra)
                    ));
                }
                1 if !auctions.is_empty() => {
                    let idx = rng.gen_range(0..auctions.len());
                    let a = auctions.remove(idx);
                    dml_statements.push(format!(
                        "DELETE FROM auction WHERE id={}, item_name='{}', description='{}', initial_bid={}, reserve={}, date_time={}, expires={}, seller={}, category={}, extra='{}'",
                        a.id, esc(&a.item_name), esc(&a.description), a.initial_bid, a.reserve, a.date_time, a.expires, a.seller, a.category, esc(&a.extra)
                    ));
                }
                2 if !bids.is_empty() => {
                    let idx = rng.gen_range(0..bids.len());
                    let b = bids.remove(idx);
                    dml_statements.push(format!(
                        "DELETE FROM bid WHERE auction={}, bidder={}, price={}, channel='{}', url='{}', date_time={}, extra='{}'",
                        b.auction, b.bidder, b.price, esc(&b.channel), esc(&b.url), b.date_time, esc(&b.extra)
                    ));
                }
                _ => {}
            }
        } else if op_type < 8 {
            // Update (40% chance)
            let tbl = rng.gen_range(0..3);
            match tbl {
                0 if !persons.is_empty() => {
                    let idx = rng.gen_range(0..persons.len());
                    let old = persons[idx].clone();
                    let mut new = old.clone();
                    new.name = format!("{} U", old.name);
                    new.city = "UpdatedCity".to_string();
                    dml_statements.push(format!(
                        "UPDATE person SET id={}, name='{}', email_address='{}', credit_card='{}', city='{}', state='{}', date_time={}, extra='{}' WHERE id={}, name='{}', email_address='{}', credit_card='{}', city='{}', state='{}', date_time={}, extra='{}'",
                        new.id, esc(&new.name), esc(&new.email_address), esc(&new.credit_card), esc(&new.city), esc(&new.state), new.date_time, esc(&new.extra),
                        old.id, esc(&old.name), esc(&old.email_address), esc(&old.credit_card), esc(&old.city), esc(&old.state), old.date_time, esc(&old.extra)
                    ));
                    persons[idx] = new;
                }
                1 if !auctions.is_empty() => {
                    let idx = rng.gen_range(0..auctions.len());
                    let old = auctions[idx].clone();
                    let mut new = old.clone();
                    new.item_name = format!("{} U", old.item_name);
                    new.initial_bid = old.initial_bid + 5;
                    dml_statements.push(format!(
                        "UPDATE auction SET id={}, item_name='{}', description='{}', initial_bid={}, reserve={}, date_time={}, expires={}, seller={}, category={}, extra='{}' WHERE id={}, item_name='{}', description='{}', initial_bid={}, reserve={}, date_time={}, expires={}, seller={}, category={}, extra='{}'",
                        new.id, esc(&new.item_name), esc(&new.description), new.initial_bid, new.reserve, new.date_time, new.expires, new.seller, new.category, esc(&new.extra),
                        old.id, esc(&old.item_name), esc(&old.description), old.initial_bid, old.reserve, old.date_time, old.expires, old.seller, old.category, esc(&old.extra)
                    ));
                    auctions[idx] = new;
                }
                2 if !bids.is_empty() => {
                    let idx = rng.gen_range(0..bids.len());
                    let old = bids[idx].clone();
                    let mut new = old.clone();
                    new.price = old.price + 10;
                    dml_statements.push(format!(
                        "UPDATE bid SET auction={}, bidder={}, price={}, channel='{}', url='{}', date_time={}, extra='{}' WHERE auction={}, bidder={}, price={}, channel='{}', url='{}', date_time={}, extra='{}'",
                        new.auction, new.bidder, new.price, esc(&new.channel), esc(&new.url), new.date_time, esc(&new.extra),
                        old.auction, old.bidder, old.price, esc(&old.channel), esc(&old.url), old.date_time, esc(&old.extra)
                    ));
                    bids[idx] = new;
                }
                _ => {}
            }
        } else {
            // Insert (20% chance)
            if let Some(event) = gen.next() {
                dml_statements.push(event.to_insert_sql());
                match event {
                    NexmarkEvent::Person(p) => persons.push(p),
                    NexmarkEvent::Auction(a) => auctions.push(a),
                    NexmarkEvent::Bid(b) => bids.push(b),
                }
            }
        }
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'nexmark-retraction-updates'")
        .await
        .expect("SET idempotency_key failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    for sql in dml_statements {
        client
            .simple_query(&sql)
            .await
            .expect("DML statement failed");
    }
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    // 6. Verify incremental == batch after modifications
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

        let mut oracle_rows = run_df_oracle(query, &persons, &auctions, &bids).await;
        psql_rows.sort();
        oracle_rows.sort();
        assert_eq!(
            psql_rows, oracle_rows,
            "Post-retraction verification failed for view {view}"
        );
    }
}

#[tokio::test]
async fn test_nexmark_q0_q3_retraction_lfs() {
    let views = ["q0", "q1", "q2", "q3"];
    let view_ddls = [
        "CREATE VIEW q0 AS SELECT * FROM bid",
        "CREATE VIEW q1 AS SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
        "CREATE VIEW q2 AS SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        "CREATE VIEW q3 AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
    ];
    let oracle_queries = [
        "SELECT auction, bidder, price, channel, url, date_time, extra FROM bid",
        "SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
        "SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        "SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
    ];

    run_retraction_test(
        "nexmark-q0-q3-retraction",
        &views,
        &view_ddls,
        &oracle_queries,
        42,
    )
    .await;
}

#[tokio::test]
async fn test_nexmark_q4_q9_retraction_lfs() {
    let views = ["q4", "q5", "q6", "q7", "q8", "q9"];
    let view_ddls = [
        "CREATE VIEW q4 AS SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category",
        "CREATE VIEW q5 AS SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5",
        "CREATE VIEW q6 AS SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)",
        "CREATE VIEW q7 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q8 AS SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000",
        "CREATE VIEW q9 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1",
    ];
    let oracle_queries = [
        "SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category",
        "SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5",
        "SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)",
        "SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1",
    ];

    run_retraction_test(
        "nexmark-q4-q9-retraction",
        &views,
        &view_ddls,
        &oracle_queries,
        43,
    )
    .await;
}

#[tokio::test]
async fn test_nexmark_q12_q22_retraction_lfs() {
    let views = [
        "q12", "q13", "q14", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ];
    let view_ddls = [
        "CREATE VIEW q12 AS SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "CREATE VIEW q14 AS SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "CREATE VIEW q16 AS SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q17 AS SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q18 AS SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "CREATE VIEW q19 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "CREATE VIEW q20 AS SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "CREATE VIEW q21 AS SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "CREATE VIEW q22 AS SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
    ];
    let oracle_queries = [
        "SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
    ];

    run_retraction_test(
        "nexmark-q12-q22-retraction",
        &views,
        &view_ddls,
        &oracle_queries,
        44,
    )
    .await;
}

// ─── MinIO and Soak Retraction Tests ────────────────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
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

#[derive(Debug, Clone)]
pub struct MinIO2024 {
    env_vars: HashMap<String, String>,
}

impl Default for MinIO2024 {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("MINIO_CONSOLE_ADDRESS".to_owned(), ":9001".to_owned());
        Self { env_vars }
    }
}

impl Image for MinIO2024 {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-11-07T00-52-20Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("API:")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec!["server", "/data"]
    }
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO2024>, u16) {
    let container = MinIO2024::default()
        .start()
        .await
        .expect("failed to start MinIO container; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

async fn run_minio_retraction_test(
    shard_name: &str,
    views: &[&str],
    view_ddls: &[&str],
    oracle_queries: &[&str],
    seed: u64,
) {
    if !docker_available() {
        eprintln!("SKIP {shard_name}: Docker not available");
        return;
    }
    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);
    run_retraction_test_with_store(shard_name, views, view_ddls, oracle_queries, seed, store).await;
}

#[tokio::test]
async fn test_nexmark_q0_q9_retraction_minio() {
    let views = ["q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9"];
    let view_ddls = [
        "CREATE VIEW q0 AS SELECT * FROM bid",
        "CREATE VIEW q1 AS SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
        "CREATE VIEW q2 AS SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
        "CREATE VIEW q3 AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
        "CREATE VIEW q4 AS SELECT a.category, CAST(AVG(b.price) AS BIGINT) as avg_price FROM auction a JOIN bid b ON a.id = b.auction WHERE b.date_time >= a.date_time AND b.date_time <= a.expires GROUP BY a.category",
        "CREATE VIEW q5 AS SELECT auction, num FROM (SELECT auction, num, ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY num DESC) as rn FROM (SELECT auction, COUNT(*) as num, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY auction, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)))) WHERE rn <= 5",
        "CREATE VIEW q6 AS SELECT seller, CAST(AVG(price) OVER (PARTITION BY seller ORDER BY date_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS BIGINT) as avg_price FROM (SELECT a.seller, b.price, b.date_time FROM auction a JOIN bid b ON a.id = b.auction)",
        "CREATE VIEW q7 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, MAX(price) as max_price FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q8 AS SELECT p.id, p.name, a.id as auction_id FROM person p JOIN auction a ON p.id = a.seller WHERE a.date_time >= p.date_time AND a.date_time <= p.date_time + 43200000",
        "CREATE VIEW q9 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1",
    ];
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
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 1",
    ];

    run_minio_retraction_test(
        "nexmark-q0-q9-retraction-minio",
        &views,
        &view_ddls,
        &oracle_queries,
        45,
    )
    .await;
}

#[tokio::test]
async fn test_nexmark_q12_q22_retraction_minio() {
    let views = [
        "q12", "q13", "q14", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
    ];
    let view_ddls = [
        "CREATE VIEW q12 AS SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "CREATE VIEW q14 AS SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "CREATE VIEW q16 AS SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q17 AS SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q18 AS SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "CREATE VIEW q19 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "CREATE VIEW q20 AS SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "CREATE VIEW q21 AS SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "CREATE VIEW q22 AS SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
    ];
    let oracle_queries = [
        "SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
    ];

    run_minio_retraction_test(
        "nexmark-q12-q22-retraction-minio",
        &views,
        &view_ddls,
        &oracle_queries,
        46,
    )
    .await;
}

#[tokio::test]
async fn test_nexmark_retraction_100_seeds_lfs() {
    // Run 100 seeds with bounded concurrency to keep runtime low and prevent overwhelming system resources.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
    let mut tasks = Vec::new();

    for seed in 0..100 {
        let sem = semaphore.clone();
        let shard_name = format!("nexmark-retraction-soak-{seed}");

        let t = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let views = ["q0", "q1", "q2", "q3"];
            let view_ddls = [
                "CREATE VIEW q0 AS SELECT * FROM bid",
                "CREATE VIEW q1 AS SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
                "CREATE VIEW q2 AS SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
                "CREATE VIEW q3 AS SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
            ];
            let oracle_queries = [
                "SELECT auction, bidder, price, channel, url, date_time, extra FROM bid",
                "SELECT auction, bidder, price * 90 / 100 AS price, channel, url, date_time, extra FROM bid",
                "SELECT auction, price, date_time FROM bid WHERE auction % 123 = 0",
                "SELECT p.name, p.city, p.state, a.id FROM auction a JOIN person p ON a.seller = p.id WHERE (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA') AND a.category = 10",
            ];
            run_retraction_test(
                &shard_name,
                &views,
                &view_ddls,
                &oracle_queries,
                seed as u64,
            )
            .await;
        });
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap();
    }
}
