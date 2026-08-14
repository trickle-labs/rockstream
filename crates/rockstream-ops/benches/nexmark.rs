use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use object_store::memory::InMemory;
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_gateway::error::GatewayError;
use rockstream_gateway::server::GatewayServer;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_ops::nexmark_regression::{percentile, NexmarkBenchmarkSummary};
use rockstream_sim::{NexmarkEvent, NexmarkGenerator};
use rockstream_storage::ShardDb;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio_postgres::NoTls;

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

struct BenchEnv {
    client: tokio_postgres::Client,
    _server_handle: tokio::task::JoinHandle<()>,
    _shard_db: Arc<ShardDb>,
}

const DATASET_SIZE: usize = 100;

async fn setup_env(seed: u64, num_base_events: usize) -> BenchEnv {
    let store = Arc::new(InMemory::new());
    let shard_name = format!("bench-nexmark-{seed}");
    let shard_db = Arc::new(
        ShardDb::builder(&shard_name, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, server_handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Register tables
    client
        .simple_query(
            "CREATE TABLE person (
        id BIGINT,
        name VARCHAR,
        email_address VARCHAR,
        credit_card VARCHAR,
        city VARCHAR,
        state VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )",
        )
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE TABLE auction (
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
    )",
        )
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE TABLE bid (
        auction BIGINT,
        bidder BIGINT,
        price BIGINT,
        channel VARCHAR,
        url VARCHAR,
        date_time BIGINT,
        extra VARCHAR
    )",
        )
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE side_input (key BIGINT, value VARCHAR)")
        .await
        .unwrap();
    for key in 0..200 {
        client
            .simple_query(&format!(
                "INSERT INTO side_input (key, value) VALUES ({key}, 'val_{key}')"
            ))
            .await
            .unwrap();
    }

    // Register views
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
        "CREATE VIEW q12 AS SELECT bidder, count(*) as bid_count, CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start FROM bid GROUP BY bidder, date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q13 AS SELECT b.auction, b.bidder, b.price, b.date_time, s.value FROM bid b JOIN side_input s ON b.auction = s.key",
        "CREATE VIEW q14 AS SELECT auction, bidder, price, CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END as price_tier, CAST(date_time AS VARCHAR) as date_time_str, length(extra) - length(replace(extra, 'a', '')) as char_count FROM bid",
        "CREATE VIEW q15 AS SELECT CAST(date_bin(INTERVAL '10 seconds', cast(date_time as timestamp)) AS BIGINT) as window_start, SUM(CASE WHEN price < 10000 THEN price ELSE 0 END) as low_sum, COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 100000 THEN bidder END) as medium_bidders, COUNT(DISTINCT CASE WHEN price >= 100000 THEN bidder END) as high_bidders FROM bid GROUP BY date_bin(INTERVAL '10 seconds', cast(date_time as timestamp))",
        "CREATE VIEW q16 AS SELECT channel, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(DISTINCT bidder) as distinct_bidders, COUNT(*) as bid_count FROM bid GROUP BY channel, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q17 AS SELECT auction, CAST(date_bin(INTERVAL '1 day', cast(date_time as timestamp)) AS BIGINT) as day, COUNT(*) as bid_count, MAX(price) as max_price, CAST(AVG(price) AS BIGINT) as avg_price FROM bid GROUP BY auction, date_bin(INTERVAL '1 day', cast(date_time as timestamp))",
        "CREATE VIEW q18 AS SELECT auction, bidder, price, date_time FROM (SELECT auction, bidder, price, date_time, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY date_time DESC) as rn FROM bid ) WHERE rn <= 1",
        "CREATE VIEW q19 AS SELECT auction, price FROM (SELECT auction, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) as rn FROM bid ) WHERE rn <= 10",
        "CREATE VIEW q20 AS SELECT b.auction, b.bidder, b.price, b.date_time, a.category FROM bid b JOIN auction a ON b.auction = a.id WHERE a.category = 10",
        "CREATE VIEW q21 AS SELECT auction, bidder, CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' THEN 'social_media' ELSE 'other' END as channel_id FROM bid",
        "CREATE VIEW q22 AS SELECT auction, bidder, split_part(url, '/', 4) as dir FROM bid",
    ];

    for ddl in &view_ddls {
        client.simple_query(ddl).await.unwrap();
    }

    // Ingest base events
    let mut gen = NexmarkGenerator::new(seed);
    let mut inserts = Vec::new();
    for _ in 0..num_base_events {
        let event = gen.next().unwrap();
        inserts.push(event.to_insert_sql());
    }

    client
        .simple_query("SET rockstream.idempotency_key = 'bench-nexmark-initial'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for sql in inserts {
        client.simple_query(&sql).await.unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();

    BenchEnv {
        client,
        _server_handle: server_handle,
        _shard_db: shard_db,
    }
}

fn generate_dml_statements(seed: u64, delta_size: usize, num_base_events: usize) -> Vec<String> {
    use rand::{Rng, SeedableRng};
    let mut gen = NexmarkGenerator::new(seed);
    let mut persons = Vec::new();
    let mut auctions = Vec::new();
    let mut bids = Vec::new();

    // Reconstruct the exact state after base events
    for _ in 0..num_base_events {
        let event = gen.next().unwrap();
        match event {
            NexmarkEvent::Person(p) => persons.push(p),
            NexmarkEvent::Auction(a) => auctions.push(a),
            NexmarkEvent::Bid(b) => bids.push(b),
        }
    }

    let mut rng = rand::rngs::StdRng::from_seed([0u8; 32]);
    let mut dml_statements = Vec::new();

    for _ in 0..delta_size {
        let op_type = rng.gen_range(0..10);
        if op_type < 4 {
            // Delete
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
            // Update
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
            // Insert
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

    dml_statements
}

async fn get_view_state(client: &tokio_postgres::Client, view: &str) -> HashMap<String, i64> {
    let res = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .unwrap();
    let mut map = HashMap::new();
    for msg in res {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            let mut fields = Vec::new();
            for col in 0..r.len() {
                fields.push(r.get(col).unwrap_or("NULL").to_string());
            }
            let key = fields.join("\t");
            *map.entry(key).or_insert(0) += 1;
        }
    }
    map
}

async fn measure_amplification_for_all_stateful_views(
    seed: u64,
    num_base_events: usize,
    dml_statements: &[String],
) -> HashMap<String, f64> {
    let env = setup_env(seed, num_base_events).await;

    let stateful_queries = [
        "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q12", "q13", "q15", "q16", "q17", "q18", "q19",
        "q20",
    ];
    let mut before_states = HashMap::new();
    for &q in &stateful_queries {
        before_states.insert(q.to_string(), get_view_state(&env.client, q).await);
    }

    env.client
        .simple_query("SET rockstream.idempotency_key = 'bench-nexmark-amp-measurement'")
        .await
        .unwrap();
    env.client.simple_query("BEGIN").await.unwrap();
    for sql in dml_statements {
        env.client.simple_query(sql).await.unwrap();
    }
    env.client.simple_query("COMMIT").await.unwrap();

    let mut amplifications = HashMap::new();
    let input_rows = dml_statements.len();

    for &q in &stateful_queries {
        let before_map = before_states.get(q).unwrap();
        let after_map = get_view_state(&env.client, q).await;

        let mut output_delta_rows = 0;
        for (row, &count_before) in before_map {
            let count_after = *after_map.get(row).unwrap_or(&0);
            output_delta_rows += (count_after - count_before).abs();
        }
        for (row, &count_after) in &after_map {
            if !before_map.contains_key(row) {
                output_delta_rows += count_after.abs();
            }
        }

        let amp = if input_rows > 0 {
            output_delta_rows as f64 / input_rows as f64
        } else {
            0.0
        };
        amplifications.insert(q.to_string(), amp);
    }

    amplifications
}

async fn measure_commit_latencies_ms(
    seed: u64,
    num_base_events: usize,
    dml_statements: &[String],
    samples: usize,
) -> Vec<f64> {
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let env = setup_env(seed, num_base_events).await;
        env.client
            .simple_query("SET rockstream.idempotency_key = 'bench-nexmark-summary'")
            .await
            .unwrap();
        let started = Instant::now();
        env.client.simple_query("BEGIN").await.unwrap();
        for sql in dml_statements {
            env.client.simple_query(sql).await.unwrap();
        }
        env.client.simple_query("COMMIT").await.unwrap();
        out.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    out
}

// This gate uses the explicit amplification and latency measurements below.
// Repeating the full state setup through Criterion's sampling loop is too
// expensive for CI and does not affect the regression summary.
fn bench_nexmark(_c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let rates = &[
        1,   // 1% change rate on DATASET_SIZE = 100
        10,  // 10% change rate
        100, // full change rate
    ];
    let mut max_delta_amplification = 0.0f64;
    let mut latency_samples_ms = Vec::new();

    for &delta_size in rates {
        let dml_statements = generate_dml_statements(42, delta_size, DATASET_SIZE);

        // 1. Measure and assert delta amplification once
        let amps = rt.block_on(async {
            measure_amplification_for_all_stateful_views(42, DATASET_SIZE, &dml_statements).await
        });
        for (view, amp) in &amps {
            println!("[nexmark_bench] View {view} delta amplification: {amp:.2}x");
            max_delta_amplification = max_delta_amplification.max(*amp);
            let limit = if view == "q6" { 15.0 } else { 10.0 };
            assert!(
                *amp <= limit,
                "Delta amplification factor for view {view} ({amp:.2}x) exceeds the maximum allowed {limit}x"
            );
        }
        latency_samples_ms.extend(rt.block_on(async {
            measure_commit_latencies_ms(42, DATASET_SIZE, &dml_statements, 5).await
        }));
    }

    let mut p50_samples = latency_samples_ms.clone();
    let mut p99_samples = latency_samples_ms;
    let summary = NexmarkBenchmarkSummary {
        max_delta_amplification,
        propagation_latency_p50_ms: percentile(&mut p50_samples, 0.50),
        propagation_latency_p99_ms: percentile(&mut p99_samples, 0.99),
    };
    println!(
        "[nexmark_summary] {}",
        serde_json::to_string(&summary).unwrap()
    );
}

criterion_group!(benches, bench_nexmark);
criterion_main!(benches);
