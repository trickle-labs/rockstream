//! v0.61.2 Slice 6: Public release binary aggregate crash recovery tests (V0612-06).
//!
//! Spawns a real `rockstream` process, creates an aggregate materialized view, executes
//! repeated-key transactions, abruptly kills the process (`SIGKILL`), restarts against the
//! same storage directory, and asserts exact recovered multiset with continued accumulation.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio_postgres::NoTls;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct ProcessGuard(Option<Child>);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    let addr = format!("127.0.0.1:{port}");
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rockstream dbname=rockstream"),
        NoTls,
    )
    .await
    .expect("connect to rockstream gateway");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

#[tokio::test]
async fn test_v0612_public_release_aggregate_crash_recovery() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let storage_dir = TempDir::new().expect("storage dir");
    let port1 = free_port();

    let child1 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port1}"),
        ])
        .spawn()
        .expect("spawn rockstream gateway");
    let mut guard1 = ProcessGuard(Some(child1));

    assert!(
        wait_for_port(port1, Duration::from_secs(10)),
        "gateway port1 failed to bind within deadline"
    );

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE items (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE");
    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW item_totals AS SELECT category, SUM(price), COUNT(price) FROM items GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW");

    // Epoch 1
    client1
        .simple_query(
            "INSERT INTO items (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50)",
        )
        .await
        .expect("INSERT epoch 1");
    client1
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 1");

    // Epoch 2: Repeated updates to category 10, additional update to category 20
    client1
        .simple_query(
            "INSERT INTO items (id, category, price) VALUES (4, 10, -50), (5, 10, 150), (6, 20, 25)",
        )
        .await
        .expect("INSERT epoch 2");
    client1
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 2");

    let pre_crash_rows = client1
        .simple_query("SELECT * FROM item_totals")
        .await
        .expect("SELECT item_totals pre-crash");
    let mut pre_crash = HashMap::new();
    for msg in pre_crash_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            let cnt: i64 = row.get(2).unwrap().parse().unwrap();
            pre_crash.insert(cat, (sum, cnt));
        }
    }
    assert_eq!(
        pre_crash,
        HashMap::from([(10, (400, 4)), (20, (75, 2))]),
        "pre-crash view state mismatch"
    );

    // Give storage a moment to flush to disk
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Abrupt crash: SIGKILL the gateway process
    if let Some(mut c) = guard1.0.take() {
        let _ = c.kill();
        let _ = c.wait();
    }

    // Restart process against the same storage directory
    let port2 = free_port();
    let child2 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port2}"),
        ])
        .spawn()
        .expect("spawn restart rockstream gateway");
    let _guard2 = ProcessGuard(Some(child2));

    assert!(
        wait_for_port(port2, Duration::from_secs(10)),
        "gateway port2 failed to bind after restart"
    );

    let client2 = connect_port(port2).await;
    // Note: Re-register view definition in catalog stubs to query recovered arrangement
    client2
        .simple_query("CREATE TABLE items (id BIGINT, category BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE on restart");
    client2
        .simple_query(
            "CREATE MATERIALIZED VIEW item_totals AS SELECT category, SUM(price), COUNT(price) FROM items GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW on restart");

    let post_crash_rows = client2
        .simple_query("SELECT * FROM item_totals")
        .await
        .expect("SELECT item_totals post-crash");
    let mut post_crash = HashMap::new();
    for msg in post_crash_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            let cnt: i64 = row.get(2).unwrap().parse().unwrap();
            post_crash.insert(cat, (sum, cnt));
        }
    }
    assert_eq!(
        post_crash,
        HashMap::from([(10, (400, 4)), (20, (75, 2))]),
        "recovered view state must be bit-identical to pre-crash state"
    );

    // Post-restart commit accumulates on top of restored arrangement
    client2
        .simple_query("INSERT INTO items (id, category, price) VALUES (7, 10, 100)")
        .await
        .expect("INSERT epoch 3");
    client2
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 3");

    let after_rows = client2
        .simple_query("SELECT * FROM item_totals")
        .await
        .expect("SELECT item_totals post-restart");
    let mut after = HashMap::new();
    for msg in after_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            let cnt: i64 = row.get(2).unwrap().parse().unwrap();
            after.insert(cat, (sum, cnt));
        }
    }
    assert_eq!(
        after,
        HashMap::from([(10, (500, 5)), (20, (75, 2))]),
        "post-restart updates must accumulate correctly on top of restored state"
    );
}
