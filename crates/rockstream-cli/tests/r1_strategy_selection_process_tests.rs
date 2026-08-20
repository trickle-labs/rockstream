use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls};

struct Gateway(Child);

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

fn effective_config(binary: &str, config: &std::path::Path) -> serde_json::Value {
    let output = Command::new(binary)
        .args(["--json", "config", "print-effective", "--file"])
        .arg(config)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    serde_json::from_slice(&output.stdout).unwrap()
}

fn start(
    binary: &str,
    config: &std::path::Path,
    storage: &std::path::Path,
    listen: &str,
    metrics: &str,
) -> Gateway {
    Gateway(
        Command::new(binary)
            .args(["start", "--storage"])
            .arg(storage)
            .args([
                "--role",
                "gateway",
                "--listen",
                listen,
                "--metrics-addr",
                metrics,
            ])
            .env("ROCKSTREAM_CONFIG", config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

async fn connect(addr: &str) -> Client {
    let addr: std::net::SocketAddr = addr.parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok((client, connection)) = tokio_postgres::connect(
            &format!("host={} port={} user=rockstream", addr.ip(), addr.port()),
            NoTls,
        )
        .await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("gateway did not accept connections at {addr}");
}

async fn metrics(addr: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await {
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            if let Some((_, body)) = response.split_once("\r\n\r\n") {
                return body.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("metrics endpoint did not respond at {addr}");
}

async fn run_mode(strategy: &str, selected: &str) -> Vec<Vec<String>> {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("rockstream.toml");
    std::fs::write(
        &config,
        format!("[execution]\njoin_strategy = \"{strategy}\"\n"),
    )
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_rockstream");
    assert_eq!(
        effective_config(binary, &config)["config"]["execution"]["join_strategy"],
        strategy
    );

    let listen = free_addr();
    let metrics_addr = free_addr();
    let _gateway = start(
        binary,
        &config,
        &root.path().join("storage"),
        &listen,
        &metrics_addr,
    );
    let client = connect(&listen).await;
    client
        .batch_execute(
            "CREATE TABLE a (id BIGINT PRIMARY KEY, k BIGINT NOT NULL); \
             CREATE TABLE b (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, val BIGINT NOT NULL); \
             CREATE MATERIALIZED VIEW totals AS SELECT a.k, SUM(b.val) AS total FROM a JOIN b ON a.k = b.k GROUP BY a.k; \
             INSERT INTO a VALUES (1,10),(2,20); \
             INSERT INTO b VALUES (3,10,7),(4,20,4);",
        )
        .await
        .unwrap();
    let rows = client
        .query("SELECT k, total FROM totals ORDER BY k", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (0..row.len()).map(|index| row.get(index)).collect())
        .collect::<Vec<Vec<String>>>();
    assert_eq!(
        rows,
        vec![
            vec!["10".to_string(), "7".to_string()],
            vec!["20".to_string(), "4".to_string()],
        ]
    );
    let body = metrics(&metrics_addr).await;
    assert!(
        body.contains(&format!(
            "rockstream_compiled_join_views{{strategy=\"{selected}\"}} 1"
        )),
        "{strategy}: {body}"
    );
    rows
}

#[tokio::test]
async fn strategy_selection_is_fixed_at_compilation_and_preserves_complete_output() {
    let auto = run_mode("auto", "factorized").await;
    let classic = run_mode("classic", "classic").await;
    let factorized = run_mode("factorized", "factorized").await;
    assert_eq!(auto, classic);
    assert_eq!(classic, factorized);
}

#[tokio::test]
async fn factorized_strategy_rejects_ineligible_sql_without_fallback() {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("rockstream.toml");
    std::fs::write(&config, "[execution]\njoin_strategy = \"factorized\"\n").unwrap();
    let listen = free_addr();
    let metrics_addr = free_addr();
    let _gateway = start(
        env!("CARGO_BIN_EXE_rockstream"),
        &config,
        &root.path().join("storage"),
        &listen,
        &metrics_addr,
    );
    let client = connect(&listen).await;
    client
        .batch_execute(
            "CREATE TABLE a (id BIGINT PRIMARY KEY, k BIGINT NOT NULL); \
             CREATE TABLE b (id BIGINT PRIMARY KEY, k BIGINT NOT NULL);",
        )
        .await
        .unwrap();
    let error = client
        .batch_execute(
            "CREATE MATERIALIZED VIEW ineligible AS SELECT a.id FROM a JOIN b ON a.k = b.k",
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap().message();
    assert!(error.contains("RS-1013"), "{error}");
    assert!(
        error.contains("execution.join_strategy=factorized"),
        "{error}"
    );
}
