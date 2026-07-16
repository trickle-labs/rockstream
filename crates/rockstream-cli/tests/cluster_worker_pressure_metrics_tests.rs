use rockstream_cli::metrics_server::start_metrics_server;
use rockstream_control::{publish_cluster_worker_pressure, PipelineShardPressureSample};
use rockstream_types::metrics::{
    read_cluster_worker_pressure, read_demanded_shard_count, read_placed_shard_count, reset_all,
};
use std::sync::LazyLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn request(addr: std::net::SocketAddr, req: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.unwrap();
    String::from_utf8_lossy(&resp).to_string()
}

#[tokio::test]
async fn gauge_values_track_scripted_demanded_and_placed_sequence_exactly() {
    let _guard = TEST_LOCK.lock().await;
    reset_all();

    let scripted = [
        (
            vec![
                PipelineShardPressureSample {
                    pipeline_id: "alpha".to_string(),
                    demanded_shard_count: 6,
                    placed_shard_count: 6,
                },
                PipelineShardPressureSample {
                    pipeline_id: "beta".to_string(),
                    demanded_shard_count: 4,
                    placed_shard_count: 4,
                },
            ],
            1.0,
            6,
            6,
        ),
        (
            vec![
                PipelineShardPressureSample {
                    pipeline_id: "alpha".to_string(),
                    demanded_shard_count: 10,
                    placed_shard_count: 5,
                },
                PipelineShardPressureSample {
                    pipeline_id: "beta".to_string(),
                    demanded_shard_count: 4,
                    placed_shard_count: 4,
                },
            ],
            2.0,
            10,
            5,
        ),
        (
            vec![
                PipelineShardPressureSample {
                    pipeline_id: "alpha".to_string(),
                    demanded_shard_count: 8,
                    placed_shard_count: 8,
                },
                PipelineShardPressureSample {
                    pipeline_id: "beta".to_string(),
                    demanded_shard_count: 3,
                    placed_shard_count: 4,
                },
            ],
            1.0,
            8,
            8,
        ),
    ];

    for (index, (samples, expected_pressure, expected_demanded, expected_placed)) in
        scripted.into_iter().enumerate()
    {
        let snapshot = publish_cluster_worker_pressure(&samples, index as u64);
        assert!(
            (snapshot.pressure - expected_pressure).abs() < 1e-9,
            "unexpected pressure at step {index}: {:?}",
            snapshot
        );
        assert_eq!(snapshot.demanded_shard_count, expected_demanded);
        assert_eq!(snapshot.placed_shard_count, expected_placed);
        assert!((read_cluster_worker_pressure() - expected_pressure).abs() < 1e-9);
        assert_eq!(read_demanded_shard_count(), expected_demanded as u64);
        assert_eq!(read_placed_shard_count(), expected_placed as u64);
    }
}

#[tokio::test]
async fn metrics_endpoint_exposes_cluster_worker_pressure_gauges() {
    let _guard = TEST_LOCK.lock().await;
    reset_all();
    publish_cluster_worker_pressure(
        &[PipelineShardPressureSample {
            pipeline_id: "alpha".to_string(),
            demanded_shard_count: 10,
            placed_shard_count: 5,
        }],
        42,
    );

    let handle = start_metrics_server("127.0.0.1:0").await.unwrap();
    let resp = request(handle.local_addr, "GET /metrics HTTP/1.1\r\n\r\n").await;

    assert!(resp.starts_with("HTTP/1.1 200 OK"));
    assert!(resp.contains("# TYPE cluster_worker_pressure gauge"));
    assert!(resp.contains("# TYPE demanded_shard_count gauge"));
    assert!(resp.contains("# TYPE placed_shard_count gauge"));
    assert!(resp.contains("cluster_worker_pressure 2.000000"));
    assert!(resp.contains("demanded_shard_count 10"));
    assert!(resp.contains("placed_shard_count 5"));

    handle.shutdown();
}
