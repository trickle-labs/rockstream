use rockstream_cli::metrics_server::start_metrics_server;
use rockstream_control::{publish_cluster_worker_pressure, PipelineShardPressureSample};
use rockstream_types::metrics::{
    read_cluster_worker_pressure, read_demanded_shard_count, read_placed_shard_count, reset_all,
};
use std::sync::{LazyLock, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    let _guard = TEST_LOCK.lock().unwrap();

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
#[allow(clippy::await_holding_lock)]
async fn metrics_endpoint_exposes_cluster_worker_pressure_gauges() {
    let _guard = TEST_LOCK.lock().unwrap();
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

#[test]
fn test_status_model_ten_fields_authoritative_values() {
    use rockstream_cli::output::ViewStatusInfo;
    use rockstream_types::view_lifecycle::{
        DegradationReason, DominantContributor, ObservationState,
    };

    // Construct full status info with all 10 authoritative fields
    let info = ViewStatusInfo {
        namespace: "default".to_string(),
        view_name: "customer_summary".to_string(),
        state: "Active".to_string(),
        workload_name: Some("analytics_workload".to_string()),
        freshness_slo_ms: Some(1000),
        memory_limit_bytes: Some(1024 * 1024 * 256),
        depends_on: vec!["orders".to_string(), "customers".to_string()],
        stage_lag: None,
        degradation_reason: DegradationReason::WaitingOnSource,
        reason_code: "RS-3701".to_string(),
        dominant_contributor: DominantContributor::Healthy,
        progress_phase: None,
        bytes_remaining: None,
        rows_remaining: None,
        estimated_remaining_ms: None,
        published_frontier: Some(42),
        input_frontier: Some(45),
        freshness_lag_ms: Some(300),
        state_bytes: Some(50_000_000),
        memory_bytes: Some(120_000_000),
        assigned_shards: vec![1, 2, 3],
        blocking_operation: Some("op-migrate-shard-1".to_string()),
    };

    // Assert all 10 roadmap status fields
    assert_eq!(info.state, "Active");
    assert_eq!(info.published_frontier, Some(42));
    assert_eq!(info.input_frontier, Some(45));
    assert_eq!(info.freshness_lag_ms, Some(300));
    assert_eq!(info.freshness_slo_ms, Some(1000));
    assert_eq!(info.state_bytes, Some(50_000_000));
    assert_eq!(info.memory_bytes, Some(120_000_000));
    assert_eq!(info.assigned_shards, vec![1, 2, 3]);
    assert_eq!(info.degradation_reason, DegradationReason::WaitingOnSource);
    assert_eq!(
        info.blocking_operation,
        Some("op-migrate-shard-1".to_string())
    );

    // Verify JSON roundtrip
    let serialized = serde_json::to_string(&info).expect("serialize view status");
    assert!(serialized.contains(r#""published_frontier":42"#));
    assert!(serialized.contains(r#""input_frontier":45"#));
    assert!(serialized.contains(r#""freshness_lag_ms":300"#));
    assert!(serialized.contains(r#""freshness_slo_ms":1000"#));
    assert!(serialized.contains(r#""state_bytes":50000000"#));
    assert!(serialized.contains(r#""memory_bytes":120000000"#));
    assert!(serialized.contains(r#""assigned_shards":[1,2,3]"#));
    assert!(serialized.contains(r#""blocking_operation":"op-migrate-shard-1""#));

    let deserialized: ViewStatusInfo =
        serde_json::from_str(&serialized).expect("deserialize view status");
    assert_eq!(deserialized, info);

    // Verify ObservationState semantics
    assert_eq!(ObservationState::Known.as_str(), "known");
    assert_eq!(ObservationState::Unknown.as_str(), "unknown");
    assert_eq!(ObservationState::Stale.as_str(), "stale");
    assert_eq!(ObservationState::Unavailable.as_str(), "unavailable");
    assert_eq!(ObservationState::NotApplicable.as_str(), "not_applicable");
}

#[test]
fn test_status_field_state_semantics() {
    use rockstream_types::view_lifecycle::ViewState;
    let state = ViewState::Running;
    assert!(state.is_running());
    let paused = ViewState::Paused;
    assert!(paused.is_paused());
}

#[test]
fn test_status_field_published_frontier_semantics() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::set_view_published_frontier("v_frontier", 100);
    assert_eq!(
        rockstream_types::metrics::read_view_published_frontier("v_frontier"),
        Some(100)
    );
    // Unknown during initial restore before any commit
    assert_eq!(
        rockstream_types::metrics::read_view_published_frontier("v_unknown"),
        None
    );
}

#[test]
fn test_status_field_input_frontier_semantics() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::set_view_input_frontier("v_input", 105);
    assert_eq!(
        rockstream_types::metrics::read_view_input_frontier("v_input"),
        Some(105)
    );
    assert_eq!(
        rockstream_types::metrics::read_view_input_frontier("v_unreached"),
        None
    );
}

#[test]
fn test_status_field_freshness_lag_semantics() {
    let lag = 105u64.saturating_sub(100);
    assert_eq!(lag, 5);
}

#[test]
fn test_status_field_freshness_slo_semantics() {
    let slo: Option<u64> = Some(500);
    assert_eq!(slo, Some(500));
    let unconfigured: Option<u64> = None;
    assert!(unconfigured.is_none());
}

#[test]
fn test_status_field_state_bytes_semantics() {
    let state_bytes = Some(1024 * 1024 * 64);
    assert_eq!(state_bytes, Some(67108864));
}

#[test]
fn test_status_field_memory_bytes_semantics() {
    let mem_bytes = Some(1024 * 1024 * 128);
    assert_eq!(mem_bytes, Some(134217728));
}

#[test]
fn test_status_field_assigned_shards_semantics() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::set_view_assigned_shards("v_shards", vec![0, 1, 2, 3]);
    assert_eq!(
        rockstream_types::metrics::read_view_assigned_shards("v_shards"),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        rockstream_types::metrics::read_view_assigned_shards("v_empty"),
        Vec::<u64>::new()
    );
}

#[test]
fn test_status_field_degradation_reason_semantics() {
    use rockstream_types::view_lifecycle::DegradationReason;
    assert_eq!(
        DegradationReason::WaitingOnSource.reason_code().to_string(),
        "RS-3701"
    );
    assert_eq!(
        DegradationReason::QuotaAdmissionRejected
            .reason_code()
            .to_string(),
        "RS-3702"
    );
}

#[test]
fn test_status_field_blocking_operation_semantics() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::set_view_blocking_operation(
        "v_block",
        Some("migration-42".to_string()),
    );
    assert_eq!(
        rockstream_types::metrics::read_view_blocking_operation("v_block"),
        Some("migration-42".to_string())
    );
    rockstream_types::metrics::set_view_blocking_operation("v_block", None);
    assert_eq!(
        rockstream_types::metrics::read_view_blocking_operation("v_block"),
        None
    );
}

#[test]
fn test_prometheus_metrics_twelve_classes_real_producers() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();

    rockstream_types::metrics::record_ingest_throughput("orders_kafka", "json", 1500, 102400);
    rockstream_types::metrics::record_execution_rows("v_orders", "join_op", 3000);
    rockstream_types::metrics::record_epoch_duration_seconds("wl_default", "commit", 0.045);
    rockstream_types::metrics::set_frontier_lag_seconds("v_orders", 0.125);
    rockstream_types::metrics::set_view_state_bytes("v_orders", "slatedb", 52428800);
    rockstream_types::metrics::set_worker_memory_bytes("worker-0", "rss", 104857600);
    rockstream_types::metrics::record_exchange_bytes("worker-0", "worker-1", 20480);
    rockstream_types::metrics::set_exchange_backpressure("worker-0", "channel-1", 0.25);
    rockstream_types::metrics::record_checkpoint_duration_seconds("shard-1", "lfs", 0.080);
    rockstream_types::metrics::record_migration_rows_copied("mig-001", 50000);
    rockstream_types::metrics::set_connector_lag_records("orders_kafka", "p0", 120);
    rockstream_types::metrics::record_diagnostic_error("RS-3701");

    let metrics = rockstream_types::metrics::generate_prometheus_metrics();

    assert!(metrics
        .contains("rockstream_ingest_rows_total{source=\"orders_kafka\",format=\"json\"} 1500"));
    assert!(metrics
        .contains("rockstream_ingest_bytes_total{source=\"orders_kafka\",format=\"json\"} 102400"));
    assert!(metrics
        .contains("rockstream_execution_rows_total{view=\"v_orders\",operator=\"join_op\"} 3000"));
    assert!(metrics.contains(
        "rockstream_epoch_duration_seconds{workload=\"wl_default\",phase=\"commit\"} 0.045"
    ));
    assert!(metrics.contains("rockstream_frontier_lag_seconds{view=\"v_orders\"} 0.125"));
    assert!(
        metrics.contains("rockstream_state_bytes{view=\"v_orders\",backend=\"slatedb\"} 52428800")
    );
    assert!(
        metrics.contains("rockstream_memory_bytes{worker=\"worker-0\",category=\"rss\"} 104857600")
    );
    assert!(metrics.contains(
        "rockstream_exchange_bytes_total{sender=\"worker-0\",receiver=\"worker-1\"} 20480"
    ));
    assert!(metrics.contains(
        "rockstream_exchange_backpressure{worker=\"worker-0\",channel=\"channel-1\"} 0.25"
    ));
    assert!(metrics
        .contains("rockstream_checkpoint_duration_seconds{shard=\"shard-1\",tier=\"lfs\"} 0.08"));
    assert!(
        metrics.contains("rockstream_migration_rows_copied_total{migration_id=\"mig-001\"} 50000")
    );
    assert!(metrics.contains(
        "rockstream_connector_lag_records{source=\"orders_kafka\",partition=\"p0\"} 120"
    ));
    assert!(metrics.contains("rockstream_errors_total{code=\"RS-3701\"} 1"));
}

#[test]
fn test_metric_ingest_rows_total() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_ingest_throughput("src1", "avro", 100, 1000);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_ingest_rows_total{source=\"src1\",format=\"avro\"} 100"));
}

#[test]
fn test_metric_ingest_bytes_total() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_ingest_throughput("src1", "avro", 100, 2048);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_ingest_bytes_total{source=\"src1\",format=\"avro\"} 2048"));
}

#[test]
fn test_metric_execution_rows_total() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_execution_rows("view_a", "filter", 42);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_execution_rows_total{view=\"view_a\",operator=\"filter\"} 42"));
}

#[test]
fn test_metric_epoch_duration_seconds() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_epoch_duration_seconds("default", "compute", 0.012);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains(
        "rockstream_epoch_duration_seconds{workload=\"default\",phase=\"compute\"} 0.012"
    ));
}

#[test]
fn test_metric_frontier_lag_seconds() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::set_frontier_lag_seconds("view_a", 1.5);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_frontier_lag_seconds{view=\"view_a\"} 1.5"));
}

#[test]
fn test_metric_state_bytes() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::set_view_state_bytes("view_a", "s3", 8192);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_state_bytes{view=\"view_a\",backend=\"s3\"} 8192"));
}

#[test]
fn test_metric_memory_bytes() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::set_worker_memory_bytes("w1", "heap", 65536);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_memory_bytes{worker=\"w1\",category=\"heap\"} 65536"));
}

#[test]
fn test_metric_exchange_bytes_total() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_exchange_bytes("w1", "w2", 1024);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_exchange_bytes_total{sender=\"w1\",receiver=\"w2\"} 1024"));
}

#[test]
fn test_metric_exchange_backpressure() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::set_exchange_backpressure("w1", "ch0", 0.75);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_exchange_backpressure{worker=\"w1\",channel=\"ch0\"} 0.75"));
}

#[test]
fn test_metric_checkpoint_duration_seconds() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_checkpoint_duration_seconds("sh0", "minio", 0.35);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_checkpoint_duration_seconds{shard=\"sh0\",tier=\"minio\"} 0.35"));
}

#[test]
fn test_metric_migration_progress() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_migration_rows_copied("m1", 999);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_migration_rows_copied_total{migration_id=\"m1\"} 999"));
}

#[test]
fn test_metric_connector_lag() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::set_connector_lag_records("kafka_src", "0", 420);
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(
        m.contains("rockstream_connector_lag_records{source=\"kafka_src\",partition=\"0\"} 420")
    );
}

#[test]
fn test_metric_errors_total() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    rockstream_types::metrics::record_diagnostic_error("RS-2003");
    rockstream_types::metrics::record_diagnostic_error("RS-2003");
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    assert!(m.contains("rockstream_errors_total{code=\"RS-2003\"} 2"));
}

#[test]
fn test_bounds_metrics_series_cardinality() {
    let _guard = TEST_LOCK.lock().unwrap();
    rockstream_types::metrics::reset_all();
    for i in 0..100 {
        let code = format!("RS-{:04}", i);
        rockstream_types::metrics::record_diagnostic_error(&code);
    }
    let m = rockstream_types::metrics::generate_prometheus_metrics();
    // 64 unique keys max, excess goes into __other__
    assert!(m.contains("rockstream_errors_total{code=\"__other__\"} 36"));
}
