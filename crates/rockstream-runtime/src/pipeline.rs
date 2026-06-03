//! Pipeline runner for RockStream.
//!
//! Connects a source → operator → sink and runs epochs until the source
//! is exhausted or a shutdown signal is received.

use rockstream_connectors::sink::Sink;
use rockstream_connectors::source::Source;
use rockstream_control::audit::FileAuditLog;
use rockstream_ops::operator::Operator;
use rockstream_types::audit::AuditEvent;
use rockstream_types::timestamp::Epoch;
use std::path::Path;

/// Result of running a pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineResult {
    /// Number of epochs completed.
    pub epochs_completed: Epoch,
    /// Pipeline name.
    pub pipeline_name: String,
}

/// Configuration for a pipeline.
pub struct PipelineConfig {
    /// Pipeline name.
    pub name: String,
    /// Storage directory.
    pub storage_dir: String,
    /// Target maximum staleness in milliseconds.
    pub freshness_slo_ms: Option<u64>,
}

/// Run a pipeline: source → operator → sink, epoch by epoch.
///
/// Returns the result after the source is exhausted.
pub async fn run_pipeline(
    config: &PipelineConfig,
    source: &mut dyn Source,
    operator: &mut dyn Operator,
    sink: &mut dyn Sink,
    audit_log: &FileAuditLog,
) -> PipelineResult {
    tracing::info!(
        pipeline = %config.name,
        source = source.name(),
        operator = operator.name(),
        sink = sink.name(),
        "pipeline starting"
    );

    // Audit: pipeline created
    let event = AuditEvent::now("system", "pipeline.created", &config.name).with_detail(format!(
        "source={}, operator={}, sink={}",
        source.name(),
        operator.name(),
        sink.name()
    ));
    if let Err(e) = audit_log.append(&event) {
        tracing::warn!(error = %e, "failed to write audit event");
    }

    // Audit: pipeline started
    let event = AuditEvent::now("system", "pipeline.started", &config.name);
    if let Err(e) = audit_log.append(&event) {
        tracing::warn!(error = %e, "failed to write audit event");
    }

    let mut epoch: Epoch = 0;
    let mut bytes_buffered: u64 = 0;
    let mut epochs_buffered: u32 = 0;
    let mut last_commit_time = tokio::time::Instant::now();

    while let Some(batch) = source.poll_batch(epoch).await {
        // Process through operator
        let output = operator.process(&batch).await;

        // Estimate bytes (assuming 256 bytes per record on average)
        bytes_buffered += (output.record_count * 256) as u64;
        epochs_buffered += 1;

        // Write/prepare to sink
        sink.prepare(&output).await;

        let elapsed = last_commit_time.elapsed().as_millis() as u64;
        let should_slo_flush = config.freshness_slo_ms.is_some_and(|slo| elapsed >= slo);

        // Respect should_flush override or hard ceiling (500 epochs or 256 MB) or SLO
        if sink.should_flush(bytes_buffered, epochs_buffered)
            || epochs_buffered >= 500
            || bytes_buffered >= 256 * 1024 * 1024
            || should_slo_flush
        {
            sink.commit(epoch).await;
            bytes_buffered = 0;
            epochs_buffered = 0;
            last_commit_time = tokio::time::Instant::now();
        }

        // Signal epoch complete
        operator.epoch_complete(epoch).await;

        tracing::debug!(epoch, "epoch completed");
        epoch += 1;
    }

    // Flush any remaining buffered data at the end of the pipeline run
    if epochs_buffered > 0 && epoch > 0 {
        sink.commit(epoch - 1).await;
    }

    // Audit: pipeline stopped
    let event = AuditEvent::now("system", "pipeline.stopped", &config.name)
        .with_detail(format!("epochs_completed={epoch}"));
    if let Err(e) = audit_log.append(&event) {
        tracing::warn!(error = %e, "failed to write audit event");
    }

    tracing::info!(pipeline = %config.name, epochs = epoch, "pipeline completed");

    PipelineResult {
        epochs_completed: epoch,
        pipeline_name: config.name.clone(),
    }
}

/// Run the default no-op pipeline in the given storage directory.
pub async fn run_noop_pipeline(storage_dir: &Path) -> PipelineResult {
    use rockstream_connectors::noop_sink::NoopSink;
    use rockstream_connectors::noop_source::NoopSource;
    use rockstream_ops::noop::NoopOperator;

    let audit_path = storage_dir.join("audit.jsonl");
    let audit_log = FileAuditLog::open(&audit_path).expect("failed to open audit log");

    let config = PipelineConfig {
        name: "noop-pipeline".to_string(),
        storage_dir: storage_dir.display().to_string(),
        freshness_slo_ms: None,
    };

    let mut source = NoopSource::new(5);
    let mut operator = NoopOperator::new();
    let mut sink = NoopSink::new();

    run_pipeline(&config, &mut source, &mut operator, &mut sink, &audit_log).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_noop_pipeline_completes() {
        let dir = tempfile::tempdir().unwrap();
        let result = run_noop_pipeline(dir.path()).await;
        assert_eq!(result.epochs_completed, 5);
        assert_eq!(result.pipeline_name, "noop-pipeline");
    }

    #[tokio::test]
    async fn run_noop_pipeline_writes_audit_events() {
        let dir = tempfile::tempdir().unwrap();
        run_noop_pipeline(dir.path()).await;

        let audit_path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&audit_path).unwrap();
        let events = log.read_all().unwrap();

        // Should have: created, started, stopped
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].action, "pipeline.created");
        assert_eq!(events[1].action, "pipeline.started");
        assert_eq!(events[2].action, "pipeline.stopped");

        // All reference the pipeline name
        for event in &events {
            assert_eq!(event.resource, "noop-pipeline");
        }
    }

    #[tokio::test]
    async fn pipeline_audit_events_have_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        run_noop_pipeline(dir.path()).await;

        let audit_path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&audit_path).unwrap();
        let events = log.read_all().unwrap();

        for event in &events {
            assert!(event.timestamp_ms > 0);
            assert_eq!(event.actor, "system");
        }
    }

    #[tokio::test]
    async fn pipeline_with_custom_source() {
        use rockstream_connectors::noop_sink::NoopSink;
        use rockstream_connectors::noop_source::NoopSource;
        use rockstream_ops::noop::NoopOperator;

        let dir = tempfile::tempdir().unwrap();
        let audit_path = dir.path().join("audit.jsonl");
        let audit_log = FileAuditLog::open(&audit_path).unwrap();

        let config = PipelineConfig {
            name: "custom-pipeline".to_string(),
            storage_dir: dir.path().display().to_string(),
            freshness_slo_ms: None,
        };

        let mut source = NoopSource::new(10);
        let mut operator = NoopOperator::new();
        let mut sink = NoopSink::new();

        let result = run_pipeline(&config, &mut source, &mut operator, &mut sink, &audit_log).await;
        assert_eq!(result.epochs_completed, 10);
        assert_eq!(result.pipeline_name, "custom-pipeline");
    }
}
