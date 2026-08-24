//! Prometheus Metric surface contributor (DOC-001).

use crate::manifest::{MetricDescriptor, MetricSurface};

pub struct MetricContributor;

impl MetricContributor {
    /// Extract registered Prometheus metrics.
    pub fn extract() -> MetricSurface {
        let mut metrics = vec![
            MetricDescriptor {
                name: "checkpoint_committed_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["tier".to_string()],
                stability: "stable".to_string(),
                description: "Total number of durable checkpoints successfully committed"
                    .to_string(),
            },
            MetricDescriptor {
                name: "dlq_messages_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["source".to_string()],
                stability: "stable".to_string(),
                description: "Total poison records routed to the dead-letter queue".to_string(),
            },
            MetricDescriptor {
                name: "dlq_replay_failed_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["source".to_string()],
                stability: "stable".to_string(),
                description: "Total dead-letter queue replay execution failures".to_string(),
            },
            MetricDescriptor {
                name: "dlq_replay_success_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["source".to_string()],
                stability: "stable".to_string(),
                description: "Total dead-letter queue messages successfully reprocessed"
                    .to_string(),
            },
            MetricDescriptor {
                name: "l0_backlog_count".to_string(),
                metric_type: "gauge".to_string(),
                unit: "count".to_string(),
                labels: vec!["shard".to_string()],
                stability: "stable".to_string(),
                description: "Current L0 SST file backlog for SlateDB storage shards".to_string(),
            },
            MetricDescriptor {
                name: "manifest_write_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["epoch".to_string()],
                stability: "stable".to_string(),
                description: "Total epoch manifest writes committed to durable storage".to_string(),
            },
            MetricDescriptor {
                name: "merge_law_applied_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["law".to_string(), "operator".to_string()],
                stability: "stable".to_string(),
                description:
                    "Total number of successful merge-law applications on state arrangements"
                        .to_string(),
            },
            MetricDescriptor {
                name: "merge_law_fallback_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["law".to_string(), "operator".to_string()],
                stability: "stable".to_string(),
                description: "Total number of merge-law fallbacks to default accumulator"
                    .to_string(),
            },
            MetricDescriptor {
                name: "merge_law_rmw_avoided_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["law".to_string()],
                stability: "stable".to_string(),
                description: "Total read-modify-write state accesses avoided through blind merge"
                    .to_string(),
            },
            MetricDescriptor {
                name: "merge_law_rmw_required_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["law".to_string()],
                stability: "stable".to_string(),
                description: "Total read-modify-write state accesses required".to_string(),
            },
            MetricDescriptor {
                name: "operator_dirty_keys".to_string(),
                metric_type: "gauge".to_string(),
                unit: "count".to_string(),
                labels: vec!["operator_id".to_string()],
                stability: "stable".to_string(),
                description: "Current number of dirty keys pending in-memory arrangement commit"
                    .to_string(),
            },
            MetricDescriptor {
                name: "operator_records_in_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["operator_id".to_string(), "view_name".to_string()],
                stability: "stable".to_string(),
                description: "Total input delta records received by operator".to_string(),
            },
            MetricDescriptor {
                name: "operator_records_out_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["operator_id".to_string(), "view_name".to_string()],
                stability: "stable".to_string(),
                description: "Total output delta records emitted by operator".to_string(),
            },
            MetricDescriptor {
                name: "pending_compaction_bytes".to_string(),
                metric_type: "gauge".to_string(),
                unit: "bytes".to_string(),
                labels: vec!["shard".to_string()],
                stability: "stable".to_string(),
                description: "Total uncompacted bytes pending in storage tier".to_string(),
            },
            MetricDescriptor {
                name: "pgwire_connections_active".to_string(),
                metric_type: "gauge".to_string(),
                unit: "count".to_string(),
                labels: vec![],
                stability: "stable".to_string(),
                description: "Number of currently active pgwire client connections".to_string(),
            },
            MetricDescriptor {
                name: "pgwire_errors_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["sqlstate".to_string(), "error_code".to_string()],
                stability: "stable".to_string(),
                description: "Total pgwire errors returned to clients".to_string(),
            },
            MetricDescriptor {
                name: "pgwire_queries_total".to_string(),
                metric_type: "counter".to_string(),
                unit: "count".to_string(),
                labels: vec!["command".to_string()],
                stability: "stable".to_string(),
                description: "Total pgwire queries processed".to_string(),
            },
            MetricDescriptor {
                name: "pgwire_query_duration_ms".to_string(),
                metric_type: "histogram".to_string(),
                unit: "ms".to_string(),
                labels: vec!["command".to_string()],
                stability: "stable".to_string(),
                description: "Duration of pgwire queries in milliseconds".to_string(),
            },
            MetricDescriptor {
                name: "storage_flush_latency_ms".to_string(),
                metric_type: "histogram".to_string(),
                unit: "ms".to_string(),
                labels: vec!["tier".to_string()],
                stability: "stable".to_string(),
                description: "Storage flush duration latency in milliseconds".to_string(),
            },
        ];

        metrics.sort_by(|a, b| a.name.cmp(&b.name));
        MetricSurface { metrics }
    }
}
