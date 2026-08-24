# Metrics reference

| Name | Type | Unit | Labels | Stability | Description |
| --- | --- | --- | --- | --- | --- |
| checkpoint_committed_total | counter | count | tier | stable | Total number of durable checkpoints successfully committed |
| dlq_messages_total | counter | count | source | stable | Total poison records routed to the dead-letter queue |
| dlq_replay_failed_total | counter | count | source | stable | Total dead-letter queue replay execution failures |
| dlq_replay_success_total | counter | count | source | stable | Total dead-letter queue messages successfully reprocessed |
| l0_backlog_count | gauge | count | shard | stable | Current L0 SST file backlog for SlateDB storage shards |
| manifest_write_total | counter | count | epoch | stable | Total epoch manifest writes committed to durable storage |
| merge_law_applied_total | counter | count | law, operator | stable | Total number of successful merge-law applications on state arrangements |
| merge_law_fallback_total | counter | count | law, operator | stable | Total number of merge-law fallbacks to default accumulator |
| merge_law_rmw_avoided_total | counter | count | law | stable | Total read-modify-write state accesses avoided through blind merge |
| merge_law_rmw_required_total | counter | count | law | stable | Total read-modify-write state accesses required |
| operator_dirty_keys | gauge | count | operator_id | stable | Current number of dirty keys pending in-memory arrangement commit |
| operator_records_in_total | counter | count | operator_id, view_name | stable | Total input delta records received by operator |
| operator_records_out_total | counter | count | operator_id, view_name | stable | Total output delta records emitted by operator |
| pending_compaction_bytes | gauge | bytes | shard | stable | Total uncompacted bytes pending in storage tier |
| pgwire_connections_active | gauge | count |  | stable | Number of currently active pgwire client connections |
| pgwire_errors_total | counter | count | sqlstate, error_code | stable | Total pgwire errors returned to clients |
| pgwire_queries_total | counter | count | command | stable | Total pgwire queries processed |
| pgwire_query_duration_ms | histogram | ms | command | stable | Duration of pgwire queries in milliseconds |
| storage_flush_latency_ms | histogram | ms | tier | stable | Storage flush duration latency in milliseconds |

