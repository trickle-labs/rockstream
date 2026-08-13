# Connector guarantees

RockStream supports exactly three external connector boundaries: PostgreSQL
CDC, Kafka source, and Kafka sink. `object_store` is internal durable state,
not a connector. The guarantees below are the contract for the retained
connectors.

## PostgreSQL CDC

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Snapshot-to-stream uses a captured fence. A committed LSN resumes without a gap or duplicate; recoverable slot loss triggers a bounded resnapshot. |
| Bound / fill metric / backpressure | `POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS=4096`, `POSTGRES_CDC_MAX_IN_FLIGHT_BYTES=8 MiB`, `POSTGRES_CDC_MAX_TRANSACTION_BYTES=8 MiB`, WAL lag `256 MiB`, and three resnapshot attempts. Queue record/byte fill is observable; replication reads pause at a bound. |
| Degraded states | `Running`, `Blocked`, and `Resnapshotting`; bounded queue rejection is explicit. |
| Failure codes | `RS-4001`, `RS-4004`, `RS-4011`, `RS-4012`, `RS-4013`, `RS-4014`, `RS-4015`, `RS-4016`, `RS-4018`, `RS-4019`, `RS-4020`, `RS-4021`, and `RS-4022`; recover with the action in the registry. |
| Proof matrix | The nine PostgreSQL cells below and `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_lfs` / `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_minio`. |

## Kafka source

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Consumer-group offsets advance only through the committed source checkpoint; recovery seeks the committed token. |
| Bound / fill metric / backpressure | `KAFKA_SOURCE_BUFFER_LIMIT=50_000` KiB, `last_poll_fill_level`, poll credits, and pause/resume. One overflow record is retained locally. |
| Degraded states | Assignment/rebalance, broker failure, and paused/backpressured recovery are observable; invalid input/configuration fails closed. |
| Failure codes | `RS-4001`, `RS-4004`, `RS-4006`, `RS-4015`, `RS-4018`, `RS-4019`, `RS-4020`, `RS-4021`, and `RS-4022`; recover with the action in the registry. |
| Proof matrix | The seven Kafka source cells below. |

## Kafka sink

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Transactional, checkpoint-coupled commit; recovery re-runs safely without a second externally visible epoch. |
| Bound / fill metric / backpressure | `KAFKA_SINK_MAX_STAGED_EPOCHS=5`, `kafka_sink_staged_epochs_count`, and `backpressure_active`; staged-epoch admission rejects overflow. |
| Degraded states | Pre-commit/commit uncertainty, timeout, staged-epoch backpressure, and idempotent recovery are explicit. |
| Failure codes | `RS-4002`, `RS-4003`, `RS-4004`, and `RS-4005`; recover with the action in the registry. |
| Proof matrix | The seven Kafka sink cells below. |

## Failure-code ownership

| Codes | Connector ownership |
| --- | --- |
| `RS-4001` | Source connection failure |
| `RS-4002` | Sink write failure |
| `RS-4003` | Sink pre-commit failure |
| `RS-4004` | Sink commit or source poll recovery failure |
| `RS-4005` | Sink duplicate delivery |
| `RS-4006` | Source epoch registry capacity |
| `RS-4007` | CREATE SINK validation |
| `RS-4008` | CREATE SOURCE validation |
| `RS-4009` | Source not found |
| `RS-4010` | Source already exists |
| `RS-4011` | PostgreSQL CDC recovery required |
| `RS-4012` | Source owner checkpoint recovery required |
| `RS-4013` | PostgreSQL CDC protocol or ownership validation |
| `RS-4014` | Source bounded in-flight capacity |
| `RS-4015` | Source checkpoint fence mismatch |
| `RS-4016` | Source checkpoint acknowledgement |
| `RS-4017` | Removed connector surface; see [connector migration](connector-migration.md) |
| `RS-4018` | Source epoch exhaustion |
| `RS-4019` | Source backfill cursor or lifecycle |
| `RS-4020` | Backfill live-delta buffer |
| `RS-4021` | Backfill admission reservation |
| `RS-4022` | Backfill publication state |

## Guarantee matrix

Every row names the exact TestContainers proof. Each proof records the full
payload/key/weight/LSN or offset transcript before and after recovery, and
asserts no loss, no duplicate, and recovery within the existing 60-second
freshness budget.

### PostgreSQL CDC

| Cell | Test |
| --- | --- |
| Snapshot → stream handoff over the v0.52.1 fence | `postgres_cdc_snapshot_stream_fence_has_exact_transcript` |
| INSERT, UPDATE including key change, DELETE, and TRUNCATE | `postgres_cdc_all_mutation_types_have_exact_transcript` |
| Restart at every commit boundary | `postgres_cdc_each_commit_boundary_recovers_exactly_once` |
| WAL lag | `postgres_cdc_wal_lag_pauses_at_bound_and_recovers_within_slo` |
| Malformed replication record | `postgres_cdc_malformed_replication_record_fails_closed_then_recovers_exactly` |
| Replication-slot loss | `postgres_cdc_replication_slot_loss_resnapshots_with_exact_transcript` |
| Publication loss | `postgres_cdc_publication_loss_fails_clearly_then_recovers_exactly` |
| Bounded backpressure | `postgres_cdc_backpressure_never_exceeds_record_or_byte_bound` |
| Long-running recovery | `postgres_cdc_long_running_recovery_is_exact_and_within_slo` |

### Kafka source

| Cell | Test |
| --- | --- |
| Consumer rebalance mid-epoch | `kafka_source_mid_epoch_rebalance_recovers_exact_transcript` |
| Partition expansion | `kafka_source_partition_expansion_has_exact_transcript` |
| Offset recovery | `kafka_source_committed_offset_recovery_has_exact_transcript` |
| Broker interruption | `kafka_source_broker_interruption_recovers_exactly_within_slo` |
| Bounded buffer | `kafka_source_buffer_bound_and_fill_level_are_exact` |
| Duplicate prevention | `kafka_source_duplicate_redelivery_has_exactly_one_transcript` |
| Transactional source/sink interaction | `kafka_source_sink_transaction_coupling_has_exact_transcript` |

### Kafka sink

| Cell | Test |
| --- | --- |
| Crash before commit | `kafka_sink_crash_before_commit_has_no_visible_payload_and_recovers_exactly` |
| Crash during commit | `kafka_sink_crash_during_commit_recovers_exactly_once_within_slo` |
| Uncertain broker response | `kafka_sink_uncertain_broker_response_recovers_exactly_once_within_slo` |
| Transaction timeout | `kafka_sink_transaction_timeout_recovers_exactly_once_within_slo` |
| Recovery re-run | `kafka_sink_recovery_rerun_has_exactly_one_payload_per_epoch` |
| Duplicate prevention | `kafka_sink_duplicate_commit_has_exactly_one_payload_per_epoch` |
| Checkpoint coupling | `kafka_sink_checkpoint_coupling_has_exact_commit_transcript` |

## Durability and cleanup proofs

- `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_lfs`
- `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_minio`
- `backfill_cleanup_uses_bounded_scan_and_point_delete`

Checkpoint recovery uses only the highest committed checkpoint. Cleanup is a
bounded scan followed by point deletes. No code path depends on SlateDB range
deletion.
