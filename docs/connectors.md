# Connector guarantees

## Secret rotation

Kafka and PostgreSQL CDC sources, and SQL-wired Kafka sinks, can bind to
`secret = '<name>'`. Kafka sinks also require
`security.protocol = 'SASL_SSL'` or `'SASL_PLAINTEXT'`. New catalog entries store
the binding as `secret://<name>`; resolved credential values are not copied
into connector metadata. Attaching a secret requires an explicit `SecretUse`
grant scoped to the secret and consumer principal; missing identity or durable
policy fails closed. The gateway rechecks a source or sink consumer's current
grant before resolving credentials. Older secret-bound sink entries without a
persisted SASL protocol fail closed. The Docker-backed
[`kafka_sasl_secret_rotation_preserves_multiview_checkpoints_and_other_consumers`](../crates/rockstream-cli/tests/oidc_release_process_tests.rs)
release proof checks sink records across secret rotation and gateway restart.

The gateway pgoutput source checks the durable secret version before a source
transaction and after its committed epoch. If it changes during the transaction,
the worker commits the complete result and slot checkpoint, then opens a new
upstream connection with the authorized replacement while keeping the source
worker alive. A Kafka source uses an explicit SASL transport and PLAIN credentials;
at the next source epoch it replaces the consumer with the authorized credentials,
seeking from the durable source checkpoint while keeping the worker alive. Kafka
source rotation still needs release-process qualification across views and
partitions. A Kafka sink checks its bound secret version before each committed
view delivery and recreates its transactional producer at an epoch boundary
when credentials rotate.

RockStream supports exactly three external connector boundaries: PostgreSQL
CDC, Kafka source, and Kafka sink. `object_store` is internal durable state,
not a connector. The guarantees below are the contract for the retained
connectors.

## Strategic contract

The two source connectors are Core and release-gated. The Kafka sink is
Experimental and delivers bounded runtime view snapshot/change envelopes. The generated
[`capability matrix`](capability-matrix.md) records these tiers.

| Capability | Tier | Guarantee reference | Named proof |
| --- | --- | --- | --- |
| PostgreSQL CDC source | **Core** | [PostgreSQL CDC](#postgresql-cdc) | [`postgres_cdc_snapshot_stream_fence_has_exact_transcript`](../crates/rockstream-connectors/tests/postgres_cdc_guarantee_matrix_tests.rs) |
| Kafka source | **Core** | [Kafka source](#kafka-source) | [`kafka_source_mid_epoch_rebalance_recovers_exact_transcript`](../crates/rockstream-connectors/tests/kafka_source_guarantee_matrix_tests.rs) |
| Kafka sink | **Core** | [Kafka sink](#kafka-sink) | [`view_snapshot_changes_and_recovery_match_exact_consumer_stream`](../crates/rockstream-ops/tests/kafka_view_delivery_tests.rs) |

These are the only external connector boundaries. Removed Iceberg, Delta,
object-store, S3, and HTTP webhook surfaces fail closed with `RS-4017`; their
permanent replacements are documented in
[`docs/connector-migration.md`](connector-migration.md).

## PostgreSQL CDC

### Bounded pgoutput transport and oversize recovery

The gateway and row-only pgoutput readers stream binary COPY results from a
single logical-decoding peek per upstream transaction. PostgreSQL is allowed to
exceed `upto_nchanges` at transaction boundaries; RockStream does not treat
that argument as an allocation limit. Instead, COPY exposes at most 8 KiB per
wire row, announces each pgoutput frame's full length first, and rejects any
frame or cumulative upstream transaction that would exceed the 8 MiB source
limit **before** buffering the excessive frame. BEGIN, relation metadata and
COMMIT frames count toward the bound, not just routed row changes.

`PgOutputTransportStats` reports actual received frames, payload bytes,
peak COPY chunk and frame buffer sizes, and per-transaction payload bytes,
independently of the coordinator's encoded-change fill metrics.
The gateway still publishes all changes and the slot LSN together only after
the transaction-wide M3 durability fence. A transport protocol or size failure
returns `RS-4014`, blocks further reads and acknowledgments, and leaves the
durable source checkpoint and PostgreSQL slot position unchanged. After such a
failure the operator must reduce the upstream transaction size (or repair the
source), then perform the supported resnapshot/replacement-slot recovery
workflow; retrying against the unchanged oversized transaction will fail
closed rather than skip it.

Real PostgreSQL regression coverage:
`pgoutput_bounded_copy_preserves_complete_ordered_thousand_row_transcript_and_ack`,
`pgoutput_copy_rejects_oversized_frame_before_buffer_or_checkpoint_advances`,
and gateway test `pgoutput_gateway_publishes_complete_large_transaction_atomically_and_acks_after_commit`.

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Snapshot-to-stream uses a captured fence. A committed LSN resumes without a gap or duplicate; recoverable slot loss triggers a bounded resnapshot. |
| Bound / fill metric / backpressure | `POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS=4096`, `POSTGRES_CDC_MAX_IN_FLIGHT_BYTES=8 MiB`, `POSTGRES_CDC_MAX_TRANSACTION_BYTES=8 MiB`, WAL lag `256 MiB`, and three resnapshot attempts. Queue record/byte fill is observable; replication reads pause at a bound. |
| Degraded states | `Running`, `Blocked`, and `Resnapshotting`; bounded queue rejection is explicit. |
| Failure codes | `RS-4001`, `RS-4004`, `RS-4011`, `RS-4012`, `RS-4013`, `RS-4014`, `RS-4015`, `RS-4016`, `RS-4018`, `RS-4019`, `RS-4020`, `RS-4021`, and `RS-4022`; recover with the action in the registry. |
| Proof matrix | The nine PostgreSQL cells below and `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_lfs` / `retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_minio`. |

### PostgreSQL Configuration Prerequisites

PostgreSQL 16+ upstream must have logical replication enabled:
- `wal_level = logical`
- `max_replication_slots >= 4`
- `max_wal_senders >= 4`

### Canonical Source DDL Contract

RockStream freezes one canonical source creation contract across all seven roadmap fields:

```sql
CREATE SOURCE orders_source TYPE postgres_cdc FORMAT pgoutput OPTIONS (
    endpoint = 'postgres.internal:5432/db',
    publication = 'orders_pub',
    slot = 'orders_slot',
    table = 'public.orders',
    schema_policy = 'evolve',
    credential_ref = 'vault://credentials/pg',
    snapshot_policy = 'initial'
);
```

Plaintext passwords or credentials inline in DDL statements are strictly rejected with `RS-4008`.

### Quad-LSN Progress Persistence and Acknowledgment Barrier

RockStream durably tracks four progress points in `ShardDb`:
1. `received_lsn`: highest LSN read from the logical replication stream.
2. `applied_lsn`: highest LSN decoded and buffered in the coordinator.
3. `durable_lsn`: highest LSN whose corresponding epoch and view effects are committed to SlateDB.
4. `published_frontier`: highest LSN visible to pgwire queries.

**Upstream Slot Acknowledgment Invariant**:
`confirmed_flush_lsn <= durable_lsn`
Standby status updates sent to PostgreSQL never outrun durable SlateDB storage.

### Schema Change Policy Decision Table

| Schema Change | Classification | Engine Action | Affected Table | Unaffected Tables | Recovery Procedure |
|---|---|---|---|---|---|
| **Add nullable column** | `Compatible` | Auto-applied in memory & recorded in history | `RUNNING` | `RUNNING` | Automatic |
| **Type widening** (`int4` → `int8`) | `Compatible` | Lossless widening applied | `RUNNING` | `RUNNING` | Automatic |
| **Rename column** | `Requires Rebuild` | Relation blocked; stops ingestion for table | `BLOCKED` (`RS-4015`) | `RUNNING` | `rockstream source rebuild <src> --table <t>` |
| **Drop column** | `Requires Rebuild` | Relation blocked; dependent view invalidated | `BLOCKED` (`RS-4015`) | `RUNNING` | Rebuild view or redefine source |
| **Primary key change** | `Unsupported` | Relation blocked; PK retraction changed | `BLOCKED` (`RS-4015`) | `RUNNING` | `rockstream source resnapshot <src> --table <t>` |

Unaffected tables in a publication continue ingestion without interruption (relation isolation).

### TRUNCATE semantics

The source-wide pgoutput gateway applies PostgreSQL `TRUNCATE` to the imported
relation and every dependent materialized view in the **same durable M3 epoch**
as its transaction and LSN. It reconstructs exact negative row deltas from
local committed table images (not from the already-truncated upstream table),
including earlier and later row changes in that PostgreSQL transaction and
multi-relation TRUNCATE messages. A TRUNCATE of an empty table is a no-op.

For bounded, atomic processing, expansion is limited to **65,536 DML operations
and 8 MiB of expanded row keys/images per upstream transaction**. A full scan
that exceeds the same bound, malformed row storage, or a missing route blocks
the source with `RS-4014` (or `RS-4013` for a protocol error) **without advancing
the slot**. Operators must resolve the bound or resnapshot; the source cannot
silently ignore or partially apply TRUNCATE. The legacy row-only
`PostgresCdcSource::poll_delta` path does not have local imported-row state and
therefore rejects TRUNCATE with `RS-4015`; it does not claim support for it.

### Recovery and Resnapshot

When PostgreSQL invalidates a replication slot (e.g. WAL retention exhaustion, errors `55000` / `58P01`), the connector transitions to `BLOCKED` with error code `RS-4011` / `RS-4016`. A source with a durable checkpoint **does not silently create a replacement slot**. If the original slot cannot be recovered, a new snapshot alone is not sufficient to reconstruct already-pruned events.

### Partitioned sources, retention, and historical view correctness

For a PostgreSQL partitioned **root** used as an imported source table, its publication **must** set `publish_via_partition_root = true`; RockStream rejects a root published under child partition relation IDs with `RS-4013`, rather than silently missing their changes. For example:

```sql
CREATE PUBLICATION events_pub FOR TABLE public.events
    WITH (publish_via_partition_root = true);
```

PostgreSQL defaults this flag to false. Changes are otherwise published using individual child relation identities, not the parent identity imported by RockStream. Configure the publication before initial capture and keep its identity mode stable.

**Important retention boundary:** `ALTER TABLE ... DETACH PARTITION` and `DROP TABLE` on a partition do not produce row-level DELETE events over `pgoutput`. A maintained view may therefore still contain the historical rows or aggregates from that partition. This is a consequence of retaining local materialization, **not** a source-retention or rebuild guarantee. A fresh initial snapshot/backfill sees only the rows still present in PostgreSQL and **cannot** reconstruct omitted historical aggregates. This includes rebuilding a view, discarding a checkpoint, and recovery after replication-slot loss. A normal process restart that restores an intact RockStream checkpoint is different from rebuilding from the upstream snapshot.

`TRUNCATE` is different from row-by-row DELETE: `pgoutput` carries a TRUNCATE message without old row images. The legacy row-only decoder fails closed with `RS-4015` before acknowledging its LSN; the relation-aware gateway expands it atomically into row retractions and blocks with `RS-4014` if the local preimages cannot be reconstructed within bounds. PostgreSQL **does not publish** direct child-partition TRUNCATE when `publish_via_partition_root = true`, so even this guard cannot detect those operations. Do not perform direct partition TRUNCATE against a source whose view must stay exact.

**Operational contract until historical resnapshot retention is implemented:** Do not treat source-side partition pruning as safe, reconstructable materialized-view retention. Keep a complete replayable source/archive and validated checkpoints, or accept that rebuilding pruned historical groups is impossible. After slot loss, preserve the current RockStream checkpoint and investigate upstream history availability; never replace it with a newly truncated snapshot and describe the result as complete. A finalized TUMBLE window surviving a checkpoint restart is **not** evidence that it can be re-derived after the input partitions have been dropped.

See [PostgreSQL's publication settings](https://www.postgresql.org/docs/current/sql-createpublication.html) for partition identity, DDL, and TRUNCATE limitations.

## Kafka source

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Consumer-group offsets advance only through the committed source checkpoint; recovery seeks the committed token. Exactly-once processing coupled with SlateDB epoch commit. |
| Bound / fill metric / backpressure | `KAFKA_SOURCE_BUFFER_LIMIT=50_000` KiB, `last_poll_fill_level`, poll credits, `max_epoch_batch_records=10,000`, `max_epoch_batch_bytes=8 MiB`, and rdkafka partition pause/resume backpressure. |
| Degraded states | Assignment/rebalance, broker failure, idle partitions, and paused/backpressured recovery are observable; invalid input/configuration fails closed. |
| Failure codes | `RS-4001`, `RS-4004`, `RS-4006`, `RS-4014`, `RS-4015`, `RS-4018`, `RS-4019`, `RS-4020`, `RS-4021`, and `RS-4022`; recover with the action in the registry. |
| Proof matrix | The seven Kafka source cells below plus end-to-end qualification and workload envelope tests. |

### Kafka Configuration Prerequisites

Apache Kafka 2.8+ or Redpanda 23+ cluster with topic partitions and consumer groups enabled:
- `enable.auto.commit = false` (RockStream strictly commits offsets via durable SlateDB epochs)
- `auto.offset.reset = earliest` (or `latest`)
- Multiple partitions supported with dynamic group rebalance
- For a `sasl_plain` secret, set `security_protocol` to `SASL_SSL` or `SASL_PLAINTEXT`; prefer `SASL_SSL` to protect credentials in transit.

### Canonical Source DDL Contract

RockStream provides canonical source creation for Kafka sources:

```sql
CREATE SOURCE events_source TYPE kafka (
    endpoint = 'kafka:9092',
    topic = 'events',
    group_id = 'rockstream_group',
    security_protocol = 'SASL_SSL',
    secret = 'kafka_auth',
    offset_policy = 'earliest',
    schema_policy = 'strict',
    poll_max_records = 1000,
    poll_max_bytes = 1048576,
    idle_partition_timeout_ms = 5000
) FORMAT json;
```

### Partition Offset Management and Epoch Assembly

- **Durable Epoch Coupling**: Offsets advance upstream only when the corresponding RockStream epoch is durable in SlateDB. Replays caused by downstream worker crashes seek directly to the committed checkpoint offset token.
- **Per-view delivery**: Each maintained view derives its own Kafka consumer group from the configured `group_id`, because views keep independent offset checkpoints. Consumers for one view may share partitions; separate views each receive every partition.
- **Idle Partition Isolation**: Multi-partition consumers do not block epoch assembly indefinitely waiting for idle partitions; partitions exceeding `idle_partition_timeout_ms` (default 5,000ms) yield their progress frontier.
- **Cluster/Topic Incarnation**: Stable cluster ID, topic UUID, and partition count are tracked in `KafkaSourceIdentityV1`. Mismatched incarnations fail closed with `RS-4015`.

### Poison-Record Handling and DLQ Diagnostics

- **Policies**: Configurable via `KafkaDlqPolicy::Block` (default fail-closed) or `KafkaDlqPolicy::Dlq`.
- **Bounded Capacity**: In-memory and durable DLQ queues are bounded at `MAX_DLQ_CAPACITY = 10,000` items. Overflow triggers `RS-4014` fail-closed backpressure.
- **Diagnostic Schema**: DLQ records emit 6 structured fields: `topic`, `partition`, `offset`, `error`, `schema`, and `payload_digest` (SHA-256).
- **Sensitive Payload Redaction**: Payloads are recursively scrubbed of sensitive fields (`password`, `secret`, `token`, `key`, `authorization`, `credit_card`) via `redact_sensitive_payload`.

### Flow Control via Pause/Resume and Truthful Lag

- **Worker Budgets**: Workers enforce `DEFAULT_MAX_EPOCH_BATCH_RECORDS = 10,000` records and `DEFAULT_MAX_EPOCH_BATCH_BYTES = 8 MiB`.
- **rdkafka Pause/Resume**: When in-flight buffers exceed memory bounds, partition consumption is paused at the rdkafka broker transport level (`pause()`), resuming automatically (`resume()`) when downstream capacity frees up.
- **Truthful Lag**: `truthful_lag(committed_offsets, high_watermarks)` computes exact per-partition and total lag against broker high watermarks without synthetic heuristics.

### Performance Envelope and SLOs

- **Throughput**: Sustained ingestion >= 10,000 msg/sec (release profile).
- **Commit Latency**: p99 <= 25 ms.
- **Freshness Latency**: p99 <= 100 ms.
- **Point-Read Latency**: p99 <= 10 ms.
- **Worker Memory Ceiling**: <= 16 MiB buffer ceiling per ingestion worker.

## Kafka sink

`rockstream_ops::kafka_sink::KafkaViewSink` exports the production `ViewSinkOp`
delta log. Construct it with a connected `KafkaSink`, the view's `ShardDb`,
operator ID, and column count, then call `deliver_next()` until it returns false.
The first envelope contains the complete materialized snapshot; later envelopes
contain signed changes for one nonempty view epoch each. The legacy
`SinkConnector::pre_commit(epoch, row_count)` still emits count markers. SQL
`CREATE SINK` wires this adapter through pgwire, durable catalog persistence,
commit-time delivery, and restart recovery.

An authenticated sink uses a `sasl_plain` secret and an explicit transport:

```sql
CREATE SINK orders_out FROM orders_mv INTO KAFKA (
    brokers = 'kafka:9092', topic = 'orders',
    secret = 'kafka_auth', security.protocol = 'SASL_SSL'
);
```

The gateway resolves credentials through the durable secret catalog. It checks
for secret rotation before each committed view delivery and recreates the
transactional producer at an epoch boundary. Failed authorization or credential
refresh skips delivery.

The canonical v1 payload is a JSON object with `version: 1`, `connector_id`,
`view` (operator ID), `epoch` (sink transaction sequence starting at zero),
`view_epoch` (last included view epoch, or null for an empty initial view),
`kind` (`snapshot` or `changes`), and `rows`. Each row has `values` in schema
order and a signed integer `weight`. Values are `"Null"`, `{"Int64":1}`,
`{"Utf8":"a"}`, `{"Boolean":true}`, or `{"Float64":"3ff0000000000000"}`
(the 16 hexadecimal IEEE-754 bits, preserving NaNs and signed zero).
Whole rows identify Z-set entries; an update retracts the old row and inserts
the new row in the same envelope. Sum weights for identical rows to reconstruct
the relation. One Kafka record, keyed by the decimal sink epoch on partition
zero, is one complete Kafka transaction. A `read_committed` consumer treats the
atomic `snapshot` envelope as snapshot completion before subsequent changes.

The shard checkpoint retains the full pending payload, target identity, snapshot
state, view cursor, and next transaction sequence. The adapter flushes this state
before Kafka commit and finalizes the cursor after commit. On restart, reconnect
the producer to fence its old transaction, then resume `deliver_next()`. Recovery
checks durable topic delivery before republishing. An unresolved lookup returns
`RS-4030` and leaves the checkpoint pending.

Use one owner, a dedicated topic/connector ID, an append-only monotonically
committed view log, and a fixed schema. Retain the log and Kafka delivery records
until recovery no longer needs them. Envelopes above 900 KiB and view log reads
above 64 MiB fail closed; chunked snapshots, automatic log compaction, and full
Core release qualification remain outside this adapter.

The [#122 acceptance contract](acceptance-contracts/122.md) is checked through a
real consumer, including exact payloads, four crash windows, and wrong-value,
missing-deletion, and duplicate-change controls. Marker tests remain transport
checks only.

| Axis | Guarantee |
| --- | --- |
| Delivery / recovery | Experimental bounded snapshot/change envelopes through `KafkaViewSink`; legacy count markers through `pre_commit`. Recovery checks all topic partitions before republishing. Setup failures, poll failures, and timeouts remain pending; only a complete stable scan confirms absence. |
| Bound / fill metric / backpressure | `KAFKA_SINK_MAX_STAGED_EPOCHS=5`, `kafka_sink_staged_epochs_count`, and `backpressure_active`; staged-epoch admission rejects overflow. |
| Degraded states | Pre-commit/commit uncertainty, timeout, staged-epoch backpressure, and idempotent recovery are explicit. |
| Failure codes | `RS-4002`, `RS-4003`, `RS-4004`, `RS-4005`, and `RS-4030`; retry unknown-outcome recovery with the original pending handle after restoring lookup. |
| Proof matrix | `kafka_view_delivery_tests` checks exact view rows and recovery; the seven marker transport cells below retain their narrower scope. |

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
| `RS-4030` | Kafka sink delivery lookup is unresolved; retry recovery |

## Guarantee matrix

The source rows name TestContainers transcript proofs. Kafka view delivery is
checked by `crates/rockstream-ops/tests/kafka_view_delivery_tests.rs`. The Kafka
sink cells below exercise legacy marker transport and recovery only.

### PostgreSQL CDC

| Cell | Test |
| --- | --- |
| Snapshot → stream handoff over the v0.52.1 fence | `postgres_cdc_snapshot_stream_fence_has_exact_transcript` |
| INSERT, UPDATE including key change, and DELETE | `postgres_cdc_insert_update_delete_have_exact_transcript` |
| TRUNCATE (nonempty tables, multi-relation, same-transaction row changes, dependent views, gateway restart) | `pgoutput_truncate_retracts_nonempty_views_and_survives_gateway_restart` (gateway TestContainers) |
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

The Docker-backed release proof named above exercises authenticated SQL sink
row delivery, rotation, exact final relation recovery, and a gateway restart.

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
