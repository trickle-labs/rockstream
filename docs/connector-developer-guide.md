# Connector Developer Guide

**Version**: v0.48.0  
**Status**: Phase-boundary documentation (ROADMAP.md §Documentation Budget)

This guide covers the Tier 1 and Tier 2 connector contracts, SDK usage,
dead-letter-queue (DLQ) configuration, and an end-to-end example connector
that declares a `COUNTER` column.

---

## Overview

RockStream connectors bridge external systems with the IVM engine. Every
connector is classified by its **tier**:

| Tier | Features | Example connectors |
|------|----------|-------------------|
| Tier 1 | Opaque `OffsetToken`, watermark, credit backpressure, DLQ routing | Kafka, Postgres CDC, HTTP, S3 |
| Tier 2 | + partition filter push-down, `should_flush` override, CRDT schema declaration | Iceberg, Delta, Parquet |

---

## Tier 1 Contract

All connectors **must** implement these methods from the `Source` or `Sink`
trait (defined in `crates/rockstream-connectors/src/source.rs` and `sink.rs`).

### Source (Tier 1)

```rust
#[async_trait]
pub trait Source: Send {
    /// Poll for the next batch. Returns None when exhausted.
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch>;

    /// Connector name for diagnostics.
    fn name(&self) -> &str;

    /// Backpressure: credits available from downstream.
    fn credits_available(&self) -> usize { usize::MAX }

    /// Update credit count.
    fn set_credits(&mut self, credits: usize) {}

    /// Current opaque offset (for checkpoint/resume).
    fn current_offset(&self) -> Option<OffsetToken> { None }
}
```

**`SourceBatch`** fields:
- `record_count: usize` — number of records in this batch.
- `epoch: Epoch` — the epoch this batch belongs to.
- `offset: Option<OffsetToken>` — opaque source offset for resume.
- `watermark: Option<EventTimeWatermark>` — event-time watermark in ms.

### Sink (Tier 1)

```rust
#[async_trait]
pub trait Sink: Send {
    /// Stage rows in pre-commit buffer.
    async fn prepare(&mut self, batch: &SinkBatch) { self.write_batch(batch).await }

    /// Legacy: equivalent to prepare.
    async fn write_batch(&mut self, batch: &SinkBatch);

    /// Commit after cluster checkpoint succeeds.
    async fn commit(&mut self, epoch: Epoch);

    /// Abort if checkpoint is cancelled.
    async fn abort(&mut self, _epoch: Epoch) {}

    /// Connector name.
    fn name(&self) -> &str;
}
```

### Dead-Letter Queue (DLQ)

Per-record decode errors are routed to the DLQ instead of crashing the
pipeline. DLQ surface (added in v0.47):

```sql
-- Inspect failed records
SELECT * FROM rockstream_catalog.dead_letter_queue
WHERE source_name = 'my-kafka-source'
ORDER BY arrived_at DESC;

-- Re-process after schema fix
ALTER SOURCE my_source REPLAY DEAD_LETTER_QUEUE SINCE '2024-01-01' UNTIL '2024-01-02';

-- Dismiss known-bad records
ALTER SOURCE my_source DISMISS DEAD_LETTER_QUEUE WHERE error_code = 'RS-1003';
```

Error `RS-1004 connector.dlq_growing` fires when the DLQ exceeds
`dlq_warn_threshold` entries/hour (default: 100).

---

## Tier 2 Contract

Tier 2 extends Tier 1 with three optional capability groups. All Tier 2
methods have default implementations, so Tier 1 connectors compile unchanged.

### 1. Partition Filter Push-Down

```rust
/// Returns true if this connector can filter partitions server-side.
fn partition_filter_support(&self) -> bool { false }

/// Bootstrap with optional partition predicate.
async fn start_snapshot(
    &mut self,
    epoch: Epoch,
    filter: Option<&PartitionFilter>,
) -> Option<SourceBatch> { ... }

/// Poll deltas with optional partition predicate.
async fn poll_delta(
    &mut self,
    epoch: Epoch,
    filter: Option<&PartitionFilter>,
) -> Option<SourceBatch> { ... }
```

When `partition_filter_support()` returns `false` (the default), the operator
layer applies equivalent filtering itself, producing identical output. This
ensures correctness for Tier 1 connectors without any changes.

**`PartitionFilter`** fields:
- `predicate: PartitionPredicate` — serialised expression + referenced columns.
- `row_level_fallback: bool` — also filter individual rows after partition pruning.

Convenience constructors:

```rust
// Equality filter
let f = PartitionFilter::eq("region", "us-east-1");

// Range filter
let f = PartitionFilter::between("partition_id", "0", "7");
```

### 2. File-Format Flush Override (`should_flush`)

```rust
/// Override flush trigger for file-format sinks.
/// bytes_buffered: total bytes staged.
/// epochs_buffered: epochs accumulated without a flush.
fn should_flush(&self, bytes_buffered: u64, epochs_buffered: u32) -> bool {
    true // default: flush every epoch (Tier 1 behaviour)
}
```

**Iceberg/Delta/Parquet sinks** should override to produce large files:

```rust
const MIN_FLUSH_BYTES: u64 = 256 * 1024 * 1024; // 256 MB
const MAX_EPOCHS: u32 = 6000;                     // ~60 s at 10ms epochs

fn should_flush(&self, bytes_buffered: u64, epochs_buffered: u32) -> bool {
    bytes_buffered >= MIN_FLUSH_BYTES || epochs_buffered >= MAX_EPOCHS
}
```

This limits file production to **≤ 2 files/minute** (≥ 256 MB each) at
10ms epoch rate.

### 3. CRDT Schema Metadata (`discover_schema`)

Connectors declare CRDT columns via `discover_schema`:

```rust
/// Return CRDT column metadata for this connector.
fn discover_schema(&self) -> LawSchemaMetadata {
    LawSchemaMetadata::empty()
}
```

`LawSchemaMetadata` builder API:

```rust
let meta = LawSchemaMetadata::empty()
    .with_column(
        "event_count",       // column name
        MergeLawId(10),      // PNCounter/v1
        "COUNTER",           // SQL CRDT type
        WriteClassification::BlindDelta,
    )
    .with_column(
        "last_seen",
        MergeLawId(11),      // MaxRegister/v1
        "MAX_REGISTER",
        WriteClassification::ExactKeyGuardedDelta,
    );
```

**`WriteClassification`** variants:

| Variant | Meaning |
|---------|---------|
| `BlindDelta` | No read-dependency; apply without reading current value. |
| `ReadDependentDelta` | Depends on current value; gateway fences concurrent writers. |
| `ExactKeyGuardedDelta` | One logical writer per key; conflicts are rare. |
| `SourceExactlyOnceProtected` | Transport-level exactly-once; skip gateway deduplication. |

### 4. Connector Lifecycle

All connectors support pause/resume/delete via:

```rust
fn lifecycle_state(&self) -> ConnectorLifecycleState { Running }
async fn pause(&mut self) -> bool { false }
async fn resume(&mut self) -> bool { false }
async fn delete(&mut self) {}
```

SQL surface:

```sql
ALTER SOURCE my_source PAUSE;
ALTER SOURCE my_source RESUME;
DROP SOURCE my_source;
```

---

## SDK: End-to-End Example (COUNTER Column)

The `example_sdk` module (`crates/rockstream-connectors/src/example_sdk.rs`)
shows a minimal third-party connector declaring a `COUNTER` column:

```rust
use rockstream_connectors::example_sdk::{ExampleSdkSource, ExampleSdkSink};
use rockstream_connectors::source::Source;

// 1. Create the connector
let mut source = ExampleSdkSource::new("events");

// 2. Discover schema — returns CRDT column metadata
let meta = source.discover_schema();
assert!(meta.columns.contains_key("event_count"));
assert_eq!(meta.columns["event_count"].crdt_type, "COUNTER");

// 3. Round-trip through ExplainTransaction
use rockstream_types::connector::ExplainTransaction;
let explain = ExplainTransaction::from_schema_metadata(
    source.name(),
    source.partition_filter_support(),
    &meta,
);
// explain.format_lines() shows write-classification in EXPLAIN TRANSACTION output
```

This pattern works for any built-in CRDT type: `COUNTER`, `MAX_REGISTER`,
`MIN_REGISTER`, `LWW`, `OR_SET`, `MV_REGISTER`.

---

## EXPLAIN TRANSACTION

Write-classification metadata surfaces in `EXPLAIN TRANSACTION`:

```
Connector: events  (partition_filter_support=false)
  column=event_count crdt_type=COUNTER law=law-0010 write_classification=blind_delta
```

The gateway's optimistic transaction validator uses this metadata to determine
whether a write requires read-modify-write fencing or can be applied blindly.

---

## Testing Your Connector

Run the Tier 1 contract tests from `crates/rockstream-connectors/src/example_sdk.rs`
as a reference baseline. Your connector should pass equivalent tests covering:

1. `poll_batch` returns valid `SourceBatch` with `offset` and `watermark`.
2. `credits_available()` / `set_credits()` gates consumption correctly.
3. `partition_filter_support()` returns the correct value.
4. `should_flush()` returns `true` for the Tier 1 default or your Tier 2 policy.
5. `discover_schema()` declares the expected CRDT columns.
6. Lifecycle `pause` → `resume` → `delete` transitions work.

```bash
cargo test --package rockstream-connectors
```

---

## Error Codes

| Code | Name | Description |
|------|------|-------------|
| `RS-1003` | `connector.decode_error` | Per-record decode failure; record routed to DLQ. |
| `RS-1004` | `connector.dlq_growing` | DLQ exceeds `dlq_warn_threshold` entries/hour. |

---

*This document satisfies the v0.48 phase-boundary documentation deliverable
(ROADMAP.md §Documentation Budget).*
