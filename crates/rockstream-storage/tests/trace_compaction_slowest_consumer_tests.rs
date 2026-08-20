//! Slowest-Consumer Trace Compaction Tests (v0.59.6).
//!
//! Asserts that trace compaction never compacts past the slowest live consumer,
//! and advances compaction as soon as the slow consumer catches up.

use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::batch::ZSetRow;
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

fn sample_spec() -> ArrangementSpec {
    ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new("items"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("id")],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("val")],
        predicate: None,
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: None,
        collation_identifier: CollationId::utf8_default(),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    }
}

#[test]
fn test_compaction_slow_consumer_retention() {
    let mut trace = SharedArrangementTrace::new(sample_spec());

    // Register fast consumer A at 30, slow consumer B at 10
    trace.register_consumer_frontier(ViewId(1), 30);
    trace.register_consumer_frontier(ViewId(2), 10);

    // Commit batches: 0..10, 10..20, 20..30
    trace.commit_trace_batch(
        0,
        10,
        vec![ZSetRow {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            weight: 1,
        }],
    );
    trace.commit_trace_batch(
        10,
        20,
        vec![ZSetRow {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            weight: 1,
        }],
    );
    trace.commit_trace_batch(
        20,
        30,
        vec![ZSetRow {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
            weight: 1,
        }],
    );

    assert_eq!(trace.compute_compaction_frontier(), 10);

    // Run compaction
    let compacted_frontier = trace.compact_trace();
    assert_eq!(compacted_frontier, 10);
    assert_eq!(trace.base_frontier, 10);
    // Base snapshot contains k1
    assert_eq!(trace.base_snapshot.len(), 1);
    // Delta batches for 10..20 and 20..30 must still exist (not pruned)
    assert_eq!(trace.delta_batches.len(), 2);
    assert_eq!(trace.delta_batches[0].from_frontier, 10);
    assert_eq!(trace.delta_batches[0].to_frontier, 20);
    assert_eq!(trace.delta_batches[1].from_frontier, 20);
    assert_eq!(trace.delta_batches[1].to_frontier, 30);

    // Slow consumer can still safely read at frontier 10
    let s10 = trace.read_trace_snapshot(10).expect("read snapshot 10");
    assert_eq!(s10.len(), 1);
    assert_eq!(s10.get(b"k1".as_ref()), Some(&(b"v1".to_vec(), 1)));
}

#[test]
fn test_compaction_advancement_on_catchup() {
    let mut trace = SharedArrangementTrace::new(sample_spec());

    // Register fast consumer A at 30, slow consumer B at 10
    trace.register_consumer_frontier(ViewId(1), 30);
    trace.register_consumer_frontier(ViewId(2), 10);

    trace.commit_trace_batch(
        0,
        10,
        vec![ZSetRow {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            weight: 1,
        }],
    );
    trace.commit_trace_batch(
        10,
        20,
        vec![ZSetRow {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            weight: 1,
        }],
    );
    trace.commit_trace_batch(
        20,
        30,
        vec![ZSetRow {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
            weight: 1,
        }],
    );

    // Run first compaction at slowest frontier 10
    trace.compact_trace();
    assert_eq!(trace.base_frontier, 10);
    assert_eq!(trace.delta_batches.len(), 2);

    // Slow consumer B catches up to frontier 30
    trace.advance_consumer_frontier(ViewId(2), 30);
    assert_eq!(trace.compute_compaction_frontier(), 30);

    // Run second compaction
    let new_compacted_frontier = trace.compact_trace();
    assert_eq!(new_compacted_frontier, 30);
    assert_eq!(trace.base_frontier, 30);
    assert_eq!(trace.base_snapshot.len(), 3);
    assert_eq!(trace.delta_batches.len(), 0); // All batches compacted into base snapshot!

    // Both consumers read at frontier 30
    let s30 = trace.read_trace_snapshot(30).expect("read snapshot 30");
    assert_eq!(s30.len(), 3);
    assert_eq!(s30.get(b"k1".as_ref()), Some(&(b"v1".to_vec(), 1)));
    assert_eq!(s30.get(b"k2".as_ref()), Some(&(b"v2".to_vec(), 1)));
    assert_eq!(s30.get(b"k3".as_ref()), Some(&(b"v3".to_vec(), 1)));
}
