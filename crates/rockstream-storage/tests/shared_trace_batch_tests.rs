//! Shared Arrangement Trace Batch Tests (v0.59.6).
//!
//! Tests committing immutable sorted trace batches, incremental snapshot reading,
//! and delta weight consolidation.

use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::batch::ZSetRow;
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use rockstream_types::metrics::{self, R1ArrangementCounters};

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
fn test_shared_trace_batches_and_snapshot_reads() {
    metrics::reset_all();
    let mut trace = SharedArrangementTrace::new(sample_spec());

    // Batch 1: frontier 0 -> 10
    let b1 = vec![
        (b"k1".to_vec(), b"v1".to_vec(), 1),
        (b"k2".to_vec(), b"v2".to_vec(), 1),
    ];
    trace.commit_source_batch(0, 10, b1, |(key, value, weight)| ZSetRow {
        key,
        value,
        weight,
    });

    // Snapshot at frontier 10
    let s10 = trace.read_trace_snapshot(10).expect("read snapshot 10");
    assert_eq!(s10.len(), 2);
    assert_eq!(s10.get(b"k1".as_ref()), Some(&(b"v1".to_vec(), 1)));
    assert_eq!(s10.get(b"k2".as_ref()), Some(&(b"v2".to_vec(), 1)));

    // Batch 2: frontier 10 -> 20 (update k1 to v1_updated, delete k2)
    let b2 = vec![
        (b"k1".to_vec(), b"v1_updated".to_vec(), 1),
        (b"k2".to_vec(), b"v2".to_vec(), -1),
        (b"k3".to_vec(), b"v3".to_vec(), 1),
    ];
    trace.commit_source_batch(10, 20, b2, |(key, value, weight)| ZSetRow {
        key,
        value,
        weight,
    });

    // Snapshot at frontier 20
    let s20 = trace.read_trace_snapshot(20).expect("read snapshot 20");
    assert_eq!(s20.len(), 2); // k1 and k3; k2 retracted
    assert_eq!(s20.get(b"k1".as_ref()), Some(&(b"v1_updated".to_vec(), 2)));
    assert_eq!(s20.get(b"k2".as_ref()), None);
    assert_eq!(s20.get(b"k3".as_ref()), Some(&(b"v3".to_vec(), 1)));

    // Historical read at frontier 10 is still accessible and isolated
    let s10_recheck = trace.read_trace_snapshot(10).expect("read snapshot 10");
    assert_eq!(s10_recheck.len(), 2);
    assert_eq!(s10_recheck.get(b"k2".as_ref()), Some(&(b"v2".to_vec(), 1)));

    trace.register_consumer_frontier(ViewId(1), 20);
    trace.register_consumer_frontier(ViewId(2), 20);
    trace.record_lfs_usage(2, 68);
    let snapshot = metrics::r1_arrangement_snapshot();
    assert_eq!(snapshot.len(), 1);
    let (arrangement_id, mut counters) = snapshot[0];
    assert_eq!(arrangement_id, sample_spec().arrangement_id());
    let source_index_cpu_ns = counters.source_index_cpu_ns;
    assert!(source_index_cpu_ns > 0);
    counters.source_index_cpu_ns = 0;
    assert_eq!(
        counters,
        R1ArrangementCounters {
            logical_trace_bytes: 68,
            consumer_metadata_bytes: 32,
            lfs_files: 2,
            lfs_bytes: 68,
            source_key_builds: 5,
            trace_rows_written: 5,
            accepted_source_changes: 5,
            source_index_cpu_ns: 0,
        }
    );

    assert_eq!(trace.compact_trace(), 20);
    let (_, mut compacted) = metrics::r1_arrangement_snapshot()[0];
    assert_eq!(compacted.source_index_cpu_ns, source_index_cpu_ns);
    compacted.source_index_cpu_ns = 0;
    assert_eq!(
        compacted,
        R1ArrangementCounters {
            logical_trace_bytes: 32,
            consumer_metadata_bytes: 32,
            lfs_files: 2,
            lfs_bytes: 68,
            source_key_builds: 5,
            trace_rows_written: 5,
            accepted_source_changes: 5,
            source_index_cpu_ns: 0,
        }
    );
}
