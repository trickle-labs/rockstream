//! View Attachment Gap & Duplicate Tests (v0.59.6).
//!
//! Asserts that attaching a new view during continuous ingestion has 0 duplicate rows
//! and 0 dropped rows against a batch multiset oracle.

use rockstream_ops::view_attach::{AttachedView, AttachmentDeltaBuffer};
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
        source_identity: SourceIdentity::new("trades"),
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
fn test_view_attachment_under_concurrent_ingestion() {
    let mut trace = SharedArrangementTrace::new(sample_spec());

    // Historical batches 0..10
    trace.commit_trace_batch(
        0,
        10,
        vec![
            ZSetRow {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                weight: 1,
            },
            ZSetRow {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                weight: 1,
            },
        ],
    );

    // Buffer deltas that arrive while view is initializing (frontiers 11, 12, 13)
    let mut delta_buffer = AttachmentDeltaBuffer::new(10_000);
    let delta11 = vec![ZSetRow {
        key: b"k3".to_vec(),
        value: b"v3".to_vec(),
        weight: 1,
    }];
    let delta12 = vec![ZSetRow {
        key: b"k1".to_vec(),
        value: b"v1_new".to_vec(),
        weight: 1,
    }];
    let delta13 = vec![ZSetRow {
        key: b"k2".to_vec(),
        value: b"v2".to_vec(),
        weight: -1, // Retraction
    }];

    delta_buffer.push(11, delta11.clone()).unwrap();
    delta_buffer.push(12, delta12.clone()).unwrap();
    delta_buffer.push(13, delta13.clone()).unwrap();

    // Commit them also to the shared trace
    trace.commit_trace_batch(10, 11, delta11);
    trace.commit_trace_batch(11, 12, delta12);
    trace.commit_trace_batch(12, 13, delta13);

    // Attach View at pinned frontier 10, draining buffer 11..13
    let mut attached = AttachedView::attach(ViewId(99), &mut trace, 10, &mut delta_buffer)
        .expect("attach succeeds");

    assert_eq!(attached.metrics.buffered_delta_batches, 3);
    assert_eq!(attached.metrics.buffered_delta_rows, 3);
    assert_eq!(attached.current_frontier, 13);

    // Now apply live batch at frontier 14
    let live_delta14 = vec![ZSetRow {
        key: b"k4".to_vec(),
        value: b"v4".to_vec(),
        weight: 1,
    }];
    trace.commit_trace_batch(13, 14, live_delta14.clone());
    attached.apply_live_batch(&mut trace, 14, live_delta14);

    // Check final state matches oracle: k1 (weight 2), k3 (weight 1), k4 (weight 1), k2 (retracted)
    assert_eq!(attached.state.len(), 3);
    assert_eq!(
        attached.state.get(b"k1".as_ref()),
        Some(&(b"v1_new".to_vec(), 2))
    );
    assert_eq!(attached.state.get(b"k2".as_ref()), None);
    assert_eq!(
        attached.state.get(b"k3".as_ref()),
        Some(&(b"v3".to_vec(), 1))
    );
    assert_eq!(
        attached.state.get(b"k4".as_ref()),
        Some(&(b"v4".to_vec(), 1))
    );
}
