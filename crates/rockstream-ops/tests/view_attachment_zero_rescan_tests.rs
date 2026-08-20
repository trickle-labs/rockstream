//! View Attachment Zero-Rescan Tests (v0.59.6).
//!
//! Asserts that attaching a new view to a shared arrangement trace:
//! 1. Dispatches 0 scan / read requests to the upstream source connector.
//! 2. Initializes the view state identically to a full relation scan.

use rockstream_ops::view_attach::{AttachedView, AttachmentDeltaBuffer};
use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalExpr, CanonicalType, CollationId, CollationVersion, NullSemantics,
    PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::batch::ZSetRow;
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use rockstream_types::metrics;

fn sample_spec() -> ArrangementSpec {
    ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new("trades"),
        source_schema_generation: 1,
        key_expressions: vec![CanonicalExpr::col("symbol")],
        key_types: vec![CanonicalType::Utf8],
        value_projection: vec![CanonicalExpr::col("price")],
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
fn test_view_attachment_zero_rescan() {
    metrics::reset_all();
    let mut trace = SharedArrangementTrace::new(sample_spec());

    // Populate trace up to frontier 100
    trace.commit_trace_batch(
        0,
        50,
        vec![
            ZSetRow {
                key: b"AAPL".to_vec(),
                value: b"150".to_vec(),
                weight: 1,
            },
            ZSetRow {
                key: b"MSFT".to_vec(),
                value: b"300".to_vec(),
                weight: 1,
            },
        ],
    );
    trace.commit_trace_batch(
        50,
        100,
        vec![
            ZSetRow {
                key: b"GOOG".to_vec(),
                value: b"2800".to_vec(),
                weight: 1,
            },
            ZSetRow {
                key: b"AAPL".to_vec(),
                value: b"155".to_vec(),
                weight: 1,
            },
        ],
    );
    let before = metrics::r1_arrangement_snapshot()[0].1;

    let mut delta_buffer = AttachmentDeltaBuffer::new(1000);

    // Attach View 21 at frontier 100 without rescanning source
    let attached = AttachedView::attach(ViewId(21), &mut trace, 100, &mut delta_buffer)
        .expect("attachment succeeds");

    // Assert zero source scans
    assert_eq!(attached.metrics.source_scan_requests, 0);
    assert_eq!(attached.metrics.pinned_frontier, 100);
    assert_eq!(attached.metrics.snapshot_rows_loaded, 3);
    assert_eq!(attached.state.len(), 3);
    assert_eq!(
        attached.state.get(b"MSFT".as_ref()),
        Some(&(b"300".to_vec(), 1))
    );
    assert_eq!(
        attached.state.get(b"GOOG".as_ref()),
        Some(&(b"2800".to_vec(), 1))
    );
    let after = metrics::r1_arrangement_snapshot()[0].1;
    assert_eq!(
        (
            after.source_key_builds,
            after.trace_rows_written,
            after.accepted_source_changes,
            after.source_index_cpu_ns,
        ),
        (
            before.source_key_builds,
            before.trace_rows_written,
            before.accepted_source_changes,
            before.source_index_cpu_ns,
        )
    );
}
