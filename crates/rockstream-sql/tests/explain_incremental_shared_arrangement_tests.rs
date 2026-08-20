//! Tests for EXPLAIN INCREMENTAL with shared arrangement facts (v0.59.6).

use rockstream_plan::PlanNode;
use rockstream_sql::explain_incremental_with_arrangements;
use rockstream_types::explain::ArrangementSharingInfo;
use rockstream_types::ids::ArrangementId;

#[test]
fn test_explain_incremental_shared_arrangement_rendering() {
    let plan = PlanNode::Source {
        name: "events".to_string(),
    };

    let arrangements = vec![ArrangementSharingInfo {
        arrangement_id: Some(ArrangementId(42)),
        consumer_count: 5,
        shared_state_bytes: 1048576,
        bytes_saved_by_sharing: 4194304,
        compaction_frontier: 100,
    }];

    let text = explain_incremental_with_arrangements(&plan, &arrangements);
    assert!(text.contains("Source[events]"), "text: {text}");
    assert!(text.contains("arrangement_id=arr-42"), "text: {text}");
    assert!(text.contains("consumers=5"), "text: {text}");
    assert!(text.contains("shared_bytes=1048576"), "text: {text}");
    assert!(text.contains("saved_bytes=4194304"), "text: {text}");
    assert!(text.contains("compaction_frontier=100"), "text: {text}");
}
