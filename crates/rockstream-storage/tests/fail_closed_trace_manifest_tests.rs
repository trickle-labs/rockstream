//! v0.59.6 Slice 6: Fail-Closed Trace Manifest Tests.

use rockstream_storage::error::StorageError;
use rockstream_storage::trace::TraceManifestHeader;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::ids::TenantId;

#[test]
fn test_incompatible_trace_manifest_fail_closed() {
    let spec = ArrangementSpec::default_for_source(TenantId(1), "events");
    let header = TraceManifestHeader::new(spec, 10);
    let bytes = header.to_bytes().unwrap();

    // Verify valid deserialization
    let parsed = TraceManifestHeader::from_bytes(&bytes).unwrap();
    assert_eq!(parsed.arrangement_id, header.arrangement_id);

    // Corrupt format version in JSON (set format_version to 99)
    let json_str = String::from_utf8(bytes).unwrap();
    let corrupted_version = json_str.replace("\"format_version\":3", "\"format_version\":99");
    let err = TraceManifestHeader::from_bytes(corrupted_version.as_bytes()).unwrap_err();
    assert!(
        matches!(err, StorageError::IncompatibleFormat { stored: 99, .. }),
        "Expected IncompatibleFormat error, got: {:?}",
        err
    );

    // Corrupted payload / invalid bytes (RS-5003)
    let err_corrupt = TraceManifestHeader::from_bytes(b"invalid json gibberish").unwrap_err();
    assert!(
        matches!(err_corrupt, StorageError::Unsupported(ref msg) if msg.contains("RS-5003")),
        "Expected RS-5003 error, got: {:?}",
        err_corrupt
    );
}

#[test]
fn test_mismatched_spec_arrangement_id_fail_closed() {
    let spec = ArrangementSpec::default_for_source(TenantId(1), "events");
    let mut header = TraceManifestHeader::new(spec, 10);
    // Tamper with arrangement_id
    header.arrangement_id = rockstream_types::ids::ArrangementId(99999);
    let bytes = serde_json::to_vec(&header).unwrap();

    let err = TraceManifestHeader::from_bytes(&bytes).unwrap_err();
    assert!(
        matches!(err, StorageError::Unsupported(ref msg) if msg.contains("RS-5003")),
        "Expected RS-5003 spec mismatch error, got: {:?}",
        err
    );
}
