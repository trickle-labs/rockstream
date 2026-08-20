use rockstream_types::candidate_identity::CandidateIdentity;

#[test]
fn test_candidate_identity_fields_populated() {
    let id = CandidateIdentity::current();
    assert_eq!(id.semantic_version, "0.59.6");
    assert!(!id.commit_sha.is_empty(), "commit_sha must not be empty");
    assert!(
        !id.build_timestamp_rfc3339.is_empty(),
        "build_timestamp_rfc3339 must not be empty"
    );
    assert!(
        id.compiler_version.contains("rustc"),
        "compiler_version must identify rustc"
    );
    assert_eq!(
        id.lockfile_digest.len(),
        64,
        "lockfile_digest must be a 64-char SHA-256 hex string"
    );
    assert!(
        id.lockfile_digest
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "lockfile_digest must be lowercase hex"
    );
}

#[test]
fn test_candidate_identity_lockfile_digest_matches_cargo_lock() {
    let id = CandidateIdentity::current();
    let lockfile_bytes = std::fs::read("../../Cargo.lock")
        .or_else(|_| std::fs::read("../Cargo.lock"))
        .or_else(|_| std::fs::read("Cargo.lock"))
        .expect("Cargo.lock must exist in repository root");
    let expected_digest = CandidateIdentity::compute_sha256_hex(&lockfile_bytes);
    assert_eq!(id.lockfile_digest, expected_digest);
}

#[test]
fn test_candidate_identity_serialization_roundtrip() {
    let id = CandidateIdentity::current();
    let json_str = id.to_json().expect("must serialize to JSON");
    let deserialized: CandidateIdentity =
        serde_json::from_str(&json_str).expect("must deserialize from JSON");
    assert_eq!(id, deserialized);
}

#[test]
fn test_candidate_identity_display_text() {
    let id = CandidateIdentity::current();
    let display = id.display_text();
    assert!(display.contains("rockstream 0.59.6"));
    assert!(display.contains(&format!("commit: {}", id.commit_sha)));
    assert!(display.contains(&format!("lockfile_digest: {}", id.lockfile_digest)));
}
