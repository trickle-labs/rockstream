use rockstream_storage::catalog::envelope::{CatalogEnvelope, CURRENT_CATALOG_FORMAT_VERSION};

#[test]
fn test_envelope_format_version_enforcement() {
    let payload = b"test payload".to_vec();
    let env = CatalogEnvelope::new(1, 100, 1, payload);
    let bytes = env.encode().unwrap();

    // Valid envelope decodes successfully
    let decoded = CatalogEnvelope::decode(&bytes).unwrap();
    assert_eq!(
        decoded.catalog_format_version,
        CURRENT_CATALOG_FORMAT_VERSION
    );

    // Unsupported format version (> 1) fails closed with RS-1002
    let mut invalid_env = env.clone();
    invalid_env.catalog_format_version = 2; // Future unsupported version
    let invalid_bytes = serde_json::to_vec(&invalid_env).unwrap();
    let res = CatalogEnvelope::decode(&invalid_bytes);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1002]"),
        "Expected RS-1002 error, got: {err}"
    );
}

#[test]
fn test_record_version_upgrade_rules() {
    let payload = b"payload".to_vec();
    let env = CatalogEnvelope::new(1, 42, 1, payload);

    // Forward upgrade from 1 to 2 succeeds
    let upgraded = env.upgrade_record(2).unwrap();
    assert_eq!(upgraded.record_version, 2);

    // Downgrade from 2 to 1 fails closed with RS-1002
    let downgrade_res = upgraded.upgrade_record(1);
    assert!(downgrade_res.is_err());
    let err = downgrade_res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1002]"),
        "Expected RS-1002 on downgrade, got: {err}"
    );
}

#[test]
fn test_object_id_roundtrip_and_invariance() {
    let payload = b"entity_data".to_vec();
    let object_id: u128 = 0x1234_5678_9abc_def0_1234_5678_9abc_def0;
    let env = CatalogEnvelope::new(1, object_id, 10, payload);
    let bytes = env.encode().unwrap();

    let decoded = CatalogEnvelope::decode(&bytes).unwrap();
    assert_eq!(decoded.object_id, object_id);

    // Zero object_id rejected with RS-1003
    let mut zero_id_env = env.clone();
    zero_id_env.object_id = 0;
    let zero_bytes = serde_json::to_vec(&zero_id_env).unwrap();
    let res = CatalogEnvelope::decode(&zero_bytes);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1003]"),
        "Expected RS-1003 on zero object_id, got: {err}"
    );
}

#[test]
fn test_catalog_revision_monotonicity() {
    let payload = b"data".to_vec();
    let env1 = CatalogEnvelope::new(1, 1, 1, payload.clone());
    let env2 = CatalogEnvelope::new(1, 2, 2, payload.clone());
    assert!(env2.catalog_revision > env1.catalog_revision);
    assert_eq!(env1.catalog_revision, 1);
    assert_eq!(env2.catalog_revision, 2);
}

#[test]
fn test_checksum_corruption_detection() {
    let payload = b"critical metadata".to_vec();
    let env = CatalogEnvelope::new(1, 999, 1, payload);
    let mut bytes = env.encode().unwrap();

    // Valid envelope passes
    assert!(CatalogEnvelope::decode(&bytes).is_ok());

    // Corrupt one byte of payload in serialized JSON
    let len = bytes.len();
    bytes[len - 5] ^= 0xFF;

    let res = CatalogEnvelope::decode(&bytes);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1003]"),
        "Expected RS-1003 on corrupted payload/checksum, got: {err}"
    );
}
