#[test]
fn migration_uses_scan_copy_verify_and_point_delete_only() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(root.join("src/format_migration.rs")).unwrap();
    assert!(!source.contains(".range_delete("), "{source}");
    assert!(!source.contains("delete_range("), "{source}");
    assert!(source.contains("raw.delete(&entry.key)"), "{source}");
    assert!(source.contains("raw.get(&new_key)"), "{source}");
}
