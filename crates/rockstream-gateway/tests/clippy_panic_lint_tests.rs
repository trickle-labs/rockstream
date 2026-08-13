//! Scoped Clippy Lint Gate Test.
//!
//! Asserts that a reintroduced `.unwrap()` or `.expect()` in an audited
//! untrusted-input module is denied by clippy lints.
//! is denied by clippy lints.

#[test]
fn deliberate_unwrap_reintroduction_fails_clippy_lint() {
    // Assert that every audited file contains the deny annotation.
    let audited_files = [
        "crates/rockstream-gateway/src/server.rs",
        "crates/rockstream-sql/src/frontend.rs",
        "crates/rockstream-connectors/src/postgres_cdc.rs",
    ];

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let root = std::path::Path::new(&manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    for relative_path in audited_files {
        let path = root.join(relative_path);
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

        assert!(
            content.contains("#![deny(clippy::unwrap_used, clippy::expect_used)]"),
            "Module {} must retain #![deny(clippy::unwrap_used, clippy::expect_used)] lint annotation",
            path.display()
        );
    }
}
