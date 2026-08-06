//! Scoped Clippy Lint Gate Test.
//!
//! Asserts that a reintroduced `.unwrap()` or `.expect()` in any of the four audited
//! untrusted-input modules (`server.rs`, `frontend.rs`, `postgres_cdc.rs`, `webhook_source.rs`)
//! is denied by clippy lints.

#[test]
fn deliberate_unwrap_reintroduction_fails_clippy_lint() {
    // Assert that all four audited files contain `#![deny(clippy::unwrap_used, clippy::expect_used)]`
    let audited_files = [
        "crates/rockstream-gateway/src/server.rs",
        "crates/rockstream-sql/src/frontend.rs",
        "crates/rockstream-connectors/src/postgres_cdc.rs",
        "crates/rockstream-gateway/src/webhook_source.rs",
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
