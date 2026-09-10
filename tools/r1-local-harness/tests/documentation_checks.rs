use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_r1_local_readme_documents_v0611_baseline_contract() {
    let root = repo_root();
    let readme_path = root.join("benchmarks/r1-local/README.md");
    assert!(
        readme_path.is_file(),
        "benchmarks/r1-local/README.md must exist"
    );

    let content = fs::read_to_string(&readme_path).expect("read benchmarks/r1-local/README.md");

    // Must document target definitions and separate p99s
    assert!(
        content.contains("read_p99_ms"),
        "README must document read_p99_ms target"
    );
    assert!(
        content.contains("commit_p99_ms"),
        "README must document commit_p99_ms target"
    );
    assert!(
        content.contains("freshness_p99_ms"),
        "README must document freshness_p99_ms target"
    );

    // Must document oracle multiset alignment
    assert!(
        content.contains("oracle") || content.contains("Oracle"),
        "README must document oracle alignment"
    );
    assert!(
        content.contains("multiset") || content.contains("Multiset"),
        "README must document multiset comparison"
    );

    // Must document matrix cells and later milestone owners (v0.67, v0.67.1, v0.68)
    assert!(
        content.contains("v0.67"),
        "README must reference v0.67 milestone"
    );
    assert!(
        content.contains("v0.67.1"),
        "README must reference v0.67.1 milestone"
    );
    assert!(
        content.contains("v0.68"),
        "README must reference v0.68 milestone"
    );

    // Must distinguish local developer scope from cloud production pricing
    assert!(
        content.contains("local") || content.contains("Local"),
        "README must document local developer scope"
    );
    assert!(
        content.contains("cost_per_million") || content.contains("Cost"),
        "README must document cost model"
    );
}
