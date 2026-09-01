use rockstream_docgen::{generate_manifest, render_reference_docs};
use std::{fs, path::Path};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
}

#[test]
fn generated_references_match_exact_checked_in_markdown() {
    let generated = render_reference_docs(&generate_manifest().unwrap());
    for (name, expected) in generated {
        let path = repo_root().join("docs/reference").join(name);
        assert_eq!(expected, fs::read_to_string(path).unwrap());
    }
}

#[test]
fn each_manifest_surface_has_one_reference_section() {
    let generated = render_reference_docs(&generate_manifest().unwrap());
    assert_eq!(
        generated.keys().collect::<Vec<_>>(),
        vec![
            "catalog.md",
            "cli.md",
            "configuration.md",
            "errors.md",
            "functions.md",
            "limits.md",
            "metrics.md",
            "sql-semantics.md",
            "sql-support.md",
            "sql-type-matrix.md",
        ]
    );
    for markdown in generated.values() {
        assert_eq!(
            markdown
                .lines()
                .filter(|line| line.starts_with("# "))
                .count(),
            1
        );
    }
}
