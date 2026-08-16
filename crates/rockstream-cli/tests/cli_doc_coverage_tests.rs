//! CLI documentation coverage and conformance tests (v0.53.1 Slice 10).

use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_docs_cli_conformance_and_coverage() {
    let root = workspace_root();
    let doc_path = root.join("docs/cli.md");
    let doc_content = fs::read_to_string(&doc_path)
        .unwrap_or_else(|_| panic!("docs/cli.md must exist at {}", doc_path.display()));

    // Required subcommands that must be documented
    let required_subcommands = [
        "rockstream start",
        "rockstream view",
        "rockstream view list",
        "rockstream view show",
        "rockstream view status",
        "rockstream view pause",
        "rockstream view resume",
        "rockstream view query",
        "rockstream view subscribe",
        "rockstream source",
        "rockstream source list",
        "rockstream source show",
        "rockstream source pause",
        "rockstream source resume",
        "rockstream source drop",
        "rockstream schema",
        "rockstream schema list",
        "rockstream schema show",
        "rockstream schema create",
        "rockstream schema drop",
        "rockstream workload",
        "rockstream workload list",
        "rockstream workload show",
        "rockstream workload create",
        "rockstream workload alter",
        "rockstream workload drop",
        "rockstream cluster",
        "rockstream cluster status",
        "rockstream cluster quotas",
        "rockstream cluster workers",
        "rockstream cluster workers drain",
        "rockstream shard",
        "rockstream shard list",
        "rockstream shard migrate",
        "rockstream checkpoint",
        "rockstream checkpoint list",
        "rockstream checkpoint export",
        "rockstream checkpoint restore",
        "rockstream support",
        "rockstream support bundle",
        "rockstream resource",
        "rockstream resource usage",
        "rockstream resource cluster",
        "rockstream schema-evolution",
        "rockstream schema-evolution status",
        "rockstream schema-evolution history",
        "rockstream audit",
        "rockstream audit tail",
        "rockstream audit query",
        "rockstream explain",
        "rockstream sql",
        "rockstream debug",
        "rockstream debug arrangement",
    ];

    for subcmd in &required_subcommands {
        assert!(
            doc_content.contains(subcmd),
            "docs/cli.md must document `{subcmd}`"
        );
    }

    // Required error codes documented
    let required_codes = [
        "RS-0002", "RS-0003", "RS-0004", "RS-0005", "RS-1001", "RS-1004", "RS-1005", "RS-1006",
        "RS-1007", "RS-1008", "RS-1012", "RS-1014", "RS-1020", "RS-1021", "RS-1731", "RS-2006",
        "RS-2401", "RS-4009", "RS-5030", "RS-5035",
    ];

    for code in &required_codes {
        assert!(
            doc_content.contains(code),
            "docs/cli.md must document error code `{code}`"
        );
    }

    // Global --json flag documented
    assert!(
        doc_content.contains("--json"),
        "docs/cli.md must document `--json` flag"
    );

    // Verify docs/sre-operations.md documents `rockstream support bundle`
    let sre_doc_path = root.join("docs/sre-operations.md");
    let sre_content = fs::read_to_string(&sre_doc_path).unwrap_or_else(|_| {
        panic!(
            "docs/sre-operations.md must exist at {}",
            sre_doc_path.display()
        )
    });
    assert!(
        sre_content.contains("rockstream support bundle"),
        "docs/sre-operations.md must document `rockstream support bundle`"
    );

    // Verify docs/metrics.md documents all stage lag and barrier flight metrics
    let metrics_doc_path = root.join("docs/metrics.md");
    let metrics_content = fs::read_to_string(&metrics_doc_path).unwrap_or_else(|_| {
        panic!(
            "docs/metrics.md must exist at {}",
            metrics_doc_path.display()
        )
    });
    let required_metrics = [
        "view_freshness_lag_source_ms",
        "view_freshness_lag_decode_ms",
        "view_freshness_lag_compute_ms",
        "view_freshness_lag_checkpoint_alignment_ms",
        "view_freshness_lag_sink_commit_ms",
        "view_freshness_lag_spill_ms",
        "view_freshness_lag_storage_pressure_ms",
        "view_freshness_lag_end_to_end_ms",
        "checkpoint_barrier_flight_time_ms",
        "checkpoint_completion_time_ms",
    ];
    for m in &required_metrics {
        assert!(
            metrics_content.contains(m),
            "docs/metrics.md must document metric `{m}`"
        );
    }
}

#[test]
fn test_cli_documentation_covers_checkpoint_export_and_restore() {
    let docs = fs::read_to_string(workspace_root().join("docs/cli.md")).unwrap();
    assert!(docs.contains("rockstream checkpoint export --destination <file://...|s3://...>"));
    assert!(docs.contains(
        "rockstream checkpoint restore --source <file://...|s3://...> --storage <file://...|s3://...> --yes"
    ));
    assert!(docs.contains("RS-5035"));
}
