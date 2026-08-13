use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rockstream_types::error_code::{
    RS_4001, RS_4002, RS_4003, RS_4004, RS_4005, RS_4006, RS_4007, RS_4008, RS_4009, RS_4010,
    RS_4011, RS_4012, RS_4013, RS_4014, RS_4015, RS_4016, RS_4017, RS_4018, RS_4019, RS_4020,
    RS_4021, RS_4022,
};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn connector_source_file_set_is_exact() {
    let mut actual = fs::read_dir(root().join("crates/rockstream-connectors/src"))
        .expect("connector sources")
        .map(|entry| {
            entry
                .expect("source entry")
                .file_name()
                .into_string()
                .expect("utf-8 name")
        })
        .collect::<Vec<_>>();
    actual.sort();
    assert_eq!(
        actual,
        [
            "fault_injecting_store.rs",
            "kafka_sink.rs",
            "kafka_source.rs",
            "lib.rs",
            "postgres_cdc.rs",
            "sink_connector.rs",
            "source_connector.rs",
            "source_epoch.rs",
            "source_json.rs",
            "source_runtime.rs"
        ]
        .map(str::to_owned)
        .to_vec()
    );
}

#[test]
fn retained_connector_exports_are_exact() {
    let source = fs::read_to_string(root().join("crates/rockstream-connectors/src/lib.rs"))
        .expect("connector lib");
    let modules = source
        .lines()
        .filter_map(|line| {
            line.strip_prefix("pub mod ")
                .and_then(|name| name.strip_suffix(';'))
        })
        .collect::<Vec<_>>();
    let exports = source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub use "))
        .filter_map(|line| line.split("::").next())
        .collect::<Vec<_>>();
    assert_eq!(
        modules,
        vec![
            "fault_injecting_store",
            "kafka_sink",
            "kafka_source",
            "postgres_cdc",
            "sink_connector",
            "source_connector",
            "source_epoch",
            "source_runtime"
        ]
    );
    assert_eq!(
        exports,
        vec![
            "fault_injecting_store",
            "kafka_sink",
            "kafka_source",
            "postgres_cdc",
            "sink_connector",
            "source_connector",
            "source_epoch",
            "source_runtime"
        ]
    );
}

#[test]
fn gateway_webhook_runtime_is_absent() {
    let gateway = root().join("crates/rockstream-gateway/src");
    assert!(!gateway.join("webhook_source.rs").exists());
    let server = fs::read_to_string(gateway.join("server.rs")).expect("gateway server");
    for symbol in [
        "WebhookResult",
        "HttpWebhookSource",
        "webhook_sources",
        "accept_webhook",
    ] {
        assert!(
            !server.contains(symbol),
            "removed runtime symbol remains: {symbol}"
        );
    }
    assert!(server.contains(r"HTTP/1.1 410 Gone\r\n"));
    assert!(server.contains("[RS-4017] connector.removed: HTTP/webhook sources have been removed."));
}

#[test]
fn removed_connector_test_inventory_is_exact() {
    let root = root();
    for file in [
        "crates/rockstream-connectors/tests/s3_source_ingestion_tests.rs",
        "crates/rockstream-connectors/tests/object_store_sink_real_bucket_tests.rs",
        "crates/rockstream-connectors/tests/iceberg_sink_recovery_tests.rs",
        "crates/rockstream-connectors/tests/deltalake_sink_recovery_tests.rs",
        "crates/rockstream-connectors/tests/cold_gc_safety_tests.rs",
        "crates/rockstream-connectors/tests/partial_write_recovery_tests.rs",
        "crates/rockstream-connectors/tests/async_hygiene_tests.rs",
        "crates/rockstream-connectors/tests/backfill_resume_tests.rs",
        "crates/rockstream-gateway/tests/source_ddl_postgres_webhook_tests.rs",
        "crates/rockstream-gateway/tests/webhook_ttl_tests.rs",
        "fuzz/fuzz_targets/fuzz_webhook_body.rs",
    ] {
        assert!(
            !root.join(file).exists(),
            "removed-only test remains: {file}"
        );
    }
}

#[test]
fn live_tree_has_no_removed_connector_references() {
    let root = root();
    for path in ["crates", "docs", "scripts", "formal"] {
        for needle in [
            "IcebergSink",
            "DeltaSink",
            "ObjectStoreSink",
            "HttpWebhookSource",
            "webhook_sources",
            "build_s3_source",
            "backfill_s3_source",
            "spawn_s3_source_worker",
            "run_s3_source_worker",
        ] {
            assert_no_reference(&root.join(path), needle);
        }
    }
}

#[test]
fn connector_guarantee_document_is_exact_and_all_matrix_tests_exist() {
    let root = root();
    let document =
        fs::read_to_string(root.join("docs/connectors.md")).expect("connector guarantee document");
    for required in [
        "# Connector guarantees",
        "## PostgreSQL CDC",
        "## Kafka source",
        "## Kafka sink",
        "Delivery / recovery",
        "Bound / fill metric / backpressure",
        "Degraded states",
        "Failure codes",
        "Proof matrix",
        "RS-4017",
        "connector-migration.md",
        "No code path depends on SlateDB range",
        "bounded scan followed by point deletes",
    ] {
        assert!(
            document.contains(required),
            "missing document contract: {required}"
        );
    }

    let expected_codes = [
        RS_4001, RS_4002, RS_4003, RS_4004, RS_4005, RS_4006, RS_4007, RS_4008, RS_4009, RS_4010,
        RS_4011, RS_4012, RS_4013, RS_4014, RS_4015, RS_4016, RS_4017, RS_4018, RS_4019, RS_4020,
        RS_4021, RS_4022,
    ]
    .into_iter()
    .map(|code| code.to_string())
    .collect::<BTreeSet<_>>();
    let documented_codes = document
        .split('`')
        .filter(|token| token.starts_with("RS-"))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(documented_codes, expected_codes);

    let matrix = [
        (
            "crates/rockstream-connectors/tests/postgres_cdc_guarantee_matrix_tests.rs",
            &[
                "postgres_cdc_snapshot_stream_fence_has_exact_transcript",
                "postgres_cdc_all_mutation_types_have_exact_transcript",
                "postgres_cdc_each_commit_boundary_recovers_exactly_once",
                "postgres_cdc_wal_lag_pauses_at_bound_and_recovers_within_slo",
                "postgres_cdc_malformed_replication_record_fails_closed_then_recovers_exactly",
                "postgres_cdc_replication_slot_loss_resnapshots_with_exact_transcript",
                "postgres_cdc_publication_loss_fails_clearly_then_recovers_exactly",
                "postgres_cdc_backpressure_never_exceeds_record_or_byte_bound",
                "postgres_cdc_long_running_recovery_is_exact_and_within_slo",
            ][..],
        ),
        (
            "crates/rockstream-connectors/tests/kafka_source_guarantee_matrix_tests.rs",
            &[
                "kafka_source_mid_epoch_rebalance_recovers_exact_transcript",
                "kafka_source_partition_expansion_has_exact_transcript",
                "kafka_source_committed_offset_recovery_has_exact_transcript",
                "kafka_source_broker_interruption_recovers_exactly_within_slo",
                "kafka_source_buffer_bound_and_fill_level_are_exact",
                "kafka_source_duplicate_redelivery_has_exactly_one_transcript",
                "kafka_source_sink_transaction_coupling_has_exact_transcript",
            ][..],
        ),
        (
            "crates/rockstream-connectors/tests/kafka_sink_guarantee_matrix_tests.rs",
            &[
                "kafka_sink_crash_before_commit_has_no_visible_payload_and_recovers_exactly",
                "kafka_sink_crash_during_commit_recovers_exactly_once_within_slo",
                "kafka_sink_uncertain_broker_response_recovers_exactly_once_within_slo",
                "kafka_sink_transaction_timeout_recovers_exactly_once_within_slo",
                "kafka_sink_recovery_rerun_has_exactly_one_payload_per_epoch",
                "kafka_sink_duplicate_commit_has_exactly_one_payload_per_epoch",
                "kafka_sink_checkpoint_coupling_has_exact_commit_transcript",
            ][..],
        ),
    ];
    for (path, tests) in matrix {
        let source = fs::read_to_string(root.join(path)).expect("matrix test target");
        for test in tests {
            assert!(
                source.contains(&format!("fn {test}")),
                "missing matrix test {path}::{test}"
            );
            assert!(
                document.contains(test),
                "matrix test is not documented: {test}"
            );
        }
    }
    for test in [
        "retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_lfs",
        "retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_minio",
        "backfill_cleanup_uses_bounded_scan_and_point_delete",
    ] {
        assert!(
            document.contains(test),
            "missing durability or cleanup proof: {test}"
        );
    }
}

fn assert_no_reference(path: &Path, needle: &str) {
    for entry in fs::read_dir(path).expect("tree entry") {
        let path = entry.expect("tree path").path();
        if path.is_dir() {
            assert_no_reference(&path, needle);
        } else if path.ends_with("connector_surface_contract_tests.rs") {
            continue;
        } else if let Ok(contents) = fs::read_to_string(&path) {
            assert!(
                !contents.contains(needle),
                "{} references {needle}",
                path.display()
            );
        }
    }
}
