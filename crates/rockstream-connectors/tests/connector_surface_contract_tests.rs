use std::fs;
use std::path::{Path, PathBuf};

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
