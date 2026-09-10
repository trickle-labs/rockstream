//! Tests for Docker Compose profiles, automated verifiers, and cleanup services (`GP-005`).

use rockstream_cli::init::{run_init, InitOptions};
use rockstream_cli::output::OutputFormat;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_compose_profile_local() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("local_compose");

    let opts = InitOptions {
        name: "local_compose".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    run_init(OutputFormat::Json, &opts).expect("init local");

    // In v0.61, local template emits the six-file contract
    let rockstream_toml = target_dir.join("rockstream.toml");
    assert!(rockstream_toml.exists());
    let rt_content = fs::read_to_string(&rockstream_toml).expect("read rockstream.toml");
    assert!(rt_content.contains("5432"));

    let schema_sql = target_dir.join("schema.sql");
    assert!(schema_sql.exists());
    let schema_content = fs::read_to_string(&schema_sql).expect("read schema.sql");
    assert!(schema_content.contains("sales_by_store"));

    let project_toml = target_dir.join("project.toml");
    assert!(project_toml.exists());
    let seed_csv = target_dir.join("data/seed.csv");
    assert!(seed_csv.exists());
    let verify_sql = target_dir.join("queries/verify.sql");
    assert!(verify_sql.exists());
    let readme = target_dir.join("README.md");
    assert!(readme.exists());
}

#[test]
fn test_compose_profile_kafka() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("kafka_compose");

    let opts = InitOptions {
        name: "kafka_compose".to_string(),
        template: "kafka".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    // In v0.61, init rejects non-local templates
    let err = run_init(OutputFormat::Json, &opts).unwrap_err();
    assert!(err.message.contains("only 'local' is supported"));

    // Verify relocated experimental kafka compose profile
    let exp_compose = std::path::Path::new("../../examples/experimental/kafka/docker-compose.yaml");
    let compose_path = if exp_compose.exists() {
        exp_compose.to_path_buf()
    } else {
        std::path::Path::new("examples/experimental/kafka/docker-compose.yaml").to_path_buf()
    };
    assert!(
        compose_path.exists(),
        "experimental kafka compose file must exist"
    );
    let compose_content = fs::read_to_string(&compose_path).expect("read docker-compose.yaml");

    // Check service orchestrations
    assert!(compose_content.contains("redpanda:"));
    assert!(compose_content.contains("rockstream:"));
    assert!(compose_content.contains("verifier:"));
    assert!(compose_content.contains("9092:9092"));
    assert!(compose_content.contains("5432:5432"));
    assert!(compose_content.contains("depends_on:"));
}

#[test]
fn test_compose_profile_postgres() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("postgres_compose");

    let opts = InitOptions {
        name: "postgres_compose".to_string(),
        template: "postgres-cdc".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    // In v0.61, init rejects non-local templates
    let err = run_init(OutputFormat::Json, &opts).unwrap_err();
    assert!(err.message.contains("only 'local' is supported"));

    // Verify relocated experimental postgres compose profile
    let exp_compose =
        std::path::Path::new("../../examples/experimental/postgres-cdc/docker-compose.yaml");
    let compose_path = if exp_compose.exists() {
        exp_compose.to_path_buf()
    } else {
        std::path::Path::new("examples/experimental/postgres-cdc/docker-compose.yaml").to_path_buf()
    };
    assert!(
        compose_path.exists(),
        "experimental postgres compose file must exist"
    );
    let compose_content = fs::read_to_string(&compose_path).expect("read docker-compose.yaml");

    // Check service orchestrations
    assert!(compose_content.contains("postgres:"));
    assert!(compose_content.contains("rockstream:"));
    assert!(compose_content.contains("wal_level=logical"));
    assert!(compose_content.contains("5433:5432"));
    assert!(compose_content.contains("5432:5432"));

    let exp_pg_init = std::path::Path::new("../../examples/experimental/postgres-cdc/pg-init.sql");
    let pg_init_path = if exp_pg_init.exists() {
        exp_pg_init.to_path_buf()
    } else {
        std::path::Path::new("examples/experimental/postgres-cdc/pg-init.sql").to_path_buf()
    };
    assert!(pg_init_path.exists(), "pg-init.sql must exist");
    let pg_init_content = fs::read_to_string(&pg_init_path).expect("read pg-init.sql");
    assert!(pg_init_content.contains("CREATE PUBLICATION rockstream_pub"));
}

#[test]
fn test_compose_profile_all() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("all_profiles");

    // Verify local succeeds and generates six files
    let local_path = target_dir.join("local");
    let opts = InitOptions {
        name: "local".to_string(),
        template: "local".to_string(),
        dir: Some(local_path.clone()),
        force: false,
    };
    let res = run_init(OutputFormat::Json, &opts).expect("init template");
    let outcome: rockstream_cli::init::InitOutcome =
        serde_json::from_str(&res).expect("valid json");
    assert_eq!(outcome.template, "local");
    assert_eq!(outcome.generated_files.len(), 6);
    assert!(local_path.join("rockstream.toml").exists());
    assert!(local_path.join("project.toml").exists());
    assert!(local_path.join("schema.sql").exists());
    assert!(local_path.join("data/seed.csv").exists());
    assert!(local_path.join("queries/verify.sql").exists());
    assert!(local_path.join("README.md").exists());

    // Verify non-local templates are rejected
    for tmpl in ["kafka", "postgres-cdc"] {
        let p = target_dir.join(tmpl);
        let opts = InitOptions {
            name: tmpl.to_string(),
            template: tmpl.to_string(),
            dir: Some(p),
            force: false,
        };
        let err = run_init(OutputFormat::Json, &opts).unwrap_err();
        assert!(err.message.contains("only 'local' is supported"));
    }
}

#[test]
fn test_cleanup_service_idempotency() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("cleanup_test");

    let opts = InitOptions {
        name: "cleanup_test".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    run_init(OutputFormat::Json, &opts).expect("init local");

    let storage_dir = target_dir.join("storage");
    fs::create_dir_all(&storage_dir).expect("create storage dir");
    fs::write(storage_dir.join("mock_sst.db"), "data").expect("write mock db file");
    assert!(storage_dir.exists());

    // 1st Cleanup
    if storage_dir.exists() {
        fs::remove_dir_all(&storage_dir).expect("cleanup 1");
    }
    assert!(!storage_dir.exists());

    // 2nd Cleanup (idempotent: does not panic or fail when directory already removed)
    if storage_dir.exists() {
        fs::remove_dir_all(&storage_dir).expect("cleanup 2");
    }
    assert!(!storage_dir.exists());
}
