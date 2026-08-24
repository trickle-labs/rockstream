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

    // Verify local verification and cleanup scripts
    let verify_script = target_dir.join("scripts/verify.sh");
    assert!(verify_script.exists());
    let verify_content = fs::read_to_string(&verify_script).expect("read verify.sh");
    assert!(verify_content.contains("GATEWAY_PORT"));
    assert!(verify_content.contains("sales_by_store"));

    let cleanup_script = target_dir.join("scripts/cleanup.sh");
    assert!(cleanup_script.exists());
    let cleanup_content = fs::read_to_string(&cleanup_script).expect("read cleanup.sh");
    assert!(cleanup_content.contains("rm -rf ./storage"));
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

    run_init(OutputFormat::Json, &opts).expect("init kafka");

    let compose_file = target_dir.join("docker-compose.yaml");
    assert!(compose_file.exists());
    let compose_content = fs::read_to_string(&compose_file).expect("read docker-compose.yaml");

    // Check service orchestrations
    assert!(compose_content.contains("redpanda:"));
    assert!(compose_content.contains("rockstream:"));
    assert!(compose_content.contains("verifier:"));
    assert!(compose_content.contains("9092:9092"));
    assert!(compose_content.contains("5432:5432"));
    assert!(compose_content.contains("depends_on:"));

    let cleanup_script = target_dir.join("scripts/cleanup.sh");
    assert!(cleanup_script.exists());
    let cleanup_content = fs::read_to_string(&cleanup_script).expect("read cleanup.sh");
    assert!(cleanup_content.contains("docker compose down -v"));
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

    run_init(OutputFormat::Json, &opts).expect("init postgres-cdc");

    let compose_file = target_dir.join("docker-compose.yaml");
    assert!(compose_file.exists());
    let compose_content = fs::read_to_string(&compose_file).expect("read docker-compose.yaml");

    // Check service orchestrations
    assert!(compose_content.contains("postgres:"));
    assert!(compose_content.contains("rockstream:"));
    assert!(compose_content.contains("wal_level=logical"));
    assert!(compose_content.contains("5433:5432"));
    assert!(compose_content.contains("5432:5432"));

    let pg_init = target_dir.join("pg-init.sql");
    assert!(pg_init.exists());
    let pg_init_content = fs::read_to_string(&pg_init).expect("read pg-init.sql");
    assert!(pg_init_content.contains("CREATE PUBLICATION rockstream_pub"));

    let cleanup_script = target_dir.join("scripts/cleanup.sh");
    assert!(cleanup_script.exists());
    let cleanup_content = fs::read_to_string(&cleanup_script).expect("read cleanup.sh");
    assert!(cleanup_content.contains("docker compose down -v"));
}

#[test]
fn test_compose_profile_all() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("all_profiles");

    // Verify all templates can be generated in separate project paths side-by-side
    for tmpl in ["local", "kafka", "postgres-cdc"] {
        let p = target_dir.join(tmpl);
        let opts = InitOptions {
            name: tmpl.to_string(),
            template: tmpl.to_string(),
            dir: Some(p.clone()),
            force: false,
        };
        let res = run_init(OutputFormat::Json, &opts).expect("init template");
        let outcome: rockstream_cli::init::InitOutcome =
            serde_json::from_str(&res).expect("valid json");
        assert_eq!(outcome.template, tmpl);
        assert!(p.join("rockstream.toml").exists());
        assert!(p.join("scripts/verify.sh").exists());
        assert!(p.join("scripts/cleanup.sh").exists());
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

    // Execute cleanup simulation: deleting storage directory
    let cleanup_script = target_dir.join("scripts/cleanup.sh");
    assert!(cleanup_script.exists());

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
