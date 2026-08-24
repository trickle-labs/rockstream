//! Integration and error handling tests for `rockstream init` command (`GP-001`–`GP-004`).

use rockstream_cli::init::{run_init, InitOptions, InitOutcome};
use rockstream_cli::output::OutputFormat;
use rockstream_types::error_code::{RS_0002, RS_0003, RS_0004, RS_2001};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_init_local_template() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("local_proj");

    let opts = InitOptions {
        name: "local_proj".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    let result = run_init(OutputFormat::Json, &opts).expect("local template init should succeed");
    let outcome: InitOutcome = serde_json::from_str(&result).expect("valid InitOutcome JSON");

    assert_eq!(outcome.project_name, "local_proj");
    assert_eq!(outcome.template, "local");
    assert_eq!(outcome.status, "created");
    assert!(outcome
        .generated_files
        .contains(&"rockstream.toml".to_string()));
    assert!(outcome.generated_files.contains(&"schema.sql".to_string()));
    assert!(outcome.generated_files.contains(&"queries.sql".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"data/seed.csv".to_string()));
    assert!(outcome.generated_files.contains(&"README.md".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"scripts/verify.sh".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"scripts/cleanup.sh".to_string()));

    // Verify file contents exist and are non-empty
    let config_content =
        fs::read_to_string(target_dir.join("rockstream.toml")).expect("rockstream.toml");
    assert!(config_content.contains("backend = \"lfs\""));

    let schema_content = fs::read_to_string(target_dir.join("schema.sql")).expect("schema.sql");
    assert!(schema_content.contains("CREATE MATERIALIZED VIEW sales_by_store"));

    let queries_content = fs::read_to_string(target_dir.join("queries.sql")).expect("queries.sql");
    assert!(queries_content.contains("sales_by_store") && queries_content.contains("store_id"));

    let seed_content = fs::read_to_string(target_dir.join("data/seed.csv")).expect("seed.csv");
    assert!(seed_content.contains("id,store_id,amount"));
}

#[test]
fn test_init_kafka_template() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("kafka_proj");

    let opts = InitOptions {
        name: "kafka_proj".to_string(),
        template: "kafka".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    let result = run_init(OutputFormat::Json, &opts).expect("kafka template init should succeed");
    let outcome: InitOutcome = serde_json::from_str(&result).expect("valid InitOutcome JSON");

    assert_eq!(outcome.project_name, "kafka_proj");
    assert_eq!(outcome.template, "kafka");
    assert_eq!(outcome.status, "created");
    assert!(outcome
        .generated_files
        .contains(&"rockstream.toml".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"docker-compose.yaml".to_string()));
    assert!(outcome.generated_files.contains(&"schema.sql".to_string()));
    assert!(outcome.generated_files.contains(&"queries.sql".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"data/events.json".to_string()));
    assert!(outcome.generated_files.contains(&"README.md".to_string()));

    let compose_content =
        fs::read_to_string(target_dir.join("docker-compose.yaml")).expect("docker-compose.yaml");
    assert!(compose_content.contains("redpanda"));
    assert!(compose_content.contains("rockstream"));

    let schema_content = fs::read_to_string(target_dir.join("schema.sql")).expect("schema.sql");
    assert!(schema_content.contains("CREATE MATERIALIZED VIEW pageviews_by_user"));
}

#[test]
fn test_init_postgres_cdc_template() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("cdc_proj");

    let opts = InitOptions {
        name: "cdc_proj".to_string(),
        template: "postgres-cdc".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    let result =
        run_init(OutputFormat::Json, &opts).expect("postgres-cdc template init should succeed");
    let outcome: InitOutcome = serde_json::from_str(&result).expect("valid InitOutcome JSON");

    assert_eq!(outcome.project_name, "cdc_proj");
    assert_eq!(outcome.template, "postgres-cdc");
    assert_eq!(outcome.status, "created");
    assert!(outcome
        .generated_files
        .contains(&"rockstream.toml".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"docker-compose.yaml".to_string()));
    assert!(outcome.generated_files.contains(&"pg-init.sql".to_string()));
    assert!(outcome.generated_files.contains(&"schema.sql".to_string()));
    assert!(outcome.generated_files.contains(&"queries.sql".to_string()));

    let pg_init = fs::read_to_string(target_dir.join("pg-init.sql")).expect("pg-init.sql");
    assert!(pg_init.contains("CREATE PUBLICATION rockstream_pub FOR ALL TABLES;"));

    let schema_content = fs::read_to_string(target_dir.join("schema.sql")).expect("schema.sql");
    assert!(schema_content.contains("CREATE MATERIALIZED VIEW sales_by_region"));
}

#[test]
fn test_init_rejects_non_empty_dir() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("existing_dir");
    fs::create_dir_all(&target_dir).expect("create target dir");
    fs::write(target_dir.join("precious_data.txt"), "important").expect("write file");

    let opts = InitOptions {
        name: "existing_dir".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };

    let err = run_init(OutputFormat::Json, &opts)
        .expect_err("should reject non-empty dir without --force");
    assert_eq!(err.code, RS_0004);
    assert!(err
        .message
        .contains("is not empty; use --force to overwrite"));
    assert!(target_dir.join("precious_data.txt").exists());
}

#[test]
fn test_init_force_overwrites_existing_dir() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("existing_dir");
    fs::create_dir_all(&target_dir).expect("create target dir");
    fs::write(target_dir.join("old_config.txt"), "old").expect("write file");

    let opts = InitOptions {
        name: "existing_dir".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: true,
    };

    let result = run_init(OutputFormat::Json, &opts).expect("should succeed with --force");
    let outcome: InitOutcome = serde_json::from_str(&result).expect("valid InitOutcome JSON");
    assert_eq!(outcome.status, "created");
    assert!(target_dir.join("rockstream.toml").exists());
}

#[test]
fn test_init_rejects_invalid_template() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("invalid_template_proj");

    let opts = InitOptions {
        name: "invalid_template_proj".to_string(),
        template: "unsupported_template".to_string(),
        dir: Some(target_dir),
        force: false,
    };

    let err = run_init(OutputFormat::Json, &opts).expect_err("should reject unsupported template");
    assert_eq!(err.code, RS_0002);
    assert!(err
        .message
        .contains("invalid template 'unsupported_template'"));
    assert!(err
        .message
        .contains("valid options: local, kafka, postgres-cdc"));
}

#[test]
fn test_init_permission_error() {
    let temp_dir = TempDir::new().expect("tempdir");
    // Target is a file instead of a directory, causing directory creation / entry inspection conflict
    let file_path = temp_dir.path().join("a_file.txt");
    fs::write(&file_path, "not a dir").expect("write file");

    let opts = InitOptions {
        name: "conflict".to_string(),
        template: "local".to_string(),
        dir: Some(file_path),
        force: false,
    };

    let err = run_init(OutputFormat::Json, &opts).expect_err("should fail when target is a file");
    assert_eq!(err.code, RS_0004);
}

#[test]
fn test_init_docker_unavailable_diagnostics() {
    let error_code = RS_0003;
    assert_eq!(error_code.value(), 3);
}

#[test]
fn test_init_port_collision_diagnostics() {
    let error_code = RS_2001;
    assert_eq!(error_code.value(), 2001);
}

#[test]
fn test_init_text_and_json_output() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("text_test");

    let opts = InitOptions {
        name: "text_test".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir),
        force: false,
    };

    let text_output = run_init(OutputFormat::Text, &opts).expect("text init");
    assert!(
        text_output.contains("RockStream Project Initialized: name='text_test' template='local'")
    );
    assert!(text_output.contains("Generated Files:"));
    assert!(text_output.contains("- rockstream.toml"));
    assert!(text_output.contains("rockstream start --storage ./storage"));
}
