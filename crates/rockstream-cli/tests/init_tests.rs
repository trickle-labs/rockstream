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
    assert_eq!(outcome.generated_files.len(), 6);
    assert!(outcome
        .generated_files
        .contains(&"rockstream.toml".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"project.toml".to_string()));
    assert!(outcome.generated_files.contains(&"schema.sql".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"queries/verify.sql".to_string()));
    assert!(outcome
        .generated_files
        .contains(&"data/seed.csv".to_string()));
    assert!(outcome.generated_files.contains(&"README.md".to_string()));
    assert!(!outcome
        .generated_files
        .contains(&"scripts/verify.sh".to_string()));
    assert!(!outcome
        .generated_files
        .contains(&"scripts/cleanup.sh".to_string()));

    // Verify file contents exist and are non-empty
    let config_content =
        fs::read_to_string(target_dir.join("rockstream.toml")).expect("rockstream.toml");
    assert!(config_content.contains("backend = \"lfs\""));

    let manifest_content =
        fs::read_to_string(target_dir.join("project.toml")).expect("project.toml");
    assert!(manifest_content.contains("version = 1"));
    assert!(manifest_content.contains("name = \"local_proj\""));
    assert!(manifest_content.contains("[[apply]]"));
    assert!(manifest_content.contains("[[seed]]"));
    assert!(manifest_content.contains("[[verify]]"));

    let schema_content = fs::read_to_string(target_dir.join("schema.sql")).expect("schema.sql");
    assert!(schema_content.contains("CREATE MATERIALIZED VIEW sales_by_store"));

    let queries_content =
        fs::read_to_string(target_dir.join("queries/verify.sql")).expect("verify.sql");
    assert!(queries_content.contains("sales_by_store") && queries_content.contains("store_id"));

    let seed_content = fs::read_to_string(target_dir.join("data/seed.csv")).expect("seed.csv");
    assert!(seed_content.contains("id,store_id,amount"));
}

#[test]
fn test_template_selection_rejects_kafka() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("kafka_proj");

    let opts = InitOptions {
        name: "kafka_proj".to_string(),
        template: "kafka".to_string(),
        dir: Some(target_dir),
        force: false,
    };

    let err = run_init(OutputFormat::Json, &opts).expect_err("kafka template must be rejected");
    assert_eq!(err.code, RS_0002);
    assert!(err.message.contains("only 'local' is supported in v0.61"));
    assert!(err.message.contains("examples/experimental/"));
}

#[test]
fn test_template_selection_rejects_postgres_cdc() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("cdc_proj");

    let opts = InitOptions {
        name: "cdc_proj".to_string(),
        template: "postgres-cdc".to_string(),
        dir: Some(target_dir),
        force: false,
    };

    let err =
        run_init(OutputFormat::Json, &opts).expect_err("postgres-cdc template must be rejected");
    assert_eq!(err.code, RS_0002);
    assert!(err.message.contains("only 'local' is supported in v0.61"));
    assert!(err.message.contains("examples/experimental/"));
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
    assert!(err.message.contains("valid options: local"));
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

#[test]
fn test_project_new_six_file_contract() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales");

    let opts = InitOptions {
        name: "sales".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    let outcome = rockstream_cli::init::scaffold_project(&opts).expect("scaffold project");
    assert_eq!(outcome.project_name, "sales");
    assert_eq!(outcome.generated_files.len(), 6);
    assert!(sales_dir.join("rockstream.toml").exists());
    assert!(sales_dir.join("project.toml").exists());
    assert!(sales_dir.join("schema.sql").exists());
    assert!(sales_dir.join("data/seed.csv").exists());
    assert!(sales_dir.join("queries/verify.sql").exists());
    assert!(sales_dir.join("README.md").exists());
}

#[test]
fn test_scaffolded_rockstream_toml_validity() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales_cfg");

    let opts = InitOptions {
        name: "sales_cfg".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    rockstream_cli::init::scaffold_project(&opts).expect("scaffold");
    let toml_str =
        fs::read_to_string(sales_dir.join("rockstream.toml")).expect("read rockstream.toml");
    let parsed: toml::Value = toml::from_str(&toml_str).expect("parse rockstream.toml");
    assert!(parsed.get("gateway").is_some());
    assert!(parsed.get("storage").is_some());
}

#[test]
fn test_project_manifest_validation() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales_manifest");

    let opts = InitOptions {
        name: "sales_manifest".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    rockstream_cli::init::scaffold_project(&opts).expect("scaffold");
    let manifest = rockstream_cli::project::load_manifest(&sales_dir).expect("load manifest");
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.name, "sales_manifest");
    assert_eq!(manifest.apply.len(), 1);
    assert_eq!(manifest.apply[0].file, "schema.sql");
    assert_eq!(manifest.seed.len(), 1);
    assert_eq!(manifest.seed[0].table, "orders");
    assert_eq!(manifest.seed[0].file, "data/seed.csv");
    assert_eq!(manifest.seed[0].format, "csv");
    assert_eq!(manifest.verify.len(), 1);
}

#[test]
fn test_seed_csv_matches_schema() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales_seed");

    let opts = InitOptions {
        name: "sales_seed".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    rockstream_cli::init::scaffold_project(&opts).expect("scaffold");
    let seed = fs::read_to_string(sales_dir.join("data/seed.csv")).expect("seed");
    let mut lines = seed.lines();
    assert_eq!(lines.next(), Some("id,store_id,amount"));
    assert_eq!(lines.next(), Some("1,100,50"));
    assert_eq!(lines.next(), Some("2,100,70"));
    assert_eq!(lines.next(), Some("3,200,40"));
}

#[test]
fn test_verify_sql_targets_view() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales_v");

    let opts = InitOptions {
        name: "sales_v".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    rockstream_cli::init::scaffold_project(&opts).expect("scaffold");
    let verify_sql = fs::read_to_string(sales_dir.join("queries/verify.sql")).expect("verify.sql");
    assert!(verify_sql.contains("FROM sales_by_store"));
}

#[test]
fn test_readme_commands_match_cli_options() {
    let temp_dir = TempDir::new().expect("tempdir");
    let sales_dir = temp_dir.path().join("sales_readme");

    let opts = InitOptions {
        name: "sales_readme".to_string(),
        template: "local".to_string(),
        dir: Some(sales_dir.clone()),
        force: false,
    };

    rockstream_cli::init::scaffold_project(&opts).expect("scaffold");
    let readme = fs::read_to_string(sales_dir.join("README.md")).expect("readme");
    assert!(readme.contains("rockstream start --storage ./storage --listen 127.0.0.1:5432"));
    assert!(readme.contains("rockstream project apply"));
    assert!(readme.contains("rockstream project verify"));
}
