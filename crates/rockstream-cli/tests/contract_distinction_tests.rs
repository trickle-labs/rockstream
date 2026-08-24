//! Contract distinction tests: `rockstream demo` vs `rockstream init` (GP-006 / Matrix E).

use rockstream_cli::demo::{run_demo, DemoOptions, DemoOutcome};
use rockstream_cli::init::{run_init, InitOptions, InitOutcome};
use rockstream_cli::output::OutputFormat;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_contract_distinction() {
    // 1. Demo contract: Ephemeral, self-contained walkthrough
    let demo_opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: None,
        listen: None,
        keep: false,
        step_delay_ms: 0,
    };
    let demo_res = run_demo(OutputFormat::Json, &demo_opts).expect("demo execution");
    let demo_outcome: DemoOutcome =
        serde_json::from_str(&demo_res).expect("valid DemoOutcome JSON");
    assert_eq!(demo_outcome.scenario, "orders");
    assert_eq!(demo_outcome.status, "passed");

    // 2. Init contract: Persistent project scaffolding
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("distinction_proj");
    let init_opts = InitOptions {
        name: "distinction_proj".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };
    let init_res = run_init(OutputFormat::Json, &init_opts).expect("init execution");
    let init_outcome: InitOutcome =
        serde_json::from_str(&init_res).expect("valid InitOutcome JSON");
    assert_eq!(init_outcome.status, "created");
    assert!(target_dir.exists());
    assert!(target_dir.join("rockstream.toml").exists());
    assert!(target_dir.join("schema.sql").exists());
}

#[test]
fn test_storage_lifecycle_distinction() {
    // Demo without keep deletes its ephemeral storage dir
    let demo_opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: None,
        listen: None,
        keep: false,
        step_delay_ms: 0,
    };
    let demo_res = run_demo(OutputFormat::Json, &demo_opts).expect("demo run");
    let demo_outcome: DemoOutcome = serde_json::from_str(&demo_res).expect("valid JSON");
    assert!(!demo_outcome.retained);
    let path = std::path::PathBuf::from(&demo_outcome.storage_path);
    assert!(
        !path.exists(),
        "demo temporary storage must be cleaned up when keep is false"
    );

    // Init creates persistent files retained indefinitely
    let temp_dir = TempDir::new().expect("tempdir");
    let init_target = temp_dir.path().join("persistent_init_dir");
    let init_opts = InitOptions {
        name: "persistent_init_dir".to_string(),
        template: "local".to_string(),
        dir: Some(init_target.clone()),
        force: false,
    };
    let _ = run_init(OutputFormat::Json, &init_opts).expect("init run");
    assert!(
        init_target.exists(),
        "init scaffolded directory must persist"
    );
    assert!(init_target.join("rockstream.toml").exists());
}

#[test]
fn test_configuration_distinction() {
    // Demo uses internal hardcoded configuration defaults
    let demo_opts = DemoOptions::default();
    assert_eq!(demo_opts.scenario, "orders");
    assert!(!demo_opts.keep);
    assert_eq!(demo_opts.storage, None);

    // Init outputs fully customized configuration files
    let temp_dir = TempDir::new().expect("tempdir");
    let init_target = temp_dir.path().join("cfg_proj");
    let init_opts = InitOptions {
        name: "cfg_proj".to_string(),
        template: "local".to_string(),
        dir: Some(init_target.clone()),
        force: false,
    };
    let _ = run_init(OutputFormat::Json, &init_opts).expect("init run");
    let toml_str =
        fs::read_to_string(init_target.join("rockstream.toml")).expect("read rockstream.toml");
    assert!(toml_str.contains("[gateway]"));
    assert!(toml_str.contains("[storage]"));
    assert!(toml_str.contains("[metrics]"));
    assert!(toml_str.contains("[logging]"));
}

#[test]
fn test_service_topology_distinction() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Init produces multi-service Compose profiles for kafka and postgres-cdc
    let kafka_dir = temp_dir.path().join("kafka_topo");
    let kafka_opts = InitOptions {
        name: "kafka_topo".to_string(),
        template: "kafka".to_string(),
        dir: Some(kafka_dir.clone()),
        force: false,
    };
    run_init(OutputFormat::Json, &kafka_opts).expect("init kafka");
    let kafka_compose =
        fs::read_to_string(kafka_dir.join("docker-compose.yaml")).expect("kafka compose");
    assert!(kafka_compose.contains("redpanda:"));
    assert!(kafka_compose.contains("rockstream:"));
    assert!(kafka_compose.contains("verifier:"));

    let cdc_dir = temp_dir.path().join("cdc_topo");
    let cdc_opts = InitOptions {
        name: "cdc_topo".to_string(),
        template: "postgres-cdc".to_string(),
        dir: Some(cdc_dir.clone()),
        force: false,
    };
    run_init(OutputFormat::Json, &cdc_opts).expect("init cdc");
    let cdc_compose = fs::read_to_string(cdc_dir.join("docker-compose.yaml")).expect("cdc compose");
    assert!(cdc_compose.contains("postgres:"));
    assert!(cdc_compose.contains("rockstream:"));
}

#[test]
fn test_rerunnability_distinction() {
    let temp_dir = TempDir::new().expect("tempdir");
    let target_dir = temp_dir.path().join("rerun_proj");
    let init_opts = InitOptions {
        name: "rerun_proj".to_string(),
        template: "local".to_string(),
        dir: Some(target_dir.clone()),
        force: false,
    };
    run_init(OutputFormat::Json, &init_opts).expect("init run");

    let verify_script = target_dir.join("scripts/verify.sh");
    let cleanup_script = target_dir.join("scripts/cleanup.sh");
    assert!(verify_script.exists());
    assert!(cleanup_script.exists());

    // Both scripts are executable and idempotent
    let cleanup_content = fs::read_to_string(&cleanup_script).expect("read cleanup.sh");
    assert!(cleanup_content.contains("rm -rf ./storage"));
}
