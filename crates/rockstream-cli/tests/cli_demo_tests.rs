//! Tests for explicit demo mode self-identification and isolation (v0.60 Slice 4).

use clap::Parser;
use rockstream_cli::cli_args::{Cli, Command, ShardCommand, ViewCommand};
use rockstream_cli::demo::{DemoOptions, DemoOutcome};
use rockstream_cli::output::{Formattable, OutputFormat};
use rockstream_cli::run_demo;

#[test]
fn demo_mode_is_strictly_partitioned_and_self_identifying() {
    let outcome = DemoOutcome {
        mode: "demo".to_string(),
        durability: "ephemeral".to_string(),
        topology: "simulated".to_string(),
        scenario: "orders".to_string(),
        status: "passed".to_string(),
        steps: Vec::new(),
        total_duration_ms: 120,
        storage_path: "/tmp/isolated_demo".to_string(),
        retained: false,
    };

    assert_eq!(
        outcome.to_text(),
        format!(
            "RockStream Demo: scenario='orders' status=passed in 120ms\nMode: demo\nDurability: ephemeral\nTopology: simulated\nStorage: /tmp/isolated_demo (retained: false)\n{}",
            "-".repeat(80)
        )
    );
    assert_eq!(
        serde_json::to_string(&outcome).expect("failed to serialize demo outcome to json"),
        r#"{"mode":"demo","durability":"ephemeral","topology":"simulated","scenario":"orders","status":"passed","steps":[],"total_duration_ms":120,"storage_path":"/tmp/isolated_demo","retained":false}"#
    );

    assert!(matches!(
        Cli::try_parse_from(["rockstream", "status"])
            .unwrap()
            .command,
        Command::Status
    ));
    assert!(matches!(
        Cli::try_parse_from(["rockstream", "view", "list"])
            .unwrap()
            .command,
        Command::View {
            command: ViewCommand::List
        }
    ));
    assert!(matches!(
        Cli::try_parse_from(["rockstream", "shard", "list"])
            .unwrap()
            .command,
        Command::Shard {
            command: ShardCommand::List
        }
    ));
}

#[test]
fn test_demo_execution_is_ephemeral_and_isolated() {
    let temp_dir = tempfile::tempdir().expect("failed to create tempdir");
    let demo_storage = temp_dir.path().join("demo_isolated_storage");

    let opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: Some(demo_storage.clone()),
        listen: Some("127.0.0.1:0".to_string()),
        keep: false,
        step_delay_ms: 0,
    };

    let result = run_demo(OutputFormat::Json, &opts);
    assert!(result.is_ok(), "run_demo failed: {:?}", result.err());

    let rendered_json = result.unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("demo output must be valid json");

    assert_eq!(parsed["mode"], "demo");
    assert_eq!(parsed["durability"], "ephemeral");
    assert_eq!(parsed["topology"], "simulated");
    assert_eq!(parsed["retained"], false);
}
