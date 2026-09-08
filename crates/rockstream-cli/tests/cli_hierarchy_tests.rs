//! Tests verifying the restructured CLI command hierarchy (v0.60 Slice 3).

use clap::Parser;
use rockstream_cli::cli_args::{AdminCommand, Cli, Command, DevCommand, ProjectCommand};

#[test]
fn test_status_command_promoted_to_top_level() {
    let cli =
        Cli::try_parse_from(["rockstream", "status"]).expect("failed to parse rockstream status");
    assert!(matches!(cli.command, Command::Status));
}

#[test]
fn test_query_command_top_level() {
    let cli = Cli::try_parse_from(["rockstream", "query", "SELECT * FROM orders"])
        .expect("failed to parse rockstream query");
    match cli.command {
        Command::Query { query } => assert_eq!(query, "SELECT * FROM orders"),
        other => panic!("expected Command::Query, got {other:?}"),
    }
}

#[test]
fn test_shell_command_top_level() {
    let cli =
        Cli::try_parse_from(["rockstream", "shell"]).expect("failed to parse rockstream shell");
    assert!(matches!(cli.command, Command::Shell));
}

#[test]
fn test_admin_commands_hierarchy() {
    // admin drain
    let cli = Cli::try_parse_from(["rockstream", "admin", "drain", "--worker-id", "42", "-y"])
        .expect("failed to parse admin drain");
    match cli.command {
        Command::Admin {
            command: AdminCommand::Drain { worker_id, yes, .. },
        } => {
            assert_eq!(worker_id, 42);
            assert!(yes);
        }
        other => panic!("expected AdminCommand::Drain, got {other:?}"),
    }

    // admin migrate
    let cli = Cli::try_parse_from([
        "rockstream",
        "admin",
        "migrate",
        "--shard-id",
        "7",
        "--target-worker",
        "2",
    ])
    .expect("failed to parse admin migrate");
    match cli.command {
        Command::Admin {
            command:
                AdminCommand::Migrate {
                    shard_id,
                    target_worker,
                    ..
                },
        } => {
            assert_eq!(shard_id, 7);
            assert_eq!(target_worker, 2);
        }
        other => panic!("expected AdminCommand::Migrate, got {other:?}"),
    }

    // admin raft status
    let cli = Cli::try_parse_from(["rockstream", "admin", "raft", "status"])
        .expect("failed to parse admin raft status");
    assert!(matches!(
        cli.command,
        Command::Admin {
            command: AdminCommand::Raft { .. }
        }
    ));

    // admin checkpoint list
    let cli = Cli::try_parse_from(["rockstream", "admin", "checkpoint", "list"])
        .expect("failed to parse admin checkpoint list");
    assert!(matches!(
        cli.command,
        Command::Admin {
            command: AdminCommand::Checkpoint { .. }
        }
    ));
}

#[test]
fn test_dev_commands_hierarchy() {
    // dev sql
    let cli = Cli::try_parse_from(["rockstream", "dev", "sql", "SELECT 1"])
        .expect("failed to parse dev sql");
    match cli.command {
        Command::Dev {
            command: DevCommand::Sql { query },
        } => assert_eq!(query, "SELECT 1"),
        other => panic!("expected DevCommand::Sql, got {other:?}"),
    }

    // dev completions
    let cli = Cli::try_parse_from(["rockstream", "dev", "completions", "bash"])
        .expect("failed to parse dev completions");
    assert!(matches!(
        cli.command,
        Command::Dev {
            command: DevCommand::Completions { .. }
        }
    ));

    // dev explain
    let cli = Cli::try_parse_from(["rockstream", "dev", "explain", "active_users", "--estimate"])
        .expect("failed to parse dev explain");
    match cli.command {
        Command::Dev {
            command:
                DevCommand::Explain {
                    view,
                    estimate,
                    op_ids,
                },
        } => {
            assert_eq!(view, "active_users");
            assert!(estimate);
            assert!(!op_ids);
        }
        other => panic!("expected DevCommand::Explain, got {other:?}"),
    }
}

#[test]
fn test_project_commands_hierarchy() {
    let cli = Cli::try_parse_from([
        "rockstream",
        "project",
        "init",
        "analytics_svc",
        "--template",
        "kafka",
    ])
    .expect("failed to parse project init");
    match cli.command {
        Command::Project {
            command: ProjectCommand::Init { name, template, .. },
        } => {
            assert_eq!(name, "analytics_svc");
            assert_eq!(template, "kafka");
        }
        other => panic!("expected ProjectCommand::Init, got {other:?}"),
    }
}

#[test]
fn test_backwards_compatible_aliases_still_parse() {
    // Top-level cluster status
    let cli = Cli::try_parse_from(["rockstream", "cluster", "status"])
        .expect("backwards compatible cluster status failed to parse");
    assert!(matches!(cli.command, Command::Cluster { .. }));

    // Top-level completions
    let cli = Cli::try_parse_from(["rockstream", "completions", "zsh"])
        .expect("backwards compatible completions failed to parse");
    assert!(matches!(cli.command, Command::Completions { .. }));

    // Top-level explain
    let cli = Cli::try_parse_from(["rockstream", "explain", "orders_view"])
        .expect("backwards compatible explain failed to parse");
    assert!(matches!(cli.command, Command::Explain { .. }));

    // Top-level init
    let cli = Cli::try_parse_from(["rockstream", "init", "legacy_proj"])
        .expect("backwards compatible init failed to parse");
    assert!(matches!(cli.command, Command::Init { .. }));
}
