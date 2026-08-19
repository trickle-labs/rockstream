//! v0.59.4 Slice 1 — Shell Completions Tests (CLI-02)

use rockstream_cli::cli_args::ShellType;
use rockstream_cli::run_completions;

#[test]
fn test_bash_completions() {
    let script = run_completions(ShellType::Bash).expect("Bash completions generation failed");
    assert!(!script.is_empty());
    assert!(script.contains("rockstream"));
    assert!(script.contains("config"));
    assert!(script.contains("completions"));
    assert!(script.contains("--output"));
    assert!(script.contains("start"));
    assert!(script.contains("view"));
}

#[test]
fn test_zsh_completions() {
    let script = run_completions(ShellType::Zsh).expect("Zsh completions generation failed");
    assert!(!script.is_empty());
    assert!(script.contains("#compdef rockstream") || script.contains("_rockstream"));
    assert!(script.contains("config"));
    assert!(script.contains("completions"));
    assert!(script.contains("--output"));
}

#[test]
fn test_fish_completions() {
    let script = run_completions(ShellType::Fish).expect("Fish completions generation failed");
    assert!(!script.is_empty());
    assert!(script.contains("complete -c rockstream"));
    assert!(script.contains("config"));
    assert!(script.contains("completions"));
}
