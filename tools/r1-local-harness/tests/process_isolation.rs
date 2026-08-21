#![cfg(unix)]

use std::process::Command;
use std::thread;
use std::time::Duration;

use r1_local_harness::process::ProcessGroup;

#[test]
fn terminate_kills_shell_and_background_descendant() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("child.pid");
    let stdout = dir.path().join("stdout.log");
    let stderr = dir.path().join("stderr.log");
    let mut process = ProcessGroup::spawn(
        Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("sleep 30 & echo $! > {}; wait", pid_file.display())),
        &stdout,
        &stderr,
    )
    .unwrap();
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let descendant: i32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    process.terminate(Duration::from_millis(100)).unwrap();
    thread::sleep(Duration::from_millis(20));
    assert!(unsafe { libc::kill(process.pid(), 0) } == -1);
    assert!(unsafe { libc::kill(descendant, 0) } == -1);
}
