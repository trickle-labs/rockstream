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
    // An orphaned Linux child may remain as a zombie awaiting PID 1.
    // A zombie is no longer executing; other Unix platforms reap normally.
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{descendant}/stat"));
        if let Ok(stat) = stat {
            assert_eq!(stat.rsplit_once(") ").unwrap().1.split_whitespace().next(), Some("Z"));
        }
    }
    #[cfg(not(target_os = "linux"))]
    assert!(unsafe { libc::kill(descendant, 0) } == -1);
}
