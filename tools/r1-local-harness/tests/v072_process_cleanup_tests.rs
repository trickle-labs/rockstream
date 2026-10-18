#![cfg(unix)]

use r1_local_harness::process::ProcessGroup;
use std::io;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

fn exists(pid: i32) -> bool {
    let status = unsafe { libc::kill(pid, 0) };
    status == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

fn wait_until(mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn exited_without_reaping(pid: i32) -> bool {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let status = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    assert_eq!(status, 0, "{}", io::Error::last_os_error());
    info.si_signo == libc::SIGCHLD
}

fn resistant_descendant(dir: &Path, parent_exits: bool) -> (ProcessGroup, i32) {
    let pid_file = dir.join("child.pid");
    let ready_file = dir.join("child.ready");
    let process = ProcessGroup::spawn(
        Command::new("/bin/sh")
            .args([
                "-c",
                "(trap \"\" TERM; printf ready > \"$2\"; exec sleep 30) & child=$!; printf \"%s\\n\" \"$child\" > \"$1\"; if [ \"$3\" = exit ]; then exit 0; fi; wait",
                "v072-cleanup-test",
            ])
            .arg(&pid_file)
            .arg(&ready_file)
            .arg(if parent_exits { "exit" } else { "wait" }),
        dir.join("stdout.log"),
        dir.join("stderr.log"),
    )
    .unwrap();
    assert!(wait_until(|| {
        ready_file.exists()
            && std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<i32>().ok())
                .is_some()
    }));
    let descendant = std::fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    assert!(descendant > 0);
    if parent_exits {
        assert!(wait_until(|| exited_without_reaping(process.pid())));
    }
    (process, descendant)
}

fn assert_complete_cleanup(process: &mut ProcessGroup, descendant: i32) {
    let result = process.terminate(Duration::from_millis(100));
    let gone = wait_until(|| !exists(process.pid()) && !exists(descendant));
    let observed = (exists(process.pid()), exists(descendant));
    // A failing regression must not leave its resistant child running.
    if !gone {
        unsafe { libc::kill(descendant, libc::SIGKILL) };
        wait_until(|| !exists(descendant));
    }
    result.unwrap();
    assert_eq!(observed, (false, false));
    process.terminate(Duration::ZERO).unwrap();
}

#[test]
fn cleanup_kills_resistant_descendant_after_leader_exits_on_term() {
    let dir = tempfile::tempdir().unwrap();
    let (mut process, descendant) = resistant_descendant(dir.path(), false);
    assert_complete_cleanup(&mut process, descendant);
    assert_eq!(std::fs::read(dir.path().join("stdout.log")).unwrap(), b"");
    assert_eq!(std::fs::read(dir.path().join("stderr.log")).unwrap(), b"");
}

#[test]
fn cleanup_kills_resistant_descendant_when_leader_has_already_exited() {
    let dir = tempfile::tempdir().unwrap();
    let (mut process, descendant) = resistant_descendant(dir.path(), true);
    assert_complete_cleanup(&mut process, descendant);
}

#[test]
fn cleanup_does_not_signal_an_independent_group() {
    let first_dir = tempfile::tempdir().unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let (mut first, descendant) = resistant_descendant(first_dir.path(), true);
    let mut other = ProcessGroup::spawn(
        Command::new("/bin/sleep").arg("30"),
        other_dir.path().join("stdout.log"),
        other_dir.path().join("stderr.log"),
    )
    .unwrap();
    assert_complete_cleanup(&mut first, descendant);
    assert_eq!(
        (exists(first.pid()), exists(descendant), exists(other.pid())),
        (false, false, true)
    );
    other.terminate(Duration::ZERO).unwrap();
    assert!(!exists(other.pid()));
}

#[test]
fn unrepresentable_cleanup_deadline_is_rejected_before_signalling() {
    let dir = tempfile::tempdir().unwrap();
    let mut process = ProcessGroup::spawn(
        Command::new("/bin/sleep").arg("30"),
        dir.path().join("stdout.log"),
        dir.path().join("stderr.log"),
    )
    .unwrap();
    let error = process.terminate(Duration::MAX).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(exists(process.pid()));
    process.terminate(Duration::ZERO).unwrap();
    assert!(!exists(process.pid()));
}
