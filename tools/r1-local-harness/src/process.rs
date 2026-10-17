use std::fs::File;
use std::io;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct ProcessGroup {
    child: Option<Child>,
    pid: i32,
    kill_sent: bool,
}

impl ProcessGroup {
    pub fn spawn(
        command: &mut Command,
        stdout: impl AsRef<Path>,
        stderr: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let stdout = File::create(stdout)?;
        let stderr = File::create(stderr)?;
        command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let child = command.spawn()?;
        let pid = child.id() as i32;
        Ok(Self {
            child: Some(child),
            pid,
            kill_sent: false,
        })
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Give the group a graceful shutdown interval, then force-stop remaining
    /// members before reaping its leader. An exited leader is not evidence that
    /// the rest of the group has stopped. Descendants that deliberately leave
    /// the managed process group are outside this interface.
    ///
    /// Post-SIGKILL cleanup is bounded to two seconds, including the disappearance
    /// of the group on Unix. On timeout a later call may retry the wait,
    /// without signalling a potentially reused process-group ID.
    pub fn terminate(&mut self, timeout: Duration) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cleanup deadline exceeds clock range",
            )
        })?;
        if !self.kill_sent {
            #[cfg(unix)]
            {
                signal_group(self.pid, libc::SIGTERM)?;
                let mut delay = Duration::from_millis(10);
                while Instant::now() < deadline && !leader_exited(self.pid)? {
                    std::thread::sleep(
                        delay.min(deadline.saturating_duration_since(Instant::now())),
                    );
                    delay = (delay * 2).min(Duration::from_millis(100));
                }
                // Keep the leader waitable until the final group signal. Reaping
                // it first would permit PID reuse, as well as losing descendants.
                signal_group(self.pid, libc::SIGKILL)?;
            }
            #[cfg(not(unix))]
            {
                while Instant::now() < deadline {
                    if child.try_wait()?.is_some() {
                        self.child = None;
                        return Ok(());
                    }
                    std::thread::sleep(
                        Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                child.kill()?;
            }
            self.kill_sent = true;
        }
        let reap_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let leader_reaped = child.try_wait()?.is_some();
            #[cfg(unix)]
            let complete = leader_reaped && group_gone(self.pid)?;
            #[cfg(not(unix))]
            let complete = leader_reaped;
            if complete {
                self.child = None;
                return Ok(());
            }
            if Instant::now() >= reap_deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "process group did not exit and drain after SIGKILL",
                ));
            }
            std::thread::sleep(
                Duration::from_millis(10)
                    .min(reap_deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.terminate(Duration::from_millis(250));
    }
}

#[cfg(unix)]
fn signal_group(pid: i32, signal: i32) -> io::Result<()> {
    // The positive PID identifies the session/group created by spawn().
    if unsafe { libc::kill(-pid, signal) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(unix)]
fn leader_exited(pid: i32) -> io::Result<bool> {
    // WNOWAIT observes completion without releasing the PID before the final
    // group signal. Zero initialization also handles a WNOHANG empty result.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let status = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if status == -1 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    Ok(info.si_signo == libc::SIGCHLD)
}

#[cfg(unix)]
fn group_gone(pid: i32) -> io::Result<bool> {
    // Read-only probing after reaping cannot signal an unrelated reused PID.
    if unsafe { libc::kill(-pid, 0) } == 0 {
        return Ok(false);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(true)
    } else {
        Err(error)
    }
}
