use std::fs::File;
use std::io;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct ProcessGroup {
    child: Option<Child>,
    pid: i32,
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
        })
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn terminate(&mut self, timeout: Duration) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if child.try_wait()?.is_some() {
            self.child = None;
            return Ok(());
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(-self.pid, libc::SIGTERM);
        }
        let deadline = Instant::now() + timeout;
        let mut delay = Duration::from_millis(10);
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                self.child = None;
                return Ok(());
            }
            std::thread::sleep(delay.min(deadline.saturating_duration_since(Instant::now())));
            delay = (delay * 2).min(Duration::from_millis(100));
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(-self.pid, libc::SIGKILL);
        }
        let _ = child.wait()?;
        self.child = None;
        Ok(())
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.terminate(Duration::from_millis(250));
    }
}
