//! Real-process counter capture for v0.72 qualification preparation.
//!
//! This *does not* manufacture a qualified benchmark sample: workload input,
//! independent result oracles, comparison topology and frozen thresholds are
//! verified by separate release-process qualification steps.

use crate::artifact::{atomic_json, sha256_file};
use crate::metrics::process_snapshot;
use crate::process::ProcessGroup;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MIN_CAPTURE_MS: u128 = 100;
const MAX_CAPTURE_MS: u128 = 3_600_000;
const MIN_SAMPLE_MS: u128 = 10;
const MAX_SAMPLE_MS: u128 = 1_000;
const MAX_SAMPLES: u128 = 6_001;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessCounterSample {
    pub monotonic_elapsed_ns: u64,
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    pub rss_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessCapture {
    pub schema_version: u32,
    /// Intentionally cannot be presented as qualified throughput evidence.
    pub qualification_status: String,
    pub started_unix_ms: u64,
    pub executable_path: String,
    pub executable_sha256: String,
    pub argv: Vec<String>,
    pub pid: u32,
    pub measurement_duration_ns: u64,
    pub samples: Vec<ProcessCounterSample>,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
}

fn checked_duration(duration: Duration, interval: Duration) -> Result<()> {
    let span = duration.as_millis();
    let step = interval.as_millis();
    if !(MIN_CAPTURE_MS..=MAX_CAPTURE_MS).contains(&span)
        || !(MIN_SAMPLE_MS..=MAX_SAMPLE_MS).contains(&step)
        || step > span
        || span.div_ceil(step) + 1 > MAX_SAMPLES
    {
        bail!(
            "invalid process capture timing: duration_ms={span}, interval_ms={step}; \
             duration must be 100..=3600000, interval 10..=1000, interval <= duration"
        );
    }
    Ok(())
}

/// Launch exactly the supplied executable in an owned process group and capture
/// *observed* counters, failing if the process exits early or counters disappear.
/// No workload throughput, correctness, cost, or result latency is inferred.
pub fn capture_process(
    executable: &Path,
    argv: &[String],
    output: &Path,
    duration: Duration,
    interval: Duration,
) -> Result<ProcessCapture> {
    checked_duration(duration, interval)?;
    let executable = executable
        .canonicalize()
        .with_context(|| format!("resolve executable {}", executable.display()))?;
    if !executable.is_file() {
        bail!("capture executable is not a regular file");
    }
    let executable_sha256 = sha256_file(&executable)?;
    // Atomic directory reservation: two concurrent sessions cannot trample
    // stdout/stderr or produce a capture spanning different process runs.
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    fs::create_dir(output).with_context(|| {
        format!(
            "refusing to reuse a process capture directory {}",
            output.display()
        )
    })?;
    let artifact = output.join("process-capture.json");
    let stdout = output.join("stdout.log");
    let stderr = output.join("stderr.log");
    let started_unix_ms: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX epoch")?
        .as_millis()
        .try_into()
        .context("UNIX millisecond timestamp does not fit u64")?;

    let mut command = Command::new(&executable);
    command.args(argv);
    let mut child = ProcessGroup::spawn(&mut command, &stdout, &stderr)
        .with_context(|| format!("start observed process {}", executable.display()))?;
    let pid = u32::try_from(child.pid()).context("invalid spawned process PID")?;
    let start = Instant::now();
    let deadline = start
        .checked_add(duration)
        .context("capture deadline overflow")?;
    let mut samples: Vec<ProcessCounterSample> = Vec::new();
    while Instant::now() < deadline {
        if child.has_exited()? {
            bail!("observed process {pid} exited before the capture ended");
        }
        let point =
            process_snapshot(pid).with_context(|| format!("sample spawned process {pid}"))?;
        let monotonic_elapsed_ns = start
            .elapsed()
            .as_nanos()
            .try_into()
            .context("capture timestamp exceeds u64")?;
        if let Some(previous) = samples.last() {
            if point.user_cpu_ns < previous.user_cpu_ns
                || point.system_cpu_ns < previous.system_cpu_ns
            {
                bail!("non-monotonic CPU counters for observed process {pid}");
            }
        }
        samples.push(ProcessCounterSample {
            monotonic_elapsed_ns,
            user_cpu_ns: point.user_cpu_ns,
            system_cpu_ns: point.system_cpu_ns,
            rss_bytes: point.rss_bytes,
        });
        std::thread::sleep(interval.min(deadline.saturating_duration_since(Instant::now())));
    }
    if samples.len() < 2 {
        bail!("fewer than two observed process samples");
    }
    if child.has_exited()? {
        bail!("observed process {pid} exited before the capture completed");
    }
    let measurement_duration_ns = start
        .elapsed()
        .as_nanos()
        .try_into()
        .context("capture duration exceeds u64")?;
    child
        .terminate(Duration::from_secs(2))
        .context("terminate observed process group")?;

    // Fail closed if the executable was replaced while the run was active.
    if sha256_file(&executable)? != executable_sha256 {
        bail!("observed executable content changed during capture");
    }
    let capture = ProcessCapture {
        schema_version: 1,
        qualification_status: "observation_only_not_capacity_qualified".to_string(),
        started_unix_ms,
        executable_path: executable.to_string_lossy().into_owned(),
        executable_sha256,
        argv: argv.to_vec(),
        pid,
        measurement_duration_ns,
        samples,
        stdout_sha256: sha256_file(&stdout)?,
        stderr_sha256: sha256_file(&stderr)?,
    };
    atomic_json(&artifact, &capture)?;
    Ok(capture)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn v072_captures_real_child_counter_series_and_digests() {
        let dir = tempfile::tempdir().unwrap();
        let sleep = Path::new("/bin/sleep");
        let output = dir.path().join("new-capture");
        let capture = capture_process(
            sleep,
            &["2".to_owned()],
            &output,
            Duration::from_millis(180),
            Duration::from_millis(30),
        )
        .unwrap();
        assert_eq!(capture.schema_version, 1);
        assert_eq!(
            capture.qualification_status,
            "observation_only_not_capacity_qualified"
        );
        assert_eq!(capture.executable_sha256, sha256_file(sleep).unwrap());
        assert_eq!(capture.argv, vec!["2"]);
        assert!(capture.pid > 0);
        assert!(capture.samples.len() >= 2);
        assert!(capture.samples.iter().any(|point| point.rss_bytes > 0));
        assert!(capture
            .samples
            .windows(2)
            .all(|w| w[0].monotonic_elapsed_ns < w[1].monotonic_elapsed_ns
                && w[0].user_cpu_ns <= w[1].user_cpu_ns
                && w[0].system_cpu_ns <= w[1].system_cpu_ns));
        let bytes = fs::read(output.join("process-capture.json")).unwrap();
        let decoded: ProcessCapture = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, capture);
        assert_eq!(decoded.stdout_sha256, crate::artifact::sha256(b""));
        assert_eq!(decoded.stderr_sha256, crate::artifact::sha256(b""));
    }

    #[test]
    #[cfg(unix)]
    fn v072_early_exit_cannot_be_written_as_observed_process_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let result = capture_process(
            Path::new("/bin/true"),
            &[],
            dir.path(),
            Duration::from_millis(300),
            Duration::from_millis(30),
        );
        assert!(result.is_err());
        assert!(!dir.path().join("process-capture.json").exists());
    }

    #[test]
    #[cfg(unix)]
    fn v072_rejects_invalid_capture_windows_before_launch() {
        for (duration, interval) in [(0, 100), (100, 0), (100, 110), (3_600_001, 100), (200, 1)] {
            let dir = tempfile::tempdir().unwrap();
            assert!(capture_process(
                Path::new("/bin/sleep"),
                &["2".to_owned()],
                dir.path(),
                Duration::from_millis(duration),
                Duration::from_millis(interval)
            )
            .is_err());
            assert!(!dir.path().join("stdout.log").exists());
        }
    }

    #[test]
    #[cfg(unix)]
    fn v072_concurrent_capture_slot_rejects_preexisting_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("used");
        fs::create_dir(&existing).unwrap();
        let result = capture_process(
            Path::new("/bin/sleep"),
            &["2".to_owned()],
            &existing,
            Duration::from_millis(100),
            Duration::from_millis(25),
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refusing to reuse"));
        assert_eq!(fs::read_dir(existing).unwrap().count(), 0);
    }

    #[test]
    #[cfg(unix)]
    fn v072_never_overwrites_a_previous_capture() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("process-capture.json"), b"sentinel").unwrap();
        let result = capture_process(
            Path::new("/bin/sleep"),
            &["2".to_owned()],
            dir.path(),
            Duration::from_millis(100),
            Duration::from_millis(25),
        );
        assert!(result.is_err());
        assert_eq!(
            fs::read(dir.path().join("process-capture.json")).unwrap(),
            b"sentinel"
        );
        assert!(!dir.path().join("stdout.log").exists());
    }
}
