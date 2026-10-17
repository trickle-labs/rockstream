use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerActivity {
    pub worker_id: u64,
    pub pid: u32,
    pub shards_owned: u64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub state_writes: u64,
    pub exchange_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSnapshot {
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    pub rss_bytes: u64,
}

pub async fn fetch(addr: &str) -> Result<Vec<Metric>> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to metrics at {addr}"))?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .context("request metrics")?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .context("read metrics")?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .context("metrics response has no body")?;
    parse(body)
}

pub fn parse(body: &str) -> Result<Vec<Metric>> {
    body.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (series, value) = line
                .rsplit_once(char::is_whitespace)
                .with_context(|| format!("invalid metric line {line:?}"))?;
            let value: f64 = value
                .parse()
                .with_context(|| format!("invalid metric value in {line:?}"))?;
            if !value.is_finite() || value < 0.0 {
                bail!("invalid metric value in {line:?}");
            }
            let (name, labels) = match series.split_once('{') {
                Some((name, labels)) => (
                    name,
                    parse_labels(labels.strip_suffix('}').context("unterminated labels")?)?,
                ),
                None => (series, BTreeMap::new()),
            };
            Ok(Metric {
                name: name.to_string(),
                labels,
                value,
            })
        })
        .collect()
}

fn parse_labels(labels: &str) -> Result<BTreeMap<String, String>> {
    if labels.is_empty() {
        return Ok(BTreeMap::new());
    }
    labels
        .split(',')
        .map(|label| {
            let (name, value) = label
                .split_once('=')
                .with_context(|| format!("invalid metric label {label:?}"))?;
            Ok((
                name.to_string(),
                value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .context("metric label must be quoted")?
                    .to_string(),
            ))
        })
        .collect()
}

pub fn worker_activity(metrics: &[Metric], pid: u32) -> Result<WorkerActivity> {
    let worker_id = metrics
        .iter()
        .find(|metric| metric.name == "rockstream_r1_worker_shards_owned")
        .and_then(|metric| metric.labels.get("worker_id"))
        .context("worker metrics have no worker_id")?
        .parse()
        .context("worker_id is not an integer")?;
    let value = |name: &str| -> Result<u64> {
        let total = metrics
            .iter()
            .filter(|metric| metric.name == name)
            .map(|metric| metric.value)
            .sum::<f64>();
        if total > u64::MAX as f64 {
            bail!("metric {name} overflows u64");
        }
        Ok(total as u64)
    };
    let shards_owned = value("rockstream_r1_worker_shards_owned")?;
    Ok(WorkerActivity {
        worker_id,
        pid,
        shards_owned,
        input_rows: value("rockstream_r1_worker_input_rows_total")?,
        output_rows: value("rockstream_r1_worker_output_rows_total")?,
        state_writes: value("rockstream_r1_worker_state_writes_total")?,
        exchange_bytes: value("rockstream_r1_worker_exchange_bytes_total")?,
    })
}

pub fn operator_counters(metrics: &[Metric]) -> BTreeMap<String, u64> {
    let mut counters = BTreeMap::new();
    for metric in metrics
        .iter()
        .filter(|metric| metric.name.starts_with("rockstream_r1_"))
    {
        *counters.entry(metric.name.clone()).or_default() += metric.value as u64;
    }
    counters
}

/// Read actual per-process user/system CPU and resident bytes. `ps stime` is
/// the *start time*, not system CPU time; never treat it as a CPU counter.
pub fn process_snapshot(pid: u32) -> Result<ProcessSnapshot> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .with_context(|| format!("read Linux process stat for {pid}"))?;
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if ticks <= 0 || page_size <= 0 {
            bail!("Linux process counter conversion has invalid clock/page scale");
        }
        parse_linux_proc_stat(&stat, ticks as u64, page_size as u64)
    }
    #[cfg(target_os = "macos")]
    {
        snapshot_macos(pid)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        bail!("v0.72 measured process counters unsupported on this operating system (pid={pid})")
    }
}

#[cfg(target_os = "linux")]
fn parse_linux_proc_stat(
    stat: &str,
    ticks_per_second: u64,
    page_size: u64,
) -> Result<ProcessSnapshot> {
    if ticks_per_second == 0 || page_size == 0 {
        bail!("invalid Linux process counter scale");
    }
    // /proc/<pid>/stat field 2 is a parenthesized command containing spaces
    // and parentheses. Find the final ') ' rather than splitting the command.
    let close = stat
        .rfind(") ")
        .context("missing Linux process name terminator")?;
    let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
    // The first remaining field is state (#3); utime is #14, stime #15,
    // resident pages #24, therefore remaining indices 11, 12 and 21.
    if fields.len() <= 21 {
        bail!("incomplete Linux process stat");
    }
    let user_ticks = fields[11]
        .parse::<u64>()
        .context("parse Linux user CPU ticks")?;
    let system_ticks = fields[12]
        .parse::<u64>()
        .context("parse Linux system CPU ticks")?;
    let resident_pages = fields[21]
        .parse::<u64>()
        .context("parse Linux resident pages")?;
    let cpu_ns = |ticks: u64| -> Result<u64> {
        (u128::from(ticks) * 1_000_000_000 / u128::from(ticks_per_second))
            .try_into()
            .context("Linux CPU nanoseconds overflow")
    };
    Ok(ProcessSnapshot {
        user_cpu_ns: cpu_ns(user_ticks)?,
        system_cpu_ns: cpu_ns(system_ticks)?,
        rss_bytes: resident_pages
            .checked_mul(page_size)
            .context("Linux resident bytes overflow")?,
    })
}

#[cfg(target_os = "macos")]
fn snapshot_macos(pid: u32) -> Result<ProcessSnapshot> {
    // macOS libproc PROC_PIDTASKINFO (4) exposes real user/system CPU time
    // and resident memory, not the ambiguous `ps stime` display keyword.
    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_pidinfo(
            pid: libc::c_int,
            flavor: libc::c_int,
            arg: u64,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }
    let pid: libc::c_int = pid.try_into().context("macOS PID out of range")?;
    // A u64 array is correctly aligned and larger than proc_taskinfo; the
    // leading members are virtual, resident, user and system nanoseconds.
    let mut info = [0u64; 16];
    let returned = unsafe {
        proc_pidinfo(
            pid,
            4,
            0,
            info.as_mut_ptr().cast(),
            std::mem::size_of_val(&info) as libc::c_int,
        )
    };
    if returned < 6 * std::mem::size_of::<u64>() as libc::c_int {
        bail!("macOS proc_pidinfo returned incomplete task counters for pid {pid}");
    }
    Ok(ProcessSnapshot {
        user_cpu_ns: info[2],
        system_cpu_ns: info[3],
        rss_bytes: info[1],
    })
}

#[cfg(test)]
fn parse_cpu_time(value: &str) -> Result<u64> {
    let (days, clock) = match value.split_once('-') {
        Some((days, clock)) => (days.parse::<u64>().context("parse CPU days")?, clock),
        None => (0, value),
    };
    let (clock, fractional_ns) = match clock.rsplit_once('.') {
        Some((clock, fractional)) => {
            if fractional.is_empty() || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
                bail!("invalid CPU time {value:?}");
            }
            let digits = fractional.len().min(9);
            let nanos = fractional[..digits]
                .parse::<u64>()
                .context("parse CPU fractional seconds")?
                * 10u64.pow((9 - digits) as u32);
            (clock, nanos)
        }
        None => (clock, 0),
    };
    let parts = clock
        .split(':')
        .map(|part| part.parse::<u64>().context("parse CPU clock"))
        .collect::<Result<Vec<_>>>()?;
    let seconds = match parts.as_slice() {
        [minutes, seconds] => days * 86_400 + minutes * 60 + seconds,
        [hours, minutes, seconds] => days * 86_400 + hours * 3_600 + minutes * 60 + seconds,
        _ => bail!("invalid CPU time {value:?}"),
    };
    Ok(seconds * 1_000_000_000 + fractional_ns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_worker_metrics_exactly() {
        let metrics = parse(
            "rockstream_r1_other_total{worker_id=\"gateway\"} 1\nrockstream_r1_worker_shards_owned{worker_id=\"7\"} 2\nrockstream_r1_worker_input_rows_total{worker_id=\"7\"} 9\nrockstream_r1_worker_output_rows_total{worker_id=\"7\"} 8\nrockstream_r1_worker_state_writes_total{worker_id=\"7\"} 3\nrockstream_r1_worker_exchange_bytes_total{worker_id=\"7\"} 44\n",
        )
        .unwrap();
        assert_eq!(
            worker_activity(&metrics, 123).unwrap(),
            WorkerActivity {
                worker_id: 7,
                pid: 123,
                shards_owned: 2,
                input_rows: 9,
                output_rows: 8,
                state_writes: 3,
                exchange_bytes: 44,
            }
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn parses_actual_linux_user_system_cpu_and_resident_bytes() {
        let mut fields = vec!["0"; 22];
        fields[0] = "S";
        fields[11] = "123";
        fields[12] = "22";
        fields[21] = "8";
        let fixture = format!("123 (nested) process name) {}", fields.join(" "));
        assert_eq!(
            parse_linux_proc_stat(&fixture, 100, 4096).unwrap(),
            ProcessSnapshot {
                user_cpu_ns: 1_230_000_000,
                system_cpu_ns: 220_000_000,
                rss_bytes: 32768,
            }
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn proc_stat_rejects_incomplete_and_unrepresentable_counters() {
        assert!(parse_linux_proc_stat("1 (short) S", 100, 4096).is_err());
        let mut fields = vec!["0"; 22];
        fields[0] = "S";
        fields[11] = "123";
        fields[12] = "22";
        fields[21] = "8";
        let fixture = format!("123 (cmd) {}", fields.join(" "));
        assert!(parse_linux_proc_stat(&fixture, 0, 4096).is_err());
        fields[21] = "18446744073709551615";
        let overflow = format!("123 (cmd) {}", fields.join(" "));
        assert!(parse_linux_proc_stat(&overflow, 100, 4096).is_err());
    }

    #[test]
    fn parses_macos_cpu_clock_with_fractional_seconds() {
        assert_eq!(parse_cpu_time("0:04.28").unwrap(), 4_280_000_000);
    }
}
