use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::process::Command;
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
        .find_map(|metric| metric.labels.get("worker_id"))
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

pub fn process_snapshot(pid: u32) -> Result<ProcessSnapshot> {
    let output = Command::new("ps")
        .args([
            "-o",
            "utime=",
            "-o",
            "stime=",
            "-o",
            "rss=",
            "-p",
            &pid.to_string(),
        ])
        .output()
        .context("run ps for process counters")?;
    if !output.status.success() {
        bail!("ps could not observe pid {pid}");
    }
    let text = String::from_utf8(output.stdout).context("ps returned non-UTF-8 output")?;
    let fields = text.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 3 {
        bail!("ps returned incomplete counters for pid {pid}: {text:?}");
    }
    Ok(ProcessSnapshot {
        user_cpu_ns: parse_cpu_time(fields[0])?,
        system_cpu_ns: parse_cpu_time(fields[1])?,
        rss_bytes: fields[2].parse::<u64>().context("parse RSS KiB")? * 1024,
    })
}

fn parse_cpu_time(value: &str) -> Result<u64> {
    let (days, clock) = match value.split_once('-') {
        Some((days, clock)) => (days.parse::<u64>().context("parse CPU days")?, clock),
        None => (0, value),
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
    Ok(seconds * 1_000_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_worker_metrics_exactly() {
        let metrics = parse(
            "rockstream_r1_worker_shards_owned{worker_id=\"7\"} 2\nrockstream_r1_worker_input_rows_total{worker_id=\"7\"} 9\nrockstream_r1_worker_output_rows_total{worker_id=\"7\"} 8\nrockstream_r1_worker_state_writes_total{worker_id=\"7\"} 3\nrockstream_r1_worker_exchange_bytes_total{worker_id=\"7\"} 44\n",
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
}
