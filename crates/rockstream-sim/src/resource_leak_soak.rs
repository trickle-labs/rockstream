//! Bounded process-resource sampling and deterministic leak-trend gating.

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceKind {
    Rss,
    OpenFileDescriptors,
    OpenSockets,
}

impl ResourceKind {
    const ALL: [Self; 3] = [Self::Rss, Self::OpenFileDescriptors, Self::OpenSockets];

    fn name(self) -> &'static str {
        match self {
            Self::Rss => "RSS",
            Self::OpenFileDescriptors => "open FD",
            Self::OpenSockets => "open socket",
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Self::Rss => "KiB",
            Self::OpenFileDescriptors | Self::OpenSockets => "count",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceSample {
    pub timestamp_secs: u64,
    pub rss_kib: u64,
    pub open_fds: u64,
    pub open_sockets: u64,
}

impl ResourceSample {
    fn value(self, kind: ResourceKind) -> u64 {
        match kind {
            ResourceKind::Rss => self.rss_kib,
            ResourceKind::OpenFileDescriptors => self.open_fds,
            ResourceKind::OpenSockets => self.open_sockets,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceGateConfig {
    pub capacity: usize,
    pub warmup_samples: usize,
    pub rolling_window: usize,
    pub rss_tolerance_kib: u64,
    pub open_fd_tolerance: u64,
    pub open_socket_tolerance: u64,
}

impl ResourceGateConfig {
    pub const fn max_samples(duration_secs: u64, sample_interval_secs: u64) -> usize {
        (duration_secs / sample_interval_secs + 1) as usize
    }

    fn tolerance(self, kind: ResourceKind) -> u64 {
        match kind {
            ResourceKind::Rss => self.rss_tolerance_kib,
            ResourceKind::OpenFileDescriptors => self.open_fd_tolerance,
            ResourceKind::OpenSockets => self.open_socket_tolerance,
        }
    }

    fn validate(self) -> Result<(), ResourceGateError> {
        if self.capacity == 0 || self.warmup_samples == 0 || self.rolling_window == 0 {
            return Err(ResourceGateError::InvalidConfiguration(
                "capacity, warmup_samples, and rolling_window must all be nonzero".to_owned(),
            ));
        }
        if self.warmup_samples + self.rolling_window > self.capacity {
            return Err(ResourceGateError::InvalidConfiguration(format!(
                "warmup_samples {} + rolling_window {} exceeds capacity {}",
                self.warmup_samples, self.rolling_window, self.capacity
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceMetric {
    pub kind: ResourceKind,
    pub baseline: u64,
    pub tolerance: u64,
    pub final_rolling_median: u64,
    pub passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceLeakSoakSummary {
    config: ResourceGateConfig,
    samples_collected: usize,
    metrics: [ResourceMetric; 3],
}

impl ResourceLeakSoakSummary {
    pub fn samples_collected(&self) -> usize {
        self.samples_collected
    }

    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    pub fn fill_percent(&self) -> usize {
        self.samples_collected * 100 / self.config.capacity
    }

    pub fn passed(&self) -> bool {
        self.metrics.iter().all(|metric| metric.passed)
    }

    pub fn render_markdown(&self) -> String {
        let status = if self.passed() { "PASS" } else { "FAIL" };
        let mut output = format!(
            "# Rockstream resource-leak soak\n\nstatus: {status}\nsamples: {}/{} (fill: {}%)\nwarmup samples: {}\nrolling window: {}\n\n| resource | unit | baseline | tolerance | final rolling median | slope verdict |\n| --- | --- | ---: | ---: | ---: | --- |\n",
            self.samples_collected,
            self.config.capacity,
            self.fill_percent(),
            self.config.warmup_samples,
            self.config.rolling_window,
        );
        for metric in &self.metrics {
            output.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                metric.kind.name(),
                metric.kind.unit(),
                metric.baseline,
                metric.tolerance,
                metric.final_rolling_median,
                if metric.passed { "PASS" } else { "FAIL" },
            ));
        }
        output.push_str("\ndiagnostic: ");
        output.push_str(&self.diagnostic());
        output.push('\n');
        output
    }

    pub fn render_json(&self) -> String {
        let status = if self.passed() { "PASS" } else { "FAIL" };
        let metrics = self
            .metrics
            .iter()
            .map(|metric| {
                format!(
                    "{{\"resource\":\"{}\",\"unit\":\"{}\",\"baseline\":{},\"tolerance\":{},\"final_rolling_median\":{},\"slope_verdict\":\"{}\"}}",
                    metric.kind.name(),
                    metric.kind.unit(),
                    metric.baseline,
                    metric.tolerance,
                    metric.final_rolling_median,
                    if metric.passed { "PASS" } else { "FAIL" },
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"status\":\"{status}\",\"samples\":{{\"collected\":{},\"capacity\":{},\"fill_percent\":{}}},\"warmup_samples\":{},\"rolling_window\":{},\"resources\":[{metrics}],\"diagnostic\":\"{}\"}}\n",
            self.samples_collected,
            self.config.capacity,
            self.fill_percent(),
            self.config.warmup_samples,
            self.config.rolling_window,
            self.diagnostic(),
        )
    }

    pub fn write_artifact(&self, directory: impl AsRef<Path>) -> Result<(), ResourceGateError> {
        let directory = directory.as_ref();
        fs::create_dir_all(directory).map_err(ResourceGateError::ArtifactWrite)?;
        fs::write(
            directory.join("resource-leak-soak-summary.md"),
            self.render_markdown(),
        )
        .map_err(ResourceGateError::ArtifactWrite)?;
        fs::write(
            directory.join("resource-leak-soak-summary.json"),
            self.render_json(),
        )
        .map_err(ResourceGateError::ArtifactWrite)
    }

    fn diagnostic(&self) -> String {
        if let Some(metric) = self.metrics.iter().find(|metric| !metric.passed) {
            return format!(
                "{} rolling median {} {} exceeds baseline {} {} + tolerance {} {}",
                metric.kind.name(),
                metric.final_rolling_median,
                metric.kind.unit(),
                metric.baseline,
                metric.kind.unit(),
                metric.tolerance,
                metric.kind.unit(),
            );
        }
        "all rolling medians are within their resource baseline + tolerance".to_owned()
    }
}

#[derive(Debug)]
pub enum ResourceGateError {
    InvalidConfiguration(String),
    InsufficientSamples { required: usize, collected: usize },
    CapacityExceeded { capacity: usize },
    Trend(Box<ResourceLeakSoakSummary>),
    ProcessRead { path: PathBuf, source: io::Error },
    ArtifactWrite(io::Error),
}

impl ResourceGateError {
    pub fn render_markdown(&self) -> String {
        match self {
            Self::Trend(summary) => summary.render_markdown(),
            _ => format!("# Rockstream resource-leak soak\n\nstatus: FAIL\n\ndiagnostic: {self}\n"),
        }
    }

    pub fn render_json(&self) -> String {
        match self {
            Self::Trend(summary) => summary.render_json(),
            _ => format!("{{\"status\":\"FAIL\",\"diagnostic\":\"{self}\"}}\n"),
        }
    }
}

impl fmt::Display for ResourceGateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid resource soak configuration: {message}")
            }
            Self::InsufficientSamples {
                required,
                collected,
            } => write!(
                formatter,
                "resource soak requires {required} samples but collected {collected}"
            ),
            Self::CapacityExceeded { capacity } => write!(
                formatter,
                "resource soak sample capacity {capacity} is full; refusing unbounded accumulation"
            ),
            Self::Trend(summary) => write!(formatter, "{}", summary.diagnostic()),
            Self::ProcessRead { path, source } => {
                write!(formatter, "failed to read {}: {source}", path.display())
            }
            Self::ArtifactWrite(source) => write!(
                formatter,
                "failed to write resource soak artifact: {source}"
            ),
        }
    }
}

impl std::error::Error for ResourceGateError {}

#[derive(Clone, Copy, Debug)]
pub struct ResourceSeriesGate {
    config: ResourceGateConfig,
}

impl ResourceSeriesGate {
    pub const fn new(config: ResourceGateConfig) -> Self {
        Self { config }
    }

    pub fn evaluate(
        &self,
        samples: &[ResourceSample],
    ) -> Result<ResourceLeakSoakSummary, ResourceGateError> {
        self.config.validate()?;
        if samples.len() > self.config.capacity {
            return Err(ResourceGateError::CapacityExceeded {
                capacity: self.config.capacity,
            });
        }
        let required = self.config.warmup_samples + self.config.rolling_window;
        if samples.len() < required {
            return Err(ResourceGateError::InsufficientSamples {
                required,
                collected: samples.len(),
            });
        }
        let metrics = ResourceKind::ALL.map(|kind| self.metric(samples, kind));
        let summary = ResourceLeakSoakSummary {
            config: self.config,
            samples_collected: samples.len(),
            metrics,
        };
        if summary.passed() {
            Ok(summary)
        } else {
            Err(ResourceGateError::Trend(Box::new(summary)))
        }
    }

    fn metric(&self, samples: &[ResourceSample], kind: ResourceKind) -> ResourceMetric {
        let baseline = median(
            samples[..self.config.warmup_samples]
                .iter()
                .map(|sample| sample.value(kind)),
        );
        let start = self.config.warmup_samples;
        let medians = samples[start..]
            .windows(self.config.rolling_window)
            .map(|window| median(window.iter().map(|sample| sample.value(kind))))
            .collect::<Vec<_>>();
        let final_rolling_median = *medians.last().expect("validated rolling window");
        let tolerance = self.config.tolerance(kind);
        ResourceMetric {
            kind,
            baseline,
            tolerance,
            final_rolling_median,
            passed: medians
                .into_iter()
                .all(|median| median <= baseline.saturating_add(tolerance)),
        }
    }
}

pub struct ProcessResourceSampler {
    pid: u32,
    samples: Vec<ResourceSample>,
    capacity: usize,
}

impl ProcessResourceSampler {
    pub fn new(
        pid: u32,
        duration_secs: u64,
        sample_interval_secs: u64,
    ) -> Result<Self, ResourceGateError> {
        if sample_interval_secs == 0 {
            return Err(ResourceGateError::InvalidConfiguration(
                "sample_interval_secs must be nonzero".to_owned(),
            ));
        }
        let capacity = ResourceGateConfig::max_samples(duration_secs, sample_interval_secs);
        Ok(Self {
            pid,
            samples: Vec::with_capacity(capacity),
            capacity,
        })
    }

    pub fn sample(&mut self, timestamp_secs: u64) -> Result<(), ResourceGateError> {
        if self.samples.len() == self.capacity {
            return Err(ResourceGateError::CapacityExceeded {
                capacity: self.capacity,
            });
        }
        self.samples.push(ResourceSample {
            timestamp_secs,
            rss_kib: read_rss_kib(self.pid)?,
            open_fds: count_open_fds(self.pid)?,
            open_sockets: count_open_sockets(self.pid)?,
        });
        Ok(())
    }

    pub fn samples(&self) -> &[ResourceSample] {
        &self.samples
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn fill_percent(&self) -> usize {
        self.samples.len() * 100 / self.capacity
    }
}

pub fn read_rss_kib(pid: u32) -> Result<u64, ResourceGateError> {
    let path = PathBuf::from(format!("/proc/{pid}/status"));
    let status = fs::read_to_string(&path).map_err(|source| ResourceGateError::ProcessRead {
        path: path.clone(),
        source,
    })?;
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .ok_or_else(|| {
            ResourceGateError::InvalidConfiguration(format!(
                "{} has no parseable VmRSS",
                path.display()
            ))
        })
}

pub fn count_open_fds(pid: u32) -> Result<u64, ResourceGateError> {
    let path = PathBuf::from(format!("/proc/{pid}/fd"));
    let mut entries = fs::read_dir(&path).map_err(|source| ResourceGateError::ProcessRead {
        path: path.clone(),
        source,
    })?;
    entries.try_fold(0_u64, |count, entry| {
        entry
            .map(|_| count + 1)
            .map_err(|source| ResourceGateError::ProcessRead {
                path: path.clone(),
                source,
            })
    })
}

pub fn count_open_sockets(pid: u32) -> Result<u64, ResourceGateError> {
    let path = PathBuf::from(format!("/proc/{pid}/fd"));
    let mut entries = fs::read_dir(&path).map_err(|source| ResourceGateError::ProcessRead {
        path: path.clone(),
        source,
    })?;
    entries.try_fold(0_u64, |count, entry| {
        let entry = entry.map_err(|source| ResourceGateError::ProcessRead {
            path: path.clone(),
            source,
        })?;
        let target =
            fs::read_link(entry.path()).map_err(|source| ResourceGateError::ProcessRead {
                path: path.clone(),
                source,
            })?;
        Ok(count + u64::from(target.to_string_lossy().starts_with("socket:[")))
    })
}

fn median(values: impl Iterator<Item = u64>) -> u64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2
    } else {
        values[middle]
    }
}
