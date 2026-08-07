//! CLI adapter for the bounded resource-leak trend gate.

use std::{env, fs, path::PathBuf, process::ExitCode};

use rockstream_sim::{ResourceGateConfig, ResourceSample, ResourceSeriesGate};

fn argument(name: &str) -> Result<String, String> {
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments
                .next()
                .ok_or_else(|| format!("{name} requires a value"));
        }
    }
    Err(format!("missing required argument {name}"))
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("{name} must be an unsigned integer"))
        })
        .unwrap_or(Ok(default))
}

fn samples(path: &str) -> Result<Vec<ResourceSample>, String> {
    fs::read_to_string(path)
        .map_err(|error| format!("failed to read {path}: {error}"))?
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields.len() != 4 {
                return Err(format!(
                    "invalid resource sample `{line}`; expected timestamp, RSS, FD, socket"
                ));
            }
            Ok(ResourceSample {
                timestamp_secs: fields[0]
                    .parse()
                    .map_err(|_| format!("invalid sample timestamp in `{line}`"))?,
                rss_kib: fields[1]
                    .parse()
                    .map_err(|_| format!("invalid RSS sample in `{line}`"))?,
                open_fds: fields[2]
                    .parse()
                    .map_err(|_| format!("invalid FD sample in `{line}`"))?,
                open_sockets: fields[3]
                    .parse()
                    .map_err(|_| format!("invalid socket sample in `{line}`"))?,
            })
        })
        .collect()
}

fn run() -> Result<(), String> {
    let samples_file = argument("--samples-file")?;
    let artifact_dir = PathBuf::from(argument("--artifact-dir")?);
    let duration_secs = env_u64("ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS", 14_400)?;
    let sample_interval_secs = env_u64("ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS", 60)?;
    if sample_interval_secs == 0 {
        return Err("ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS must be nonzero".to_owned());
    }
    let config = ResourceGateConfig {
        capacity: ResourceGateConfig::max_samples(duration_secs, sample_interval_secs),
        warmup_samples: env_u64("ROCKSTREAM_RESOURCE_SOAK_WARMUP_SAMPLES", 3)? as usize,
        rolling_window: env_u64("ROCKSTREAM_RESOURCE_SOAK_ROLLING_WINDOW", 3)? as usize,
        rss_tolerance_kib: env_u64("ROCKSTREAM_RESOURCE_SOAK_RSS_TOLERANCE_KIB", 262_144)?,
        open_fd_tolerance: env_u64("ROCKSTREAM_RESOURCE_SOAK_OPEN_FD_TOLERANCE", 128)?,
        open_socket_tolerance: env_u64("ROCKSTREAM_RESOURCE_SOAK_OPEN_SOCKET_TOLERANCE", 128)?,
    };
    let result = ResourceSeriesGate::new(config).evaluate(&samples(&samples_file)?);
    match result {
        Ok(summary) => summary
            .write_artifact(&artifact_dir)
            .map_err(|error| error.to_string()),
        Err(error) => {
            fs::create_dir_all(&artifact_dir).map_err(|write_error| {
                format!("failed to create {}: {write_error}", artifact_dir.display())
            })?;
            fs::write(
                artifact_dir.join("resource-leak-soak-summary.md"),
                error.render_markdown(),
            )
            .map_err(|write_error| format!("failed to write Markdown artifact: {write_error}"))?;
            fs::write(
                artifact_dir.join("resource-leak-soak-summary.json"),
                error.render_json(),
            )
            .map_err(|write_error| format!("failed to write JSON artifact: {write_error}"))?;
            Err(error.to_string())
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("RS-0002: {error}. next_steps: inspect resource-leak-soak-summary.md");
            ExitCode::FAILURE
        }
    }
}
