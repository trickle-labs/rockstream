use crate::metrics::{
    fetch, operator_counters, process_snapshot, worker_activity, Metric, ProcessSnapshot,
    WorkerActivity,
};
use crate::process::ProcessGroup;
use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

pub struct Cluster {
    pub pgwire_addr: String,
    processes: Vec<Node>,
    worker_metrics: Vec<(u32, String)>,
    state_dirs: Vec<PathBuf>,
    _state_root: tempfile::TempDir,
}

struct Node {
    role: String,
    process: ProcessGroup,
}

impl Cluster {
    pub async fn start(
        binary: &Path,
        worker_count: usize,
        run_dir: &Path,
        config: Option<&Path>,
        standalone: bool,
    ) -> Result<Self> {
        fs::create_dir_all(run_dir)
            .with_context(|| format!("create run directory {}", run_dir.display()))?;
        let state_root = tempfile::Builder::new()
            .prefix("rockstream-r1-local-")
            .tempdir()
            .context("create temporary LFS root")?;
        let mut allocated = BTreeSet::new();
        let pgwire_addr = free_addr(&mut allocated)?;
        if standalone {
            let state = state_root.path().join("all");
            fs::create_dir_all(&state)?;
            let metrics = free_addr(&mut allocated)?;
            let process = spawn_node(
                binary,
                &[
                    "start",
                    "--storage",
                    path(&state)?,
                    "--role",
                    "all",
                    "--listen",
                    &pgwire_addr,
                    "--metrics-addr",
                    &metrics,
                ],
                config,
                run_dir,
                "all",
            )?;
            return Ok(Self {
                pgwire_addr,
                processes: vec![Node {
                    role: "all".to_string(),
                    process,
                }],
                worker_metrics: Vec::new(),
                state_dirs: vec![state],
                _state_root: state_root,
            });
        }

        let control_addr = free_addr(&mut allocated)?;
        let control_metrics = free_addr(&mut allocated)?;
        let control_state = state_root.path().join("control");
        fs::create_dir_all(&control_state)?;
        let control = spawn_node(
            binary,
            &[
                "start",
                "--storage",
                path(&control_state)?,
                "--role",
                "control",
                "--control-bind",
                &control_addr,
                "--metrics-addr",
                &control_metrics,
                "--daemon",
            ],
            config,
            run_dir,
            "control",
        )?;
        let mut processes = vec![Node {
            role: "control".to_string(),
            process: control,
        }];
        let mut worker_metrics = Vec::with_capacity(worker_count);
        let mut state_dirs = vec![control_state.clone()];
        for index in 0..worker_count {
            let worker_id = (index + 1).to_string();
            let state = state_root.path().join(format!("worker-{worker_id}"));
            fs::create_dir_all(&state)?;
            let metrics = free_addr(&mut allocated)?;
            let process = spawn_node(
                binary,
                &[
                    "start",
                    "--storage",
                    path(&state)?,
                    "--role",
                    "worker",
                    "--control",
                    &control_addr,
                    "--worker-id",
                    &worker_id,
                    "--metrics-addr",
                    &metrics,
                ],
                config,
                run_dir,
                &format!("worker-{}", index + 1),
            )?;
            worker_metrics.push((process.pid() as u32, metrics));
            processes.push(Node {
                role: "worker".to_string(),
                process,
            });
            state_dirs.push(state);
        }
        wait_for_registrations(&control_state.join("audit.jsonl"), worker_count).await?;
        let gateway_state = state_root.path().join("gateway");
        fs::create_dir_all(&gateway_state)?;
        let gateway_metrics = free_addr(&mut allocated)?;
        let gateway = spawn_node(
            binary,
            &[
                "start",
                "--storage",
                path(&gateway_state)?,
                "--role",
                "gateway",
                "--control",
                &control_addr,
                "--listen",
                &pgwire_addr,
                "--metrics-addr",
                &gateway_metrics,
            ],
            config,
            run_dir,
            "gateway",
        )?;
        processes.push(Node {
            role: "gateway".to_string(),
            process: gateway,
        });
        state_dirs.push(gateway_state);
        Ok(Self {
            pgwire_addr,
            processes,
            worker_metrics,
            state_dirs,
            _state_root: state_root,
        })
    }

    pub fn process_snapshots(&self) -> Result<Vec<(String, u32, ProcessSnapshot)>> {
        self.processes
            .iter()
            .map(|node| {
                let pid = node.process.pid() as u32;
                Ok((node.role.clone(), pid, process_snapshot(pid)?))
            })
            .collect()
    }

    pub async fn observed_workers(&self) -> Result<(Vec<WorkerActivity>, BTreeMap<String, u64>)> {
        let mut workers = Vec::with_capacity(self.worker_metrics.len());
        let mut all_metrics: Vec<Metric> = Vec::new();
        for (pid, address) in &self.worker_metrics {
            let metrics = fetch(address).await?;
            workers.push(worker_activity(&metrics, *pid)?);
            all_metrics.extend(metrics);
        }
        workers.sort_by_key(|worker| worker.worker_id);
        if workers
            .windows(2)
            .any(|pair| pair[0].worker_id == pair[1].worker_id)
        {
            bail!("worker identity changed or was duplicated");
        }
        Ok((workers, operator_counters(&all_metrics)))
    }

    pub fn lfs_bytes(&self) -> Result<u64> {
        self.state_dirs
            .iter()
            .try_fold(0, |total, path| Ok(total + allocated_bytes(path)?))
    }
}

fn spawn_node(
    binary: &Path,
    args: &[&str],
    config: Option<&Path>,
    run_dir: &Path,
    name: &str,
) -> Result<ProcessGroup> {
    let mut command = Command::new(binary);
    command.args(args);
    if let Some(config) = config {
        command.env("ROCKSTREAM_CONFIG", config);
    }
    ProcessGroup::spawn(
        &mut command,
        run_dir.join(format!("{name}.stdout.log")),
        run_dir.join(format!("{name}.stderr.log")),
    )
    .with_context(|| format!("start {name} from {}", binary.display()))
}

async fn wait_for_registrations(audit: &Path, expected: usize) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut delay = Duration::from_millis(20);
    loop {
        let registrations = fs::read_to_string(audit)
            .unwrap_or_default()
            .matches("\"action\":\"worker.registered\"")
            .count();
        if registrations >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("only {registrations} of {expected} workers registered");
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_millis(500));
    }
}

fn free_addr(allocated: &mut BTreeSet<String>) -> Result<String> {
    loop {
        let address = TcpListener::bind("127.0.0.1:0")?.local_addr()?.to_string();
        if allocated.insert(address.clone()) {
            return Ok(address);
        }
    }
}

fn path(path: &Path) -> Result<&str> {
    path.to_str().context("run path is not UTF-8")
}

fn allocated_bytes(path: &Path) -> Result<u64> {
    fs::read_dir(path)?.try_fold(0, |total, entry| {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            Ok(total + allocated_bytes(&entry.path())?)
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                Ok(total + metadata.blocks() * 512)
            }
            #[cfg(not(unix))]
            {
                Ok(total + metadata.len())
            }
        }
    })
}
