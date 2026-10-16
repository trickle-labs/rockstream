use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentArea {
    OneKeyUpdates,
    CompatibleViews,
    WorkerScaling,
    ConcurrentLoad,
    BeyondRamAndCompaction,
    OverloadAndLoss,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatrixCellStatus {
    RunnableStandalone,
    Blocked {
        owning_milestone: String,
        blocker_description: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixCellDefinition {
    pub cell_id: String,
    pub area: ExperimentArea,
    pub description: String,
    pub status: MatrixCellStatus,
    pub lookup_cost_separated: bool,
    pub state_write_rule: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixExecutionResult {
    pub cell_id: String,
    pub area: ExperimentArea,
    pub status: String,
    pub owning_milestone: Option<String>,
    pub blocker_description: Option<String>,
    pub throughput_rows_per_sec: Option<f64>,
    pub read_p99_ms: Option<f64>,
    pub commit_p99_ms: Option<f64>,
    pub freshness_p99_ms: Option<f64>,
    pub state_writes_per_change: Option<f64>,
    pub lookup_cost_ms: Option<f64>,
    pub worker_pids: Vec<u32>,
    pub worker_active: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatrixReport {
    pub schema_version: u32,
    pub cells: Vec<MatrixExecutionResult>,
    pub all_areas_covered: bool,
    pub runnable_measured_count: usize,
    pub blocked_count: usize,
}

pub fn default_matrix_cells() -> Vec<MatrixCellDefinition> {
    vec![
        // Area 1: One-Key Updates
        MatrixCellDefinition {
            cell_id: "one-key-1k-standalone".to_string(),
            area: ExperimentArea::OneKeyUpdates,
            description: "One-key updates with 1K groups (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: true,
            state_write_rule: Some("approximately constant (ratio <= 1.10)".to_string()),
        },
        MatrixCellDefinition {
            cell_id: "one-key-100k-standalone".to_string(),
            area: ExperimentArea::OneKeyUpdates,
            description: "One-key updates with 100K groups (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: true,
            state_write_rule: Some("approximately constant (ratio <= 1.10)".to_string()),
        },
        MatrixCellDefinition {
            cell_id: "one-key-10m-beyond-ram".to_string(),
            area: ExperimentArea::OneKeyUpdates,
            description: "One-key updates with 10M groups (beyond RAM)".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67.1".to_string(),
                blocker_description:
                    "Requires spillable arrangements and state beyond RAM (v0.67.1)".to_string(),
            },
            lookup_cost_separated: true,
            state_write_rule: None,
        },
        // Area 2: Compatible Views
        MatrixCellDefinition {
            cell_id: "views-1-view-standalone".to_string(),
            area: ExperimentArea::CompatibleViews,
            description: "Compatible views with 1 view (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "views-20-views-standalone".to_string(),
            area: ExperimentArea::CompatibleViews,
            description: "Compatible views with 20 views unshared (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "views-shared-execution-benefit".to_string(),
            area: ExperimentArea::CompatibleViews,
            description: "Compatible views with shared execution benefit".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67".to_string(),
                blocker_description:
                    "Requires multi-consumer shared execution and direct data plane (v0.67)"
                        .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        // Area 3: Worker Scaling
        MatrixCellDefinition {
            cell_id: "worker-scaling-1-worker-standalone".to_string(),
            area: ExperimentArea::WorkerScaling,
            description: "Worker scaling with 1 actual worker (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "worker-scaling-2-workers".to_string(),
            area: ExperimentArea::WorkerScaling,
            description: "Worker scaling with 2 actual workers (distributed)".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67".to_string(),
                blocker_description:
                    "Requires distributed data plane without control-plane bottleneck (v0.67)"
                        .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "worker-scaling-4-workers".to_string(),
            area: ExperimentArea::WorkerScaling,
            description: "Worker scaling with 4 actual workers (distributed)".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67".to_string(),
                blocker_description:
                    "Requires distributed data plane without control-plane bottleneck (v0.67)"
                        .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "worker-scaling-8-workers".to_string(),
            area: ExperimentArea::WorkerScaling,
            description: "Worker scaling with 8 actual workers (distributed)".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67".to_string(),
                blocker_description:
                    "Requires distributed data plane without control-plane bottleneck (v0.67)"
                        .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        // Area 4: Concurrent Load
        MatrixCellDefinition {
            cell_id: "concurrent-ingestion-and-queries-standalone".to_string(),
            area: ExperimentArea::ConcurrentLoad,
            description: "Concurrent ingestion and queries (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        // Area 5: Beyond RAM, Compaction, and Restart
        MatrixCellDefinition {
            cell_id: "compaction-and-restart-standalone".to_string(),
            area: ExperimentArea::BeyondRamAndCompaction,
            description: "Beyond RAM, compaction, and restart (standalone)".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "state-beyond-ram-spillable".to_string(),
            area: ExperimentArea::BeyondRamAndCompaction,
            description: "State larger than RAM spillable arrangements".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67.1".to_string(),
                blocker_description:
                    "Requires spillable arrangements and paging to completion (v0.67.1)".to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        // Area 6: Overload and Loss
        MatrixCellDefinition {
            cell_id: "standalone-overload-and-backpressure".to_string(),
            area: ExperimentArea::OverloadAndLoss,
            description: "Standalone overload under bounded backpressure".to_string(),
            status: MatrixCellStatus::RunnableStandalone,
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "worker-loss-failover".to_string(),
            area: ExperimentArea::OverloadAndLoss,
            description: "Worker loss and failover".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.67".to_string(),
                blocker_description: "Requires distributed worker supervision and failover (v0.67)"
                    .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
        MatrixCellDefinition {
            cell_id: "shard-migration".to_string(),
            area: ExperimentArea::OverloadAndLoss,
            description: "Shard migration".to_string(),
            status: MatrixCellStatus::Blocked {
                owning_milestone: "v0.68".to_string(),
                blocker_description: "Requires durable distributed migration sagas (v0.68)"
                    .to_string(),
            },
            lookup_cost_separated: false,
            state_write_rule: None,
        },
    ]
}

fn metric(value: Option<f64>, name: &str, cell: &str, positive: bool) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("missing {name} in cell {cell}"))?;
    if !value.is_finite() || value < 0.0 || (positive && value == 0.0) {
        bail!("invalid {name} in cell {cell}");
    }
    Ok(value)
}

/// Validate the historical matrix's complete structure and measured values.
/// Historical blockers remain reportable here, but cannot qualify v0.72.
/// PIDs and summary metrics alone are not proof of a real process measurement.
pub fn verify_matrix(report: &MatrixReport) -> Result<()> {
    if report.schema_version != 1 {
        bail!("matrix report schema_version must be 1");
    }

    let defined = default_matrix_cells();
    let by_id: HashMap<_, _> = defined
        .iter()
        .map(|cell| (cell.cell_id.as_str(), cell))
        .collect();
    let mut reported = HashSet::new();
    for cell in &report.cells {
        if !by_id.contains_key(cell.cell_id.as_str()) {
            bail!("unknown matrix cell: {}", cell.cell_id);
        }
        if !reported.insert(cell.cell_id.as_str()) {
            bail!("duplicate matrix cell: {}", cell.cell_id);
        }
    }
    // Report omissions in contract order, not randomized hash-map iteration order.
    for cell in &defined {
        if !reported.contains(cell.cell_id.as_str()) {
            bail!("missing required matrix cell: {}", cell.cell_id);
        }
    }

    let mut measured = 0;
    let mut blocked = 0;
    let mut writes = Vec::new();
    for cell in &report.cells {
        let definition = by_id[cell.cell_id.as_str()];
        if cell.area != definition.area {
            bail!("wrong experiment area for cell {}", cell.cell_id);
        }
        match cell.status.as_str() {
            "MEASURED" => {
                if !cell.worker_active {
                    bail!("inactive worker process detected in cell {}", cell.cell_id);
                }
                if cell.worker_pids.is_empty() {
                    bail!("no worker PIDs recorded for cell {}", cell.cell_id);
                }
                let mut pids = HashSet::new();
                if cell
                    .worker_pids
                    .iter()
                    .any(|pid| *pid == 0 || !pids.insert(*pid))
                {
                    bail!("zero or duplicate worker PID in cell {}", cell.cell_id);
                }
                let expected_workers = match cell.cell_id.as_str() {
                    "worker-scaling-1-worker-standalone" => Some(1),
                    "worker-scaling-2-workers" => Some(2),
                    "worker-scaling-4-workers" => Some(4),
                    "worker-scaling-8-workers" => Some(8),
                    _ => None,
                };
                if let Some(expected) = expected_workers {
                    if cell.worker_pids.len() != expected {
                        bail!(
                            "cell {} requires {expected} distinct worker PIDs",
                            cell.cell_id
                        );
                    }
                }
                if cell.error.is_some()
                    || cell.owning_milestone.is_some()
                    || cell.blocker_description.is_some()
                {
                    bail!("measured cell {} carries an error or blocker", cell.cell_id);
                }
                metric(
                    cell.throughput_rows_per_sec,
                    "throughput_rows_per_sec",
                    &cell.cell_id,
                    true,
                )?;
                for (name, value) in [
                    ("read_p99_ms", cell.read_p99_ms),
                    ("commit_p99_ms", cell.commit_p99_ms),
                    ("freshness_p99_ms", cell.freshness_p99_ms),
                ] {
                    metric(value, name, &cell.cell_id, false)?;
                }
                if definition.lookup_cost_separated {
                    metric(cell.lookup_cost_ms, "lookup_cost_ms", &cell.cell_id, false)?;
                    writes.push(metric(
                        cell.state_writes_per_change,
                        "state_writes_per_change",
                        &cell.cell_id,
                        true,
                    )?);
                } else {
                    for (name, value) in [
                        ("lookup_cost_ms", cell.lookup_cost_ms),
                        ("state_writes_per_change", cell.state_writes_per_change),
                    ] {
                        if value.is_some() {
                            metric(value, name, &cell.cell_id, false)?;
                        }
                    }
                }
                measured += 1;
            }
            "BLOCKED" => {
                let owner = cell
                    .owning_milestone
                    .as_deref()
                    .filter(|owner| !owner.trim().is_empty());
                let Some(owner) = owner else {
                    bail!(
                        "unowned blocked cell {} missing owning milestone",
                        cell.cell_id
                    );
                };
                if !owner.starts_with("v0.") {
                    bail!(
                        "blocked cell {} has invalid owning milestone {}",
                        cell.cell_id,
                        owner
                    );
                }
                if cell
                    .blocker_description
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty())
                {
                    bail!("blocked cell {} missing blocker description", cell.cell_id);
                }
                if matches!(definition.status, MatrixCellStatus::RunnableStandalone) {
                    bail!(
                        "runnable cell {} cannot use a historical blocker",
                        cell.cell_id
                    );
                }
                if cell.throughput_rows_per_sec.is_some()
                    || cell.read_p99_ms.is_some()
                    || cell.commit_p99_ms.is_some()
                    || cell.freshness_p99_ms.is_some()
                    || cell.state_writes_per_change.is_some()
                    || cell.lookup_cost_ms.is_some()
                    || !cell.worker_pids.is_empty()
                    || cell.worker_active
                    || cell.error.is_some()
                {
                    bail!(
                        "blocked cell {} must not claim measured evidence",
                        cell.cell_id
                    );
                }
                blocked += 1;
            }
            status => bail!(
                "unsupported matrix cell status {status:?} for {}",
                cell.cell_id
            ),
        }
    }

    let areas: HashSet<_> = report.cells.iter().map(|cell| cell.area).collect();
    if areas.len() != 6 || !report.all_areas_covered {
        bail!("matrix report all_areas_covered does not match its cells");
    }
    if measured != report.runnable_measured_count || blocked != report.blocked_count {
        bail!("matrix report counts do not match its cells");
    }
    // Every measured one-key case participates, including 10M when supplied.
    let minimum = writes.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = writes.iter().copied().fold(0.0, f64::max);
    if maximum / minimum > 1.10 {
        bail!("one-key write amplification ratio exceeds frozen threshold 1.10");
    }
    Ok(())
}

/// v0.72 makes all sixteen named local matrix cells applicable. Earlier
/// milestone blockers are historical records, not an acceptance exemption.
/// This checks matrix completeness, not raw-evidence provenance or capacity.
pub fn verify_v072_matrix(report: &MatrixReport) -> Result<()> {
    verify_matrix(report)?;
    for cell in &report.cells {
        if cell.status != "MEASURED" {
            bail!("v0.72 applicable cell {} is not measured", cell.cell_id);
        }
    }
    Ok(())
}
