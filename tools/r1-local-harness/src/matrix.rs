use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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

pub fn verify_matrix(report: &MatrixReport) -> Result<()> {
    if report.schema_version != 1 {
        bail!("matrix report schema_version must be 1");
    }

    let defined_cells = default_matrix_cells();
    let defined_ids: HashSet<String> = defined_cells.iter().map(|c| c.cell_id.clone()).collect();
    let reported_ids: HashSet<String> = report.cells.iter().map(|c| c.cell_id.clone()).collect();

    for id in &defined_ids {
        if !reported_ids.contains(id) {
            bail!("missing required matrix cell: {}", id);
        }
    }

    let areas: HashSet<ExperimentArea> = report.cells.iter().map(|c| c.area).collect();
    if areas.len() < 6 {
        bail!(
            "matrix report does not cover all 6 experiment areas (covered: {})",
            areas.len()
        );
    }

    for cell in &report.cells {
        if cell.status == "MEASURED" {
            if !cell.worker_active {
                bail!("inactive worker process detected in cell {}", cell.cell_id);
            }
            if cell.worker_pids.is_empty() {
                bail!("no worker PIDs recorded for cell {}", cell.cell_id);
            }
        } else if cell.status == "BLOCKED" {
            let owner = match &cell.owning_milestone {
                Some(o) if !o.is_empty() => o,
                _ => bail!(
                    "unowned blocked cell {} missing owning milestone",
                    cell.cell_id
                ),
            };
            if !owner.starts_with("v0.") {
                bail!(
                    "blocked cell {} has invalid owning milestone {}",
                    cell.cell_id,
                    owner
                );
            }
            let blocker = match &cell.blocker_description {
                Some(b) if !b.is_empty() => b,
                _ => bail!("blocked cell {} missing blocker description", cell.cell_id),
            };
            if blocker.trim().is_empty() {
                bail!(
                    "blocked cell {} has empty blocker description",
                    cell.cell_id
                );
            }
            if cell.throughput_rows_per_sec.is_some() {
                bail!(
                    "blocked cell {} must not claim measured throughput",
                    cell.cell_id
                );
            }
        }
    }

    // Check write amplification rule between 1K and 100K if both are measured
    let cell_1k = report
        .cells
        .iter()
        .find(|c| c.cell_id == "one-key-1k-standalone" && c.status == "MEASURED");
    let cell_100k = report
        .cells
        .iter()
        .find(|c| c.cell_id == "one-key-100k-standalone" && c.status == "MEASURED");
    if let (Some(c1k), Some(c100k)) = (cell_1k, cell_100k) {
        if let (Some(w1k), Some(w100k)) =
            (c1k.state_writes_per_change, c100k.state_writes_per_change)
        {
            if w1k > 0.0 && w100k > 0.0 {
                let ratio = if w100k > w1k {
                    w100k / w1k
                } else {
                    w1k / w100k
                };
                if ratio > 1.10 {
                    bail!(
                        "write amplification ratio {:.3} between 1K and 100K exceeds frozen threshold 1.10",
                        ratio
                    );
                }
            }
        }
    }

    Ok(())
}
