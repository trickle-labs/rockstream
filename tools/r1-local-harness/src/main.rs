use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use r1_local_harness::artifact::{append_jsonl, atomic_json, canonical_rows, sha256, sha256_file};
use r1_local_harness::cluster::Cluster;
use r1_local_harness::corpus::{canonical_changes_json, canonical_input_json, generate};
use r1_local_harness::evidence::{
    FreshnessHistogram, ProcessUsage, RawSample, StructuralEvidence, StructuralResult,
};
use r1_local_harness::{load, oracle, report};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "r1-local-harness")]
struct Cli {
    #[command(subcommand)]
    command: HarnessCommand,
}

#[derive(Subcommand)]
enum HarnessCommand {
    Prepare {
        #[arg(long)]
        profile: PathBuf,
        #[arg(long)]
        corpus: PathBuf,
    },
    BuildCandidates {
        #[arg(long)]
        output: PathBuf,
    },
    Structural {
        #[arg(long)]
        output: PathBuf,
    },
    Run {
        #[arg(long)]
        workload: String,
        #[arg(long)]
        workers: usize,
        #[arg(long)]
        output: PathBuf,
    },
    Evaluate {
        #[arg(long)]
        evidence: PathBuf,
    },
    Verify {
        #[arg(long)]
        evidence: PathBuf,
    },
}

#[derive(Debug, Deserialize)]
struct CorpusConfig {
    load: LoadConfig,
    freshness: FreshnessConfig,
    generation: GenerationConfig,
    repetitions: RepetitionConfig,
}

#[derive(Debug, Deserialize)]
struct LoadConfig {
    lanes: usize,
    transaction_rows: usize,
    warm_up_seconds: u64,
    measurement_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct FreshnessConfig {
    histogram_buckets_ms: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct GenerationConfig {
    change_rows_per_workload: usize,
}

#[derive(Debug, Deserialize)]
struct RepetitionConfig {
    count: usize,
    order: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WorkloadConfig {
    name: String,
    seed: u64,
    source_rows: Option<usize>,
    dimension_rows: Option<usize>,
    changed_rows: Option<usize>,
    live_groups: Option<usize>,
    sql: String,
}

#[derive(Debug, Deserialize)]
struct CandidateRecord {
    candidates: Vec<Candidate>,
}

#[derive(Debug, Clone, Deserialize)]
struct Candidate {
    id: String,
    kind: String,
    binary_path: PathBuf,
    binary_sha256: String,
}

#[derive(Debug, Clone)]
struct Side {
    candidate: Candidate,
    strategy: String,
    join_strategy: String,
    sql: String,
}

#[derive(Serialize)]
struct PreparedArtifact {
    schema_version: u32,
    source_sha256: String,
    value: JsonValue,
}

#[derive(Serialize)]
struct FailureArtifact<'a> {
    error: String,
    canonical_input_sha256: &'a str,
    rockstream_output_sha256: &'a str,
    sqlite_oracle_output_sha256: &'a str,
    rockstream_rows: &'a [Vec<String>],
    sqlite_oracle_rows: &'a [Vec<String>],
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        HarnessCommand::Prepare { profile, corpus } => prepare(&profile, &corpus),
        HarnessCommand::BuildCandidates { output } => build_candidates(&output),
        HarnessCommand::Structural { output } => structural(&output),
        HarnessCommand::Run {
            workload,
            workers,
            output,
        } => run_workload(&workload, workers, &output).await,
        HarnessCommand::Evaluate { evidence } => {
            let decision = report::write_decision(&evidence)?;
            println!("{}", serde_json::to_string(&decision)?);
            Ok(())
        }
        HarnessCommand::Verify { evidence } => {
            report::verify(&evidence)?;
            println!("R1 local evidence regenerates exactly");
            Ok(())
        }
    }
}

fn prepare(profile: &Path, corpus: &Path) -> Result<()> {
    let root = repository_root(profile)?;
    let output = root.join("evidence/r1-local");
    let profile_bytes = fs::read(profile).with_context(|| format!("read {}", profile.display()))?;
    let corpus_bytes = fs::read(corpus).with_context(|| format!("read {}", corpus.display()))?;
    let profile_value: toml::Value =
        toml::from_slice(&profile_bytes).context("parse profile TOML")?;
    let corpus_value: toml::Value = toml::from_slice(&corpus_bytes).context("parse corpus TOML")?;
    atomic_json(
        &output.join("profile.json"),
        &PreparedArtifact {
            schema_version: 1,
            source_sha256: sha256(&profile_bytes),
            value: serde_json::to_value(profile_value)?,
        },
    )?;
    atomic_json(
        &output.join("corpus.json"),
        &PreparedArtifact {
            schema_version: 1,
            source_sha256: sha256(&corpus_bytes),
            value: serde_json::to_value(corpus_value)?,
        },
    )?;
    println!("prepared R1 profile and corpus in {}", output.display());
    Ok(())
}

fn build_candidates(output: &Path) -> Result<()> {
    let root = repository_root(output)?;
    fs::create_dir_all(output)?;
    let status = Command::new("python3")
        .args(["scripts/r1-local-candidates.py", "record", "--root"])
        .arg(&root)
        .arg("--output")
        .arg(output.join("candidates.json"))
        .arg("--artifact-dir")
        .arg(output.join("artifacts"))
        .current_dir(&root)
        .status()
        .context("run candidate builder")?;
    if !status.success() {
        bail!("candidate builder exited with {status}");
    }
    Ok(())
}

fn structural(output: &Path) -> Result<()> {
    let root = repository_root(output)?;
    fs::create_dir_all(output)?;
    let tests: [(&str, &[&str]); 3] = [
        (
            "one-key-persistence",
            &[
                "test",
                "-p",
                "rockstream-ops",
                "--release",
                "--test",
                "constant_write_amplification_scale_tests",
            ],
        ),
        (
            "shared-arrangement",
            &[
                "test",
                "-p",
                "rockstream-sim",
                "--release",
                "--test",
                "scale_proof_20_views_sharing_tests",
            ],
        ),
        (
            "factorized-join",
            &[
                "test",
                "-p",
                "rockstream-ops",
                "--release",
                "--test",
                "factorized_join_aggregate_oracle_tests",
                "fanout_100_classic_and_factorized_outputs_and_work_are_exact",
            ],
        ),
    ];
    let mut results = Vec::with_capacity(tests.len());
    for (name, args) in tests {
        let result = Command::new("cargo")
            .args(args)
            .current_dir(&root)
            .output()
            .with_context(|| format!("run structural proof {name}"))?;
        let mut log = result.stdout;
        log.extend_from_slice(&result.stderr);
        fs::write(output.join(format!("{name}.log")), &log)?;
        if !result.status.success() {
            bail!("structural proof {name} failed; see {name}.log");
        }
        let proof_digest = sha256(&log);
        results.push(StructuralResult {
            name: name.to_string(),
            passed: true,
            counters: BTreeMap::from([("exact_proof_passed".to_string(), 1)]),
            log_sha256: proof_digest,
        });
    }
    atomic_json(
        &output.join("structural-results.json"),
        &StructuralEvidence {
            schema_version: 1,
            results,
        },
    )?;
    println!("recorded three structural proofs in {}", output.display());
    Ok(())
}

async fn run_workload(name: &str, workers: usize, output: &Path) -> Result<()> {
    if workers == 0 {
        bail!("--workers must be nonzero");
    }
    let root = repository_root(output)?;
    let benchmark = root.join("benchmarks/r1-local");
    let corpus_path = benchmark.join("corpus.toml");
    let thresholds_path = benchmark.join("thresholds.toml");
    let profile_path = benchmark.join("profile.toml");
    let corpus_config: CorpusConfig = toml::from_str(&fs::read_to_string(&corpus_path)?)?;
    if corpus_config.repetitions.count != corpus_config.repetitions.order.len() {
        bail!("corpus repetition count and order length differ");
    }
    let workload_path = benchmark.join("workloads").join(format!("{name}.toml"));
    let workload: WorkloadConfig = toml::from_str(&fs::read_to_string(&workload_path)?)?;
    if workload.name != name {
        bail!("workload file name is {}, expected {name}", workload.name);
    }
    let source_rows = workload
        .source_rows
        .context("structural workload must use the structural command")?;
    let dimension_rows = workload.dimension_rows.unwrap_or(1);
    let live_groups = workload.live_groups.unwrap_or(source_rows);
    let changed_rows = workload
        .changed_rows
        .unwrap_or(corpus_config.generation.change_rows_per_workload);
    let generated = generate(
        workload.seed,
        source_rows,
        dimension_rows,
        live_groups as u64,
        changed_rows,
    );
    let input_sha256 = sha256(&canonical_input_json(&generated));
    let change_sha256 = sha256(&canonical_changes_json(&generated.changes));
    let candidates: CandidateRecord =
        serde_json::from_slice(&fs::read(output.join("candidates.json"))?)?;
    let sides = comparison_sides(name, &candidates, &workload)?;
    for repetition in 0..corpus_config.repetitions.count {
        let mut ordered = sides.clone();
        if corpus_config.repetitions.order[repetition] == "b_then_a" {
            ordered.reverse();
        }
        for side in ordered {
            let pair_id = format!("{name}-{workers}-pair-{}", repetition + 1);
            let run_id = format!("{pair_id}-{}-{}", side.candidate.id, side.strategy);
            let sql = fs::read_to_string(benchmark.join(&side.sql))?;
            let (view, oracle_query) = oracle::admitted_query(&sql)?;
            let sample = run_side(
                &root,
                output,
                workers,
                repetition,
                &run_id,
                &pair_id,
                &side,
                &corpus_config,
                &workload,
                &generated,
                &sql,
                &view,
                &oracle_query,
                &input_sha256,
                &change_sha256,
                &profile_path,
                &corpus_path,
                &thresholds_path,
            )
            .await?;
            append_jsonl(&output.join("raw-samples.jsonl"), &sample)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_side(
    root: &Path,
    output: &Path,
    workers: usize,
    repetition: usize,
    run_id: &str,
    pair_id: &str,
    side: &Side,
    corpus_config: &CorpusConfig,
    workload: &WorkloadConfig,
    generated: &r1_local_harness::corpus::Corpus,
    sql: &str,
    view: &str,
    oracle_query: &str,
    input_sha256: &str,
    change_sha256: &str,
    profile_path: &Path,
    corpus_path: &Path,
    thresholds_path: &Path,
) -> Result<RawSample> {
    let binary = root.join(&side.candidate.binary_path);
    if sha256_file(&binary)? != side.candidate.binary_sha256 {
        bail!("candidate binary digest changed for {}", side.candidate.id);
    }
    let run_dir = output.join("runs").join(run_id);
    let config_path = run_dir.join("rockstream.toml");
    fs::create_dir_all(&run_dir)?;
    fs::write(
        &config_path,
        format!("[execution]\njoin_strategy = \"{}\"\n", side.join_strategy),
    )?;
    let cluster = Cluster::start(
        &binary,
        workers,
        &run_dir,
        Some(&config_path),
        side.candidate.kind == "baseline_rebuild",
    )
    .await?;
    let prepared = load::prepare(
        &cluster.pgwire_addr,
        sql,
        view,
        generated,
        corpus_config.load.transaction_rows,
        Duration::from_secs(corpus_config.load.warm_up_seconds),
    )
    .await?;
    let before = cluster.process_snapshots()?;
    let (workers_before, operator_counters_before) = cluster.observed_workers().await?;
    let loaded = load::execute(
        prepared,
        &cluster.pgwire_addr,
        view,
        generated,
        corpus_config.load.lanes,
        corpus_config.load.transaction_rows,
        Duration::from_secs(corpus_config.load.measurement_seconds),
        &corpus_config.freshness.histogram_buckets_ms,
    )
    .await?;
    let (rockstream_rows, rockstream_sha256) = canonical_rows(loaded.rows)?;
    let mut expected = generated.clone();
    expected.changes = loaded.final_changes.clone();
    let (oracle_rows, oracle_sha256) =
        canonical_rows(oracle::complete_output(&expected, oracle_query)?)?;
    let result: Result<RawSample> = async {
        let after = cluster.process_snapshots()?;
        let (observed_workers, operator_counters) = cluster.observed_workers().await?;
        if workers_before
            .iter()
            .map(|worker| (worker.worker_id, worker.pid))
            .ne(observed_workers
                .iter()
                .map(|worker| (worker.worker_id, worker.pid)))
        {
            bail!("worker identity changed during repetition");
        }
        let processes = before
            .into_iter()
            .zip(after)
            .map(|((role, pid, before), (after_role, after_pid, after))| {
                if role != after_role || pid != after_pid {
                    bail!("process identity changed during repetition");
                }
                Ok(ProcessUsage {
                    role,
                    pid,
                    user_cpu_ns: after.user_cpu_ns.saturating_sub(before.user_cpu_ns),
                    system_cpu_ns: after.system_cpu_ns.saturating_sub(before.system_cpu_ns),
                    rss_bytes: after.rss_bytes,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let measurement_workers = observed_workers
            .iter()
            .zip(&workers_before)
            .map(
                |(after, before)| r1_local_harness::metrics::WorkerActivity {
                    worker_id: after.worker_id,
                    pid: after.pid,
                    shards_owned: after.shards_owned,
                    input_rows: after.input_rows.saturating_sub(before.input_rows),
                    output_rows: after.output_rows.saturating_sub(before.output_rows),
                    state_writes: after.state_writes.saturating_sub(before.state_writes),
                    exchange_bytes: after.exchange_bytes.saturating_sub(before.exchange_bytes),
                },
            )
            .collect::<Vec<_>>();
        let operator_counters = operator_counters
            .into_iter()
            .map(|(name, after)| {
                (
                    name.clone(),
                    after.saturating_sub(*operator_counters_before.get(&name).unwrap_or(&0)),
                )
            })
            .collect();
        let logical_bytes = loaded.logical_bytes;
        let exchange_bytes = measurement_workers
            .iter()
            .map(|worker| worker.exchange_bytes)
            .sum();
        let sample = RawSample {
            schema_version: 1,
            run_id: run_id.to_string(),
            pair_id: pair_id.to_string(),
            order: corpus_config.repetitions.order[repetition].clone(),
            candidate_id: side.candidate.id.clone(),
            binary_sha256: side.candidate.binary_sha256.clone(),
            profile_sha256: sha256_file(profile_path)?,
            corpus_sha256: sha256_file(corpus_path)?,
            thresholds_sha256: sha256_file(thresholds_path)?,
            workload: workload.name.clone(),
            strategy: side.strategy.clone(),
            worker_count: if side.candidate.kind == "baseline_rebuild" {
                0
            } else {
                workers as u32
            },
            seed: workload.seed,
            change_stream_sha256: change_sha256.to_string(),
            monotonic_duration_ns: loaded
                .duration
                .as_nanos()
                .try_into()
                .context("duration exceeds u64")?,
            accepted_changes: loaded.accepted_changes,
            visible_changes: loaded.visible_changes,
            freshness_histogram: FreshnessHistogram {
                upper_bounds_ms: corpus_config.freshness.histogram_buckets_ms.clone(),
                counts: loaded.freshness_counts,
            },
            processes,
            logical_bytes,
            lfs_bytes: cluster.lfs_bytes()?,
            exchange_bytes,
            max_queue_depth: 0,
            operator_counters,
            workers: measurement_workers,
            canonical_input_sha256: input_sha256.to_string(),
            rockstream_output_sha256: rockstream_sha256.clone(),
            sqlite_oracle_output_sha256: oracle_sha256.clone(),
            outputs_equal: rockstream_sha256 == oracle_sha256,
        };
        sample.validate()?;
        Ok(sample)
    }
    .await;
    match result {
        Ok(sample) => Ok(sample),
        Err(error) => {
            atomic_json(
                &run_dir.join("failure.json"),
                &FailureArtifact {
                    error: format!("{error:#}"),
                    canonical_input_sha256: input_sha256,
                    rockstream_output_sha256: &rockstream_sha256,
                    sqlite_oracle_output_sha256: &oracle_sha256,
                    rockstream_rows: &rockstream_rows,
                    sqlite_oracle_rows: &oracle_rows,
                },
            )?;
            Err(error)
        }
    }
}

fn comparison_sides(
    workload: &str,
    record: &CandidateRecord,
    config: &WorkloadConfig,
) -> Result<Vec<Side>> {
    let candidate = |id: &str| {
        record
            .candidates
            .iter()
            .find(|candidate| candidate.id == id)
            .cloned()
            .with_context(|| format!("candidate record has no {id}"))
    };
    match workload {
        "ordinary-aggregate" | "ordinary-join" => Ok(vec![
            Side {
                candidate: candidate("b0-v0.59.4-local-rebuild")?,
                strategy: "auto".to_string(),
                join_strategy: "auto".to_string(),
                sql: config.sql.clone(),
            },
            Side {
                candidate: candidate("current")?,
                strategy: "auto".to_string(),
                join_strategy: "auto".to_string(),
                sql: config.sql.clone(),
            },
        ]),
        "factorized-join" => Ok(vec![
            Side {
                candidate: candidate("current")?,
                strategy: "classic".to_string(),
                join_strategy: "classic".to_string(),
                sql: config.sql.clone(),
            },
            Side {
                candidate: candidate("current")?,
                strategy: "factorized".to_string(),
                join_strategy: "factorized".to_string(),
                sql: config.sql.clone(),
            },
        ]),
        "shared-arrangement" => Ok(vec![
            Side {
                candidate: candidate("current")?,
                strategy: "one-shared".to_string(),
                join_strategy: "auto".to_string(),
                sql: "sql/shared-arrangement-one.sql".to_string(),
            },
            Side {
                candidate: candidate("current")?,
                strategy: "twenty-shared".to_string(),
                join_strategy: "auto".to_string(),
                sql: config.sql.clone(),
            },
            Side {
                candidate: candidate("current")?,
                strategy: "twenty-private".to_string(),
                join_strategy: "auto".to_string(),
                sql: config.sql.clone(),
            },
        ]),
        "uniform-worker-scaling" => Ok(vec![Side {
            candidate: candidate("current")?,
            strategy: "auto".to_string(),
            join_strategy: "auto".to_string(),
            sql: config.sql.clone(),
        }]),
        _ => bail!("unknown timing workload {workload}"),
    }
}

fn repository_root(start: &Path) -> Result<PathBuf> {
    let absolute = if start.is_absolute() {
        start.to_path_buf()
    } else {
        std::env::current_dir()?.join(start)
    };
    for origin in [absolute, std::env::current_dir()?] {
        for path in origin.ancestors() {
            let directory = if path.is_file() {
                path.parent().unwrap_or(path)
            } else {
                path
            };
            if directory.join("r1-green-implementation-plan.md").is_file() {
                return Ok(directory.to_path_buf());
            }
        }
    }
    bail!("could not find repository root from {}", start.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_arrangement_has_all_three_exact_profiles() {
        let record = CandidateRecord {
            candidates: vec![Candidate {
                id: "current".to_string(),
                kind: "current".to_string(),
                binary_path: PathBuf::from("target/release/rockstream"),
                binary_sha256: "binary".to_string(),
            }],
        };
        let config = WorkloadConfig {
            name: "shared-arrangement".to_string(),
            seed: 1,
            source_rows: Some(100_000),
            dimension_rows: None,
            changed_rows: None,
            live_groups: None,
            sql: "sql/shared-arrangement.sql".to_string(),
        };

        let profiles = comparison_sides("shared-arrangement", &record, &config)
            .unwrap()
            .into_iter()
            .map(|side| (side.strategy, side.join_strategy, side.sql))
            .collect::<Vec<_>>();

        assert_eq!(
            profiles,
            vec![
                (
                    "one-shared".to_string(),
                    "auto".to_string(),
                    "sql/shared-arrangement-one.sql".to_string(),
                ),
                (
                    "twenty-shared".to_string(),
                    "auto".to_string(),
                    "sql/shared-arrangement.sql".to_string(),
                ),
                (
                    "twenty-private".to_string(),
                    "auto".to_string(),
                    "sql/shared-arrangement.sql".to_string(),
                ),
            ]
        );
    }
}
