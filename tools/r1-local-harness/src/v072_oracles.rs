//! Independent SQLite complete-result oracle for all eight canonical v0.72
//! workload *shapes*. This is not an ingestion, source-transport, or release-
//! process attestation: observed rows and epochs are caller-supplied.
//!
//! The workload query is selected from the frozen list by enum, rather than
//! trusting SQL passed in an evidence bundle. All expected results are derived
//! from the complete input/change corpus by the separate SQLite oracle.

use crate::artifact::{canonical_rows, sha256};
use crate::corpus::Corpus;
use crate::oracle::{compare_multisets_at_epoch, complete_output};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

pub const FROZEN_PROFILE_SHA256: &str =
    "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    FilterProjection,
    GroupAggregate,
    HighCardinalityAggregate,
    TwoWayJoin,
    Window,
    PostgresCdc,
    KafkaIngestion,
    DistributedExchange,
}

impl WorkloadKind {
    pub const ALL: [Self; 8] = [
        Self::FilterProjection,
        Self::GroupAggregate,
        Self::HighCardinalityAggregate,
        Self::TwoWayJoin,
        Self::Window,
        Self::PostgresCdc,
        Self::KafkaIngestion,
        Self::DistributedExchange,
    ];

    /// Independent query semantics. The PG CDC and Kafka workloads reuse the
    /// same canonical table changes but must separately demonstrate their
    /// connector ingest/ack lineage during release-process qualification.
    pub const fn sqlite_query(self) -> &'static str {
        match self {
            Self::FilterProjection => {
                "SELECT id, value FROM r1_source WHERE active = 1"
            }
            Self::GroupAggregate => {
                "SELECT group_id, COUNT(*), SUM(value) FROM r1_source GROUP BY group_id"
            }
            Self::HighCardinalityAggregate => {
                "SELECT id, COUNT(*), SUM(value) FROM r1_source GROUP BY id"
            }
            Self::TwoWayJoin => {
                "SELECT s.id, d.bucket, s.value FROM r1_source s JOIN r1_dimension d ON s.dimension_id = d.id"
            }
            Self::Window => {
                "SELECT group_id, id, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY value DESC, id) FROM r1_source"
            }
            Self::PostgresCdc => {
                "SELECT id, group_id, value FROM r1_source WHERE active = 1"
            }
            Self::KafkaIngestion => {
                "SELECT group_id, COUNT(*), SUM(value) FROM r1_source WHERE active = 1 GROUP BY group_id"
            }
            Self::DistributedExchange => {
                "SELECT d.bucket, COUNT(*), SUM(s.value) FROM r1_source s JOIN r1_dimension d ON s.dimension_id = d.id GROUP BY d.bucket"
            }
        }
    }

    pub const fn expected_transport(self) -> Transport {
        match self {
            Self::PostgresCdc => Transport::PostgresCdc,
            Self::KafkaIngestion => Transport::Kafka,
            Self::DistributedExchange => Transport::Exchange,
            _ => Transport::PgWire,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    PgWire,
    PostgresCdc,
    Kafka,
    Exchange,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleEvidenceManifest {
    pub schema_version: u32,
    pub profile_sha256: String,
    pub runs: Vec<OracleEvidenceRun>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleEvidenceRun {
    pub workload: WorkloadKind,
    /// Source+change corpus, retaining the full input including retractions.
    pub input_path: PathBuf,
    /// Complete observed result row multiset (JSON array of string arrays).
    pub observed_rows_path: PathBuf,
    pub reported_transport: Transport,
    pub committed_epoch: u64,
    pub observed_epoch: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct OracleEvidenceSummary {
    pub schema_version: u32,
    pub status: &'static str,
    pub profile_sha256: String,
    pub complete_results: BTreeMap<WorkloadKind, CompleteResultSummary>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct CompleteResultSummary {
    pub row_count: usize,
    pub expected_rows_sha256: String,
    pub observed_rows_sha256: String,
    pub input_sha256: String,
    pub observed_epoch: u64,
}

/// Reject traversal, absolute paths and symlink escape, including a symlink in
/// a parent directory, before consuming any untrusted evidence file.
fn evidence_file(root: &Path, raw: &Path) -> Result<PathBuf> {
    if raw.as_os_str().is_empty()
        || raw
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("invalid evidence relative path: {}", raw.display());
    }
    let resolved = root
        .join(raw)
        .canonicalize()
        .with_context(|| format!("resolve evidence file {}", raw.display()))?;
    if !resolved.starts_with(root) || !resolved.is_file() {
        bail!(
            "evidence file escapes root or is not a regular file: {}",
            raw.display()
        );
    }
    Ok(resolved)
}

/// Verifies the frozen profile digest plus complete independently recomputed
/// SQLite result multisets, rejecting missing, duplicated, or mislabelled
/// scenarios. This cannot validate the authenticity of the observed process,
/// connector transport, or committed epoch: release-process evidence is still
/// mandatory. A success has *only* the status below, never "qualified".
pub fn verify_eight_oracles(
    root: &Path,
    frozen_profile: &[u8],
    manifest: &OracleEvidenceManifest,
) -> Result<OracleEvidenceSummary> {
    if sha256(frozen_profile) != FROZEN_PROFILE_SHA256
        || manifest.profile_sha256 != FROZEN_PROFILE_SHA256
    {
        bail!("v0.72 frozen profile digest mismatch");
    }
    if manifest.schema_version != 1 || manifest.runs.len() != WorkloadKind::ALL.len() {
        bail!("oracle manifest must contain exactly eight canonical workload records");
    }
    let root = root
        .canonicalize()
        .context("resolve oracle evidence root")?;
    let mut seen = BTreeSet::new();
    let mut seen_files = BTreeSet::new();
    let mut complete_results = BTreeMap::new();
    for run in &manifest.runs {
        if !seen.insert(run.workload) {
            bail!("duplicate canonical workload {:?}", run.workload);
        }
        if run.reported_transport != run.workload.expected_transport() {
            bail!(
                "workload {:?} has incorrect declared transport",
                run.workload
            );
        }
        if run.committed_epoch == 0 || run.committed_epoch != run.observed_epoch {
            bail!(
                "workload {:?} has missing or stale committed epoch",
                run.workload
            );
        }
        let input = evidence_file(&root, &run.input_path)?;
        let observed = evidence_file(&root, &run.observed_rows_path)?;
        if input == observed || !seen_files.insert(observed.clone()) {
            bail!(
                "workload {:?} aliases an observed result file",
                run.workload
            );
        }
        let raw_input =
            fs::read(&input).with_context(|| format!("read input {}", input.display()))?;
        let corpus: Corpus = serde_json::from_slice(&raw_input)
            .with_context(|| format!("parse complete input {}", input.display()))?;
        let expected = complete_output(&corpus, run.workload.sqlite_query())
            .with_context(|| format!("SQLite oracle for {:?}", run.workload))?;
        let observed_rows: Vec<Vec<String>> = serde_json::from_slice(&fs::read(&observed)?)
            .with_context(|| format!("parse complete result for {:?}", run.workload))?;
        compare_multisets_at_epoch(
            &observed_rows,
            &expected,
            run.observed_epoch,
            run.committed_epoch,
        )
        .with_context(|| format!("complete-result oracle mismatch for {:?}", run.workload))?;
        let (expected, expected_rows_sha256) = canonical_rows(expected)?;
        let (observed_rows, observed_rows_sha256) = canonical_rows(observed_rows)?;
        if expected != observed_rows || expected_rows_sha256 != observed_rows_sha256 {
            bail!("noncanonical oracle result for {:?}", run.workload);
        }
        complete_results.insert(
            run.workload,
            CompleteResultSummary {
                row_count: expected.len(),
                expected_rows_sha256,
                observed_rows_sha256,
                input_sha256: sha256(&raw_input),
                observed_epoch: run.observed_epoch,
            },
        );
    }
    if seen.len() != WorkloadKind::ALL.len() {
        bail!("missing canonical workloads");
    }
    Ok(OracleEvidenceSummary {
        schema_version: 1,
        status: "independent_multiset_oracle_only_not_capacity_qualified",
        profile_sha256: FROZEN_PROFILE_SHA256.to_owned(),
        complete_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::{Change, SourceRow};

    const PROFILE: &[u8] = include_bytes!("../../../benchmarks/r1-local/profile-v072.toml");

    fn fixture(dir: &Path) -> OracleEvidenceManifest {
        let row = |id, group_id, dimension_id, value, active| SourceRow {
            id,
            group_id,
            dimension_id,
            value,
            active,
        };
        let before = row(0, 1, 0, 10, true);
        let corpus = Corpus {
            source: vec![
                before.clone(),
                row(1, 1, 1, 20, true),
                row(2, 2, 0, 5, false),
            ],
            dimension: vec![(0, 7), (1, 8)],
            changes: vec![
                Change::Update {
                    before,
                    after: row(0, 1, 0, 15, true),
                },
                Change::Delete {
                    before: row(1, 1, 1, 20, true),
                },
                Change::Insert {
                    after: row(3, 2, 1, 4, true),
                },
            ],
        };
        let input = serde_json::to_vec(&corpus).unwrap();
        fs::write(dir.join("source.json"), input).unwrap();
        let runs = WorkloadKind::ALL
            .iter()
            .map(|workload| {
                let output = complete_output(&corpus, workload.sqlite_query()).unwrap();
                let name = format!("{:?}.json", workload);
                fs::write(dir.join(&name), serde_json::to_vec(&output).unwrap()).unwrap();
                OracleEvidenceRun {
                    workload: *workload,
                    input_path: PathBuf::from("source.json"),
                    observed_rows_path: PathBuf::from(name),
                    reported_transport: workload.expected_transport(),
                    committed_epoch: 4,
                    observed_epoch: 4,
                }
            })
            .collect();
        OracleEvidenceManifest {
            schema_version: 1,
            profile_sha256: FROZEN_PROFILE_SHA256.to_owned(),
            runs,
        }
    }

    #[test]
    fn validates_eight_independently_derived_complete_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = fixture(dir.path());
        let summary = verify_eight_oracles(dir.path(), PROFILE, &manifest).unwrap();
        assert_eq!(
            summary.status,
            "independent_multiset_oracle_only_not_capacity_qualified"
        );
        assert_eq!(summary.complete_results.len(), 8);
        assert!(summary
            .complete_results
            .values()
            .all(|record| record.expected_rows_sha256 == record.observed_rows_sha256));
        assert_eq!(summary.complete_results[&WorkloadKind::Window].row_count, 3);
    }

    #[test]
    fn rejects_missing_duplicate_and_unsupported_scenarios() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = fixture(dir.path());
        manifest.runs.pop();
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("exactly eight"));
        let mut manifest = fixture(dir.path());
        manifest.runs[1].workload = manifest.runs[0].workload;
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("duplicate canonical"));
    }

    #[test]
    fn rejects_changed_row_and_duplicate_multiplicity() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = fixture(dir.path());
        let actual = dir.path().join(&manifest.runs[0].observed_rows_path);
        let mut rows: Vec<Vec<String>> =
            serde_json::from_slice(&fs::read(&actual).unwrap()).unwrap();
        rows[0][1] = "999".to_string();
        fs::write(&actual, serde_json::to_vec(&rows).unwrap()).unwrap();
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("complete-result oracle mismatch"));
        let manifest = fixture(dir.path());
        let actual = dir.path().join(&manifest.runs[3].observed_rows_path);
        let mut rows: Vec<Vec<String>> =
            serde_json::from_slice(&fs::read(&actual).unwrap()).unwrap();
        rows.push(rows[0].clone());
        fs::write(&actual, serde_json::to_vec(&rows).unwrap()).unwrap();
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("complete-result oracle mismatch"));
    }

    #[test]
    fn rejects_stale_epoch_and_mislabelled_transport() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = fixture(dir.path());
        manifest.runs[5].observed_epoch = 3;
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("stale committed epoch"));
        manifest.runs[5].observed_epoch = 4;
        manifest.runs[5].reported_transport = Transport::PgWire;
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("incorrect declared transport"));
    }

    #[test]
    fn rejects_input_tampering_and_profile_revision_drift() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = fixture(dir.path());
        fs::write(dir.path().join("source.json"), b"not JSON").unwrap();
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("parse complete input"));
        let manifest = fixture(dir.path());
        assert!(
            verify_eight_oracles(dir.path(), b"updated profile", &manifest)
                .unwrap_err()
                .to_string()
                .contains("profile digest")
        );
    }

    #[test]
    fn rejects_traversal_and_result_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = fixture(dir.path());
        manifest.runs[0].input_path = PathBuf::from("../source.json");
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("invalid evidence relative path"));
        let mut manifest = fixture(dir.path());
        manifest.runs[1].observed_rows_path = manifest.runs[0].observed_rows_path.clone();
        assert!(verify_eight_oracles(dir.path(), PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("aliases an observed result file"));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlink_escape() {
        let parent = tempfile::tempdir().unwrap();
        let dir = parent.path().join("evidence");
        fs::create_dir(&dir).unwrap();
        let manifest = fixture(&dir);
        let escape = parent.path().join("elsewhere.json");
        fs::write(&escape, b"[]").unwrap();
        fs::remove_file(dir.join(&manifest.runs[0].observed_rows_path)).unwrap();
        std::os::unix::fs::symlink(escape, dir.join(&manifest.runs[0].observed_rows_path)).unwrap();
        assert!(verify_eight_oracles(&dir, PROFILE, &manifest)
            .unwrap_err()
            .to_string()
            .contains("escapes root"));
    }
}
