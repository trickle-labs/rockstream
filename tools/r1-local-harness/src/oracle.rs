use crate::corpus::{Change, Corpus};
use anyhow::{bail, Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    WrongColumnValue {
        row_index: usize,
        expected: Vec<String>,
        actual: Vec<String>,
    },
    DuplicateRow {
        row: Vec<String>,
        expected_count: usize,
        actual_count: usize,
    },
    MissingRow {
        row: Vec<String>,
        expected_count: usize,
        actual_count: usize,
    },
    StaleEpoch {
        expected_epoch: u64,
        actual_epoch: u64,
        message: String,
    },
    CardinalityMismatch {
        expected_total: usize,
        actual_total: usize,
    },
}

impl std::fmt::Display for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongColumnValue {
                row_index,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "RS-3032: Multiset value mismatch at row {row_index}: expected {expected:?}, got {actual:?}. Next steps: verify IVM operator delta logic."
                )
            }
            Self::DuplicateRow {
                row,
                expected_count,
                actual_count,
            } => {
                write!(
                    f,
                    "RS-3032: Multiset duplicate row: {row:?} appears {actual_count} times, expected {expected_count}. Next steps: check duplicate retraction handling."
                )
            }
            Self::MissingRow {
                row,
                expected_count,
                actual_count,
            } => {
                write!(
                    f,
                    "RS-3032: Multiset missing row: {row:?} appears {actual_count} times, expected {expected_count}. Next steps: verify aggregation and join frontiers."
                )
            }
            Self::StaleEpoch {
                expected_epoch,
                actual_epoch,
                message,
            } => {
                write!(
                    f,
                    "RS-3032: Multiset stale epoch: expected {expected_epoch}, got {actual_epoch} ({message}). Next steps: ensure view drain reaches source frontier."
                )
            }
            Self::CardinalityMismatch {
                expected_total,
                actual_total,
            } => {
                write!(
                    f,
                    "RS-3032: Multiset cardinality mismatch: expected {expected_total} rows, got {actual_total} rows. Next steps: inspect operator output count."
                )
            }
        }
    }
}

impl std::error::Error for OracleError {}

pub fn compare_multisets(
    actual: &[Vec<String>],
    expected: &[Vec<String>],
) -> Result<(), OracleError> {
    let mut actual_sorted = actual.to_vec();
    let mut expected_sorted = expected.to_vec();
    actual_sorted.sort();
    expected_sorted.sort();

    let mut actual_counts: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for row in actual {
        *actual_counts.entry(row.clone()).or_insert(0) += 1;
    }
    let mut expected_counts: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for row in expected {
        *expected_counts.entry(row.clone()).or_insert(0) += 1;
    }

    // Check for duplicate row surplus
    for (row, &act_count) in &actual_counts {
        let exp_count = expected_counts.get(row).copied().unwrap_or(0);
        if act_count > exp_count && exp_count > 0 {
            return Err(OracleError::DuplicateRow {
                row: row.clone(),
                expected_count: exp_count,
                actual_count: act_count,
            });
        }
    }

    if actual.len() > expected.len() {
        for (row, &act_count) in &actual_counts {
            let exp_count = expected_counts.get(row).copied().unwrap_or(0);
            if act_count > exp_count {
                return Err(OracleError::DuplicateRow {
                    row: row.clone(),
                    expected_count: exp_count,
                    actual_count: act_count,
                });
            }
        }
        return Err(OracleError::CardinalityMismatch {
            expected_total: expected.len(),
            actual_total: actual.len(),
        });
    }

    if actual.len() < expected.len() {
        for (row, &exp_count) in &expected_counts {
            let act_count = actual_counts.get(row).copied().unwrap_or(0);
            if act_count < exp_count {
                return Err(OracleError::MissingRow {
                    row: row.clone(),
                    expected_count: exp_count,
                    actual_count: act_count,
                });
            }
        }
        return Err(OracleError::CardinalityMismatch {
            expected_total: expected.len(),
            actual_total: actual.len(),
        });
    }

    // actual.len() == expected.len()
    for (i, (act, exp)) in actual_sorted.iter().zip(expected_sorted.iter()).enumerate() {
        if act != exp {
            return Err(OracleError::WrongColumnValue {
                row_index: i,
                expected: exp.clone(),
                actual: act.clone(),
            });
        }
    }

    Ok(())
}

pub fn compare_multisets_at_epoch(
    actual: &[Vec<String>],
    expected: &[Vec<String>],
    actual_epoch: u64,
    expected_epoch: u64,
) -> Result<(), OracleError> {
    if actual_epoch != expected_epoch {
        return Err(OracleError::StaleEpoch {
            expected_epoch,
            actual_epoch,
            message: format!("observed epoch {actual_epoch} != committed epoch {expected_epoch}"),
        });
    }
    compare_multisets(actual, expected)
}

pub fn admitted_query(workload_sql: &str) -> Result<(String, String)> {
    let statement = workload_sql
        .split(';')
        .map(str::trim)
        .find(|statement| statement.starts_with("CREATE MATERIALIZED VIEW "))
        .context("workload SQL has no materialized view")?;
    let rest = statement
        .strip_prefix("CREATE MATERIALIZED VIEW ")
        .expect("prefix checked");
    let (view, query) = rest
        .split_once(" AS ")
        .context("materialized view has no AS query")?;
    Ok((view.to_string(), query.to_string()))
}

pub fn complete_output(corpus: &Corpus, query: &str) -> Result<Vec<Vec<String>>> {
    let mut db = Connection::open_in_memory().context("open bundled SQLite oracle")?;
    db.execute_batch(
        "CREATE TABLE r1_source (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, dimension_id INTEGER NOT NULL, value INTEGER NOT NULL, active INTEGER NOT NULL);\n\
         CREATE TABLE r1_dimension (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL);",
    )?;
    let transaction = db.transaction()?;
    {
        let mut dimension =
            transaction.prepare("INSERT INTO r1_dimension (id, bucket) VALUES (?1, ?2)")?;
        for (id, bucket) in &corpus.dimension {
            dimension.execute(params![id, bucket])?;
        }
        let mut source = transaction.prepare(
            "INSERT INTO r1_source (id, group_id, dimension_id, value, active) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for row in &corpus.source {
            source.execute(params![
                row.id,
                row.group_id,
                row.dimension_id,
                row.value,
                row.active
            ])?;
        }
    }
    for change in &corpus.changes {
        match change {
            Change::Insert { after } => {
                transaction.execute(
                    "INSERT INTO r1_source (id, group_id, dimension_id, value, active) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![after.id, after.group_id, after.dimension_id, after.value, after.active],
                )?;
            }
            Change::Update { before, after } => {
                let changed = transaction.execute(
                    "UPDATE r1_source SET group_id=?1, dimension_id=?2, value=?3, active=?4 WHERE id=?5 AND group_id=?6 AND dimension_id=?7 AND value=?8 AND active=?9",
                    params![after.group_id, after.dimension_id, after.value, after.active, before.id, before.group_id, before.dimension_id, before.value, before.active],
                )?;
                if changed != 1 {
                    bail!(
                        "SQLite update did not match canonical before row {}",
                        before.id
                    );
                }
            }
            Change::Delete { before } => {
                let changed = transaction.execute(
                    "DELETE FROM r1_source WHERE id=?1 AND group_id=?2 AND dimension_id=?3 AND value=?4 AND active=?5",
                    params![before.id, before.group_id, before.dimension_id, before.value, before.active],
                )?;
                if changed != 1 {
                    bail!(
                        "SQLite delete did not match canonical before row {}",
                        before.id
                    );
                }
            }
        }
    }
    transaction.commit()?;
    let mut statement = db
        .prepare(query)
        .with_context(|| format!("prepare oracle query {query:?}"))?;
    let columns = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| canonical_value(row.get_ref(column)?))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn canonical_value(value: ValueRef<'_>) -> rusqlite::Result<String> {
    Ok(match value {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => value.to_string(),
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
        ValueRef::Blob(value) => hex::encode(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::canonical_rows;
    use crate::corpus::SourceRow;

    #[test]
    fn replays_complete_aggregate_and_join_outputs() {
        let row = |id, group_id, dimension_id, value| SourceRow {
            id,
            group_id,
            dimension_id,
            value,
            active: true,
        };
        let corpus = Corpus {
            source: vec![row(0, 1, 0, 10), row(1, 1, 1, 20), row(2, 2, 0, 5)],
            dimension: vec![(0, 7), (1, 8)],
            changes: vec![
                Change::Update {
                    before: row(0, 1, 0, 10),
                    after: row(0, 1, 0, 15),
                },
                Change::Delete {
                    before: row(1, 1, 1, 20),
                },
                Change::Insert {
                    after: row(3, 2, 1, 4),
                },
            ],
        };
        let (aggregate, _) = canonical_rows(
            complete_output(
                &corpus,
                "SELECT group_id, COUNT(*), SUM(value) FROM r1_source GROUP BY group_id",
            )
            .unwrap(),
        )
        .unwrap();
        let (join, _) = canonical_rows(
            complete_output(
                &corpus,
                "SELECT d.bucket, COUNT(*), SUM(s.value) FROM r1_source s JOIN r1_dimension d ON s.dimension_id = d.id GROUP BY d.bucket",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            (aggregate, join),
            (
                vec![
                    vec!["1".to_string(), "1".to_string(), "15".to_string()],
                    vec!["2".to_string(), "2".to_string(), "9".to_string()],
                ],
                vec![
                    vec!["7".to_string(), "2".to_string(), "20".to_string()],
                    vec!["8".to_string(), "1".to_string(), "4".to_string()],
                ],
            )
        );
    }
}
