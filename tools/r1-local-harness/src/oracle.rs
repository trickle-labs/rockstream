use crate::corpus::{Change, Corpus};
use anyhow::{bail, Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection};

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
