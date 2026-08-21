use crate::corpus::{canonical_changes_json, Change, Corpus, SourceRow};
use anyhow::{bail, Context, Result};
use std::time::{Duration, Instant};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, Row};

pub struct LoadOutcome {
    pub duration: Duration,
    pub accepted_changes: u64,
    pub visible_changes: u64,
    pub freshness_counts: Vec<u64>,
    pub rows: Vec<Vec<String>>,
    pub final_source: Vec<SourceRow>,
    pub logical_bytes: u64,
}

pub struct PreparedLoad {
    admin: Client,
}

pub async fn prepare(
    address: &str,
    workload_sql: &str,
    view: &str,
    corpus: &Corpus,
    transaction_rows: usize,
    warm_up: Duration,
) -> Result<PreparedLoad> {
    if transaction_rows == 0 {
        bail!("transaction rows must be nonzero");
    }
    let admin = connect(address).await?;
    for statement in workload_sql
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        admin
            .batch_execute(statement)
            .await
            .with_context(|| format!("execute workload DDL {statement:?}"))?;
    }
    insert_dimensions(&admin, &corpus.dimension, transaction_rows).await?;
    insert_sources(&admin, &corpus.source, transaction_rows).await?;
    admin
        .query(&visibility_query(view), &[])
        .await
        .context("warm materialized view")?;
    tokio::time::sleep(warm_up).await;
    Ok(PreparedLoad { admin })
}

fn visibility_query(view: &str) -> String {
    format!("SELECT * FROM {view} LIMIT 1")
}

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    prepared: PreparedLoad,
    address: &str,
    view: &str,
    corpus: &Corpus,
    lanes: usize,
    transaction_rows: usize,
    measurement: Duration,
    histogram_bounds_ms: &[u64],
) -> Result<LoadOutcome> {
    if lanes == 0
        || transaction_rows == 0
        || measurement.is_zero()
        || histogram_bounds_ms.is_empty()
    {
        bail!("load lanes, transaction rows, measurement, and histogram bounds must be nonzero");
    }

    let chunks = corpus.changes.chunks(transaction_rows).collect::<Vec<_>>();
    let mut per_lane = vec![Vec::new(); lanes];
    for (index, chunk) in chunks.into_iter().enumerate() {
        per_lane[index % lanes].push(chunk.to_vec());
    }
    let started = Instant::now();
    let deadline = started + measurement;
    let mut tasks = Vec::with_capacity(lanes);
    for lane in per_lane {
        let address = address.to_string();
        let view = view.to_string();
        tasks.push(tokio::spawn(async move {
            let client = connect(&address).await?;
            let inverse = inverse_changes(&lane);
            let mut final_changes = Vec::new();
            let mut accepted_changes = 0;
            let mut logical_bytes = 0;
            let mut latencies = Vec::new();
            let mut forward = true;
            while Instant::now() < deadline {
                for changes in if forward { &lane } else { &inverse } {
                    if Instant::now() >= deadline {
                        break;
                    }
                    client
                        .batch_execute(&transaction_sql(changes))
                        .await
                        .context("submit change transaction")?;
                    let committed = Instant::now();
                    client
                        .query(&visibility_query(&view), &[])
                        .await
                        .context("await query-visible output frontier")?;
                    track_final_changes(&mut final_changes, changes, forward);
                    accepted_changes += changes.len() as u64;
                    logical_bytes += canonical_changes_json(changes).len() as u64;
                    latencies.push(committed.elapsed());
                }
                forward = !forward;
            }
            Ok::<_, anyhow::Error>((latencies, final_changes, accepted_changes, logical_bytes))
        }));
    }
    let mut latencies = Vec::new();
    let mut final_changes = Vec::new();
    let mut accepted_changes = 0;
    let mut logical_bytes = 0;
    for task in tasks {
        let (lane_latencies, lane_changes, lane_accepted, lane_logical_bytes) =
            task.await.context("load lane panicked")??;
        latencies.extend(lane_latencies);
        final_changes.extend(lane_changes);
        accepted_changes += lane_accepted;
        logical_bytes += lane_logical_bytes;
    }
    let duration = started.elapsed();
    let rows = query_rows(&prepared.admin, &format!("SELECT * FROM {view}")).await?;
    let final_source = query_source(&prepared.admin).await?;
    let mut freshness_counts = vec![0; histogram_bounds_ms.len()];
    for latency in latencies {
        let elapsed_ms = latency.as_micros().div_ceil(1_000) as u64;
        let bucket = histogram_bounds_ms
            .iter()
            .position(|bound| elapsed_ms <= *bound)
            .unwrap_or(histogram_bounds_ms.len() - 1);
        freshness_counts[bucket] += 1;
    }
    Ok(LoadOutcome {
        duration,
        accepted_changes,
        visible_changes: accepted_changes,
        freshness_counts,
        rows,
        final_source,
        logical_bytes,
    })
}

fn inverse_changes(changes: &[Vec<Change>]) -> Vec<Vec<Change>> {
    changes
        .iter()
        .rev()
        .map(|changes| {
            changes
                .iter()
                .rev()
                .map(|change| match change {
                    Change::Insert { after } => Change::Delete {
                        before: after.clone(),
                    },
                    Change::Update { before, after } => Change::Update {
                        before: after.clone(),
                        after: before.clone(),
                    },
                    Change::Delete { before } => Change::Insert {
                        after: before.clone(),
                    },
                })
                .collect()
        })
        .collect()
}

fn track_final_changes(applied: &mut Vec<Change>, changes: &[Change], forward: bool) {
    if forward {
        applied.extend_from_slice(changes);
    } else {
        applied.truncate(applied.len() - changes.len());
    }
}

async fn connect(address: &str) -> Result<Client> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut delay = Duration::from_millis(20);
    loop {
        match tokio_postgres::connect(
            &format!(
                "host={} port={} user=rockstream dbname=rockstream",
                host(address)?,
                port(address)?
            ),
            NoTls,
        )
        .await
        {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return Ok(client);
            }
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
            }
            Err(error) => {
                return Err(error).with_context(|| format!("connect to PGWire at {address}"))
            }
        }
    }
}

fn host(address: &str) -> Result<&str> {
    address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .context("PGWire address has no port")
}

fn port(address: &str) -> Result<u16> {
    address
        .rsplit_once(':')
        .context("PGWire address has no port")?
        .1
        .parse()
        .context("parse PGWire port")
}

async fn insert_dimensions(client: &Client, rows: &[(u64, u64)], chunk_size: usize) -> Result<()> {
    for chunk in rows.chunks(chunk_size) {
        let values = chunk
            .iter()
            .map(|(id, bucket)| format!("({id},{bucket})"))
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!(
                "INSERT INTO r1_dimension (id, bucket) VALUES {values}"
            ))
            .await?;
    }
    Ok(())
}

async fn insert_sources(client: &Client, rows: &[SourceRow], chunk_size: usize) -> Result<()> {
    for chunk in rows.chunks(chunk_size) {
        let values = chunk
            .iter()
            .map(source_values)
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!(
                "INSERT INTO r1_source (id, group_id, dimension_id, value, active) VALUES {values}"
            ))
            .await?;
    }
    Ok(())
}

fn transaction_sql(changes: &[Change]) -> String {
    let mut sql = String::from("BEGIN;");
    for change in changes {
        match change {
            Change::Insert { after } => {
                sql.push_str(&format!(
                    "INSERT INTO r1_source (id, group_id, dimension_id, value, active) VALUES {};",
                    source_values(after)
                ));
            }
            Change::Update { before, after } => sql.push_str(&format!(
                "UPDATE r1_source SET group_id={},dimension_id={},value={},active={} WHERE id={} AND group_id={} AND dimension_id={} AND value={} AND active={};",
                after.group_id, after.dimension_id, after.value, after.active, before.id, before.group_id, before.dimension_id, before.value, before.active
            )),
            Change::Delete { before } => sql.push_str(&format!(
                "DELETE FROM r1_source WHERE id={} AND group_id={} AND dimension_id={} AND value={} AND active={};",
                before.id, before.group_id, before.dimension_id, before.value, before.active
            )),
        }
    }
    sql.push_str("COMMIT;");
    sql
}

fn source_values(row: &SourceRow) -> String {
    format!(
        "({},{},{},{},{})",
        row.id, row.group_id, row.dimension_id, row.value, row.active
    )
}

async fn query_rows(client: &Client, sql: &str) -> Result<Vec<Vec<String>>> {
    client
        .query(sql, &[])
        .await
        .with_context(|| format!("query complete RockStream output {sql:?}"))?
        .iter()
        .map(canonical_row)
        .collect()
}

async fn query_source(client: &Client) -> Result<Vec<SourceRow>> {
    client
        .query(
            "SELECT id, group_id, dimension_id, value, active FROM r1_source ORDER BY id",
            &[],
        )
        .await?
        .iter()
        .map(|row| {
            Ok(SourceRow {
                id: row.try_get::<_, i64>(0)? as u64,
                group_id: row.try_get::<_, i64>(1)? as u64,
                dimension_id: row.try_get::<_, i64>(2)? as u64,
                value: row.try_get(3)?,
                active: row.try_get(4)?,
            })
        })
        .collect()
}

fn canonical_row(row: &Row) -> Result<Vec<String>> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| match *column.type_() {
            Type::INT8 => Ok(row
                .try_get::<_, Option<i64>>(index)?
                .map_or("NULL".to_string(), |value| value.to_string())),
            Type::INT4 => Ok(row
                .try_get::<_, Option<i32>>(index)?
                .map_or("NULL".to_string(), |value| value.to_string())),
            Type::BOOL => Ok(row
                .try_get::<_, Option<bool>>(index)?
                .map_or("NULL".to_string(), |value| value.to_string())),
            Type::FLOAT8 => Ok(row
                .try_get::<_, Option<f64>>(index)?
                .map_or("NULL".to_string(), |value| value.to_string())),
            Type::TEXT | Type::VARCHAR => Ok(row
                .try_get::<_, Option<String>>(index)?
                .unwrap_or_else(|| "NULL".to_string())),
            ref kind => bail!("unsupported PGWire output type {kind}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inverse_changes_exactly_reverses_a_transaction_sequence() {
        let row = |id, value| SourceRow {
            id,
            group_id: 1,
            dimension_id: 2,
            value,
            active: true,
        };
        let changes = vec![vec![
            Change::Insert { after: row(3, 30) },
            Change::Update {
                before: row(1, 10),
                after: row(1, 11),
            },
            Change::Delete { before: row(2, 20) },
        ]];
        assert_eq!(
            inverse_changes(&changes),
            vec![vec![
                Change::Insert { after: row(2, 20) },
                Change::Update {
                    before: row(1, 11),
                    after: row(1, 10),
                },
                Change::Delete { before: row(3, 30) },
            ]]
        );
    }

    #[test]
    fn visibility_query_requests_one_complete_row() {
        assert_eq!(
            visibility_query("r1_factorized"),
            "SELECT * FROM r1_factorized LIMIT 1"
        );
    }

    #[test]
    fn inverse_prefix_leaves_the_exact_forward_prefix() {
        let row = |id| SourceRow {
            id,
            group_id: 1,
            dimension_id: 2,
            value: id as i64,
            active: true,
        };
        let forward = vec![
            Change::Insert { after: row(1) },
            Change::Insert { after: row(2) },
            Change::Insert { after: row(3) },
        ];
        let inverse = inverse_changes(std::slice::from_ref(&forward));
        let mut applied = Vec::new();

        track_final_changes(&mut applied, &forward, true);
        track_final_changes(&mut applied, &inverse[0][..2], false);

        assert_eq!(applied, vec![Change::Insert { after: row(1) }]);
    }
}
