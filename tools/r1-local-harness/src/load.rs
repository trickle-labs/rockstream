use crate::corpus::{Change, Corpus, SourceRow};
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
}

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    address: &str,
    workload_sql: &str,
    view: &str,
    corpus: &Corpus,
    lanes: usize,
    transaction_rows: usize,
    warm_up: Duration,
    histogram_bounds_ms: &[u64],
) -> Result<LoadOutcome> {
    if lanes == 0 || transaction_rows == 0 || histogram_bounds_ms.is_empty() {
        bail!("load lanes, transaction rows, and histogram bounds must be nonzero");
    }
    let admin = connect(address).await?;
    for statement in workload_sql
        .split(';')
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
    {
        admin
            .batch_execute(statement)
            .await
            .with_context(|| format!("execute workload DDL {statement:?}"))?;
    }
    insert_dimensions(&admin, &corpus.dimension, transaction_rows).await?;
    insert_sources(&admin, &corpus.source, transaction_rows).await?;
    admin
        .query(&format!("SELECT COUNT(*) FROM {view}"), &[])
        .await
        .context("warm materialized view")?;
    tokio::time::sleep(warm_up).await;

    let chunks = corpus.changes.chunks(transaction_rows).collect::<Vec<_>>();
    let mut per_lane = vec![Vec::new(); lanes];
    for (index, chunk) in chunks.into_iter().enumerate() {
        per_lane[index % lanes].push(chunk.to_vec());
    }
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(lanes);
    for lane in per_lane {
        let address = address.to_string();
        let view = view.to_string();
        tasks.push(tokio::spawn(async move {
            let client = connect(&address).await?;
            let mut latencies = Vec::with_capacity(lane.len());
            for changes in lane {
                let sql = transaction_sql(&changes);
                client
                    .batch_execute(&sql)
                    .await
                    .context("submit change transaction")?;
                let committed = Instant::now();
                client
                    .query(&format!("SELECT COUNT(*) FROM {view}"), &[])
                    .await
                    .context("await query-visible output frontier")?;
                latencies.push(committed.elapsed());
            }
            Ok::<_, anyhow::Error>(latencies)
        }));
    }
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.context("load lane panicked")??);
    }
    let duration = started.elapsed();
    let rows = query_rows(&admin, &format!("SELECT * FROM {view}")).await?;
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
        accepted_changes: corpus.changes.len() as u64,
        visible_changes: corpus.changes.len() as u64,
        freshness_counts,
        rows,
    })
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
            .batch_execute(&format!("INSERT INTO r1_dimension VALUES {values}"))
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
            .batch_execute(&format!("INSERT INTO r1_source VALUES {values}"))
            .await?;
    }
    Ok(())
}

fn transaction_sql(changes: &[Change]) -> String {
    let mut sql = String::from("BEGIN;");
    for change in changes {
        match change {
            Change::Insert { after } => {
                sql.push_str(&format!("INSERT INTO r1_source VALUES {};", source_values(after)));
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
