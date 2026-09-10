use crate::artifact::canonical_rows;
use crate::corpus::{canonical_changes_json, Change, Corpus, SourceRow};
use crate::oracle;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, Row};

#[derive(Debug, Clone)]
pub struct QueuedItem<T> {
    pub scheduled_at: Instant,
    pub item: T,
}

#[derive(Debug)]
pub struct GeneratorQueue<T> {
    capacity: usize,
    items: VecDeque<QueuedItem<T>>,
    enqueued_count: u64,
    overflow_drops: u64,
    total_offered: u64,
}

impl<T> GeneratorQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity.min(10_000)),
            enqueued_count: 0,
            overflow_drops: 0,
            total_offered: 0,
        }
    }

    pub fn try_enqueue(&mut self, scheduled_at: Instant, item: T) -> bool {
        self.total_offered += 1;
        if self.items.len() >= self.capacity {
            self.overflow_drops += 1;
            false
        } else {
            self.items.push_back(QueuedItem { scheduled_at, item });
            self.enqueued_count += 1;
            true
        }
    }

    pub fn try_dequeue(&mut self) -> Option<QueuedItem<T>> {
        self.items.pop_front()
    }

    pub fn current_len(&self) -> usize {
        self.items.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn enqueued_count(&self) -> u64 {
        self.enqueued_count
    }

    pub fn overflow_drops(&self) -> u64 {
        self.overflow_drops
    }

    pub fn total_offered(&self) -> u64 {
        self.total_offered
    }
}

#[derive(Debug, Clone)]
pub struct DecoupledLoadConfig {
    pub target_rate_per_second: u64,
    pub max_queue_capacity: usize,
    pub measurement_duration: Duration,
    pub warm_up_duration: Duration,
    pub query_interval: Duration,
}

impl Default for DecoupledLoadConfig {
    fn default() -> Self {
        Self {
            target_rate_per_second: 1000,
            max_queue_capacity: 10_000,
            measurement_duration: Duration::from_secs(60),
            warm_up_duration: Duration::from_secs(10),
            query_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyRecord {
    pub read_p99_ms: f64,
    pub commit_p99_ms: f64,
    pub freshness_p99_ms: f64,
    pub read_latencies_ms: Vec<f64>,
    pub commit_latencies_ms: Vec<f64>,
    pub freshness_latencies_ms: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratorStats {
    pub scheduled_events: u64,
    pub dispatched_events: u64,
    pub dropped_events: u64,
    pub queue_overflow_drops: u64,
    pub generator_delays_ms: Vec<f64>,
    pub max_generator_delay_ms: f64,
    pub mean_generator_delay_ms: f64,
    pub timeouts: u64,
    pub errors: u64,
    pub unfinished_requests: u64,
    pub max_queue_depth: usize,
}

pub struct DecoupledLoadOutcome {
    pub duration: Duration,
    pub accepted_changes: u64,
    pub visible_changes: u64,
    pub latencies: LatencyRecord,
    pub generator_stats: GeneratorStats,
    pub rows: Vec<Vec<String>>,
    pub final_source: Vec<SourceRow>,
    pub logical_bytes: u64,
}

pub fn calculate_p99(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((values.len() as f64) * 0.99).ceil() as usize;
    let clamped = (index.saturating_sub(1)).min(values.len() - 1);
    values[clamped]
}

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
    oracle_query: &str,
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
    let final_source = query_source(&prepared.admin).await?;
    let mut expected = corpus.clone();
    expected.source = final_source.clone();
    expected.changes.clear();
    let (expected_rows, _) = canonical_rows(oracle::complete_output(&expected, oracle_query)?)?;
    let drain_deadline = Instant::now() + Duration::from_secs(60);
    let rows = loop {
        let rows = query_rows(&prepared.admin, &format!("SELECT * FROM {view}")).await?;
        let (canonical, _) = canonical_rows(rows)?;
        if canonical == expected_rows {
            break canonical;
        }
        if Instant::now() >= drain_deadline {
            bail!("output did not reach the source frontier within the 60-second drain");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
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

#[allow(clippy::too_many_arguments)]
pub async fn execute_decoupled(
    prepared: PreparedLoad,
    address: &str,
    view: &str,
    corpus: &Corpus,
    lanes: usize,
    transaction_rows: usize,
    config: DecoupledLoadConfig,
    oracle_query: &str,
) -> Result<DecoupledLoadOutcome> {
    if lanes == 0
        || transaction_rows == 0
        || config.measurement_duration.is_zero()
        || config.max_queue_capacity == 0
    {
        bail!("lanes, transaction_rows, measurement duration, and queue capacity must be nonzero");
    }

    let chunks = corpus
        .changes
        .chunks(transaction_rows)
        .map(|c| c.to_vec())
        .collect::<Vec<_>>();
    if chunks.is_empty() {
        bail!("corpus has no change chunks");
    }

    let queue = Arc::new(Mutex::new(GeneratorQueue::new(config.max_queue_capacity)));
    let running = Arc::new(AtomicBool::new(true));

    // 1. Independent generator task
    let gen_queue = Arc::clone(&queue);
    let gen_running = Arc::clone(&running);
    let total_chunks = chunks.len();
    let rate = config.target_rate_per_second.max(1);
    let tick_interval = Duration::from_micros((1_000_000 / rate).max(1));

    let generator_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let mut chunk_idx = 0;
        let mut scheduled_count = 0u64;
        let mut delays = Vec::new();

        while gen_running.load(Ordering::Relaxed) {
            interval.tick().await;
            let scheduled = Instant::now();
            let batch = chunks[chunk_idx % total_chunks].clone();
            chunk_idx += 1;
            scheduled_count += 1;

            let mut q = gen_queue.lock().await;
            let dispatch_time = Instant::now();
            let delay_ms = dispatch_time.duration_since(scheduled).as_secs_f64() * 1000.0;
            delays.push(delay_ms);
            q.try_enqueue(scheduled, batch);
        }

        (scheduled_count, delays)
    });

    let started = Instant::now();
    let deadline = started + config.measurement_duration;

    // 2. Worker lanes
    let mut worker_handles = Vec::with_capacity(lanes);
    let shared_committed_batches = Arc::new(Mutex::new(Vec::new()));
    let shared_timeouts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let shared_errors = Arc::new(std::sync::atomic::AtomicU64::new(0));

    for _ in 0..lanes {
        let worker_queue = Arc::clone(&queue);
        let worker_running = Arc::clone(&running);
        let addr = address.to_string();
        let committed_batches = Arc::clone(&shared_committed_batches);
        let timeouts = Arc::clone(&shared_timeouts);
        let errors = Arc::clone(&shared_errors);

        worker_handles.push(tokio::spawn(async move {
            let client = connect(&addr).await?;
            let mut commit_latencies = Vec::new();
            let mut accepted_changes = 0u64;
            let mut logical_bytes = 0u64;
            let mut applied_changes = Vec::new();

            while worker_running.load(Ordering::Relaxed) {
                let item_opt = {
                    let mut q = worker_queue.lock().await;
                    q.try_dequeue()
                };

                match item_opt {
                    Some(queued) => {
                        let sql = transaction_sql(&queued.item);
                        let commit_start = Instant::now();
                        match client.batch_execute(&sql).await {
                            Ok(_) => {
                                let commit_lat = commit_start.elapsed().as_secs_f64() * 1000.0;
                                commit_latencies.push(commit_lat);
                                accepted_changes += queued.item.len() as u64;
                                logical_bytes += canonical_changes_json(&queued.item).len() as u64;
                                applied_changes.extend(queued.item.clone());
                                committed_batches
                                    .lock()
                                    .await
                                    .push((queued.scheduled_at, queued.item));
                            }
                            Err(e) => {
                                if e.to_string().to_lowercase().contains("timeout") {
                                    timeouts.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }

            Ok::<_, anyhow::Error>((
                commit_latencies,
                accepted_changes,
                logical_bytes,
                applied_changes,
            ))
        }));
    }

    // 3. Reader & Freshness task
    let reader_running = Arc::clone(&running);
    let reader_addr = address.to_string();
    let reader_view = view.to_string();
    let query_interval = config.query_interval;
    let committed_batches_reader = Arc::clone(&shared_committed_batches);

    let reader_handle = tokio::spawn(async move {
        let client = connect(&reader_addr).await?;
        let mut read_latencies = Vec::new();
        let mut freshness_latencies = Vec::new();

        while reader_running.load(Ordering::Relaxed) {
            tokio::time::sleep(query_interval).await;
            let read_start = Instant::now();
            let query = format!("SELECT * FROM {reader_view}");
            if let Ok(_rows) = client.query(&query, &[]).await {
                let read_lat = read_start.elapsed().as_secs_f64() * 1000.0;
                read_latencies.push(read_lat);

                let batches = committed_batches_reader.lock().await;
                if let Some((oldest_scheduled, _)) = batches.last() {
                    let fresh_lat = read_start.elapsed().as_secs_f64() * 1000.0
                        + read_start.duration_since(*oldest_scheduled).as_secs_f64() * 1000.0;
                    freshness_latencies.push(fresh_lat.max(read_lat));
                }
            }
        }
        Ok::<_, anyhow::Error>((read_latencies, freshness_latencies))
    });

    // Run until deadline
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    running.store(false, Ordering::Relaxed);

    let (scheduled_count, delays) = generator_handle.await.context("generator panicked")?;
    let (read_latencies, freshness_latencies) =
        reader_handle.await.context("reader panicked")??;

    let mut all_commit_latencies = Vec::new();
    let mut total_accepted = 0u64;
    let mut total_logical_bytes = 0u64;

    for handle in worker_handles {
        let (commits, accepted, bytes, _applied) = handle.await.context("worker panicked")??;
        all_commit_latencies.extend(commits);
        total_accepted += accepted;
        total_logical_bytes += bytes;
    }

    let q = queue.lock().await;
    let overflow_drops = q.overflow_drops();
    let unfinished_requests = q.current_len() as u64;
    let max_queue_depth = q.current_len();

    let mean_generator_delay = if delays.is_empty() {
        0.0
    } else {
        delays.iter().sum::<f64>() / delays.len() as f64
    };
    let max_generator_delay = delays.iter().copied().fold(0.0f64, f64::max);

    let read_p99 = calculate_p99(read_latencies.clone());
    let commit_p99 = calculate_p99(all_commit_latencies.clone());
    let freshness_p99 = calculate_p99(freshness_latencies.clone());

    let final_source = query_source(&prepared.admin).await?;
    let mut expected = corpus.clone();
    expected.source = final_source.clone();
    expected.changes.clear();
    let (expected_rows, _) = canonical_rows(oracle::complete_output(&expected, oracle_query)?)?;

    // Drain & complete multiset comparison using oracle
    let drain_deadline = Instant::now() + Duration::from_secs(60);
    let rows = loop {
        let rows = query_rows(&prepared.admin, &format!("SELECT * FROM {view}")).await?;
        let (canonical, _) = canonical_rows(rows)?;
        if oracle::compare_multisets(&canonical, &expected_rows).is_ok() {
            break canonical;
        }
        if Instant::now() >= drain_deadline {
            bail!("output did not reach the source frontier within the 60-second drain");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    Ok(DecoupledLoadOutcome {
        duration: started.elapsed(),
        accepted_changes: total_accepted,
        visible_changes: total_accepted,
        latencies: LatencyRecord {
            read_p99_ms: read_p99,
            commit_p99_ms: commit_p99,
            freshness_p99_ms: freshness_p99,
            read_latencies_ms: read_latencies,
            commit_latencies_ms: all_commit_latencies,
            freshness_latencies_ms: freshness_latencies,
        },
        generator_stats: GeneratorStats {
            scheduled_events: scheduled_count,
            dispatched_events: total_accepted,
            dropped_events: overflow_drops,
            queue_overflow_drops: overflow_drops,
            generator_delays_ms: delays,
            max_generator_delay_ms: max_generator_delay,
            mean_generator_delay_ms: mean_generator_delay,
            timeouts: shared_timeouts.load(Ordering::Relaxed),
            errors: shared_errors.load(Ordering::Relaxed),
            unfinished_requests,
            max_queue_depth,
        },
        rows,
        final_source,
        logical_bytes: total_logical_bytes,
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
