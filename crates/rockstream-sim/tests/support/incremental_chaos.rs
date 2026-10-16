//! #125: one source, Int64 SUM/COUNT/AVG, durable pre-ack restart; no cluster SLO claim.
use arrow::{
    array::{ArrayRef, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use object_store::memory::InMemory;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use rockstream_connectors::{
    OffsetToken, PollDeltaResult, SnapshotDeltaFence, SnapshotStream, SourceCheckpointStore,
    SourceConnector, SourceError, SourceRuntimeCoordinator,
};
use rockstream_ops::{
    aggregate::{load_frontier, AggregateOp},
    sink::{read_view_output, ColumnValue, ViewSinkOp},
    ArrowZSet,
};
use rockstream_sim::SimClock;
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_test_support::typed_output::{
    add_batch_rows, compare_results, empty_result, OracleMismatch, TypedResult,
};
use rockstream_types::{
    connector::PartitionFilter,
    ids::{ConnectorId, OperatorId},
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const WORKLOAD_SEED: u64 = 0x125_111;
const FAULT_SEED: u64 = 0xdead_beef_cafe_1234;
const LIMIT_MS: u64 = 60_000;
const OP: OperatorId = OperatorId(125);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Delta {
    key: i64,
    value: i64,
    weight: i64,
}

fn history(seed: u64) -> Vec<Delta> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let key = rng.gen_range(0..4);
    let a = rng.gen_range(-100..100);
    let b = a + 1; // Every history includes a fractional average and a retraction.
    [
        (key, a, 1),
        (key, b, 1),
        (key, a, -1),
        (key, a, 1),
        (key, b, -1),
        (key + 1, b, 1),
    ]
    .into_iter()
    .map(|(key, value, weight)| Delta { key, value, weight })
    .collect()
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Fault {
    epoch: u64,
    unavailable_ms: u64,
}

fn fault(seed: u64) -> Fault {
    let mut rng = SmallRng::seed_from_u64(seed);
    Fault {
        epoch: rng.gen_range(2..=4),
        unavailable_ms: rng.gen_range(1..=10) * 100,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum Control {
    None,
    Value,
    Duplicate,
    MissingAck,
    Late,
}

#[derive(Debug, PartialEq, Eq)]
enum Failure {
    Output(OracleMismatch),
    Progress {
        expected: (u64, Vec<u8>, Option<u64>),
        actual: (u64, Vec<u8>, Option<u64>),
    },
    Acknowledgements {
        expected: Vec<u64>,
        actual: Vec<u64>,
    },
    RecoveryDeadline {
        elapsed_ms: u64,
        limit_ms: u64,
    },
}

#[derive(Default)]
struct Journal {
    polled: Vec<Delta>,
    acknowledgements: Vec<u64>,
    unavailable_until: Option<Duration>,
    suppress: bool,
}

struct Source {
    history: Arc<Vec<Delta>>,
    journal: Arc<Mutex<Journal>>,
    clock: SimClock,
}

fn offset(epoch: u64) -> OffsetToken {
    OffsetToken::new(epoch.to_be_bytes().to_vec())
}

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]))
}

#[async_trait]
impl SourceConnector for Source {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(input_schema())
    }
    async fn start_snapshot(
        &mut self,
        _: &SnapshotDeltaFence,
        _: Option<OffsetToken>,
        _: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        unreachable!("this scenario qualifies delta ingestion only")
    }
    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        _: usize,
        _: usize,
        _: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        let index = u64::from_be_bytes(after.as_bytes().try_into().unwrap()) as usize;
        let delta = self.history[index].clone();
        self.journal.lock().unwrap().polled.push(delta.clone());
        let columns: Vec<ArrayRef> = [delta.key, delta.value, delta.weight]
            .into_iter()
            .map(|v| Arc::new(Int64Array::from(vec![v])) as ArrayRef)
            .collect();
        Ok(PollDeltaResult {
            batches: vec![RecordBatch::try_new(input_schema(), columns).unwrap()],
            new_offset: offset(index as u64 + 1),
            watermark: None,
        })
    }
    async fn commit_offset(&mut self, epoch: u64, token: OffsetToken) -> Result<(), SourceError> {
        assert_eq!(token, offset(epoch));
        let mut journal = self.journal.lock().unwrap();
        if journal
            .unavailable_until
            .is_some_and(|until| self.clock.current_offset() < until)
        {
            return Err(SourceError::Io("scheduled source outage".into()));
        }
        if !journal.suppress {
            journal.acknowledgements.push(epoch);
        }
        Ok(())
    }
    async fn pause(&mut self, _: String) -> Result<(), SourceError> {
        Ok(())
    }
    async fn resume(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

fn coordinator(
    db: Arc<ShardDb>,
    history: Arc<Vec<Delta>>,
    journal: Arc<Mutex<Journal>>,
    clock: SimClock,
) -> SourceRuntimeCoordinator<Source> {
    SourceRuntimeCoordinator::new(
        Source {
            history,
            journal,
            clock,
        },
        ConnectorId(125),
        offset(0),
        SourceCheckpointStore::new(db, 0, ConnectorId(125)),
    )
}

fn output_batch(rows: &[(i64, i64, i64, f64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("sum_v", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("avg_v", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.3))),
        ],
    )
    .unwrap()
}

// Independent batch arithmetic: no production operator or persisted state supplies expected rows.
fn reference(history: &[Delta]) -> TypedResult {
    let mut groups = BTreeMap::<i64, (i64, i64)>::new();
    for delta in history {
        let (sum, count) = groups.entry(delta.key).or_default();
        *sum += delta.value * delta.weight;
        *count += delta.weight;
    }
    let rows: Vec<_> = groups
        .into_iter()
        .filter(|(_, (_, count))| *count > 0)
        .map(|(key, (sum, count))| (key, sum, count, sum as f64 / count as f64))
        .collect();
    let batch = output_batch(&rows);
    let mut result = empty_result(batch.schema().as_ref());
    add_batch_rows(&mut result, &batch, &vec![1; rows.len()]);
    result
}

async fn persisted(db: &ShardDb) -> TypedResult {
    let mut result = empty_result(output_batch(&[]).schema().as_ref());
    for (_, _, columns, weight) in read_view_output(db, OP, 4).await.unwrap() {
        let row = match columns.as_slice() {
            [ColumnValue::Int64(k), ColumnValue::Int64(sum), ColumnValue::Int64(count), ColumnValue::Float64(avg)] => {
                (*k, *sum, *count, *avg)
            }
            other => panic!("unexpected persisted types: {other:?}"),
        };
        add_batch_rows(&mut result, &output_batch(&[row]), &[weight]);
    }
    result
}

fn sorted_rows(result: &TypedResult) -> Vec<String> {
    let mut rows: Vec<_> = result
        .rows
        .iter()
        .map(|(r, w)| format!("{r:?}: {w}"))
        .collect();
    rows.sort();
    rows
}

struct Run {
    failure: Option<Failure>,
    events: Vec<Value>,
    polled: Vec<Delta>,
}

async fn check(
    db: &ShardDb,
    source: &SourceRuntimeCoordinator<Source>,
    history: &[Delta],
    epoch: u64,
    phase: &str,
    events: &mut Vec<Value>,
) -> Result<(), Failure> {
    let expected = reference(history);
    let actual = persisted(db).await;
    let progress = (
        source.committed_epoch(),
        source.committed_offset().as_bytes().to_vec(),
        load_frontier(db).await.unwrap(),
    );
    events.push(json!({"transition": phase, "frontier": epoch, "expected_fields": format!("{:?}", expected.fields),
        "actual_fields": format!("{:?}", actual.fields), "expected_rows": sorted_rows(&expected),
        "actual_rows": sorted_rows(&actual), "source_progress": progress}));
    compare_results(&expected, &actual).map_err(Failure::Output)?;
    let expected_progress = (epoch, offset(epoch).as_bytes().to_vec(), Some(epoch));
    if progress != expected_progress {
        return Err(Failure::Progress {
            expected: expected_progress,
            actual: progress,
        });
    }
    Ok(())
}

async fn execute(history: &[Delta], fault: Option<Fault>, control: Control) -> Run {
    let store = Arc::new(InMemory::new());
    let mut db = Arc::new(
        ShardDb::builder("incremental-chaos", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let history = Arc::new(history.to_vec());
    let journal = Arc::new(Mutex::new(Journal::default()));
    let clock = SimClock::new();
    let mut events = vec![];
    let result: Result<(), Failure> = async {
        let mut source = coordinator(db.clone(), history.clone(), journal.clone(), clock.clone());
        let mut aggregate = AggregateOp::new(OP);
        assert_eq!(source.recover().await.unwrap(), None);
        let mut owner = source.acquire_owner("initial").unwrap();
        events.push(json!({"transition": "source_recovered_empty", "at_ms": 0}));
        for index in 0..history.len() {
            let epoch = index as u64 + 1;
            let polled = source.poll_delta(1024, 1).await.unwrap();
            events.push(json!({"transition": "source_delta_polled", "epoch": epoch, "at_ms": clock.current_offset().as_millis()}));
            let batch = polled.batches[0].clone();
            let weight = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            let mut out = aggregate.process_delta(ArrowZSet::new(batch, vec![weight])).unwrap();
            events.push(json!({"transition": "aggregate_processed", "operator": "AggregateOp", "epoch": epoch, "at_ms": clock.current_offset().as_millis()}));
            if epoch == 1 && control == Control::Value {
                let mut columns = out.data.columns().to_vec();
                let sum = columns[1].as_any().downcast_ref::<Int64Array>().unwrap().value(0);
                columns[1] = Arc::new(Int64Array::from(vec![sum + 1]));
                out.data = RecordBatch::try_new(out.data.schema(), columns).unwrap();
            }
            if epoch == 1 && control == Control::Duplicate {
                out.data = arrow::compute::concat_batches(&out.data.schema(), [&out.data, &out.data]).unwrap();
                out.weights.push(out.weights[0]);
            }
            let mut batch = aggregate.state_write_batch();
            ViewSinkOp::new(db.clone(), OP).append_epoch(&mut batch, &out, epoch);
            batch.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
            let crashing = fault.is_some_and(|f| f.epoch == epoch);
            if crashing {
                let duration = fault.unwrap().unavailable_ms;
                journal.lock().unwrap().unavailable_until = Some(clock.current_offset() + Duration::from_millis(duration));
                events.push(json!({"transition": "source_outage_started", "epoch": epoch, "at_ms": clock.current_offset().as_millis(), "unavailable_ms": duration}));
            }
            let committed = source.commit_epoch(&owner, epoch, polled.new_offset, batch).await;
            if crashing {
                let error = committed.unwrap_err().to_string();
                assert_eq!(error, "RS-4001: source I/O error: RS-4016: M3 source input committed but upstream acknowledgement failed: RS-4001: source I/O error: scheduled source outage; next steps: recover the checkpoint and retry acknowledgement");
                let started = clock.current_offset();
                events.push(json!({"transition": "durable_commit_ack_failed", "epoch": epoch, "at_ms": started.as_millis(), "error": error}));
                check(&db, &source, &history[..=index], epoch, "durable_frontier_before_restart", &mut events).await?;
                drop(source);
                drop(aggregate);
                // Restart runtime objects and reopen storage after the durable M3 commit.
                db.as_ref().clone().close().await.unwrap();
                db = Arc::new(ShardDb::builder("incremental-chaos", store.clone()).build().await.unwrap());
                source = coordinator(db.clone(), history.clone(), journal.clone(), clock.clone());
                let checkpoint = source.recover().await.unwrap().unwrap();
                assert_eq!((checkpoint.source_epoch, checkpoint.token), (epoch, offset(epoch)));
                owner = source.acquire_owner("restarted").unwrap();
                aggregate = AggregateOp::load_from_storage(&db, OP).await.unwrap();
                check(&db, &source, &history[..=index], epoch, "restart_restored", &mut events).await?;
                journal.lock().unwrap().suppress = control == Control::MissingAck;
                loop {
                    match source.acknowledge_recovered(&owner).await {
                        Ok(()) => break,
                        Err(error) => {
                            events.push(json!({"transition": "recovered_ack_blocked", "at_ms": clock.current_offset().as_millis(), "error": error.to_string()}));
                            clock.advance(Duration::from_millis(100));
                        }
                    }
                    if clock.current_offset() - started > Duration::from_millis(LIMIT_MS + 100) { panic!("unbounded recovery retry"); }
                }
                let elapsed_ms = (clock.current_offset() - started).as_millis() as u64;
                events.push(json!({"transition": "recovered_ack_completed", "at_ms": clock.current_offset().as_millis(), "elapsed_ms": elapsed_ms}));
                if elapsed_ms > LIMIT_MS { return Err(Failure::RecoveryDeadline { elapsed_ms, limit_ms: LIMIT_MS }); }
            } else {
                committed.unwrap();
                events.push(json!({"transition": "source_m3_commit_ack_completed", "epoch": epoch, "at_ms": clock.current_offset().as_millis()}));
            }
            aggregate.clear_dirty_keys();
            check(&db, &source, &history[..=index], epoch, "frontier_verified", &mut events).await?;
            let actual = journal.lock().unwrap().acknowledgements.clone();
            let expected = (1..=epoch).collect::<Vec<_>>();
            events.push(json!({"transition": "source_acknowledgements", "expected": expected, "actual": actual}));
            if actual != expected { return Err(Failure::Acknowledgements { expected, actual }); }
        }
        Ok(())
    }.await;
    db.as_ref().clone().close().await.unwrap();
    let polled = journal.lock().unwrap().polled.clone();
    Run {
        failure: result.err(),
        events,
        polled,
    }
}

async fn recorded(workload_seed: u64, fault_seed: u64, control: Control) -> Run {
    let history = history(workload_seed);
    let mut schedule = fault(fault_seed);
    if control == Control::Late {
        schedule.unavailable_ms = LIMIT_MS + 100;
    }
    let reference_run = execute(&history, None, Control::None).await;
    assert_eq!(reference_run.failure, None);
    assert_eq!(reference_run.polled, history);
    let run = execute(&history, Some(schedule), control).await;
    let mut minimized = history.clone();
    if let Some(failure) = &run.failure {
        // Minimize to the shortest prefix reproducing the exact failure; preserve source offsets.
        for end in 1..=history.len() {
            if execute(&history[..end], Some(schedule), control)
                .await
                .failure
                .as_ref()
                == Some(failure)
            {
                minimized = history[..end].to_vec();
                break;
            }
        }
    } else {
        assert_eq!(run.polled, history);
    }
    let artifact = json!({"scenario": "source_m3_int64_sum_count_avg_pre_ack_restart", "workload_seed": workload_seed,
        "fault_seed": fault_seed, "fault_schedule": schedule, "control": control, "history": history,
        "reference_polled_history": reference_run.polled, "faulted_polled_history": run.polled,
        "reference_transitions": reference_run.events, "production_transitions": run.events,
        "failure": run.failure.as_ref().map(|f| format!("{f:?}")),
        "minimization": "shortest prefix reproducing the exact failure", "minimized_failing_history": if run.failure.is_some() { Some(minimized) } else { None }});
    let directory = std::env::var_os("ROCKSTREAM_INCREMENTAL_CHAOS_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/incremental-chaos")
        });
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(format!(
            "{workload_seed:016x}-{fault_seed:016x}-{control:?}.json"
        )),
        serde_json::to_vec_pretty(&artifact).unwrap(),
    )
    .unwrap();
    run
}

pub async fn regression() {
    let first = recorded(WORKLOAD_SEED, FAULT_SEED, Control::None).await;
    assert_eq!(first.failure, None);
    let replay = execute(
        &history(WORKLOAD_SEED),
        Some(fault(FAULT_SEED)),
        Control::None,
    )
    .await;
    assert_eq!((first.polled, first.events), (replay.polled, replay.events));
    assert_eq!(replay.failure, None);
}

#[tokio::test]
async fn value_and_duplicate_negative_controls() {
    let expected = reference(&history(WORKLOAD_SEED)[..1]);
    let delta = &history(WORKLOAD_SEED)[0];
    for control in [Control::Value, Control::Duplicate] {
        let batch = output_batch(&[(
            delta.key,
            delta.value + i64::from(control == Control::Value),
            1,
            delta.value as f64,
        )]);
        let mut corrupted = empty_result(batch.schema().as_ref());
        add_batch_rows(
            &mut corrupted,
            &batch,
            &[if control == Control::Duplicate { 2 } else { 1 }],
        );
        let mismatch = compare_results(&expected, &corrupted).unwrap_err();
        assert_eq!(
            recorded(WORKLOAD_SEED, FAULT_SEED, control).await.failure,
            Some(Failure::Output(mismatch))
        );
    }
}

#[tokio::test]
async fn acknowledgement_and_deadline_negative_controls() {
    let epoch = fault(FAULT_SEED).epoch;
    assert_eq!(
        recorded(WORKLOAD_SEED, FAULT_SEED, Control::MissingAck)
            .await
            .failure,
        Some(Failure::Acknowledgements {
            expected: (1..=epoch).collect(),
            actual: (1..epoch).collect()
        })
    );
    assert_eq!(
        recorded(WORKLOAD_SEED, FAULT_SEED, Control::Late)
            .await
            .failure,
        Some(Failure::RecoveryDeadline {
            elapsed_ms: LIMIT_MS + 100,
            limit_ms: LIMIT_MS
        })
    );
}

#[tokio::test]
async fn bounded_seed_run() {
    let seeds: u64 = std::env::var("ROCKSTREAM_INCREMENTAL_CHAOS_SEEDS")
        .map(|v| v.parse().expect("integer seed count"))
        .unwrap_or(2);
    assert!(
        (1..=128).contains(&seeds),
        "seed run must be bounded to 1..=128"
    );
    for seed in 0..seeds {
        assert_eq!(
            recorded(WORKLOAD_SEED + seed, FAULT_SEED, Control::None)
                .await
                .failure,
            None
        );
        assert_eq!(
            recorded(WORKLOAD_SEED, FAULT_SEED + seed + 1, Control::None)
                .await
                .failure,
            None
        );
    }
}
