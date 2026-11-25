//! Worker client daemon for RockStream.
//!
//! Manages registration, periodic heartbeats, shard lease assignment,
//! and fencing validation with the control plane over TCP.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use serde_json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use rockstream_types::config::ExchangeConfig;
use rockstream_types::data_plane::{
    DeploymentDescriptor, RuntimeExchangeMessage, RuntimeOutputDelta, RuntimeRow, ShardOutput,
    DEPLOYMENT_DESCRIPTOR_VERSION, WORKLOAD_SNAPSHOT_VERSION,
};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLocation,
    WorkerMessage, WorkerRegistration,
};

use crate::epoch_compaction::{EpochCompactionConfig, EpochCompactor};
use crate::secrets::WorkerSecretManager;
use crate::shard_actor::{CancellationReporter, OwnedFrameExecutor, ShardActorRegistry};
use rockstream_ops::PhysicalCommitGroup;
use rockstream_storage::{ShardDb, WriteBatch};

pub struct WorkerDeployment {
    pub descriptor: DeploymentDescriptor,
    pub schemas: HashMap<String, SchemaRef>,
    pub db: Arc<ShardDb>,
    pub compiled: rockstream_ops::compile::CompiledView,
    pub commit_group: Arc<PhysicalCommitGroup>,
    pub compactor: Arc<EpochCompactor>,
    pub(crate) execution_requires_recovery: tokio::sync::Mutex<bool>,
    owned_epoch: crate::owned_epoch::OwnedEpochJournal,
    recovery_on_open: bool,
}

impl WorkerDeployment {
    pub fn compactor(&self) -> &Arc<EpochCompactor> {
        &self.compactor
    }
}

/// Active-shard state and workload deployments can hold clones of one writer.
/// A close affects every clone, so a handoff or drain must visit it only once.
fn unique_shard_databases(databases: Vec<ShardDb>) -> Vec<ShardDb> {
    let mut unique: Vec<ShardDb> = Vec::with_capacity(databases.len());
    for db in databases {
        if !unique
            .iter()
            .any(|existing| existing.same_open_instance(&db))
        {
            unique.push(db);
        }
    }
    unique
}

/// The caller holds the deployment's execution gate. Journal presence is
/// authoritative even when control has restarted and lost its in-flight map.
async fn validate_idle_journals(deployment: &WorkerDeployment) -> Result<(), String> {
    if deployment.owned_epoch.load().await?.is_some() {
        return Err("workload has an unfinished owned epoch".to_owned());
    }
    if crate::skew_routing::load_snapshot(&deployment.db, deployment.descriptor.workload_id)
        .await?
        .pending
        .is_some()
    {
        return Err("workload has an unfinished routing epoch".to_owned());
    }
    Ok(())
}

async fn verify_workload_idle(
    deployments: &WorkerDeployments,
    direct_grants: &crate::direct_grants::DirectGrantRegistry,
    active_shards: &Arc<RwLock<HashMap<ShardId, ShardState>>>,
    workload_id: WorkloadId,
    anchor: &ShardLease,
) -> Result<(), String> {
    // Drain requests that validated a capability before its deadline. Waiting
    // commands revalidate after taking their shared side of this gate.
    let _admission = direct_grants.installation_guard(workload_id).await?;
    let deployment = deployments
        .read()
        .get(&(workload_id, anchor.shard_id))
        .cloned()
        .ok_or("workload anchor is not deployed")?;
    let recovering = deployment.execution_requires_recovery.lock().await;
    if *recovering {
        return Err("workload anchor requires recovery".to_owned());
    }
    if !deployment.descriptor.shard.same_owner(anchor)
        || !active_shards
            .read()
            .get(&anchor.shard_id)
            .is_some_and(|state| state.lease.same_owner(anchor) && state.db.is_some())
    {
        return Err("workload anchor lease changed".to_owned());
    }
    deployment
        .db
        .verify_lease_token(anchor.lease_token)
        .await
        .map_err(|error| error.to_string())?;
    validate_idle_journals(&deployment).await
}

async fn read_workload_shards(
    deployments: &WorkerDeployments,
    active_shards: &Arc<RwLock<HashMap<ShardId, ShardState>>>,
    workload_id: WorkloadId,
    shard_ids: &[ShardId],
) -> io::Result<Vec<ShardOutput>> {
    let targets = {
        let deployed = deployments.read();
        let active = active_shards.read();
        let mut seen = std::collections::HashSet::new();
        let mut targets = Vec::with_capacity(shard_ids.len());
        for shard_id in shard_ids {
            if !seen.insert(*shard_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate shard in workload read request",
                ));
            }
            let deployment = deployed
                .get(&(workload_id, *shard_id))
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "shard is not deployed"))?;
            if !active.get(shard_id).is_some_and(|state| {
                state.lease.worker_id == deployment.descriptor.shard.worker_id
                    && state.lease.lease_token == deployment.descriptor.shard.lease_token
                    && state.db.is_some()
            }) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "shard lease is not active",
                ));
            }
            targets.push(deployment);
        }
        targets
    };
    if targets.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workload read requires at least one shard",
        ));
    }

    let mut outputs = Vec::with_capacity(targets.len());
    for deployment in targets {
        let descriptor = &deployment.descriptor;
        // Read state and its durable frontier under the same gate as writes,
        // routing prepare, and routing finish. The coordinator then compares
        // these per-shard cuts rather than accepting a mixed-epoch snapshot.
        let recovering = deployment.execution_requires_recovery.lock().await;
        if *recovering {
            return Err(io::Error::other("workload shard requires recovery"));
        }
        if !active_shards
            .read()
            .get(&descriptor.shard.shard_id)
            .is_some_and(|state| state.lease.same_owner(&descriptor.shard) && state.db.is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shard lease changed during read",
            ));
        }
        deployment
            .db
            .verify_lease_token(descriptor.shard.lease_token)
            .await
            .map_err(io::Error::other)?;
        let routing = crate::skew_routing::load_snapshot(&deployment.db, workload_id)
            .await
            .map_err(io::Error::other)?;
        if routing.pending.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "skew routing epoch is not fully committed",
            ));
        }
        let committed_epoch = match deployment
            .db
            .get(&rockstream_storage::ShardKeyEncoder::frontier_key())
            .await
            .map_err(io::Error::other)?
        {
            Some(bytes) => u64::from_be_bytes(
                bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| io::Error::other("invalid committed read frontier"))?,
            ),
            None => 0,
        };
        let routing_epoch = if let Some(completed) = routing.completed {
            if completed.shard_ids.first() != Some(&descriptor.shard.shard_id) {
                return Err(io::Error::other("routing journal is on a non-anchor shard"));
            }
            Some(completed.epoch)
        } else if descriptor.shard_index == 0 {
            // Before the first journal, the anchor's durable frontier is its
            // certificate. Uneven legacy participants fail closed until a
            // complete direct epoch advances every shard.
            Some(committed_epoch)
        } else {
            None
        };
        let stored = rockstream_ops::read_view_output(
            &deployment.db,
            descriptor.sink_operator_id,
            descriptor.output_columns.len(),
        )
        .await
        .map_err(io::Error::other)?;
        let rows = rockstream_ops::sink::materialize_view_state(stored, &descriptor.primary_key)
            .into_values()
            .filter(|(_, count)| *count > 0)
            .map(|(row, weight)| {
                let values_tsv =
                    String::from_utf8(rockstream_ops::sink::column_values_to_tsv_bytes(&row))
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                Ok(RuntimeRow { values_tsv, weight })
            })
            .collect::<io::Result<Vec<_>>>()?;
        deployment
            .db
            .verify_lease_token(descriptor.shard.lease_token)
            .await
            .map_err(io::Error::other)?;
        outputs.push(ShardOutput {
            shard_id: descriptor.shard.shard_id,
            lease_token: Some(descriptor.shard.lease_token),
            committed_epoch: Some(committed_epoch),
            routing_epoch,
            deltas: Vec::new(),
            rows,
        });
    }
    outputs.sort_by_key(|shard| shard.shard_id);
    Ok(outputs)
}

pub type WorkerDeployments = Arc<RwLock<HashMap<(WorkloadId, ShardId), Arc<WorkerDeployment>>>>;
type FrontierReportWaiter =
    Arc<parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<Result<u64, String>>>>>;

pub(crate) fn deployment_schema(
    descriptor: &DeploymentDescriptor,
) -> io::Result<HashMap<String, SchemaRef>> {
    deployment_schemas_from_slice(&descriptor.schemas)
}

pub(crate) fn deployment_schemas_from_slice(
    schemas: &[rockstream_types::data_plane::DeploymentSchema],
) -> io::Result<HashMap<String, SchemaRef>> {
    schemas
        .iter()
        .map(|schema| {
            let fields = schema
                .columns
                .iter()
                .map(|column| {
                    let data_type = match column.data_type.to_ascii_lowercase().as_str() {
                        "i64" | "int64" | "bigint" => DataType::Int64,
                        "f64" | "float64" | "double" => DataType::Float64,
                        "bool" | "boolean" => DataType::Boolean,
                        "string" | "utf8" | "text" => DataType::Utf8,
                        other => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("unsupported deployment type {other}"),
                            ));
                        }
                    };
                    Ok(Field::new(&column.name, data_type, true))
                })
                .collect::<io::Result<Vec<_>>>()?;
            Ok((schema.relation.clone(), Arc::new(Schema::new(fields))))
        })
        .collect()
}

pub(crate) fn rows_to_zset(
    rows: &[RuntimeRow],
    schema: SchemaRef,
) -> io::Result<rockstream_ops::zset::ArrowZSet> {
    let split = rows
        .iter()
        .map(|row| row.values_tsv.split('\t').collect::<Vec<_>>())
        .collect::<Vec<_>>();
    if split.iter().any(|row| row.len() != schema.fields().len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TSV row width does not match source schema",
        ));
    }
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column, field)| -> io::Result<ArrayRef> {
            macro_rules! parsed {
                ($ty:ty, $array:ty) => {{
                    let values = split
                        .iter()
                        .map(|row| {
                            (!row[column].is_empty() && row[column] != "\\N")
                                .then(|| row[column].parse::<$ty>())
                                .transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    Ok(Arc::new(<$array>::from(values)) as ArrayRef)
                }};
            }
            match field.data_type() {
                DataType::Int64 => parsed!(i64, Int64Array),
                DataType::Float64 => parsed!(f64, Float64Array),
                DataType::Boolean => {
                    let values = split
                        .iter()
                        .map(|row| match row[column] {
                            "" | "\\N" => Ok(None),
                            value if value.eq_ignore_ascii_case("true") => Ok(Some(true)),
                            value if value.eq_ignore_ascii_case("false") => Ok(Some(false)),
                            value => Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("invalid boolean value {value:?}"),
                            )),
                        })
                        .collect::<io::Result<Vec<_>>>()?;
                    Ok(Arc::new(BooleanArray::from(values)))
                }
                DataType::Utf8 => Ok(Arc::new(StringArray::from(
                    split
                        .iter()
                        .map(|row| {
                            (!row[column].is_empty() && row[column] != "\\N").then_some(row[column])
                        })
                        .collect::<Vec<_>>(),
                ))),
                other => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported Arrow type {other}"),
                )),
            }
        })
        .collect::<io::Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(rockstream_ops::zset::ArrowZSet::new(
        batch,
        rows.iter().map(|row| row.weight).collect(),
    ))
}

pub(crate) fn zset_to_rows(zset: &rockstream_ops::zset::ArrowZSet) -> io::Result<Vec<RuntimeRow>> {
    (0..zset.data.num_rows())
        .map(|row| {
            let values_tsv = zset
                .data
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row) {
                        return Ok(String::new());
                    }
                    match column.data_type() {
                        DataType::Int64 => column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .map(|array| array.value(row).to_string()),
                        DataType::Float64 => column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .map(|array| array.value(row).to_string()),
                        DataType::Boolean => column
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .map(|array| array.value(row).to_string()),
                        DataType::Utf8 => column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .map(|array| array.value(row).to_string()),
                        other => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("unsupported Arrow type {other}"),
                            ));
                        }
                    }
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Arrow array type mismatch")
                    })
                })
                .collect::<io::Result<Vec<_>>>()?
                .join("\t");
            Ok(RuntimeRow {
                values_tsv,
                weight: zset.weights[row],
            })
        })
        .collect()
}

fn failure_identity(frame: &RuntimeExchangeMessage) -> RuntimeOutputDelta {
    RuntimeOutputDelta {
        attempt_id: frame.attempt_id,
        version: frame.version,
        request_id: frame.request_id.clone(),
        workload_id: frame.workload_id,
        shard_id: frame.shard_id,
        epoch: frame.epoch,
        operator_id: frame.operator_id,
        lease_token: frame.lease_token,
        source: frame.source.clone(),
        rows: Vec::new(),
    }
}

fn execution_record(frame: &RuntimeExchangeMessage) -> io::Result<(Vec<u8>, [u8; 32])> {
    // shortcut: retain execution records until a durable replay-retention watermark is available.
    let mut key = rockstream_storage::keys::ShardKeyEncoder::meta_key(b"owned-execution/");
    key.extend_from_slice(&frame.epoch.to_be_bytes());
    let mut input = serde_json::to_vec(&(
        frame.version,
        &frame.request_id,
        frame.workload_id,
        frame.shard_id,
        frame.operator_id,
        frame.epoch,
        &frame.source,
        &frame.rows,
    ))
    .map_err(io::Error::other)?;
    // Preserve pre-skew replay identities for ordinary frames. A bootstrap is
    // part of the same durable input and cannot be changed on a retry.
    if let Some(bootstrap) = &frame.join_bootstrap {
        input.extend_from_slice(b"\0join-bootstrap/v1\0");
        input.extend_from_slice(&serde_json::to_vec(bootstrap).map_err(io::Error::other)?);
    }
    Ok((key, *blake3::hash(&input).as_bytes()))
}

fn validate_join_bootstrap(
    deployment: &WorkerDeployment,
    frame: &RuntimeExchangeMessage,
) -> io::Result<()> {
    let Some(bootstrap) = &frame.join_bootstrap else {
        return Ok(());
    };
    let join = deployment.compiled.join.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "join bootstrap requires an inner join",
        )
    })?;
    if !matches!(
        join.pipeline.join(),
        rockstream_ops::live_exec::JoinKind::Inner(_)
            | rockstream_ops::live_exec::JoinKind::Factorized(_)
    ) || bootstrap.source == frame.source
        || !((frame.source == join.left_source && bootstrap.source == join.right_source)
            || (frame.source == join.right_source && bootstrap.source == join.left_source))
        || bootstrap.rows.iter().any(|row| row.weight <= 0)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "join bootstrap must contain positive rows from the opposite input",
        ));
    }
    crate::skew_join::validate_join_final_rows(&deployment.descriptor, &bootstrap.final_rows)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let bytes = bootstrap
        .rows
        .iter()
        .chain(&bootstrap.final_rows)
        .try_fold(0usize, |bytes, row| {
            bytes.checked_add(row.values_tsv.len() + std::mem::size_of::<i64>())
        });
    if bytes.is_none_or(|bytes| bytes > crate::skew_join::MAX_JOIN_BOOTSTRAP_BYTES) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "join bootstrap exceeds the bounded replication budget",
        ));
    }
    Ok(())
}

fn cancellation_reporter(sender: mpsc::Sender<WorkerMessage>) -> CancellationReporter {
    Arc::new(move |frame| {
        let output = failure_identity(frame);
        let sender = sender.clone();
        Box::new(move || {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = sender
                        .send(WorkerMessage::ExecutionFailed {
                            output,
                            message: "worker execution interrupted before completion".to_owned(),
                        })
                        .await;
                });
            }
        })
    })
}

pub async fn execute_frame(
    client: &WorkerClientHandle,
    deployments: &WorkerDeployments,
    frame: RuntimeExchangeMessage,
) -> io::Result<()> {
    let deployment = deployments
        .read()
        .get(&(frame.workload_id, frame.shard_id))
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "workload is not deployed"))?;
    let mut needs_recovery = deployment.execution_requires_recovery.lock().await;
    let descriptor = &deployment.descriptor;
    client
        .secret_manager
        .initialize_workload_epoch(frame.workload_id, descriptor.frontier);
    if frame.version != DEPLOYMENT_DESCRIPTOR_VERSION
        || frame.shard_id != descriptor.shard.shard_id
        || frame.operator_id != descriptor.sink_operator_id
        || frame.lease_token != descriptor.shard.lease_token
        || client.worker_id() != Some(descriptor.shard.worker_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "stale or mismatched execution identity",
        ));
    }
    if !client.verify_local_lease_fence(frame.shard_id, frame.lease_token) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "RS-1702: lease fence rejected execution",
        ));
    }
    deployment
        .db
        .verify_lease_token(frame.lease_token)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error.to_string()))?;
    if *needs_recovery {
        return Err(io::Error::other(
            "worker deployment requires recovery after failed execution",
        ));
    }
    if let Some(intent) = deployment
        .owned_epoch
        .load()
        .await
        .map_err(io::Error::other)?
    {
        if intent.workload_id != frame.workload_id
            || Some(intent.attempt_id) != frame.attempt_id
            || intent.request_id != frame.request_id
            || intent.epoch != frame.epoch
            || intent.source != frame.source
            || !intent.target_shards.contains(&frame.shard_id)
        {
            return Err(io::Error::other(
                "another owned source epoch requires reconciliation",
            ));
        }
    }
    let schema = deployment
        .schemas
        .get(&frame.source)
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown source relation"))?;
    validate_join_bootstrap(&deployment, &frame)?;
    client
        .secret_manager
        .advance_workload_epoch(frame.workload_id, frame.epoch);
    let (execution_key, execution_digest) = execution_record(&frame)?;
    if let Some(frontier) = deployment
        .db
        .get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
        .await
        .map_err(io::Error::other)?
    {
        let bytes: [u8; 8] = frontier.as_ref().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid committed frontier")
        })?;
        let frontier = u64::from_be_bytes(bytes);
        if frame.epoch <= frontier {
            match deployment
                .db
                .get(&execution_key)
                .await
                .map_err(io::Error::other)?
            {
                Some(recorded) if recorded.as_ref() == execution_digest.as_slice() => {
                    client
                        .msg_tx
                        .send(WorkerMessage::ExecutionProgress {
                            output: failure_identity(&frame),
                            input_rows: 0,
                            output_rows: 0,
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::ConnectionAborted,
                                "client channel closed",
                            )
                        })?;
                    return Ok(());
                }
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "committed execution does not match replay",
                    ));
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "committed execution record is unavailable",
                    ));
                }
            }
        }
    }
    if frame.epoch < descriptor.frontier {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "execution epoch precedes the deployment frontier",
        ));
    }
    let worker_id = client
        .worker_id()
        .expect("execution identity checked above");
    let strategy = if deployment
        .compiled
        .join
        .as_ref()
        .is_some_and(|join| join.pipeline.strategy() == "factorized")
    {
        rockstream_types::metrics::R1ExecutionStrategy::Factorized
    } else {
        rockstream_types::metrics::R1ExecutionStrategy::Classic
    };
    frame
        .record_encoded_exchange(worker_id, strategy)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let compacted_input_rows = deployment.compactor.compact_slice(&frame.rows);
    if compacted_input_rows.is_empty()
        && frame
            .join_bootstrap
            .as_ref()
            .is_none_or(|bootstrap| bootstrap.rows.is_empty() && bootstrap.final_rows.is_empty())
    {
        if !client.verify_local_lease_fence(frame.shard_id, frame.lease_token) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "RS-1702: lease fence rejected persistence",
            ));
        }
        deployment
            .db
            .verify_lease_token(frame.lease_token)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error.to_string()))?;
        *needs_recovery = true;
        let mut writes = WriteBatch::new();
        writes.put(&execution_key, &execution_digest);
        deployment
            .commit_group
            .commit_epoch(frame.epoch, writes)
            .await
            .map_err(io::Error::other)?;
        *needs_recovery = false;
        rockstream_types::metrics::add_r1_worker_rows(worker_id, frame.rows.len() as u64, 0);
        client.record_shard_load(&frame, 0);
        let _ = client
            .msg_tx
            .send(WorkerMessage::ExecutionProgress {
                output: RuntimeOutputDelta {
                    attempt_id: frame.attempt_id,
                    version: frame.version,
                    request_id: frame.request_id,
                    workload_id: frame.workload_id,
                    shard_id: frame.shard_id,
                    epoch: frame.epoch,
                    operator_id: frame.operator_id,
                    lease_token: frame.lease_token,
                    source: frame.source,
                    rows: Vec::new(),
                },
                input_rows: frame.rows.len() as u64,
                output_rows: 0,
            })
            .await;
        return Ok(());
    }

    let input = rows_to_zset(&compacted_input_rows, schema)?;
    let process_started = std::time::Instant::now();
    *needs_recovery = true;
    let output = rockstream_types::metrics::with_r1_execution_context(
        rockstream_types::metrics::R1ExecutionContext {
            worker_id,
            workload_id: frame.workload_id,
            shard_id: frame.shard_id,
        },
        || -> io::Result<_> {
            if let Some(join) = &deployment.compiled.join {
                let other_input = |source: &str| {
                    let rows = frame
                        .join_bootstrap
                        .as_ref()
                        .filter(|bootstrap| bootstrap.source == source)
                        .map_or(&[][..], |bootstrap| bootstrap.rows.as_slice());
                    rows_to_zset(rows, deployment.schemas[source].clone())
                };
                if frame.source == join.left_source {
                    join.pipeline
                        .process(input, other_input(&join.right_source)?)
                        .map_err(io::Error::other)
                } else if frame.source == join.right_source {
                    join.pipeline
                        .process(other_input(&join.left_source)?, input)
                        .map_err(io::Error::other)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "source is not an input of the join",
                    ))
                }
            } else {
                deployment
                    .compiled
                    .pipeline
                    .process(input)
                    .map_err(io::Error::other)
            }
        },
    )?;
    let process_nanos = process_started
        .elapsed()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    if !client.verify_local_lease_fence(frame.shard_id, frame.lease_token) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "RS-1702: lease fence rejected persistence",
        ));
    }
    deployment
        .db
        .verify_lease_token(frame.lease_token)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error.to_string()))?;
    let mut raw_output_rows = zset_to_rows(&output)?;
    if let Some(bootstrap) = &frame.join_bootstrap {
        raw_output_rows.extend(bootstrap.final_rows.clone());
    }
    let output_rows = deployment.compactor.compact_slice(&raw_output_rows);
    let output_for_sink = if output_rows.len() != raw_output_rows.len()
        || raw_output_rows.len() != output.num_rows()
    {
        rows_to_zset(&output_rows, output.data.schema())?
    } else {
        output
    };
    let mut writes = WriteBatch::new();
    writes.put(&execution_key, &execution_digest);
    deployment
        .compiled
        .sink
        .append_epoch(&mut writes, &output_for_sink, frame.epoch);
    if let Some(join) = &deployment.compiled.join {
        join.pipeline
            .append_state(&deployment.db, &mut writes)
            .await
            .map_err(io::Error::other)?;
    } else {
        deployment
            .compiled
            .pipeline
            .append_state(&deployment.db, &mut writes)
            .await
            .map_err(io::Error::other)?;
    }
    deployment
        .commit_group
        .commit_epoch(frame.epoch, writes)
        .await
        .map_err(io::Error::other)?;
    if let Some(join) = &deployment.compiled.join {
        join.pipeline.ack_persisted_state();
    } else {
        deployment.compiled.pipeline.ack_persisted_state();
    }
    *needs_recovery = false;
    rockstream_types::metrics::add_r1_worker_rows(
        worker_id,
        frame.rows.len() as u64,
        output_rows.len() as u64,
    );
    client.record_shard_load(&frame, process_nanos);
    let _ = client
        .msg_tx
        .send(WorkerMessage::ExecutionProgress {
            output: RuntimeOutputDelta {
                attempt_id: frame.attempt_id,
                version: frame.version,
                request_id: frame.request_id,
                workload_id: frame.workload_id,
                shard_id: frame.shard_id,
                epoch: frame.epoch,
                operator_id: frame.operator_id,
                lease_token: frame.lease_token,
                source: frame.source,
                rows: output_rows.clone(),
            },
            input_rows: frame.rows.len() as u64,
            output_rows: output_rows.len() as u64,
        })
        .await;
    Ok(())
}

async fn execute_frame_and_report(
    client: &WorkerClientHandle,
    deployments: &WorkerDeployments,
    frame: RuntimeExchangeMessage,
) -> Result<(), String> {
    let output = failure_identity(&frame);
    if let Err(error) = execute_frame(client, deployments, frame).await {
        tracing::warn!(
            code = %rockstream_types::error_code::RS_0001,
            %error,
            "worker execution refused"
        );
        let message = error.to_string();
        let _ = client
            .msg_tx
            .send(WorkerMessage::ExecutionFailed {
                output,
                message: message.clone(),
            })
            .await;
        return Err(message);
    }
    Ok(())
}

pub(crate) fn owned_shuffle_handler(
    actors: ShardActorRegistry,
    deployments: WorkerDeployments,
    client: WorkerClientHandle,
) -> crate::exchange::service::OwnedShuffleHandler {
    Arc::new(move |frame, context, require_actor_grant| {
        let actors = actors.clone();
        let deployments = deployments.clone();
        let client = client.clone();
        Box::pin(async move {
            let admission = match client
                .direct_grants
                .command_guard(&context, crate::direct_grants::now_ms())
                .await
            {
                Ok(admission) => admission,
                Err(error) => {
                    client
                        .exchange_registry
                        .audit_direct_epoch_denial(&frame)
                        .await
                        .map_err(|audit| audit.to_string())?;
                    return Err(error);
                }
            };
            let context = match client.direct_grants.unwrap_context(
                context,
                &frame,
                require_actor_grant,
                crate::direct_grants::now_ms(),
            ) {
                Ok(context) => context,
                Err(error) => {
                    drop(admission);
                    client
                        .exchange_registry
                        .audit_direct_epoch_denial(&frame)
                        .await
                        .map_err(|audit| audit.to_string())?;
                    return Err(error);
                }
            };
            // Keep admission through the execution receipt, including time in
            // the actor queue. Replacement grants first drain this guard.
            let _admission = admission;
            let envelope: serde_json::Value = serde_json::from_slice(&context)
                .map_err(|_| "invalid owned shuffle context".to_owned())?;
            if envelope.get("operation").is_some() {
                // A malformed routing envelope must never fall through to
                // source execution with the read-only authorization decision.
                let command =
                    serde_json::from_value::<crate::skew_routing::RoutingCommand>(envelope)
                        .map_err(|_| "invalid owned routing context".to_owned())?;
                return crate::skew_routing::handle_command(&deployments, &client, frame, command)
                    .await;
            }
            let mut owner: RuntimeExchangeMessage = serde_json::from_slice(&context)
                .map_err(|error| format!("invalid shuffle execution context: {error}"))?;
            let output = failure_identity(&owner);
            let prepared = (|| -> Result<(), String> {
                let (expected_target_shard, schema) = deployments
                    .read()
                    .get(&(owner.workload_id, owner.shard_id))
                    .and_then(|deployment| {
                        deployment
                            .schemas
                            .get(&owner.source)
                            .cloned()
                            .map(|schema| (deployment.descriptor.shard_index, schema))
                    })
                    .ok_or_else(|| {
                        "RS-3004: shuffle source is not deployed on this worker".to_owned()
                    })?;
                if !owner.rows.is_empty()
                    || frame.exchange_id != owner.workload_id.0
                    || (frame.target_shard != expected_target_shard
                        && frame.target_shard != (owner.shard_id.0 as u32)
                        && frame.target_shard
                            != owned_shuffle_shard(owner.workload_id, owner.shard_id)
                                .unwrap_or(u32::MAX))
                    || frame.epoch != owner.epoch
                    || frame.seq != 0
                {
                    return Err("owned shuffle identity does not match frame".to_owned());
                }
                let input =
                    crate::exchange::serialization::deserialize_zset(&frame.payload, schema)?;
                if input.num_rows() != frame.row_count as usize {
                    return Err("owned shuffle row count does not match payload".to_owned());
                }
                owner.rows = zset_to_rows(&input).map_err(|error| error.to_string())?;
                Ok(())
            })();
            let receipt = match prepared.and_then(|()| {
                actors
                    .enqueue_with_completion(owner)
                    .map_err(|error| error.to_string())
            }) {
                Ok(receipt) => receipt,
                Err(message) => {
                    let _ = client
                        .msg_tx
                        .send(WorkerMessage::ExecutionFailed {
                            output,
                            message: message.clone(),
                        })
                        .await;
                    return Err(message);
                }
            };
            receipt
                .await
                .map_err(|_| "worker execution receipt was lost".to_owned())?
                .map(|()| bytes::Bytes::new())
        })
    })
}

async fn install_direct_epoch_grant(
    client: &WorkerClientHandle,
    deployments: &WorkerDeployments,
    grant: rockstream_types::direct_epoch::DirectEpochGrant,
) -> Result<(), String> {
    if !grant.metadata.is_valid() || grant.members.is_empty() || grant.members.len() > 4096 {
        return Err("RS-2401: invalid direct epoch grant".into());
    }
    let _admission = client
        .direct_grants
        .installation_guard(grant.metadata.workload_id)
        .await?;
    let mut targets: Vec<_> = deployments
        .read()
        .values()
        .filter(|deployment| deployment.descriptor.workload_id == grant.metadata.workload_id)
        .cloned()
        .collect();
    targets.sort_by_key(|deployment| deployment.descriptor.shard.shard_id);
    if targets.is_empty() || targets.len() > grant.members.len() {
        return Err("RS-2401: direct epoch grant has no complete local membership".into());
    }
    let mut execution_guards = Vec::with_capacity(targets.len());
    for deployment in &targets {
        let guard = deployment.execution_requires_recovery.lock().await;
        if *guard {
            return Err("RS-3602: direct epoch deployment requires recovery".into());
        }
        execution_guards.push(guard);
    }
    for deployment in &targets {
        let shard = &deployment.descriptor.shard;
        if client.worker_id() != Some(shard.worker_id)
            || grant.members.get(&shard.shard_id) != Some(&shard.lease_token)
            || !client.verify_local_lease_fence(shard.shard_id, shard.lease_token)
            || !deployment.schemas.contains_key(&grant.metadata.source)
        {
            return Err("RS-2401: direct epoch grant does not match local deployment".into());
        }
        deployment
            .db
            .verify_lease_token(shard.lease_token)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(intent) = deployment.owned_epoch.load().await? {
            if intent.workload_id != grant.metadata.workload_id
                || intent.source != grant.metadata.source
                || intent.request_id != grant.metadata.request_id
                || intent.epoch != grant.metadata.epoch
            {
                return Err("RS-3602: another owned source epoch requires reconciliation".into());
            }
        }
        let routing =
            crate::skew_routing::load_snapshot(&deployment.db, grant.metadata.workload_id).await?;
        if let Some(pending) = routing.pending {
            if !crate::direct_grants::metadata_matches_record(&grant.metadata, &pending)
                || pending.shard_ids != grant.members.keys().copied().collect::<Vec<_>>()
            {
                return Err("RS-3602: another routing epoch requires reconciliation".into());
            }
        }
    }
    if !client
        .direct_grants
        .install(grant, crate::direct_grants::now_ms())
    {
        return Err(
            "RS-2401: direct epoch grant is expired, conflicting, or exceeds capacity".into(),
        );
    }
    Ok(())
}

fn owned_shuffle_shard(workload_id: WorkloadId, shard_id: ShardId) -> Result<u32, String> {
    let index = shard_id.0.wrapping_sub(workload_id.0.wrapping_mul(16));
    u32::try_from(index)
        .map_err(|_| "owned shuffle shard index exceeds the supported range".to_owned())
}

async fn dispatch_owned_shuffle(
    mux: &crate::exchange::multiplexer::WorkerStreamMultiplexer,
    deployments: &WorkerDeployments,
    target_worker: WorkerId,
    frame: RuntimeExchangeMessage,
) -> Result<(), String> {
    let (src_shard, schema) = deployments
        .read()
        .values()
        .filter(|deployment| deployment.descriptor.workload_id == frame.workload_id)
        .filter_map(|deployment| {
            deployment
                .schemas
                .get(&frame.source)
                .cloned()
                .map(|schema| (deployment.descriptor.shard_index, schema))
        })
        .min_by_key(|(shard_idx, _)| *shard_idx)
        .ok_or_else(|| "shuffle source is not deployed".to_owned())?;
    let target_shard = deployments
        .read()
        .get(&(frame.workload_id, frame.shard_id))
        .map(|deployment| deployment.descriptor.shard_index)
        .unwrap_or_else(|| {
            owned_shuffle_shard(frame.workload_id, frame.shard_id)
                .unwrap_or(frame.shard_id.0 as u32)
        });
    let input = rows_to_zset(&frame.rows, schema).map_err(|error| error.to_string())?;
    let row_count = u32::try_from(input.num_rows())
        .map_err(|_| "shuffle batch has too many rows".to_owned())?;
    let payload = crate::exchange::serialization::serialize_zset(&input)?;
    let context =
        serde_json::to_vec(&failure_identity(&frame)).map_err(|error| error.to_string())?;
    let shuffle = crate::exchange::proto::ShuffleFrame {
        exchange_id: frame.workload_id.0,
        src_shard,
        target_shard,
        epoch: frame.epoch,
        seq: 0,
        payload: payload.into(),
        row_count,
    };
    mux.send_owned_frame(target_worker, shuffle, context.into())
        .await
}

async fn update_owned_epoch(
    client: &WorkerClientHandle,
    deployments: &WorkerDeployments,
    intent: &rockstream_types::data_plane::OwnedEpochIntent,
    finish: bool,
) -> Result<(), String> {
    if intent.version != DEPLOYMENT_DESCRIPTOR_VERSION
        || intent.attempt_id.is_nil()
        || intent.request_id.is_empty()
        || intent.source.is_empty()
        || intent.target_shards.is_empty()
        || !intent.target_shards.contains(&intent.anchor_shard)
        || intent
            .target_shards
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err("invalid owned source epoch".to_owned());
    }
    let deployment = deployments
        .read()
        .get(&(intent.workload_id, intent.anchor_shard))
        .cloned()
        .ok_or_else(|| "owned epoch anchor is not deployed".to_owned())?;
    if client.worker_id() != Some(deployment.descriptor.shard.worker_id) {
        return Err("owned epoch anchor is not assigned to this worker".to_owned());
    }
    if !client
        .check_fence_write(intent.anchor_shard, deployment.descriptor.shard.lease_token)
        .await
        .map_err(|error| error.to_string())?
    {
        return Err("owned epoch anchor is no longer current".to_owned());
    }
    let mut needs_recovery = deployment.execution_requires_recovery.lock().await;
    if *needs_recovery {
        return Err("worker deployment requires recovery after failed execution".to_owned());
    }
    *needs_recovery = true;
    let result = if finish {
        deployment.owned_epoch.finish(intent).await
    } else {
        deployment.owned_epoch.prepare(intent).await
    };
    if result.is_ok() {
        *needs_recovery = false;
    }
    result
}

/// Helper to construct a test worker deployment with in-memory / local storage and epoch compaction.
pub async fn setup_test_deployment(
    storage_dir: &Path,
    compaction_config: EpochCompactionConfig,
) -> (
    WorkerClientHandle,
    WorkerDeployments,
    Arc<ShardDb>,
    Arc<EpochCompactor>,
    mpsc::Receiver<WorkerMessage>,
) {
    let (msg_tx, mut msg_rx) = mpsc::channel(32);
    let worker_id = Arc::new(RwLock::new(Some(WorkerId(42))));
    let active_shards = Arc::new(RwLock::new(HashMap::new()));
    let topology_workers = Arc::new(RwLock::new(HashMap::new()));
    let shard_leases = Arc::new(RwLock::new(HashMap::new()));
    let security_audit_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let frontier_report_waiter = Arc::new(parking_lot::Mutex::new(None));
    let secret_manager = Arc::new(WorkerSecretManager::new("worker-42".to_string()));
    let storage_context = Arc::new(
        rockstream_storage::storage_context::WorkerStorageContext::new_with_worker_id(
            "worker-42",
            536_870_912,
        ),
    );
    let deployments = Arc::new(RwLock::new(HashMap::new()));
    let comp_config_lock = Arc::new(RwLock::new(compaction_config.clone()));

    let exchange_registry = crate::exchange::service::ExchangeRegistry::with_worker_state(
        active_shards.clone(),
        topology_workers.clone(),
    )
    .with_shard_leases(shard_leases.clone())
    .with_security_audit(msg_tx.clone(), security_audit_waiters.clone());

    let client = WorkerClientHandle {
        direct_grants: crate::direct_grants::DirectGrantRegistry::default(),
        shard_loads: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        worker_id,
        active_shards: active_shards.clone(),
        topology_workers,
        shard_leases,
        msg_tx,
        security_audit_waiters,
        fence_waiters: fence_waiters.clone(),
        frontier_report_waiter: frontier_report_waiter.clone(),
        secret_manager,
        storage_context: storage_context.clone(),
        compaction_config: comp_config_lock,
        deployments: deployments.clone(),
        tls_reloader: None,
        exchange_registry,
    };

    let store = rockstream_storage::build_runtime_object_store(storage_dir, "test_root").unwrap();
    let db = Arc::new(
        ShardDb::builder("db", store)
            .with_storage_context(storage_context)
            .build()
            .await
            .unwrap(),
    );
    db.fence_lease_token(LeaseToken(777)).await.unwrap();
    active_shards.write().insert(
        ShardId(100),
        ShardState {
            lease: ShardLease::new(ShardId(100), WorkerId(42), LeaseToken(777)),
            db: Some((*db).clone()),
        },
    );

    let plan = rockstream_plan::PlanNode::ViewSink {
        view_name: "items_view".to_string(),
        pk: vec![0],
        child: Box::new(rockstream_plan::PlanNode::Source {
            name: "items".to_string(),
        }),
    };

    let descriptor = DeploymentDescriptor {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id: WorkloadId(1),
        plan_json: serde_json::to_string(&plan).unwrap(),
        join_strategy: rockstream_types::config::JoinStrategy::Auto,
        schemas: vec![rockstream_types::data_plane::DeploymentSchema {
            relation: "items".to_string(),
            columns: vec![
                rockstream_types::data_plane::DeploymentColumn {
                    name: "id".to_string(),
                    data_type: "i64".to_string(),
                },
                rockstream_types::data_plane::DeploymentColumn {
                    name: "name".to_string(),
                    data_type: "utf8".to_string(),
                },
            ],
        }],
        frontier: 0,
        storage_root: "test_root".to_string(),
        sink_operator_id: OperatorId(10),
        output_columns: vec!["id".to_string(), "name".to_string()],
        primary_key: vec![0],
        merge_key_columns: vec![],
        routing_columns: std::collections::BTreeMap::new(),
        shard: ShardLease::new(ShardId(100), WorkerId(42), LeaseToken(777)),
        storage_identity: format!("lfs:{}", storage_dir.display()),
        // A one-shard deployment uses placement index zero. The legacy
        // workload-relative shuffle alias (84) is validated separately.
        shard_index: 0,
    };

    let schemas = deployment_schema(&descriptor).unwrap();
    let compiled = rockstream_ops::compile_plan_with_sink_id_and_strategy(
        &plan,
        db.clone(),
        &schemas,
        descriptor.sink_operator_id,
        descriptor.join_strategy,
    )
    .unwrap();

    let commit_group = Arc::new(PhysicalCommitGroup::open(db.clone()).await.unwrap());
    let compactor = Arc::new(EpochCompactor::new(compaction_config));
    let owned_epoch = crate::owned_epoch::OwnedEpochJournal::new(db.clone());
    let recovery_on_open = owned_epoch.load().await.unwrap().is_some();

    let deployment = Arc::new(WorkerDeployment {
        descriptor: descriptor.clone(),
        schemas,
        db: db.clone(),
        compiled,
        commit_group,
        compactor: compactor.clone(),
        execution_requires_recovery: tokio::sync::Mutex::new(recovery_on_open),
        owned_epoch,
        recovery_on_open,
    });

    deployments.write().insert(
        (descriptor.workload_id, descriptor.shard.shard_id),
        deployment,
    );

    let fence_waiters_task = fence_waiters.clone();
    let (progress_tx, progress_rx) = mpsc::channel(32);
    tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                WorkerMessage::FenceWrite { shard_id, .. } => {
                    if let Some(waiters) = fence_waiters_task.lock().remove(&shard_id) {
                        for tx in waiters {
                            let _ = tx.send(true);
                        }
                    }
                }
                WorkerMessage::ExecutionProgress { .. } | WorkerMessage::ExecutionFailed { .. } => {
                    let _ = progress_tx.send(msg).await;
                }
                _ => {}
            }
        }
    });

    (client, deployments, db, compactor, progress_rx)
}

#[cfg(test)]
mod data_plane_tests {
    #[test]
    fn local_mode_checks_control_peer() {
        let local = std::net::IpAddr::from([127, 0, 0, 1]);
        let remote = std::net::IpAddr::from([192, 0, 2, 1]);
        let error = super::validate_local_control_peer(true, false, remote).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "explicit local mode requires a loopback control address"
        );
        assert!(super::validate_local_control_peer(true, false, local).is_ok());
        assert!(super::validate_local_control_peer(true, true, remote).is_ok());
        assert!(super::validate_local_control_peer(false, false, remote).is_ok());
    }

    use super::*;
    use rockstream_types::ids::OperatorId;

    #[tokio::test]
    async fn handoff_flushes_and_closes_each_open_writer_once() {
        // The active-shard map keeps a clone of the deployment's writer.
        // An independent database can use the same path on another store.
        let first = ShardDb::builder("db", Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        let second = ShardDb::builder("db", Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap();
        let active = first.clone();
        assert!(first.same_open_instance(&active));
        assert!(!first.same_open_instance(&second));
        let databases = unique_shard_databases(vec![first, active, second]);
        assert_eq!(databases.len(), 2);
        for db in &databases {
            db.flush().await.unwrap();
        }
        for db in databases {
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn direct_grant_installation_checks_durable_pending_after_draining_execution() {
        use rockstream_types::direct_epoch::{
            DirectEpochGrant, SourceEpochMetadata, DIRECT_EPOCH_GRANT_TTL_MS,
        };
        let dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, _) =
            setup_test_deployment(dir.path(), EpochCompactionConfig::default()).await;
        let deployment = deployments.read()[&(WorkloadId(1), ShardId(100))].clone();
        let mut grant = DirectEpochGrant {
            grant_id: uuid::Uuid::from_u128(11),
            metadata: SourceEpochMetadata {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                workload_id: WorkloadId(1),
                source: "items".into(),
                request_id: "replacement".into(),
                epoch: 1,
                input_digest: [3; 32],
            },
            expires_at_ms: crate::direct_grants::now_ms() + DIRECT_EPOCH_GRANT_TTL_MS,
            members: std::collections::BTreeMap::from([(ShardId(100), LeaseToken(777))]),
        };
        let pending = crate::skew_routing::RoutingEpochRecord {
            version: crate::skew_routing::ROUTING_VERSION,
            workload_id: WorkloadId(1),
            source: "items".into(),
            request_id: "pending".into(),
            epoch: 1,
            input_digest: [9; 32],
            generation: 1,
            shard_ids: vec![ShardId(100)],
            plan: serde_json::json!({"kind":"passthrough"}),
            next_state: serde_json::Value::Null,
            point_mutations: std::collections::BTreeMap::new(),
        };
        let execution = deployment.execution_requires_recovery.lock().await;
        let mut installing = Box::pin(install_direct_epoch_grant(
            &client,
            &deployments,
            grant.clone(),
        ));
        assert!(futures::poll!(installing.as_mut()).is_pending());
        crate::skew_routing::prepare_record(&db, pending.clone())
            .await
            .unwrap();
        drop(execution);
        assert_eq!(
            installing.await,
            Err("RS-3602: another routing epoch requires reconciliation".into())
        );
        grant.metadata.request_id = pending.request_id.clone();
        // Matching labels alone cannot replay a different original input.
        assert_eq!(
            install_direct_epoch_grant(&client, &deployments, grant.clone()).await,
            Err("RS-3602: another routing epoch requires reconciliation".into())
        );
        grant.metadata.input_digest = pending.input_digest;
        assert_eq!(
            install_direct_epoch_grant(&client, &deployments, grant.clone()).await,
            Ok(())
        );
        assert_eq!(
            crate::skew_routing::load_snapshot(&db, WorkloadId(1))
                .await
                .unwrap(),
            crate::skew_routing::RoutingSnapshot {
                pending: Some(pending),
                ..Default::default()
            }
        );
        *deployment.execution_requires_recovery.lock().await = true;
        assert_eq!(
            install_direct_epoch_grant(&client, &deployments, grant.clone()).await,
            Err("RS-3602: direct epoch deployment requires recovery".into())
        );
        *deployment.execution_requires_recovery.lock().await = false;
        let mut owned = rockstream_types::data_plane::OwnedEpochIntent {
            attempt_id: uuid::Uuid::from_u128(17),
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "other-owned-request".into(),
            workload_id: WorkloadId(1),
            epoch: 1,
            source: "items".into(),
            anchor_shard: ShardId(100),
            target_shards: vec![ShardId(100)],
        };
        deployment.owned_epoch.prepare(&owned).await.unwrap();
        assert_eq!(
            install_direct_epoch_grant(&client, &deployments, grant.clone()).await,
            Err("RS-3602: another owned source epoch requires reconciliation".into())
        );
        deployment.owned_epoch.finish(&owned).await.unwrap();
        owned.request_id = grant.metadata.request_id.clone();
        deployment.owned_epoch.prepare(&owned).await.unwrap();
        assert_eq!(
            install_direct_epoch_grant(&client, &deployments, grant).await,
            Ok(())
        );
        assert_eq!(deployment.owned_epoch.load().await.unwrap(), Some(owned));
    }

    #[tokio::test]
    async fn workload_read_waits_for_execution_gate_and_rejects_recovery_state() {
        let dir = tempfile::tempdir().unwrap();
        let (client, deployments, _, _, _) =
            setup_test_deployment(dir.path(), EpochCompactionConfig::default()).await;
        let deployment = deployments
            .read()
            .get(&(WorkloadId(1), ShardId(100)))
            .unwrap()
            .clone();
        let mut write_guard = deployment.execution_requires_recovery.lock().await;
        let shard_ids = [ShardId(100)];
        let read = read_workload_shards(
            &deployments,
            &client.active_shards,
            WorkloadId(1),
            &shard_ids,
        );
        tokio::pin!(read);
        assert!(matches!(
            futures::poll!(read.as_mut()),
            std::task::Poll::Pending
        ));
        *write_guard = true;
        drop(write_guard);
        let error = read.await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "workload shard requires recovery");
        *deployment.execution_requires_recovery.lock().await = false;
        assert_eq!(
            read_workload_shards(
                &deployments,
                &client.active_shards,
                WorkloadId(1),
                &[ShardId(100)]
            )
            .await
            .unwrap(),
            vec![ShardOutput {
                shard_id: ShardId(100),
                lease_token: Some(LeaseToken(777)),
                committed_epoch: Some(0),
                routing_epoch: Some(0),
                deltas: Vec::new(),
                rows: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn workload_read_pairs_rows_with_durable_epoch_and_pending_journal_guard() {
        let dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, _) =
            setup_test_deployment(dir.path(), EpochCompactionConfig::default()).await;
        let frame = RuntimeExchangeMessage {
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "read-epoch-1".into(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            epoch: 1,
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            source: "items".into(),
            join_bootstrap: None,
            rows: vec![RuntimeRow {
                values_tsv: "1\tfirst".into(),
                weight: 1,
            }],
        };
        execute_frame(&client, &deployments, frame).await.unwrap();
        let before = read_workload_shards(
            &deployments,
            &client.active_shards,
            WorkloadId(1),
            &[ShardId(100)],
        )
        .await
        .unwrap();
        assert_eq!(
            before,
            vec![ShardOutput {
                shard_id: ShardId(100),
                lease_token: Some(LeaseToken(777)),
                committed_epoch: Some(1),
                routing_epoch: Some(1),
                deltas: Vec::new(),
                rows: vec![RuntimeRow {
                    values_tsv: "1\tfirst".into(),
                    weight: 1
                }]
            }]
        );
        let record = crate::skew_routing::RoutingEpochRecord {
            version: 1,
            workload_id: WorkloadId(1),
            source: "items".into(),
            request_id: "read-epoch-2".into(),
            epoch: 2,
            input_digest: [0; 32],
            generation: 1,
            shard_ids: vec![ShardId(100)],
            plan: serde_json::json!({"kind":"passthrough"}),
            next_state: serde_json::Value::Null,
            point_mutations: Default::default(),
        };
        crate::skew_routing::prepare_record(&db, record)
            .await
            .unwrap();
        let error = read_workload_shards(
            &deployments,
            &client.active_shards,
            WorkloadId(1),
            &[ShardId(100)],
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            error.to_string(),
            "skew routing epoch is not fully committed"
        );
    }

    #[tokio::test]
    async fn idle_anchor_requires_empty_durable_owned_and_routing_journals() {
        let temp = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, _) =
            setup_test_deployment(temp.path(), EpochCompactionConfig::default()).await;
        let deployment = deployments.read()[&(WorkloadId(1), ShardId(100))].clone();
        let anchor = deployment.descriptor.shard.clone();
        assert_eq!(
            verify_workload_idle(
                &deployments,
                &client.direct_grants,
                &client.active_shards,
                WorkloadId(1),
                &anchor
            )
            .await,
            Ok(())
        );
        let intent = rockstream_types::data_plane::OwnedEpochIntent {
            attempt_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            version: rockstream_types::data_plane::DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "idle-check".into(),
            workload_id: WorkloadId(1),
            epoch: 1,
            source: "items".into(),
            anchor_shard: ShardId(100),
            target_shards: vec![ShardId(100)],
        };
        deployment.owned_epoch.prepare(&intent).await.unwrap();
        assert_eq!(
            verify_workload_idle(
                &deployments,
                &client.direct_grants,
                &client.active_shards,
                WorkloadId(1),
                &anchor
            )
            .await,
            Err("workload has an unfinished owned epoch".to_owned())
        );
        deployment.owned_epoch.finish(&intent).await.unwrap();
        let record = crate::skew_routing::RoutingEpochRecord {
            version: crate::skew_routing::ROUTING_VERSION,
            workload_id: WorkloadId(1),
            source: "items".into(),
            request_id: "idle-check".into(),
            epoch: 1,
            input_digest: [9; 32],
            generation: 1,
            shard_ids: vec![ShardId(100)],
            plan: serde_json::Value::Null,
            next_state: serde_json::Value::Null,
            point_mutations: std::collections::BTreeMap::new(),
        };
        crate::skew_routing::prepare_record(&db, record.clone())
            .await
            .unwrap();
        assert_eq!(
            verify_workload_idle(
                &deployments,
                &client.direct_grants,
                &client.active_shards,
                WorkloadId(1),
                &anchor
            )
            .await,
            Err("workload has an unfinished routing epoch".to_owned())
        );
        crate::skew_routing::finish_record(&db, &record)
            .await
            .unwrap();
        assert_eq!(
            verify_workload_idle(
                &deployments,
                &client.direct_grants,
                &client.active_shards,
                WorkloadId(1),
                &anchor
            )
            .await,
            Ok(())
        );
        let mut stale = anchor;
        stale.lease_token = LeaseToken(776);
        assert_eq!(
            verify_workload_idle(
                &deployments,
                &client.direct_grants,
                &client.active_shards,
                WorkloadId(1),
                &stale
            )
            .await,
            Err("workload anchor lease changed".to_owned())
        );
    }

    #[tokio::test]
    async fn worker_tls_reload_rejects_missing_credentials_and_disabled_mtls() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, ..) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        let pki = rockstream_test_support::pki::TestPki::generate();

        assert_eq!(
            client
                .reload_tls(rockstream_types::identity::InternalTlsConfig::default())
                .unwrap_err(),
            "RS-2410: worker shuffle TLS certificate and private key are required"
        );
        let mut without_mtls = pki.worker_tls_config(1);
        without_mtls.client_auth_required = false;
        assert_eq!(
            client.reload_tls(without_mtls).unwrap_err(),
            "RS-2410: worker shuffle requires mutual TLS"
        );
    }

    #[tokio::test]
    async fn join_bootstrap_commits_both_inputs_once_and_rejects_changed_replay() {
        use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, JoinSemantics, PlanNode};
        use rockstream_types::data_plane::JoinBootstrap;
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, mut progress) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        {
            let mut deployments = deployments.write();
            let deployment =
                Arc::get_mut(deployments.get_mut(&(WorkloadId(1), ShardId(100))).unwrap()).unwrap();
            let mut left = deployment.descriptor.schemas[0].clone();
            left.relation = "left".into();
            left.columns[1].data_type = "i64".into();
            let mut right = left.clone();
            right.relation = "right".into();
            deployment.descriptor.schemas = vec![left, right];
            deployment.descriptor.join_strategy =
                rockstream_types::config::JoinStrategy::Factorized;
            let plan = PlanNode::ViewSink {
                view_name: "joined".into(),
                pk: vec![0],
                child: Box::new(PlanNode::Aggregate {
                    input: Box::new(PlanNode::InnerJoin {
                        left: Box::new(PlanNode::Source {
                            name: "left".into(),
                        }),
                        right: Box::new(PlanNode::Source {
                            name: "right".into(),
                        }),
                        left_keys: vec![0],
                        right_keys: vec![0],
                        left_arr_id: OperatorId(40),
                        right_arr_id: OperatorId(41),
                        semantics: JoinSemantics::default(),
                    }),
                    group_by: vec![Expr::Column(0)],
                    aggregates: vec![AggregateExpr {
                        func: AggregateFunc::Sum,
                        input: Expr::Column(1),
                        distinct: false,
                    }],
                }),
            };
            deployment.descriptor.plan_json = serde_json::to_string(&plan).unwrap();
            deployment.schemas = deployment_schema(&deployment.descriptor).unwrap();
            deployment.compiled = rockstream_ops::compile_plan_with_sink_id_and_strategy(
                &plan,
                db.clone(),
                &deployment.schemas,
                OperatorId(10),
                deployment.descriptor.join_strategy,
            )
            .unwrap();
            assert_eq!(deployment.compiled.strategy(), "factorized");
        }
        let frame = RuntimeExchangeMessage {
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "promote-1".into(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "left".into(),
            rows: vec![RuntimeRow {
                values_tsv: "1\t7".into(),
                weight: 3,
            }],
            join_bootstrap: Some(JoinBootstrap {
                source: "right".into(),
                rows: vec![RuntimeRow {
                    values_tsv: "1\t10".into(),
                    weight: 2,
                }],
                final_rows: Vec::new(),
            }),
        };
        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();
        progress.recv().await.unwrap();
        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();
        progress.recv().await.unwrap();
        let mut wrong = frame.clone();
        wrong.join_bootstrap.as_mut().unwrap().rows[0].weight = 3;
        assert_eq!(
            execute_frame(&client, &deployments, wrong)
                .await
                .unwrap_err()
                .to_string(),
            "committed execution does not match replay"
        );
        let read = read_workload_shards(
            &deployments,
            &client.active_shards,
            WorkloadId(1),
            &[ShardId(100)],
        )
        .await
        .unwrap();
        assert_eq!(
            read[0].rows,
            vec![RuntimeRow {
                values_tsv: "1\t42".into(),
                weight: 1
            }]
        );
        let next = RuntimeExchangeMessage {
            epoch: 2,
            request_id: "promote-2".into(),
            join_bootstrap: None,
            rows: vec![RuntimeRow {
                values_tsv: "1\t7".into(),
                weight: -1,
            }],
            ..frame
        };
        execute_frame(&client, &deployments, next).await.unwrap();
        progress.recv().await.unwrap();
        let read = read_workload_shards(
            &deployments,
            &client.active_shards,
            WorkloadId(1),
            &[ShardId(100)],
        )
        .await
        .unwrap();
        assert_eq!(
            read[0].rows,
            vec![RuntimeRow {
                values_tsv: "1\t28".into(),
                weight: 1
            }]
        );
        let deployment = deployments
            .read()
            .get(&(WorkloadId(1), ShardId(100)))
            .cloned()
            .unwrap();
        let mut pending = WriteBatch::new();
        deployment
            .compiled
            .join
            .as_ref()
            .unwrap()
            .pipeline
            .append_state(&db, &mut pending)
            .await
            .unwrap();
        assert!(
            pending.is_empty(),
            "successful commit must acknowledge all factorized dirty rows"
        );
        db.as_ref().clone().close().await.unwrap();
    }

    #[test]
    fn linux_memory_headroom_uses_available_memory() {
        let meminfo = "MemTotal: 1000 kB\nMemFree: 100 kB\nMemAvailable: 250 kB\n";

        assert_eq!(
            parse_linux_memory_headroom(meminfo).unwrap(),
            CapacityHeadroom::new(0.25)
        );
    }

    #[test]
    fn runtime_rows_convert_exactly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("enabled", DataType::Boolean, false),
        ]));
        let rows = vec![
            RuntimeRow {
                values_tsv: "7\tstone\tTRUE".into(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "9\tstream\tFaLsE".into(),
                weight: -2,
            },
        ];

        assert_eq!(
            zset_to_rows(&rows_to_zset(&rows, schema).unwrap()).unwrap(),
            vec![
                RuntimeRow {
                    values_tsv: "7\tstone\ttrue".into(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "9\tstream\tfalse".into(),
                    weight: -2,
                },
            ]
        );
    }

    #[tokio::test]
    async fn rejected_worker_execution_reports_failure_without_advancing_frontier() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "rejected-request".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "unknown".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "10\talice".to_owned(),
                weight: 1,
            }],
        };
        let expected = failure_identity(&frame);
        let interrupted = cancellation_reporter(client.msg_tx.clone())(&frame);
        assert_eq!(
            execute_frame_and_report(&client, &deployments, frame).await,
            Err("unknown source relation".to_owned())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => {
                assert_eq!(
                    (output, message),
                    (expected.clone(), "unknown source relation".to_owned())
                );
            }
            other => panic!("unexpected worker response: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap(),
            None
        );
        interrupted();
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => assert_eq!(
                (output, message),
                (
                    expected,
                    "worker execution interrupted before completion".to_owned()
                )
            ),
            other => panic!("unexpected worker response: {other:?}"),
        }
    }

    #[test]
    fn owned_shuffle_shard_uses_checked_workload_relative_index() {
        let workload = WorkloadId((1_u64 << 60) + 91);
        let base = workload.0.wrapping_mul(16);
        assert_eq!(
            owned_shuffle_shard(workload, ShardId(base.wrapping_add(42))),
            Ok(42)
        );
        assert_eq!(
            owned_shuffle_shard(workload, ShardId(base.wrapping_add(1_u64 << 32))),
            Err("owned shuffle shard index exceeds the supported range".to_owned())
        );
    }

    #[tokio::test]
    async fn owned_shuffle_runs_the_worker_and_waits_for_commit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        {
            let mut deployments = deployments.write();
            let deployment =
                Arc::get_mut(deployments.get_mut(&(WorkloadId(1), ShardId(100))).unwrap()).unwrap();
            deployment.descriptor.schemas[0].columns[1].data_type = "i64".to_owned();
            let schemas = deployment_schema(&deployment.descriptor).unwrap();
            let plan = serde_json::from_str(&deployment.descriptor.plan_json).unwrap();
            deployment.compiled = rockstream_ops::compile_plan_with_sink_id_and_strategy(
                &plan,
                db.clone(),
                &schemas,
                deployment.descriptor.sink_operator_id,
                deployment.descriptor.join_strategy,
            )
            .unwrap();
            deployment.schemas = schemas;
        }
        let owner = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "owned-shuffle".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_owned(),
            rows: Vec::new(),
        };
        let rows = vec![
            RuntimeRow {
                values_tsv: "1\t10".to_owned(),
                weight: 1,
            },
            RuntimeRow {
                values_tsv: "2\t20".to_owned(),
                weight: 1,
            },
        ];
        let (placement_index, schema) = {
            let deployed = deployments.read();
            let deployment = &deployed[&(owner.workload_id, owner.shard_id)];
            (
                deployment.descriptor.shard_index,
                deployment.schemas["items"].clone(),
            )
        };
        let legacy_index = owned_shuffle_shard(owner.workload_id, owner.shard_id).unwrap();
        let input = rows_to_zset(&rows, schema.clone()).unwrap();
        let frame = crate::exchange::proto::ShuffleFrame {
            exchange_id: 1,
            src_shard: placement_index,
            // Explicit compatibility frames use the legacy alias; ordinary
            // dispatch below must use the descriptor's actual placement.
            target_shard: legacy_index,
            epoch: 1,
            seq: 0,
            payload: crate::exchange::serialization::serialize_zset(&input)
                .unwrap()
                .into(),
            row_count: 2,
        };
        let actors = ShardActorRegistry::new();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let actor_started = started.clone();
        let actor_release = release.clone();
        let executor_client = client.clone();
        let executor_deployments = deployments.clone();
        let execute: OwnedFrameExecutor = Arc::new(move |frame| {
            let client = executor_client.clone();
            let deployments = executor_deployments.clone();
            let started = actor_started.clone();
            let release = actor_release.clone();
            let first = first.clone();
            Box::pin(async move {
                if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    started.notify_one();
                    release.notified().await;
                }
                execute_frame_and_report(&client, &deployments, frame).await
            })
        });
        let actors_for_test = actors.clone();
        let sender_deployments = deployments.clone();
        let handle = owned_shuffle_handler(actors, deployments, client);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry = crate::exchange::service::ExchangeRegistry::new();
        let (inlet_tx, inlet_rx) = mpsc::channel(1);
        registry.register(1, placement_index, inlet_tx, schema.clone());
        drop(inlet_rx);
        let service = crate::exchange::service::ExchangeService::new(registry.clone())
            .with_owned_handler(handle)
            .with_insecure_internal_dev();
        let server_task = service.start_listener(listener).await.unwrap();
        let info = |id, address: String, host: &str| {
            let registration = WorkerRegistration::new(
                WorkerId(id),
                NodeRole::Worker,
                address,
                CapacityHeadroom::FULL,
            )
            .with_location(WorkerLocation::new(host, "same-zone"));
            rockstream_types::topology::WorkerInfo::from_registration(&registration)
        };
        let peers = Arc::new(RwLock::new(HashMap::from([(
            WorkerId(42),
            addr.to_string(),
        )])));
        let pool = crate::exchange::pool::ShuffleClientPool::new(peers);
        pool.set_local_worker_info(info(811, "127.0.0.1:9811".to_owned(), "host-a"));
        pool.upsert_peer_info(info(42, addr.to_string(), "host-b"));
        let flow = crate::exchange::flow_control::FlowController::new();
        let mux = crate::exchange::multiplexer::WorkerStreamMultiplexer::new(pool, flow.clone());
        let context = bytes::Bytes::from(serde_json::to_vec(&owner).unwrap());
        let wrong = crate::exchange::proto::ShuffleFrame {
            epoch: 2,
            ..frame.clone()
        };
        assert_eq!(
            mux.send_owned_frame(WorkerId(42), wrong, context.clone())
                .await,
            Err("owned shuffle identity does not match frame".to_owned())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => assert_eq!(
                (output, message),
                (
                    failure_identity(&owner),
                    "owned shuffle identity does not match frame".to_owned()
                )
            ),
            other => panic!("unexpected worker response: {other:?}"),
        }
        let unavailable =
            crate::shard_actor::ShardActorError::UnknownShard(owner.shard_id).to_string();
        assert_eq!(
            mux.send_owned_frame(WorkerId(42), frame.clone(), context.clone())
                .await,
            Err(unavailable.clone())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => {
                assert_eq!((output, message), (failure_identity(&owner), unavailable))
            }
            other => panic!("unexpected unavailable-target response: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap(),
            None
        );
        let (inlet_tx, mut reopened_inlet) = mpsc::channel(1);
        registry.register(1, placement_index, inlet_tx, schema);
        actors_for_test.register_owned(owner.shard_id, owner.lease_token, execute);
        let sending = {
            let mux = mux.clone();
            let deployments = sender_deployments.clone();
            let source = RuntimeExchangeMessage {
                rows: rows.clone(),
                ..owner.clone()
            };
            tokio::spawn(async move {
                dispatch_owned_shuffle(&mux, &deployments, WorkerId(42), source).await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        assert!(!sending.is_finished());
        assert_eq!(flow.rows_in_flight(1, placement_index, placement_index), 2);
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap(),
            None
        );
        release.notify_one();
        assert_eq!(sending.await.unwrap(), Ok(()));
        assert!(matches!(
            reopened_inlet.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!((output.rows, input_rows, output_rows), (rows, 2, 2)),
            other => panic!("unexpected worker response: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|value| value.to_vec()),
            Some(1_u64.to_be_bytes().to_vec())
        );
        use rockstream_ops::sink::{read_view_output, ColumnValue};
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        let expected = vec![
            (1, vec![ColumnValue::Int64(1), ColumnValue::Int64(10)], 1),
            (1, vec![ColumnValue::Int64(2), ColumnValue::Int64(20)], 1),
        ];
        assert_eq!(
            stored
                .into_iter()
                .map(|(epoch, _, row, weight)| (epoch, row, weight))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            mux.send_owned_frame(WorkerId(42), frame.clone(), context)
                .await,
            Ok(())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!(
                (output, input_rows, output_rows),
                (failure_identity(&owner), 0, 0)
            ),
            other => panic!("unexpected replay response: {other:?}"),
        }
        actors_for_test.revoke(owner.shard_id);
        let next_owner = RuntimeExchangeMessage {
            epoch: 2,
            ..owner.clone()
        };
        let next_frame = crate::exchange::proto::ShuffleFrame { epoch: 2, ..frame };
        let context = bytes::Bytes::from(serde_json::to_vec(&next_owner).unwrap());
        let unavailable =
            crate::shard_actor::ShardActorError::UnknownShard(owner.shard_id).to_string();
        assert_eq!(
            mux.send_owned_frame(WorkerId(42), next_frame, context)
                .await,
            Err(unavailable.clone())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => assert_eq!(
                (output, message),
                (failure_identity(&next_owner), unavailable)
            ),
            other => panic!("unexpected unavailable-target response: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|value| value.to_vec()),
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert_eq!(
            read_view_output(&db, OperatorId(10), 2)
                .await
                .unwrap()
                .into_iter()
                .map(|(epoch, _, row, weight)| (epoch, row, weight))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            (
                flow.rows_in_flight(1, placement_index, placement_index),
                flow.batches_in_flight(1, placement_index, placement_index),
                flow.rows_in_flight(1, placement_index, legacy_index),
                flow.batches_in_flight(1, placement_index, legacy_index)
            ),
            (0, 0, 0, 0)
        );
        mux.shutdown().await;
        service.shutdown().await;
        server_task.abort();
    }

    #[tokio::test]
    async fn test_zero_weight_omission_prior_to_pipeline_and_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Frame with offsetting updates (+1 and -1) on key "10\talice"
        // and a non-zero row (+1) on key "20\tbob"
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "req-1".to_string(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_string(),
            rows: vec![
                RuntimeRow {
                    values_tsv: "10\talice".to_string(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "10\talice".to_string(),
                    weight: -1,
                },
                RuntimeRow {
                    values_tsv: "20\tbob".to_string(),
                    weight: 1,
                },
            ],
        };

        let executor_client = client.clone();
        let executor_deployments = deployments.clone();
        let execute: OwnedFrameExecutor = Arc::new(move |frame| {
            let client = executor_client.clone();
            let deployments = executor_deployments.clone();
            Box::pin(async move { execute_frame_and_report(&client, &deployments, frame).await })
        });
        let actors = ShardActorRegistry::new();
        actors.register_owned(frame.shard_id, frame.lease_token, execute);
        let receipt = actors.enqueue_with_completion(frame).unwrap();
        assert_eq!(receipt.await.unwrap(), Ok(()));

        // 1. Check emitted progress delta: only key 20 survived
        let msg = progress_rx.recv().await.expect("progress message expected");
        match msg {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
                assert_eq!(input_rows, 3);
                assert_eq!(output_rows, 1);
                assert_eq!(
                    output.rows,
                    vec![RuntimeRow {
                        values_tsv: "20\tbob".to_string(),
                        weight: 1,
                    }]
                );
            }
            other => panic!("unexpected worker message: {other:?}"),
        }

        // 2. Check compaction metrics: 1 zero-weight cancelled update
        let metrics = compactor.metrics_snapshot();
        // 3 input updates + 1 output update = 4 total updates evaluated
        assert_eq!(metrics.input_updates, 4);
        assert_eq!(metrics.cancelled_zero_weight_updates, 1);
        assert_eq!(metrics.collapsed_updates, 1);
        assert_eq!(metrics.emitted_updates, 2);

        // 3. Check storage: only key 20 was written, key 10 was omitted completely
        use rockstream_ops::sink::{read_view_output, ColumnValue};
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].0, 1); // epoch 1
        assert_eq!(
            stored[0].2,
            vec![ColumnValue::Int64(20), ColumnValue::Utf8("bob".to_string()),]
        );
        assert_eq!(stored[0].3, 1); // weight 1
    }

    #[tokio::test]
    async fn net_zero_input_commits_frontier_without_output_rows() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Frame where all rows offset to net-zero (+1 and -1 on same key)
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "req-net-zero".to_string(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_string(),
            rows: vec![
                RuntimeRow {
                    values_tsv: "99\tghost".to_string(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "99\tghost".to_string(),
                    weight: -1,
                },
            ],
        };

        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();

        // 1. Check emitted progress delta: empty rows, output_rows == 0
        let msg = progress_rx.recv().await.expect("progress message expected");
        match msg {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
                assert_eq!(input_rows, 2);
                assert_eq!(output_rows, 0);
                assert!(output.rows.is_empty());
            }
            other => panic!("unexpected worker message: {other:?}"),
        }

        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!(
                (output, input_rows, output_rows),
                (failure_identity(&frame), 0, 0)
            ),
            other => panic!("unexpected zero-input replay: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|value| value.to_vec()),
            Some(1_u64.to_be_bytes().to_vec())
        );

        // 2. Check compaction metrics: cancelled = 1, emitted = 0
        let metrics = compactor.metrics_snapshot();
        assert_eq!(metrics.input_updates, 2);
        assert_eq!(metrics.cancelled_zero_weight_updates, 1);
        assert_eq!(metrics.emitted_updates, 0);

        // 3. The frontier advances without writing view rows.
        use rockstream_ops::sink::read_view_output;
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        assert!(stored.is_empty(), "net-zero input must not write view rows");
    }

    #[tokio::test]
    async fn failed_empty_epoch_reports_failure_without_progress() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        db.set_fail_writes(true);
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "empty-failed".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_owned(),
            rows: Vec::new(),
        };
        let expected = failure_identity(&frame);
        assert_eq!(
            execute_frame_and_report(&client, &deployments, frame).await,
            Err("[RS-0001] Internal error: unsupported operation: injected storage write failure; next_steps: report this issue".to_owned())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionFailed { output, message } => {
                assert_eq!(
                    (output, message),
                    (
                        expected,
                        "[RS-0001] Internal error: unsupported operation: injected storage write failure; next_steps: report this issue".to_owned()
                    )
                );
            }
            other => panic!("unexpected worker response: {other:?}"),
        }
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            progress_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn reopened_worker_deduplicates_only_matching_committed_input() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, _, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "replay".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "10\talice".to_owned(),
                weight: 1,
            }],
        };
        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
                assert_eq!(
                    (output.rows, input_rows, output_rows),
                    (frame.rows.clone(), 1, 1)
                );
            }
            other => panic!("unexpected worker response: {other:?}"),
        }
        (*db).clone().close().await.unwrap();
        drop((client, deployments, db, progress_rx));

        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        assert_eq!(
            deployments.read()[&(WorkloadId(1), ShardId(100))]
                .commit_group
                .last_committed(),
            1
        );
        assert_eq!(
            execute_frame(&client, &deployments, frame.clone())
                .await
                .map_err(|error| error.to_string()),
            Ok(())
        );
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!(
                (output, input_rows, output_rows),
                (failure_identity(&frame), 0, 0)
            ),
            other => panic!("unexpected replay response: {other:?}"),
        }
        let conflicting = RuntimeExchangeMessage {
            rows: vec![RuntimeRow {
                values_tsv: "11\tbob".to_owned(),
                weight: 1,
            }],
            ..frame.clone()
        };
        assert_eq!(
            execute_frame(&client, &deployments, conflicting)
                .await
                .unwrap_err()
                .to_string(),
            "committed execution does not match replay"
        );
        let (record, _) = execution_record(&frame).unwrap();
        db.delete(&record).await.unwrap();
        assert_eq!(
            execute_frame(&client, &deployments, frame)
                .await
                .unwrap_err()
                .to_string(),
            "committed execution record is unavailable"
        );
        assert_eq!(compactor.metrics_snapshot().input_updates, 0);
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|v| v.to_vec()),
            Some(1_u64.to_be_bytes().to_vec())
        );
        assert!(matches!(
            progress_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn concurrent_duplicate_input_is_processed_once() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, _, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "concurrent-replay".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "10\talice".to_owned(),
                weight: 1,
            }],
        };
        let (first, second) = tokio::join!(
            execute_frame(&client, &deployments, frame.clone()),
            execute_frame(&client, &deployments, frame.clone())
        );
        let mut outcomes = [
            first.map_err(|e| e.to_string()),
            second.map_err(|e| e.to_string()),
        ];
        outcomes.sort();
        assert_eq!(outcomes, [Ok(()), Ok(())]);
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!(
                (output.rows, input_rows, output_rows),
                (frame.rows.clone(), 1, 1)
            ),
            other => panic!("unexpected worker response: {other:?}"),
        }
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => assert_eq!(
                (output, input_rows, output_rows),
                (failure_identity(&frame), 0, 0)
            ),
            other => panic!("unexpected replay response: {other:?}"),
        }
        assert_eq!(compactor.metrics_snapshot().input_updates, 2);
        assert!(matches!(
            progress_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn failed_commit_refuses_later_input_before_processing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "failed-commit".to_owned(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "10\talice".to_owned(),
                weight: 1,
            }],
        };
        db.set_fail_writes(true);
        assert_eq!(
            execute_frame(&client, &deployments, frame.clone())
                .await
                .unwrap_err()
                .to_string(),
            "[RS-0001] Internal error: unsupported operation: injected storage write failure; next_steps: report this issue"
        );
        db.set_fail_writes(false);
        let retry = RuntimeExchangeMessage {
            epoch: 2,
            ..frame.clone()
        };
        assert_eq!(
            execute_frame(&client, &deployments, retry)
                .await
                .unwrap_err()
                .to_string(),
            "worker deployment requires recovery after failed execution"
        );
        assert_eq!(compactor.metrics_snapshot().input_updates, 2);
        assert_eq!(
            db.get(&rockstream_storage::keys::ShardKeyEncoder::frontier_key())
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            progress_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        (*db).clone().close().await.unwrap();
        drop((client, deployments, db, compactor, progress_rx));

        let (client, deployments, db, _, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;
        execute_frame(&client, &deployments, frame.clone())
            .await
            .unwrap();
        match progress_rx.recv().await.unwrap() {
            WorkerMessage::ExecutionProgress {
                output,
                input_rows,
                output_rows,
            } => {
                assert_eq!((output.rows, input_rows, output_rows), (frame.rows, 1, 1));
            }
            other => panic!("unexpected worker response: {other:?}"),
        }
        use rockstream_ops::sink::{read_view_output, ColumnValue};
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        assert_eq!(
            stored
                .into_iter()
                .map(|(epoch, _, row, weight)| (epoch, row, weight))
                .collect::<Vec<_>>(),
            vec![(
                1,
                vec![
                    ColumnValue::Int64(10),
                    ColumnValue::Utf8("alice".to_owned())
                ],
                1
            )]
        );
    }

    #[tokio::test]
    async fn test_collapsing_multiple_updates_into_net_weight() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Multiple updates on key 42: +3 and -1 -> net weight +2
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "req-collapse".to_string(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(100),
            operator_id: OperatorId(10),
            lease_token: LeaseToken(777),
            epoch: 1,
            source: "items".to_string(),
            rows: vec![
                RuntimeRow {
                    values_tsv: "42\tcollapsed".to_string(),
                    weight: 3,
                },
                RuntimeRow {
                    values_tsv: "42\tcollapsed".to_string(),
                    weight: -1,
                },
            ],
        };

        execute_frame(&client, &deployments, frame).await.unwrap();

        let msg = progress_rx.recv().await.expect("progress message expected");
        match msg {
            WorkerMessage::ExecutionProgress {
                output,
                output_rows,
                ..
            } => {
                assert_eq!(output_rows, 1);
                assert_eq!(
                    output.rows,
                    vec![RuntimeRow {
                        values_tsv: "42\tcollapsed".to_string(),
                        weight: 2,
                    }]
                );
            }
            other => panic!("unexpected worker message: {other:?}"),
        }

        assert_eq!(compactor.metrics().collapsed_updates(), 1);
        assert_eq!(compactor.metrics().cancelled_zero_weight_updates(), 0);

        use rockstream_ops::sink::{read_view_output, ColumnValue};
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored[0].2,
            vec![
                ColumnValue::Int64(42),
                ColumnValue::Utf8("collapsed".to_string()),
            ]
        );
        assert_eq!(stored[0].3, 2);
    }

    #[tokio::test]
    async fn test_client_compaction_config_and_handle_accessors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let custom_window = Duration::from_millis(250);
        let custom_config = EpochCompactionConfig::new(custom_window).unwrap();
        let (client, _deployments, _db, compactor, _rx) =
            setup_test_deployment(temp_dir.path(), custom_config.clone()).await;

        assert_eq!(client.compaction_config().window_duration, custom_window);
        assert_eq!(compactor.config().window_duration, custom_window);

        let new_config = EpochCompactionConfig::new(Duration::from_millis(150)).unwrap();
        client.set_compaction_config(new_config.clone()).unwrap();
        assert_eq!(
            client.compaction_config().window_duration,
            Duration::from_millis(150)
        );

        // Invalid updates must not mutate the accepted worker configuration.
        let invalid = new_config
            .clone()
            .with_bounds(Duration::from_millis(500), Duration::from_millis(100));
        assert!(client.set_compaction_config(invalid).is_err());
        assert_eq!(
            client.compaction_config().window_duration,
            Duration::from_millis(150)
        );

        let retrieved_compactor = client
            .compactor(WorkloadId(1), ShardId(100))
            .expect("deployment exists");
        assert_eq!(retrieved_compactor.config().window_duration, custom_window);
    }
}

/// Tracks a shard lease and its local active database instance.
pub struct ShardState {
    pub lease: ShardLease,
    pub db: Option<ShardDb>,
}

/// A client handle to interact with the running worker daemon.
#[derive(Clone)]
pub struct WorkerClientHandle {
    pub(crate) direct_grants: crate::direct_grants::DirectGrantRegistry,
    shard_loads:
        Arc<parking_lot::Mutex<HashMap<ShardId, rockstream_types::topology::ShardLoadSample>>>,
    worker_id: Arc<RwLock<Option<WorkerId>>>,
    active_shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    topology_workers: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
    shard_leases: Arc<RwLock<HashMap<ShardId, ShardLease>>>,
    pub(crate) msg_tx: mpsc::Sender<WorkerMessage>,
    #[allow(dead_code)]
    security_audit_waiters: crate::exchange::service::SecurityAuditWaiters,
    fence_waiters:
        Arc<parking_lot::Mutex<HashMap<ShardId, Vec<tokio::sync::oneshot::Sender<bool>>>>>,
    frontier_report_waiter: FrontierReportWaiter,
    secret_manager: Arc<WorkerSecretManager>,
    storage_context: Arc<rockstream_storage::storage_context::WorkerStorageContext>,
    compaction_config: Arc<RwLock<EpochCompactionConfig>>,
    deployments: WorkerDeployments,
    tls_reloader: Option<Arc<rockstream_types::tls::TlsCertificateReloader>>,
    exchange_registry: crate::exchange::service::ExchangeRegistry,
}

impl WorkerClientHandle {
    fn record_shard_load(&self, frame: &RuntimeExchangeMessage, process_nanos: u64) {
        let mut samples = self.shard_loads.lock();
        if samples.len() >= 4096 && !samples.contains_key(&frame.shard_id) {
            return;
        }
        samples.insert(
            frame.shard_id,
            rockstream_types::topology::ShardLoadSample {
                shard_id: frame.shard_id,
                state_bytes: 0,
                rows_per_epoch: frame.rows.len() as u64,
                cpu_nanos: process_nanos,
                bytes_per_epoch: frame
                    .rows
                    .iter()
                    .map(|row| row.values_tsv.len() as u64)
                    .sum(),
                state_writes_per_epoch: 0,
                key_loads: Vec::new(),
            },
        );
    }
    /// Reload the worker shuffle listener's mTLS credentials without closing active streams.
    pub fn reload_tls(
        &self,
        config: rockstream_types::identity::InternalTlsConfig,
    ) -> Result<(), String> {
        if !config.is_enabled() {
            return Err(
                "RS-2410: worker shuffle TLS certificate and private key are required".to_string(),
            );
        }
        if !config.client_auth_required {
            return Err("RS-2410: worker shuffle requires mutual TLS".to_string());
        }
        config.validate()?;
        self.tls_reloader
            .as_ref()
            .ok_or_else(|| "RS-2410: worker shuffle TLS is not enabled".to_string())?
            .reload(config)
    }

    /// Returns the worker-wide epoch compaction config.
    pub fn compaction_config(&self) -> EpochCompactionConfig {
        self.compaction_config.read().clone()
    }

    /// Set the worker-wide epoch compaction config.
    pub fn set_compaction_config(
        &self,
        config: EpochCompactionConfig,
    ) -> Result<(), crate::epoch_compaction::EpochCompactionError> {
        config.validate()?;
        *self.compaction_config.write() = config;
        Ok(())
    }

    /// Get active deployment for a workload and shard.
    pub fn get_deployment(
        &self,
        workload_id: WorkloadId,
        shard_id: ShardId,
    ) -> Option<Arc<WorkerDeployment>> {
        self.deployments
            .read()
            .get(&(workload_id, shard_id))
            .cloned()
    }

    /// Execute a runtime exchange message frame against deployed workloads.
    pub async fn execute_frame(&self, frame: RuntimeExchangeMessage) -> io::Result<()> {
        execute_frame(self, &self.deployments, frame).await
    }

    /// Get the compactor for a workload and shard deployment.
    pub fn compactor(
        &self,
        workload_id: WorkloadId,
        shard_id: ShardId,
    ) -> Option<Arc<EpochCompactor>> {
        self.get_deployment(workload_id, shard_id)
            .map(|d| d.compactor.clone())
    }

    /// Returns the worker storage context.
    pub fn storage_context(
        &self,
    ) -> Arc<rockstream_storage::storage_context::WorkerStorageContext> {
        self.storage_context.clone()
    }

    /// Returns the worker ID assigned by the control plane.
    pub fn worker_id(&self) -> Option<WorkerId> {
        *self.worker_id.read()
    }

    /// Check if we own a shard lease and get its database instance.
    pub fn get_shard_db(&self, shard_id: ShardId) -> Option<ShardDb> {
        self.active_shards
            .read()
            .get(&shard_id)
            .and_then(|state| state.db.clone())
    }

    /// Get active leases held by this worker.
    pub fn leases(&self) -> Vec<ShardLease> {
        self.active_shards
            .read()
            .values()
            .map(|s| s.lease.clone())
            .collect()
    }

    /// Latest topology snapshot advertised by the control plane.
    pub fn topology_snapshot(&self) -> Vec<WorkerInfo> {
        self.topology_workers.read().values().cloned().collect()
    }

    /// Shared exchange registry backing this worker's gRPC `ShuffleService`
    /// and deployed application exchange inlets (Issue #400 R2).
    pub fn exchange_registry(&self) -> crate::exchange::service::ExchangeRegistry {
        self.exchange_registry.clone()
    }

    /// Number of gRPC Arrow IPC shuffle frames received by this worker.
    pub fn grpc_frames_received(&self) -> u64 {
        self.exchange_registry.grpc_frames_received()
    }

    /// Control-free hot-path local lease fence check (Issue #400 R4).
    ///
    /// Verifies that this worker still owns `shard_id` with `lease_token` and
    /// that no strictly higher `LeaseToken` is known in the local topology
    /// lease snapshot, without making a synchronous RPC to `ControlService`.
    pub fn verify_local_lease_fence(&self, shard_id: ShardId, lease_token: LeaseToken) -> bool {
        let Some(local_worker_id) = self.worker_id() else {
            return false;
        };
        let active_ok = self.active_shards.read().get(&shard_id).is_some_and(|s| {
            s.lease.worker_id == local_worker_id && s.lease.lease_token == lease_token
        });
        if !active_ok {
            return false;
        }
        if let Some(known) = self.shard_leases.read().get(&shard_id) {
            if known.lease_token.0 > lease_token.0 || known.worker_id != local_worker_id {
                return false;
            }
        }
        true
    }

    /// Request a fresh short-lived token over the authenticated control channel.
    pub async fn request_secret_token(
        &self,
        workload_id: rockstream_types::ids::WorkloadId,
        secret_name: impl Into<String>,
    ) -> Result<(), io::Error> {
        let secret_name = secret_name.into();
        self.secret_manager
            .track_refresh(workload_id, secret_name.clone())
            .map_err(io::Error::other)?;
        self.msg_tx
            .send(WorkerMessage::ResolveWorkloadSecretToken {
                workload_id,
                secret_name,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "Client channel closed"))
    }

    /// Read a decrypted credential from memory. No storage path is consulted.
    pub fn secret(
        &self,
        workload_id: rockstream_types::ids::WorkloadId,
        secret_name: &str,
        now_secs: u64,
    ) -> Option<crate::secrets::ResolvedSecret> {
        self.secret_manager.get(workload_id, secret_name, now_secs)
    }

    pub fn secret_manager(&self) -> Arc<WorkerSecretManager> {
        self.secret_manager.clone()
    }

    /// Send a request to acquire a shard lease.
    pub async fn request_shard(&self, shard_id: ShardId) -> Result<(), io::Error> {
        let wid = self.worker_id().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "Worker is not registered yet")
        })?;
        let msg = WorkerMessage::RequestShard {
            worker_id: wid,
            shard_id,
        };
        self.msg_tx
            .send(msg)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "Client channel closed"))
    }

    /// Send a fence write check to the control plane and return whether it is valid.
    pub async fn check_fence_write(
        &self,
        shard_id: ShardId,
        lease_token: LeaseToken,
    ) -> Result<bool, io::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            self.fence_waiters
                .lock()
                .entry(shard_id)
                .or_default()
                .push(tx);
        }
        let msg = WorkerMessage::FenceWrite {
            shard_id,
            lease_token,
        };
        if self.msg_tx.send(msg).await.is_err() {
            self.fence_waiters
                .lock()
                .get_mut(&shard_id)
                .map(|w| w.pop());
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Client channel closed",
            ));
        }
        rx.await.map_err(|_| {
            io::Error::new(io::ErrorKind::ConnectionAborted, "Response channel closed")
        })
    }

    /// Report committed shard progress and await the control-plane response.
    /// The current wire protocol permits one outstanding report per client.
    pub async fn report_shard_frontier(
        &self,
        shard_id: ShardId,
        epoch: u64,
    ) -> Result<u64, String> {
        // shortcut: the wire protocol has no request ID, so one report may be pending per client; add correlation IDs before supporting concurrent reports.
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut waiter = self.frontier_report_waiter.lock();
            if waiter.is_some() {
                return Err("a frontier report is already pending".to_owned());
            }
            *waiter = Some(tx);
        }
        self.msg_tx
            .send(WorkerMessage::ReportShardFrontier { shard_id, epoch })
            .await
            .map_err(|_| {
                self.frontier_report_waiter.lock().take();
                "Client channel closed".to_owned()
            })?;
        rx.await
            .map_err(|_| "Control response channel closed".to_owned())?
    }
}

fn validate_local_control_peer(
    local_mode: bool,
    protected: bool,
    peer: std::net::IpAddr,
) -> io::Result<()> {
    if local_mode && !protected && !peer.is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "explicit local mode requires a loopback control address",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct WorkerExchangeEndpoint {
    pub bind_addr: SocketAddr,
    pub advertise_addr: String,
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Connect to the control plane and start the worker client daemon loop.
pub async fn start_worker_client(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        WorkerLocation::default(),
        WorkerCapabilities::default(),
    )
    .await
}

/// Connect to the control plane and start the worker client daemon loop with
/// explicit locality/capability metadata.
pub async fn start_worker_client_with_metadata(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_and_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        InternalTlsConfig::default(),
    )
    .await
}

/// Connect to the control plane over mTLS and start the worker client daemon loop.
pub async fn start_worker_client_with_tls(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    tls_config: InternalTlsConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_and_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        WorkerLocation::default(),
        WorkerCapabilities::default(),
        tls_config,
    )
    .await
}

/// Connect to the control plane over mTLS with explicit locality/capability metadata.
pub async fn start_worker_client_with_tls_and_metadata(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_metadata_and_compaction(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        tls_config,
        EpochCompactionConfig::default(),
    )
    .await
}

/// Connect to the control plane and start the worker client daemon loop with custom compaction config.
pub async fn start_worker_client_with_compaction_config(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    compaction_config: EpochCompactionConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_metadata_and_compaction(
        proposed_worker_id,
        control_url,
        storage_dir,
        WorkerLocation::default(),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        compaction_config,
    )
    .await
}

/// Connect to the control plane over mTLS with explicit locality, capability metadata, and compaction config.
pub async fn start_worker_client_with_tls_metadata_and_compaction(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
    compaction_config: EpochCompactionConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_runtime(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        tls_config,
        compaction_config,
        None,
        ExchangeConfig::default(),
        false,
    )
    .await
}

pub async fn start_worker_client_with_insecure_internal_dev_and_metadata(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_runtime(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        InternalTlsConfig::default(),
        EpochCompactionConfig::default(),
        None,
        ExchangeConfig::default(),
        true,
    )
    .await
}

pub async fn start_worker_client_with_insecure_internal_dev_and_metadata_and_tls(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_runtime(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        tls_config,
        EpochCompactionConfig::default(),
        None,
        ExchangeConfig::default(),
        true,
    )
    .await
}

/// Start a worker with its peer shuffle listener available before registration.
#[allow(clippy::too_many_arguments)]
pub async fn start_worker_client_with_exchange_endpoint(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
    exchange_config: ExchangeConfig,
    endpoint: WorkerExchangeEndpoint,
    allow_insecure_internal_dev: bool,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_runtime(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        tls_config,
        EpochCompactionConfig::default(),
        Some(endpoint),
        exchange_config,
        allow_insecure_internal_dev,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_worker_client_with_runtime(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
    compaction_config: EpochCompactionConfig,
    exchange_endpoint: Option<WorkerExchangeEndpoint>,
    exchange_config: ExchangeConfig,
    allow_insecure_internal_dev: bool,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    compaction_config
        .validate()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let registration_address = exchange_endpoint
        .as_ref()
        .map(|endpoint| endpoint.advertise_addr.clone())
        .unwrap_or_else(|| "127.0.0.1:0".to_owned());
    let initial_headroom = system_memory_headroom()?;
    let clean_url = control_url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let stream = TcpStream::connect(clean_url).await?;
    validate_local_control_peer(
        allow_insecure_internal_dev,
        tls_config.is_enabled(),
        stream.peer_addr()?.ip(),
    )?;
    let local_ip = stream.local_addr()?.ip();
    let exchange_ip = if !tls_config.is_enabled() && !local_ip.is_loopback() {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        local_ip
    };
    let exchange_addr = std::net::SocketAddr::new(exchange_ip, 0);

    if tls_config.is_enabled() {
        let connector = crate::tls::build_client_tls_connector(&tls_config).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RS-2405: TLS client error: {e}"),
            )
        })?;
        let host = clean_url.split(':').next().unwrap_or("localhost");
        let server_name =
            rustls::pki_types::ServerName::try_from(host.to_string()).unwrap_or_else(|_| {
                rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap()
            });
        let tls_stream = connector.connect(server_name, stream).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("RS-2411: TLS handshake error: {e}"),
            )
        })?;
        let (reader, writer) = tokio::io::split(tls_stream);
        run_worker_client(
            proposed_worker_id,
            storage_dir,
            location,
            capabilities,
            initial_headroom,
            compaction_config,
            registration_address,
            exchange_addr,
            exchange_endpoint.clone(),
            tls_config.clone(),
            exchange_config.clone(),
            allow_insecure_internal_dev,
            reader,
            writer,
        )
        .await
    } else {
        let (reader, writer) = stream.into_split();
        run_worker_client(
            proposed_worker_id,
            storage_dir,
            location,
            capabilities,
            initial_headroom,
            compaction_config,
            registration_address,
            exchange_addr,
            exchange_endpoint,
            tls_config,
            exchange_config,
            allow_insecure_internal_dev,
            reader,
            writer,
        )
        .await
    }
}

#[cfg(target_os = "linux")]
fn system_memory_headroom() -> io::Result<CapacityHeadroom> {
    // shortcut: reports host headroom; read cgroup limits when workers are container-constrained.
    parse_linux_memory_headroom(&std::fs::read_to_string("/proc/meminfo")?)
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_memory_headroom(meminfo: &str) -> io::Result<CapacityHeadroom> {
    fn kibibytes(meminfo: &str, field: &str) -> Option<u64> {
        for line in meminfo.lines() {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name == field {
                return value.split_whitespace().next()?.parse().ok();
            }
        }
        None
    }

    let total = kibibytes(meminfo, "MemTotal")
        .filter(|total| *total > 0)
        .ok_or_else(|| io::Error::other("could not read physical memory headroom"))?;
    let available = kibibytes(meminfo, "MemAvailable")
        .ok_or_else(|| io::Error::other("could not read physical memory headroom"))?;
    Ok(CapacityHeadroom::new(available as f64 / total as f64))
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn system_memory_headroom() -> io::Result<CapacityHeadroom> {
    let total_pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    if total_pages <= 0 {
        return Err(io::Error::other("could not read physical memory headroom"));
    }
    let mut stats: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
    let mut count = libc::HOST_VM_INFO64_COUNT;
    let result = unsafe {
        libc::host_statistics64(
            libc::mach_host_self(),
            libc::HOST_VM_INFO64,
            &mut stats as *mut _ as libc::host_info64_t,
            &mut count,
        )
    };
    if result != 0 {
        return Err(io::Error::other("could not read physical memory headroom"));
    }
    let available_pages =
        stats.free_count + stats.inactive_count + stats.speculative_count + stats.purgeable_count;
    Ok(CapacityHeadroom::new(
        available_pages as f64 / total_pages as f64,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn system_memory_headroom() -> io::Result<CapacityHeadroom> {
    Err(io::Error::other(
        "physical memory headroom is unsupported on this platform",
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_worker_client<R, W>(
    proposed_worker_id: u64,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    initial_headroom: CapacityHeadroom,
    compaction_config: EpochCompactionConfig,
    mut registration_address: String,
    exchange_addr: std::net::SocketAddr,
    exchange_endpoint: Option<WorkerExchangeEndpoint>,
    tls_config: InternalTlsConfig,
    exchange_config: ExchangeConfig,
    allow_insecure_internal_dev: bool,
    reader: R,
    mut writer: W,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut capabilities = capabilities;
    capabilities.shared_shard_store_id = shared_shard_store_id();
    let worker_id = Arc::new(RwLock::new(None));
    let active_shards = Arc::new(RwLock::new(HashMap::new()));
    let topology_workers = Arc::new(RwLock::new(HashMap::new()));
    let shard_leases = Arc::new(RwLock::new(HashMap::new()));
    let (msg_tx, mut msg_rx) = mpsc::channel::<WorkerMessage>(32);
    let security_audit_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let security_audit_capacity = msg_tx.max_capacity();
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let frontier_report_waiter = Arc::new(parking_lot::Mutex::new(None));
    let secret_manager = Arc::new(WorkerSecretManager::new(format!(
        "worker-{proposed_worker_id}"
    )));
    let storage_context = Arc::new(
        rockstream_storage::storage_context::WorkerStorageContext::new_with_worker_id(
            &format!("worker-{proposed_worker_id}"),
            536_870_912,
        ),
    );
    let compaction_config = Arc::new(RwLock::new(compaction_config));

    let deployments = Arc::new(RwLock::new(HashMap::<
        (WorkloadId, ShardId),
        Arc<WorkerDeployment>,
    >::new()));
    let tls_reloader = if tls_config.is_enabled() {
        Some(Arc::new(
            rockstream_types::tls::TlsCertificateReloader::new(tls_config.clone())
                .map_err(io::Error::other)?,
        ))
    } else {
        None
    };

    let exchange_registry = crate::exchange::service::ExchangeRegistry::with_worker_state(
        active_shards.clone(),
        topology_workers.clone(),
    )
    .with_shard_leases(shard_leases.clone())
    .with_security_audit(msg_tx.clone(), security_audit_waiters.clone());

    let handle = WorkerClientHandle {
        direct_grants: crate::direct_grants::DirectGrantRegistry::default(),
        shard_loads: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        worker_id: worker_id.clone(),
        active_shards: active_shards.clone(),
        topology_workers: topology_workers.clone(),
        shard_leases: shard_leases.clone(),
        msg_tx: msg_tx.clone(),
        security_audit_waiters: security_audit_waiters.clone(),
        fence_waiters: fence_waiters.clone(),
        frontier_report_waiter: frontier_report_waiter.clone(),
        secret_manager: secret_manager.clone(),
        storage_context: storage_context.clone(),
        compaction_config: compaction_config.clone(),
        deployments: deployments.clone(),
        tls_reloader: tls_reloader.clone(),
        exchange_registry: exchange_registry.clone(),
    };

    let actor_registry =
        ShardActorRegistry::new().with_cancellation_reporter(cancellation_reporter(msg_tx.clone()));
    let executor_client = handle.clone();
    let executor_deployments = deployments.clone();
    let execute: OwnedFrameExecutor = Arc::new(move |frame| {
        let client = executor_client.clone();
        let deployments = executor_deployments.clone();
        Box::pin(async move { execute_frame_and_report(&client, &deployments, frame).await })
    });

    let mut shuffle_pool =
        crate::exchange::pool::ShuffleClientPool::new(Arc::new(RwLock::new(HashMap::new())));
    let exchange_bind_addr = exchange_endpoint
        .as_ref()
        .map_or(exchange_addr, |endpoint| endpoint.bind_addr);
    let should_start_exchange =
        exchange_endpoint.is_some() || allow_insecure_internal_dev || tls_config.is_enabled();
    let mut exchange_service =
        crate::exchange::service::ExchangeService::new(handle.exchange_registry())
            .with_exchange_config(exchange_config.clone())
            .with_owned_handler(owned_shuffle_handler(
                actor_registry.clone(),
                deployments.clone(),
                handle.clone(),
            ));
    if tls_config.is_enabled() {
        exchange_service = exchange_service.with_internal_tls(tls_config.clone());
        if let Some(reloader) = tls_reloader.clone() {
            exchange_service = exchange_service.with_tls_reloader(reloader);
        }
        shuffle_pool = shuffle_pool.with_internal_tls(tls_config.clone());
    } else if allow_insecure_internal_dev {
        exchange_service = exchange_service.with_insecure_internal_dev();
    }
    let exchange_shutdown = if should_start_exchange {
        let listener = tokio::net::TcpListener::bind(exchange_bind_addr).await?;
        let local_addr = listener.local_addr()?;
        registration_address = match exchange_endpoint.as_ref() {
            Some(endpoint) if endpoint.bind_addr.port() == 0 => endpoint
                .advertise_addr
                .parse::<std::net::SocketAddr>()
                .ok()
                .filter(|addr| addr.port() == 0)
                .map(|addr| std::net::SocketAddr::new(addr.ip(), local_addr.port()).to_string())
                .unwrap_or(registration_address),
            Some(_) => registration_address,
            None => local_addr.to_string(),
        };
        let shutdown = exchange_service.cancel_token().clone();
        let _exchange_task = exchange_service
            .start_listener(listener)
            .await
            .map_err(io::Error::other)?;
        Some(shutdown)
    } else {
        None
    };
    let shuffle_mux = crate::exchange::multiplexer::WorkerStreamMultiplexer::new(
        shuffle_pool.clone(),
        crate::exchange::flow_control::FlowController::new(),
    )
    .with_src_worker(WorkerId(proposed_worker_id));
    let worker_id_clone = worker_id.clone();
    let epoch_client = handle.clone();
    let shard_loads_for_heartbeat = handle.shard_loads.clone();
    let active_shards_clone = active_shards.clone();
    let fence_waiters_clone = fence_waiters.clone();
    let frontier_report_waiter_clone = frontier_report_waiter.clone();
    let storage_dir = storage_dir.to_path_buf();
    let secret_manager_clone = secret_manager.clone();
    let deployments_clone = deployments.clone();
    let actor_registry_clone = actor_registry.clone();
    let execute_clone = execute.clone();
    let storage_context_clone = storage_context.clone();
    let compaction_config_clone = compaction_config.clone();

    let join_handle = tokio::spawn(async move {
        let _exchange_shutdown = exchange_shutdown.map(CancelOnDrop);
        // 1. Send Registration message.
        let reg = WorkerRegistration::new(
            WorkerId(proposed_worker_id),
            NodeRole::Worker,
            registration_address,
            initial_headroom,
        )
        .with_location(location.clone())
        .with_capabilities(capabilities)
        .with_compatibility(
            rockstream_types::compatibility::SupportedVersionRange::v4_only(),
            rockstream_types::compatibility::SupportedStorageFormatRange::v1_through_v2(),
        );
        let mut local_info = rockstream_types::topology::WorkerInfo::from_registration(&reg);
        let reg_msg = WorkerMessage::Register(reg);
        let reg_line = serde_json::to_string(&reg_msg).unwrap() + "\n";
        if let Err(e) = writer.write_all(reg_line.as_bytes()).await {
            tracing::error!(
                code = %rockstream_types::error_code::RS_0001,
                "Failed to write registration to control plane: {:?}",
                e
            );
            return;
        }

        // 2. Spawn writer task to forward WorkerMessage channel over TCP.
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let mut writer_tx = writer;
        let _writer_task = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = msg_rx.recv() => {
                        if let Some(msg) = msg {
                            if let Ok(line) = serde_json::to_string(&msg) {
                                let line = line + "\n";
                                if let Err(e) = writer_tx.write_all(line.as_bytes()).await {
                                    tracing::error!(
                                        code = %rockstream_types::error_code::RS_0001,
                                        "Worker client write error: {:?}",
                                        e
                                    );
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        }));

        // 3. Spawn heartbeat task once registered.
        let msg_tx_hb = msg_tx.clone();
        let worker_id_hb = worker_id_clone.clone();
        let _heartbeat_task = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                let wid_opt = *worker_id_hb.read();
                if let Some(wid) = wid_opt {
                    let Ok(capacity_headroom) = system_memory_headroom() else {
                        tracing::warn!("could not sample worker memory headroom");
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    };
                    let hb = WorkerMessage::Heartbeat {
                        worker_id: wid,
                        capacity_headroom,
                    };
                    if msg_tx_hb.send(hb).await.is_err() {
                        break;
                    }
                    let samples: Vec<_> = shard_loads_for_heartbeat
                        .lock()
                        .drain()
                        .map(|(_, sample)| sample)
                        .collect();
                    if !samples.is_empty() {
                        let _ = msg_tx_hb
                            .send(WorkerMessage::ShardLoadReport {
                                worker_id: wid,
                                samples,
                            })
                            .await;
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
        }));

        let refresh_msg_tx = msg_tx.clone();
        let refresh_secret_manager = secret_manager_clone.clone();
        let refresh_deployments = deployments_clone.clone();
        let _secret_refresh_task = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await;
            loop {
                interval.tick().await;
                let active_workloads: std::collections::HashSet<_> = refresh_deployments
                    .read()
                    .keys()
                    .map(|(workload_id, _)| *workload_id)
                    .collect();
                for (workload_id, secret_name) in
                    refresh_secret_manager.refresh_targets(&active_workloads)
                {
                    if refresh_msg_tx
                        .send(WorkerMessage::ResolveWorkloadSecretToken {
                            workload_id,
                            secret_name,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }));

        // 4. Read Loop: process ControlMessage commands from control plane.
        let mut lines = BufReader::new(reader).lines();
        let mut explicit_shutdown = false;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let msg: ControlMessage = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        code = %rockstream_types::error_code::RS_0001,
                        "Invalid message from control plane: {:?}, raw: {}",
                        e,
                        line
                    );
                    continue;
                }
            };

            match msg {
                ControlMessage::InstallDirectEpochGrant { grant } => {
                    let grant_id = grant.grant_id;
                    let installed =
                        install_direct_epoch_grant(&epoch_client, &deployments_clone, grant).await;
                    if let Err(error) = &installed {
                        tracing::warn!(%grant_id, %error, "direct epoch grant installation rejected");
                    }
                    let accepted = installed.is_ok();
                    let _ = msg_tx
                        .send(WorkerMessage::DirectEpochGrantInstalled {
                            worker_id: WorkerId(proposed_worker_id),
                            grant_id,
                            accepted,
                        })
                        .await;
                }
                ControlMessage::RevokeDirectEpochGrants { metadata, grant_id } => {
                    epoch_client.direct_grants.revoke(&metadata, grant_id);
                }
                ControlMessage::SecurityAuditRecorded {
                    request_id,
                    persisted,
                } => {
                    if let Some(waiter) = crate::exchange::service::remove_security_audit_waiter(
                        &security_audit_waiters,
                        &request_id,
                        security_audit_capacity,
                    ) {
                        let _ = waiter.send(persisted);
                    }
                }
                ControlMessage::BeginDrain(request) => {
                    let Some(id) = *worker_id_clone.read() else {
                        continue;
                    };
                    if request.worker_id != id {
                        tracing::error!(code = %rockstream_types::error_code::RS_0001, worker = %id, requested = %request.worker_id, "worker rejected a drain request for another identity");
                        continue;
                    }
                    let shard_ids = active_shards_clone
                        .read()
                        .keys()
                        .copied()
                        .collect::<Vec<_>>();
                    let mut databases = Vec::new();
                    for shard_id in &shard_ids {
                        actor_registry_clone.revoke(*shard_id);
                        deployments_clone.write().retain(|_, deployment| {
                            if deployment.descriptor.shard.shard_id == *shard_id {
                                databases.push(deployment.db.as_ref().clone());
                                false
                            } else {
                                true
                            }
                        });
                        if let Some(state) = active_shards_clone.write().remove(shard_id) {
                            if let Some(db) = state.db {
                                databases.push(db);
                            }
                        }
                    }
                    let mut flushed = true;
                    for db in unique_shard_databases(databases) {
                        if let Err(error) = db.flush().await {
                            tracing::error!(code = %rockstream_types::error_code::RS_0001, worker = %id, %error, "worker drain stopped because shard flush failed");
                            flushed = false;
                            break;
                        }
                        if let Err(error) = db.close().await {
                            tracing::error!(code = %rockstream_types::error_code::RS_0001, worker = %id, %error, "worker drain stopped because shard close failed");
                            flushed = false;
                            break;
                        }
                    }
                    if flushed {
                        rockstream_types::metrics::set_r1_worker_shards_owned(id, 0);
                        if msg_tx
                            .send(WorkerMessage::DrainAck {
                                worker_id: id,
                                shards_remaining: 0,
                            })
                            .await
                            .is_err()
                        {
                            tracing::warn!(worker = %id, "worker drain acknowledgement could not be queued");
                        }
                    }
                }
                ControlMessage::Registered {
                    worker_id: wid,
                    protocol_version,
                } => {
                    if !rockstream_types::compatibility::SupportedVersionRange::v4_only()
                        .contains(protocol_version)
                    {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_5021,
                            %protocol_version,
                            "control plane does not support the worker protocol required for audited shard ownership and complete workload reads"
                        );
                        break;
                    }
                    tracing::info!("Worker client registered successfully as {:?}", wid);
                    *worker_id_clone.write() = Some(wid);
                    local_info.worker_id = wid;
                    shuffle_pool.set_local_worker_info(local_info.clone());
                    shuffle_pool.upsert_peer_info(local_info.clone());
                    let _ = msg_tx
                        .send(WorkerMessage::OwnedShuffleReady { worker_id: wid })
                        .await;
                }
                ControlMessage::TopologyChanged {
                    workers,
                    shard_leases: leases,
                } => {
                    shuffle_pool.replace_peer_infos(workers.clone());
                    if let Some(worker) = workers
                        .iter()
                        .find(|worker| Some(worker.worker_id) == *worker_id_clone.read())
                    {
                        local_info = worker.clone();
                    }
                    shuffle_pool.set_local_worker_info(local_info.clone());
                    shuffle_pool.upsert_peer_info(local_info.clone());
                    let mut topology = topology_workers.write();
                    topology.clear();
                    topology.extend(workers.into_iter().map(|worker| (worker.worker_id, worker)));
                    let mut current_leases = shard_leases.write();
                    current_leases.clear();
                    current_leases.extend(leases.into_iter().map(|lease| (lease.shard_id, lease)));
                }
                ControlMessage::Deploy { descriptor } => {
                    let result: io::Result<Arc<WorkerDeployment>> = async {
                        if descriptor.version != DEPLOYMENT_DESCRIPTOR_VERSION
                            || worker_id_clone.read().as_ref() != Some(&descriptor.shard.worker_id)
                            || active_shards_clone
                                .read()
                                .get(&descriptor.shard.shard_id)
                                .map(|state| state.lease.lease_token)
                                != Some(descriptor.shard.lease_token)
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "deployment identity does not match the current lease",
                            ));
                        }
                        let path = descriptor
                            .storage_identity
                            .strip_prefix("lfs:")
                            .unwrap_or(&descriptor.storage_identity);
                        let store = rockstream_storage::build_runtime_object_store(
                            Path::new(path),
                            &descriptor.object_store_prefix(),
                        )
                        .map_err(io::Error::other)?;
                        if std::env::var_os("ROCKSTREAM_OBJECT_STORE_ENDPOINT").is_some() {
                            use futures::StreamExt;
                            let manifest_prefix = object_store::path::Path::from("db/manifest");
                            if store.list(Some(&manifest_prefix)).next().await.transpose().map_err(io::Error::other)?.is_none() {
                                let legacy = rockstream_storage::build_runtime_object_store(Path::new(path), &descriptor.storage_root).map_err(io::Error::other)?;
                                if legacy.list(Some(&manifest_prefix)).next().await.transpose().map_err(io::Error::other)?.is_some() {
                                    return Err(io::Error::other("RS-0003: legacy shared workload namespace exists; migrate it to isolated workload/shard prefixes before deployment"));
                                }
                            }
                        }
                        let db = Arc::new(
                            ShardDb::builder("db", store)
                                .with_storage_context(storage_context_clone.clone())
                                .build()
                                .await
                                .map_err(io::Error::other)?,
                        );
                        db.fence_lease_token(descriptor.shard.lease_token)
                            .await
                            .map_err(|error| {
                                io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
                            })?;
                        let schemas = deployment_schema(&descriptor)?;
                        let plan: rockstream_plan::PlanNode =
                            serde_json::from_str(&descriptor.plan_json).map_err(|error| {
                                io::Error::new(io::ErrorKind::InvalidData, error)
                            })?;
                        let compiled = rockstream_ops::compile_plan_with_sink_id_and_strategy(
                            &plan,
                            db.clone(),
                            &schemas,
                            descriptor.sink_operator_id,
                            descriptor.join_strategy,
                        )
                        .map_err(io::Error::other)?;
                        if let Some(join) = &compiled.join {
                            join.pipeline.restore(&db).await.map_err(io::Error::other)?;
                        } else {
                            compiled
                                .pipeline
                                .restore(&db)
                                .await
                                .map_err(io::Error::other)?;
                        }
                        let mut comp_cfg = compaction_config_clone.read().clone();
                        if !descriptor.primary_key.is_empty() {
                            comp_cfg =
                                comp_cfg.with_primary_key_columns(descriptor.primary_key.clone());
                        }
                        let compactor = Arc::new(EpochCompactor::new(comp_cfg));
                        let owned_epoch = crate::owned_epoch::OwnedEpochJournal::new(db.clone());
                        let recovery_on_open = !matches!(owned_epoch.load().await, Ok(None));
                        Ok(Arc::new(WorkerDeployment {
                            descriptor,
                            schemas,
                            commit_group: Arc::new(
                                PhysicalCommitGroup::open(db.clone())
                                    .await
                                    .map_err(io::Error::other)?,
                            ),
                            db,
                            compiled,
                            compactor,
                            execution_requires_recovery: tokio::sync::Mutex::new(recovery_on_open),
                            owned_epoch,
                            recovery_on_open,
                        }))
                    }
                    .await;
                    match result {
                        Ok(deployment) => {
                            let descriptor = &deployment.descriptor;
                            let old_db = active_shards_clone
                                .write()
                                .insert(
                                    descriptor.shard.shard_id,
                                    ShardState {
                                        lease: descriptor.shard.clone(),
                                        db: Some((*deployment.db).clone()),
                                    },
                                )
                                .and_then(|state| state.db);
                            if let Some(db) = old_db {
                                let _ = db.close().await;
                            }
                            rockstream_types::metrics::set_r1_worker_shards_owned(
                                descriptor.shard.worker_id,
                                active_shards_clone.read().len() as u64,
                            );
                            deployments_clone.write().insert(
                                (descriptor.workload_id, descriptor.shard.shard_id),
                                deployment.clone(),
                            );
                            // Wire bounded application exchange inlets into this worker's
                            // ExchangeRegistry so direct Arrow IPC shuffle frames and
                            // receiver credit backpressure flow end-to-end (Issue #400 R2).
                            if let Some((_source_name, source_schema)) =
                                deployment.schemas.iter().next()
                            {
                                let inlet_cap = exchange_config.frame_channel_capacity.max(1);
                                let (inlet_tx, mut inlet_rx) =
                                    mpsc::channel::<rockstream_ops::zset::ArrowZSet>(inlet_cap);
                                exchange_registry.register(
                                    descriptor.workload_id.0,
                                    descriptor.shard_index,
                                    inlet_tx,
                                    source_schema.clone(),
                                );
                                tokio::spawn(async move {
                                    while let Some(_zset) = inlet_rx.recv().await {
                                        // Draining bounded channel releases receiver buffer
                                        // capacity and propagates credit backpressure upstream.
                                    }
                                });
                            }
                            if deployment.recovery_on_open {
                                let _ = msg_tx
                                    .send(WorkerMessage::OwnedEpochRecoveryRequired {
                                        worker_id: descriptor.shard.worker_id,
                                        workload_id: descriptor.workload_id,
                                        shard_id: descriptor.shard.shard_id,
                                    })
                                    .await;
                            }
                            let _ = msg_tx
                                .send(WorkerMessage::DeploymentReady {
                                    version: descriptor.version,
                                    workload_id: descriptor.workload_id,
                                    shard_id: descriptor.shard.shard_id,
                                    worker_id: descriptor.shard.worker_id,
                                    process_id: std::process::id(),
                                    operator_ids: vec![descriptor.sink_operator_id],
                                    frontier: descriptor.frontier,
                                })
                                .await;
                        }
                        Err(error) => tracing::warn!(%error, "worker deployment refused"),
                    }
                }
                ControlMessage::Execute { frame } => {
                    let output = failure_identity(&frame);
                    if let Err(error) = actor_registry_clone.enqueue(frame) {
                        tracing::warn!(
                            code = %rockstream_types::error_code::RS_0001,
                            %error,
                            "worker execution frame refused"
                        );
                        let _ = msg_tx
                            .send(WorkerMessage::ExecutionFailed {
                                output,
                                message: error.to_string(),
                            })
                            .await;
                    }
                }
                ControlMessage::ReadWorkload {
                    version,
                    request_id,
                    workload_id,
                    shard_ids,
                } => {
                    let result = if version == WORKLOAD_SNAPSHOT_VERSION {
                        read_workload_shards(
                            &deployments_clone,
                            &active_shards_clone,
                            workload_id,
                            &shard_ids,
                        )
                        .await
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported workload snapshot version",
                        ))
                    };
                    let (available, shards) = match result {
                        Ok(shards) => (true, shards),
                        Err(error) => {
                            tracing::warn!(%workload_id, %error, "worker workload read unavailable");
                            (false, Vec::new())
                        }
                    };
                    let worker_id = *worker_id_clone.read();
                    if let Some(worker_id) = worker_id {
                        if msg_tx
                            .send(WorkerMessage::WorkloadReadReport {
                                version: WORKLOAD_SNAPSHOT_VERSION,
                                request_id,
                                workload_id,
                                worker_id,
                                available,
                                shards,
                            })
                            .await
                            .is_err()
                        {
                            tracing::warn!(%worker_id, "workload read report could not be queued");
                        }
                    }
                }
                ControlMessage::PrepareOwnedEpoch { intent } => {
                    let Some(worker_id) = *worker_id_clone.read() else {
                        continue;
                    };
                    let client = epoch_client.clone();
                    let deployments = deployments_clone.clone();
                    let sender = msg_tx.clone();
                    tokio::spawn(async move {
                        let error = update_owned_epoch(&client, &deployments, &intent, false)
                            .await
                            .err();
                        let _ = sender
                            .send(WorkerMessage::OwnedEpochPrepared {
                                worker_id,
                                intent,
                                error,
                            })
                            .await;
                    });
                }
                ControlMessage::FinishOwnedEpoch { intent } => {
                    let Some(worker_id) = *worker_id_clone.read() else {
                        continue;
                    };
                    let client = epoch_client.clone();
                    let deployments = deployments_clone.clone();
                    let sender = msg_tx.clone();
                    tokio::spawn(async move {
                        let error = update_owned_epoch(&client, &deployments, &intent, true)
                            .await
                            .err();
                        let _ = sender
                            .send(WorkerMessage::OwnedEpochFinished {
                                worker_id,
                                intent,
                                error,
                            })
                            .await;
                    });
                }
                ControlMessage::DispatchOwnedShuffle { target, frame } => {
                    shuffle_pool.upsert_peer_info(target.clone());
                    let target_worker = target.worker_id;
                    let mux = shuffle_mux.clone();
                    let deployments = deployments_clone.clone();
                    let sender = msg_tx.clone();
                    tokio::spawn(async move {
                        let output = failure_identity(&frame);
                        if let Err(message) =
                            dispatch_owned_shuffle(&mux, &deployments, target_worker, frame).await
                        {
                            let _ = sender
                                .send(WorkerMessage::ShuffleDispatchFailed { output, message })
                                .await;
                        }
                    });
                }
                ControlMessage::ReadOperatorStats {
                    request_id,
                    workload_id,
                    operator_ids,
                } => {
                    let active = deployments_clone
                        .read()
                        .keys()
                        .any(|(active_workload_id, _)| *active_workload_id == workload_id);
                    let worker_id = *worker_id_clone.read();
                    if let Some(worker_id) = worker_id {
                        let stats = if active {
                            rockstream_types::metrics::operator_runtime_stats_for_ids(&operator_ids)
                        } else {
                            Vec::new()
                        };
                        if msg_tx
                            .send(WorkerMessage::OperatorStatsReport {
                                request_id,
                                workload_id,
                                worker_id,
                                stats,
                            })
                            .await
                            .is_err()
                        {
                            tracing::warn!(%worker_id, "operator statistics report could not be queued");
                        }
                    }
                }
                ControlMessage::ShardAssigned {
                    lease,
                    operation_id,
                } => {
                    tracing::info!("Received ShardAssigned lease for {:?}", lease.shard_id);
                    let already_open =
                        active_shards_clone
                            .read()
                            .get(&lease.shard_id)
                            .is_some_and(|state| {
                                state.lease.worker_id == lease.worker_id
                                    && state.lease.lease_token == lease.lease_token
                                    && state.db.is_some()
                            });
                    if already_open {
                        if let Some(operation_id) = operation_id {
                            let _ = msg_tx
                                .send(WorkerMessage::ShardTransferAck {
                                    operation_id,
                                    stage: "recipient".to_owned(),
                                    worker_id: lease.worker_id,
                                    shard_id: lease.shard_id,
                                    lease_token: lease.lease_token,
                                    success: true,
                                    error: None,
                                })
                                .await;
                        }
                        continue;
                    }
                    // Open database for this shard
                    let shard_path = storage_dir
                        .join("shards")
                        .join(lease.shard_id.0.to_string());
                    let remote_prefix = format!("shards/{}", lease.shard_id.0);
                    let store = match rockstream_storage::build_runtime_object_store(
                        &shard_path,
                        &remote_prefix,
                    ) {
                        Ok(store) => store,
                        Err(error) => {
                            tracing::error!(
                                code = %rockstream_types::error_code::RS_0003,
                                shard = ?lease.shard_id,
                                "Failed to configure shard object store: {error}"
                            );
                            if let Some(operation_id) = operation_id {
                                let _ = msg_tx
                                    .send(WorkerMessage::ShardTransferAck {
                                        operation_id,
                                        stage: "recipient".to_owned(),
                                        worker_id: lease.worker_id,
                                        shard_id: lease.shard_id,
                                        lease_token: lease.lease_token,
                                        success: false,
                                        error: Some(error.to_string()),
                                    })
                                    .await;
                            }
                            continue;
                        }
                    };

                    let inline_compaction = !matches!(
                        std::env::var("ROCKSTREAM_COMPACTION_MODE")
                            .ok()
                            .as_deref()
                            .map(str::to_ascii_lowercase)
                            .as_deref(),
                        Some("remote")
                    );
                    // Attempt to open the ShardDb
                    let mut builder = ShardDb::builder("db", store)
                        .with_supported_format_range(
                            rockstream_types::compatibility::SupportedStorageFormatRange::v1_through_v2(
                            ),
                        )
                        .with_inline_compaction(inline_compaction)
                        .with_storage_context(storage_context_clone.clone());
                    if let Ok(metric_shard_id) = u16::try_from(lease.shard_id.0) {
                        builder = builder
                            .with_metrics_identity(metric_shard_id, lease.worker_id.to_string());
                    }
                    match builder.build().await {
                        Ok(db) => {
                            let shard_id = lease.shard_id;
                            let lease_token = lease.lease_token;
                            let owner = lease.worker_id;
                            active_shards_clone.write().insert(
                                shard_id,
                                ShardState {
                                    lease,
                                    db: Some(db),
                                },
                            );
                            actor_registry_clone.register_owned(
                                shard_id,
                                lease_token,
                                execute_clone.clone(),
                            );
                            rockstream_types::metrics::set_r1_worker_shards_owned(
                                owner,
                                active_shards_clone.read().len() as u64,
                            );
                            if let Some(operation_id) = operation_id {
                                let _ = msg_tx
                                    .send(WorkerMessage::ShardTransferAck {
                                        operation_id,
                                        stage: "recipient".to_owned(),
                                        worker_id: owner,
                                        shard_id,
                                        lease_token,
                                        success: true,
                                        error: None,
                                    })
                                    .await;
                            }
                        }
                        Err(e) => {
                            if let Some(operation_id) = operation_id {
                                let _ = msg_tx
                                    .send(WorkerMessage::ShardTransferAck {
                                        operation_id,
                                        stage: "recipient".to_owned(),
                                        worker_id: lease.worker_id,
                                        shard_id: lease.shard_id,
                                        lease_token: lease.lease_token,
                                        success: false,
                                        error: Some(e.to_string()),
                                    })
                                    .await;
                            }
                            match &e {
                                rockstream_storage::StorageError::IncompatibleFormat {
                                    stored,
                                    min,
                                    max,
                                } => tracing::error!(
                                    code = %rockstream_types::error_code::RS_5001,
                                    stored,
                                    min,
                                    max,
                                    "Failed to open ShardDb for {:?}: {}",
                                    lease.shard_id,
                                    e
                                ),
                                rockstream_storage::StorageError::MalformedFormatMarker {
                                    length,
                                    min,
                                    max,
                                } => tracing::error!(
                                    code = %rockstream_types::error_code::RS_5001,
                                    stored = "malformed",
                                    marker_length = length,
                                    min,
                                    max,
                                    "Failed to open ShardDb for {:?}: {}",
                                    lease.shard_id,
                                    e
                                ),
                                _ => tracing::error!(
                                    code = %rockstream_types::error_code::RS_0003,
                                    "Failed to open ShardDb for {:?}: {}",
                                    lease.shard_id,
                                    e
                                ),
                            }
                        }
                    }
                }
                ControlMessage::VerifyWorkloadIdle {
                    operation_id,
                    workload_id,
                    anchor,
                } => {
                    let result = if *worker_id_clone.read() == Some(anchor.worker_id) {
                        verify_workload_idle(
                            &deployments_clone,
                            &epoch_client.direct_grants,
                            &active_shards_clone,
                            workload_id,
                            &anchor,
                        )
                        .await
                    } else {
                        Err("worker identity does not match the workload anchor".to_owned())
                    };
                    let _ = msg_tx
                        .send(WorkerMessage::ShardTransferAck {
                            operation_id,
                            stage: format!("idle:{}", workload_id.0),
                            worker_id: anchor.worker_id,
                            shard_id: anchor.shard_id,
                            lease_token: anchor.lease_token,
                            success: result.is_ok(),
                            error: result.err(),
                        })
                        .await;
                }
                ControlMessage::PrepareShardTransfer {
                    operation_id,
                    lease,
                } => {
                    let worker_id = *worker_id_clone.read();
                    let active_lease = active_shards_clone
                        .read()
                        .get(&lease.shard_id)
                        .map(|state| state.lease.lease_token);
                    let has_lease = worker_id == Some(lease.worker_id)
                        && active_lease.is_none_or(|token| token == lease.lease_token);
                    let result = if !has_lease {
                        Err("worker does not hold the requested shard lease".to_owned())
                    } else {
                        let mut deployed: Vec<_> = deployments_clone
                            .read()
                            .values()
                            .filter(|deployment| {
                                deployment.descriptor.shard.shard_id == lease.shard_id
                            })
                            .cloned()
                            .collect();
                        deployed.sort_by_key(|deployment| deployment.descriptor.workload_id);
                        async {
                            // Wait for any admitted application commit before revoking
                            // its actor. Keep every gate held through flush and close.
                            let mut gates = Vec::with_capacity(deployed.len());
                            let mut admission_gates = Vec::with_capacity(deployed.len());
                            for deployment in &deployed {
                                admission_gates.push(
                                    epoch_client
                                        .direct_grants
                                        .installation_guard(deployment.descriptor.workload_id)
                                        .await?,
                                );
                                let gate = deployment.execution_requires_recovery.lock().await;
                                if *gate || !deployment.descriptor.shard.same_owner(&lease) {
                                    return Err("workload requires recovery or has changed lease"
                                        .to_owned());
                                }
                                deployment
                                    .db
                                    .verify_lease_token(lease.lease_token)
                                    .await
                                    .map_err(|error| error.to_string())?;
                                validate_idle_journals(deployment).await?;
                                gates.push(gate);
                            }
                            let mut databases: Vec<_> = deployed
                                .iter()
                                .map(|deployment| deployment.db.as_ref().clone())
                                .collect();
                            if let Some(db) = active_shards_clone
                                .read()
                                .get(&lease.shard_id)
                                .and_then(|state| state.db.clone())
                            {
                                databases.push(db);
                            }
                            let databases = unique_shard_databases(databases);
                            // Flush all distinct writers before changing readiness. A flush
                            // failure must not leave an otherwise live donor revoked.
                            for db in &databases {
                                db.flush()
                                    .await
                                    .map_err(|error| format!("shard flush failed: {error}"))?;
                            }
                            // A direct request that already captured an Arc must fail
                            // after waking, even after these gates are released.
                            for gate in &mut gates {
                                **gate = true;
                            }
                            actor_registry_clone.revoke(lease.shard_id);
                            for db in databases {
                                db.close()
                                    .await
                                    .map_err(|error| format!("shard close failed: {error}"))?;
                            }
                            active_shards_clone.write().remove(&lease.shard_id);
                            deployments_clone.write().retain(|_, deployment| {
                                deployment.descriptor.shard.shard_id != lease.shard_id
                            });
                            rockstream_types::metrics::set_r1_worker_shards_owned(
                                lease.worker_id,
                                active_shards_clone.read().len() as u64,
                            );
                            Ok(())
                        }
                        .await
                    };
                    if let Err(error) = &result {
                        tracing::warn!(
                            %operation_id,
                            shard_id = ?lease.shard_id,
                            %error,
                            "worker shard transfer preparation refused"
                        );
                    }
                    let _ = msg_tx
                        .send(WorkerMessage::ShardTransferAck {
                            operation_id,
                            stage: "donor".to_owned(),
                            worker_id: lease.worker_id,
                            shard_id: lease.shard_id,
                            lease_token: lease.lease_token,
                            success: result.is_ok(),
                            error: result.err(),
                        })
                        .await;
                }
                ControlMessage::CreateShardCheckpoint {
                    request_id,
                    checkpoint_id,
                    lease,
                } => {
                    let result = async {
                        if *worker_id_clone.read() != Some(lease.worker_id) {
                            return Err("worker identity does not match the shard lease".to_owned());
                        }
                        let db = active_shards_clone
                            .read()
                            .get(&lease.shard_id)
                            .filter(|state| state.lease == lease)
                            .and_then(|state| state.db.clone())
                            .ok_or_else(|| {
                                "worker does not hold the requested active shard lease".to_owned()
                            })?;
                        // Flush any buffered writes before pinning a durable SlateDB checkpoint so
                        // active workloads produce a complete, point-in-time snapshot without
                        // blocking foreground ingestion.
                        db.flush()
                            .await
                            .map_err(|error| format!("flush shard before checkpoint: {error}"))?;
                        db.create_checkpoint()
                            .await
                            .map(|handle| (handle.shard_checkpoint_id, handle.snapshot_id))
                            .map_err(|error| format!("create SlateDB checkpoint: {error}"))
                    }
                    .await;
                    let (shard_checkpoint_id, snapshot_id, error) = match result {
                        Ok((manifest_id, snapshot_id)) => {
                            (Some(manifest_id), Some(snapshot_id), None)
                        }
                        Err(error) => (None, None, Some(error)),
                    };
                    let _ = msg_tx
                        .send(WorkerMessage::ShardCheckpointAck {
                            request_id,
                            checkpoint_id,
                            worker_id: lease.worker_id,
                            shard_id: lease.shard_id,
                            lease_token: lease.lease_token,
                            shard_checkpoint_id,
                            snapshot_id,
                            error,
                        })
                        .await;
                }
                ControlMessage::ShardRevoked { shard_id, reason } => {
                    tracing::info!(
                        "Received ShardRevoked for {:?} due to {:?}",
                        shard_id,
                        reason
                    );
                    actor_registry_clone.revoke(shard_id);
                    // Close and drop ShardDb without holding lock across await
                    let db_to_close = {
                        active_shards_clone
                            .write()
                            .remove(&shard_id)
                            .and_then(|state| state.db)
                    };
                    if let Some(db) = db_to_close {
                        let _ = db.close().await;
                    }
                    if let Some(worker_id) = *worker_id_clone.read() {
                        rockstream_types::metrics::set_r1_worker_shards_owned(
                            worker_id,
                            active_shards_clone.read().len() as u64,
                        );
                    }
                    let revoked_inlets: Vec<(u64, u32)> = deployments_clone
                        .read()
                        .values()
                        .filter(|d| d.descriptor.shard.shard_id == shard_id)
                        .map(|d| (d.descriptor.workload_id.0, d.descriptor.shard_index))
                        .collect();
                    for (exchange_id, target_shard) in revoked_inlets {
                        exchange_registry.unregister(exchange_id, target_shard);
                    }
                    deployments_clone
                        .write()
                        .retain(|_, deployment| deployment.descriptor.shard.shard_id != shard_id);
                }
                ControlMessage::FenceAck { shard_id, valid } => {
                    tracing::info!("Received FenceAck for {:?}: valid={}", shard_id, valid);
                    if !valid {
                        actor_registry_clone.revoke(shard_id);
                        // Immediately detach and close ShardDb
                        let db_to_close = {
                            active_shards_clone
                                .write()
                                .remove(&shard_id)
                                .and_then(|state| state.db)
                        };
                        if let Some(db) = db_to_close {
                            let _ = db.close().await;
                        }
                        if let Some(worker_id) = *worker_id_clone.read() {
                            rockstream_types::metrics::set_r1_worker_shards_owned(
                                worker_id,
                                active_shards_clone.read().len() as u64,
                            );
                        }
                    }
                    // Notify waiters
                    let waiters = { fence_waiters_clone.lock().remove(&shard_id) };
                    if let Some(waiters) = waiters {
                        for tx in waiters {
                            let _ = tx.send(valid);
                        }
                    }
                }
                ControlMessage::ClusterFrontierAdvanced { epoch } => {
                    if let Some(waiter) = frontier_report_waiter_clone.lock().take() {
                        let _ = waiter.send(Ok(epoch));
                    }
                    tracing::info!("Cluster frontier advanced to {}", epoch);
                    let dbs: Vec<rockstream_storage::ShardDb> = active_shards_clone
                        .read()
                        .values()
                        .filter_map(|state| state.db.clone())
                        .collect();
                    for db in dbs {
                        if let Err(e) =
                            crate::exchange::persistence::gc_exchange_storage(&db, epoch).await
                        {
                            tracing::error!(
                                code = %rockstream_types::error_code::RS_0003,
                                "Failed to gc exchange storage: {:?}",
                                e
                            );
                        }
                    }
                }
                ControlMessage::OperationFailed { code, message, .. } => {
                    if let Some(waiter) = frontier_report_waiter_clone.lock().take() {
                        // Preserve the registered error code supplied by the control plane.
                        let error = format!("{code}: {message}");
                        let _ = waiter.send(Err(error));
                    }
                }
                ControlMessage::NotLeader { current_leader } => {
                    if let Some(waiter) = frontier_report_waiter_clone.lock().take() {
                        let error = format!(
                            "{}: not leader: {current_leader:?}",
                            rockstream_types::error_code::RS_1731
                        );
                        let _ = waiter.send(Err(error));
                    }
                }
                ControlMessage::SecretTokenIssued { token } => {
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if let Err(error) = secret_manager_clone.resolve_token(&token, now_secs) {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_2423,
                            error = %error,
                            "worker secret token rejected"
                        );
                    }
                }
                ControlMessage::SecretRefreshRequired {
                    workload_id,
                    secret_name,
                } => {
                    if let Err(error) =
                        secret_manager_clone.track_refresh(workload_id, secret_name.clone())
                    {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_3601,
                            %error,
                            "worker secret refresh target limit reached"
                        );
                        continue;
                    }
                    if let Err(error) = msg_tx
                        .send(WorkerMessage::ResolveWorkloadSecretToken {
                            workload_id,
                            secret_name,
                        })
                        .await
                    {
                        tracing::warn!(code = %rockstream_types::error_code::RS_0001, error = %error, "secret rotation refresh request could not be queued");
                    }
                }
                ControlMessage::SecretTokenRejected {
                    workload_id,
                    secret_name,
                    code,
                    message,
                    retry,
                } => {
                    secret_manager_clone.reject_refresh(workload_id, &secret_name, retry);
                    tracing::warn!(
                        %code,
                        %message,
                        workload_id = %workload_id,
                        %secret_name,
                        "worker secret token refresh was rejected"
                    );
                }
                ControlMessage::Shutdown => {
                    tracing::info!("Control plane requested shutdown");
                    explicit_shutdown = true;
                    break;
                }
                _ => {}
            }
        }

        for waiter in
            crate::exchange::service::drain_security_audit_waiters(&security_audit_waiters)
        {
            let _ = waiter.send(false);
        }
        if let Some(waiter) = frontier_report_waiter_clone.lock().take() {
            let _ = waiter.send(Err("Control channel closed".to_owned()));
        }
        let _ = shutdown_tx.send(true);
        if explicit_shutdown || deployments_clone.read().is_empty() {
            if let Some(shutdown) = &_exchange_shutdown {
                shutdown.0.cancel();
            }
            topology_workers.write().clear();
            shard_leases.write().clear();
            actor_registry_clone.shutdown();
            exchange_service.shutdown().await;
            shuffle_mux.shutdown().await;
            let dbs_to_close: Vec<ShardDb> = {
                let mut shards = active_shards_clone.write();
                shards.drain().filter_map(|(_, state)| state.db).collect()
            };
            for db in dbs_to_close {
                let _ = db.close().await;
            }
        } else {
            // Issue #400 R4: Control-node restart / outage tolerance.
            // Keep `_exchange_shutdown`, `exchange_service`, `actor_registry`,
            // and active shard leases alive until this task is aborted.
            tracing::warn!(
                "control channel closed while deployments are active; keeping direct data-plane exchange alive"
            );
            std::future::pending::<()>().await;
        }
    });

    Ok((handle, join_handle))
}

fn shared_shard_store_id() -> Option<[u8; 32]> {
    let endpoint = std::env::var("ROCKSTREAM_OBJECT_STORE_ENDPOINT").ok()?;
    let bucket = std::env::var("ROCKSTREAM_OBJECT_STORE_BUCKET").ok()?;
    let region =
        std::env::var("ROCKSTREAM_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let mut digest = Sha256::new();
    for value in [endpoint, bucket, region] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    Some(digest.finalize().into())
}
