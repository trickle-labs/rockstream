//! Worker client daemon for RockStream.
//!
//! Manages registration, periodic heartbeats, shard lease assignment,
//! and fencing validation with the control plane over TCP.

use std::collections::HashMap;
use std::io;
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
use tokio_util::task::AbortOnDropHandle;

use rockstream_types::data_plane::{
    DeploymentDescriptor, RuntimeExchangeMessage, RuntimeOutputDelta, RuntimeRow,
    DEPLOYMENT_DESCRIPTOR_VERSION,
};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLocation,
    WorkerMessage, WorkerRegistration,
};

use crate::epoch_compaction::{EpochCompactor, EpochCompactionConfig};
use crate::secrets::WorkerSecretManager;
use crate::shard_actor::{FrameExecutor, ShardActorRegistry};
use rockstream_ops::PhysicalCommitGroup;
use rockstream_storage::{ShardDb, WriteBatch};

pub struct WorkerDeployment {
    pub descriptor: DeploymentDescriptor,
    pub schemas: HashMap<String, SchemaRef>,
    pub db: Arc<ShardDb>,
    pub compiled: rockstream_ops::compile::CompiledView,
    pub commit_group: Arc<PhysicalCommitGroup>,
    pub compactor: Arc<EpochCompactor>,
}

impl WorkerDeployment {
    pub fn compactor(&self) -> &Arc<EpochCompactor> {
        &self.compactor
    }
}

pub type WorkerDeployments = Arc<RwLock<HashMap<(WorkloadId, ShardId), Arc<WorkerDeployment>>>>;

fn deployment_schema(descriptor: &DeploymentDescriptor) -> io::Result<HashMap<String, SchemaRef>> {
    descriptor
        .schemas
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
                            ))
                        }
                    };
                    Ok(Field::new(&column.name, data_type, true))
                })
                .collect::<io::Result<Vec<_>>>()?;
            Ok((schema.relation.clone(), Arc::new(Schema::new(fields))))
        })
        .collect()
}

fn rows_to_zset(
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

fn zset_to_rows(zset: &rockstream_ops::zset::ArrowZSet) -> io::Result<Vec<RuntimeRow>> {
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
                            ))
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
    let descriptor = &deployment.descriptor;
    if frame.version != DEPLOYMENT_DESCRIPTOR_VERSION
        || frame.shard_id != descriptor.shard.shard_id
        || frame.operator_id != descriptor.sink_operator_id
        || frame.lease_token != descriptor.shard.lease_token
        || frame.epoch < descriptor.frontier
        || client.worker_id() != Some(descriptor.shard.worker_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "stale or mismatched execution identity",
        ));
    }
    if !client
        .check_fence_write(frame.shard_id, frame.lease_token)
        .await?
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "lease fence rejected execution",
        ));
    }
    let schema = deployment
        .schemas
        .get(&frame.source)
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown source relation"))?;
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
    if compacted_input_rows.is_empty() {
        rockstream_types::metrics::add_r1_worker_rows(
            worker_id,
            frame.rows.len() as u64,
            0,
        );
        client
            .msg_tx
            .send(WorkerMessage::ExecutionProgress {
                output: RuntimeOutputDelta {
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
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "client channel closed"))?;
        return Ok(());
    }

    let input = rows_to_zset(&compacted_input_rows, schema)?;
    let output = rockstream_types::metrics::with_r1_execution_context(
        rockstream_types::metrics::R1ExecutionContext {
            worker_id,
            workload_id: frame.workload_id,
            shard_id: frame.shard_id,
        },
        || -> io::Result<_> {
            if let Some(join) = &deployment.compiled.join {
                let empty = |schema: SchemaRef| rows_to_zset(&[], schema);
                if frame.source == join.left_source {
                    join.pipeline
                        .process(
                            input,
                            empty(deployment.schemas[&join.right_source].clone())?,
                        )
                        .map_err(io::Error::other)
                } else if frame.source == join.right_source {
                    join.pipeline
                        .process(empty(deployment.schemas[&join.left_source].clone())?, input)
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
    if !client
        .check_fence_write(frame.shard_id, frame.lease_token)
        .await?
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "lease fence rejected persistence",
        ));
    }
    let raw_output_rows = zset_to_rows(&output)?;
    let output_rows = deployment.compactor.compact_slice(&raw_output_rows);
    let output_for_sink = if output_rows.len() != raw_output_rows.len() {
        rows_to_zset(&output_rows, output.data.schema())?
    } else {
        output
    };
    let mut writes = WriteBatch::new();
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
    rockstream_types::metrics::add_r1_worker_rows(
        worker_id,
        frame.rows.len() as u64,
        output_rows.len() as u64,
    );
    client
        .msg_tx
        .send(WorkerMessage::ExecutionProgress {
            output: RuntimeOutputDelta {
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
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "client channel closed"))
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
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let secret_manager = Arc::new(WorkerSecretManager::new("worker-42".to_string()));
    let storage_context = Arc::new(
        rockstream_storage::storage_context::WorkerStorageContext::new_with_worker_id(
            "worker-42",
            536_870_912,
        ),
    );
    let deployments = Arc::new(RwLock::new(HashMap::new()));
    let comp_config_lock = Arc::new(RwLock::new(compaction_config.clone()));

    let client = WorkerClientHandle {
        worker_id,
        active_shards,
        topology_workers,
        msg_tx,
        fence_waiters: fence_waiters.clone(),
        secret_manager,
        storage_context: storage_context.clone(),
        compaction_config: comp_config_lock,
        deployments: deployments.clone(),
    };

    let store =
        rockstream_storage::build_runtime_object_store(storage_dir, "test_root").unwrap();
    let db = Arc::new(
        ShardDb::builder("db", store)
            .with_storage_context(storage_context)
            .build()
            .await
            .unwrap(),
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

    let commit_group = Arc::new(PhysicalCommitGroup::new(db.clone()));
    let compactor = Arc::new(EpochCompactor::new(compaction_config));

    let deployment = Arc::new(WorkerDeployment {
        descriptor: descriptor.clone(),
        schemas,
        db: db.clone(),
        compiled,
        commit_group,
        compactor: compactor.clone(),
    });

    deployments
        .write()
        .insert((descriptor.workload_id, descriptor.shard.shard_id), deployment);

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
                WorkerMessage::ExecutionProgress { .. } => {
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
    use super::*;
    use rockstream_types::ids::OperatorId;

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
    async fn test_zero_weight_omission_prior_to_pipeline_and_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Frame with offsetting updates (+1 and -1) on key "10\talice"
        // and a non-zero row (+1) on key "20\tbob"
        let frame = RuntimeExchangeMessage {
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

        execute_frame(&client, &deployments, frame).await.unwrap();

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
            vec![
                ColumnValue::Int64(20),
                ColumnValue::Utf8("bob".to_string()),
            ]
        );
        assert_eq!(stored[0].3, 1); // weight 1
    }

    #[tokio::test]
    async fn test_all_rows_net_zero_omits_pipeline_and_storage_flush() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Frame where all rows offset to net-zero (+1 and -1 on same key)
        let frame = RuntimeExchangeMessage {
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

        execute_frame(&client, &deployments, frame).await.unwrap();

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

        // 2. Check compaction metrics: cancelled = 1, emitted = 0
        let metrics = compactor.metrics_snapshot();
        assert_eq!(metrics.input_updates, 2);
        assert_eq!(metrics.cancelled_zero_weight_updates, 1);
        assert_eq!(metrics.emitted_updates, 0);

        // 3. Check storage: no rows written at all!
        use rockstream_ops::sink::read_view_output;
        let stored = read_view_output(&db, OperatorId(10), 2).await.unwrap();
        assert!(
            stored.is_empty(),
            "storage flush must be omitted when all rows offset to net-zero"
        );
    }

    #[tokio::test]
    async fn test_collapsing_multiple_updates_into_net_weight() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client, deployments, db, compactor, mut progress_rx) =
            setup_test_deployment(temp_dir.path(), EpochCompactionConfig::default()).await;

        // Multiple updates on key 42: +3 and -1 -> net weight +2
        let frame = RuntimeExchangeMessage {
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
        client.set_compaction_config(new_config.clone());
        assert_eq!(client.compaction_config().window_duration, Duration::from_millis(150));

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
    worker_id: Arc<RwLock<Option<WorkerId>>>,
    active_shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    topology_workers: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
    msg_tx: mpsc::Sender<WorkerMessage>,
    fence_waiters:
        Arc<parking_lot::Mutex<HashMap<ShardId, Vec<tokio::sync::oneshot::Sender<bool>>>>>,
    secret_manager: Arc<WorkerSecretManager>,
    storage_context: Arc<rockstream_storage::storage_context::WorkerStorageContext>,
    compaction_config: Arc<RwLock<EpochCompactionConfig>>,
    deployments: WorkerDeployments,
}

impl WorkerClientHandle {
    /// Returns the worker-wide epoch compaction config.
    pub fn compaction_config(&self) -> EpochCompactionConfig {
        self.compaction_config.read().clone()
    }

    /// Set the worker-wide epoch compaction config.
    pub fn set_compaction_config(&self, config: EpochCompactionConfig) {
        *self.compaction_config.write() = config;
    }

    /// Get active deployment for a workload and shard.
    pub fn get_deployment(
        &self,
        workload_id: WorkloadId,
        shard_id: ShardId,
    ) -> Option<Arc<WorkerDeployment>> {
        self.deployments.read().get(&(workload_id, shard_id)).cloned()
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

    /// Request a fresh short-lived token over the authenticated control channel.
    pub async fn request_secret_token(
        &self,
        secret_name: impl Into<String>,
    ) -> Result<(), io::Error> {
        self.msg_tx
            .send(WorkerMessage::ResolveSecretToken {
                secret_name: secret_name.into(),
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "Client channel closed"))
    }

    /// Read a decrypted credential from memory. No storage path is consulted.
    pub fn secret(
        &self,
        secret_name: &str,
        now_secs: u64,
    ) -> Option<crate::secrets::ResolvedSecret> {
        self.secret_manager.get(secret_name, now_secs)
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
    let initial_headroom = system_memory_headroom()?;
    let clean_url = control_url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let stream = TcpStream::connect(clean_url).await?;

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
            reader,
            writer,
        )
        .await
    }
}

#[cfg(target_os = "linux")]
fn system_memory_headroom() -> io::Result<CapacityHeadroom> {
    // ponytail: host-wide free pages ignore container quotas; use cgroup limits if constrained workers become supported.
    let total_pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let available_pages = unsafe { libc::sysconf(libc::_SC_AVPHYS_PAGES) };
    if total_pages <= 0 || available_pages < 0 {
        return Err(io::Error::other("could not read physical memory headroom"));
    }
    Ok(CapacityHeadroom::new(
        available_pages as f64 / total_pages as f64,
    ))
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
    let (msg_tx, mut msg_rx) = mpsc::channel::<WorkerMessage>(32);
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
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

    let handle = WorkerClientHandle {
        worker_id: worker_id.clone(),
        active_shards: active_shards.clone(),
        topology_workers: topology_workers.clone(),
        msg_tx: msg_tx.clone(),
        fence_waiters: fence_waiters.clone(),
        secret_manager: secret_manager.clone(),
        storage_context: storage_context.clone(),
        compaction_config: compaction_config.clone(),
        deployments: deployments.clone(),
    };

    let actor_registry = ShardActorRegistry::new();
    let executor_client = handle.clone();
    let executor_deployments = deployments.clone();
    let execute: FrameExecutor = Arc::new(move |frame| {
        let client = executor_client.clone();
        let deployments = executor_deployments.clone();
        Box::pin(async move {
            if let Err(error) = execute_frame(&client, &deployments, frame).await {
                tracing::warn!(
                    code = %rockstream_types::error_code::RS_0001,
                    %error,
                    "worker execution refused"
                );
            }
        })
    });

    let worker_id_clone = worker_id.clone();
    let active_shards_clone = active_shards.clone();
    let fence_waiters_clone = fence_waiters.clone();
    let storage_dir = storage_dir.to_path_buf();
    let secret_manager_clone = secret_manager.clone();
    let deployments_clone = deployments.clone();
    let actor_registry_clone = actor_registry.clone();
    let execute_clone = execute.clone();
    let storage_context_clone = storage_context.clone();
    let compaction_config_clone = compaction_config.clone();

    let join_handle = tokio::spawn(async move {
        // 1. Send Registration message.
        let reg = WorkerRegistration::new(
            WorkerId(proposed_worker_id),
            NodeRole::Worker,
            "127.0.0.1:0", // Default loopback
            initial_headroom,
        )
        .with_location(location.clone())
        .with_capabilities(capabilities)
        .with_compatibility(
            rockstream_types::compatibility::SupportedVersionRange::v1_through_v2(),
            rockstream_types::compatibility::SupportedStorageFormatRange::v1_through_v2(),
        );
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
                }
                sleep(Duration::from_millis(500)).await;
            }
        }));

        // 4. Read Loop: process ControlMessage commands from control plane.
        let mut lines = BufReader::new(reader).lines();
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
                    for db in databases {
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
                ControlMessage::Registered { worker_id: wid } => {
                    tracing::info!("Worker client registered successfully as {:?}", wid);
                    *worker_id_clone.write() = Some(wid);
                }
                ControlMessage::TopologyChanged { workers } => {
                    let mut topology = topology_workers.write();
                    topology.clear();
                    topology.extend(workers.into_iter().map(|worker| (worker.worker_id, worker)));
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
                            &descriptor.storage_root,
                        )
                        .map_err(io::Error::other)?;
                        let db = Arc::new(
                            ShardDb::builder("db", store)
                                .with_storage_context(storage_context_clone.clone())
                                .build()
                                .await
                                .map_err(io::Error::other)?,
                        );
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
                            comp_cfg = comp_cfg
                                .with_primary_key_columns(descriptor.primary_key.clone());
                        }
                        let compactor = Arc::new(EpochCompactor::new(comp_cfg));
                        Ok(Arc::new(WorkerDeployment {
                            descriptor,
                            schemas,
                            commit_group: Arc::new(PhysicalCommitGroup::new(db.clone())),
                            db,
                            compiled,
                            compactor,
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
                    if let Err(error) = actor_registry_clone.enqueue(frame) {
                        tracing::warn!(
                            code = %rockstream_types::error_code::RS_0001,
                            %error,
                            "worker execution frame refused"
                        );
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

                    // Attempt to open the ShardDb
                    let mut builder = ShardDb::builder("db", store)
                        .with_supported_format_range(
                            rockstream_types::compatibility::SupportedStorageFormatRange::v1_through_v2(
                            ),
                        )
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
                            actor_registry_clone.register(
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
                        actor_registry_clone.revoke(lease.shard_id);
                        let mut databases = deployments_clone
                            .read()
                            .values()
                            .filter(|deployment| {
                                deployment.descriptor.shard.shard_id == lease.shard_id
                            })
                            .map(|deployment| deployment.db.as_ref().clone())
                            .collect::<Vec<_>>();
                        if let Some(db) = active_shards_clone
                            .read()
                            .get(&lease.shard_id)
                            .and_then(|state| state.db.clone())
                        {
                            databases.push(db);
                        }
                        let mut failure = None;
                        for db in databases {
                            if let Err(error) = db.flush().await {
                                failure = Some(format!("shard flush failed: {error}"));
                                break;
                            }
                            if let Err(error) = db.close().await {
                                failure = Some(format!("shard close failed: {error}"));
                                break;
                            }
                        }
                        if let Some(error) = failure {
                            Err(error)
                        } else {
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
                    };
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
                        if deployments_clone
                            .read()
                            .keys()
                            .any(|(_, shard_id)| *shard_id == lease.shard_id)
                        {
                            return Err(
                                "shard has an active workload; backup requires an idle shard"
                                    .to_owned(),
                            );
                        }
                        let db = active_shards_clone
                            .read()
                            .get(&lease.shard_id)
                            .filter(|state| state.lease == lease)
                            .and_then(|state| state.db.clone())
                            .ok_or_else(|| {
                                "worker does not hold the requested active shard lease".to_owned()
                            })?;
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
                ControlMessage::SecretRotated { rotation } => {
                    if let Err(error) = msg_tx
                        .send(WorkerMessage::ResolveSecretToken {
                            secret_name: rotation.secret_name,
                        })
                        .await
                    {
                        tracing::warn!(code = %rockstream_types::error_code::RS_0001, error = %error, "secret rotation refresh request could not be queued");
                    }
                }
                ControlMessage::Shutdown => {
                    tracing::info!("Control plane requested shutdown");
                    break;
                }
                _ => {}
            }
        }

        // Clean up remaining active shards on shutdown/disconnect
        let dbs_to_close: Vec<ShardDb> = {
            let mut shards = active_shards_clone.write();
            shards.drain().filter_map(|(_, state)| state.db).collect()
        };
        for db in dbs_to_close {
            let _ = db.close().await;
        }
        actor_registry_clone.shutdown();

        let _ = shutdown_tx.send(true);
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
