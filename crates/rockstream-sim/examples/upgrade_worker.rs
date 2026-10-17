//! Image entry point compiled against each release's production crates.
use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    KafkaSink, KafkaSource, OffsetToken, SourceCheckpointStore, SourceConnector,
    SourceRuntimeCoordinator,
};
use rockstream_ops::{
    compile_plan_with_sink_id, kafka_sink::KafkaViewSink, sink::write_view_directory_entry,
    ArrowZSet,
};
use rockstream_plan::PlanNode;
use rockstream_runtime::start_worker_client;
use rockstream_storage::{keys::ShardKeyEncoder, WriteBatch};
use rockstream_types::{
    arrow_batch::split_weight_column,
    ids::{ConnectorId, OperatorId, ShardId},
};
use serde_json::json;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[allow(dead_code)]
#[path = "../tests/common/recovery.rs"]
mod recovery;
use recovery::{now_ms, observe, save};

const ID: u64 = 126;

#[tokio::main]
async fn main() {
    // Defined failure boundary: before registration, leases, or state access.
    if std::env::var_os("ROCKSTREAM_UPGRADE_FAIL_START").is_some() {
        eprintln!("injected replacement failure before registration");
        std::process::exit(42);
    }
    let root = std::env::var("ROCKSTREAM_RECOVERY_WORKER_ROOT").unwrap();
    let root = Path::new(&root);
    let id: u64 = std::env::var("ROCKSTREAM_RECOVERY_WORKER_ID")
        .unwrap()
        .parse()
        .unwrap();
    let control = std::env::var("ROCKSTREAM_RECOVERY_CONTROL").unwrap();
    let bootstrap = std::env::var("ROCKSTREAM_RECOVERY_KAFKA").unwrap();
    let (worker, _task) = start_worker_client(id, &control, &root.join(format!("worker-{id}")))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while worker.worker_id().is_none() {
        assert!(Instant::now() < deadline, "registration deadline");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    save(
        &root.join(format!("worker-{id}-ready.json")),
        &json!({
            "worker_id": worker.worker_id().unwrap().0,
            "registered_at_ms": now_ms(),
            "revision": option_env!("ROCKSTREAM_QUALIFICATION_REVISION").unwrap_or("unqualified"),
            "version": env!("CARGO_PKG_VERSION"),
            "protocol": "rolling-upgrade-kafka-minio-v1"
        }),
    );
    if id == 1 || id == 7 {
        worker.request_shard(ShardId(ID)).await.unwrap();
    }
    // Standbys keep a real production worker session and accept reassignment.
    let db = loop {
        if let Some(db) = worker.get_shard_db(ShardId(ID)) {
            break Arc::new(db);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let source = KafkaSource::connect(
        ConnectorId(ID),
        schema.clone(),
        &bootstrap,
        "worker-failure-input",
        "worker-failure-source",
    )
    .unwrap();
    let observed_schema = json!(source
        .discover_schema()
        .unwrap()
        .fields()
        .iter()
        .map(|f| (f.name(), f.data_type().to_string(), f.is_nullable()))
        .collect::<Vec<_>>());
    let mut source = SourceRuntimeCoordinator::new(
        source,
        ConnectorId(ID),
        OffsetToken::new(Vec::new()),
        SourceCheckpointStore::new(db.clone(), ID as u128, ConnectorId(ID)),
    );
    source.recover().await.unwrap();
    let owner = source.acquire_owner(format!("worker-{id}")).unwrap();
    let plan = PlanNode::ViewSink {
        view_name: "letters".into(),
        pk: vec![],
        child: Box::new(PlanNode::Source {
            name: "letters_source".into(),
        }),
    };
    let compiled = compile_plan_with_sink_id(
        &plan,
        db.clone(),
        &HashMap::from([("letters_source".into(), schema)]),
        OperatorId(ID),
    )
    .unwrap();
    compiled.pipeline.restore(&db).await.unwrap();
    write_view_directory_entry(&db, "letters", OperatorId(ID), 2, &[])
        .await
        .unwrap();
    let mut sink = KafkaViewSink::new(
        KafkaSink::connect(ConnectorId(ID), &bootstrap, "worker-failure-output").unwrap(),
        db.clone(),
        OperatorId(ID),
        2,
    );
    // Recover a possibly committed source epoch whose sink delivery was interrupted.
    if source.committed_epoch() > 0 {
        while sink.deliver_next().await.unwrap() {}
        save(
            &root.join(format!("worker-{id}-reopened.json")),
            &json!({
                "observation": observe(&db, &worker, &source).await,
                "schema": observed_schema
            }),
        );
    }
    loop {
        let delta = source.poll_delta(64 * 1024, 1).await.unwrap();
        if delta.batches.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        let epoch = source.next_epoch().unwrap();
        let lease = worker
            .leases()
            .into_iter()
            .find(|l| l.shard_id == ShardId(ID))
            .unwrap();
        assert!(worker
            .check_fence_write(ShardId(ID), lease.lease_token)
            .await
            .unwrap());
        let mut writes = WriteBatch::new();
        for batch in delta.batches {
            let (data, weights) = split_weight_column(&batch).unwrap();
            let output = compiled
                .pipeline
                .process(ArrowZSet::new(data, weights))
                .unwrap();
            compiled.sink.append_epoch(&mut writes, &output, epoch);
        }
        compiled
            .pipeline
            .append_state(&db, &mut writes)
            .await
            .unwrap();
        writes.put(&ShardKeyEncoder::frontier_key(), &epoch.to_be_bytes());
        source
            .commit_epoch(&owner, epoch, delta.new_offset, writes)
            .await
            .unwrap();
        assert!(sink.deliver_next().await.unwrap());
        save(
            &root.join(format!("worker-{id}-epoch-{epoch}.json")),
            &json!({
                "observation": observe(&db, &worker, &source).await,
                "schema": observed_schema
            }),
        );
    }
}
