# Worker-failure recovery evidence

Run the fixed scenario with Docker available:

```sh
ROCKSTREAM_CHAOS_ARTIFACT_DIR="$PWD/target/real-cluster-chaos" \
  cargo test -p rockstream-sim --features simulation,docker_tests \
  --test real_cluster_chaos_soak_tests \
  worker_failure_recovery_from_view_and_kafka -- --exact
```

The canonical scenario identity is `worker-failure-kafka-minio-v1`. Its immutable
workload and independent expected results are in
[`worker-failure-kafka-minio-v1.json`](worker-failure-kafka-minio-v1.json).
Changing its history, fault boundary, expected rows, or deadlines requires a new
scenario version. Qualification consumers use the scenario ID, manifest SHA-256,
and source Git revision together, and deserialize `WorkerRecoveryEvidence` through
`rockstream_sim::qualification::worker_failure::validate`.

The runner starts the production control service and two separate worker fixture
processes. Each fixture uses the production worker lease client, Kafka source,
compiled view pipeline, checkpoint-coupled M3 commit, MinIO-backed ShardDb, and
transactional Kafka view exporter. The fixture supplies scheduling around those
APIs; this is not a test of the standalone CLI's gateway-to-worker transport.
The first worker stops at a durable pending Kafka transaction after the update
retraction, with source records still outstanding, and the parent kills it.
The control service detects the actual dropped connection and assigns a higher
lease token to worker 2. That worker recovers the source checkpoint and pending
sink transaction, then completes the fixed history.

The parent queries through the production `HotOnlyViewReader` against a separate
ShardReader at the stored frontier, and consumes Kafka with `read_committed` to
the partition end. The validator compares the entire typed weighted view log,
materialized relation, sink keys and envelopes, independently reconstructed sink
relation, source offsets, ownership, runtime fill levels, and durable sink cursor.
The source in-flight metric and sink pending state are observed, not assigned
buffer literals. No storage-brownout buffer-exhaustion claim is made.

The artifact directory contains `worker-failure-kafka-minio-v1.json` (actual
observations), `real-cluster-chaos-summary.md`, and both worker logs. Event times
measure fault to control disconnect detection, replacement ownership observation,
and the first correct view query at frontier 5. Docker's `info` command and worker
kill/wait durations are reported separately. A run is one recovery sample and
does not estimate p99. Corrupting a returned value, duplicating a consumed record,
suppressing frontier progress, and missing the query deadline are independent
negative controls against the same validator.

This replaces the former copied-input multi-category chaos result. Other failure
categories retain their simulation coverage and explicitly lack this real-backend
evidence; this scenario must not qualify them.

The companion [`worker-source-ack-gap-recovery.md`](worker-source-ack-gap-recovery.md)
scenario uses a separately versioned history to stop after its M3 commit but
before the Kafka source acknowledgment. Together, the two scenarios cover the
source acknowledgment and pending sink transaction boundaries for Kafka to
Kafka recovery; they do not qualify PostgreSQL CDC or other connector types.

The separately versioned `worker-sink-cursor-gap-kafka-minio-v1` scenario
targets the later sink boundary: it kills worker 1 after Kafka transaction 2
commits but before the MinIO-backed `KafkaViewSink` cursor clears its pending
state. Worker 2 recovers and must produce the exact five-record sink stream,
source offsets, weighted view, materialized relation, and query rows. Its
one-shard forced replacement does not claim network partition or consumer-group
rebalance coverage. The test writes a distinct artifact directory with fault,
before/after observations, worker logs, versions, replay seed, and SHA-256
digests.

The separately versioned `worker-state-enospc-kafka-minio-v1` scenario fills an
isolated 64 MiB MinIO `/data` tmpfs until the OS reports ENOSPC while worker 1
holds a two-record Kafka epoch before `SourceCheckpointStore::prepare`. It
requires MinIO to reject the worker WAL write and checks that the source group
offset, committed view, frontier, Kafka sink stream, and durable sink cursor
remain at the prior epoch. The storage future may remain pending while the
backend is full; the test kills worker 1, restores capacity, then requires
worker 2 to replay to the complete independent Kafka and MinIO oracle.
Artifacts retain the ENOSPC output, `df` state, outcome or pending-write proof,
both worker logs, MinIO logs, versions, and digests. This qualifies the tested
MinIO-backed persistence path only; it is not a local filesystem worker test,
PostgreSQL CDC test, or host power-loss test.

For qualification, run:

```sh
bash scripts/run-release-qualification.sh --suite recovery \
  --output target/real-cluster-chaos/qualification.json
```

This executes the scenario, binds its observations to the runner's complete
candidate identity, derives durations from the captured events, and embeds both
the successful record and six deliberately corrupted records. A fresh run
directory prevents reuse of stale proof. The first query is still fetched by
the separate production view reader above; the qualification layer never fills
it from the expected relation. The runner checks every negative record and its
intended failure reason exactly. A successful manifest qualifies only this
single-sample worker-failure scenario; the remaining unsupported claims are
listed explicitly. The fast observer/topology/oracle tests remain helper evidence.
