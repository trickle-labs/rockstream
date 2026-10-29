# Stale worker ownership evidence

Run with Docker available:

```sh
ROCKSTREAM_CHAOS_ARTIFACT_DIR="$PWD/target/real-cluster-chaos/stale-owner" \
  cargo test --locked -p rockstream-sim --features simulation,docker_tests \
  --test real_cluster_chaos_soak_tests \
  stale_worker_write_and_frontier_report_are_rejected_after_reassignment -- --exact
```

The fixed history and full expected weighted output, final relation, query rows,
and transactional sink records are in
[`worker-stale-owner-kafka-minio-v1.json`](worker-stale-owner-kafka-minio-v1.json).
Evidence is written to `worker-stale-owner-kafka-minio-v1.json`, with a summary
and both worker logs in the selected artifact directory. The evidence binds the
scenario to its manifest SHA, source/control-service revision, replay seed,
pinned service images, process IDs, lease tokens, offset history, and exact
before/after observations.

Worker A prepares but does not commit source epoch 3. The test force-reassigns
the shard to worker B through `ShardManager`, then B obtains a higher token
through the worker client's normal `RequestShard` path and commits through epoch
5. Only then does A resume: the control service returns `FenceAck(false)`, the
source commit/ACK is not issued, and the real frontier-report RPC returns
`RS-8004`. The test reopens the MinIO-backed shard and checks the exact durable
frontier and query result, committed offsets for both workers, unchanged worker
state, and the complete read-committed Kafka sink output.

The test uses one shard and separate Kafka source groups so B can advance while
A remains live at the gate. It exercises control-plane forced reassignment, not
a network partition, a Kafka consumer-group rebalance, or every split-brain
schedule. The frontier claim is limited to this one-shard scenario. The
`P1-STALE-WORKER-FENCE-ALWAYS-VALID` mutation removes the control service's
lease-token check; the same real-process test must fail at its stale-fence
assertion, and the mutation runner retains the baseline, mutant, restored
baseline, scenario evidence, logs, and source diff.
