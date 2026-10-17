# Rolling-upgrade qualification

The canonical scenario is `rolling-upgrade-kafka-minio-v1`, defined by
[`rolling-upgrade-kafka-minio-v1.json`](rolling-upgrade-kafka-minio-v1.json).
It reuses #126's immutable workload, typed weighted output oracle, Kafka envelope
contract, production worker client, compiled view, M3 source checkpoint, MinIO
storage, transactional exporter, and independent view/Kafka readers. A reset
retraction between repetitions keeps every checkpoint relation independently
specified. The original recovery manifest's SHA-256 is part of the evidence.

Three separate worker processes initially run the old image. One owns the shard;
two maintain live production worker sessions ready for control-plane assignment.
The runner replaces workers 1, 2, and 3 sequentially with candidate workers 4, 5,
and 6. Actual control-plane placement determines ownership; the runner does not
assign fabricated ownership or epoch vectors. Each replacement starts a six-record
insert/update/delete phase. Two broker acknowledgements precede termination; four
more arrive while the old process is absent and before the candidate registers.
The producer pauses briefly at that boundary so the runner can query and compare
the exact intermediate relation while a worker is down. This is a bounded fixed
workload, not an uninterrupted load generator or a query latency benchmark.

All eleven checkpoints compare the complete typed weighted view history,
materialized relation, independently opened `HotOnlyViewReader` result, consumed
`read_committed` sink keys/envelopes, reconstructed sink relation, source offsets,
sink transaction cursor, observed source schema, and actual fenced control lease.
Acknowledgements are Kafka producer delivery results, recorded with exact payload,
partition, offset, and receipt time. Application acceptance is established
separately by the matching durable source checkpoint and complete view/sink effects.
After all replacements, another workload phase must succeed. Every worker is then
terminated, topology and ownership must empty, and a new candidate process with a
fresh local directory reopens the durable state and accepts another phase.
Write checkpoints have a 60-second deadline from the final operation's broker
acknowledgement. Cold reopen has a separate 60-second deadline from the recorded
launch attempt after all prior workers and leases are gone. The validator checks
that this start follows the post-upgrade query and precedes the fresh registration,
and that post-reopen writes are acknowledged after the successful reopen query.

The second candidate launch injects exit 42 **before registration or state access**.
It must leave no worker registration, readiness artifact, or state directory.
Survivors must finish the entire acknowledged phase, and the same candidate is
retried forward. Downgrade is unsupported by this qualification: the runner rejects
candidate-to-old transitions before launch. This guard does not claim that the
product detects arbitrary on-disk downgrades. The test uses abrupt process
replacement and does not qualify the graceful drain sequence, control/gateway
upgrades, standalone CLI transport, multiple active shards, zero query downtime,
or a general cross-version compatibility range.

## Images and execution

Use Linux Docker host networking (the scheduled Ubuntu runner supplies it).
`ROCKSTREAM_ROLLING_N_IMAGE` and `ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE` must be distinct
`sha256:<64 hex>` image IDs or `repository@sha256:<64 hex>` references. Tags,
equal digest suffixes under different repositories, identical resolved image IDs,
missing images, and identical source revisions fail. Images must already be
available locally; explicitly pull remote digest references before running.
There is no fallback image or Docker/image skip.

Build `deploy/qualification/Dockerfile` against each exact source revision, with
`SOURCE_REVISION` and `SOURCE_VERSION` build arguments. The old revision must
contain #126's production APIs (baseline
`996b2c56258c1ad3c3deb30f339c5615dfdbed83`). Overlay only
`crates/rockstream-sim/examples/upgrade_worker.rs` and
`crates/rockstream-sim/tests/common/recovery.rs` from the candidate onto the old
tree, as the workflow does. The shared adapter compiles against each tree's own
production crates; it is never a candidate executable mounted into the old
image. Production code and the old lockfile are not overlaid. Source revision,
package version, fixture protocol, resolved image ID, container ID, and actual
worker registration are recorded and checked. Package versions may coincide;
the distinct source revisions identify the exact versions being qualified.

```sh
ROCKSTREAM_ROLLING_N_IMAGE=sha256:<old-image-id> \
ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE=sha256:<candidate-image-id> \
ROCKSTREAM_UPGRADE_ARTIFACT_DIR="$PWD/target/rolling-upgrade" \
  cargo test -p rockstream-sim --features simulation,docker_tests \
    --test rolling_upgrade_tests -- --exact three_worker_n_to_n_plus_1_zero_loss_tc
```

The `release-docker` job in `test-accounting-expensive.yml` runs on the daily
schedule, version tags, and explicit release dispatches. It builds both images
from exact source revisions, pins their resolved image IDs, and runs without
retries. Override the old immutable revision using repository variable
`ROCKSTREAM_ROLLING_N_REVISION`. A failed image build or unavailable prerequisite
fails qualification rather than accepting startup as proof.

## Exact artifacts

The artifact directory contains `images.json`, both built image IDs, each worker's
compiled identity/process manifest and log, eleven `checkpoint-*.json` files,
`acknowledged-operations.jsonl`, the failed-launch log, and the complete
`rolling-upgrade-kafka-minio-v1.json` transcript. `validation.json` records the
success and the exact expected/actual errors of four independent negative controls:
corrupted view value, suppressed source progress, changed schema, and duplicate
sink record. Their full corrupted observation artifacts are saved too. Unit
controls additionally reject lost sink records, invalid ownership, reordered
replacements, altered history/identity, missed deadlines, and broken reopen output.
Qualification consumers use `rolling_upgrade::validate` against the scenario ID,
both manifest hashes, and fixed source/image identities. Successful startup alone
cannot qualify a run. Partial checkpoints and worker logs remain available on
failure; no actual run is inferred from validator unit fixtures.
