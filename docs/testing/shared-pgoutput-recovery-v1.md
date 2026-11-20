# Shared-slot PostgreSQL CDC recovery qualification (v1)

Issue: [#350](https://github.com/trickle-labs/rockstream/issues/350).
Scenario ID: **shared-pgoutput-v1**. The existing healthy two-table
attachment/slot-count/incompatible-schema helper is a *positive control*,
not evidence for any recovery scenario.

## Real execution and proof record

Run on a Docker-capable host with PostgreSQL 11; these four tests **fail** if
Docker is not available. Each starts its own isolated real PostgreSQL server
with `wal_level=logical`, uses the production pgoutput connector, one
publication, one logical slot, one local-filesystem-backed SlateDB shard and
a real pgwire gateway. The tests enforce a three-minute upper bound per case.

```sh
rtk cargo test --locked -p rockstream-gateway --test cdc_transaction_atomicity_tests pgoutput_shared_slot_ -- --nocapture --test-threads=1
```

The CI `pr-gateway` case-inventory runner executes this test target and
persists case-level evidence. Search the captured libtest output for
`SHARED_SLOT_PROOF` to find **scenario ID, exact git revision** (the
running checkout's `git rev-parse HEAD`), **actual server_version**,
**selected cut**, **fault/recovery witness**, and **expected and observed
base/view tuples and checkpoint/slot progress** at each phase. Merely
listing a test or running without Docker is not a qualified execution.

## Fixed upstream history and independent oracle

All relations have `(id BIGINT PRIMARY KEY, val BIGINT NOT NULL)` upstream
with `REPLICA IDENTITY FULL`. The imported gateway base has columns
`(id BIGINT, val BIGINT)`; each view is
`SELECT id, val * 2 AS computed FROM <relation>`. Compare *complete
ordered tuples* in both base and view (not row counts or projections
of the actual gateway rows). The expected view is calculated from the
fixed base history in the test, never by querying RockStream. Exactly
one publication (`shared_pub`) and logical slot (`shared_slot`) are
asserted. The third table is published/attached only in the disconnect case.

| Table | Initial base (id,val) | After transaction | After follow-up |
| --- | --- | --- | --- |
| a | (1,10), (2,20) | (1,15), (2,20), (3,30) | (1,15), (2,20), (3,30), (4,40) |
| b | (11,110), (12,120) | (11,110), (13,130) | (11,110), (13,130), (14,140) |
| c | (21,210), (22,220) | (21,215), (22,220), (23,230) | (21,215), (22,220), (23,230), (24,240) |

The first committed transaction updates/inserts in a, deletes/inserts
in b, and (in the three-table case) updates/inserts in c. The second
transaction inserts one new row into every attached table. Both
transactions are **one PostgreSQL COMMIT each**, expected to produce
one shared source checkpoint epoch apiece.

## Required fault/cut witnesses

| Scenario | Deterministic boundary | Mandatory observation | Recovery assertion |
| --- | --- | --- | --- |
| `restart-before-m3` | Pause before the actual shared-slot batch's view-output/M3 write; abort owning worker, **without releasing pause** | Barrier entered; source checkpoint and slot LSN exactly unchanged | Close/reopen SlateDB; new gateway/worker replays transaction once, then continues at next commit |
| `restart-after-m3-before-slot-ack` | Pause after durable M3 flush in `SourceRuntimeCoordinator::commit_replayable_epoch`, before the PostgreSQL slot ACK; abort owning worker, **without releasing pause** | Barrier entered; exactly one durable epoch advanced; slot LSN unchanged | Close/reopen SlateDB; new owner recovers exact same epoch, acknowledges slot, does not reapply rows, and continues |
| `three-table-disconnect-resume` | Terminate the *actual* named pgoutput PostgreSQL backend by PID while a three-table upstream transaction is open, then commit | `pg_terminate_backend` returns true; distinct new backend PID observed; all three tables in publication | Original gateway reconnects under the fenced owner; exact three-table results and monotone checkpoint/slot after both commits |
| `schema-change-before-commit` | Begin upstream transaction that inserts into a, drops b's PK/id, then inserts b's new shape; assert before COMMIT, then COMMIT | Before COMMIT no new imported rows/LSN; after COMMIT source reports exact `BLOCKED` / `RS-1002` | Neither base nor view changes; neither checkpoint nor slot advances. A further valid upstream a-only insert remains blocked until an explicit rebuild/resnapshot |

The schema case's **supported** outcome is fail-closed rejection of the
whole shared-slot transaction. It must not claim continued ingestion while
the source remains `BLOCKED`: repair/resnapshot is a separate user action,
not an automatic retry of an incompatible schema. The other three cases
prove continued progress with a subsequent valid transaction.

The restart cases deliberately cancel the **source task**, not merely
the TCP accept loop. They assert task exit, drop the old gateway and
**close and reopen** the durable SlateDB from the same object-store
directory. A timeout, missed barrier, unexpectedly continued old worker,
unchanged recovered checkpoint, stale view, missing third table, or
unobserved reconnect fails the test.

This qualifies the named single-shard PostgreSQL shared-slot boundaries,
not physical power loss, partition failover, MinIO, or independent
Kafka-sink durability.
