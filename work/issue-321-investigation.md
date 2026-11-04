# Issue 321 investigation (in progress)

Base revision: `febf9c7c` (clean checkout). The initial reproductions ran with
only the failure injection and observation tests; no repair had been applied. The SQL test
uses a local pgwire gateway and a SlateDB shard backed by LocalFileSystem.

The injection rejects the first batch containing a view-output key, after the
base transaction batch succeeds. With a compiled projection over `t`, COMMIT
returned `CommandComplete(1)`, and the frontier advanced from 1 to 2. The
table contained `[(1, 10), (2, 20)]`, while the view contained `[(1, 10)]`.
After reopening the shard and compiled view, the same discrepancy remained.
After inserting `(3, 30)`, the table contained `[(1, 10), (2, 20), (3, 30)]`
and the view contained `[(1, 10), (3, 30)]`. The regression deliberately
fails on this observation. The fault was injected before any production repair.

The existing failed-base-write test also exposed an allocated epoch as the
frontier: it advanced from 1 to 2 despite the rejected batch. Separating
allocation from publication makes that focused test pass. This alone does not
repair the view-output failure above; the base batch still precedes refresh.

DISTINCT checkpoint recovery also resurrected a removed row: after a persisted
insert and persisted delete, reloading suppressed the next insert of that row.
The checkpoint now records point deletions and supports in-place restore;
filesystem tests cover failed-write retry and deletions made after a snapshot.
Other operator families and the atomic commit path still require review.

An aggregate restored under a one-group memory limit retained dirty keys from
the abandoned attempt. Its next checkpoint deleted an evicted but committed
group. In-place restoration now clears those keys after a successful reload;
the regression compares all stored records before and after checkpointing.

Window rollback tests found session restoration retained uncommitted rows.
Tumble and session restores now replace the old state after a successful read.
Session checkpointing also omitted deleted rows without deleting their old
records; point deletions, failed-write retry, and acknowledgement after a
snapshot are covered by filesystem tests. Input frontier persistence and the
remaining operator families still need examination before using restoration
as a general transaction rollback mechanism.

The direct gRPC closed-inlet observation received exactly one ACK with fields
`(exchange_id, src_shard, target_shard, epoch, seq, credit_grant) =
(100, 0, 1, 1, 1, 2)` and then end-of-stream. Healthy direct delivery and the
existing filesystem-backed committed-frontier replay test passed. These do
not establish recovery of a rejected frame by an owning epoch. The multiplexer
returned `Ok(())` for a closed-inlet frame and released its in-flight credits.
After reopening the inlet, no frame arrived during a 100 ms observation;
explicit replay delivered exactly `[(1, 10), (2, 20)]`. This manual replay is
not an owning recovery path. A repository
search found `WorkerStreamMultiplexer::send_frame` callers in tests,
simulations and benchmarks, but no production caller; the owning recovery
observation remains inconclusive.

Management authentication remains owned by open issue #303. No authenticated
release-process path was qualified in this investigation.
