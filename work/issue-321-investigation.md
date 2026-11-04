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

On candidate `ccd2ac7b`, COPY FROM STDIN acknowledged two rows and the base
table contained `[(1, 10), (2, 20)]`, but its compiled projection was empty.
COPY batches now use the same commit path as SQL writes. The filesystem-backed
wire test checks exact rows after COPY, an injected view-output failure, a later
insert, and reopening. COPY can still flush multiple batches; this test does not
claim whole-COPY atomicity across those batches.

Older composite-key checkpoints contain only encoded key bytes and surrogate
IDs. Recovery now decodes those bytes using known compiled key types; tests
cover mixed text and numeric keys across a real gateway restart and exercise
all supported scalar encodings. Unknown or unsupported types and malformed
records remain explicit recovery errors rather than guessed values.

DISTINCT checkpoint recovery also resurrected a removed row: after a persisted
insert and persisted delete, reloading suppressed the next insert of that row.
The checkpoint now records point deletions and supports in-place restore;
filesystem tests cover failed-write retry and deletions made after a snapshot.
Other operator families and the atomic commit path still require review.

An aggregate restored under a one-group memory limit retained dirty keys from
the abandoned attempt. Its next checkpoint deleted an evicted but committed
group. In-place restoration now clears those keys after a successful reload;
the regression compares all stored records before and after checkpointing.

Work in progress: local SQL stages base rows, view output, state, and frontier
in one batch. Projection and aggregate write-failure tests now pass. A failed
state restore previously let the next aggregate commit produce 50 instead of
30; a recovery gate now rejects reads while the failed scan persists and
restores state before the next commit. This is not yet release evidence:
Compiled in-memory TopK restoration now reads checkpoint pages within its
per-partition buffer bound and reconstructs the emitted rows. Spill-backed
TopK restoration remains unavailable; recovery after cancellation
and process restart needs full-path testing, and the source-path locking changes
need their own tests. The distributed path and shuffle owning-epoch recovery
are not established by these tests.

A test-only storage pause cancels the local handler before the combined write,
after storage accepts it but before the durable-write future completes, and
after that future but before flush. The first case restores the old rows; the
latter cases block a concurrent pgwire read until recovery flushes and exposes
the complete committed rows, which also survive reopen. Recovery refreshes the
frontier from storage after the ambiguous write. This directly cancels the handler
future; the pgwire CancelRequest path remains untested.
A separate injected flush failure rejects reads until a successful recovery
flush, after which complete table and view rows survive a later commit and reopen.

A join-view write failure is followed by an exact update, reopen, and an update
to the other join input; all resulting table and view rows match expectations.
A transitive view initially backfilled no rows because its inlined pipeline was
fed from the intermediate view name instead of the base relation. Initial
backfill and local commit refresh now use the inlined base input; exact rows
are checked across an injected failure, retry, reopen, and later commit.

A local pgwire test confirms its incremental plan includes TopK, then checks
exact rows after an injected write failure, a valid transaction, reopen, and
another valid transaction. A spilled checkpoint above the in-memory limit is
rejected without replacing live state.

Analytic-window checkpoints retained old output-cache rows after a rank change,
causing stale retractions after reload. Point deletions and in-place restore now
cover that boundary, including failed writes, concurrent snapshot acknowledgement,
corrupt records, and a pgwire rollback/restart test.

MIN/MAX checkpoint scans previously ignored truncation. Recovery and stale-key
removal now scan in bounded pages; the loader rejects malformed or inconsistent
records. Page continuation seeks from the prior key rather than rescanning
earlier pages. Tests compare all records across multiple pages and exercise a failed
MIN view commit followed by recovery and reopen.

Window rollback tests found session restoration retained uncommitted rows.
Tumble and session restores now replace the old state after a successful read.
Session checkpointing also omitted deleted rows without deleting their old
records; point deletions, failed-write retry, and acknowledgement after a
snapshot are covered by filesystem tests. Tumble and session checkpoints now
retain the complete input frontier in a versioned record; malformed records
fail restoration without replacing live state. The remaining operator families
still need examination before using restoration as a general rollback mechanism.

Hopping-window recovery re-emitted already committed rows on the next update
because its output cache was not restored. Recovery now reconstructs that
cache and restores the input frontier; exact deltas are checked after both
in-place restore and reload.

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
