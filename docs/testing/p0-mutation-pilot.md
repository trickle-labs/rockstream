# P0 mutation pilot evidence

Baseline: `2bb60012318fa966e1dc98866122daceabc59607` (2026-10-27). The machine-readable inventory is [p0-mutation-inventory.json](p0-mutation-inventory.json).

## Results

| Scenario | Unchanged detector | Mutant result |
| --- | --- | --- |
| Group commit reports an unknown write/flush outcome as durable | `unknown_flush_keeps_later_epoch_queued_until_reconciliation` passed (1/1) | The detector failed at the intended ACK assertion: `unwrap_err()` received `Ok([1])`. |
| Catalog recovery omits committed operation identities from a snapshot | `test_snapshot_retry_preserves_deleted_table_and_objects` passed (1/1) | The detector failed at the intended replay assertion: revision 1 and the deleted table reappeared instead of revision 2 with no tables. |

Both changes were applied only as isolated experiments and then reverted. The unchanged tests passed again afterward. The ACK regression now checks the exact storage error string, so an incorrect success reports the returned epoch in the failure output.

## Evidence limits

The group-commit case uses a real local filesystem-backed `ShardDb`, but injects the flush error before the backend flush. It proves that this failure cannot be acknowledged as durable and checks retained work and stored values through retry; it does not simulate power loss after ACK.

The catalog case calls the production recovery path twice and checks replay idempotence plus the stored object inventory. Its object store is `object_store::memory::InMemory`; the mutant fails on resurrected metadata before reaching the inventory assertion. It does not qualify recovery on a physical object store.

These two executions are partial P0 evidence, not mutation qualification. No recurring runner, schedule, cost bound, static/random comparison, or LLM-selection comparison was evaluated. No PR gate changed.

## Reproduction

```sh
rtk cargo test -p rockstream-ops unknown_flush_keeps_later_epoch_queued_until_reconciliation --lib
rtk cargo test -p rockstream-storage --test catalog_storage_tests test_snapshot_retry_preserves_deleted_table_and_objects
```
