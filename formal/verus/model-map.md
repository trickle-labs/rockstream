# Verus model map

VS0 covers one executable planning kernel.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-verified/src/lib.rs::normalize_power_of_two_bucket_count` | Every `u16` input returns a power of two in `1..=32768`; valid powers are preserved; zero and oversized inputs follow the documented clamp. | `rockstream-plan` keeps the public wrapper and uses the verified executable function. |

VS1 covers checked scalar and merge-law adapters.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-types/src/laws/arithmetic.rs` | Checked addition, multiplication, negation, narrowing, and exact-width scalar decoding return success only for representable values and exact formats. | Law and storage callers map failure to the existing non-retryable RS error paths. |
| `crates/rockstream-types/src/laws/weight_add.rs`, `sum_count.rs` | Mathematical identities are exposed only with the checked `i64` executable domain; regrouping requires an absolute-sum admission. | Optimizers inspect `LawDescriptor::can_reassociate`; otherwise they use the spill fallback. |
| `crates/rockstream-storage/src/merge_registry.rs` | Existing tagged formats are preserved while first operands, tags, widths, and checked results are validated. | Storage never treats a malformed no-existing operand as an initial value. |

VS2 covers the selected persistent encoding and routing kernels.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-storage/src/keys.rs::{minmax_sort_key,window_sort_key}` | The shared sign-bit transform round-trips every `i64` and preserves ascending/descending lexicographic order. | Storage preserves the eight-byte big-endian field and uses the decoder for the same direction. |
| `crates/rockstream-storage/src/keys.rs::{encode,decode,CatalogKeyEncoder}` | Fixed-width u64/u128 fields round-trip; malformed widths and factor-payload lengths fail before slicing. | Generic shard and catalog scans use complete namespace prefixes; specialized variable layouts remain explicitly scoped in `key-layouts.toml`. |
| `crates/rockstream-plan/src/virtual_bucket.rs::route_power_of_two_bucket` | A valid power-of-two count, clamped prefix, and u64 hash produce a bucket below the count; invalid counts return `None`. | The planner retains the FNV-1a hash and compatibility routing behavior; no balance or collision claim is inferred. |

The selected layout inventory and allocation assumptions are recorded in
[`key-layouts.toml`](key-layouts.toml). This milestone has no protocol-state
correspondence. FizzBee remains the owner of the existing protocol models.

VS3 covers the Z-set adapter and aggregate transition boundary.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-ops/src/zset.rs::{try_new,validate,select_rows}` | Row and weight lengths align; ordered selection preserves requested order/repetition, rejects invalid indices, and carries the frontier. | Arrow row equality and decoding remain adapter responsibilities; zero filtering does not imply duplicate consolidation. |
| `crates/rockstream-verified/src/aggregate.rs::transition` | A valid absent or positive-count state plus a consolidated delta yields an exact present/deleted state or a checked rejection; zero-count nonzero-sum states are invalid. | `AggregateOp` validates/casts Arrow input and supplies valid-retraction semantics. |
| `crates/rockstream-ops/src/aggregate.rs::{StagedEpochAggregator::ingest_delta,AggregateOp::process_delta_with_result}` | Candidate arithmetic, output, and touched-key mutations are ready before authoritative state installation. | Durable write atomicity, restart, and storage outcomes remain VS5 obligations. |

VS4 covers membership-aware frontier admission and the local publication
boundary. The FizzBee M2 model remains the bounded interleaving model for
lease/failover behavior; the executable Rust transition owns configuration
generation and incarnation admission.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-verified/src/frontier.rs::admit_frontier_report` | A report is admitted only for the supported envelope version, active member, matching generation, and matching incarnation; accepted epochs are monotone. | `rockstream-control::frontier::FrontierAggregator` validates scope and authority before calling the kernel. |
| `crates/rockstream-verified/src/frontier.rs::fixed_membership_transition` | A fixed-membership report transition never lowers the admitted epoch. | The control adapter computes the meet only across the configured active set and treats missing reports as no completeness. |
| `crates/rockstream-verified/src/frontier.rs::activation_preserves_publication` | A new member's durable bootstrap is at least the currently published frontier, or no publication exists. | Membership activation/replacement supplies the durable catch-up evidence and rejects generation overflow. |
| `crates/rockstream-verified/src/frontier.rs::completes_before` | Frontier `F` completes epochs strictly less than `F`. | Reporters, readers, and cleanup consumers use the exclusive comparison. |

VS5 covers the local persistence decisions at the storage boundary. The
FizzBee M1/M3/M4/M6 models remain bounded protocol and failure references; they
do not establish refinement of SlateDB or ObjectStore implementations.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-verified/src/persistence.rs::{epoch_is_admissible,next_epoch}` | Valid authority admits only a newer epoch; consecutive and gapped source/shard sequences reject duplicates and exhaustion. | Source epoch registries validate identity and persist the entry before advancing in-memory state. |
| `crates/rockstream-verified/src/persistence.rs::{commit_outcome,coupled_commit_is_durable}` | State, outputs, source markers, and frontier are authoritative only after one successful write and flush; write-success/flush-failure remains unknown. | `SourceCheckpointStore` and group commit keep acknowledgements/frontiers closed until the durable boundary succeeds. |
| `crates/rockstream-verified/src/persistence.rs::{replay_decision,recovery_scan_status,recovery_is_ready}` | Prepared/invalid records do not advance replay; duplicates are no-ops; bounded scans distinguish completion, quota, cancellation, corruption, and continuation; readiness requires complete restored state. | Storage scans continue with progress, reject incomplete metadata, and recover the highest valid committed record. |
| `crates/rockstream-verified/src/persistence.rs::compaction_is_eligible` | Reclamation requires both reader and replay horizons and excludes retained snapshots/in-flight deltas. | Arrangement/catalog adapters supply the real horizons and storage-specific snapshot/lease evidence. |
