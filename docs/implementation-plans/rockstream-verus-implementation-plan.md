# Verus adoption plan for RockStream

**Status:** VS0–VS5 locally implemented on `feat/verus-adoption`; repository required-check enforcement remains an administrator action. VS6 remains open.
**Prepared:** 2026-09-17.  
**Repository baseline:** `trickle-labs/rockstream`, commit `1056d55f2d302de10f169681173fa3a442007512` (`main` at review time), workspace version `0.67.0`. [R0] [R1]  
**Suggested repository location:** `docs/implementation-plans/verus-adoption.md`.  
**Scope:** A cross-cutting engineering-quality track, not a new release-number sequence.  
**Evidence status:** VS0–VS5 implementation and local evidence are recorded in the verified crate, manifest, layout inventory, model map, ADRs, and sign-offs. Repository required-check enforcement, external backend qualification, and VS6 remain open.

## 1. Objective and success criteria

Introduce Verus where a small amount of executable Rust determines important correctness properties: arithmetic and merge laws, persistent encodings, incremental aggregate transitions, progress tracking, and selected commit/replay decisions.

The objective is **verified production kernels with explicit contracts**, not a second implementation that happens to be correct, and not a percentage-of-code verification target.

The first useful release of this work must establish that selected production functions are the functions Verus checks; normal Rust builds continue to work; malformed and out-of-contract inputs have defined behavior; and required CI checks fail when a promised property is broken. Subsequent milestones extend those guarantees through the real adapters, persistence paths, and distributed protocols.

### Relationship to existing assurance

RockStream already maintains FizzBee coordination models, a DataFusion-based incremental-result oracle, simulation, and release-process qualification requirements. Preserve these rather than substituting Verus for them. [R2] [R3] [R4]

| Layer | Responsibility in this plan | What it does not establish by itself |
|---|---|---|
| Verus | Check executable kernels against explicit mathematical specifications and assumptions. | Correct SQL semantics, safe callers, storage durability, or distributed liveness outside the proved boundary. |
| FizzBee | Explore protocol safety and liveness within the checked model, configuration, and fairness assumptions. | That arbitrary Rust implementations refine the model, or that finite model-checking bounds cover every deployment. |
| Oracle and property tests | Compare full supported query results and deltas against independent expectations. | Universal correctness over untested inputs. |
| Simulation and real-process tests | Exercise adapters, concurrency, failure handling, restart, and operational behavior. | A universal proof of all possible schedules and failures. |

Verus supports verification within Cargo projects, while unselected crates remain ordinary Rust. Its specifications and proof machinery do not automatically turn caller preconditions into production validation. Ghost code is erased, and upstream documentation specifically warns against using verification-only conditional compilation to change executable behavior. [V1] [V2] [V3]

## 2. Baseline findings that determine the work

These are source observations and mathematical consequences, not claims that production incidents have been reproduced.

| Observation at the reviewed revision | Required response |
|---|---|
| `WeightAddV1` and `SumCountV1` advertise group properties but use checked `i64` addition. [R5] [R6] | Specify mathematical laws separately from executable overflow behavior. Gate optimizations on an applicable, enforced domain. |
| `SumCountV1` returns `AnyAdvancement`, while `FrontierPolicy::ExactOnly` documentation names `SumCount/v1` as requiring exact progress. [R6] [R7] | Resolve the meaning of partial output versus complete query-visible results. Do not choose a policy solely from idempotence. |
| `SumCountMergeOperator` has a separate tagged merge implementation, and its no-existing-value branch returns the operand without validating it. [R8] | Audit both merge paths; define and enforce first-operand validation, format/version behavior, and their relationship to law metadata. |
| `keys.rs` implements generic and specialized key layouts, signed-order transformations, and variable-length fields. [R9] | Prove round trips and ordering, then audit disjointness under actual operator-ID allocation and supported layouts. |
| `StagedEpochAggregator::ingest_delta` updates an existing sum before checking the new count; the caller currently stages before applying aggregate transitions. [R10] | Make staging failure behavior explicit. Distinguish a reusable stager's atomicity from the surrounding operator's atomicity. |
| `ArrowZSet::compact` removes zero-weight rows; it does not consolidate equal rows. `select_rows` uses a mask, so duplicates and requested order are not preserved. [R11] | Specify these operations accurately before proving them. Audit callers before changing their behavior. |
| `FrontierAggregator::ingest` admits new shards and never retreats publication, including when a new shard reports a lower epoch. [R12] | Define membership-aware safety; monotonic publication alone does not imply safety against the new minimum. |
| `ShardFrontierReport::epoch` is an exclusive frontier: epochs strictly below it are committed. `FreshnessToken` explicitly is not an idempotent lattice because of its hash field. [R13] | Preserve the exclusive boundary and specify semantic progress separately from token/hash bookkeeping. |
| Production policy restricts dependency sources and pins Rust 1.88. [R14] [R15] | Establish Verus and library compatibility before adopting a dependency or raising the compiler baseline. |
| CI filters and path-coupling checks know existing crate paths; the coupling script accepts any change under `formal/`. [R16] [R17] | Add the new crate to trigger coverage and prevent a Verus documentation change from accidentally satisfying a FizzBee update obligation. |

### Two regression cases to establish immediately

**Checked arithmetic is not unrestricted associative `Result` arithmetic.** Let `M = i64::MAX`. Left-associated checked addition of `(M, 1, -1)` fails, while right-associated checked addition succeeds with `M`. A representable final sum therefore does not justify every intermediate merge order. `i64::MIN` also lacks a representable additive inverse under checked `i64` semantics. These follow directly from the reviewed merge implementations. [R5] [R6]

**Dynamic membership changes the frontier invariant.** Starting with no reports, ingest `(shard A, 10)`, then `(shard B, 5)`. The reviewed local update rule retains publication `10`, while the minimum of registered reports becomes `5`. This contradicts an invariant over *all currently registered shards* unless membership/activation semantics restrict which shards that claim covers. Whether an unsafe public path can reach the scenario must be traced and tested. [R12]

Preserve these cases even when the resolution is an explicit contract restriction rather than a format or algorithm change.

## 3. Architecture and non-negotiable rules

### 3.1 One executable implementation

Create a small bottom-layer crate, provisionally `rockstream-verified`. Existing crates call its executable functions. Keep Arrow, DataFusion, Tokio, SlateDB, serialization frameworks, and network clients outside it initially.

The dependency direction must be:

```text
rockstream-types / plan / storage / ops / control / runtime
                         |
                         v
                rockstream-verified
                         |
                         v
            pinned Verus support dependencies
```

The verified crate must not depend back on `rockstream-types`. Use primitive IDs, small value types, slices, and explicitly modeled collections at this boundary. Keep wire and public API conversions in the owning crate. Avoid whole-state copies or a second in-memory engine merely to make verification convenient.

Proposed layout; these are additions, not existing files:

```text
crates/rockstream-verified/
  Cargo.toml
  src/{lib,arithmetic,laws,codecs,keys,routing,zset,aggregate,frontier,epoch}.rs
formal/verus/
  README.md
  toolchain.lock.toml
  manifest.toml
  assumptions.toml
  model-map.md
  negative/
scripts/
  install-verus.sh
  verify-rust.sh
  check-verus-manifest.py
  test-verus-gates.sh
sign-offs/verus/
  VS0.md ... VS6.md
```

`toolchain.lock.toml` is a proposed RockStream tooling record, not an upstream Verus configuration format. Co-locate specifications and proofs with their executable kernels; use `formal/verus` for inventories, model mappings, and gate fixtures.

### 3.2 The boundary is part of the contract

Public entry points called by unverified Rust should normally be total, checked functions returning typed errors. Where an internal function requires an invariant, make its type/constructor and call path enforce that invariant. A proof-only `requires` clause is not an input validator.

For every covered operation, distinguish:

- The mathematical domain and desired result.
- The accepted executable domain, integer widths, and resource limits.
- Success conditions and exact results.
- Error conditions and allowed state effects.
- External assumptions and the code responsible for satisfying them.

Define both directions where practical: valid in-domain operations succeed, and successful operations satisfy the specification. An implementation that always rejects must not satisfy the intended success contract.

### 3.3 Trust policy

Record Verus, the solver, support-library specifications, Rust/LLVM, and relevant platform behavior in the trusted baseline. Upstream documents mechanisms such as `assume`, external bodies, and external function specifications that can introduce assumptions. [V4]

Default to **zero application-authored unchecked assumptions in the arithmetic, codec, routing, and aggregate kernels**. Any later exception needs an identifier, exact contract, owning reviewer, justification, executable validation where possible, and a removal/review condition. Upstream support-library assumptions remain visible; do not misreport an empty application allowlist as an empty trusted base.

Do not introduce new `unsafe` code to simplify adoption. Do not weaken a postcondition, add an impossible precondition, disable a check, or exclude a function merely to obtain a green result. Treat changes to specifications and admitted domains as API changes requiring review.

### 3.4 Scope limits

Initially exclude full SQL-language verification, floating-point aggregate equivalence, full asynchronous runtime verification, storage-engine internals, and cryptographic/hash collision freedom. Verify bounded scalar and collection logic first. State explicitly whether a claim covers termination or only partial correctness; do not infer distributed liveness from local invariant preservation.

Error-atomicity claims must identify the state they protect: temporary staging, live operator state, or committed durable state. Treat allocator aborts and process loss as crash/recovery cases, not ordinary returned errors. Proved item-count or estimated-byte bounds are not a proof of whole-process RSS; qualify real memory behavior separately.

Do not change persistent encodings, arithmetic semantics, or protocol envelopes inside a supposedly behavior-preserving extraction. Separate those changes, version them where necessary, and test compatibility and interruption behavior.

## 4. Milestone map

`VS0`–`VS6` identify this adoption program, not the existing FizzBee M-models. Owners below are responsibilities to assign, not assumed staffing or delivery dates.

| Milestone | Outcome | Dependencies | Accountable roles |
|---|---|---|---|
| VS0 | Reproducible toolchain, production pilot, and non-bypassable initial gate | None | Build/release owner; verification reviewer |
| VS1 | Honest, enforced arithmetic and merge-law contracts | VS0 | Types/storage owner; query-planning reviewer |
| VS2 | Verified persistent encodings and routing primitives | VS0; VS1 codec primitives as needed | Storage owner; compatibility reviewer |
| VS3 | Verified Z-set and aggregate kernels integrated into real operators | VS1, VS2 for persistent mutation encoding | IVM/operator owner; oracle reviewer |
| VS4 | Membership-aware progress transitions and explicit model correspondence | VS0; contract decisions coordinated with VS1 | Control/runtime owner; protocol reviewer |
| VS5 | Commit, replay, recovery, and compaction obligations at the storage boundary | VS2, VS3, VS4 | Runtime/storage owner; recovery reviewer |
| VS6 | Reproducible qualification and long-term proof maintenance | Starts at VS0; completion requires VS1–VS5 | Release owner; maintainers of covered components |

VS1 and VS2 can proceed in parallel after VS0. VS4 can develop alongside operator work once its semantics are agreed. The first bounded adoption increment is VS0–VS2; it may ship with narrowly stated kernel claims. Broader aggregate or durability claims wait for their owning milestones.

## 5. VS0 — Toolchain, production pilot, and proof discipline

**Primary touchpoints:** workspace `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `DEPENDENCY_POLICY.md`, `deny.toml`, `.github/workflows/ci.yml`, `Makefile`, new verified crate and tooling. [R1] [R14] [R15] [R16] [R18]

- [x] **VS0-01 — Freeze the compatibility matrix.** Select and record an exact Verus release/commit, solver identity, support-crate versions, release checksums, and verification compiler. Build the actual annotated production crate and its consumers with RockStream's Rust 1.88 toolchain. Check license/source policy and dependency auditing. An incompatible support crate requires a compatible pin or a separately reviewed MSRV change; a separate verifier toolchain alone does not solve production dependency incompatibility. **Evidence:** clean-environment build, verification, and dependency-check logs with resolved versions.

- [x] **VS0-02 — Add a production-called pilot.** Extract `normalize_power_of_two_bucket_count` from `rockstream-plan/src/virtual_bucket.rs` into the verified crate, preserving the wrapper and behavior. Prove that every `u16` input produces a power of two in `[1, 32768]`, preserves existing valid inputs, and follows the documented clamping rule. [R19] **Evidence:** theorem results, boundary tests, and an ordinary Cargo consumer test that executes the extracted function through the production wrapper.

- [x] **VS0-03 — Establish a proof inventory.** Define `manifest.toml` entries linking stable claim IDs to executable symbols, theorem symbols, accepted domains, production callers, supported configurations, dependencies, assumptions, and regression tests. Mark entries as planned, kernel-verified, adapter-qualified, or release-qualified. Require explicit review of precondition satisfiability and successful examples. **Evidence:** a manifest checker that rejects missing symbols, unregistered covered code, and falsely completed entries.

- [x] **VS0-04 — Audit trust and build equivalence.** Add the assumption allowlist and checks for unchecked proof constructs, excluded items, and verification-only executable branches. Permit verification-only imports/attributes only under a narrow reviewed rule. Verify and compile the same source under declared feature/target settings; prohibit a separate production algorithm selected when Verus is absent. **Evidence:** negative fixtures for an unchecked assumption and an executable `cfg(verus_only)` substitution.

- [x] **VS0-05 — Wire CI without weakening existing gates.** Add a required `verus-verify` job and proposed `verify-rust`/`verify-proof-contracts` Make targets. Preserve the FizzBee suite as `verify-protocol` and keep `make verify` as the aggregate entry point. Update FizzBee triggers and path coupling for `rockstream-verified`; distinguish actual protocol-model changes from unrelated `formal/verus` changes. Include lockfiles, verifier settings, adapters, and workflow changes in trigger coverage. **Evidence:** trigger self-tests and a deliberately failing proof that blocks the check.

- [x] **VS0-06 — Verify the verifier gate is doing real work.** Add a valid smoke proof and an isolated invalid theorem fixture. Require the fixture to fail for the expected verification reason, not a download, syntax, or dependency error. Missing tooling, zero verified targets, timeouts, incomplete runs, and excluded required symbols must fail qualification. Register required-check enforcement through the repository's applicable ruleset/branch settings. **Evidence:** positive/negative gate logs and recorded merge-rule configuration.

**Exit gate:** The production routing pilot builds normally and verifies from a clean checkout; all six tasks have evidence; the initial gate cannot turn a failed or skipped proof into success. No broader RockStream correctness claim is made.

## 6. VS1 — Arithmetic and merge laws with enforceable domains

**Primary touchpoints:** `rockstream-types/src/laws/{weight_add,sum_count,registry}.rs`, `merge_law.rs`, `rockstream-storage/src/merge_registry.rs`, their real consumers, and verified arithmetic/law/codec modules. [R5] [R6] [R7] [R8]

### Target contracts

For checked scalar addition over representable operands:

```text
add(a, b) = Ok(r)  iff  MIN <= int(a) + int(b) <= MAX
Ok(r)             implies int(r) = int(a) + int(b)
```

For reordering, use a distinct statement:

```text
admitted(operands, merge_strategy)
  implies every permitted intermediate result is representable
  and every permitted merge tree has the specified mathematical result
```

An example sufficient bound for unrestricted regrouping is an absolute-sum budget, computed in a domain where the bound check itself cannot overflow. It is a candidate policy, not a presumed existing workload guarantee. Widening from `i64` to `i128` alone is not a universal solution.

- [x] **VS1-01 — Specify the arithmetic decision.** Write an ADR covering mathematical integers, representable accumulators, per-row multiplication, negative weights, inverses, overflow error semantics, and permitted regroupings. Trace planner, exchange, storage, and gateway uses of law metadata, including separate tagged merges. Choose enforceable bounds, a sufficiently justified representation, or restrictions on optimizations; do not silently use wrapping or saturation for exact results. **Evidence:** accepted ADR and the checked-addition/inverse regression cases from section 2.

- [x] **VS1-02 — Verify checked primitives and codecs.** Implement production-used addition, multiplication, conversions, and fixed-width encode/decode helpers. Prove exact success/failure conditions, round trips, and index bounds. Malformed lengths must fail explicitly; serialized bytes must not bypass scalar range or format checks. Map typed failures to existing RS error codes, adding catalog entries and documentation only where needed. **Evidence:** Verus results and boundary tests for `MIN`, `MAX`, zero, negative weights, truncation, and trailing bytes according to the chosen format contract.

- [x] **VS1-03 — Replace declarations with reviewed law evidence.** Prove mathematical identity, commutativity, associativity, and inverse properties in their proper domains. Separately prove executable refinement and the domain needed for regrouping. Keep signed partial sum/count states distinct from valid materialized group states. Make law descriptors identify the implementation version and domain/evidence reference; do not attach unrestricted group semantics to checked `i64` results. **Evidence:** descriptor-to-theorem mapping and a negative test for claiming the full-domain law.

- [x] **VS1-04 — Enforce the contract in actual merge paths.** Route both `LawBundle` implementations and applicable tagged storage merges through the shared checked kernels, retaining their external formats unless versioned separately. Validate the first operand even when no existing value is present. Cover bad tags, unknown versions, incompatible operands, and width errors. **Evidence:** production-caller inventory and differential tests for both merge mechanisms, including the no-existing-value branch.

- [x] **VS1-05 — Make optimization admission fail closed.** Audit every code path that consumes associativity, composability, duplicate policy, or identity-based cleanup. Require a proved domain with executable admission before reordering/pushdown; otherwise reject explicitly or use a documented semantics-preserving fallback. Test batching, shard partitioning, and compaction grouping near arithmetic limits. Associativity never authorizes applying a duplicate non-idempotent delta twice. **Evidence:** tests through real planner/exchange/storage/gateway callers with asserted results and errors.

- [x] **VS1-06 — Resolve progress and SQL-boundary semantics.** Reconcile `AnyAdvancement`/`ExactOnly` documentation and implementations using separate contracts for provisional deltas, committed snapshots, and complete results. Document NULL handling, `COUNT(*)` versus `COUNT(expr)`, empty groups, decimal conversions, and floating-point finalization as distinct obligations. Initially prove integer accumulator logic, not all SQL aggregation. **Evidence:** reviewed contract, matching documentation, and public-query tests at the promised frontier.

**Exit gate:** No covered optimization is justified by an unqualified boolean alone. Overflow behavior is documented and enforced through its actual callers; malformed operands fail as specified; law proofs and executable implementations share code.

## 7. VS2 — Persistent encodings and routing

**Primary touchpoints:** `rockstream-storage/src/keys.rs`, `rockstream-types/src/merge_law.rs`, `rockstream-plan/src/virtual_bucket.rs`, and verified codecs/keys/routing modules. [R7] [R9] [R19]

### Target contracts

```text
decode(encode(value)) = Ok(value)

x < y implies min_key(x) <lex min_key(y)
x < y implies max_key(x) >lex max_key(y)

key_matches_prefix(encode(value), prefix)
  iff value belongs to the specified scan domain

valid_bucket_count(n) and route(...) = Some(b) implies 0 <= b < n
```

State domains precisely. Integer-key ordering is not SQL collation ordering. Prefix separation is a layout/allocation property, not a hash-collision claim.

- [x] **VS2-01 — Inventory layouts and allocation assumptions.** Catalogue generic shard keys, specialized arrangement keys, catalog namespaces, discriminators, variable-length fields, and scan prefixes. Trace operator-ID allocation and which layouts can coexist. Construct collision and prefix-overreach tests across those actual domains; do not assume a discriminator automatically separates a specialized key from a generic one. **Evidence:** `formal/verus/key-layouts.toml`, `ShardKeyEncoder`/`CatalogKeyEncoder` prefix tests, and the VS2 manifest claim.

- [x] **VS2-02 — Verify signed-order encodings.** Extract the shared scalar logic used by `minmax_sort_key`, its decoder, and the window sort-key functions. Prove inverse and strict lexicographic ordering in both directions across the full signed range. **Evidence:** `rockstream-verified::keys`, signed-extreme tests, and MIN/MAX/window adapter tests that call the shared implementation.

- [x] **VS2-03 — Verify structured key codecs.** Prove round trips, injectivity within each admitted layout, disjointness where required, and exact prefix membership. Check length addition, narrowing such as `usize` to `u32`, field framing, and index bounds before allocation or slicing. Separate malformed-length rejection from allocation exhaustion. **Evidence:** verified fixed-width codecs, checked factor-payload decoder, malformed-length tests, and unchanged-byte golden assertions for the selected layouts.

- [x] **VS2-04 — Qualify scans and compatibility.** Run the real storage encoder/decoder and prefix scans against supported LFS and MinIO paths, including unrelated operators and namespaces. If an overlap requires a format change, assign a version and test old reads, supported migration, interruption, and explicit rejection/rollback boundaries. Do not silently rewrite persisted keys. **Evidence:** existing LFS/storage scan suites, namespace isolation tests, and no persistent-byte changes in this slice.

- [x] **VS2-05 — Complete routing proofs.** Extend the VS0 normalization pilot to `route_power_of_two_bucket`: validate nonzero powers of two, prefix-length clamping, mask bounds, casts, and deterministic behavior for the same input/configuration. Preserve the compatibility routing path unless deliberately migrated. Do not claim balance, collision freedom, or minimal movement from a bounds proof. **Evidence:** verified routing kernel and stable-routing golden tests across zero, invalid, one-bucket, and maximum-mask cases.

- [x] **VS2-06 — Add targeted proof mutations.** Break the sign-bit transform, endian order, one length check, one discriminator, and the bucket mask in isolated fixtures. Require the relevant theorem or adapter test to reject each mutation. Distinguish proved codec behavior from storage scan behavior that remains an external contract. **Evidence:** exact signed-extreme, endian, malformed-length, discriminator, and mask-bound assertions mapped in the VS2 manifest claim.

**Exit gate:** Selected production key families have proved codecs and ordering, a reviewed namespace/scan contract, and passing real storage compatibility tests. No persistent-format change is hidden inside extraction.

## 8. VS3 — Z-set and aggregate correctness

**Primary touchpoints:** `rockstream-ops/src/zset.rs`, `aggregate.rs`, operator result/state-mutation types, aggregate oracle and simulation tests, and the existing state-beyond-RAM work. [R10] [R11] [R20]

### Target semantics

Model a Z-set as a finite-support map from rows to mathematical integer weights. State the supported row equality and decoding relation explicitly.

For an admissible input bag `B` and epoch delta `D`, the integer aggregate target is:

```text
new_state = aggregate(B + D)
output_delta = rows(new_state) - rows(old_state)
```

The per-key scalar transition starts from an absent state or a positive-count `(sum, count)` and computes a checked final state from consolidated deltas. A zero final count removes the group. Relating that transition to a valid input bag additionally requires valid-retraction and row-semantics assumptions; positive aggregate count alone cannot prove that every deleted row existed.

Distinguish same-epoch consolidation from combining separate committed epochs. Final snapshots may agree while observable intermediate deltas differ. Likewise, a mathematically valid batch may contain row orders with temporarily negative counts or overflowing intermediates; do not assert equivalence to a row-at-a-time implementation that rejects those orders.

- [x] **VS3-01 — Specify and protect the Z-set boundary.** Define the row/weight model and executable validation for aligned lengths and supported schemas. Audit public fields, struct literals, constructors, and mutation paths that can bypass `ArrowZSet::new`. Specify zero filtering separately from duplicate consolidation. Resolve whether `select_rows` is mask/set selection or ordered gather; cover repeated/out-of-range indices and frontier preservation, including empty output. **Evidence:** documented API contract and caller-level negative tests.

- [x] **VS3-02 — Verify batch kernels without verifying Arrow.** Implement shared mask/index/weight transformation and consolidation kernels where practical. Prove row/weight alignment and preservation of the specified Z-set semantics, including cancellation and repeated rows. Use explicit bounds and error behavior for weights and memory accounting. Keep Arrow decoding/filtering as a named adapter obligation rather than assuming it has been verified. **Evidence:** kernel proofs and independent adapter comparisons over mixed signed weights.

- [x] **VS3-03 — Make staging exact and failure-atomic.** Refactor `StagedEpochAggregator::ingest_delta` to calculate checked candidate sum/count values before changing either field, or enforce an explicit consumed-on-error API that cannot be reused. Prove successful accumulation against the admitted arithmetic model and failure leaves the permitted state unchanged. Check group/byte-accounting arithmetic before modification. Preserve documented per-row multiplication behavior unless separately changed under VS1. **Evidence:** reused-stager failure tests, overflow cases, and capacity-bound tests.

- [x] **VS3-04 — Extract and prove group transitions.** Add a production-used pure transition over old state and consolidated deltas. Prove positive-count state validity, absent/created/deleted groups, exact integer results, unchanged-output suppression, and correct old-row retraction/new-row insertion. Define handling for inconsistent zero-count/nonzero-sum inputs; either reject them or tie their exclusion to enforced valid-input semantics. **Evidence:** theorem set and exhaustive small-domain plus numeric-boundary tests.

- [x] **VS3-05 — Integrate atomic operator results and mutations.** Plan all touched-key transitions and output construction before installing state, or use an explicit rollback/epoch overlay. Cover every recoverable failure after validation, including Arrow conversion/build failures; verify which failures are impossible under the adapter contract. Prove or separately qualify the correspondence between new state and dirty-key `Put`/`Delete` mutations, with no mutation for unchanged keys. **Evidence:** injected failure on a later key/build step leaves authoritative state unchanged and emits no successful result.

- [x] **VS3-06 — Qualify actual query and large-state paths.** Extend the existing aggregate oracle and simulation tests, then exercise release-mode public ingestion/query paths for supported integer aggregates, NULLs, duplicate multiplicities, deletion, multiple batches per epoch, overflow, and limit rejection. Maintain touched-key complexity; integrate with demand-loaded/spillable arrangements rather than cloning all state. The repository's v0.67.1 plan owns large-state public qualification. [R20] **Evidence:** full result/delta comparisons, counter evidence, and measured memory/latency profiles for claimed workloads.

**Exit gate:** The real aggregate operator calls the proved transition and staging kernels. The exact supported SQL subset is named. Kernel/adapter error atomicity passes; end-to-end durable epoch atomicity remains a separate VS5 obligation until qualified there.

## 9. VS4 — Membership-aware frontier safety

**Primary touchpoints:** `rockstream-control/src/frontier.rs`, `rockstream-types/src/frontier.rs`, relevant reporter/worker/gateway consumers, `formal/m2_frontier_agg.fizz`, `FIZZBEE_TEST_PLAN.md`, and verified frontier logic. [R2] [R12] [R13]

### Target invariant

For the explicitly identified membership/configuration and query scope:

```text
published_frontier <= min(durable_next_epoch[s] for s in active_members)
```

Each admitted report must conservatively represent the matching shard incarnation's durable state. A frontier `F` completes epochs `e < F`, not `e <= F`.

This invariant needs bootstrap, membership, authority, and activation rules. Retaining `max(old_publication, new_minimum)` is not sufficient when the active set changes. A newly introduced shard must be caught up before activation, or the protocol must establish an equivalent configuration-scoped safety argument. Removing or replacing members also requires data-coverage and handoff conditions, not merely deleting a low report.

- [x] **VS4-01 — Define membership and progress semantics.** Write an ADR for configured versus reporting versus active shards, bootstrap completion, scope per view/deployment, shard incarnations, configuration generations, activation/retirement, and reader interpretation. Decide whether report/envelope versioning is needed; the current scalar report cannot simply be assumed to authenticate incarnation or membership. Define empty-membership behavior explicitly. **Evidence:** reviewed protocol contract and the two-shard regression case from section 2.

- [x] **VS4-02 — Extract fixed-membership transitions.** Begin with a pure transition for a fixed active set with conservative initial progress. Prove monotone admitted reports, publication bounded by the meet, duplicate/stale report behavior, and unchanged state on rejection. Define behavior at the maximum epoch without wraparound. The unverified adapter must validate authority and serialize updates as assumed. **Evidence:** proof results and tests for missing, duplicate, stale, and invalid reports.

- [x] **VS4-03 — Extend to membership changes.** Prove activation only when its catch-up/configuration conditions preserve the publication invariant. Add guarded retirement/replacement transitions that maintain coverage and reject stale-incarnation messages. Exercise reports arriving before, during, and after reconfiguration. Avoid proving a helper under an “all shards ready” assumption without showing who establishes it. **Evidence:** transition proofs and new-shard, restart, migration, and stale-generation fixtures.

- [x] **VS4-04 — Formalize the model correspondence.** Define an abstraction relation from Rust state to a Verus protocol specification, including pending I/O and rejected/no-op steps. Prove covered executable transitions simulate specification transitions and preserve the invariant. Separately map that specification to the FizzBee M2 actions and document model bounds/fairness. Matching names or mirrored comments are not a machine-checked cross-tool refinement proof. **Evidence:** local refinement lemmas, reviewed `model-map.md`, and updated FizzBee results.

- [x] **VS4-05 — Audit vector/token semantics and consumers.** Verify exclusive-frontier comparisons at reporters and readers. Separate source progress, event-time watermarks, cluster publication, and diagnostic hashes. Preserve the explicit non-idempotent nature of `FreshnessToken`; do not prove lattice laws for its XOR hash field. Decide what missing source/watermark information means before using a minimum to authorize progress or cleanup. **Evidence:** boundary tests and a semantic-field-to-consumer inventory.

- [x] **VS4-06 — Qualify public progress behavior.** Run reordered/duplicated report simulations, publisher failover, missing-shard startup, incarnation changes, and supported migration scenarios. Include real multi-process tests demonstrating that query-visible completeness never advances beyond durable data for the active configuration. Verify lost leases, delayed storage, cancellation, and fresh restart. **Evidence:** complete results at boundary epochs, pinned simulation seeds, and logs of actual topology and authority changes.

**Exit gate:** Publication safety has a membership-aware specification, verified local preservation, explicit model correspondence, and public-path evidence. Global liveness remains qualified by the separate FizzBee/runtime assumptions and tests.

## 10. VS5 — Persistence, replay, and compaction safety

**Primary touchpoints:** shard database/write-batch APIs, group commit, recovery, fencing, source/sink epoch handling, arrangement retention, and FizzBee M1/M3/M4/M6 as affected. Existing models are starting points, not proof that these Rust paths already satisfy the new contracts. [R2]

Proving a pure “may commit” predicate does not prove durable commit. A locally checked fence token does not prove cross-process fencing. A zero aggregate value does not prove that older versions or replay metadata can be deleted.

- [x] **VS5-01 — State the storage and concurrency assumptions.** Document batch atomicity, visibility versus durability, synchronization/flush semantics, conditional-write linearization, exclusive ownership, and ambiguous I/O outcomes for the exact backing APIs used. Identify checks separated from writes by `await` or lock release. Audit whether ownership/fence enforcement is durable and cross-process, rather than supplied only by a process-local mutex. **Evidence:** API-to-assumption mapping, named serialization points, and adversarial adapter tests.

- [x] **VS5-02 — Verify commit/replay decision kernels.** Extract local epoch-state transitions that distinguish staged, committed, failed, and outcome-unknown operations. Specify duplicate identity, shard/source scope, gaps, authority, and sequence exhaustion. Prove stale/duplicate requests cannot authorize a second logical application under the declared history. Keep sink-profile-specific guarantees distinct. **Evidence:** transition proofs, replay fixtures, and concrete production callers for each authorization decision.

- [x] **VS5-03 — Couple state, outputs, and progress durably.** Ensure state mutations, materialized outputs, source/dedup markers, and frontier changes share the required atomic boundary or an explicitly specified recovery protocol. A downstream operator failure must not leave upstream speculative state authoritative. An uncertain storage result must not trigger blind additive replay. **Evidence:** failure injection before/after write, acknowledgement loss, cancellation, and successful exact recovery from durable history.

- [x] **VS5-04 — Protect complete recovery.** Use verified codecs/validation and bounded continuation logic where appropriate. Distinguish end-of-scan from quota exhaustion, corruption, cancellation, and I/O failure. Do not declare readiness from a truncated or partially restored state. Bind checkpoint/schema/format/incarnation metadata to the restored state before rejoining progress publication. **Evidence:** multi-page restoration and corruption tests, full recovered multisets, and absence of ready/healthy publication on incomplete recovery.

- [x] **VS5-05 — Gate compaction and metadata retirement.** Define the retained-reader/replay horizon and prove the local eligibility predicate against it. Qualify that real deletion/compaction preserves all retained snapshots and required duplicate suppression. Test cancellation to zero while older data, snapshots, in-flight deltas, or delayed duplicates remain. Do not infer snapshot-safe garbage collection from the additive identity law. **Evidence:** retention-boundary proofs and real storage tests across compaction/restart.

- [x] **VS5-06 — Run the crash and handoff matrix.** Exercise process destruction at commit, flush, checkpoint, replay, fencing, migration, and compaction boundaries; compare all committed data and metadata after restart. Reuse the v0.67.1/v0.68 owning qualification tracks rather than creating a competing spill/migration implementation. [R20] [R21] **Evidence:** LFS/MinIO results where supported, actual worker/publisher handoffs, complete output checks, and preserved regression seeds.

**Exit gate:** Covered crash/replay/retention claims are tied to real storage and authority contracts, not just local proof results. Unsupported storage outcomes or recovery configurations fail explicitly. No claim that SlateDB itself has been formally verified is made.

## 11. VS6 — Qualification and sustainable maintenance

**Primary touchpoints:** proof manifest, CI and release workflows, sign-off automation, documentation/capability claims, and the existing release evidence conventions. [R4] [R16]

- [ ] **VS6-01 — Maintain claim-level evidence.** For each manifest claim, record theorem IDs, implementation/caller paths, assumptions, admitted domain, actual verifier output, adapter tests, public tests where relevant, and measured configuration. Separate mathematical proof, bounded model checking, test evidence, and manual review. Do not promote a claim to release-qualified because only its kernel passed. **Evidence:** complete evidence matrix with no unclassified required claim.

- [ ] **VS6-02 — Cover supported builds.** Freeze the feature/target matrix that can affect covered executable code, including release builds and the production compiler. Re-run proofs when flags, macros, dependencies, widths, or target assumptions change. Verify normal Cargo artifacts and record their source revision/digest alongside the proof run. **Evidence:** matrix results and a clean, non-incremental qualification run; no silent exclusions.

- [ ] **VS6-03 — Set measured proof and runtime budgets.** Record cold/incremental verification times, solver resource settings, proof-maintenance cost, compile impact, and runtime performance. Reuse frozen R1 workload profiles and their accepted regression tolerances; freeze missing tolerances before comparing candidates. Include touched-key writes, intermediate rows, allocations/RSS, commit latency, and freshness. **Evidence:** repeatable before/after measurements; no universal runtime or memory claim inferred from ghost erasure.

- [ ] **VS6-04 — Qualify the failure-detection machinery.** Maintain designated mutants for bad arithmetic, sign encoding, skipped validation, weakened publication guards, missing durable markers, undeclared assumptions, and missing proof targets. Verify that each fails the intended layer. Test changed-file triggers and proof inventories when directories move. **Evidence:** all required negative fixtures reject correctly and logs preserve original process exit status.

- [ ] **VS6-05 — Make sign-offs enforceable.** Extend evidence/documentation checking to understand this cross-cutting plan and `sign-offs/verus/VS*.md`; do not assume existing version-ID parsers already do. Require task-by-task evidence, independently reviewed specification changes, stable artifact links, supported backends, and explicit exclusions. Mark skipped or unavailable tests blocked, not passed. **Evidence:** checker self-tests and a release claim report generated from accepted sign-offs.

- [ ] **VS6-06 — Establish the maintenance policy.** Assign code/specification owners, document the local workflow, and require explicit review of verifier/solver/support-library upgrades and assumption changes. Re-run full proofs and required integration matrices on upgrades. Prevent emergency fixes from silently disabling proof gates; any changed coverage or temporary scope limitation must be visible and release claims updated. **Evidence:** contributor instructions, review ownership, and a rehearsed upgrade/regression procedure.

**Exit gate:** Every claimed milestone has reproducible evidence, required checks remain enforced, and the team can change and re-verify the covered code without relying on one-off proof artifacts.

## 12. CI and developer workflow

The following commands are the intended interface after VS0 implements the crate, scripts, and Make targets; they are not claimed to work in the baseline checkout.

```sh
# Verify the opted-in production crate and its verified dependencies.
cargo verus verify -p rockstream-verified --locked

# Compile and exercise the same source using ordinary Cargo.
cargo test --locked -p rockstream-verified
cargo build --locked --release -p rockstream-cli

# Repository-owned gate entry points introduced by this plan.
make verify-rust
make verify-proof-contracts
make verify-protocol
make verify
```

Upstream documents `cargo verus verify`, opt-in package metadata, and ordinary Cargo builds of annotated code. Use the fully checked verification path for CI rather than the dependency-skipping development shortcut. Validate exact flags against the selected pinned toolchain during VS0. [V1]

`verify-rust` should verify the complete required manifest, not whichever functions happened to change. Initially run the small verified crate on every pull request; optimize triggers only after proving that the dependency/configuration closure is covered. Isolate verifier caches by toolchain/solver/support-library/configuration identity, and retain clean runs for qualification.

Keep all existing applicable formatting, lint, dependency, oracle, simulation, formal-model, and real-process gates. Add claim-specific commands to the manifest as tests are implemented. Record actual test counts and test selection; an exit code from a command that executed zero relevant cases is not evidence.

### Minimum evidence record per claim

| Field | Required content |
|---|---|
| Identity | Stable claim/task ID and source revision. |
| Contract | Mathematical meaning, success/failure behavior, and admitted domain. |
| Implementation | Executable symbols, owning crate, and production callers. |
| Proof | Exact theorem symbols, verifier configuration, result, and raw log. |
| Trust | External assumptions, library/tool versions, and validation responsibilities. |
| Integration | Positive/failure test names, configurations, complete assertions, and raw results. |
| Qualification | Build digest, backend/topology, restart/performance evidence where applicable. |
| Review | Specification owner, implementation reviewer, exclusions, and approval status. |

A suggested sign-off entry is:

```markdown
- [ ] VS3-05: Atomic operator results and dirty-key mutation correspondence.
  - Implementation and production callers:
  - Theorems and accepted domain:
  - Assumptions and adapter obligations:
  - Positive, failure, and mutation tests:
  - Revision, binary digest, commands, and raw evidence:
  - Reviewer and remaining exclusions:
```

## 13. Delivery sequence and decision checkpoints

Use small pull requests with a production caller, a proof/test obligation, and evidence. Do not combine an arithmetic semantic change, persistent format migration, and toolchain adoption into one review.

| Delivery slice | Contents | Decision required before merging |
|---|---|---|
| A | VS0 compatibility record, verified crate, routing pilot, initial CI/gate fixtures | Production build compatibility and dependency-policy approval. |
| B | VS1 arithmetic ADR, regression fixtures, checked primitives and codecs | Accepted overflow/domain semantics; no misleading unrestricted law claims. |
| C | VS1 integration and optimization admission | Actual callers establish the domain or fail/fall back explicitly. |
| D | VS2 key families, ordering, prefixes, routing, compatibility | Existing bytes preserved or separately approved versioned migration. |
| E | VS3 Z-set/staging/transition kernels and operator integration | Exact supported aggregate semantics and error-atomicity boundary. |
| F | VS4 membership ADR, local refinement, model updates and public qualification | Configuration/incarnation contract and protocol compatibility. |
| G | VS5 durable coupling, recovery, replay, retention and failure matrix | Storage assumptions established and owning lifecycle gates satisfied. |
| H | VS6 complete evidence, performance, and maintenance qualification | Claims match what was actually proved and exercised. |

The shared implementation-plan rules remain authoritative for release scope and sign-off. Attach these task IDs to the owning release plans rather than inventing release versions. In particular, coordinate aggregate/spill and restoration work with v0.67.1, and distributed handoff/migration work with v0.68. [R4] [R20] [R21]

### Decisions that block dependent work

| Decision | Owner | Blocks |
|---|---|---|
| Compatible verifier/support-library/production compiler combination | Build/release | All production extraction beyond the pilot. |
| Exact overflow semantics and optimization-admission domain | Types/IVM/storage | Merge regrouping and aggregate consolidation claims. |
| Key-layout separation and format-version strategy | Storage | Namespace/scan safety claims and any migration. |
| Accepted row equality, NULL behavior, and valid retractions | IVM/oracle | Aggregate-to-query semantic refinement. |
| Membership, incarnation, activation, and completeness scope | Control/runtime | Dynamic frontier safety and handoff. |
| Durable atomicity, authority, unknown-outcome recovery, and retention horizon | Runtime/storage | Exactly-once and crash/compaction claims. |

An unresolved decision blocks only its dependent claim. It does not justify freezing unrelated codec or tooling work, and it must not be replaced with an undocumented assumption.

## 14. Expansion backlog after the first program

Prioritize future kernels by consequence of failure, independence from external libraries, and clarity of specification—not by ease of accumulating verified lines.

| Candidate | Concrete next proof | Prerequisite |
|---|---|---|
| DISTINCT | Membership/output changes at zero crossings under valid multiplicities. | Shared Z-set semantics and arithmetic contracts. |
| Inner/outer joins | Delta identity, match-count transitions, and correct unmatched-row retractions. | Arrangement identity, valid-retraction semantics, and NULL-aware join specification. |
| Windows and Top-K | Window assignment/boundaries, retained-state safety, and rank/tie correctness. | Explicit time/order semantics and retention horizon. |
| Differentiation/plan rewrites | Each selected rewrite preserves denotation for a declared SQL subset. | Proven operator semantics and explicit error/overflow equivalence. |
| Concurrent structures | Ownership/resource invariants for a specific measured bottleneck. | Stable kernel boundary and a justified need for concurrency-level verification. |

Verus has a transition-system/token framework suitable for deeper ownership/concurrency work, but that should be a later, separately scoped project rather than the entry cost of adoption. [V5]

## 15. Definition of done

A task is complete only when its implementation is on the relevant production path, its stated proof/test evidence exists, and its assumptions and limitations are recorded. A milestone is complete only when every mandatory task ID is signed off.

The adoption program is complete when **the covered laws and transitions have checked executable contracts; real callers satisfy those contracts; failure, restart, and compatibility behavior have the required integration evidence; and CI keeps those claims true as the code changes**.

Do not describe the result as “RockStream is formally verified.” Describe the specific verified kernels, supported domains, qualified adapters, and remaining trusted components.

## References

Repository links below are pinned to the reviewed commit. Re-audit changed files when implementation begins. External documentation was checked on 2026-09-17 and should be tied to the selected Verus version during VS0.

**Baseline and policy:** [reviewed commit][R0], [workspace manifest][R1], [dependency policy][R14], [Rust toolchain][R15].

**Existing assurance:** [formal models][R2], [oracle][R3], [shared implementation/evidence rules][R4], [CI workflow][R16], [path coupling][R17], [Make targets][R18].

**Production kernels:** [WeightAdd][R5], [SumCount][R6], [merge-law types][R7], [tagged storage merges][R8], [key encodings][R9], [aggregate implementation][R10], [Arrow Z-sets][R11], [frontier aggregator][R12], [progress types][R13], [bucket routing][R19].

**Related milestones:** [state beyond RAM][R20], [durable distributed lifecycle][R21].

**Verus documentation:** [Cargo integration][V1], [overview][V2], [ghost erasure][V3], [trusted components][V4], [transition systems][V5].

[R0]: https://github.com/trickle-labs/rockstream/commit/1056d55f2d302de10f169681173fa3a442007512 "Reviewed repository revision"
[R1]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/Cargo.toml "Workspace and production dependencies"
[R2]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/formal/README.md "Existing FizzBee model index"
[R3]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-oracle/src/lib.rs "Incremental-result oracle"
[R4]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/docs/implementation-plans/README.md "Shared implementation and evidence requirements"
[R5]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-types/src/laws/weight_add.rs "WeightAdd executable law"
[R6]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-types/src/laws/sum_count.rs "SumCount executable law"
[R7]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-types/src/merge_law.rs "Law descriptors, progress policies, and arrangement header"
[R8]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-storage/src/merge_registry.rs "Tagged storage merge implementation"
[R9]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-storage/src/keys.rs "Storage key encodings and prefixes"
[R10]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-ops/src/aggregate.rs "Staging and aggregate implementation"
[R11]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-ops/src/zset.rs "Arrow Z-set representation and transformations"
[R12]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-control/src/frontier.rs "Frontier ingestion and publisher implementation"
[R13]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-types/src/frontier.rs "Progress types, tokens, and exclusive-frontier semantics"
[R14]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/DEPENDENCY_POLICY.md "Dependency policy"
[R15]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/rust-toolchain.toml "Pinned production Rust toolchain"
[R16]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/.github/workflows/ci.yml "Existing CI gates and changed-path filters"
[R17]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/scripts/check-path-coupling.sh "Coordination/model coupling checks"
[R18]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/Makefile "Existing developer verification entry points"
[R19]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/crates/rockstream-plan/src/virtual_bucket.rs "Bucket normalization and routing"
[R20]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/docs/implementation-plans/v0.67.1.md "State-beyond-RAM implementation and qualification plan"
[R21]: https://github.com/trickle-labs/rockstream/blob/1056d55f2d302de10f169681173fa3a442007512/docs/implementation-plans/v0.68.md "Durable distributed lifecycle implementation plan"
[V1]: https://verus-lang.github.io/verus/guide/cargo_verus.html "Using Verus via Cargo"
[V2]: https://verus-lang.github.io/verus/guide/ "Verus overview and scope"
[V3]: https://verus-lang.github.io/verus/guide/erasure.html "Ghost erasure and conditional-compilation cautions"
[V4]: https://verus-lang.github.io/verus/guide/tcb.html "Assumptions and trusted components"
[V5]: https://verus-lang.github.io/verus/state_machines/ "Verus transition systems"
[V6]: https://www.amazon.science/blog/developing-provably-correct-rust-code-with-verus "Motivating Amazon Science article, August 31, 2026"

**Background:** The motivating article describes the implementation-level verification approach behind this proposal. The milestones above are specific engineering recommendations for RockStream, not adoption results reported by that article. [V6]
