## Executive take

My opinion: **the post-0.48 direction is credible, but only if it becomes a stabilization-and-contraction phase rather than “two more feature slices.”** The roadmap has the right philosophy — evidence over dates, correctness before scale, simulation from the beginning, operability not deferred, and splitting oversized work — which is exactly the mindset needed for a cloud-native IVM/database-like system. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

However, RockStream is aiming at a hard intersection: **incremental SQL correctness + distributed execution + object-storage persistence + cloud-native operability**. The implementation plan spans single-shard IVM, SQL/frontend joins, advanced operators, correctness soaks, multi-shard exchange, frontier protocol, fault tolerance, elasticity, connectors, query gateway, observability and hardening.  That is realistic as a multi-stage engineering programme, but **not realistic if post-0.48 continues to expand scope instead of proving the system boringly works.** [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/IMPLEMENTATION_PLAN.md)

***

# 1) Is the post-0.48 roadmap realistic?

## Mostly yes — but I would treat it as optimistic

The roadmap’s core assumptions are good: versions are planning units, not public-quality promises; each version is about evidence; and completion requires tests, benchmarks, simulation, docs, audit events, and error codes.  That is the right framing. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

Where I would be cautious:

### A. “10 person-weeks per version” is useful, but too neat

For normal application features, 10 person-weeks is a reasonable slice. For a distributed IVM engine, the hard parts do not scale linearly:

* frontier/progress semantics,
* exactly-once recovery,
* rebalance correctness,
* object-storage consistency/cost behavior,
* connector contracts,
* rolling upgrades,
* query freshness guarantees,
* backfill/recovery lifecycle,
* and operator-facing diagnostics.

The design explicitly includes causal time, async scheduling, object-storage-backed SlateDB constraints, cost previews, quotas, audit logs, support bundles, and named degradation reasons.  Those are excellent, but each one creates cross-cutting acceptance work. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

**Recommendation:** post-0.48 should have fewer “features” and more **release gates**:

* upgrade gate,
* recovery gate,
* correctness gate,
* operator usability gate,
* performance regression gate,
* connector contract gate,
* public API freeze gate.

### B. v0.48+ should be treated as product hardening, not architecture discovery

The project already frames itself as a distributed database-like system and explicitly says there is no rush to 1.0.  That is good. But by post-0.48, I would expect most fundamental architectural choices to be closed. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

**If post-0.48 still contains new primitives**, that is a warning sign. New primitives at that point should be allowed only if they remove risk or simplify the system.

Suggested rule:

> After v0.48, every roadmap item must either improve correctness, reduce operational risk, reduce public surface, improve debuggability, or prove production behavior. Anything else moves to post-1.0.

### C. Cloud-native storage needs special proof

RockStream’s positioning around object storage and SlateDB is compelling: bottomless capacity, durability, cost efficiency, and elastic workers responsible for slices of data.  But object-storage systems fail differently from local-disk systems: latency spikes, partial failures, throttling, eventual service errors, listing consistency assumptions, compaction pressure, and runaway storage cost. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/README.md)

**Recommendation:** add explicit post-0.48 acceptance criteria for:

* object-store throttling,
* stale/slow reads,
* compaction backlog,
* checkpoint corruption simulation,
* recovery from interrupted uploads,
* multi-worker recovery after partial object-store unavailability,
* cost growth under long-running workloads.

***

# 2) Do you need more documentation?

## Yes — but not “more docs” in the generic sense

The existing roadmap already says public surface must be documented and that new operator-visible failures need RS error codes.  The design also emphasizes cost preview, quotas, audit events, support bundles, and named degradation reasons.  That is a strong foundation. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md) [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

What I would add is **evidence-oriented documentation**: docs that make correctness and operability reviewable.

## Documentation I would require before 1.0

### A. Operator handbook

This should be the most important doc after the SQL/user docs.

Include:

* how to deploy,
* how to size,
* how to set freshness SLOs,
* how to read frontier lag,
* how to investigate stale views,
* how to recover failed workers,
* how to restore from checkpoint,
* how to inspect support bundles,
* how to interpret RS error codes,
* how to know when RockStream is degraded but safe.

The design says the CLI should surface pipelines and views, not shards or antichains.  The docs should mirror that: operators should not need a PhD in differential dataflow to diagnose the system. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

### B. Correctness model doc

RockStream needs a short but precise “what correctness means” document.

Include:

* exactly-once definition,
* view freshness definition,
* timestamp/frontier semantics,
* delete/retract semantics,
* outer join semantics,
* late data behavior,
* schema evolution behavior,
* recovery guarantees,
* what is guaranteed vs best-effort.

This is especially important because the implementation plan includes advanced SQL, joins, windows, recursion, view-on-view, lateral, distributed exchange, fault tolerance, and elasticity. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/IMPLEMENTATION_PLAN.md)

### C. Compatibility and SQL subset doc

Avoid vague “Postgres compatible” expectations. The roadmap already says Postgres wire compatibility is an access layer, not the product goal, and that RockStream is for live SQL views and streaming analytics, not high-concurrency OLTP.  Make that distinction painfully clear. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

Recommended docs:

* supported SQL subset,
* unsupported SQL,
* behavior differences from Postgres,
* deterministic vs non-deterministic functions,
* transaction semantics,
* isolation semantics,
* supported connector guarantees,
* supported sink guarantees.

### D. Failure-mode playbooks

For a rock-solid IVM system, docs should answer:

* Why is my view stale?
* Why is memory growing?
* Why is object-store cost growing?
* Why did a pipeline pause?
* Why did rebalancing not proceed?
* Why is a connector backpressured?
* Can I safely restart this node?
* Can I safely upgrade now?

### E. Public API stability policy

Post-0.48 is the right place to introduce:

* stable,
* experimental,
* internal,
* deprecated,

for every CLI command, config key, SQL extension, system table, metric, API endpoint, and error code.

***

# 3) Do you need more tests?

## Yes — especially more system, simulation, property, recovery, and compatibility tests

The roadmap already has a good baseline: format, clippy, workspace tests, unit tests, property/simulation/integration tests depending on risk, benchmark notes for performance claims, and seeded SimRuntime tests for distributed coordination.  The implementation plan also calls out TPC-H, Nexmark, fuzzing, parity against pg\_trickle and DataFusion batch during correctness soak. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md) [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/IMPLEMENTATION_PLAN.md)

That is exactly the right direction. I would strengthen it further.

## Test areas I would add or make explicit

### A. Differential correctness oracle tests

For every supported SQL feature, compare RockStream incremental results against a trusted batch result.

Use:

* DataFusion batch result,
* Postgres where semantics match,
* pg\_trickle-derived oracle where appropriate,
* hand-written multiset expected results for edge cases.

Acceptance rule:

> Every supported SQL construct must have randomized insert/update/delete/retract tests comparing incremental output against batch recomputation.

### B. Metamorphic SQL tests

Add tests where equivalent queries should produce equivalent results:

* predicate pushdown vs no pushdown,
* join order variants,
* CTE vs inline subquery,
* aggregate rewrite equivalence,
* view-on-view vs expanded query,
* filter-before-join vs join-before-filter where valid.

This is very useful for catching optimizer and PlanIR bugs.

### C. Long-running state drift tests

IVM bugs often do not show up immediately. Add long random workloads:

* millions of changes,
* deletes and re-inserts,
* skewed keys,
* hot partitions,
* NULL-heavy data,
* late data,
* schema changes,
* worker restarts,
* checkpoint/restore cycles.

The roadmap already treats long soaks as gates rather than loopholes.  I would make post-0.48 soak requirements very concrete. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

### D. Deterministic simulation as a release blocker

The roadmap’s SimRuntime and buggify discipline is one of the best parts.  Make it central to release quality. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

Post-0.48 should require seeded simulations for:

* worker crash during checkpoint,
* coordinator crash during rebalance,
* object-store timeout during compaction,
* connector restart during backfill,
* duplicate source events,
* partial sink failure,
* frontier stuck conditions,
* network partitions,
* slow shard,
* rolling upgrade.

### E. Upgrade and downgrade tests

If RockStream wants to be cloud-native and production-grade, upgrades are not optional.

Test:

* rolling upgrade from N-1 to N,
* mixed-version cluster behavior,
* storage format compatibility,
* failed upgrade rollback,
* config migration,
* metric/error-code continuity,
* existing views surviving upgrade.

The design already mentions storage format versioning and rolling upgrades as design concerns.  These should be explicit post-0.48 release blockers. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

### F. Connector contract tests

The repo’s recent activity mentions closing connector contract gaps before any connector ships.  That is the right instinct. For each connector, require a conformance suite: [\[github.com\]](https://github.com/trickle-labs/rockstream)

* at-least-once input,
* duplicate input,
* out-of-order input,
* source restart,
* offset rewind,
* schema evolution,
* sink idempotency,
* backpressure,
* authentication failure,
* permission failure.

***

# 4) Can you reduce the public surface?

## Yes — and I think you should

This is probably my strongest recommendation: **reduce public surface aggressively before 1.0.**

The design already has the right philosophy: one binary, one CLI, one config; node roles are flags; and the CLI surface should be pipelines and views, not shards or antichains.  That should become a hard product constraint. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

## What I would keep stable for 1.0

A minimal public surface could be:

### Stable

* create/manage sources,
* create/manage live views,
* inspect status/freshness,
* explain incremental plan/cost,
* set freshness SLO/priority/quota,
* pause/resume/rebuild pipeline,
* export support bundle,
* read documented metrics,
* documented SQL subset,
* documented error codes.

### Experimental

* advanced windowing,
* recursion,
* custom sinks,
* advanced tuning knobs,
* low-level shard controls,
* manual frontier manipulation,
* internal system tables,
* debug endpoints,
* non-core connectors.

### Internal/private

* physical shard placement,
* antichain/frontier internals,
* SlateDB layout details,
* exchange protocol details,
* compaction internals,
* worker coordination internals,
* internal PlanIR unless explicitly meant as extension API.

## Specific surface-reduction suggestions

### A. Fewer knobs

Expose intent, not mechanism.

Good public knobs:

* freshness target,
* maximum cost/quota,
* priority,
* retention,
* connector credentials,
* durability profile.

Avoid exposing:

* shard count as a normal user choice,
* compaction internals,
* exchange fanout,
* frontier internals,
* batch sizing internals,
* operator-specific execution tuning.

The design already points toward SLO-driven behavior and self-tuning, with manual knobs as overrides rather than the primary control path.  Lean into that. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/DESIGN.md)

### B. Fewer APIs

Avoid shipping too many equivalent surfaces:

* CLI,
* SQL extensions,
* REST API,
* gRPC API,
* Postgres wire,
* config file,
* Kubernetes CRDs.

Pick one canonical control plane API, then make the others thin wrappers.

### C. Keep Postgres wire compatibility narrow

The roadmap explicitly warns against becoming an accidental Postgres clone.  I agree strongly. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

For 1.0, Postgres compatibility should probably mean:

* users can query views with familiar tools,
* common SQL works where documented,
* errors are clear when unsupported,
* no promise of OLTP compatibility,
* no implication that all Postgres behavior is replicated.

### D. Mark unstable things visibly

Use names like:

```text
rockstream experimental ...
rockstream debug ...
rockstream internal ...
```

And for SQL/system APIs:

```sql
rockstream_experimental.*
rockstream_internal.*
```

Do not let operators accidentally depend on internals.

***

# My recommended post-0.48 shape

If I were guiding the team, I would make post-0.48 look like this:

## v0.49 — Surface freeze and production evidence

Goals:

* freeze stable CLI/config/SQL subset,
* label experimental/internal surfaces,
* complete operator handbook,
* complete correctness model doc,
* run full correctness oracle suite,
* run distributed simulation matrix,
* run upgrade tests,
* run 7–14 day soak,
* publish benchmark methodology.

Exit criteria:

* no new stable public API without review,
* all public errors documented,
* all public metrics documented,
* all supported SQL has oracle coverage,
* all distributed coordination paths have seeded simulation tests.

## v0.50 — Release candidate hardening

Goals:

* remove or hide unstable surface,
* fix all release-blocking correctness bugs,
* validate recovery and rolling upgrades,
* validate connector contracts,
* validate support bundle usefulness,
* validate object-store cost and failure behavior,
* run production-like load tests.

Exit criteria:

* clean long soak,
* clean upgrade test,
* clean recovery test,
* no unexplained freshness stalls,
* no undocumented public behavior,
* no unbounded resource growth under documented workloads.

***

# Final recommendation to the developers

RockStream’s roadmap has the right instincts: correctness first, simulation early, operability as a built-in property, and evidence-based milestones.  The biggest risk is not lack of ambition — it is **too much surface area becoming stable before the system has enough production evidence**. [\[github.com\]](https://github.com/trickle-labs/rockstream/blob/main/ROADMAP.md)

So my guidance would be:

1. **Yes, it is realistic as a direction — but optimistic as a schedule.**
2. **Yes, add more docs — especially correctness, operations, failure modes, compatibility, and API stability docs.**
3. **Yes, add more tests — especially oracle, simulation, recovery, upgrade, soak, and connector conformance tests.**
4. **Yes, reduce public surface — aggressively. Make the stable 1.0 API small, boring, documented, and supportable.**

For a rock-solid cloud-native IVM, the winning move is probably not “more features after v0.48.” It is: **freeze, prove, simplify, soak, and only then call it production-ready.**
