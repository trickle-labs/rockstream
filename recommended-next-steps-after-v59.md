# Executive assessment

**RockStream v0.59 is a substantial engineering milestone, but it is not yet a defensible v1.0 release candidate under the project’s own definition of done.** The principal gap is no longer feature implementation. It is the integrity and provenance of the release evidence: several tests do not observe the behavior they claim to prove, the release gate can be satisfied by checked-in assertions rather than immutable run artifacts, and the required 14-day frozen-candidate soak demonstrably did not occur.

I would describe the current state as:

> **v0.59.0 engineering-complete technical preview; v1.0 qualification pending.**

I would **not tag `v1.0.0` yet**.

The clearest contradiction is chronological. The v0.59 plan requires a continuous two-week, maximum-pressure, wall-clock soak whose clock restarts after every merged change. The v0.58.3 implementation landed at 06:43:06 UTC on August 18, 2026, and the v0.59 RC sign-off landed at 07:39:44 UTC—about 57 minutes later. Therefore, the declared soak gate could not have passed for the signed-off candidate.

There are also basic release-identity inconsistencies. The workspace and binary still identify themselves as version `0.42.0`; the public tags page currently surfaces `v0.52.10` as the newest tag rather than `v0.59.0` or `v1.0.0`; and the RC sign-off commit is unsigned on an unprotected `main` branch with no required status checks.   ([GitHub][1])

This assessment is based on the current source tree, tests, workflows, formal-model records, plans, sign-offs, and GitHub metadata. I did not independently execute the full test suite or long-running jobs, so I treat repository test counts and pass reports as project-reported evidence rather than independently reproduced results.

## Readiness scorecard

| Area                                 | Assessment    | Judgment                                                                                                     |
| ------------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------ |
| Product definition and focus         | Strong        | The cloud-native IVM north star and scope discipline are unusually clear.                                    |
| Architecture                         | Strong beta   | Coherent layering, single binary, sensible durable-state model.                                              |
| Correctness engineering              | Strong        | Oracle tests, deterministic simulation, formal models, error contracts, and capability tiering are valuable. |
| Test breadth                         | Strong        | Very broad coverage across unit, property, integration, storage, simulation, and formal layers.              |
| Test evidentiary quality             | Weak to mixed | Several high-level claims are supported by lower-level, nominal, static, or self-fulfilling tests.           |
| Recovery and upgrade qualification   | Mixed         | Disaster-recovery components have substance; real rolling-upgrade and chaos evidence do not.                 |
| Operability                          | Promising     | Good diagnostic concepts, but insufficient sustained real-cluster qualification.                             |
| Security assurance                   | Incomplete    | Internal security work exists; independent-review provenance is not established publicly.                    |
| Release engineering                  | Not ready     | Version, tag, artifact, signing, provenance, and branch-governance gaps are release blockers.                |
| Documentation and contract coherence | Mixed         | Excellent depth but significant drift between current code, contract, status, and historical roadmap.        |
| External validation                  | Limited       | Public evidence of real adopter operation is currently thin.                                                 |
| **v1.0 readiness**                   | **No-go**     | The project’s own release criteria have not yet been met.                                                    |

# What is genuinely strong

## 1. The strategic direction is correct

`ROCKSTREAM_PROJECT_FOCUS.md` gives RockStream a defensible product identity: a durable, object-storage-backed incremental-view-maintenance system, not another general-purpose OLTP database, warehouse, connector marketplace, or lakehouse platform. It explicitly prioritizes correctness, recovery, resource bounds, observability, upgrades, disaster recovery, and production security over adding more breadth. The decision to narrow the supported integration boundary to PostgreSQL CDC and Kafka sources plus a Kafka sink is directionally sound.

That strategy should remain the governing document. In particular, it argues strongly against reacting to issue #74 by adding DuckLake now. That request may be reasonable in isolation, but it conflicts with the explicitly accepted connector and lakehouse scope reduction. The correct response is to explain the boundary, label the issue as out of scope or future-admission-required, and avoid reopening the connector surface before the core product is proven.

## 2. The architecture has a coherent center

The crate layering is intelligible:

* SQL, planning, and differentiation form the compilation front end.
* Operators, storage, and runtime form the execution engine.
* Control, gateway, connectors, and CLI form the distributed and user-facing layer.
* Oracle and simulation provide validation infrastructure.
* A single `rockstream` binary can run different node roles.

The object-storage-backed arrangement and checkpoint model, epoch/frontier vocabulary, incremental circuit model, and PostgreSQL-compatible serving layer form a coherent product rather than a collection of unrelated features.

The single-binary constraint is particularly valuable. It reduces packaging combinations, operational variation, and role-specific version skew. Keep that constraint.

## 3. The correctness culture is a real differentiator

The project has invested in multiple complementary techniques:

* Incremental-versus-batch oracle testing.
* Deterministic simulation with permanent seeds.
* Formal models for commit, frontier aggregation, sink two-phase commit, fencing, migration, and control-plane coordination.
* Runtime invariant pairing.
* Structured error-code enforcement.
* Capability matrices and explicit strategic tiers.
* Per-crate coverage gates.

The formal-verification history is especially encouraging because it shows actual self-correction. Earlier records admitted that some models had been marked defined or passed before the model checker had truly executed. Once the toolchain was run, it found broken specifications and later exposed a substantive lease-regrant liveness problem: the control plane could regrant a lease to a worker it had already declared dead. That model was repaired and the full set was subsequently run to completion. This is the kind of defect-finding loop that can become a genuine RockStream advantage.

`capabilities.toml` is another strong asset. It does not simply advertise broad feature families as complete; it demotes aggregates, relational operators, and analytics/time to Experimental where type-matrix gaps remain. That is precisely the kind of honest compatibility contract a pre-1.0 system needs.

## 4. Some real-process and storage evidence is meaningful

The long resource test is not merely a unit test. It launches the actual binary, sends pgwire DDL and DML, maintains and queries a materialized view, exercises source pause/resume, churns connections, samples RSS, file descriptors, and sockets, and contains an injected-leak variant that must fail the same gate. That is a sound test structure.

The disaster-recovery implementation also has substantive component-level coverage. It exports and restores checkpoint data through local object storage and MinIO, verifies committed state rather than newer uncommitted state, restores catalog and connector metadata, and fails closed when exported objects are corrupted.

These tests should be retained and extended rather than discarded. The problem is the larger claims attached to them, not the value of the tests themselves.

# Why v0.59 is not yet a release candidate

## 1. There is no immutable, correctly identified release unit

A release candidate must be a specific artifact—not merely a commit that a document calls RC1.

At present:

* `Cargo.toml` says `0.42.0`.
* The CLI inherits that version.
* The public tags do not include `v0.59.0` or `v1.0.0`.
* `main` is unprotected.
* No required status checks are enforced.
* The RC sign-off commit is unsigned.
* There is no dedicated artifact-publishing workflow in `.github/workflows`.   ([GitHub][1])

The Makefile’s release target only edits the workspace version, runs `cargo check`, commits, creates an annotated Git tag, and pushes it. It does not produce or publish platform binaries or OCI images, calculate checksums, generate an SBOM, create supply-chain provenance, sign artifacts, or attach a release evidence manifest. It also uses a macOS-specific `sed -i ''` command.

Consequences include:

* A support bundle cannot reliably identify which roadmap release produced the binary.
* Upgrade tests cannot prove compatibility between two accurately versioned builds.
* Security and performance evidence cannot be bound to a published artifact digest.
* Users cannot reproduce or verify what was tested.
* A future force-push or unreviewed merge could alter the branch state associated with the sign-off.

This must be fixed before any v1 tag.

## 2. The 14-day release soak is unfulfilled

The plan explicitly requires:

* A continuous two-week automated chaos cycle.
* Maximum cluster pressure.
* A single-region deployment.
* A wall-clock-bound run.
* Restarting the clock after any P0/P1 fix or merged change.
* Tagging `v1.0.0` only after the gate.

The 57-minute interval between the final prerequisite commit and the RC sign-off conclusively means that requirement was not met for the signed-off SHA. This is not a documentation technicality; it is one of the defining v1 release gates.

The existing scheduled resource soak runs for four hours, not two weeks. Its workload is also light: one inserted row and one source pause/resume/view refresh cycle per sampling interval, with connection churn and generous resource tolerances. It is a useful leak-regression job, but not “maximum cluster pressure.”

The separate real-cluster chaos workflow is short—the target suite runtime is at most 300 seconds—and its minimum sustained throughput threshold is only one row per second, even though the checked-in published baseline claims 2,500 rows per second.

The current sign-off should therefore be amended from `PASS` to something like:

> **Implementation gate passed; frozen-candidate soak not yet executed.**

## 3. The “real-cluster chaos” test does not prove RockStream recovery

The test does use real Kafka and MinIO containers, but several of its core observations bypass RockStream:

* It writes and reads Kafka with Kafka tooling directly.
* The expected committed set is populated by the test itself.
* “Failure detection” and “reassignment” timing are derived from container kill/start operations rather than observing RockStream’s failure detector, lease ownership, or shard assignment.
* Brownout buffering is represented by a hard-coded count.
* The oracle largely compares data assembled by the harness against data the same harness submitted.
* The performance floor is one row per second.

That proves container lifecycle and direct Kafka round-tripping. It does **not** prove:

* The RockStream Kafka source resumed from its durable offset.
* A RockStream worker detected another worker’s death.
* The control plane reassigned a real shard.
* A circuit resumed from a committed checkpoint.
* Materialized-view output stayed exactly correct through the fault.
* A Kafka sink avoided duplicate output through its transaction protocol.
* Object-store backpressure remained bounded and later drained correctly.

The test’s name and release claim therefore exceed its observable behavior.

## 4. The RC validator verifies declarations more than execution

The automated release-candidate script checks many valuable structural properties, but for several gates it primarily verifies:

* A file exists.
* A test source file contains a test annotation.
* A document contains particular status text.
* A checked-in JSON file contains acceptable values.
* Required documentation paths are present.

It does not establish that:

* The named tests ran successfully for the candidate SHA.
* Docker-backed tests did not return early or skip.
* A workflow ran for 336 hours.
* The raw metrics generated the checked-in summary.
* Artifacts came from an unmodified candidate.
* The security review came from an independent assessor.
* The package version and release tag match the candidate.
* Release binaries correspond to the tested source.
* The target hardware, workload, dependencies, and environment are recorded.

This permits a circular form of evidence:

1. A JSON file says measured throughput is 2,500 rows/s.
2. The gate reads that JSON file.
3. The gate passes because the number in the file is at least 2,500.

The measurement itself is not established.

## 5. Several high-level “proof” tests are nominal or non-substantive

### Rolling upgrade

The rolling-upgrade test starts three containers using `--version`, allows the N and N+1 image names to default to the same image, silently returns when prerequisites are absent, and finally compares a constant vector of epoch strings with an identical constant vector. It runs no cluster, no workload, no mixed-version protocol, no shard reassignment, and no upgrade.

That means Gate 5’s claimed “three-worker N→N+1 zero-loss rolling upgrade” is not implemented as an end-to-end test.

### Failure matrix

Many failure-matrix tests are useful specification examples, but they create local vectors and counters rather than driving the corresponding production component. Examples include appending epoch numbers to a vector after advancing simulated time, incrementing a term variable, retrying locally counted frames, and resetting a local migration counter.

Those are best classified as **executable model examples**, not implementation recovery proofs.

### Recovery SLOs

The “24-hour” recovery tests use a simulated duration, and the “1 TB state” test assigns `1_000_000_000_000` to a configuration field rather than operating on a terabyte of state. The test named as a MinIO integration test does not start or access MinIO; it invokes the same simulated scenario.

Simulation is entirely appropriate for exhaustive timing and interleaving logic. It is not evidence of wall-clock p99 recovery against a real object store or a 1 TB deployment.

### Disaster recovery

The disaster-recovery storage tests are considerably stronger, but the CLI runbook test verifies hard-coded documentation strings such as a measured RTO of 0.42 seconds rather than timing a fresh-cluster restore.

The correct conclusion is:

* Checkpoint export/restore machinery has meaningful component proof.
* A full operational disaster-recovery drill still needs to be run.

## 6. The independent security review is not independently evidenced

`SECURITY_REVIEW_COMMISSION.md` says the review is independent and closed with zero open P0/P1 findings. However, the public record names no assessor, company, engagement identifier, review methodology, report date, report digest, scope exclusions, signature, or externally verifiable closure statement. Its listed evidence consists of RockStream’s own tests and scripts.

An actual third-party review may exist privately. The repository evidence does not currently establish that it does.

For a public v1 release, acceptable options include:

* Publish a redacted audit report.
* Publish an assessor-signed attestation with report hash and scope.
* Name the reviewing organization and engagement dates.
* Record excluded components and unresolved lower-severity findings.
* Add a `SECURITY.md` vulnerability disclosure and supported-version policy.

Until then, call this an **internal security readiness review**, not an independent audit.

## 7. Documentation has outpaced the authoritative contract

The documentation is impressively extensive, but it contains substantial temporal and surface drift:

* The README still says the current release is v0.42 and that work is proceeding toward v0.59.
* The roadmap table says v0.59 requires the two-week soak and `v1.0.0` tag.
* Some sections discuss features as planned while other documents mark related versions complete.
* The language-feature document includes extensive historical correction notes because earlier iterations described Rust APIs or planned constructs as SQL-reachable features.
* The architecture document describes thirteen purpose-built crates, while the workspace currently has fourteen named RockStream crates plus the fuzz member.

`capabilities.toml` is the best candidate for the authoritative source. User-facing support tables, README status, SQL feature documentation, and conformance matrices should be generated from it wherever possible.

The project should also distinguish evidence classes explicitly:

| Evidence class                  | What it can support                              |
| ------------------------------- | ------------------------------------------------ |
| Static check                    | Wiring, source conventions, document consistency |
| Unit/property test              | Local function or algebraic behavior             |
| Formal model                    | Abstract protocol safety/liveness                |
| Deterministic simulation        | Rust behavior under modeled faults               |
| Real-backend component test     | Storage or connector integration                 |
| Real multi-process cluster test | End-to-end recovery and upgrade behavior         |
| Frozen wall-clock soak          | Sustained stability and operational performance  |

A formal model should never be cited as evidence that a real object store recovered in 12 seconds. A Docker smoke test should never be cited as a mixed-version rolling upgrade. Each release claim should require evidence from the appropriate class.

## 8. Dependency and maintenance work needs a clean reset

The current dependency-upgrade draft raises the Rust baseline and updates important libraries, but its own description says the full gateway suite still has 28 unrelated backfill operator-matrix failures. Its patch also lowers several coverage floors materially, including control and connector coverage.

Do not merge that work by normalizing the lower coverage or accepting known failures. Instead:

1. Rebase it onto the qualified candidate.
2. Separate mechanical dependency upgrades from behavioral changes.
3. Resolve the 28 failures.
4. Remeasure coverage from the complete feature matrix.
5. Require an explicit explanation for any legitimate coverage movement.
6. Run the future N→N+1 upgrade test using the old and new dependency builds as genuinely different images.

## 9. External product validation is still thin

As of August 18, 2026, the public repository page shows one star, no forks, and five pull requests; the substantive open public issue found during this review is a request for DuckLake support. That does not prove there are no private users, but it does mean the public record provides little evidence that an external operator has run RockStream under a sustained real workload. ([GitHub][2])

Before declaring a broad production-ready v1, RockStream would benefit substantially from a small number of design partners operating:

* A Kafka-to-materialized-view workload.
* A PostgreSQL-CDC workload.
* A high-cardinality or skewed aggregate/join workload.
* A real backup, restore, and upgrade procedure.

The goal is not feature requests. It is discovering operational assumptions the internal harness does not model.

# The underlying problem

RockStream has optimized very effectively for **proof-surface coverage**: every roadmap item receives a plan, evidence document, sign-off, test name, and often a validation script.

That discipline is valuable, but it has developed a failure mode:

> **The existence and naming of the evidence artifact has sometimes become interchangeable with the occurrence of the event it claims to prove.**

Examples include:

* A checked-in measured baseline accepted as proof of measurement.
* A test named “rolling upgrade” accepted despite not upgrading anything.
* A simulated 24-hour clock presented beside real-cloud SLO language.
* A container lifecycle test presented as shard detection and reassignment.
* A security review closure document presented without independent provenance.
* A 14-day gate signed off less than an hour after the last prerequisite merge.

The next phase should not add more test names or roadmap versions. It should make evidence **causal, immutable, observable, and bound to a published artifact**.

# Recommended next roadmap

## v0.59.1 — Evidence integrity and honest release state

This should be the immediate next milestone.

### Reclassify the current release

Change the public status to:

> **v0.59.0 implemented; v1.0 release qualification in progress.**

Amend the v0.59 sign-off so it separates:

* Implementation completion.
* Short CI gate completion.
* Security review status.
* Frozen-candidate soak status.
* Artifact publication status.
* Final release authorization.

Do not mark a section passed until the corresponding evidence exists.

### Establish one candidate identity

Set a consistent version everywhere:

* Workspace package version.
* `rockstream --version`.
* Docker image labels.
* support bundles.
* metrics/build-info endpoint.
* release manifest.
* documentation.
* Git tag.

A suitable progression would be:

* `0.59.0` for the current technical preview.
* `1.0.0-rc.1` only after all non-soak gates are genuinely complete.
* `1.0.0` only after the frozen soak.

Embed at least:

* Semantic version.
* Git commit SHA.
* build timestamp.
* Rust compiler version.
* Cargo lockfile digest.
* enabled feature set.

### Protect the release history

Enable:

* Branch protection on `main`.
* Pull-request-only changes.
* Required CI, formal, security, Docker, coverage, and evidence checks.
* No force pushes.
* Signed tags.
* Preferably signed commits for release-authorizing changes.
* CODEOWNERS approval for release workflows, formal specifications, security policy, and compatibility contracts.

### Replace self-attestation with an evidence manifest

Every release proof should produce a machine-readable, immutable manifest similar to:

```json
{
  "candidate_sha": "...",
  "artifact_digests": {
    "linux_amd64": "sha256:...",
    "oci_image": "sha256:..."
  },
  "workflow_run_id": "...",
  "started_at": "...",
  "completed_at": "...",
  "environment_digest": "...",
  "workload_spec_digest": "...",
  "fault_schedule_seed": "...",
  "tests": {
    "passed": 0,
    "failed": 0,
    "skipped": 0
  },
  "raw_metrics_digest": "...",
  "result": "PASS"
}
```

The RC validator should reject evidence when:

* The candidate SHA differs.
* The artifact digest differs.
* A required job was skipped.
* Docker tests returned early.
* Raw data is missing.
* The run started before the candidate was frozen.
* The soak elapsed time is less than 1,209,600 seconds.
* A code merge occurred during the qualification interval.
* A summary value cannot be regenerated from raw artifacts.

Checked-in baseline files may define **targets**. They must not be accepted as the source of **measured results**.

## v0.59.2 — True end-to-end release qualification

Replace the current high-level proof tests with one integrated qualification harness.

### Required topology

Run genuinely separate processes or containers for:

* Three control-plane nodes.
* At least three workers.
* A gateway.
* Kafka.
* MinIO.
* A network-fault proxy or equivalent fault-injection layer.
* An independent workload generator.
* An independent correctness auditor.

### Required data path

The harness should:

1. Create tables, sources, views, and the Kafka sink through the public pgwire or CLI surface.
2. Ingest deterministic records through the real RockStream Kafka or PostgreSQL CDC source.
3. Include inserts, updates, deletes, out-of-order events, skewed keys, and high-cardinality state.
4. Maintain multiple representative Core queries.
5. Query results through pgwire.
6. Consume sink output independently.
7. Compute the expected result using an external batch oracle.
8. Compare full result multisets and committed frontiers.

The expected set must not be populated by the same code that declares RockStream’s committed set.

### Required failure observations

For every injected failure, record actual RockStream events:

* Worker heartbeat loss.
* Failure-detector transition.
* Previous shard owner and new shard owner.
* Lease/fencing epochs.
* checkpoint selected for recovery.
* source offset or LSN before and after recovery.
* view frontier before and after recovery.
* sink transaction or epoch marker.
* first correct post-recovery query.
* data-loss and duplicate counts.

Measure failure-detection, shard-reassignment, and freshness-recovery time from those events—not from the duration of `docker kill` or `docker start`.

### Real rolling upgrade

The rolling-upgrade test should use two genuinely different immutable image digests:

1. Start an N cluster.
2. Run continuous mixed writes and reads.
3. Replace one node at a time with N+1.
4. Keep both versions active concurrently.
5. Verify version negotiation and assignment restrictions.
6. Verify no committed epoch gap.
7. Verify exact materialized-view output throughout.
8. Verify rollback or fail-closed behavior for an incompatible format.
9. Finish with all nodes on N+1.
10. Restart the final cluster and verify durable state again.

A test must fail, not return successfully, when CI declares Docker or the required images mandatory.

### Real disaster recovery

Extend the existing component tests into an operational drill:

1. Run a live cluster and commit a known workload.
2. Export a checkpoint to an independent bucket.
3. Destroy all original process-local state and cluster metadata.
4. Start a fresh cluster with a new cluster identity.
5. Restore through the public CLI.
6. Resume source ingestion from the stored offset.
7. Compare complete view checksums, frontiers, catalog state, and sink state.
8. Measure RPO and RTO during the run.
9. Corrupt one object and prove restoration fails before publishing an active-generation pointer.

The existing object-store tests provide a good foundation for this.

### Honest performance qualification

Choose one of two valid outcomes:

* Prove the published 2,500 rows/s target on declared hardware and workload, or
* Publish a lower, honestly measured initial envelope.

Record:

* hardware and instance class.
* number of workers and shards.
* object-store configuration.
* row width and key distribution.
* query definitions.
* update/delete ratio.
* batch size.
* state size.
* compaction state.
* p50, p95, and p99 latency.
* throughput during normal operation and recovery.
* object-store request counts and cost-relevant metrics.

One-row-per-second gating should not coexist with a public 2,500-row-per-second baseline.

## v0.59.3 — Security, release engineering, and contract reconciliation

### Complete the actual independent security review

Require an assessor-issued artifact or attestation covering:

* Internal mTLS and certificate lifecycle.
* Authentication and authorization.
* Cross-tenant/session isolation.
* Secret storage and rotation.
* Object-store credentials.
* Connector trust boundaries.
* upgrade and rollback paths.
* checkpoint export confidentiality and integrity.
* support-bundle redaction.
* supply-chain and release process.
* fuzzing and parser exposure.
* denial-of-service and resource-bound behavior.

Publish zero-open-P0/P1 status only after findings are independently verified.

### Build reproducible release artifacts

A v1 release workflow should produce:

* Linux x86-64 binary.
* Linux ARM64 binary.
* Multi-architecture OCI image.
* SHA-256 checksums.
* SPDX or CycloneDX SBOM.
* Vulnerability scan results.
* Signed provenance attestation.
* Signed Git tag.
* Signed container image.
* release notes.
* configuration reference.
* known limitations.
* supported SQL matrix.
* N/N+1 upgrade matrix.
* backup and restore runbook.
* artifact-to-source reproducibility instructions.

The release evidence manifest should include every artifact digest.

### Make the contract generate the docs

Treat `capabilities.toml` as the source of truth and generate:

* README support summary.
* SQL feature matrix.
* Core/Maintain/Experimental tables.
* named proof references.
* connector support matrix.
* compatibility and deprecation tables.

Move the long historical roadmap into a history or project-development document. The main README should tell a new user what is supported **now**, how to start it, what is experimental, and what is not guaranteed.

### Resolve the dependency-upgrade branch without weakening gates

Do not lower coverage merely to make the upgrade green. Fix the failing backfill matrix, split unrelated dependency groups into reviewable changes, and use the resulting old and new binaries in the real rolling-upgrade test.

## v1.0.0-rc.1 — Frozen candidate

Only create `v1.0.0-rc.1` after all preceding work is complete.

Freeze:

* Source SHA.
* Cargo lockfile.
* compiler/toolchain.
* build container.
* release artifacts.
* deployment configuration.
* workload specification.
* fault schedule policy.
* acceptance thresholds.

No source changes should be merged into the candidate during qualification.

## v1.0.0 — Complete the 336-hour qualification

Run the exact signed RC artifacts continuously for the required 14 days.

The workload should include:

* Kafka ingestion.
* PostgreSQL CDC ingestion where practical.
* direct pgwire DML.
* aggregates and joins from the documented Core subset.
* updates and deletes.
* skewed and high-cardinality keys.
* sustained query load.
* connection churn.
* object-store latency and throttling.
* worker failures.
* control-node failures.
* source disconnects.
* checkpoint interruption.
* storage pressure.
* process restarts.
* at least one rehearsed restore.
* mixed-version upgrade qualification completed against the same candidate family.

Produce daily immutable evidence bundles, but determine the final pass from the complete interval.

Any merged change, artifact replacement, or P0/P1 correction resets the clock, exactly as the existing plan requires.

Only then create the signed `v1.0.0` tag.

# Concrete go/no-go checklist for v1

The final release should be a **no-go** unless every item below is true:

| Requirement         | Acceptance condition                                                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Release identity    | Package, binary, image, manifest, documentation, and tag all say `1.0.0`.                                                     |
| Immutable candidate | Every artifact digest is tied to one protected, signed commit.                                                                |
| Required checks     | Branch protection enforces all release checks; zero required tests skip or return early.                                      |
| Correctness         | End-to-end mixed DML/CDC output equals an independent batch oracle.                                                           |
| Recovery            | Actual worker loss, control loss, source disconnect, and object-store faults produce zero lost or duplicated committed state. |
| Recovery timing     | p99 values come from many observed real-cluster events, not constants or simulated clock values.                              |
| Rolling upgrade     | Real N and N+1 images coexist under load with zero epoch gaps and exact output.                                               |
| Disaster recovery   | A fresh cluster restores from an independent export and resumes ingestion correctly.                                          |
| Resource bounds     | Sustained high-cardinality workloads remain within documented RSS, FD, socket, queue, and state limits.                       |
| Security            | Independent assessor evidence exists; zero open P0/P1 findings.                                                               |
| Supply chain        | Signed binaries/images, checksums, SBOM, and provenance are published.                                                        |
| Documentation       | Public claims are generated from or reconciled with the machine-readable capability contract.                                 |
| Soak                | The exact frozen artifact completes 336 uninterrupted hours under the declared workload.                                      |
| Operator usability  | A person not involved in implementation can install, diagnose, upgrade, and restore using the public runbooks.                |

# What should not happen next

Do **not** begin another broad feature phase.

Specifically:

* Do not add DuckLake or reopen the removed lakehouse sink/catalog surface.
* Do not add new connector families.
* Do not expand PostgreSQL compatibility merely for completeness.
* Do not promote broad aggregates, joins, or time analytics from Experimental until their exact supported type matrices are proven and demanded by real workloads.
* Do not add more roadmap versions whose primary deliverables are sign-off documents.
* Do not lower coverage or performance thresholds to accommodate dependency upgrades.
* Do not use test count as a proxy for production readiness.
* Do not describe simulated duration or nominal state size as real wall-clock or storage-scale evidence.

After v1, the highest-value priorities are likely to be adopter-driven expansion of exact SQL subsets, cost and capacity modeling, upgrade compatibility, and operational packaging—not another large feature family.

# Recommended first five changes

1. **`release: reopen v1 qualification and correct public status`**
   Amend the sign-off, update the README, freeze feature work, and establish a v1 blocker milestone.

2. **`release: unify version identity and protect release provenance`**
   Set the workspace to `0.59.0`, embed build metadata, protect `main`, require checks, and sign release tags.

3. **`ci: make release evidence executable and SHA-bound`**
   Introduce the evidence manifest, raw-artifact digests, zero-skip enforcement, and real elapsed-time validation.

4. **`test: replace nominal RC proofs with one real multi-process qualification harness`**
   Rewrite chaos, rolling upgrade, recovery timing, and fresh-cluster restore around actual RockStream observations.

5. **`release: publish a signed frozen 1.0 RC and start the 336-hour gate`**
   Build reproducible artifacts, attach the security attestation and SBOM, freeze the candidate, and begin the restart-on-change soak.

The architecture and engineering foundation justify continuing toward v1. The current evidence does not justify declaring that destination reached. **The next milestone should be evidence integrity and real-system qualification, not feature expansion.**

[1]: https://github.com/trickle-labs/rockstream/tags "https://github.com/trickle-labs/rockstream/tags"
[2]: https://github.com/trickle-labs/rockstream "https://github.com/trickle-labs/rockstream"
