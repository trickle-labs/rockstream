# RockStream wiki brief

Write concise English documentation for contributors and operators. Explain
behavior, ownership, data flow, invariants, failure handling, and where to make
changes. Group pages by systems and workflows, rather than one page per file or
symbol. Link related pages and existing canonical documentation.

## Evidence and scope

- Start with `Cargo.toml`, `README.md`, `ARCHITECTURE.md`,
  `ROCKSTREAM_PROJECT_FOCUS.md`, `docs/README.md`, and relevant `docs/adr/`.
  Inspect headings and Rust signatures first, then read bounded sections and
  trace callers and focused tests. Avoid loading entire design documents.
- Verify factual claims against current implementations, callers, and tests.
  Cite bounded repository evidence through OpenWiki's Claims lifecycle. Treat
  roadmaps and design proposals as intent; distinguish them from shipped behavior.
- Use `capabilities.toml`, `docs/capability-matrix.md`, and the implementation
  when describing Core, Maintain, Experimental, removed, or unsupported features.
  Surface disagreements rather than resolving them by assumption.
- Keep existing persona entry points and `docs/reference/` URLs canonical,
  following ADRs 0001 and 0002. Link to generated references instead of copying
  CLI options, configuration defaults, SQL matrices, or error catalogs.
- Honor `.openwikiignore`, including when using native repository tools.
  Keep source, tests, fixtures, formal specifications, benchmarks, and evidence
  manifests available; skip raw artifacts and generated corpora.
- Inspect tests as evidence without running builds, tests, benchmarks, or formal
  verification unless explicitly requested. Describe unexecuted checks honestly.
- Use RTK for repository commands. Follow the repository's GitHub authorization
  rules; wiki work alone does not authorize tracker changes or publication.

## Coverage priorities

1. A task-routing quickstart and an architecture overview grounded in the Cargo
   workspace and actual entry points.
2. SQL and PostgreSQL protocol requests through parsing, planning,
   differentiation, incremental operators, worker execution, durable state,
   and committed result visibility.
3. Epochs, timestamps, frontiers, checkpoint barriers, persistence, replay,
   compaction, and recovery; trace control-plane and worker responsibilities.
4. Cluster topology, shard placement, leases, exchange, flow control, and
   failure handling across gateway, control plane, runtime, and storage.
5. Kafka source, PostgreSQL CDC, and Kafka sink behavior, delivery guarantees,
   recovery, and capability limits verified against current code.
6. Configuration, CLI lifecycle, standalone and distributed deployment,
   diagnostics, security boundaries, and operator recovery procedures.
7. Correctness protection: batch oracle, property tests, deterministic
   simulation, fuzz targets, formal specifications, and release evidence.

## Updates

Use incremental updates after initialization. Recheck stale Claims against
current source, preserve accurate unaffected content, and avoid rewriting pages
for style alone. Use small Mermaid diagrams only when they clarify a verified
flow or invariant. Keep unsupported and planned behavior explicitly labeled.
