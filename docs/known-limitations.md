# RockStream v1.0 Known Limitations

The following architectural and SQL limitations are documented for the RockStream v1.0 release:

---

## 1. Single-Region Cluster Boundary

RockStream v1.0 is engineered and release-qualified for single-region deployments. Cross-region active-active replication and multi-region quorum consensus are not supported in v1.0.

---

## 2. Retraction-Producing Temporal Filters

Temporal sliding-window queries with dynamic wall-clock predicates (e.g. `WHERE occurred_at > NOW() - INTERVAL '1 hour'`) that require proactive timer-driven retractions without incoming records are not supported in Core SQL and are scheduled for post-v1 releases.

---

## 3. Quarantined Third-Party Sinks

Kafka source and PostgreSQL CDC source are Core. The Kafka sink is Experimental:
it emits epoch/count markers only, with no materialized-view snapshot rows,
incremental change rows, or row payload recovery. Legacy or unmaintained
third-party connectors are rejected at admission with `RS-4017`.

---

## 4. Key Type Restrictions in Specific Non-Equi Joins

Floating-point join keys and complex nested structured keys in full outer joins are restricted to prevent IEEE-754 precision mismatches in incremental delta state indexes.
