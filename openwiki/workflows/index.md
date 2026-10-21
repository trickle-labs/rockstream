# Files

- [PostgreSQL requests, transactions, and committed reads](postgres-requests.md) - Trace PostgreSQL wire requests through session dispatch, buffered DML, local commit publication, committed read paths, and bounded snapshot-to-live subscriptions. Explains freshness, authorization, cancellation, and the limits of atomicity and replay guarantees.
- [View lifecycle, backfill, and schema changes](views-and-schema.md) - How SQL view definitions become executable pipelines and durable output, how source backfill is fenced and published, and how schema changes or restart affect existing state.
