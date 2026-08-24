# Schema-evolution cookbook

Use a compatible change in place. A new nullable column and a widening change
advance the schema version and preserve the existing view.

<!-- claim: compatible schema changes advance the schema version -->
Proof: `crates/rockstream-sql/tests/lfs_catalog.rs::compatible_schema_change_accepted`

```text
backend=lfs
change=add nullable column
result=accepted
schema_version=2
columns=k:Int64,s:Int64,note:Utf8?
backend=postgres-cdc
change=add nullable column
result=accepted
schema_version=2
history_entries=1
```

Use a new view or migrate the source for a breaking change. The existing data
remains untouched and the operation returns `RS-1002`.

<!-- claim: incompatible schema changes return RS-1002 -->
Proof: `crates/rockstream-sql/tests/lfs_catalog.rs::incompatible_schema_change_returns_rs1002`

```text
backend=lfs
change=rename column s to renamed
result=error
code=RS-1002
rows=[]
schema_version=1
backend=postgres-cdc
change=drop column value
result=error
code=RS-1002
rows=[]
```

The LFS behavior is covered by
`crates/rockstream-sql/tests/lfs_catalog.rs`. PostgreSQL-CDC route history and
breaking-change classification are covered by
`crates/rockstream-gateway/tests/postgres_cdc_schema_evolution_tests.rs`.
