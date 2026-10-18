# PostgreSQL differential corpus

Issue #124's bounded corpus is `pg18_differential_conformance_tests`.
The `pinned_postgres_numeric_and_array_corpus` test starts a real Rockstream
gateway backed by a temporary ShardDb and a fresh `postgres:18.0` Docker
container. It verifies `server_version_num = 180000`; neither a floating
PostgreSQL version nor unavailable Docker/reference infrastructure can pass.

Run from the repository root with Docker running:

```sh
cargo test --locked -p rockstream-gateway --test pg18_differential_conformance_tests -- --include-ignored --nocapture
```

The PostgreSQL differential integration CI job runs this command on every PR
and push to main. The aggregate Check job requires it to succeed. Ordinary
local test runs omit the Docker test via Rust's explicit `ignore` annotation;
mandatory execution includes it and has no skip-on-unavailable path. CI retains
the statement-by-statement transcript, including both complete result records.

Both servers receive identical startup credentials, schema, data, SQL, and wire
requests. DDL/DML/numeric queries use simple-query protocol; array cases use
Parse/Bind/Describe-portal/Execute/Sync, explicit array OIDs, binary parameters
encoded by `postgres-types` (as in `tokio-postgres`), and text results.

The corpus covers:

- `DECIMAL(18,4)` rows `(1,1234.5678)` and `(2,9999.0000)` with exact scale.
- Precision overflow (`22003`), failed transaction state (`E`), subsequent
  statement rejection (`25P02`), rollback to idle (`I`), and unchanged contents.
- Autocommit multirow insert atomicity when a valid row precedes overflow.
- Text-array membership returning IDs `10,30`, integer-array membership returning
  `20,30`, and an empty array returning a row description and zero data rows.
- A nullable text column returning SQL NULL rather than an empty string.

Ordered response records compare column names, table OIDs, attribute numbers,
type OIDs, type sizes, type modifiers, format codes, every nullable value,
complete command tags, SQLSTATEs, and ReadyForQuery transaction state. Parse and
Bind completions also compare. Missing rows cannot be replaced by command tags.

## Bounded compatibility differences

Rockstream's current pgwire metadata reports zero table OID, attribute number,
type size, and typmod. For this corpus only, the comparator accepts those exact
zeros against PostgreSQL's positive table OID and these exact metadata tuples:

| Column | Type OID | Attribute | Type size | Typmod |
|---|---|---|---|---|
| numeric table `id` | 23 | 1 | 4 | -1 |
| `amount` | 1700 | 2 | -1 | 1179656 (`NUMERIC(18,4)`) |
| items table `id` | 20 | 1 | 8 | -1 |
| `category` | 25 | 2 | -1 | -1 |

For `CREATE TABLE` statements only, Rockstream's exact `CREATE TABLE 0` tag is
accepted against PostgreSQL's `CREATE TABLE` tag. Other command tags compare
exactly; there is no general removal of row counts or tag suffixes.

Names, type OIDs, format codes, values, SQLSTATEs, and transaction state
have no compatibility exceptions. All metadata is retained in the transcript;
unknown column/type combinations or other metadata changes fail. Startup
parameter statuses/backend keys are server-specific and outside result
comparison. Error prose, severity, source location, and hints are outside this
SQLSTATE contract; no error-code remapping is allowed.

Negative controls change one value, one type, and one SQLSTATE and assert exact
statement/result diagnostics. A fourth rejects a command tag replacing a row.
Other existing tests in this file are Rockstream-only semantic regressions;
this is not a claim of unrestricted PostgreSQL compatibility.
