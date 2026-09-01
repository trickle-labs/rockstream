# SQL semantics and PostgreSQL compatibility

Authoritative v1 SQL semantics, PostgreSQL 18.0 differential compatibility, and system boundaries.

## Reference database

- **Engine:** postgresql
- **Version:** 18.0
- **Canonical image:** `postgres:18.0@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c`
- **AMD64 digest:** `sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c`
- **ARM64 digest:** `sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c`

## Collation and string ordering

- **Active collation:** `rockstream_binary_v1`
- **Semantics:** Raw UTF-8 byte-wise lexicographical ordering independent of OS/libc locale
- **Unsupported collations:** Rejected fail-closed with `RS-1013`.

## Numeric precision and decimal bounds

- **Admitted precision:** `DECIMAL(p, s)` where 1 <= p <= 38, 0 <= s <= p.
- **Arithmetic overflow:** Fails closed with `RS-1016`.
- **Invalid precision/scale:** Fails closed with `RS-1012`.

## Temporal policy and time zones

- **Fractional precision:** 6 digits (resolution: microsecond).
- **Internal storage:** UTC.
- **Invalid format:** Fails closed with `RS-1012`.

## Identifier case folding

- **Unquoted identifiers:** Folded to lowercase.
- **Quoted identifiers:** Preserved verbatim (case-sensitive).
- **Maximum byte length:** 63 bytes (exceeding rejected with `RS-1012`).

## Three-valued logic and NULL semantics

- **Evaluation logic:** ANSI 3VL (`TRUE`, `FALSE`, `UNKNOWN`).
- **Equality:** `NULL = NULL` evaluates to `UNKNOWN`.
- **Distinctness:** `IS NOT DISTINCT FROM` is NULL-safe.

## Prepared statement array parameters

- **Supported array dimensions:** 1.
- **Array membership:** `col = ANY($1)` / `col IN (SELECT UNNEST($1))` supported.
- **Invalid array parameter:** Fails closed with `RS-1012`.

## Multiset bag semantics and IVM retractions

RockStream preserves exact bag/multiset duplicate counts under incremental view maintenance. Retraction underflow fails closed with `RS-1017`.

## Unmatched DML

`UPDATE` or `DELETE` statements matching zero rows succeed without error and return command tags `UPDATE 0` and `DELETE 0`.

## Floating-point join restrictions

Floating-point equality joins (`FLOAT4`/`FLOAT8`) are explicitly rejected fail-closed with `RS-1019` due to non-total IEEE-754 ordering.

