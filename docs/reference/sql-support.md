# SQL support reference

## `ARRAY`

**Family:** array  
**Aliases:** LIST, T[], ARRAY

Homogeneous element sequence array

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `BOOLEAN`

**Family:** boolean  
**Aliases:** BOOL, BOOLEAN

Logical boolean (true/false/null)

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `BYTEA`

**Family:** binary  
**Aliases:** BLOB, BYTEA

Binary octet string sequence

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Unsupported | RS-1012 | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `DATE`

**Family:** temporal  
**Aliases:** CALENDAR_DATE, DATE

Calendar date (year, month, day)

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `DECIMAL`

**Family:** decimal  
**Aliases:** DEC, DECIMAL

Exact decimal precision number

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `FLOAT4`

**Family:** floating_point  
**Aliases:** REAL, FLOAT4

Single-precision four-byte floating-point number

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Unsupported | RS-1019 | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `FLOAT8`

**Family:** floating_point  
**Aliases:** DOUBLE PRECISION, FLOAT8

Double-precision eight-byte floating-point number

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Unsupported | RS-1019 | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `INT2`

**Family:** exact_integer  
**Aliases:** SMALLINT, INT2

Signed two-byte integer

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Core | — | — |
| arithmetic | Core | — | — |
| bitwise | Core | — | — |
| comparison | Core | — | — |
| dml | Core | — | — |
| joins | Core | — | — |
| parameter_binding | Core | — | — |
| windows | Core | — | — |

## `INT4`

**Family:** exact_integer  
**Aliases:** INT, INTEGER, INT4

Signed four-byte integer

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Core | — | — |
| arithmetic | Core | — | — |
| bitwise | Core | — | — |
| comparison | Core | — | — |
| dml | Core | — | — |
| joins | Core | — | — |
| parameter_binding | Core | — | — |
| windows | Core | — | — |

## `INT8`

**Family:** exact_integer  
**Aliases:** BIGINT, INT8

Signed eight-byte integer

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Core | — | — |
| arithmetic | Core | — | — |
| bitwise | Core | — | — |
| comparison | Core | — | — |
| dml | Core | — | — |
| joins | Core | — | — |
| parameter_binding | Core | — | — |
| windows | Core | — | — |

## `INTERVAL`

**Family:** temporal  
**Aliases:** TIME INTERVAL, INTERVAL

Time span / duration delta

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Unsupported | RS-1021 | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `NUMERIC`

**Family:** decimal  
**Aliases:** DECIMAL, NUMERIC

Arbitrary-precision fixed-point number

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `TEXT`

**Family:** character_string  
**Aliases:** STRING, TEXT

Variable-length character string without limit

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `TIMESTAMP`

**Family:** temporal  
**Aliases:** TIMESTAMP WITHOUT TIME ZONE, TIMESTAMP

Date and time without time zone

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `TIMESTAMPTZ`

**Family:** temporal  
**Aliases:** TIMESTAMP WITH TIME ZONE, TIMESTAMPTZ

Date and time with UTC time zone

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Supported | — | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `UUID`

**Family:** uuid  
**Aliases:** GUID, UUID

Universally unique identifier 128-bit

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

## `VARCHAR`

**Family:** character_string  
**Aliases:** CHARACTER VARYING, VARCHAR

Variable-length character string with length constraint

| Operation | Status | Rejection code | Notes |
| --- | --- | --- | --- |
| aggregates | Supported | — | — |
| arithmetic | Unsupported | RS-1012 | — |
| bitwise | Unsupported | RS-1012 | — |
| comparison | Supported | — | — |
| dml | Supported | — | — |
| joins | Supported | — | — |
| parameter_binding | Supported | — | — |
| windows | Supported | — | — |

