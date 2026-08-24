# Functions reference

| Name | Category | Signature | Arguments | Returns | Null handling | Examples | Description |
| --- | --- | --- | --- | --- | --- | --- | --- |
| abs | scalar | abs(int8) -> int8 | INT8 | INT8 | returns_null_on_null | SELECT abs(-42); | Returns the absolute value of an integer expression |
| array_agg | aggregate | array_agg(any) -> array | ANY | ARRAY | handles_null | SELECT array_agg(id) FROM t GROUP BY category; | Aggregates grouped values into an array |
| avg | aggregate | avg(int8) -> float8 | INT8 | FLOAT8 | returns_null_on_null | SELECT avg(val) FROM t; | Computes arithmetic mean over non-null numeric values |
| bool_and | aggregate | bool_and(boolean) -> boolean | BOOLEAN | BOOLEAN | returns_null_on_null | SELECT bool_and(flag) FROM t; | Returns true if all non-null input values are true, otherwise false |
| bool_or | aggregate | bool_or(boolean) -> boolean | BOOLEAN | BOOLEAN | returns_null_on_null | SELECT bool_or(flag) FROM t; | Returns true if any non-null input value is true, otherwise false |
| cast_int64 | scalar | cast_int64(any) -> int8 | ANY | INT8 | returns_null_on_null | SELECT cast_int64('123'); | Casts input expression to 64-bit signed integer |
| char_length | scalar | char_length(text) -> int8 | TEXT | INT8 | returns_null_on_null | SELECT char_length('rockstream'); | Computes character count of text |
| character_length | scalar | character_length(text) -> int8 | TEXT | INT8 | returns_null_on_null | SELECT character_length('rockstream'); | Computes character count of text |
| coalesce | scalar | coalesce(any, ...) -> any | ANY, ANY | ANY | handles_null | SELECT coalesce(NULL, 'fallback'); | Returns the first non-null argument |
| concat | scalar | concat(text, ...) -> text | TEXT, TEXT | TEXT | returns_null_on_null | SELECT concat('hello', ' ', 'world'); | Concatenates string arguments |
| count | aggregate | count(*) -> int8 | ANY | INT8 | handles_null | SELECT count(*) FROM t; | Counts the number of input rows |
| current_date | scalar | current_date() -> date |  | DATE | handles_null | SELECT current_date; | Returns current calendar date |
| current_timestamp | scalar | current_timestamp() -> timestamptz |  | TIMESTAMPTZ | handles_null | SELECT current_timestamp; | Returns current transaction timestamp with UTC zone |
| date_trunc | scalar | date_trunc(text, timestamp) -> timestamp | TEXT, TIMESTAMP | TIMESTAMP | returns_null_on_null | SELECT date_trunc('hour', timestamp '2026-08-24 15:30:00'); | Truncates timestamp to specified date precision part |
| dense_rank | window | dense_rank() -> int8 |  | INT8 | handles_null | SELECT dense_rank() OVER (PARTITION BY dep ORDER BY sal DESC); | Computes rank without gaps across peer groups |
| extract | scalar | extract(text, timestamp) -> int8 | TEXT, TIMESTAMP | INT8 | returns_null_on_null | SELECT extract('year', timestamp '2026-08-24 15:30:00'); | Extracts numeric subfield from date or timestamp |
| greatest | scalar | greatest(any, ...) -> any | ANY, ANY | ANY | handles_null | SELECT greatest(10, 20, 5); | Selects greatest value from argument list |
| lag | window | lag(any, int8) -> any | ANY, INT8 | ANY | handles_null | SELECT lag(price, 1) OVER (ORDER BY ts); | Accesses value from prior row within current window partition |
| lead | window | lead(any, int8) -> any | ANY, INT8 | ANY | handles_null | SELECT lead(price, 1) OVER (ORDER BY ts); | Accesses value from subsequent row within current window partition |
| least | scalar | least(any, ...) -> any | ANY, ANY | ANY | handles_null | SELECT least(10, 20, 5); | Selects smallest value from argument list |
| length | scalar | length(text) -> int8 | TEXT | INT8 | returns_null_on_null | SELECT length('rockstream'); | Computes character length of string |
| lower | scalar | lower(text) -> text | TEXT | TEXT | returns_null_on_null | SELECT lower('ROCKSTREAM'); | Converts text characters to lowercase |
| ltrim | scalar | ltrim(text) -> text | TEXT | TEXT | returns_null_on_null | SELECT ltrim('   hello'); | Strips leading whitespace from string |
| max | aggregate | max(any) -> any | ANY | ANY | returns_null_on_null | SELECT max(score) FROM t; | Returns maximum value across grouped rows |
| min | aggregate | min(any) -> any | ANY | ANY | returns_null_on_null | SELECT min(score) FROM t; | Returns minimum value across grouped rows |
| now | scalar | now() -> timestamptz |  | TIMESTAMPTZ | handles_null | SELECT now(); | Returns current transaction timestamp |
| nullif | scalar | nullif(any, any) -> any | ANY, ANY | ANY | handles_null | SELECT nullif(10, 10); | Returns null if first argument equals second argument |
| rank | window | rank() -> int8 |  | INT8 | handles_null | SELECT rank() OVER (PARTITION BY dep ORDER BY sal DESC); | Computes rank with gaps across peer groups |
| replace | scalar | replace(text, text, text) -> text | TEXT, TEXT, TEXT | TEXT | returns_null_on_null | SELECT replace('abc_def', '_', '-'); | Replaces occurrences of substring in source string |
| row_number | window | row_number() -> int8 |  | INT8 | handles_null | SELECT row_number() OVER (PARTITION BY dep ORDER BY id); | Returns sequential 1-based index within current window partition |
| rtrim | scalar | rtrim(text) -> text | TEXT | TEXT | returns_null_on_null | SELECT rtrim('hello   '); | Strips trailing whitespace from string |
| substr | scalar | substr(text, int8, int8) -> text | TEXT, INT8, INT8 | TEXT | returns_null_on_null | SELECT substr('rockstream', 1, 4); | Extracts substring starting at 1-based index with optional length |
| substring | scalar | substring(text, int8, int8) -> text | TEXT, INT8, INT8 | TEXT | returns_null_on_null | SELECT substring('rockstream' from 1 for 4); | Extracts substring starting at 1-based index with optional length |
| sum | aggregate | sum(int8) -> int8 | INT8 | INT8 | returns_null_on_null | SELECT sum(amount) FROM t; | Computes sum of 64-bit integer values |
| sum | aggregate | sum(numeric) -> numeric | NUMERIC | NUMERIC | returns_null_on_null | SELECT sum(price) FROM t; | Computes sum of arbitrary-precision numeric values |
| trim | scalar | trim(text) -> text | TEXT | TEXT | returns_null_on_null | SELECT trim('   hello   '); | Removes leading and trailing whitespace |
| upper | scalar | upper(text) -> text | TEXT | TEXT | returns_null_on_null | SELECT upper('rockstream'); | Converts text characters to uppercase |

