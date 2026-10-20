# v0.72 eager cursor admission

`DECLARE <name> CURSOR FOR SELECT ...` is an eager query producer.
It authorizes the target view using the ordinary SELECT policy and accepts
exactly one read-only SQL query. It admits at most 10,000 returned rows,
8 MiB of encoded row values **across all retained cursors in one connection**,
and a 5-second producer-read deadline. A failed/oversized read creates no cursor.
The storage scan has its own 8 MiB bounded-key/value scan ceiling, and view
readers are asked for at most 10,001 rows to detect truncation.
At most four eager cursor producers may read concurrently on one gateway.
A fifth declaration fails fast with `54000` / `cursor.producer_busy` before
starting another scan. The producer slot is released on success, error,
timeout or request cancellation; this is not an aggregate cross-connection
byte reservation.

Exhausted budgets return SQLSTATE `54000` / `RS-3006`, timed-out reads
return `57014` / `RS-9001`. Existing named-cursor count and duplicate-name
limits still apply.

This bounds the legacy eager cursor lifecycle; it does not claim that all
SQL query plans and intermediate operators are producer-bounded.

Verify with `cargo test -p rockstream-gateway --test v072_bounded_cursor_pgwire_tests`.
