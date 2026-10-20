# v0.72 eager cursor admission

`DECLARE <name> CURSOR FOR SELECT ...` is an eager query producer.
It authorizes the target view using the ordinary SELECT policy and accepts
exactly one read-only SQL query. It admits at most 10,000 returned rows,
8 MiB of encoded row values **across all retained cursors in one connection**,
and a 5-second producer-read deadline. A failed/oversized read creates no cursor.
The storage scan has its own 8 MiB bounded-key/value scan ceiling, and view
readers are asked for at most 10,001 rows to detect truncation.
Exhausted budgets return SQLSTATE `54000` / `RS-3006`, timed-out reads
return `57014` / `RS-9001`. Existing named-cursor count and duplicate-name
limits still apply.

This bounds the legacy eager cursor lifecycle; it does not claim that all
SQL query plans and intermediate operators are producer-bounded.
