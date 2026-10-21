# v0.72 gateway query producer limits

## DataFusion ad hoc SELECT fallback (v0.72)

The gateway's standalone DataFusion SELECT fallback (queries without a
production relation scan) now checks producer output **while streaming
record batches**, rather than calling unbounded `collect()`. Limits are
10,000 returned rows, 8 MiB of Arrow array memory held for the response,
a 5-second planning/execution/stream-consumption deadline, and 16 concurrent
fallback queries gateway-wide. Overflow fails the request rather than
truncating results. Cancellation or timeout drops the stream and concurrency
permit. Limits apply to this fallback path only; they do not certify every
DataFusion operator's intermediate allocation or the separate distributed
query-time execution path.
