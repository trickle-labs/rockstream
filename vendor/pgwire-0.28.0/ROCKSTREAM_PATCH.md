# RockStream security patch for pgwire 0.28.0

Upstream source: https://github.com/sunng87/pgwire/tree/v0.28.0
Original license: dual MIT and Apache-2.0 (retained unchanged here).
Only `src/messages/codec.rs` and `src/error.rs` change production behavior.

The historical 0.28 decoder accepts malformed length fields below the four-byte
header and passes them to `CopyData::decode_body`, which can panic on unsigned
subtraction. An unterminated C string may also cause an out-of-bounds
`BytesMut::split_to`. This local patch rejects invalid or oversized lengths,
isolates one complete message body, returns a protocol error on malformed
remaining bytes, and prevents legacy decoder panics from escaping when Rust's
unwind strategy permits. Packet-size cap: 64 MiB.

Reproducer: `d 00 00 00 00`; scheduled fuzz run
https://github.com/trickle-labs/rockstream/actions/runs/39639160914
and issue https://github.com/trickle-labs/rockstream/issues/227.

This is not a protocol redesign or a claim that an arbitrary newer pgwire major
version is compatible with RockStream. Replace the patch with a qualified
upstream release as soon as one is tested. Keep decoder fuzzing enabled;
never mask errors in the fuzz target or disable the required checks.

Focused regression: `cargo test -p pgwire frame_regressions`, with full
repository checks and a real socket malformed-frame smoke required before merge.
