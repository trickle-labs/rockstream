# RockStream security patch for pgwire 0.28.0

Upstream source: https://github.com/sunng87/pgwire/tree/v0.28.0
Original license: dual MIT and Apache-2.0 (retained unchanged here).
Only `src/messages/codec.rs`, `src/messages/simplequery.rs`, `src/messages/extendedquery.rs` and `src/error.rs` change production behavior.

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

Focused regressions: `cargo test --manifest-path vendor/pgwire-0.28.0/Cargo.toml frame_regressions` and
`cargo test -p rockstream-gateway --test v072_pgwire_malformed_frame_tests`,
with full repository checks and a real socket malformed-frame smoke required
before merge. The vendored package is intentionally excluded from workspace
membership, but remains the patched Cargo dependency.

## 2026-10-22 follow-up

Scheduled main fuzz [39720408746](https://github.com/trickle-labs/rockstream/actions/runs/39720408746) minimized a distinct `Execute` panic to `45 00 00 00 04` (a frame with no body); this is not the earlier `CopyData` length-zero bug. `Execute::decode_body` now checks for the required portal-name NUL and four-byte row count before calling `get_i32`, avoiding abort-on-panic in fuzzer builds. Added short-message and valid-boundary regression; full CI and scheduled fuzz verification remain required.

## 2026-10-23 follow-up

Long-duration scheduled main decoder fuzz run [39898746096](https://github.com/trickle-labs/rockstream/actions/runs/39898746096) reproduced a third truncated-message panic, minimized to hex `50 00 00 00 04` (`Parse` with no body). This affects the existing pinned decoder where `Parse::decode_body` unconditionally reads OID count and OID values. The narrow correction checks both NUL-terminated strings, the two-byte OID count, and the exact array byte length before any unchecked `Buf` getter; it includes minimized-input and valid-message regressions. The prior CopyData and Execute corrections remain in force. Verify with the same strict fuzz engine and full required CI on this exact head; a PR-only green result is not sufficient evidence that every malformed frontend message is safe.

## 2026-10-25 follow-up

Close (`C`) and Describe (`D`) are also production frontend messages.
Their 0.28 body decoders used `Buf::get_u8` without ensuring that the target
kind or NUL-terminated name existed. Validate the full body (including target
kind, single terminating NUL, and absence of extra fields) before any getter.
Direct-body tests cover malformed input without consuming it and valid round
trips. The authenticated TCP regression sends empty-body Close/Describe frames
and verifies a subsequent healthy client remains usable. This specifically
hardens abort-on-panic fuzzing; it is not exhaustive PgWire protocol security
qualification and does not replace exact-head CI and fuzz verification.

## 2026-10-25 active-COPY TCP and disconnect follow-up

The existing malformed `CopyData` socket regression sent the minimized frame
after authentication but outside an active `COPY FROM STDIN` session.
The gateway regression now creates a real table, enters COPY IN and verifies
the server's `CopyInResponse` before sending malformed zero/negative-length
`CopyData` frames and incomplete header/body input. Complete valid split frames
still reach COPY's row buffer. EOF and malformed-frame disconnects must release
the COPY session and return all connection-state maps and the buffered-row
metric to baseline. A separate healthy PostgreSQL client remains usable.

The gateway's `CopyState` destructor now reconciles unflushed buffered-row
accounting when a connection is aborted or discarded without CopyDone/CopyFail.
These tests exercise the production TCP path but do not prove that every
PgWire frame, all stateful COPY semantics, or all network-failure classes
are safe. Require exact-head CI, longer decoder fuzz, and failure/cancellation
runner checks before integration.
