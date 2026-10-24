# VS7-03 PgWire framing review

The vendored `pgwire` decoder calls the executable `read_length` and `frame_span` kernels before advancing the received buffer. Pinned Verus proves the header read bound, checked frame offsets, and zero consumption for incomplete frames. Tests cover every split of canonical Startup, Query, Parse, Bind, and Execute messages, exact decoded fields and trailing bytes, invalid lengths, malformed terminators and counts, and multi-message recovery.

Parse and Bind now reject short or inconsistent field counts before any unchecked read. The existing raw-byte fuzz entry remains active; it also generates valid bounded messages for structured round-trip assertions. A path-scoped PR workflow runs a short decoder-engine budget and retains corpus, minimized inputs, replay logs, and metadata. Longer scheduled soak runs remain in `fuzz-soak.yml`.

The proof excludes message-specific body semantics, Tokio/socket delivery, and future upstream `pgwire` changes. Vendor updates must retain this shared production call path.
