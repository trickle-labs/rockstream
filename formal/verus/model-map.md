# Verus model map

VS0 covers one executable planning kernel.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-verified/src/lib.rs::normalize_power_of_two_bucket_count` | Every `u16` input returns a power of two in `1..=32768`; valid powers are preserved; zero and oversized inputs follow the documented clamp. | `rockstream-plan` keeps the public wrapper and uses the verified executable function. |

VS1 covers checked scalar and merge-law adapters.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-types/src/laws/arithmetic.rs` | Checked addition, multiplication, negation, narrowing, and exact-width scalar decoding return success only for representable values and exact formats. | Law and storage callers map failure to the existing non-retryable RS error paths. |
| `crates/rockstream-types/src/laws/weight_add.rs`, `sum_count.rs` | Mathematical identities are exposed only with the checked `i64` executable domain; regrouping requires an absolute-sum admission. | Optimizers inspect `LawDescriptor::can_reassociate`; otherwise they use the spill fallback. |
| `crates/rockstream-storage/src/merge_registry.rs` | Existing tagged formats are preserved while first operands, tags, widths, and checked results are validated. | Storage never treats a malformed no-existing operand as an initial value. |

This pilot has no protocol-state correspondence. FizzBee remains the owner of the existing protocol models.
