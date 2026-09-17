# Verus model map

VS0 covers one executable planning kernel.

| Production code | Verus contract | Caller obligation |
|---|---|---|
| `crates/rockstream-verified/src/lib.rs::normalize_power_of_two_bucket_count` | Every `u16` input returns a power of two in `1..=32768`; valid powers are preserved; zero and oversized inputs follow the documented clamp. | `rockstream-plan` keeps the public wrapper and uses the verified executable function. |

This pilot has no protocol-state correspondence. FizzBee remains the owner of the existing protocol models.
