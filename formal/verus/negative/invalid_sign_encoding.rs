use vstd::prelude::*;

verus! {
    fn main() {
        assert((0u64 ^ 0x8000_0000_0000_0000u64) == 0);
    }
}
