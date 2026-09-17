use vstd::prelude::*;

verus! {
    fn unchecked_assumption() {
        assume(true);
    }
}
