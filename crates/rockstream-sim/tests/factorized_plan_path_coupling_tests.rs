//! v0.59.7: compile-time selection has no live plan replacement protocol.

const LIVE_EXEC: &str = include_str!("../../rockstream-ops/src/live_exec.rs");
const COMPILE: &str = include_str!("../../rockstream-ops/src/compile.rs");

#[test]
fn factorized_path_has_no_live_cutover_or_switch_generation() {
    let production = [LIVE_EXEC, COMPILE].concat();
    for forbidden in ["switch_generation", "dual_active_graph", "live_cutover"] {
        assert!(
            !production.contains(forbidden),
            "v0.59.7 must reject live plan replacement until its dedicated formal model exists: {forbidden}"
        );
    }
}
