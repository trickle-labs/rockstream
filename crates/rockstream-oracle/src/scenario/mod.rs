//! Public-path scenario & differential framework (`TST-001`-`TST-006`, v0.59.17).
//!
//! See `.claude/v0.59.17-plan.md` for scope. Phase 3a implemented the typed
//! DSL, the driver trait and its three drivers, and the typed transcript.
//! Phase 3b implements the unified [`oracle::Oracle`] trait (`TST-004`).
//! Capability proof levels and the differential/metamorphic suites remain
//! out of scope for this phase.

pub mod driver;
pub mod dsl;
pub mod oracle;
pub mod transcript;
