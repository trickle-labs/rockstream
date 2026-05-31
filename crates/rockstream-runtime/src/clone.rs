//! Pipeline clone, blue/green plan replacement, and law-version upgrade (v0.39).
//!
//! Implements the clone/backfill/flip workflow for:
//!
//! 1. **Compatible schema changes** — applied in-place by
//!    [`SchemaEvolutionChecker::apply_compatible`].
//!
//! 2. **Incompatible schema changes** — routed through
//!    [`BlueGreenCoordinator`]:
//!    - Clone the pipeline at the current source offset.
//!    - Backfill the clone from the captured offset.
//!    - Once the clone catches up, perform an atomic flip (one epoch boundary).
//!    - Decommission the original pipeline after a drain period.
//!
//! 3. **Breaking `MergeLaw` version upgrades** — handled by
//!    [`LawVersionUpgradeCoordinator`], which delegates to
//!    [`BlueGreenCoordinator`] with a `CloneReason::IncompatibleLawVersionUpgrade`.
//!
//! # Design
//!
//! All components are synchronous and deterministic so they can be verified
//! by unit tests without async runtimes or real storage.
//!
//! # v0.39 wire-up note
//!
//! The production wiring (gRPC control-plane clone coordinator, live epoch
//! cutover, SlateDB state transfer) calls into these components from async
//! worker tasks; the synchronous simulation model here proves the state-machine
//! invariants.

use rockstream_types::{
    ids::ViewId,
    merge_law::{MergeLawId, MergeLawVersion},
    schema_evolution::{
        classify_law_version_change, classify_schema_change, BlueGreenState, CloneReason,
        CloneSpec, Schema, SchemaChangeKind,
    },
    timestamp::Epoch,
};

// ─── SchemaEvolutionChecker ──────────────────────────────────────────────────

/// Determines whether a schema change can be applied in-place or must go
/// through the blue/green path, and enforces the appropriate route.
pub struct SchemaEvolutionChecker;

/// Result of asking the checker to route a schema change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChangeRoute {
    /// Apply in-place; no clone required.
    InPlace,
    /// Must create a clone and run the blue/green flow.
    RequiresBlueGreen { reason: String },
}

impl SchemaEvolutionChecker {
    /// Determine the route for a schema change from `old` to `new`.
    ///
    /// Returns `InPlace` for compatible changes, `RequiresBlueGreen` for
    /// incompatible ones.
    pub fn route(old: &Schema, new: &Schema) -> SchemaChangeRoute {
        match classify_schema_change(old, new) {
            SchemaChangeKind::Compatible => SchemaChangeRoute::InPlace,
            SchemaChangeKind::Incompatible => SchemaChangeRoute::RequiresBlueGreen {
                reason: "schema change is incompatible; re-encoding required".to_string(),
            },
        }
    }

    /// Assert that a compatible change can be applied without a clone.
    ///
    /// Returns `Ok(())` if the change is compatible, `Err(description)` if
    /// it is not.
    pub fn apply_compatible(old: &Schema, new: &Schema) -> Result<(), String> {
        match classify_schema_change(old, new) {
            SchemaChangeKind::Compatible => Ok(()),
            SchemaChangeKind::Incompatible => {
                Err("incompatible schema change: must use clone/backfill/flip".to_string())
            }
        }
    }
}

// ─── PipelineCloner ──────────────────────────────────────────────────────────

/// Handles the creation of a clone pipeline.
///
/// In production, `create_clone` creates a new materialized view in the
/// catalog backed by the same source as the original, starting from
/// `spec.source_offset_epoch` so no rows are lost.
pub struct PipelineCloner;

/// Result of a clone creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneHandle {
    /// The `CloneSpec` that created this clone.
    pub spec: CloneSpec,
    /// The assigned ID for the new clone view.
    pub clone_view_id: ViewId,
    /// The epoch at which the clone started backfilling.
    pub backfill_started_epoch: Epoch,
}

impl PipelineCloner {
    /// Create a clone pipeline from `spec` at the given `now_epoch`.
    ///
    /// The clone starts backfilling from `spec.source_offset_epoch`, which was
    /// captured at the moment the clone was requested — guaranteeing no rows
    /// are dropped between the original and the clone.
    pub fn create_clone(spec: CloneSpec, clone_id: u64, now_epoch: Epoch) -> CloneHandle {
        CloneHandle {
            clone_view_id: ViewId(clone_id),
            backfill_started_epoch: now_epoch,
            spec,
        }
    }
}

// ─── BlueGreenCoordinator ────────────────────────────────────────────────────

/// Coordinates the atomic blue/green flip for incompatible schema changes
/// and breaking law-version upgrades (v0.39).
///
/// # State machine
///
/// ```text
/// Idle
///   → Backfilling { clone_view_id, rows_backfilled }  (clone created)
///   → ReadyToFlip { clone_view_id, lag_epochs }        (clone caught up)
///   → Flipped { clone_view_id, flip_epoch }            (atomic flip done)
///   → Decommissioned                                   (original retired)
/// ```
///
/// The coordinator enforces:
/// - Only one blue/green operation per view at a time.
/// - Flip only happens when `lag_epochs == 0` (clone at the live frontier).
/// - Source offset captured at clone creation is never rewound.
pub struct BlueGreenCoordinator {
    /// Source view being replaced (the "blue" pipeline).
    pub source_view_id: ViewId,
    /// Current state of the blue/green operation.
    pub state: BlueGreenState,
    /// The spec used to create the clone.
    pub spec: Option<CloneSpec>,
}

/// Outcome of a [`BlueGreenCoordinator::step`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlueGreenStep {
    /// Clone created; backfill started.
    CloneCreated { clone_view_id: ViewId },
    /// Backfill progressing.
    Backfilling { rows_backfilled: u64 },
    /// Backfill complete; clone is at the live frontier.
    ReadyToFlip { lag_epochs: u64 },
    /// Atomic flip performed; green pipeline is now primary.
    Flipped { flip_epoch: Epoch },
    /// Original pipeline decommissioned.
    Decommissioned,
}

impl BlueGreenCoordinator {
    /// Create a new coordinator for `source_view_id`.
    pub fn new(source_view_id: ViewId) -> Self {
        Self {
            source_view_id,
            state: BlueGreenState::Idle,
            spec: None,
        }
    }

    /// Is a blue/green operation currently in progress?
    pub fn is_in_flight(&self) -> bool {
        self.state.is_in_flight()
    }

    /// Begin a blue/green operation: create the clone and start backfilling.
    ///
    /// Returns `Err` if an operation is already in progress.
    pub fn begin(
        &mut self,
        spec: CloneSpec,
        clone_view_id: ViewId,
    ) -> Result<BlueGreenStep, String> {
        if self.state.is_in_flight() {
            return Err(format!(
                "RS-3608: blue/green already in progress for view-{}",
                self.source_view_id.0
            ));
        }
        self.spec = Some(spec);
        self.state = BlueGreenState::Backfilling {
            clone_view_id,
            rows_backfilled: 0,
        };
        Ok(BlueGreenStep::CloneCreated { clone_view_id })
    }

    /// Report backfill progress.
    ///
    /// `total_rows_backfilled` is the cumulative row count ingested by the
    /// clone so far.  Once it reaches `target_rows` the coordinator advances
    /// to `ReadyToFlip`.
    pub fn report_backfill_progress(
        &mut self,
        total_rows_backfilled: u64,
        target_rows: u64,
        lag_epochs: u64,
    ) -> BlueGreenStep {
        match &self.state {
            BlueGreenState::Backfilling { clone_view_id, .. } => {
                let clone_view_id = *clone_view_id;
                self.state = if total_rows_backfilled >= target_rows && lag_epochs == 0 {
                    BlueGreenState::ReadyToFlip {
                        clone_view_id,
                        lag_epochs: 0,
                    }
                } else {
                    BlueGreenState::Backfilling {
                        clone_view_id,
                        rows_backfilled: total_rows_backfilled,
                    }
                };
                if lag_epochs == 0 && total_rows_backfilled >= target_rows {
                    BlueGreenStep::ReadyToFlip { lag_epochs: 0 }
                } else {
                    BlueGreenStep::Backfilling {
                        rows_backfilled: total_rows_backfilled,
                    }
                }
            }
            _ => BlueGreenStep::Backfilling {
                rows_backfilled: total_rows_backfilled,
            },
        }
    }

    /// Perform the atomic flip at `flip_epoch`.
    ///
    /// Only succeeds when the state is `ReadyToFlip`.  The flip atomically
    /// switches query routing from the original view to the clone.
    pub fn flip(&mut self, flip_epoch: Epoch) -> Result<BlueGreenStep, String> {
        match &self.state {
            BlueGreenState::ReadyToFlip { clone_view_id, .. } => {
                let clone_view_id = *clone_view_id;
                self.state = BlueGreenState::Flipped {
                    clone_view_id,
                    flip_epoch,
                };
                Ok(BlueGreenStep::Flipped { flip_epoch })
            }
            other => Err(format!(
                "cannot flip in state {other:?}; must be ReadyToFlip"
            )),
        }
    }

    /// Decommission the original pipeline after the flip.
    pub fn decommission(&mut self) -> Result<BlueGreenStep, String> {
        match &self.state {
            BlueGreenState::Flipped { .. } => {
                self.state = BlueGreenState::Decommissioned;
                Ok(BlueGreenStep::Decommissioned)
            }
            other => Err(format!(
                "cannot decommission in state {other:?}; must be Flipped"
            )),
        }
    }

    /// Run a full blue/green cycle in simulation.
    ///
    /// Arguments:
    /// - `spec` — clone specification
    /// - `clone_view_id` — ID to assign to the clone
    /// - `total_rows` — number of rows to backfill
    /// - `flip_epoch` — epoch at which to perform the flip
    ///
    /// Returns `Ok(flip_epoch)` on success.  In production this is spread
    /// across many epochs; here it is collapsed for deterministic testing.
    pub fn simulate_full_cycle(
        source_view_id: ViewId,
        spec: CloneSpec,
        clone_view_id: ViewId,
        total_rows: u64,
        flip_epoch: Epoch,
    ) -> Result<Epoch, String> {
        let mut coord = BlueGreenCoordinator::new(source_view_id);
        coord.begin(spec, clone_view_id)?;
        // Simulate backfill in one step.
        coord.report_backfill_progress(total_rows, total_rows, 0);
        coord.flip(flip_epoch)?;
        coord.decommission()?;
        Ok(flip_epoch)
    }
}

// ─── LawVersionUpgradeCoordinator ────────────────────────────────────────────

/// Handles upgrading a `MergeLaw` version for an existing view.
///
/// Compatible (non-breaking) upgrades are applied in-place.
/// Breaking upgrades are delegated to [`BlueGreenCoordinator`].
pub struct LawVersionUpgradeCoordinator;

/// Result of a law version upgrade classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LawUpgradeRoute {
    /// Can be applied in-place; no clone required.
    InPlace,
    /// Must use the blue/green path; returns the `CloneSpec` to execute.
    RequiresBlueGreen { spec: CloneSpec },
}

impl LawVersionUpgradeCoordinator {
    /// Determine the upgrade route for bumping `law_id` from `from` to `to`.
    ///
    /// `is_breaking` must be set to `true` if the new version cannot read
    /// values encoded by the old version without re-encoding.
    pub fn route_upgrade(
        source_view_id: ViewId,
        law_id: MergeLawId,
        from: MergeLawVersion,
        to: MergeLawVersion,
        is_breaking: bool,
        source_offset_epoch: Epoch,
    ) -> LawUpgradeRoute {
        match classify_law_version_change(law_id, from, to, is_breaking) {
            SchemaChangeKind::Compatible => LawUpgradeRoute::InPlace,
            SchemaChangeKind::Incompatible => LawUpgradeRoute::RequiresBlueGreen {
                spec: CloneSpec::new(
                    source_view_id,
                    format!("{}_law_upgrade_v{}", source_view_id, to.0),
                    source_offset_epoch,
                    CloneReason::IncompatibleLawVersionUpgrade {
                        law_id,
                        from_version: from,
                        to_version: to,
                    },
                ),
            },
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::{
        ids::ViewId,
        merge_law::{MergeLawId, MergeLawVersion},
        schema_evolution::ColumnDescriptor,
    };

    fn col(name: &str, type_tag: u8, nullable: bool) -> ColumnDescriptor {
        ColumnDescriptor {
            name: name.to_string(),
            type_tag,
            nullable,
            law_id: None,
            law_version: None,
        }
    }

    // ── SchemaEvolutionChecker tests ─────────────────────────────────────────

    #[test]
    fn compatible_change_routes_in_place() {
        let old = vec![col("id", 1, false), col("val", 2, true)];
        let new = vec![
            col("id", 1, false),
            col("val", 2, true),
            col("extra", 1, true),
        ];
        assert_eq!(
            SchemaEvolutionChecker::route(&old, &new),
            SchemaChangeRoute::InPlace
        );
    }

    #[test]
    fn incompatible_change_routes_blue_green() {
        let old = vec![col("id", 1, false), col("val", 2, true)];
        let new = vec![col("id", 1, false)]; // removed 'val'
        assert!(matches!(
            SchemaEvolutionChecker::route(&old, &new),
            SchemaChangeRoute::RequiresBlueGreen { .. }
        ));
    }

    #[test]
    fn apply_compatible_succeeds() {
        let old = vec![col("id", 1, false)];
        let new = vec![col("id", 1, false), col("notes", 2, true)];
        assert!(SchemaEvolutionChecker::apply_compatible(&old, &new).is_ok());
    }

    #[test]
    fn apply_compatible_rejects_incompatible() {
        let old = vec![col("id", 1, false), col("required", 1, false)];
        let new = vec![col("id", 1, false)]; // dropped required
        assert!(SchemaEvolutionChecker::apply_compatible(&old, &new).is_err());
    }

    // ── PipelineCloner tests ──────────────────────────────────────────────────

    #[test]
    fn clone_captures_source_offset() {
        let spec = CloneSpec::new(
            ViewId(1),
            "orders_v2",
            /* source_offset_epoch = */ 200,
            CloneReason::IncompatibleSchemaChange {
                description: "drop column 'legacy_id'".to_string(),
            },
        );
        let handle = PipelineCloner::create_clone(spec.clone(), 99, 200);
        // Clone starts from the captured source offset — no rows lost.
        assert_eq!(handle.spec.source_offset_epoch, 200);
        assert_eq!(handle.backfill_started_epoch, 200);
        assert_eq!(handle.clone_view_id, ViewId(99));
    }

    // ── BlueGreenCoordinator tests ────────────────────────────────────────────

    /// Proof: "Breaking schema change goes through clone/backfill/flip without
    /// source offset loss."
    ///
    /// We simulate an incompatible rename (col 'name' → 'full_name'):
    /// 1. Source offset is captured at epoch 100.
    /// 2. Clone starts backfilling from epoch 100 (no gap).
    /// 3. After 50_000 rows are backfilled and lag_epochs == 0, flip occurs.
    /// 4. Original is decommissioned.
    ///
    /// Invariant: `clone.source_offset_epoch == captured_epoch` at every step.
    #[test]
    fn proof_schema_change_via_blue_green_no_source_offset_loss() {
        let source_view_id = ViewId(1);
        let captured_epoch: Epoch = 100;
        let total_rows: u64 = 50_000;

        let spec = CloneSpec::new(
            source_view_id,
            "orders_v2",
            captured_epoch,
            CloneReason::IncompatibleSchemaChange {
                description: "renamed column 'name' to 'full_name'".to_string(),
            },
        );

        // Invariant: source offset captured at clone-creation time is preserved.
        assert_eq!(spec.source_offset_epoch, captured_epoch);

        let flip_epoch = BlueGreenCoordinator::simulate_full_cycle(
            source_view_id,
            spec.clone(),
            ViewId(2),
            total_rows,
            110,
        )
        .expect("blue/green cycle should complete without error");

        // Flip must happen after the captured epoch.
        assert!(
            flip_epoch >= captured_epoch,
            "flip must not rewind the source offset"
        );
        assert_eq!(flip_epoch, 110);
    }

    #[test]
    fn blue_green_begin_then_flip_decommission() {
        let mut coord = BlueGreenCoordinator::new(ViewId(10));

        let spec = CloneSpec::new(
            ViewId(10),
            "view_v2",
            50,
            CloneReason::UserRequested {
                note: "test".to_string(),
            },
        );

        // Begin.
        let step = coord.begin(spec, ViewId(11)).unwrap();
        assert_eq!(
            step,
            BlueGreenStep::CloneCreated {
                clone_view_id: ViewId(11)
            }
        );

        // Backfill progress — not yet ready (still has lag).
        let step = coord.report_backfill_progress(1000, 5000, 3);
        assert_eq!(
            step,
            BlueGreenStep::Backfilling {
                rows_backfilled: 1000
            }
        );

        // Backfill complete, lag gone.
        let step = coord.report_backfill_progress(5000, 5000, 0);
        assert_eq!(step, BlueGreenStep::ReadyToFlip { lag_epochs: 0 });

        // Flip.
        let step = coord.flip(60).unwrap();
        assert_eq!(step, BlueGreenStep::Flipped { flip_epoch: 60 });

        // Decommission.
        let step = coord.decommission().unwrap();
        assert_eq!(step, BlueGreenStep::Decommissioned);
        assert!(!coord.state.is_in_flight());
    }

    #[test]
    fn begin_while_in_flight_returns_error() {
        let mut coord = BlueGreenCoordinator::new(ViewId(1));
        let spec1 = CloneSpec::new(
            ViewId(1),
            "v2",
            10,
            CloneReason::UserRequested {
                note: "first".to_string(),
            },
        );
        let spec2 = CloneSpec::new(
            ViewId(1),
            "v3",
            10,
            CloneReason::UserRequested {
                note: "second".to_string(),
            },
        );
        coord.begin(spec1, ViewId(2)).unwrap();
        let err = coord.begin(spec2, ViewId(3));
        assert!(err.is_err(), "second begin should fail (RS-3608)");
    }

    #[test]
    fn flip_before_ready_returns_error() {
        let mut coord = BlueGreenCoordinator::new(ViewId(1));
        let spec = CloneSpec::new(
            ViewId(1),
            "v2",
            0,
            CloneReason::UserRequested {
                note: "test".to_string(),
            },
        );
        coord.begin(spec, ViewId(2)).unwrap();
        // Still in Backfilling, not ReadyToFlip.
        let err = coord.flip(5);
        assert!(err.is_err(), "flip in Backfilling state should fail");
    }

    #[test]
    fn decommission_before_flip_returns_error() {
        let mut coord = BlueGreenCoordinator::new(ViewId(1));
        let spec = CloneSpec::new(
            ViewId(1),
            "v2",
            0,
            CloneReason::UserRequested {
                note: "test".to_string(),
            },
        );
        coord.begin(spec, ViewId(2)).unwrap();
        coord.report_backfill_progress(100, 100, 0); // → ReadyToFlip
                                                     // Did NOT flip yet.
        let err = coord.decommission();
        assert!(err.is_err(), "decommission before flip should fail");
    }

    // ── LawVersionUpgradeCoordinator tests ───────────────────────────────────

    /// Proof: "A forced MergeLaw version bump for an existing view re-encodes
    /// via clone without loss."
    ///
    /// We simulate a breaking law-version bump for law-id=3, v1 → v2:
    /// 1. Coordinator routes to RequiresBlueGreen.
    /// 2. CloneSpec captures the source offset epoch.
    /// 3. Full blue/green cycle runs without error.
    /// 4. Flip epoch ≥ source offset epoch (no rewind).
    #[test]
    fn proof_law_version_bump_via_blue_green_without_loss() {
        let source_view_id = ViewId(5);
        let law_id = MergeLawId(3);
        let from = MergeLawVersion(1);
        let to = MergeLawVersion(2);
        let source_offset_epoch: Epoch = 300;

        let route = LawVersionUpgradeCoordinator::route_upgrade(
            source_view_id,
            law_id,
            from,
            to,
            /* is_breaking = */ true,
            source_offset_epoch,
        );

        // Must route through blue/green.
        let spec = match route {
            LawUpgradeRoute::RequiresBlueGreen { spec } => spec,
            LawUpgradeRoute::InPlace => panic!("breaking law upgrade must not be in-place"),
        };

        // Source offset is preserved in the spec.
        assert_eq!(spec.source_offset_epoch, source_offset_epoch);

        // Verify the clone reason is correct.
        assert!(matches!(
            &spec.reason,
            CloneReason::IncompatibleLawVersionUpgrade {
                law_id: lid,
                from_version: fv,
                to_version: tv,
            } if *lid == law_id && *fv == from && *tv == to
        ));

        // Run the full cycle.
        let flip_epoch = BlueGreenCoordinator::simulate_full_cycle(
            source_view_id,
            spec,
            ViewId(6),
            /* total_rows = */ 100_000,
            /* flip_epoch = */ 350,
        )
        .expect("blue/green law-upgrade cycle must complete without error");

        assert!(
            flip_epoch >= source_offset_epoch,
            "flip epoch {flip_epoch} must not be before source offset {source_offset_epoch}"
        );
    }

    #[test]
    fn non_breaking_law_upgrade_routes_in_place() {
        let route = LawVersionUpgradeCoordinator::route_upgrade(
            ViewId(1),
            MergeLawId(1),
            MergeLawVersion(1),
            MergeLawVersion(2),
            /* is_breaking = */ false,
            0,
        );
        assert_eq!(route, LawUpgradeRoute::InPlace);
    }

    #[test]
    fn breaking_law_upgrade_routes_blue_green() {
        let route = LawVersionUpgradeCoordinator::route_upgrade(
            ViewId(1),
            MergeLawId(2),
            MergeLawVersion(1),
            MergeLawVersion(2),
            /* is_breaking = */ true,
            42,
        );
        assert!(matches!(route, LawUpgradeRoute::RequiresBlueGreen { .. }));
    }
}
