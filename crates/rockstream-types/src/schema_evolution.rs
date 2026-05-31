//! Schema evolution and pipeline clone types (v0.39).
//!
//! These types support the clone/backfill/flip blue-green workflow for
//! schema changes and `MergeLaw` version upgrades.
//!
//! # Workflow
//!
//! ```text
//! 1. User detects or declares an incompatible change.
//! 2. Control plane calls CloneSpec::new() and creates a clone pipeline (v2).
//! 3. v2 backfills from the source offset captured at clone time (no gap).
//! 4. Once v2 catches up to the live frontier the control plane performs the
//!    atomic flip: query routing switches from v1 → v2 in one epoch.
//! 5. v1 is decommissioned after a configurable drain period.
//! ```
//!
//! Compatible changes (e.g. adding a nullable column) are applied in-place
//! without a clone.

use serde::{Deserialize, Serialize};

use crate::ids::ViewId;
use crate::merge_law::{MergeLawId, MergeLawVersion};
use crate::timestamp::Epoch;

// ─── Schema change classification ────────────────────────────────────────────

/// Whether a schema change can be applied in-place or requires the
/// blue/green clone/backfill/flip path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaChangeKind {
    /// The change preserves the existing encoded values; can be applied
    /// without a clone (e.g. adding a nullable column, widening an integer
    /// type, or a new merge-law version that reads old values unchanged).
    Compatible,
    /// The change requires re-encoding existing data; must go through
    /// clone/backfill/flip (e.g. renaming or dropping a column, narrowing
    /// a type, or a breaking `MergeLaw` version bump).
    Incompatible,
}

impl SchemaChangeKind {
    /// Returns `true` if the change requires the blue/green path.
    pub fn requires_blue_green(self) -> bool {
        matches!(self, Self::Incompatible)
    }
}

impl std::fmt::Display for SchemaChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compatible => write!(f, "COMPATIBLE"),
            Self::Incompatible => write!(f, "INCOMPATIBLE"),
        }
    }
}

// ─── Clone specification ──────────────────────────────────────────────────────

/// Specification for cloning a pipeline (v0.39).
///
/// Captures all the information needed to create a v2 pipeline from an
/// existing v1 pipeline and schedule the blue/green flip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneSpec {
    /// The view being cloned (the "blue" / v1 pipeline).
    pub source_view_id: ViewId,
    /// The new view name for the clone (the "green" / v2 pipeline).
    pub clone_view_name: String,
    /// The source offset epoch captured at clone creation time.
    /// The clone will replay from this epoch forward so no rows are lost.
    pub source_offset_epoch: Epoch,
    /// Reason for the clone (audit trail).
    pub reason: CloneReason,
}

impl CloneSpec {
    /// Create a new clone specification.
    pub fn new(
        source_view_id: ViewId,
        clone_view_name: impl Into<String>,
        source_offset_epoch: Epoch,
        reason: CloneReason,
    ) -> Self {
        Self {
            source_view_id,
            clone_view_name: clone_view_name.into(),
            source_offset_epoch,
            reason,
        }
    }
}

/// Why a pipeline clone was requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloneReason {
    /// Incompatible schema change (column rename/drop/type narrowing).
    IncompatibleSchemaChange {
        /// Human-readable description of the change.
        description: String,
    },
    /// Incompatible `MergeLaw` version upgrade.
    IncompatibleLawVersionUpgrade {
        /// Which law is being upgraded.
        law_id: MergeLawId,
        /// Old version.
        from_version: MergeLawVersion,
        /// New version.
        to_version: MergeLawVersion,
    },
    /// Explicit user-requested clone (e.g. blue/green A/B test).
    UserRequested {
        /// Free-text annotation from the user.
        note: String,
    },
}

// ─── Blue/green state machine ─────────────────────────────────────────────────

/// State of a blue/green replacement operation (v0.39).
///
/// Transitions:
/// ```text
/// Idle
///   → Backfilling { clone_view_id, rows_backfilled }
///   → ReadyToFlip { clone_view_id, lag_epochs }
///   → Flipped { clone_view_id, flip_epoch }
///   → Decommissioned
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlueGreenState {
    /// No blue/green operation in progress.
    Idle,
    /// Clone pipeline is being backfilled.
    Backfilling {
        /// View ID of the clone (green pipeline).
        clone_view_id: ViewId,
        /// Rows ingested into the clone so far.
        rows_backfilled: u64,
    },
    /// Backfill complete; clone is caught up and ready for the atomic flip.
    /// The flip should be scheduled at the next epoch boundary.
    ReadyToFlip {
        /// View ID of the clone (green pipeline).
        clone_view_id: ViewId,
        /// How many epochs behind the live frontier the clone is.
        lag_epochs: u64,
    },
    /// Flip performed; the clone is now the primary pipeline.
    /// The original view (blue) is being drained.
    Flipped {
        /// View ID of the new primary (former clone).
        clone_view_id: ViewId,
        /// Epoch at which the flip was performed.
        flip_epoch: Epoch,
    },
    /// Original view has been decommissioned.
    Decommissioned,
}

impl BlueGreenState {
    /// Returns `true` if a blue/green operation is currently in flight.
    pub fn is_in_flight(&self) -> bool {
        !matches!(self, Self::Idle | Self::Decommissioned)
    }

    /// Returns `true` if the flip has already been performed.
    pub fn is_flipped(&self) -> bool {
        matches!(self, Self::Flipped { .. } | Self::Decommissioned)
    }
}

// ─── Schema column descriptor ────────────────────────────────────────────────

/// Simple descriptor for a schema column, used for compatibility checking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    /// Column name.
    pub name: String,
    /// Encoded type tag (opaque u8 for simplicity; production uses Arrow DataType).
    pub type_tag: u8,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// The merge law attached to this column, if any.
    pub law_id: Option<MergeLawId>,
    /// The merge law version attached to this column, if any.
    pub law_version: Option<MergeLawVersion>,
}

/// A schema is a sequence of column descriptors.
pub type Schema = Vec<ColumnDescriptor>;

// ─── Schema compatibility check ───────────────────────────────────────────────

/// Classify the schema change from `old` to `new`.
///
/// Returns [`SchemaChangeKind::Compatible`] for purely additive or widening
/// changes, [`SchemaChangeKind::Incompatible`] for anything that requires
/// re-encoding (column removal, rename, type narrowing, or a breaking law
/// version bump).
///
/// # Rules
///
/// | Change | Classification |
/// |--------|---------------|
/// | Add a new nullable column | Compatible |
/// | Widen an integer type (e.g. u32 → u64, represented as type_tag increase) | Compatible |
/// | Remove a column | Incompatible |
/// | Rename a column (detected as remove + add) | Incompatible |
/// | Narrow a type (type_tag decrease) | Incompatible |
/// | Add a new non-nullable column | Incompatible |
/// | Change column from nullable → non-nullable | Incompatible |
/// | Same `law_id` but higher `law_version` (minor bump) | Compatible |
/// | Different `law_id` or `law_version` decreases | Incompatible |
pub fn classify_schema_change(old: &Schema, new: &Schema) -> SchemaChangeKind {
    use SchemaChangeKind::*;

    // Build lookup maps keyed by column name.
    let old_cols: std::collections::HashMap<&str, &ColumnDescriptor> =
        old.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_cols: std::collections::HashMap<&str, &ColumnDescriptor> =
        new.iter().map(|c| (c.name.as_str(), c)).collect();

    // Check for removed columns.
    for name in old_cols.keys() {
        if !new_cols.contains_key(name) {
            return Incompatible;
        }
    }

    for (name, new_col) in &new_cols {
        match old_cols.get(name) {
            // New column.
            None => {
                if !new_col.nullable {
                    return Incompatible; // non-nullable addition requires re-encode
                }
                // nullable addition: compatible
            }
            Some(old_col) => {
                // Type narrowing.
                if new_col.type_tag < old_col.type_tag {
                    return Incompatible;
                }
                // Nullable → non-nullable.
                if old_col.nullable && !new_col.nullable {
                    return Incompatible;
                }
                // Law ID changed.
                if new_col.law_id != old_col.law_id {
                    return Incompatible;
                }
                // Law version decreased (downgrade not allowed).
                if let (Some(new_v), Some(old_v)) = (new_col.law_version, old_col.law_version) {
                    if new_v < old_v {
                        return Incompatible;
                    }
                }
            }
        }
    }

    Compatible
}

/// Classify a `MergeLaw` version bump as compatible or incompatible.
///
/// A bump is compatible if it is a minor increase within the same law ID
/// (the new version can read values encoded by the old version unchanged).
/// Any law-ID change or a decrease in version is incompatible.
pub fn classify_law_version_change(
    law_id: MergeLawId,
    from: MergeLawVersion,
    to: MergeLawVersion,
    is_breaking: bool,
) -> SchemaChangeKind {
    let _ = law_id; // law_id is provided for future law-change detection; currently unused
    if to >= from && !is_breaking {
        SchemaChangeKind::Compatible
    } else {
        SchemaChangeKind::Incompatible
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ViewId;

    fn col(name: &str, type_tag: u8, nullable: bool) -> ColumnDescriptor {
        ColumnDescriptor {
            name: name.to_string(),
            type_tag,
            nullable,
            law_id: None,
            law_version: None,
        }
    }

    fn col_with_law(name: &str, type_tag: u8, law_id: u16, law_version: u16) -> ColumnDescriptor {
        ColumnDescriptor {
            name: name.to_string(),
            type_tag,
            nullable: false,
            law_id: Some(MergeLawId(law_id)),
            law_version: Some(MergeLawVersion(law_version)),
        }
    }

    // ── SchemaChangeKind tests ────────────────────────────────────────────────

    #[test]
    fn add_nullable_column_is_compatible() {
        let old = vec![col("id", 1, false), col("name", 2, true)];
        let new = vec![
            col("id", 1, false),
            col("name", 2, true),
            col("email", 2, true),
        ];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn remove_column_is_incompatible() {
        let old = vec![col("id", 1, false), col("name", 2, true)];
        let new = vec![col("id", 1, false)];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn rename_column_is_incompatible() {
        // Detected as: old `name` removed + new `full_name` added (non-nullable → incompatible).
        let old = vec![col("id", 1, false), col("name", 2, false)];
        let new = vec![col("id", 1, false), col("full_name", 2, false)];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn type_narrowing_is_incompatible() {
        let old = vec![col("amount", 4, false)]; // u64 (tag=4)
        let new = vec![col("amount", 2, false)]; // u16 (tag=2)
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn type_widening_is_compatible() {
        let old = vec![col("amount", 2, false)]; // u16 (tag=2)
        let new = vec![col("amount", 4, false)]; // u64 (tag=4)
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn add_non_nullable_column_is_incompatible() {
        let old = vec![col("id", 1, false)];
        let new = vec![col("id", 1, false), col("required_field", 1, false)];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn nullable_to_non_nullable_is_incompatible() {
        let old = vec![col("name", 2, true)];
        let new = vec![col("name", 2, false)];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn no_change_is_compatible() {
        let schema = vec![col("id", 1, false), col("val", 2, true)];
        assert_eq!(
            classify_schema_change(&schema, &schema),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn law_minor_version_bump_is_compatible() {
        let old = vec![col_with_law("counter", 8, 1, 1)];
        let new = vec![col_with_law("counter", 8, 1, 2)]; // same law, higher version
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn law_version_downgrade_is_incompatible() {
        let old = vec![col_with_law("counter", 8, 1, 3)];
        let new = vec![col_with_law("counter", 8, 1, 2)]; // same law, lower version
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn law_id_change_is_incompatible() {
        let old = vec![col_with_law("val", 8, 1, 1)];
        let new = vec![col_with_law("val", 8, 2, 1)]; // different law ID
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    // ── SchemaChangeKind method tests ─────────────────────────────────────────

    #[test]
    fn schema_change_kind_requires_blue_green() {
        assert!(!SchemaChangeKind::Compatible.requires_blue_green());
        assert!(SchemaChangeKind::Incompatible.requires_blue_green());
    }

    #[test]
    fn schema_change_kind_display() {
        assert_eq!(SchemaChangeKind::Compatible.to_string(), "COMPATIBLE");
        assert_eq!(SchemaChangeKind::Incompatible.to_string(), "INCOMPATIBLE");
    }

    // ── CloneSpec tests ───────────────────────────────────────────────────────

    #[test]
    fn clone_spec_roundtrip() {
        let spec = CloneSpec::new(
            ViewId(1),
            "orders_v2",
            100,
            CloneReason::IncompatibleSchemaChange {
                description: "renamed column 'name' to 'full_name'".to_string(),
            },
        );
        let json = serde_json::to_string(&spec).unwrap();
        let decoded: CloneSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, decoded);
        assert_eq!(decoded.clone_view_name, "orders_v2");
        assert_eq!(decoded.source_offset_epoch, 100);
    }

    #[test]
    fn law_upgrade_clone_spec_roundtrip() {
        let spec = CloneSpec::new(
            ViewId(2),
            "counters_v2",
            500,
            CloneReason::IncompatibleLawVersionUpgrade {
                law_id: MergeLawId(3),
                from_version: MergeLawVersion(1),
                to_version: MergeLawVersion(2),
            },
        );
        let json = serde_json::to_string(&spec).unwrap();
        let decoded: CloneSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, decoded);
    }

    // ── BlueGreenState tests ──────────────────────────────────────────────────

    #[test]
    fn blue_green_state_idle_not_in_flight() {
        assert!(!BlueGreenState::Idle.is_in_flight());
        assert!(!BlueGreenState::Idle.is_flipped());
    }

    #[test]
    fn blue_green_state_backfilling_is_in_flight() {
        let s = BlueGreenState::Backfilling {
            clone_view_id: ViewId(2),
            rows_backfilled: 1000,
        };
        assert!(s.is_in_flight());
        assert!(!s.is_flipped());
    }

    #[test]
    fn blue_green_state_flipped() {
        let s = BlueGreenState::Flipped {
            clone_view_id: ViewId(2),
            flip_epoch: 42,
        };
        assert!(s.is_in_flight());
        assert!(s.is_flipped());
    }

    #[test]
    fn blue_green_state_decommissioned_not_in_flight() {
        assert!(!BlueGreenState::Decommissioned.is_in_flight());
        assert!(BlueGreenState::Decommissioned.is_flipped());
    }

    #[test]
    fn blue_green_state_serde_roundtrip() {
        let s = BlueGreenState::ReadyToFlip {
            clone_view_id: ViewId(3),
            lag_epochs: 2,
        };
        let json = serde_json::to_string(&s).unwrap();
        let decoded: BlueGreenState = serde_json::from_str(&json).unwrap();
        assert_eq!(s, decoded);
    }

    // ── classify_law_version_change tests ────────────────────────────────────

    #[test]
    fn non_breaking_law_version_bump_is_compatible() {
        assert_eq!(
            classify_law_version_change(
                MergeLawId(1),
                MergeLawVersion(1),
                MergeLawVersion(2),
                false
            ),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn breaking_law_version_bump_is_incompatible() {
        assert_eq!(
            classify_law_version_change(
                MergeLawId(1),
                MergeLawVersion(1),
                MergeLawVersion(2),
                true
            ),
            SchemaChangeKind::Incompatible
        );
    }
}
