//! Wire protocol version skew contract (DESIGN.md §5.5, v0.36).
//!
//! During a rolling upgrade there is a window where nodes run different
//! binary versions. Each gRPC service announces a `protocol_version` header.
//! The receiving side rejects requests with a higher version than it supports
//! (`RS-5021`). The N+1 binary must be able to send messages that N can parse
//! (backward-compatible wire format for one version).

pub use rockstream_types::compatibility::{ProtocolVersion, SupportedVersionRange};

/// Result of wire protocol version negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationResult {
    /// Versions are compatible; the agreed version is returned.
    Compatible { agreed: ProtocolVersion },
    /// Remote version is outside our supported range; `RS-5021` must be returned.
    Incompatible {
        local_max: ProtocolVersion,
        remote_version: ProtocolVersion,
    },
}

/// Negotiate the wire protocol version between local and remote nodes.
///
/// - Accepts if `remote_version` falls within `local_range`.
/// - Rejects with `RS-5021` if outside the range.
/// - Agreed version is `min(remote_version, local_max)`.
pub fn negotiate_version(
    local_range: SupportedVersionRange,
    remote_version: ProtocolVersion,
) -> NegotiationResult {
    if remote_version < local_range.min || remote_version > local_range.max {
        NegotiationResult::Incompatible {
            local_max: local_range.max,
            remote_version,
        }
    } else {
        NegotiationResult::Compatible {
            agreed: remote_version.min(local_range.max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_version_compatible() {
        let result = negotiate_version(SupportedVersionRange::v1_only(), ProtocolVersion::V1);
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V1
            }
        );
    }

    #[test]
    fn newer_remote_rejected() {
        let result = negotiate_version(SupportedVersionRange::v1_only(), ProtocolVersion::V2);
        assert!(matches!(result, NegotiationResult::Incompatible { .. }));
    }

    #[test]
    fn v2_node_accepts_v1() {
        let result = negotiate_version(
            SupportedVersionRange::v2_with_v1_compat(),
            ProtocolVersion::V1,
        );
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V1
            }
        );
    }

    #[test]
    fn v2_node_accepts_v2() {
        let result = negotiate_version(
            SupportedVersionRange::v2_with_v1_compat(),
            ProtocolVersion::V2,
        );
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V2
            }
        );
    }

    #[test]
    fn too_old_remote_rejected() {
        let range = SupportedVersionRange {
            min: ProtocolVersion::V2,
            max: ProtocolVersion::V2,
        };
        let result = negotiate_version(range, ProtocolVersion::V1);
        assert!(matches!(result, NegotiationResult::Incompatible { .. }));
    }
}
