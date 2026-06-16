use crate::audit::{AuditEvent, FileAuditLog};

/// Derived freshness and execution parameters for a workload target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DerivedParameters {
    pub min_epoch_ms: u64,
    pub max_epoch_ms: u64,
    pub initial_parallelism: usize,
}

/// Derive pipeline execution parameters from a freshness SLO.
pub fn derive_parameters(
    target_ms: u64,
    manual_override: Option<DerivedParameters>,
    audit: Option<&FileAuditLog>,
) -> DerivedParameters {
    let derived = {
        let max_epoch_ms = (target_ms / 2).clamp(10, 5000);
        let min_epoch_ms = (target_ms / 10).clamp(10, 250).min(max_epoch_ms);
        let initial_parallelism = if target_ms < 500 {
            8
        } else if target_ms < 1000 {
            4
        } else if target_ms < 5000 {
            2
        } else {
            1
        };
        DerivedParameters {
            min_epoch_ms,
            max_epoch_ms,
            initial_parallelism,
        }
    };

    let final_params = manual_override.unwrap_or(derived);

    if let Some(aud) = audit {
        let (action, detail) = if manual_override.is_some() {
            (
                "config.override",
                format!(
                    "target_ms={}, min_epoch_ms={}, max_epoch_ms={}, initial_parallelism={}",
                    target_ms,
                    final_params.min_epoch_ms,
                    final_params.max_epoch_ms,
                    final_params.initial_parallelism
                ),
            )
        } else {
            (
                "config.derived",
                format!(
                    "target_ms={}, min_epoch_ms={}, max_epoch_ms={}, initial_parallelism={}",
                    target_ms,
                    final_params.min_epoch_ms,
                    final_params.max_epoch_ms,
                    final_params.initial_parallelism
                ),
            )
        };
        let event = AuditEvent::now("control", action, "pipeline").with_detail(detail);
        let _ = aud.append(&event);
    }

    final_params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derivation_mapping() {
        // Tight target (< 500ms)
        let p1 = derive_parameters(200, None, None);
        assert_eq!(p1.min_epoch_ms, 20);
        assert_eq!(p1.max_epoch_ms, 100);
        assert_eq!(p1.initial_parallelism, 8);

        // Medium tight target
        let p2 = derive_parameters(800, None, None);
        assert_eq!(p2.min_epoch_ms, 80);
        assert_eq!(p2.max_epoch_ms, 400);
        assert_eq!(p2.initial_parallelism, 4);

        // Relaxed target
        let p3 = derive_parameters(4000, None, None);
        assert_eq!(p3.min_epoch_ms, 250); // Clamped to 250
        assert_eq!(p3.max_epoch_ms, 2000);
        assert_eq!(p3.initial_parallelism, 2);

        // Very relaxed target (> 5000ms)
        let p4 = derive_parameters(12000, None, None);
        assert_eq!(p4.min_epoch_ms, 250);
        assert_eq!(p4.max_epoch_ms, 5000); // Clamped to 5000
        assert_eq!(p4.initial_parallelism, 1);
    }

    #[test]
    fn test_manual_override() {
        let over = DerivedParameters {
            min_epoch_ms: 50,
            max_epoch_ms: 100,
            initial_parallelism: 16,
        };
        let p = derive_parameters(1000, Some(over), None);
        assert_eq!(p, over);
    }

    #[test]
    fn test_audit_logging() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&log_path).unwrap();

        let _ = derive_parameters(1000, None, Some(&log));
        let events = log.read_all().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, "config.derived");
        assert!(events[0]
            .detail
            .as_ref()
            .unwrap()
            .contains("target_ms=1000"));

        let over = DerivedParameters {
            min_epoch_ms: 50,
            max_epoch_ms: 100,
            initial_parallelism: 16,
        };
        let _ = derive_parameters(1000, Some(over), Some(&log));
        let events = log.read_all().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].action, "config.override");
        assert!(events[1]
            .detail
            .as_ref()
            .unwrap()
            .contains("initial_parallelism=16"));
    }
}
