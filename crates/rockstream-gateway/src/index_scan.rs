use crate::error::GatewayError;
use rockstream_types::state_budget::StateBudgetMeter;
use rockstream_types::view_lifecycle::ViewState;

/// Query scan execution path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPath {
    ShardScan,
    IndexScan,
}

/// Select whether to use index_scan or shard_scan.
pub fn select_scan_path(
    selectivity: f64,
    selectivity_threshold: f64,
    state: &ViewState,
    lag_ms: u64,
    max_lag_ms: u64,
) -> ScanPath {
    if let ViewState::BackfillingFromEpoch(_) = state {
        return ScanPath::ShardScan;
    }
    if lag_ms > max_lag_ms {
        return ScanPath::ShardScan;
    }
    if selectivity < selectivity_threshold {
        ScanPath::IndexScan
    } else {
        ScanPath::ShardScan
    }
}

/// Check the index status and return gateway errors if building or lagging.
pub fn check_index_status(
    state: &ViewState,
    lag_ms: u64,
    max_lag_ms: u64,
    name: &str,
) -> Result<(), GatewayError> {
    if let ViewState::BackfillingFromEpoch(_) = state {
        return Err(GatewayError::IndexBuilding {
            name: name.to_string(),
        });
    }
    if lag_ms > max_lag_ms {
        return Err(GatewayError::IndexFrontierLag {
            name: name.to_string(),
            lag_ms,
        });
    }
    Ok(())
}

/// Charge index state bytes against the state budget.
pub fn charge_index_budget(
    budget: &StateBudgetMeter,
    state_bytes: u64,
) -> Result<(), rockstream_types::state_budget::StateBudgetError> {
    budget.try_acquire(state_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::error_code::{RS_2014, RS_2015};

    #[test]
    fn test_select_scan_path() {
        // Ready, low selectivity -> IndexScan
        assert_eq!(
            select_scan_path(0.005, 0.01, &ViewState::Running, 100, 1000),
            ScanPath::IndexScan
        );
        // Ready, high selectivity -> ShardScan
        assert_eq!(
            select_scan_path(0.05, 0.01, &ViewState::Running, 100, 1000),
            ScanPath::ShardScan
        );
        // Building -> ShardScan
        assert_eq!(
            select_scan_path(0.005, 0.01, &ViewState::BackfillingFromEpoch(0), 100, 1000),
            ScanPath::ShardScan
        );
        // Lagging -> ShardScan
        assert_eq!(
            select_scan_path(0.005, 0.01, &ViewState::Running, 1500, 1000),
            ScanPath::ShardScan
        );
    }

    #[test]
    fn test_check_index_status() {
        assert!(check_index_status(&ViewState::Running, 100, 1000, "idx").is_ok());

        let err_building =
            check_index_status(&ViewState::BackfillingFromEpoch(0), 100, 1000, "idx").unwrap_err();
        assert_eq!(err_building.error_code(), RS_2014);

        let err_lag = check_index_status(&ViewState::Running, 1500, 1000, "idx").unwrap_err();
        assert_eq!(err_lag.error_code(), RS_2015);
    }
}
