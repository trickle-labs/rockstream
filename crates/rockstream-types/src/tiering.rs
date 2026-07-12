use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StorageTieringConfig {
    pub shard_meta_backend: Option<String>,
    pub cold_sst_backend: Option<String>,
    pub cold_sst_age_threshold: Option<u64>,
}
