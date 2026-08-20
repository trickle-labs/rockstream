//! Delta-native state mutation and epoch result types.
//!
//! Provides `StateMutation`, `EpochStateDelta`, and `OperatorEpochMetrics` for
//! fine-grained incremental arrangement maintenance and O(1) group commit.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::merge_law::{MergeLawId, MergeLawVersion};

/// First-class delta-native state mutation primitive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateMutation {
    /// Merge an associative operand into the arrangement key state.
    Merge {
        key: Vec<u8>,
        law: MergeLawId,
        law_version: MergeLawVersion,
        operand: Bytes,
    },
    /// Point put for ordered/replacement state updates.
    Put { key: Vec<u8>, value: Bytes },
    /// Point delete / tombstone for zero-weight or retracted entries.
    Delete { key: Vec<u8> },
}

impl StateMutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Merge { key, .. } => key,
            Self::Put { key, .. } => key,
            Self::Delete { key } => key,
        }
    }

    pub fn size_bytes(&self) -> usize {
        match self {
            Self::Merge { key, operand, .. } => key.len() + operand.len() + 4,
            Self::Put { key, value } => key.len() + value.len(),
            Self::Delete { key } => key.len(),
        }
    }
}

/// Collection of per-operator mutations committed within an epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochStateDelta {
    pub mutations: Vec<StateMutation>,
    pub dirty_key_count: usize,
    pub logical_delta_bytes: usize,
}

impl EpochStateDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_mutations(mutations: Vec<StateMutation>) -> Self {
        let dirty_key_count = mutations.len();
        let logical_delta_bytes = mutations.iter().map(|m| m.size_bytes()).sum();
        Self {
            mutations,
            dirty_key_count,
            logical_delta_bytes,
        }
    }

    pub fn push(&mut self, mutation: StateMutation) {
        self.logical_delta_bytes += mutation.size_bytes();
        self.dirty_key_count += 1;
        self.mutations.push(mutation);
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }
}

/// Hot-path metrics for operator epoch execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorEpochMetrics {
    pub input_records: usize,
    pub output_records: usize,
    pub dirty_keys: usize,
    pub state_mutations: usize,
    pub logical_mutation_bytes: usize,
    pub full_state_entries_visited: usize,
    pub state_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_mutation_size_and_key() {
        let put = StateMutation::Put {
            key: vec![1, 2, 3],
            value: Bytes::from_static(b"hello"),
        };
        assert_eq!(put.key(), &[1, 2, 3]);
        assert_eq!(put.size_bytes(), 8);

        let del = StateMutation::Delete { key: vec![4, 5] };
        assert_eq!(del.key(), &[4, 5]);
        assert_eq!(del.size_bytes(), 2);

        let merge = StateMutation::Merge {
            key: vec![10],
            law: MergeLawId(1),
            law_version: MergeLawVersion(1),
            operand: Bytes::from_static(b"data"),
        };
        assert_eq!(merge.key(), &[10]);
        assert_eq!(merge.size_bytes(), 9);
    }

    #[test]
    fn test_epoch_state_delta_accumulation() {
        let mut delta = EpochStateDelta::new();
        assert!(delta.is_empty());
        delta.push(StateMutation::Put {
            key: vec![1],
            value: Bytes::from_static(b"v"),
        });
        assert_eq!(delta.len(), 1);
        assert_eq!(delta.dirty_key_count, 1);
        assert_eq!(delta.logical_delta_bytes, 2);
    }
}
