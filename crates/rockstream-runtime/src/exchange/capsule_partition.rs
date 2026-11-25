//! Once-per-batch canonical [`BatchKeyCapsule`] exchange partitioning (Issue #402 R3).
//!
//! Computes or reuses the batch's canonical `BatchKeyCapsule` for `key_cols`,
//! routes rows to target partitions using the capsule's precomputed `row_hash_u64`,
//! and slices the capsule onto each partition batch so downstream joins,
//! aggregates, and storage writers reuse the exact same encoded key bytes with
//! zero re-encoding.

use std::sync::Arc;

use rockstream_ops::{ArrowZSet, OpError};
use rockstream_types::BatchKeyCapsule;

/// Partition `batch` into `num_partitions` buckets by hashing `key_cols` via a
/// canonical [`BatchKeyCapsule`], preserving the sliced capsule on every
/// non-empty output partition without re-encoding keys.
pub fn partition_batch_by_capsule(
    batch: &ArrowZSet,
    key_cols: &[usize],
    num_partitions: usize,
) -> Result<Vec<ArrowZSet>, OpError> {
    if num_partitions == 0 {
        return Err(OpError::unsupported_plan_node(
            "partition_batch_by_capsule requires num_partitions > 0",
        ));
    }
    batch.validate()?;
    if batch.is_empty() {
        return Ok((0..num_partitions)
            .map(|_| {
                let mut z = ArrowZSet::empty(batch.schema());
                z.frontier = batch.frontier.clone();
                z
            })
            .collect());
    }

    // Reuse an attached capsule if present, otherwise encode once for this batch.
    let capsule: Arc<BatchKeyCapsule> = batch.get_or_build_key_capsule(key_cols)?;
    let mut bucket_indices: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
    for row_idx in 0..batch.num_rows() {
        let part = (capsule.row_hash_u64(row_idx) as usize) % num_partitions;
        bucket_indices[part].push(row_idx);
    }

    let mut base = batch.clone();
    if base.key_capsule_for(key_cols).is_none() {
        base.push_key_capsule(capsule);
    }

    let mut out = Vec::with_capacity(num_partitions);
    for indices in bucket_indices {
        if indices.is_empty() {
            let mut empty = ArrowZSet::empty(batch.schema());
            empty.frontier = batch.frontier.clone();
            out.push(empty);
        } else {
            out.push(base.select_rows(&indices)?);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::measure_batch_key_capsule_encodings;

    #[test]
    fn partitions_and_reuses_capsule_with_single_encoding() {
        let batch = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30), (1, 40)], 1);
        let (parts, encodings) =
            measure_batch_key_capsule_encodings(|| partition_batch_by_capsule(&batch, &[0], 4));
        let parts = parts.unwrap();
        assert_eq!(encodings, 1, "key columns must be encoded once per batch");
        assert_eq!(parts.len(), 4);

        let total_rows: usize = parts.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // Downstream consumer requesting the same key spec must reuse with 0 encodings.
        let (_, downstream_encodings) = measure_batch_key_capsule_encodings(|| {
            for part in &parts {
                if !part.is_empty() {
                    let cap = part.get_or_build_key_capsule(&[0]).unwrap();
                    assert_eq!(cap.len(), part.num_rows());
                }
            }
        });
        assert_eq!(downstream_encodings, 0);
    }
}
