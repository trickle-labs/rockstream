//! Key encoders for shard-local and catalog keys.
//!
//! All catalog keys include `namespace_id` immediately after the type byte
//! to support multi-tenancy from day one.
//!
//! Shard-local key prefixes (per DESIGN.md):
//! - `0x01` → op_state
//! - `0x02` → op_index
//! - `0x03` → view_output
//! - `0x04` → shuffle_inbox
//! - `0x05` → shuffle_outbox
//! - `0x06` → shard_meta

/// Shard-local key namespace prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShardPrefix {
    /// Operator state storage.
    OpState = 0x01,
    /// Secondary indexes, cached extrema.
    OpIndex = 0x02,
    /// Materialized view outputs.
    ViewOutput = 0x03,
    /// Incoming shuffle batches.
    ShuffleInbox = 0x04,
    /// Outgoing shuffle batches.
    ShuffleOutbox = 0x05,
    /// Shard metadata (frontiers, epoch markers, offsets).
    ShardMeta = 0x06,
}

impl ShardPrefix {
    /// Returns the single-byte prefix.
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Arrangement discriminator byte for the MIN/MAX sorted multiset (IVM-3).
///
/// Used as a sub-namespace byte immediately after the `ShardPrefix` byte in
/// MinMax arrangement keys to distinguish them from other operator state.
pub const MINMAX_DISCRIMINATOR: u8 = 0x4D; // 'M'

/// Distinct arrangement discriminator bytes (v0.10 — IVM-6): ASCII 'D', 'S'.
pub const DISTINCT_DISCRIMINATOR: [u8; 2] = [0x44, 0x53];

/// Left/right side discriminator for join arrangements (v0.8 — IVM-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    /// Two-byte discriminator for this side.
    pub fn disc_bytes(self) -> [u8; 2] {
        match self {
            JoinSide::Left => [0x4A, 0x4C],  // "JL"
            JoinSide::Right => [0x4A, 0x52], // "JR"
        }
    }
}

/// Compute the sort key for a value in the MIN/MAX multiset.
///
/// The encoding maps i64 values to `[u8; 8]` such that lexicographic byte
/// order matches signed integer order:
///   - For MIN (`invert = false`): smallest value → smallest sort key → first in scan.
///   - For MAX (`invert = true`):  largest value  → smallest sort key → first in scan.
///
/// This lets `scan_prefix(group_prefix).next()` return the extremum in O(1).
pub fn minmax_sort_key(v: i64, invert: bool) -> [u8; 8] {
    let raw = (v as u64) ^ 0x8000_0000_0000_0000_u64;
    if invert {
        (!raw).to_be_bytes()
    } else {
        raw.to_be_bytes()
    }
}

/// Decode a value from a sort key (inverse of [`minmax_sort_key`]).
pub fn minmax_sort_key_decode(sort_key: [u8; 8], invert: bool) -> i64 {
    let raw = u64::from_be_bytes(sort_key);
    let xored = if invert { !raw } else { raw };
    (xored ^ 0x8000_0000_0000_0000_u64) as i64
}

/// Encoder for shard-local keys.
///
/// Format: `[prefix:1][operator_id:8][suffix...]`
pub struct ShardKeyEncoder;

impl ShardKeyEncoder {
    /// Encode a shard-local key.
    ///
    /// # Arguments
    /// - `prefix`: The shard namespace prefix
    /// - `operator_id`: The operator instance ID
    /// - `suffix`: Arbitrary suffix bytes (key within operator state)
    pub fn encode(prefix: ShardPrefix, operator_id: u64, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 8 + suffix.len());
        key.push(prefix.as_byte());
        key.extend_from_slice(&operator_id.to_be_bytes());
        key.extend_from_slice(suffix);
        key
    }

    /// Decode a shard-local key into (prefix_byte, operator_id, suffix).
    /// Returns None if the key is too short.
    pub fn decode(key: &[u8]) -> Option<(u8, u64, &[u8])> {
        if key.len() < 9 {
            return None;
        }
        let prefix = key[0];
        let operator_id = u64::from_be_bytes(key[1..9].try_into().ok()?);
        let suffix = &key[9..];
        Some((prefix, operator_id, suffix))
    }

    /// Encode a join arrangement key: `[0x01][side_disc:2][op_id:8][join_key][row_id:16]`
    pub fn join_arr_key(side: JoinSide, op_id: u64, join_key: &[u8], row_id: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + join_key.len() + 16);
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&side.disc_bytes());
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(join_key);
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    // ─── Distinct arrangement keys (IVM-6) ──────────────────────────────────

    /// Encode a Distinct arrangement entry key.
    ///
    /// Format: `[0x01 (OpState)][0x44 0x53 ('DS')][op_id:8][row_hash:16]`
    /// Value: `[weight:8 BE][row_bytes: n_cols * 8 BE]`
    ///
    /// The `row_hash` is a 128-bit hash of the full row content, used as a
    /// compact and bounded key suffix.
    pub fn distinct_key(op_id: u64, row_hash: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + 16);
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&DISTINCT_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&row_hash.to_be_bytes());
        key
    }

    /// Prefix for scanning all distinct arrangement entries for a single operator.
    ///
    /// Format: `[0x01][0x44 0x53][op_id:8]`
    pub fn distinct_op_prefix(op_id: u64) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(1 + 2 + 8);
        prefix.push(ShardPrefix::OpState.as_byte());
        prefix.extend_from_slice(&DISTINCT_DISCRIMINATOR);
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix
    }

    /// Prefix for scanning all join arrangement entries for a single operator:
    /// `[0x01][side_disc:2][op_id:8]`
    pub fn join_arr_op_prefix(side: JoinSide, op_id: u64) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(1 + 2 + 8);
        prefix.push(ShardPrefix::OpState.as_byte());
        prefix.extend_from_slice(&side.disc_bytes());
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix
    }

    /// Build the prefix bytes for scanning all keys of a given operator.
    pub fn operator_prefix(prefix: ShardPrefix, operator_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(prefix.as_byte());
        key.extend_from_slice(&operator_id.to_be_bytes());
        key
    }

    /// Build the prefix bytes for scanning all keys in a shard namespace.
    pub fn namespace_prefix(prefix: ShardPrefix) -> Vec<u8> {
        vec![prefix.as_byte()]
    }

    /// Encode a shard metadata key (frontier, epoch marker, etc).
    pub fn meta_key(meta_type: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + meta_type.len());
        key.push(ShardPrefix::ShardMeta.as_byte());
        key.extend_from_slice(meta_type);
        key
    }

    /// The frontier key for a shard.
    pub fn frontier_key() -> Vec<u8> {
        Self::meta_key(b"frontier")
    }

    // ─── MIN/MAX arrangement keys (IVM-3) ────────────────────────────────────

    /// Encode a MIN/MAX multiset entry key.
    ///
    /// Format: `[0x01 (OpState)][0x4D][op_id:8][group_key:8][sort_key:8]`
    ///
    /// The `sort_key` is produced by [`minmax_sort_key`] and encodes the value
    /// so that the extremum always occupies the first entry in a prefix scan.
    pub fn minmax_multiset_key(op_id: u64, group_key: i64, sort_key: [u8; 8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 1 + 8 + 8 + 8);
        key.push(ShardPrefix::OpState.as_byte());
        key.push(MINMAX_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&group_key.to_be_bytes());
        key.extend_from_slice(&sort_key);
        key
    }

    /// Prefix for scanning all MIN/MAX multiset entries for a specific group.
    ///
    /// Format: `[0x01][0x4D][op_id:8][group_key:8]`
    pub fn minmax_group_prefix(op_id: u64, group_key: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 1 + 8 + 8);
        key.push(ShardPrefix::OpState.as_byte());
        key.push(MINMAX_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&group_key.to_be_bytes());
        key
    }

    /// Prefix for scanning all MIN/MAX multiset entries for an operator.
    ///
    /// Format: `[0x01][0x4D][op_id:8]`
    pub fn minmax_operator_prefix(op_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 1 + 8);
        key.push(ShardPrefix::OpState.as_byte());
        key.push(MINMAX_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key
    }

    /// Key for the cached extremum of a group.
    ///
    /// Format: `[0x02 (OpIndex)][0x4D][op_id:8][group_key:8]`
    pub fn minmax_extremum_key(op_id: u64, group_key: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 1 + 8 + 8);
        key.push(ShardPrefix::OpIndex.as_byte());
        key.push(MINMAX_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&group_key.to_be_bytes());
        key
    }

    /// Prefix for scanning all cached extrema for an operator.
    ///
    /// Format: `[0x02][0x4D][op_id:8]`
    pub fn minmax_extremum_op_prefix(op_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 1 + 8);
        key.push(ShardPrefix::OpIndex.as_byte());
        key.push(MINMAX_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key
    }

    /// The epoch marker key for a given epoch number.
    pub fn epoch_key(epoch: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 8 + 8);
        key.push(ShardPrefix::ShardMeta.as_byte());
        key.extend_from_slice(b"epoch");
        key.extend_from_slice(&epoch.to_be_bytes());
        key
    }

    /// Encode an idempotency key.
    /// Format: `0x02` (OpIndex) + `b"IK"` + `shard_id: u32` + `key_hash: [u8; 16]`
    pub fn idempotency_key(shard_id: u32, key_hash: [u8; 16]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 4 + 16);
        key.push(ShardPrefix::OpIndex.as_byte());
        key.extend_from_slice(b"IK");
        key.extend_from_slice(&shard_id.to_be_bytes());
        key.extend_from_slice(&key_hash);
        key
    }

    /// Decode an idempotency key.
    /// Returns (shard_id, key_hash) if it is a valid idempotency key.
    pub fn decode_idempotency_key(key: &[u8]) -> Option<(u32, [u8; 16])> {
        if key.len() != 23 {
            return None;
        }
        if key[0] != ShardPrefix::OpIndex.as_byte() || &key[1..3] != b"IK" {
            return None;
        }
        let shard_id = u32::from_be_bytes(key[3..7].try_into().ok()?);
        let key_hash = key[7..23].try_into().ok()?;
        Some((shard_id, key_hash))
    }
}

/// Encoder for catalog (control-plane) keys.
///
/// Format: `[type_byte:1][namespace_id:16][object_id:16][suffix...]`
///
/// The `namespace_id` is always present in catalog keys to enable multi-tenancy
/// from day one. Default namespace uses `namespace_id = 0`.
pub struct CatalogKeyEncoder;

/// Catalog object types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CatalogType {
    /// Pipeline definition.
    Pipeline = 0x10,
    /// Source definition.
    Source = 0x11,
    /// View definition.
    View = 0x12,
    /// Table definition.
    Table = 0x13,
    /// Schema version.
    Schema = 0x14,
    /// Connector configuration.
    Connector = 0x15,
}

impl CatalogKeyEncoder {
    /// Encode a catalog key with namespace.
    ///
    /// # Arguments
    /// - `catalog_type`: The type of catalog object
    /// - `namespace_id`: The namespace (tenant) identifier (128-bit)
    /// - `object_id`: The object identifier (128-bit)
    pub fn encode(catalog_type: CatalogType, namespace_id: u128, object_id: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 16 + 16);
        key.push(catalog_type as u8);
        key.extend_from_slice(&namespace_id.to_be_bytes());
        key.extend_from_slice(&object_id.to_be_bytes());
        key
    }

    /// Encode a catalog key with namespace and an additional suffix.
    pub fn encode_with_suffix(
        catalog_type: CatalogType,
        namespace_id: u128,
        object_id: u128,
        suffix: &[u8],
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 16 + 16 + suffix.len());
        key.push(catalog_type as u8);
        key.extend_from_slice(&namespace_id.to_be_bytes());
        key.extend_from_slice(&object_id.to_be_bytes());
        key.extend_from_slice(suffix);
        key
    }

    /// Decode a catalog key into (type_byte, namespace_id, object_id, suffix).
    pub fn decode(key: &[u8]) -> Option<(u8, u128, u128, &[u8])> {
        if key.len() < 33 {
            return None;
        }
        let type_byte = key[0];
        let namespace_id = u128::from_be_bytes(key[1..17].try_into().ok()?);
        let object_id = u128::from_be_bytes(key[17..33].try_into().ok()?);
        let suffix = &key[33..];
        Some((type_byte, namespace_id, object_id, suffix))
    }

    /// Build a prefix for scanning all objects of a type in a namespace.
    pub fn namespace_prefix(catalog_type: CatalogType, namespace_id: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 16);
        key.push(catalog_type as u8);
        key.extend_from_slice(&namespace_id.to_be_bytes());
        key
    }

    /// Build a prefix for scanning all objects of a type (across all namespaces).
    pub fn type_prefix(catalog_type: CatalogType) -> Vec<u8> {
        vec![catalog_type as u8]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_key_encode_decode_roundtrip() {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 42, b"hello");
        let (prefix, op_id, suffix) = ShardKeyEncoder::decode(&key).unwrap();
        assert_eq!(prefix, ShardPrefix::OpState.as_byte());
        assert_eq!(op_id, 42);
        assert_eq!(suffix, b"hello");
    }

    #[test]
    fn shard_key_prefix_is_proper_prefix() {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpIndex, 99, b"data");
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpIndex, 99);
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn shard_key_ordering_preserves_operator_id() {
        let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"a");
        let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 2, b"a");
        assert!(k1 < k2);
    }

    #[test]
    fn catalog_key_includes_namespace() {
        let key = CatalogKeyEncoder::encode(CatalogType::View, 1, 100);
        let (type_byte, ns, obj, suffix) = CatalogKeyEncoder::decode(&key).unwrap();
        assert_eq!(type_byte, CatalogType::View as u8);
        assert_eq!(ns, 1);
        assert_eq!(obj, 100);
        assert!(suffix.is_empty());
    }

    #[test]
    fn catalog_key_default_namespace_is_zero() {
        let key = CatalogKeyEncoder::encode(CatalogType::Pipeline, 0, 42);
        let (_, ns, obj, _) = CatalogKeyEncoder::decode(&key).unwrap();
        assert_eq!(ns, 0);
        assert_eq!(obj, 42);
    }

    #[test]
    fn catalog_key_with_suffix() {
        let key = CatalogKeyEncoder::encode_with_suffix(CatalogType::Schema, 5, 10, b"version_3");
        let (_, ns, obj, suffix) = CatalogKeyEncoder::decode(&key).unwrap();
        assert_eq!(ns, 5);
        assert_eq!(obj, 10);
        assert_eq!(suffix, b"version_3");
    }

    #[test]
    fn catalog_namespace_prefix_filters_correctly() {
        let key1 = CatalogKeyEncoder::encode(CatalogType::View, 1, 100);
        let key2 = CatalogKeyEncoder::encode(CatalogType::View, 2, 100);
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::View, 1);
        assert!(key1.starts_with(&prefix));
        assert!(!key2.starts_with(&prefix));
    }

    #[test]
    fn shard_key_too_short_returns_none() {
        assert!(ShardKeyEncoder::decode(b"short").is_none());
    }

    #[test]
    fn catalog_key_too_short_returns_none() {
        assert!(CatalogKeyEncoder::decode(b"short").is_none());
    }

    #[test]
    fn frontier_key_has_shard_meta_prefix() {
        let key = ShardKeyEncoder::frontier_key();
        assert_eq!(key[0], ShardPrefix::ShardMeta.as_byte());
    }

    #[test]
    fn epoch_keys_sort_by_epoch_number() {
        let k1 = ShardKeyEncoder::epoch_key(1);
        let k2 = ShardKeyEncoder::epoch_key(2);
        let k100 = ShardKeyEncoder::epoch_key(100);
        assert!(k1 < k2);
        assert!(k2 < k100);
    }

    #[test]
    fn idempotency_key_encode_decode_roundtrip() {
        let hash = [7u8; 16];
        let key = ShardKeyEncoder::idempotency_key(123, hash);
        assert_eq!(key[0], ShardPrefix::OpIndex.as_byte());
        assert_eq!(&key[1..3], b"IK");
        let (shard, decoded_hash) = ShardKeyEncoder::decode_idempotency_key(&key).unwrap();
        assert_eq!(shard, 123);
        assert_eq!(decoded_hash, hash);
    }
}
