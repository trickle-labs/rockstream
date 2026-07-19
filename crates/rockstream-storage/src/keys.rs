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

/// Window arrangement discriminator bytes (v0.11 — IVM-7): ASCII 'W', 'N'.
pub const WINDOW_DISCRIMINATOR: [u8; 2] = [0x57, 0x4E];

/// Tumbling-window partial-state discriminator bytes (v0.12 — IVM-8): ASCII 'T', 'W'.
pub const TW_DISCRIMINATOR: [u8; 2] = [0x54, 0x57];

/// Watermark state discriminator bytes (v0.12 — IVM-8): ASCII 'W', 'M'.
pub const WM_DISCRIMINATOR: [u8; 2] = [0x57, 0x4D];

/// Top-K buffer discriminator bytes (v0.12 — IVM-9): ASCII 'T', 'K'.
pub const TK_DISCRIMINATOR: [u8; 2] = [0x54, 0x4B];

/// Recursion arrangement discriminator bytes (v0.50): ASCII 'R', 'C'.
pub const RC_DISCRIMINATOR: [u8; 2] = [0x52, 0x43];

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

/// Encode an i64 sort key for window ordering.
///
/// XORs with `0x8000_0000_0000_0000` so that lexicographic byte order matches
/// signed integer order: most-negative value → smallest bytes, most-positive
/// value → largest bytes.  A prefix scan over `[OpState][WN][op_id][part_key]`
/// returns rows in ascending order_key order.
pub fn window_sort_key(v: i64) -> [u8; 8] {
    ((v as u64) ^ 0x8000_0000_0000_0000_u64).to_be_bytes()
}

/// Decode a value from a window sort key (inverse of [`window_sort_key`]).
pub fn window_sort_key_decode(sort_key: [u8; 8]) -> i64 {
    (u64::from_be_bytes(sort_key) ^ 0x8000_0000_0000_0000_u64) as i64
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

    /// Prefix for scanning all idempotency keys for a given shard.
    /// Format: `0x02` (OpIndex) + `b"IK"` + `shard_id: u32`
    pub fn idempotency_prefix(shard_id: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 4);
        key.push(ShardPrefix::OpIndex.as_byte());
        key.extend_from_slice(b"IK");
        key.extend_from_slice(&shard_id.to_be_bytes());
        key
    }

    // ─── Window arrangement keys (IVM-7) ────────────────────────────────────

    /// Encode a Window input arrangement entry key.
    ///
    /// Format: `[0x01 (OpState)][0x57 0x4E ('WN')][op_id:8][part_key][order_key][row_id:16]`
    pub fn window_arr_key(op_id: u64, part_key: &[u8], order_key: &[u8], row_id: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + part_key.len() + order_key.len() + 16);
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&WINDOW_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(part_key);
        key.extend_from_slice(order_key);
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    /// Prefix for scanning all input arrangement rows for one partition.
    ///
    /// Format: `[0x01][WN][op_id:8][part_key]`
    pub fn window_arr_partition_prefix(op_id: u64, part_key: &[u8]) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(1 + 2 + 8 + part_key.len());
        prefix.push(ShardPrefix::OpState.as_byte());
        prefix.extend_from_slice(&WINDOW_DISCRIMINATOR);
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix.extend_from_slice(part_key);
        prefix
    }

    /// Prefix for scanning all input arrangement rows for one operator.
    ///
    /// Format: `[0x01][WN][op_id:8]`
    pub fn window_arr_op_prefix(op_id: u64) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(1 + 2 + 8);
        prefix.push(ShardPrefix::OpState.as_byte());
        prefix.extend_from_slice(&WINDOW_DISCRIMINATOR);
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix
    }

    /// Encode a Window output cache entry key.
    ///
    /// Format: `[0x02 (OpIndex)][WN][op_id:8][part_key][row_hash:16]`
    pub fn window_prev_output_key(op_id: u64, part_key: &[u8], row_hash: u128) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + part_key.len() + 16);
        key.push(ShardPrefix::OpIndex.as_byte());
        key.extend_from_slice(&WINDOW_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(part_key);
        key.extend_from_slice(&row_hash.to_be_bytes());
        key
    }

    /// Prefix for scanning all output cache entries for one partition.
    ///
    /// Format: `[0x02][WN][op_id:8][part_key]`
    pub fn window_prev_output_partition_prefix(op_id: u64, part_key: &[u8]) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(1 + 2 + 8 + part_key.len());
        prefix.push(ShardPrefix::OpIndex.as_byte());
        prefix.extend_from_slice(&WINDOW_DISCRIMINATOR);
        prefix.extend_from_slice(&op_id.to_be_bytes());
        prefix.extend_from_slice(part_key);
        prefix
    }

    // ─── Tumbling-window keys (IVM-8) ────────────────────────────────────────

    /// Encode a tumbling-window partial-state key.
    ///
    /// Format: `[0x01 (OpState)][0x54 0x57 ('TW')][op_id:8][window_id:8 BE ms][group_key:var]`
    pub fn tumble_window_key(op_id: u64, window_id: i64, group_key: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + 8 + group_key.len());
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&TW_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&window_id.to_be_bytes());
        key.extend_from_slice(group_key);
        key
    }

    /// Prefix for scanning all partial-state keys for an operator.
    ///
    /// Format: `[0x01][TW][op_id:8]`
    pub fn tumble_window_op_prefix(op_id: u64) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + 2 + 8);
        p.push(ShardPrefix::OpState.as_byte());
        p.extend_from_slice(&TW_DISCRIMINATOR);
        p.extend_from_slice(&op_id.to_be_bytes());
        p
    }

    /// Prefix for scanning all partial-state keys for a specific window.
    ///
    /// Format: `[0x01][TW][op_id:8][window_id:8 BE]`
    pub fn tumble_window_window_prefix(op_id: u64, window_id: i64) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + 2 + 8 + 8);
        p.push(ShardPrefix::OpState.as_byte());
        p.extend_from_slice(&TW_DISCRIMINATOR);
        p.extend_from_slice(&op_id.to_be_bytes());
        p.extend_from_slice(&window_id.to_be_bytes());
        p
    }

    /// Key for the watermark MaxRegister state of an operator.
    ///
    /// Format: `[0x06 (ShardMeta)][0x57 0x4D ('WM')][op_id:8]`
    /// Value: `[watermark_ms:8 BE]`
    pub fn watermark_key(op_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8);
        key.push(ShardPrefix::ShardMeta.as_byte());
        key.extend_from_slice(&WM_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key
    }

    // ─── Top-K keys (IVM-9) ─────────────────────────────────────────────────

    /// Compute value_desc_bytes for Top-K lexicographic ordering.
    ///
    /// Inverts the XOR-flip sort key so that highest value → smallest bytes
    /// → first in a prefix scan.
    pub fn topk_value_desc_bytes(v: i64) -> [u8; 8] {
        (!((v as u64) ^ 0x8000_0000_0000_0000_u64)).to_be_bytes()
    }

    /// Decode a value from value_desc_bytes (inverse of [`topk_value_desc_bytes`]).
    pub fn topk_value_desc_decode(bytes: [u8; 8]) -> i64 {
        let raw = !u64::from_be_bytes(bytes);
        (raw ^ 0x8000_0000_0000_0000_u64) as i64
    }

    /// Encode a Top-K buffer entry key.
    ///
    /// Format: `[0x01 (OpState)][0x54 0x4B ('TK')][op_id:8][partition_key:var][value_desc_bytes:8][row_id:16]`
    pub fn topk_key(op_id: u64, partition_key: &[u8], value: i64, row_id: u128) -> Vec<u8> {
        let vd = Self::topk_value_desc_bytes(value);
        let mut key = Vec::with_capacity(1 + 2 + 8 + partition_key.len() + 8 + 16);
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&TK_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(partition_key);
        key.extend_from_slice(&vd);
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    /// Prefix for scanning all Top-K entries for one partition of an operator.
    ///
    /// Format: `[0x01][TK][op_id:8][partition_key]`
    pub fn topk_partition_prefix(op_id: u64, partition_key: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + 2 + 8 + partition_key.len());
        p.push(ShardPrefix::OpState.as_byte());
        p.extend_from_slice(&TK_DISCRIMINATOR);
        p.extend_from_slice(&op_id.to_be_bytes());
        p.extend_from_slice(partition_key);
        p
    }

    /// Prefix for scanning all Top-K entries for an operator.
    ///
    /// Format: `[0x01][TK][op_id:8]`
    pub fn topk_op_prefix(op_id: u64) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + 2 + 8);
        p.push(ShardPrefix::OpState.as_byte());
        p.extend_from_slice(&TK_DISCRIMINATOR);
        p.extend_from_slice(&op_id.to_be_bytes());
        p
    }

    /// Encode a recursion arrangement entry key.
    ///
    /// Format: `[0x01 (OpState)][0x52 0x43 ('RC')][op_id:8][row_hash:16][iteration:4 BE]`
    pub fn recursion_key(op_id: u64, row_hash: u128, iteration: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 2 + 8 + 16 + 4);
        key.push(ShardPrefix::OpState.as_byte());
        key.extend_from_slice(&RC_DISCRIMINATOR);
        key.extend_from_slice(&op_id.to_be_bytes());
        key.extend_from_slice(&row_hash.to_be_bytes());
        key.extend_from_slice(&iteration.to_be_bytes());
        key
    }

    /// Prefix for scanning all recursion arrangement entries for an operator.
    ///
    /// Format: `[0x01][RC][op_id:8]`
    pub fn recursion_op_prefix(op_id: u64) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + 2 + 8);
        p.push(ShardPrefix::OpState.as_byte());
        p.extend_from_slice(&RC_DISCRIMINATOR);
        p.extend_from_slice(&op_id.to_be_bytes());
        p
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
    /// Secondary index definition (v0.32).
    Index = 0x16,
    /// Workload definition.
    Workload = 0x17,
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
    fn window_key_ordering_within_partition_preserves_order() {
        let op_id = 7u64;
        let part_key = b"pk1";

        // Two rows in the same partition, different order_key values.
        // Row A: order_key = -5 (negative), Row B: order_key = 42 (positive).
        // Signed order: -5 < 42, so key_a < key_b lexicographically.
        let order_key_a = window_sort_key(-5);
        let order_key_b = window_sort_key(42);

        let key_a = ShardKeyEncoder::window_arr_key(op_id, part_key, &order_key_a, 1);
        let key_b = ShardKeyEncoder::window_arr_key(op_id, part_key, &order_key_b, 2);

        assert!(
            key_a < key_b,
            "key for order_key=-5 must sort before key for order_key=42"
        );

        // Also verify sort key ordering for positive values.
        let sk_low = window_sort_key(1);
        let sk_mid = window_sort_key(100);
        let sk_high = window_sort_key(i64::MAX);
        assert!(sk_low < sk_mid);
        assert!(sk_mid < sk_high);

        // And negative ordering.
        let sk_neg_large = window_sort_key(i64::MIN);
        let sk_neg_small = window_sort_key(-1);
        assert!(sk_neg_large < sk_neg_small);
        assert!(sk_neg_small < sk_low);

        // Partition prefix is a proper prefix of arr key.
        let prefix = ShardKeyEncoder::window_arr_partition_prefix(op_id, part_key);
        assert!(key_a.starts_with(&prefix));
        assert!(key_b.starts_with(&prefix));
    }

    #[test]
    fn tumble_window_key_roundtrip() {
        let op_id = 5u64;
        let window_id_a = 1000i64; // window start ms
        let window_id_b = 2000i64;
        let group_key = b"gk1";

        // Encode/decode: prefix is correct length and value bytes parse back.
        let key_a = ShardKeyEncoder::tumble_window_key(op_id, window_id_a, group_key);
        // prefix: 1 + 2 + 8 = 11, then window_id: 8, then group_key
        assert_eq!(key_a[0], ShardPrefix::OpState.as_byte());
        assert_eq!(&key_a[1..3], &TW_DISCRIMINATOR);
        let decoded_op = u64::from_be_bytes(key_a[3..11].try_into().unwrap());
        assert_eq!(decoded_op, op_id);
        let decoded_win = i64::from_be_bytes(key_a[11..19].try_into().unwrap());
        assert_eq!(decoded_win, window_id_a);
        assert_eq!(&key_a[19..], group_key);

        // Two keys with different window_id sort in ascending window_id order.
        let key_b = ShardKeyEncoder::tumble_window_key(op_id, window_id_b, group_key);
        assert!(
            key_a < key_b,
            "window_id=1000 must sort before window_id=2000"
        );

        // Op prefix is a proper prefix of window key.
        let op_prefix = ShardKeyEncoder::tumble_window_op_prefix(op_id);
        assert!(key_a.starts_with(&op_prefix));

        // Window prefix is a proper prefix of keys in that window.
        let win_prefix = ShardKeyEncoder::tumble_window_window_prefix(op_id, window_id_a);
        assert!(key_a.starts_with(&win_prefix));
        assert!(!key_b.starts_with(&win_prefix));

        // Watermark key uses ShardMeta prefix.
        let wm = ShardKeyEncoder::watermark_key(op_id);
        assert_eq!(wm[0], ShardPrefix::ShardMeta.as_byte());
        assert_eq!(&wm[1..3], &WM_DISCRIMINATOR);
        let decoded_op2 = u64::from_be_bytes(wm[3..11].try_into().unwrap());
        assert_eq!(decoded_op2, op_id);
    }

    #[test]
    fn topk_key_scan_order() {
        let op_id = 9u64;
        let part_key = b"p1";

        // Insert keys for values [10, 5, 20, 1] with distinct row_ids.
        let vals_and_ids: Vec<(i64, u128)> = vec![(10, 1), (5, 2), (20, 3), (1, 4)];
        let mut keys: Vec<(Vec<u8>, i64)> = vals_and_ids
            .iter()
            .map(|&(v, rid)| (ShardKeyEncoder::topk_key(op_id, part_key, v, rid), v))
            .collect();

        // Sort lexicographically (as a DB scan would).
        keys.sort_by(|a, b| a.0.cmp(&b.0));

        // Expect descending value order: [20, 10, 5, 1].
        let sorted_vals: Vec<i64> = keys.iter().map(|(_, v)| *v).collect();
        assert_eq!(sorted_vals, vec![20, 10, 5, 1]);

        // Partition prefix is a proper prefix of all keys.
        let prefix = ShardKeyEncoder::topk_partition_prefix(op_id, part_key);
        for (k, _) in &keys {
            assert!(k.starts_with(&prefix));
        }
    }

    #[test]
    fn topk_value_desc_bytes_roundtrip() {
        for v in [-i64::MAX, -1i64, 0, 1, i64::MAX] {
            let bytes = ShardKeyEncoder::topk_value_desc_bytes(v);
            let decoded = ShardKeyEncoder::topk_value_desc_decode(bytes);
            assert_eq!(decoded, v, "roundtrip failed for v={v}");
        }
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
