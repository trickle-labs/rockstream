//! Durable shuffle fallback path for RockStream exchange (v0.31).
//!
//! When the direct gRPC path is unavailable (receiver fault) or a batch is
//! too large for credit limits, the exchange layer falls back to writing
//! coalesced shuffle objects to the object store.  The receiver catches up
//! by reading the objects using **outbox metadata** — it never issues a LIST
//! call on the hot path.
//!
//! ## Object layout (`RSSHUF01`)
//!
//! ```text
//! [magic: 8]  b"RSSHUF01"
//! [frame_0]:  [target_shard: 8][key_len: 4][val_len: 4][key][val]
//! [frame_1]:  ...
//! ...
//! [index]:    [frame_count: 4][offset_0: 8][offset_1: 8]...
//! [footer]:   [index_offset: 8]  ← last 8 bytes of object
//! ```
//!
//! ## No-LIST guarantee
//!
//! Senders record each written object as an `OutboxEntry` (path + frame
//! count).  Receivers are handed the metadata vector; they only call
//! `store.get(&path)` — never `store.list(prefix)`.
//!
//! ## Law-aware re-merge
//!
//! After reading frames from an object the receiver calls
//! [`DurableShuffleReader::merge_frames`] which groups rows by
//! `(target_shard, key)` and folds values using the registered `LawBundle`,
//! matching what the direct path would have produced.

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use rockstream_types::ids::{ExchangeId, ShardId, WorkerId};
use rockstream_types::laws::LawRegistry;
use rockstream_types::merge_law::MergeLawId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Magic bytes that head every coalesced shuffle object.
const MAGIC: &[u8; 8] = b"RSSHUF01";

/// A single row to shuffle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleFrame {
    /// Destination shard for this row.
    pub target_shard: ShardId,
    /// Row key bytes.
    pub key: Vec<u8>,
    /// Row value bytes (encoded per the attached `MergeLawId`).
    pub value: Vec<u8>,
}

/// Metadata describing one written outbox object.
///
/// Receivers carry this metadata in memory so they can read back the object
/// without any LIST calls.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    /// Object-store path of the coalesced shuffle object.
    pub path: String,
    /// Monotone sequence number assigned by the writer.
    pub sequence: u64,
    /// Number of frames encoded in the object.
    pub frame_count: usize,
}

/// Error type for the durable shuffle path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableError {
    /// The object was not found in the store.
    NotFound(String),
    /// The object is smaller than expected.
    TruncatedObject,
    /// Magic bytes did not match `RSSHUF01`.
    BadMagic,
    /// A frame's length fields point past the end of the object.
    MalformedFrame(usize),
    /// Object store I/O error.
    Store(String),
    /// Law merge failure.
    Merge(String),
    /// Law not registered.
    UnknownLaw(MergeLawId),
    /// Malformed object structure.
    MalformedObject,
}

impl std::fmt::Display for DurableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DurableError::NotFound(p) => write!(f, "object not found: {p}"),
            DurableError::TruncatedObject => write!(f, "object is truncated"),
            DurableError::BadMagic => write!(f, "bad magic bytes (expected RSSHUF01)"),
            DurableError::MalformedFrame(idx) => write!(f, "malformed frame at index {idx}"),
            DurableError::Store(e) => write!(f, "object store error: {e}"),
            DurableError::Merge(e) => write!(f, "merge error: {e}"),
            DurableError::UnknownLaw(id) => write!(f, "unknown law: {id}"),
            DurableError::MalformedObject => write!(f, "malformed object structure"),
        }
    }
}

// ── Encoding ─────────────────────────────────────────────────────────────────

/// Encode a slice of frames into a coalesced shuffle object.
///
/// Format: `[magic(8)][frame...][index_footer][footer_offset(8)]`
pub fn encode_object(frames: &[ShuffleFrame]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(MAGIC);

    let mut offsets: Vec<u64> = Vec::with_capacity(frames.len());
    for frame in frames {
        offsets.push(buf.len() as u64);
        buf.extend_from_slice(&frame.target_shard.0.to_be_bytes()); // 8 bytes
        let kl = frame.key.len() as u32;
        let vl = frame.value.len() as u32;
        buf.extend_from_slice(&kl.to_be_bytes()); // 4 bytes
        buf.extend_from_slice(&vl.to_be_bytes()); // 4 bytes
        buf.extend_from_slice(&frame.key);
        buf.extend_from_slice(&frame.value);
    }

    // Index footer: frame_count(4) + offsets(8 * count)
    let index_offset = buf.len() as u64;
    buf.extend_from_slice(&(frames.len() as u32).to_be_bytes());
    for off in &offsets {
        buf.extend_from_slice(&off.to_be_bytes());
    }
    // Footer offset: 8 bytes at the very end
    buf.extend_from_slice(&index_offset.to_be_bytes());
    buf
}

/// Decode a coalesced shuffle object back into frames.
pub fn decode_object(data: &[u8]) -> Result<Vec<ShuffleFrame>, DurableError> {
    if data.len() < 8 + 8 {
        return Err(DurableError::TruncatedObject);
    }
    if &data[..8] != MAGIC {
        return Err(DurableError::BadMagic);
    }

    // Read footer offset (last 8 bytes)
    let last_8_bytes = data
        .get(data.len() - 8..)
        .ok_or(DurableError::TruncatedObject)?;
    let footer_off_bytes: [u8; 8] = last_8_bytes
        .try_into()
        .map_err(|_| DurableError::MalformedObject)?;
    let footer_off = u64::from_be_bytes(footer_off_bytes) as usize;

    if footer_off + 4 > data.len() - 8 {
        return Err(DurableError::TruncatedObject);
    }

    let count_slice = data
        .get(footer_off..footer_off + 4)
        .ok_or(DurableError::TruncatedObject)?;
    let count_bytes: [u8; 4] = count_slice
        .try_into()
        .map_err(|_| DurableError::MalformedObject)?;
    let frame_count = u32::from_be_bytes(count_bytes) as usize;

    let offsets_start = footer_off + 4;
    let offsets_end = offsets_start + frame_count * 8;
    if offsets_end > data.len() - 8 {
        return Err(DurableError::TruncatedObject);
    }

    let mut frames = Vec::with_capacity(frame_count);
    for i in 0..frame_count {
        let offset_slice = data
            .get(offsets_start + i * 8..offsets_start + i * 8 + 8)
            .ok_or(DurableError::TruncatedObject)?;
        let off_bytes: [u8; 8] = offset_slice
            .try_into()
            .map_err(|_| DurableError::MalformedObject)?;
        let offset = u64::from_be_bytes(off_bytes) as usize;

        // Each frame: target_shard(8) + key_len(4) + val_len(4) + key + val
        let header_end = offset + 16;
        if header_end > data.len() {
            return Err(DurableError::MalformedFrame(i));
        }

        let shard_slice = data
            .get(offset..offset + 8)
            .ok_or(DurableError::MalformedFrame(i))?;
        let shard_bytes: [u8; 8] = shard_slice
            .try_into()
            .map_err(|_| DurableError::MalformedObject)?;
        let target_shard = u64::from_be_bytes(shard_bytes);

        let key_len_slice = data
            .get(offset + 8..offset + 12)
            .ok_or(DurableError::MalformedFrame(i))?;
        let key_len_bytes: [u8; 4] = key_len_slice
            .try_into()
            .map_err(|_| DurableError::MalformedObject)?;
        let key_len = u32::from_be_bytes(key_len_bytes) as usize;

        let val_len_slice = data
            .get(offset + 12..offset + 16)
            .ok_or(DurableError::MalformedFrame(i))?;
        let val_len_bytes: [u8; 4] = val_len_slice
            .try_into()
            .map_err(|_| DurableError::MalformedObject)?;
        let val_len = u32::from_be_bytes(val_len_bytes) as usize;

        let key_end = header_end + key_len;
        let val_end = key_end + val_len;
        if val_end > data.len() {
            return Err(DurableError::MalformedFrame(i));
        }

        frames.push(ShuffleFrame {
            target_shard: ShardId(target_shard),
            key: data[header_end..key_end].to_vec(),
            value: data[key_end..val_end].to_vec(),
        });
    }

    Ok(frames)
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Writes coalesced shuffle objects to the object store.
///
/// Each call to [`DurableShuffleWriter::write`] encodes all supplied frames
/// into one object and `put`s it at a deterministic path derived from the
/// writer's identity and a monotone sequence number.  The returned
/// `OutboxEntry` is the only metadata the receiver needs to fetch the object
/// — no LIST operation is ever needed.
pub struct DurableShuffleWriter {
    store: Arc<dyn ObjectStore>,
    sender_worker: WorkerId,
    exchange_id: ExchangeId,
    sequence: AtomicU64,
}

impl DurableShuffleWriter {
    /// Create a new writer for the given worker / exchange pair.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        sender_worker: WorkerId,
        exchange_id: ExchangeId,
    ) -> Self {
        DurableShuffleWriter {
            store,
            sender_worker,
            exchange_id,
            sequence: AtomicU64::new(0),
        }
    }

    /// Write `frames` as one coalesced object.
    ///
    /// Returns an `OutboxEntry` that the receiver can use to fetch and decode
    /// the object without issuing any LIST calls.
    pub async fn write(&self, frames: Vec<ShuffleFrame>) -> Result<OutboxEntry, DurableError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let path_str = format!(
            "shuffle_outbox/{}/{}/{}",
            self.sender_worker.0, self.exchange_id.0, seq
        );
        let path = Path::from(path_str.clone());
        let encoded = encode_object(&frames);
        let frame_count = frames.len();
        let payload = PutPayload::from_bytes(Bytes::from(encoded));
        self.store
            .put(&path, payload)
            .await
            .map_err(|e| DurableError::Store(e.to_string()))?;
        Ok(OutboxEntry {
            path: path_str,
            sequence: seq,
            frame_count,
        })
    }
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Reads coalesced shuffle objects from the object store.
///
/// Callers hand the reader an `OutboxEntry` (never a glob/prefix); the
/// reader fetches the object with a single `get` call and decodes it.
/// `merge_frames` additionally folds duplicate `(target_shard, key)` pairs
/// using the registered `LawBundle`, matching what the direct path would
/// have produced.
pub struct DurableShuffleReader {
    store: Arc<dyn ObjectStore>,
    registry: Arc<LawRegistry>,
}

impl DurableShuffleReader {
    /// Create a new reader.
    pub fn new(store: Arc<dyn ObjectStore>, registry: Arc<LawRegistry>) -> Self {
        DurableShuffleReader { store, registry }
    }

    /// Fetch and decode one outbox object.
    ///
    /// The reader issues exactly one `get` call.  No LIST operation is made.
    pub async fn read(&self, entry: &OutboxEntry) -> Result<Vec<ShuffleFrame>, DurableError> {
        let path = Path::from(entry.path.as_str());
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(|e| DurableError::Store(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| DurableError::Store(e.to_string()))?;
        decode_object(&bytes)
    }

    /// Fetch, decode, and law-merge frames from one outbox object.
    ///
    /// Groups rows by `(target_shard, key)` and folds values using the
    /// `LawBundle` identified by `law_id`.  The result is semantically
    /// identical to what the direct path's pre-shuffle combiner would
    /// produce after delivery.
    pub async fn merge_frames(
        &self,
        law_id: MergeLawId,
        entry: &OutboxEntry,
    ) -> Result<HashMap<(ShardId, Vec<u8>), Vec<u8>>, DurableError> {
        let law = self
            .registry
            .get(law_id)
            .ok_or(DurableError::UnknownLaw(law_id))?;

        let frames = self.read(entry).await?;
        let mut state: HashMap<(ShardId, Vec<u8>), Vec<u8>> = HashMap::new();
        for frame in frames {
            let key = (frame.target_shard, frame.key);
            match state.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let merged = law
                        .merge(e.get(), &frame.value)
                        .map_err(DurableError::Merge)?;
                    *e.get_mut() = merged;
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(frame.value);
                }
            }
        }
        Ok(state)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::ShardId;

    fn frame(shard: u64, key: &[u8], val: &[u8]) -> ShuffleFrame {
        ShuffleFrame {
            target_shard: ShardId(shard),
            key: key.to_vec(),
            value: val.to_vec(),
        }
    }

    #[test]
    fn encode_decode_roundtrip_single_frame() {
        let frames = vec![frame(1, b"k1", b"v1")];
        let data = encode_object(&frames);
        let decoded = decode_object(&data).unwrap();
        assert_eq!(decoded, frames);
    }

    #[test]
    fn encode_decode_roundtrip_many_frames() {
        let frames: Vec<ShuffleFrame> = (0u64..16)
            .map(|i| {
                frame(
                    i % 4,
                    format!("key-{i}").as_bytes(),
                    format!("val-{i}").as_bytes(),
                )
            })
            .collect();
        let data = encode_object(&frames);
        let decoded = decode_object(&data).unwrap();
        assert_eq!(decoded, frames);
    }

    #[test]
    fn encode_decode_empty_batch() {
        let data = encode_object(&[]);
        let decoded = decode_object(&data).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn bad_magic_returns_error() {
        let mut data = encode_object(&[frame(0, b"k", b"v")]);
        data[0] = 0xFF; // corrupt magic
        assert_eq!(decode_object(&data), Err(DurableError::BadMagic));
    }

    #[test]
    fn truncated_data_returns_error() {
        let data = encode_object(&[frame(0, b"k", b"v")]);
        let truncated = &data[..4]; // way too short
        assert!(matches!(
            decode_object(truncated),
            Err(DurableError::TruncatedObject)
        ));
    }

    #[test]
    fn fuzz_decode_object_robustness() {
        // Generate a valid object with multiple frames
        let frames = vec![
            frame(1, b"key1", b"val1"),
            frame(2, b"longerkey", b"shorter"),
            frame(42, b"", b""),
        ];
        let data = encode_object(&frames);

        // Test every truncation length
        for len in 0..data.len() {
            let truncated = &data[..len];
            let _ = decode_object(truncated); // Must not panic
        }

        // Fuzz single-byte corruptions across 10,000 seeds/variations
        // We use a deterministic LCG pseudo-random generator for 10k iterations.
        let mut rng_state: u64 = 0x5EED;
        let next_rng = |state: &mut u64| {
            *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *state
        };

        for _ in 0..10_000 {
            let mut corrupted = data.clone();
            let index = (next_rng(&mut rng_state) as usize) % corrupted.len();
            let bit_flip = next_rng(&mut rng_state) as u8;
            corrupted[index] ^= bit_flip;
            let _ = decode_object(&corrupted); // Must not panic
        }
    }
}
