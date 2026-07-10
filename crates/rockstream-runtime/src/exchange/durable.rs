//! Coalesced Durable Shuffle Writer & Reader.
//!
//! Concatenates frames into a single object-store object with a JSON index footer,
//! permitting individual shard data extraction using precise range reads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use rockstream_types::error_code::RS_3010;

/// Named upper bound for in-memory shuffle frame accumulation (16MB).
pub const MAX_DURABLE_BUFFER_SIZE_BYTES: usize = 16 * 1024 * 1024;

macro_rules! retry_op {
    ($store:expr, $op:expr) => {{
        let mut attempts = 0;
        let mut delay = std::time::Duration::from_millis(10);
        loop {
            match $op.await {
                Ok(val) => break Ok(val),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("429") || err_str.contains("Too Many Requests") || err_str.contains("throttling") {
                        attempts += 1;
                        if attempts >= 5 {
                            break Err(format!(
                                "[{}] Failed to complete operation after 5 rate-limited attempts: {:?}",
                                RS_3010, e
                            ));
                        }
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                    } else {
                        break Err(format!(
                            "[{}] Object store operation failed: {:?}",
                            RS_3010, e
                        ));
                    }
                }
            }
        }
    }};
}

/// Metadata entry representing a single serialized frame in the coalesced shuffle object.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShuffleIndexEntry {
    pub src_shard: u32,
    pub target_shard: u32,
    pub seq: u64,
    pub offset: u64,
    pub length: u64,
}

/// The index footer containing metadata for all coalesced frames.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShuffleIndexFooter {
    pub entries: Vec<ShuffleIndexEntry>,
}

/// Concatenates frame bytes, builds `ShuffleIndexFooter`, and uploads the coalesced object.
pub struct DurableShuffleWriter {
    buffer: Vec<u8>,
    footer: ShuffleIndexFooter,
    fill_level: Arc<AtomicUsize>,
}

impl Default for DurableShuffleWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl DurableShuffleWriter {
    /// Create a new writer.
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            footer: ShuffleIndexFooter {
                entries: Vec::new(),
            },
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the current size of the accumulated frame buffer in bytes (fill-level metric).
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Add a frame to the coalesced buffer. Enforces the buffer capacity bounds.
    pub fn add_frame(
        &mut self,
        src_shard: u32,
        target_shard: u32,
        seq: u64,
        payload: &[u8],
    ) -> Result<(), String> {
        let frame_len = payload.len();
        if self.buffer.len() + frame_len > MAX_DURABLE_BUFFER_SIZE_BYTES {
            return Err(format!(
                "[{}] Durable shuffle buffer capacity exceeded (limit: {} bytes)",
                RS_3010, MAX_DURABLE_BUFFER_SIZE_BYTES
            ));
        }

        let offset = self.buffer.len() as u64;
        self.buffer.extend_from_slice(payload);

        self.footer.entries.push(ShuffleIndexEntry {
            src_shard,
            target_shard,
            seq,
            offset,
            length: frame_len as u64,
        });

        self.fill_level.store(self.buffer.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Serializes the footer, appends it along with the footer length, and uploads the object.
    pub async fn finish(mut self, store: &dyn ObjectStore, path: &Path) -> Result<(), String> {
        let footer_bytes = serde_json::to_vec(&self.footer)
            .map_err(|e| format!("[{}] Failed to serialize index footer: {:?}", RS_3010, e))?;

        let footer_len = footer_bytes.len() as u64;

        // Append footer bytes
        self.buffer.extend_from_slice(&footer_bytes);

        // Append footer length as 8-byte big-endian u64
        self.buffer.extend_from_slice(&footer_len.to_be_bytes());

        // Put to object store with retry
        let payload = Bytes::from(self.buffer);
        retry_op!(store, store.put(path, payload.clone().into()))?;

        Ok(())
    }
}

/// Reads the coalesced object footer and extracts target frames using range reads.
pub struct DurableShuffleReader;

impl DurableShuffleReader {
    /// Retrieve the `ShuffleIndexFooter` from the end of the coalesced object-store file.
    pub async fn read_footer(
        store: &dyn ObjectStore,
        path: &Path,
    ) -> Result<ShuffleIndexFooter, String> {
        let meta = retry_op!(store, store.head(path))?;

        let size = meta.size as u64;

        if size < 8 {
            return Err(format!(
                "[{}] File too small to contain a footer length: size={}",
                RS_3010, size
            ));
        }

        // 1. Read last 8 bytes to get the footer length
        let footer_len_range = (size - 8)..size;
        let footer_len_bytes = retry_op!(store, store.get_range(path, footer_len_range.clone()))?;

        if footer_len_bytes.len() != 8 {
            return Err(format!(
                "[{}] Invalid footer length bytes read: expected 8, got {}",
                RS_3010,
                footer_len_bytes.len()
            ));
        }

        let footer_len = u64::from_be_bytes(footer_len_bytes.as_ref().try_into().unwrap());

        if size < 8 + footer_len {
            return Err(format!(
                "[{}] File size too small for specified footer length: size={}, footer_len={}",
                RS_3010, size, footer_len
            ));
        }

        // 2. Read footer bytes
        let footer_range = (size - 8 - footer_len)..(size - 8);
        let footer_bytes = retry_op!(store, store.get_range(path, footer_range.clone()))?;

        // 3. Deserialize footer
        let footer: ShuffleIndexFooter = serde_json::from_slice(&footer_bytes)
            .map_err(|e| format!("[{}] Failed to deserialize index footer: {:?}", RS_3010, e))?;

        Ok(footer)
    }

    /// Read frame payload bytes for a specific index entry.
    pub async fn read_frame(
        store: &dyn ObjectStore,
        path: &Path,
        entry: &ShuffleIndexEntry,
    ) -> Result<Bytes, String> {
        let start = entry.offset;
        let end = entry.offset + entry.length;
        retry_op!(store, store.get_range(path, start..end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_durable_shuffle_writer_reader_roundtrip() {
        let store = InMemory::new();
        let path = Path::from("test_shuffle.arrow");

        let mut writer = DurableShuffleWriter::new();
        writer.add_frame(1, 10, 100, b"frame_one_payload").unwrap();
        writer
            .add_frame(2, 10, 101, b"frame_two_payload_is_longer")
            .unwrap();
        writer.add_frame(1, 11, 102, b"frame_three").unwrap();

        assert_eq!(writer.fill_level(), 17 + 27 + 11);

        writer.finish(&store, &path).await.unwrap();

        // Now read footer back
        let footer = DurableShuffleReader::read_footer(&store, &path)
            .await
            .unwrap();
        assert_eq!(footer.entries.len(), 3);

        assert_eq!(
            footer.entries[0],
            ShuffleIndexEntry {
                src_shard: 1,
                target_shard: 10,
                seq: 100,
                offset: 0,
                length: 17,
            }
        );
        assert_eq!(
            footer.entries[1],
            ShuffleIndexEntry {
                src_shard: 2,
                target_shard: 10,
                seq: 101,
                offset: 17,
                length: 27,
            }
        );
        assert_eq!(
            footer.entries[2],
            ShuffleIndexEntry {
                src_shard: 1,
                target_shard: 11,
                seq: 102,
                offset: 44,
                length: 11,
            }
        );

        // Read frames back
        let f1 = DurableShuffleReader::read_frame(&store, &path, &footer.entries[0])
            .await
            .unwrap();
        assert_eq!(f1.as_ref(), b"frame_one_payload");

        let f2 = DurableShuffleReader::read_frame(&store, &path, &footer.entries[1])
            .await
            .unwrap();
        assert_eq!(f2.as_ref(), b"frame_two_payload_is_longer");

        let f3 = DurableShuffleReader::read_frame(&store, &path, &footer.entries[2])
            .await
            .unwrap();
        assert_eq!(f3.as_ref(), b"frame_three");
    }

    #[tokio::test]
    async fn test_durable_shuffle_buffer_limit() {
        let mut writer = DurableShuffleWriter::new();
        let large_payload = vec![0u8; MAX_DURABLE_BUFFER_SIZE_BYTES + 1];
        let res = writer.add_frame(1, 2, 3, &large_payload);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("RS-3010"));
    }
}
