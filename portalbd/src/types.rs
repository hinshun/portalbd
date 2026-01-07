//! Core types for portalbd.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Block size in bytes (64KB).
pub const BLOCK_SIZE: usize = 64 * 1024;

const _: () = {
    assert!(BLOCK_SIZE >= 512);
    assert!(BLOCK_SIZE.is_power_of_two());
};

/// Returns a reference-counted zero block.
///
/// This avoids allocating 64KB on every unwritten block read.
/// `Bytes::clone()` is O(1) as it only increments the reference count.
#[inline]
pub fn zero_block() -> Bytes {
    static ZERO_BLOCK: std::sync::OnceLock<Bytes> = std::sync::OnceLock::new();
    ZERO_BLOCK
        .get_or_init(|| Bytes::from_static(&[0u8; BLOCK_SIZE]))
        .clone()
}

/// Block index for SlateDB key encoding.
///
/// Encodes as 8-byte big-endian for lexicographic ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct BlockIndex(pub u64);

impl BlockIndex {
    #[inline]
    pub fn from_offset(offset: u64) -> Self {
        Self(offset / BLOCK_SIZE as u64)
    }

    #[inline]
    pub fn to_offset(self) -> u64 {
        self.0 * BLOCK_SIZE as u64
    }

    #[inline]
    pub fn to_key(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_key(key: &[u8]) -> Option<Self> {
        let bytes: [u8; 8] = key.try_into().ok()?;
        Some(Self(u64::from_be_bytes(bytes)))
    }
}

impl std::fmt::Display for BlockIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Bounded payload length for NBD operations that transfer data.
///
/// This bounds the maximum size of READ/WRITE payloads to prevent unbounded
/// memory allocation. Per the NBD protocol specification, the default maximum
/// payload size is 32 MiB (2^25 bytes).
///
/// Note: Operations without payloads (TRIM, WRITE_ZEROES, CACHE) are not
/// bounded by this limit - they can specify ranges up to the device size.
/// The NBD spec explicitly states: "For commands without payload requirements
/// like NBD_CMD_TRIM, the client MAY request an effect length larger than
/// the maximum payload size."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoLength(u32);

impl IoLength {
    /// Maximum payload size per NBD protocol specification (32 MiB).
    ///
    /// This is the default maximum that portable clients expect servers to support.
    /// It bounds memory allocation for READ/WRITE buffers.
    pub const MAX: u32 = 32 * 1024 * 1024; // 32 MiB (2^25 bytes)

    #[inline]
    pub fn new(length: u32) -> Option<Self> {
        if length <= Self::MAX {
            Some(Self(length))
        } else {
            None
        }
    }

    #[inline]
    pub fn get(self) -> u32 {
        self.0
    }

    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

/// Snapshot information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub id: Uuid,
    pub name: String,
    pub created_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_index_roundtrip() {
        let idx = BlockIndex(12345);
        assert_eq!(BlockIndex::from_key(&idx.to_key()), Some(idx));
    }

    #[test]
    fn block_index_from_offset() {
        assert_eq!(BlockIndex::from_offset(0).0, 0);
        assert_eq!(BlockIndex::from_offset(BLOCK_SIZE as u64).0, 1);
        assert_eq!(BlockIndex::from_offset(BLOCK_SIZE as u64 + 100).0, 1);
    }

    #[test]
    fn io_length_bounded() {
        assert!(IoLength::new(0).is_some());
        assert!(IoLength::new(IoLength::MAX).is_some());
        assert!(IoLength::new(IoLength::MAX + 1).is_none());
    }
}
