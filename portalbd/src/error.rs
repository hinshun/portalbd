//! Error types for portalbd.

use std::io;
use thiserror::Error;
use uuid::Uuid;

// Re-export NbdError from the nbd crate
pub use nbd::NbdError;

pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type.
#[derive(Debug, Error)]
pub enum Error {
    #[error("block store error: {0}")]
    Store(#[from] StoreError),

    #[error("nbd protocol error: {0}")]
    Nbd(#[from] NbdError),

    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Block store errors.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(Uuid),

    #[error("block out of bounds: index {index}, device has {device_block_count} blocks")]
    BlockOutOfBounds { index: u64, device_block_count: u64 },

    #[error("device is read-only")]
    ReadOnly,

    #[error("invalid block size: expected {expected_bytes}, got {actual_bytes}")]
    InvalidBlockSize {
        expected_bytes: usize,
        actual_bytes: usize,
    },

    #[error("invalid device size: {reason}")]
    InvalidDeviceSize { reason: &'static str },

    #[error(
        "trim out of bounds: offset {offset} + length {length} exceeds device size {device_size}"
    )]
    TrimOutOfBounds {
        offset: u64,
        length: u64,
        device_size: u64,
    },

    #[error("storage backend error: {message}")]
    Backend { message: String },
}

impl StoreError {
    pub fn backend(err: impl std::fmt::Display) -> Self {
        Self::Backend {
            message: err.to_string(),
        }
    }
}

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    ReadError(io::Error),

    #[error("failed to parse config: {0}")]
    ParseError(String),

    #[error("invalid configuration: {field}: {reason}")]
    InvalidValue {
        field: &'static str,
        reason: &'static str,
    },

    #[error("unsupported storage scheme: {scheme}")]
    UnsupportedScheme { scheme: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let err = StoreError::BlockOutOfBounds {
            index: 100,
            device_block_count: 50,
        };
        assert!(err.to_string().contains("100"));
    }
}
