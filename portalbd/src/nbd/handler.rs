//! NBD operation handler trait for middleware composition.
//!
//! This module provides a trait-based approach for handling NBD operations,
//! enabling middleware patterns like recording, metrics, and fault injection.
//!
//! # Example
//!
//! ```ignore
//! // Compose handlers: recording wraps the core handler
//! let core = BlockStoreHandler::new(store);
//! let handler = RecordingHandler::new(core, recorder);
//!
//! // Use with NBD server
//! let server = NbdServer::with_handler(Arc::new(handler));
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::StoreError;
use crate::store::BlockStore;
use crate::types::{BLOCK_SIZE, BlockIndex};

/// Result of an NBD operation.
pub type HandlerResult<T> = std::result::Result<T, StoreError>;

/// Trait for handling NBD operations.
///
/// Implementations can be composed as middleware layers. Each handler can
/// delegate to an inner handler after performing its own logic.
#[async_trait]
pub trait TransmissionHandler: Send + Sync {
    /// Read bytes from the device.
    async fn read(&self, offset: u64, length: usize) -> HandlerResult<Bytes>;

    /// Write bytes to the device.
    async fn write(&self, offset: u64, data: Bytes) -> HandlerResult<()>;

    /// Trim (discard) a byte range.
    async fn trim(&self, offset: u64, length: u64) -> HandlerResult<()>;

    /// Write zeroes to a byte range.
    async fn write_zeroes(&self, offset: u64, length: u64) -> HandlerResult<()>;

    /// Flush pending writes to stable storage.
    async fn flush(&self) -> HandlerResult<()>;
}

/// Core handler that performs actual I/O against a BlockStore.
pub struct BlockStoreHandler {
    store: Arc<RwLock<BlockStore>>,
}

impl BlockStoreHandler {
    pub fn new(store: Arc<RwLock<BlockStore>>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl TransmissionHandler for BlockStoreHandler {
    async fn read(&self, offset: u64, length: usize) -> HandlerResult<Bytes> {
        use bytes::BytesMut;

        let guard = self.store.read().await;
        let mut result = BytesMut::with_capacity(length);
        let mut remaining = length;
        let mut pos = offset;

        while remaining > 0 {
            let block_idx = BlockIndex::from_offset(pos);
            let in_block = (pos % BLOCK_SIZE as u64) as usize;
            let to_read = remaining.min(BLOCK_SIZE - in_block);

            let block = guard.read_block(block_idx).await?;
            result.extend_from_slice(&block[in_block..in_block + to_read]);

            remaining -= to_read;
            pos += to_read as u64;
        }

        Ok(result.freeze())
    }

    async fn write(&self, offset: u64, data: Bytes) -> HandlerResult<()> {
        let guard = self.store.read().await;
        let mut remaining = &data[..];
        let mut pos = offset;
        let mut blocks = Vec::new();

        // Collect all blocks to write
        while !remaining.is_empty() {
            let block_idx = BlockIndex::from_offset(pos);
            let in_block = (pos % BLOCK_SIZE as u64) as usize;
            let to_write = remaining.len().min(BLOCK_SIZE - in_block);

            let block = if in_block == 0 && to_write == BLOCK_SIZE {
                Bytes::copy_from_slice(&remaining[..to_write])
            } else {
                let mut buf = guard.read_block(block_idx).await?.to_vec();
                buf[in_block..in_block + to_write].copy_from_slice(&remaining[..to_write]);
                Bytes::from(buf)
            };

            blocks.push((block_idx, block));
            remaining = &remaining[to_write..];
            pos += to_write as u64;
        }

        // Write all blocks in one batch for better performance
        if blocks.len() == 1 {
            let (idx, data) = blocks.into_iter().next().unwrap();
            guard.write_block(idx, data).await
        } else {
            guard.write_blocks(blocks).await
        }
    }

    async fn trim(&self, offset: u64, length: u64) -> HandlerResult<()> {
        let guard = self.store.read().await;
        guard.trim_range(offset, length).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> HandlerResult<()> {
        if length == 0 {
            return Ok(());
        }

        let guard = self.store.read().await;
        let zero_block = Bytes::from(vec![0u8; BLOCK_SIZE]);
        let mut remaining = length;
        let mut pos = offset;
        let mut blocks = Vec::new();

        // Collect all blocks to write
        while remaining > 0 {
            let block_idx = BlockIndex::from_offset(pos);
            let in_block = (pos % BLOCK_SIZE as u64) as usize;
            let to_write = (remaining as usize).min(BLOCK_SIZE - in_block);

            let block = if in_block == 0 && to_write == BLOCK_SIZE {
                zero_block.clone()
            } else {
                let mut buf = guard.read_block(block_idx).await?.to_vec();
                buf[in_block..in_block + to_write].fill(0);
                Bytes::from(buf)
            };

            blocks.push((block_idx, block));
            remaining -= to_write as u64;
            pos += to_write as u64;
        }

        // Write all blocks in one batch for better performance
        if blocks.len() == 1 {
            let (idx, data) = blocks.into_iter().next().unwrap();
            guard.write_block(idx, data).await
        } else {
            guard.write_blocks(blocks).await
        }
    }

    async fn flush(&self) -> HandlerResult<()> {
        let guard = self.store.read().await;
        guard.flush().await
    }
}
