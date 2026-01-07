//! BlockStore: Block storage with SlateDB checkpoint-based snapshots.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use slatedb::admin::{Admin, AdminBuilder};
use slatedb::config::{CheckpointOptions, CheckpointScope, PutOptions, Settings, WriteOptions};
use slatedb::{Db, WriteBatch};
use uuid::Uuid;

/// Write options that don't wait for WAL durability.
/// Data goes to memtable immediately; durability is achieved via explicit flush().
const WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
};

use crate::config::LsmConfig;
use crate::error::StoreError;
use crate::store::flush_queue::FlushQueue;
use crate::types::{BLOCK_SIZE, BlockIndex, SnapshotInfo, zero_block};

pub struct BlockStore {
    db: Arc<Db>,
    flush_queue: FlushQueue,
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    size_bytes: u64,
    block_count: u64,
    lsm_config: LsmConfig,
}

impl BlockStore {
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        size_bytes: u64,
    ) -> Result<Self, StoreError> {
        Self::open_with_config(path, object_store, size_bytes, LsmConfig::default()).await
    }

    pub async fn open_with_config(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        size_bytes: u64,
        lsm_config: LsmConfig,
    ) -> Result<Self, StoreError> {
        if size_bytes == 0 {
            return Err(StoreError::InvalidDeviceSize {
                reason: "size must be > 0",
            });
        }
        if !size_bytes.is_multiple_of(BLOCK_SIZE as u64) {
            return Err(StoreError::InvalidDeviceSize {
                reason: "size must be block-aligned",
            });
        }

        let path = path.into();
        let block_count = size_bytes / BLOCK_SIZE as u64;
        let settings = Self::build_settings(&lsm_config);

        // Log the LSM configuration being applied
        tracing::info!(
            compression = ?lsm_config.compression_codec,
            block_size = ?lsm_config.sst_block_size,
            "Applying LSM configuration"
        );

        let mut builder = Db::builder(path.clone(), object_store.clone()).with_settings(settings);

        // SST block size is configured via builder, not Settings
        if let Some(block_size) = lsm_config.sst_block_size {
            builder = builder.with_sst_block_size(block_size);
        }

        let db = Arc::new(builder.build().await.map_err(StoreError::backend)?);

        let flush_queue = FlushQueue::new(Arc::clone(&db));

        Ok(Self {
            db,
            flush_queue,
            path,
            object_store,
            size_bytes,
            block_count,
            lsm_config,
        })
    }

    fn build_settings_with_compactor(lsm_config: &LsmConfig, enable_compactor: bool) -> Settings {
        let mut settings = Settings::default();
        // WAL is enabled by default - portalbd relies on SlateDB's write batching for performance
        // settings.wal_enabled = false;
        if let Some(flush_interval) = lsm_config.flush_interval() {
            settings.flush_interval = Some(flush_interval);
        }
        if let Some(l0_sst_size) = lsm_config.l0_sst_size_bytes {
            settings.l0_sst_size_bytes = l0_sst_size as usize;
        }
        if let Some(l0_max_ssts) = lsm_config.l0_max_ssts {
            settings.l0_max_ssts = l0_max_ssts as usize;
        }
        if let Some(max_unflushed) = lsm_config.max_unflushed_bytes {
            settings.max_unflushed_bytes = max_unflushed as usize;
        }
        if let Some(filter_bits) = lsm_config.filter_bits_per_key {
            settings.filter_bits_per_key = filter_bits;
        }
        settings.compression_codec = lsm_config.compression_codec;
        // Note: sst_block_size is configured via DbBuilder, not Settings
        // TODO(slatedb): Re-enable compactor after restore once SlateDB fixes
        // the compactor epoch mismatch issue in restore_checkpoint.
        // See: https://github.com/slatedb/slatedb/pull/1072
        if !enable_compactor {
            settings.compactor_options = None;
        }
        settings
    }

    fn build_settings(lsm_config: &LsmConfig) -> Settings {
        Self::build_settings_with_compactor(lsm_config, true)
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    fn admin(&self) -> Admin {
        AdminBuilder::new(self.path.clone(), self.object_store.clone()).build()
    }

    /// Read a block. Returns zeros if the block was never written.
    pub async fn read_block(&self, index: BlockIndex) -> Result<Bytes, StoreError> {
        if index.0 >= self.block_count {
            return Err(StoreError::BlockOutOfBounds {
                index: index.0,
                device_block_count: self.block_count,
            });
        }

        if let Some(data) = self
            .db
            .get(&index.to_key())
            .await
            .map_err(StoreError::backend)?
        {
            return Ok(data);
        }

        // Unwritten blocks return zeros (using static allocation)
        Ok(zero_block())
    }

    /// Read multiple consecutive blocks efficiently using a single range scan.
    ///
    /// This is faster than individual `read_block` calls when reading consecutive
    /// blocks, as the underlying store builds one iterator instead of N.
    ///
    /// # Arguments
    /// * `start` - First block index to read
    /// * `count` - Number of consecutive blocks to read
    ///
    /// # Returns
    /// Vector of block data in order. Unwritten blocks return zeros.
    pub async fn read_blocks(
        &self,
        start: BlockIndex,
        count: u64,
    ) -> Result<Vec<Bytes>, StoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let end = start.0.saturating_add(count);
        if end > self.block_count {
            return Err(StoreError::BlockOutOfBounds {
                index: end - 1,
                device_block_count: self.block_count,
            });
        }

        // For single block, use point lookup
        if count == 1 {
            return Ok(vec![self.read_block(start).await?]);
        }

        // Use range scan for multiple blocks
        let start_key = start.to_key();
        let end_key = BlockIndex(end).to_key();

        let mut iter = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(StoreError::backend)?;

        // Pre-allocate result with zero blocks
        let mut results: Vec<Bytes> = (0..count).map(|_| zero_block()).collect();

        // Fill in written blocks from scan
        while let Some(entry) = iter.next().await.map_err(StoreError::backend)? {
            if let Some(block_idx) = BlockIndex::from_key(&entry.key) {
                let offset = block_idx.0 - start.0;
                if offset < count {
                    results[offset as usize] = entry.value;
                }
            }
        }

        Ok(results)
    }

    pub async fn write_block(&self, index: BlockIndex, data: Bytes) -> Result<(), StoreError> {
        if data.len() != BLOCK_SIZE {
            return Err(StoreError::InvalidBlockSize {
                expected_bytes: BLOCK_SIZE,
                actual_bytes: data.len(),
            });
        }
        if index.0 >= self.block_count {
            return Err(StoreError::BlockOutOfBounds {
                index: index.0,
                device_block_count: self.block_count,
            });
        }

        // Write to memtable immediately; durability via explicit flush()
        self.db
            .put_with_options(
                &index.to_key(),
                &data,
                &PutOptions::default(),
                &WRITE_OPTIONS,
            )
            .await
            .map_err(StoreError::backend)
    }

    /// Write multiple blocks in a single batch operation.
    ///
    /// This is significantly faster than individual `write_block` calls when
    /// writing multiple blocks, as the underlying store can amortize the
    /// synchronization overhead across all writes.
    ///
    /// # Arguments
    /// * `blocks` - Iterator of (BlockIndex, Bytes) pairs to write
    ///
    /// # Errors
    /// Returns an error if any block has invalid size or is out of bounds.
    /// All blocks are validated before any writes occur.
    pub async fn write_blocks<I>(&self, blocks: I) -> Result<(), StoreError>
    where
        I: IntoIterator<Item = (BlockIndex, Bytes)>,
    {
        let blocks: Vec<_> = blocks.into_iter().collect();

        // Validate all blocks first
        for (index, data) in &blocks {
            if data.len() != BLOCK_SIZE {
                return Err(StoreError::InvalidBlockSize {
                    expected_bytes: BLOCK_SIZE,
                    actual_bytes: data.len(),
                });
            }
            if index.0 >= self.block_count {
                return Err(StoreError::BlockOutOfBounds {
                    index: index.0,
                    device_block_count: self.block_count,
                });
            }
        }

        // Write all blocks in a single batch; durability via explicit flush()
        let mut batch = WriteBatch::new();
        for (index, data) in blocks {
            batch.put(index.to_key(), &data);
        }
        self.db
            .write_with_options(batch, &WRITE_OPTIONS)
            .await
            .map_err(StoreError::backend)
    }

    /// Delete a block, causing subsequent reads to return zeros.
    ///
    /// This is used by trim operations to reclaim storage space.
    pub async fn delete_block(&self, index: BlockIndex) -> Result<(), StoreError> {
        if index.0 >= self.block_count {
            return Err(StoreError::BlockOutOfBounds {
                index: index.0,
                device_block_count: self.block_count,
            });
        }

        self.db
            .delete(&index.to_key())
            .await
            .map_err(StoreError::backend)
    }

    /// Trim a byte range, deleting all blocks that fall within the range.
    ///
    /// Uses batched deletes for efficiency. After trim, reads to the trimmed
    /// region will return zeros.
    ///
    /// # Arguments
    /// * `offset` - Starting byte offset (will be aligned down to block boundary)
    /// * `length` - Number of bytes to trim
    pub async fn trim_range(&self, offset: u64, length: u64) -> Result<(), StoreError> {
        if length == 0 {
            return Ok(());
        }

        let end_offset = offset.saturating_add(length);
        if end_offset > self.size_bytes {
            return Err(StoreError::TrimOutOfBounds {
                offset,
                length,
                device_size: self.size_bytes,
            });
        }

        // Calculate block range (inclusive start, exclusive end)
        let start_block = BlockIndex::from_offset(offset);
        let end_block = BlockIndex::from_offset(end_offset.saturating_sub(1));

        // Process in batches to bound memory usage
        const BATCH_SIZE: u64 = 1024;
        let mut current = start_block.0;

        while current <= end_block.0 {
            let batch_end = (current + BATCH_SIZE).min(end_block.0 + 1);
            let mut batch = WriteBatch::new();

            for block_idx in current..batch_end {
                batch.delete(BlockIndex(block_idx).to_key());
            }

            self.db
                .write_with_options(batch, &WRITE_OPTIONS)
                .await
                .map_err(StoreError::backend)?;
            current = batch_end;
        }

        Ok(())
    }

    /// Create a snapshot capturing the current state of all blocks.
    pub async fn snapshot_create(&self, name: &str) -> Result<SnapshotInfo, StoreError> {
        let options = CheckpointOptions {
            name: Some(name.to_string()),
            lifetime: None,
            ..Default::default()
        };

        let result = self
            .db
            .create_checkpoint(CheckpointScope::All, &options)
            .await
            .map_err(StoreError::backend)?;

        let checkpoints = self
            .admin()
            .list_checkpoints(Some(name))
            .await
            .map_err(StoreError::backend)?;

        let checkpoint = checkpoints
            .into_iter()
            .find(|c| c.id == result.id)
            .ok_or_else(|| StoreError::Backend {
                message: "checkpoint created but not found".to_string(),
            })?;

        Ok(SnapshotInfo {
            id: checkpoint.id,
            name: checkpoint.name.unwrap_or_default(),
            created_at: checkpoint.create_time.timestamp(),
        })
    }

    pub async fn snapshot_list(&self) -> Result<Vec<SnapshotInfo>, StoreError> {
        let checkpoints = self
            .admin()
            .list_checkpoints(None)
            .await
            .map_err(StoreError::backend)?;

        Ok(checkpoints
            .into_iter()
            .filter_map(|c| {
                c.name.map(|name| SnapshotInfo {
                    id: c.id,
                    name,
                    created_at: c.create_time.timestamp(),
                })
            })
            .collect())
    }

    pub async fn snapshot_get(&self, name: &str) -> Result<Option<SnapshotInfo>, StoreError> {
        let checkpoints = self
            .admin()
            .list_checkpoints(Some(name))
            .await
            .map_err(StoreError::backend)?;

        Ok(checkpoints.into_iter().next().map(|c| SnapshotInfo {
            id: c.id,
            name: c.name.unwrap_or_default(),
            created_at: c.create_time.timestamp(),
        }))
    }

    /// Delete a snapshot. Any snapshot can be deleted (data is copied on restore).
    pub async fn snapshot_delete(&self, name: &str) -> Result<(), StoreError> {
        let checkpoints = self
            .admin()
            .list_checkpoints(Some(name))
            .await
            .map_err(StoreError::backend)?;

        let checkpoint = checkpoints
            .into_iter()
            .next()
            .ok_or(StoreError::SnapshotNotFound(Uuid::nil()))?;

        self.admin()
            .delete_checkpoint(checkpoint.id)
            .await
            .map_err(StoreError::backend)
    }

    /// Restore to a snapshot using SlateDB's restore_checkpoint.
    ///
    /// This restores the database to the checkpoint state at the same path.
    /// Requires briefly closing and reopening the database.
    ///
    /// # Arguments
    /// * `name` - Name of the snapshot to restore to
    pub async fn snapshot_restore(&mut self, name: &str) -> Result<(), StoreError> {
        // Find the target checkpoint
        let all_checkpoints = self
            .admin()
            .list_checkpoints(None)
            .await
            .map_err(StoreError::backend)?;

        let target = all_checkpoints
            .iter()
            .find(|c| c.name.as_deref() == Some(name))
            .ok_or(StoreError::SnapshotNotFound(Uuid::nil()))?
            .clone();

        // Record named checkpoints that exist before restore. SlateDB's
        // restore_checkpoint revives checkpoints that existed when the target
        // was created, so we clean up any revived checkpoints afterward.
        let checkpoints_before: HashSet<_> = all_checkpoints
            .iter()
            .filter(|c| c.name.is_some())
            .map(|c| c.id)
            .collect();

        // TODO(slatedb): Remove this workaround once SlateDB's restore_checkpoint
        // supports preserving later checkpoints. See:
        // https://github.com/slatedb/slatedb/pull/1072
        //
        // Currently, restore_checkpoint fails if there are active checkpoints
        // created after the target checkpoint. We must delete them first.
        let checkpoints_to_delete: Vec<_> = all_checkpoints
            .iter()
            .filter(|c| c.manifest_id > target.manifest_id && c.id != target.id)
            .filter(|c| c.name.is_some()) // Only delete named checkpoints (user-created)
            .map(|c| c.id)
            .collect();

        for checkpoint_id in checkpoints_to_delete {
            self.admin()
                .delete_checkpoint(checkpoint_id)
                .await
                .map_err(StoreError::backend)?;
        }

        // Close the current database (required for restore)
        self.db.close().await.map_err(StoreError::backend)?;

        self.admin()
            .restore_checkpoint(target.id)
            .await
            .map_err(StoreError::backend)?;

        // Reopen at the same path with compactor disabled.
        // TODO(slatedb): Re-enable compactor after restore once SlateDB fixes
        // the compactor epoch mismatch issue in restore_checkpoint.
        let db = Arc::new(
            Db::builder(self.path.clone(), self.object_store.clone())
                .with_settings(Self::build_settings_with_compactor(&self.lsm_config, false))
                .build()
                .await
                .map_err(StoreError::backend)?,
        );

        self.flush_queue = FlushQueue::new(Arc::clone(&db));
        self.db = db;

        // Clean up any checkpoints revived by restore. SlateDB's restore brings
        // back the complete checkpoint metadata from the target's manifest, which
        // may include checkpoints that were deleted after the target was created.
        let checkpoints_after = self
            .admin()
            .list_checkpoints(None)
            .await
            .map_err(StoreError::backend)?;

        for checkpoint in checkpoints_after {
            if checkpoint.name.is_some() && !checkpoints_before.contains(&checkpoint.id) {
                self.admin()
                    .delete_checkpoint(checkpoint.id)
                    .await
                    .map_err(StoreError::backend)?;
            }
        }

        Ok(())
    }

    /// Flush all pending writes to durable storage.
    pub async fn flush(&self) -> Result<(), StoreError> {
        self.flush_queue.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn make_store(blocks: u64) -> BlockStore {
        let object_store = Arc::new(InMemory::new());
        BlockStore::open("test", object_store, blocks * BLOCK_SIZE as u64)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn read_unwritten_returns_zeros() {
        let store = make_store(10).await;
        let data = store.read_block(BlockIndex(0)).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn write_and_read() {
        let store = make_store(10).await;
        let data = Bytes::from(vec![0xAB; BLOCK_SIZE]);
        store
            .write_block(BlockIndex(0), data.clone())
            .await
            .unwrap();
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap(), data);
    }

    #[tokio::test]
    async fn bounds_checking() {
        let store = make_store(10).await;
        assert!(matches!(
            store.read_block(BlockIndex(100)).await,
            Err(StoreError::BlockOutOfBounds { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_lifecycle() {
        let store = make_store(10).await;
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAB; BLOCK_SIZE]))
            .await
            .unwrap();

        store.snapshot_create("snap1").await.unwrap();
        assert_eq!(store.snapshot_list().await.unwrap().len(), 1);

        store.snapshot_delete("snap1").await.unwrap();
        assert!(store.snapshot_list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn can_delete_snapshot_after_restore() {
        let mut store = make_store(10).await;
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap1").await.unwrap();

        // Write new data
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xBB; BLOCK_SIZE]))
            .await
            .unwrap();

        // Restore to snap1
        store.snapshot_restore("snap1").await.unwrap();

        // Data is restored
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xAA);

        // Verify the data is still accessible after restore
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xAA);
    }

    #[tokio::test]
    async fn concurrent_writes_to_different_blocks() {
        let store = Arc::new(make_store(100).await);
        let mut handles = Vec::new();

        for i in 0..10 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let idx = BlockIndex(i * 10 + j);
                    let pattern = ((i * 10 + j) % 256) as u8;
                    let data = Bytes::from(vec![pattern; BLOCK_SIZE]);
                    store.write_block(idx, data).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        for i in 0..100 {
            let data = store.read_block(BlockIndex(i)).await.unwrap();
            let expected = (i % 256) as u8;
            assert!(data.iter().all(|&b| b == expected), "block {} mismatch", i);
        }
    }

    #[tokio::test]
    async fn concurrent_reads_and_writes() {
        let store = Arc::new(make_store(10).await);
        let pattern = Bytes::from(vec![0xAB; BLOCK_SIZE]);
        store
            .write_block(BlockIndex(0), pattern.clone())
            .await
            .unwrap();

        let mut handles = Vec::new();

        for _ in 0..5 {
            let store = Arc::clone(&store);
            let pattern = pattern.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let data = store.read_block(BlockIndex(0)).await.unwrap();
                    assert_eq!(data, pattern);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn snapshot_restore_returns_snapshot_data() {
        let mut store = make_store(10).await;

        // Write initial data
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(1), Bytes::from(vec![0xBB; BLOCK_SIZE]))
            .await
            .unwrap();

        // Create snapshot
        store.snapshot_create("s1").await.unwrap();

        // Write new data to block 0 (overwrite) and block 2 (new)
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xCC; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(2), Bytes::from(vec![0xDD; BLOCK_SIZE]))
            .await
            .unwrap();

        // Verify current state before restore
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xCC);
        assert_eq!(store.read_block(BlockIndex(2)).await.unwrap()[0], 0xDD);

        // Restore to snapshot
        store.snapshot_restore("s1").await.unwrap();

        // Block 0 should be restored to snapshot value (0xAA), not current value (0xCC)
        let b0 = store.read_block(BlockIndex(0)).await.unwrap();
        assert_eq!(b0[0], 0xAA, "block 0 should be restored to snapshot value");

        // Block 1 should still have snapshot value
        let b1 = store.read_block(BlockIndex(1)).await.unwrap();
        assert_eq!(b1[0], 0xBB, "block 1 should retain snapshot value");

        // Block 2 was written after snapshot, should be zeros after restore
        let b2 = store.read_block(BlockIndex(2)).await.unwrap();
        assert!(
            b2.iter().all(|&b| b == 0),
            "block 2 should be zeros after restore"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_then_write_creates_new_state() {
        let mut store = make_store(10).await;

        // Write initial data and create snapshot
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("s1").await.unwrap();

        // Overwrite and restore
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xBB; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_restore("s1").await.unwrap();

        // Verify restored state
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xAA);

        // Write new data after restore
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xCC; BLOCK_SIZE]))
            .await
            .unwrap();

        // New write should take precedence over snapshot
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xCC);
    }

    #[tokio::test]
    async fn snapshot_restore_multiple_generations() {
        // Tests the exact scenario from integration test: gen1 -> snap1 -> gen2 -> snap2 -> restore snap1
        let object_store = Arc::new(InMemory::new());
        let mut store = BlockStore::open("test", object_store.clone(), 10 * BLOCK_SIZE as u64)
            .await
            .unwrap();

        // Generation 1: Write initial data
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x11; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(1), Bytes::from(vec![0x11; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap1").await.unwrap();

        // Generation 2: Overwrite block 0, add block 2
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x22; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(2), Bytes::from(vec![0x22; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap2").await.unwrap();

        // Verify gen2 state
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0x22);
        assert_eq!(store.read_block(BlockIndex(1)).await.unwrap()[0], 0x11);
        assert_eq!(store.read_block(BlockIndex(2)).await.unwrap()[0], 0x22);

        // Restore to snap1 (deletes snap2 as workaround)
        store.snapshot_restore("snap1").await.unwrap();

        // Verify gen1 state is restored
        assert_eq!(
            store.read_block(BlockIndex(0)).await.unwrap()[0],
            0x11,
            "block 0 should be gen1"
        );
        assert_eq!(
            store.read_block(BlockIndex(1)).await.unwrap()[0],
            0x11,
            "block 1 should be gen1"
        );
        assert!(
            store
                .read_block(BlockIndex(2))
                .await
                .unwrap()
                .iter()
                .all(|&b| b == 0),
            "block 2 should be zeros (didn't exist in snap1)"
        );
    }

    #[tokio::test]
    async fn snapshot_after_restore_creates_nested_state() {
        // Test: snap1 -> write -> restore snap1 -> write -> snap2 -> restore snap2
        let mut store = make_store(10).await;

        // Initial state and snap1
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap1").await.unwrap();

        // Modify and restore
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xBB; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_restore("snap1").await.unwrap();

        // Verify restored state
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xAA);

        // Write new data after restore and create snap2
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xCC; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap2").await.unwrap();

        // Restore to snap2
        store.snapshot_restore("snap2").await.unwrap();
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 0xCC);
    }

    #[tokio::test]
    async fn snapshot_after_restore_includes_checkpoint_data() {
        // Critical test: snapshot created after restore must include ALL data,
        // not just the writes made after restore.
        let mut store = make_store(10).await;

        // Write to multiple blocks and create snap1
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x11; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(1), Bytes::from(vec![0x11; BLOCK_SIZE]))
            .await
            .unwrap();
        store
            .write_block(BlockIndex(2), Bytes::from(vec![0x11; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap1").await.unwrap();

        // Overwrite some blocks
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x22; BLOCK_SIZE]))
            .await
            .unwrap();

        // Restore to snap1
        store.snapshot_restore("snap1").await.unwrap();

        // Write new data to ONE block after restore
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x33; BLOCK_SIZE]))
            .await
            .unwrap();

        // Create snap2 - this includes all inherited data plus new writes
        store.snapshot_create("snap2").await.unwrap();

        // Now restore to snap2 and verify ALL blocks are present
        store.snapshot_restore("snap2").await.unwrap();

        // Block 0 was written after restore, should be 0x33
        assert_eq!(
            store.read_block(BlockIndex(0)).await.unwrap()[0],
            0x33,
            "block 0 should be the post-restore write"
        );

        // Blocks 1 and 2 were inherited from snap1 and should be in snap2
        assert_eq!(
            store.read_block(BlockIndex(1)).await.unwrap()[0],
            0x11,
            "block 1 should be inherited from snap1"
        );
        assert_eq!(
            store.read_block(BlockIndex(2)).await.unwrap()[0],
            0x11,
            "block 2 should be inherited from snap1"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_nonexistent_fails() {
        let mut store = make_store(10).await;
        let result = store.snapshot_restore("nonexistent").await;
        assert!(matches!(result, Err(StoreError::SnapshotNotFound(_))));
    }

    #[tokio::test]
    async fn snapshot_restore_preserves_unwritten_blocks_as_zeros() {
        let mut store = make_store(10).await;

        // Only write to block 5
        store
            .write_block(BlockIndex(5), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("snap1").await.unwrap();

        // Write to many blocks after snapshot
        for i in 0..10 {
            store
                .write_block(BlockIndex(i), Bytes::from(vec![0xBB; BLOCK_SIZE]))
                .await
                .unwrap();
        }

        // Restore
        store.snapshot_restore("snap1").await.unwrap();

        // Only block 5 should have data, all others should be zeros
        for i in 0..10 {
            let data = store.read_block(BlockIndex(i)).await.unwrap();
            if i == 5 {
                assert_eq!(data[0], 0xAA, "block 5 should have snapshot data");
            } else {
                assert!(data.iter().all(|&b| b == 0), "block {} should be zeros", i);
            }
        }
    }

    #[tokio::test]
    async fn delete_block_returns_zeros() {
        let store = make_store(10).await;

        // Write then delete
        store
            .write_block(BlockIndex(0), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();
        store.delete_block(BlockIndex(0)).await.unwrap();

        // Should return zeros
        let data = store.read_block(BlockIndex(0)).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn trim_range_deletes_blocks() {
        let store = make_store(10).await;

        // Write blocks 0-4
        for i in 0..5 {
            store
                .write_block(BlockIndex(i), Bytes::from(vec![(i + 1) as u8; BLOCK_SIZE]))
                .await
                .unwrap();
        }

        // Trim blocks 1-3 (by byte range)
        let offset = BLOCK_SIZE as u64;
        let length = 3 * BLOCK_SIZE as u64;
        store.trim_range(offset, length).await.unwrap();

        // Block 0 should be preserved
        assert_eq!(store.read_block(BlockIndex(0)).await.unwrap()[0], 1);

        // Blocks 1-3 should be zeros
        for i in 1..4 {
            let data = store.read_block(BlockIndex(i)).await.unwrap();
            assert!(
                data.iter().all(|&b| b == 0),
                "block {} should be zeros after trim",
                i
            );
        }

        // Block 4 should be preserved
        assert_eq!(store.read_block(BlockIndex(4)).await.unwrap()[0], 5);
    }

    #[tokio::test]
    async fn invalid_block_size_rejected() {
        let store = make_store(10).await;
        let result = store
            .write_block(BlockIndex(0), Bytes::from(vec![0u8; 100]))
            .await;
        assert!(matches!(result, Err(StoreError::InvalidBlockSize { .. })));
    }

    #[tokio::test]
    async fn write_bounds_checking() {
        let store = make_store(10).await;
        let result = store
            .write_block(BlockIndex(100), Bytes::from(vec![0u8; BLOCK_SIZE]))
            .await;
        assert!(matches!(result, Err(StoreError::BlockOutOfBounds { .. })));
    }

    #[tokio::test]
    async fn snapshot_get_returns_none_for_unknown() {
        let store = make_store(10).await;
        let result = store.snapshot_get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn write_blocks_batch() {
        let store = make_store(10).await;
        let data = Bytes::from(vec![0xAB; BLOCK_SIZE]);

        let blocks: Vec<_> = (0..5).map(|i| (BlockIndex(i), data.clone())).collect();

        store.write_blocks(blocks).await.unwrap();

        // Verify data was written correctly
        for i in 0..5 {
            let read = store.read_block(BlockIndex(i)).await.unwrap();
            assert_eq!(read[0], 0xAB);
        }
    }

    #[tokio::test]
    async fn write_blocks_validates_all() {
        let store = make_store(10).await;

        // Should fail if any block is invalid
        let blocks = vec![
            (BlockIndex(0), Bytes::from(vec![0xAB; BLOCK_SIZE])),
            (BlockIndex(100), Bytes::from(vec![0xAB; BLOCK_SIZE])), // out of bounds
        ];

        let result = store.write_blocks(blocks).await;
        assert!(matches!(result, Err(StoreError::BlockOutOfBounds { .. })));

        // Verify first block wasn't written (atomic all-or-nothing)
        let data = store.read_block(BlockIndex(0)).await.unwrap();
        assert!(data.iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn read_blocks_batch() {
        let store = make_store(20).await;

        // Write alternating blocks (0, 2, 4, 6, 8)
        for i in (0..10).step_by(2) {
            store
                .write_block(BlockIndex(i), Bytes::from(vec![(i + 1) as u8; BLOCK_SIZE]))
                .await
                .unwrap();
        }

        // Read blocks 0-9 in one batch
        let blocks = store.read_blocks(BlockIndex(0), 10).await.unwrap();
        assert_eq!(blocks.len(), 10);

        // Verify written blocks have data, unwritten blocks are zeros
        for (i, block) in blocks.iter().enumerate() {
            if i % 2 == 0 {
                // Written block
                assert_eq!(block[0], (i + 1) as u8, "block {} should have data", i);
            } else {
                // Unwritten block should be zeros
                assert!(block.iter().all(|&b| b == 0), "block {} should be zeros", i);
            }
        }
    }

    #[tokio::test]
    async fn read_blocks_empty() {
        let store = make_store(10).await;
        let blocks = store.read_blocks(BlockIndex(0), 0).await.unwrap();
        assert!(blocks.is_empty());
    }

    #[tokio::test]
    async fn read_blocks_single() {
        let store = make_store(10).await;
        store
            .write_block(BlockIndex(5), Bytes::from(vec![0xAA; BLOCK_SIZE]))
            .await
            .unwrap();

        // Single block read uses point lookup
        let blocks = store.read_blocks(BlockIndex(5), 1).await.unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0][0], 0xAA);
    }

    #[tokio::test]
    async fn read_blocks_bounds_check() {
        let store = make_store(10).await;
        let result = store.read_blocks(BlockIndex(5), 10).await;
        assert!(matches!(result, Err(StoreError::BlockOutOfBounds { .. })));
    }

    #[tokio::test]
    async fn multiple_snapshots() {
        let store = make_store(10).await;

        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x01; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("s1").await.unwrap();

        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x02; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("s2").await.unwrap();

        store
            .write_block(BlockIndex(0), Bytes::from(vec![0x03; BLOCK_SIZE]))
            .await
            .unwrap();
        store.snapshot_create("s3").await.unwrap();

        let snapshots = store.snapshot_list().await.unwrap();
        assert_eq!(snapshots.len(), 3);

        store.snapshot_delete("s2").await.unwrap();
        let snapshots = store.snapshot_list().await.unwrap();
        assert_eq!(snapshots.len(), 2);
    }
}
