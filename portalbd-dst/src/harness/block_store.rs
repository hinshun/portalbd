//! BlockStore-level simulation harness.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use portalbd::object_store::ObjectStore;
use portalbd::object_store::memory::InMemory;
use portalbd::{BLOCK_SIZE, BlockIndex, BlockStore};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::SimulationError;

/// Actions that can be performed in simulation.
#[derive(Debug, Clone)]
pub enum BlockStoreAction {
    Read { index: u64 },
    Write { index: u64, data: Vec<u8> },
    SnapshotCreate { name: String },
    SnapshotDelete { name: String },
    SnapshotRestore { name: String },
    Flush,
}

/// In-memory oracle for verifying correctness.
pub struct BlockStoreOracle {
    blocks: HashMap<u64, Vec<u8>>,
    snapshots: HashMap<String, HashMap<u64, Vec<u8>>>,
    snapshot_order: Vec<String>,
}

impl BlockStoreOracle {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            snapshots: HashMap::new(),
            snapshot_order: Vec::new(),
        }
    }

    pub fn read(&self, index: u64) -> Vec<u8> {
        self.blocks
            .get(&index)
            .cloned()
            .unwrap_or_else(|| vec![0u8; BLOCK_SIZE])
    }

    pub fn write(&mut self, index: u64, data: Vec<u8>) {
        self.blocks.insert(index, data);
    }

    pub fn snapshot_create(&mut self, name: &str) {
        self.snapshots.insert(name.to_string(), self.blocks.clone());
        self.snapshot_order.push(name.to_string());
    }

    pub fn snapshot_delete(&mut self, name: &str) -> bool {
        if self.snapshots.remove(name).is_some() {
            self.snapshot_order.retain(|n| n != name);
            true
        } else {
            false
        }
    }

    pub fn snapshot_restore(&mut self, name: &str) -> bool {
        if let Some(snapshot) = self.snapshots.get(name) {
            self.blocks = snapshot.clone();
            if let Some(pos) = self.snapshot_order.iter().position(|n| n == name) {
                let to_delete: Vec<String> =
                    self.snapshot_order.iter().skip(pos + 1).cloned().collect();
                for snap_name in to_delete {
                    self.snapshots.remove(&snap_name);
                }
                self.snapshot_order.truncate(pos + 1);
            }
            true
        } else {
            false
        }
    }

    pub fn snapshot_names(&self) -> Vec<String> {
        self.snapshots.keys().cloned().collect()
    }
}

impl Default for BlockStoreOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulation statistics.
#[derive(Debug, Default, Clone)]
pub struct BlockStoreStats {
    pub operations: u64,
    pub reads: u64,
    pub writes: u64,
    pub snapshots_created: u64,
}

/// BlockStore simulation harness.
pub struct BlockStoreHarness {
    rng: SmallRng,
    store: Option<BlockStore>,
    oracle: BlockStoreOracle,
    object_store: Arc<dyn ObjectStore>,
    block_count: u64,
    snapshot_counter: u64,
    stats: BlockStoreStats,
}

impl BlockStoreHarness {
    pub fn new(seed: u64, block_count: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            store: None,
            oracle: BlockStoreOracle::new(),
            object_store: Arc::new(InMemory::new()),
            block_count,
            snapshot_counter: 0,
            stats: BlockStoreStats::default(),
        }
    }

    pub async fn init(&mut self) -> Result<(), portalbd::StoreError> {
        let size = self.block_count * BLOCK_SIZE as u64;
        let store = BlockStore::open("dst", self.object_store.clone(), size).await?;
        self.store = Some(store);
        Ok(())
    }

    fn store(&self) -> &BlockStore {
        self.store.as_ref().expect("not initialized")
    }

    fn store_mut(&mut self) -> &mut BlockStore {
        self.store.as_mut().expect("not initialized")
    }

    /// Run the simulation for a given number of operations.
    pub async fn run(&mut self, operations: u64) -> Result<(), SimulationError> {
        for _ in 0..operations {
            let action = self.sample_action();
            self.execute(action).await?;
            self.stats.operations += 1;
            self.validate_invariants().await?;
        }
        Ok(())
    }

    async fn validate_invariants(&self) -> Result<(), SimulationError> {
        let store = self.store();

        if store.block_count() != self.block_count {
            return Err(SimulationError::Mismatch {
                context: format!(
                    "block_count: expected {}, got {}",
                    self.block_count,
                    store.block_count()
                ),
            });
        }

        let store_snapshots = store
            .snapshot_list()
            .await
            .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
        let oracle_names = self.oracle.snapshot_names();

        if store_snapshots.len() != oracle_names.len() {
            return Err(SimulationError::Mismatch {
                context: format!(
                    "snapshot_count: expected {}, got {}",
                    oracle_names.len(),
                    store_snapshots.len()
                ),
            });
        }

        Ok(())
    }

    async fn validate_all_blocks(&self) -> Result<(), SimulationError> {
        let store = self.store();

        for idx in 0..self.block_count {
            let expected = self.oracle.read(idx);
            let actual = store
                .read_block(BlockIndex(idx))
                .await
                .map_err(|e| SimulationError::Unexpected(e.to_string()))?;

            if actual.as_ref() != expected.as_slice() {
                return Err(SimulationError::Mismatch {
                    context: format!(
                        "block {} mismatch: expected {:02x?}..., got {:02x?}...",
                        idx,
                        &expected[..8.min(expected.len())],
                        &actual[..8.min(actual.len())]
                    ),
                });
            }
        }

        Ok(())
    }

    fn sample_action(&mut self) -> BlockStoreAction {
        let choice = self.rng.random_range(0..100);
        match choice {
            0..50 => BlockStoreAction::Read {
                index: self.rng.random_range(0..self.block_count),
            },
            50..85 => BlockStoreAction::Write {
                index: self.rng.random_range(0..self.block_count),
                data: vec![self.rng.random(); BLOCK_SIZE],
            },
            85..90 => {
                self.snapshot_counter += 1;
                BlockStoreAction::SnapshotCreate {
                    name: format!("snap_{}", self.snapshot_counter),
                }
            }
            90..93 => {
                let names = self.oracle.snapshot_names();
                if names.is_empty() {
                    BlockStoreAction::Flush
                } else {
                    let idx = self.rng.random_range(0..names.len());
                    BlockStoreAction::SnapshotRestore {
                        name: names[idx].clone(),
                    }
                }
            }
            93..96 => {
                let names = self.oracle.snapshot_names();
                if names.is_empty() {
                    BlockStoreAction::Flush
                } else {
                    let idx = self.rng.random_range(0..names.len());
                    BlockStoreAction::SnapshotDelete {
                        name: names[idx].clone(),
                    }
                }
            }
            _ => BlockStoreAction::Flush,
        }
    }

    /// Execute a single action (public for harness testing).
    pub async fn execute(&mut self, action: BlockStoreAction) -> Result<(), SimulationError> {
        match action {
            BlockStoreAction::Read { index } => {
                self.stats.reads += 1;
                let expected = self.oracle.read(index);
                let actual = self
                    .store()
                    .read_block(BlockIndex(index))
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
                if actual.as_ref() != expected.as_slice() {
                    return Err(SimulationError::Mismatch {
                        context: format!("read({})", index),
                    });
                }
            }
            BlockStoreAction::Write { index, data } => {
                self.stats.writes += 1;
                self.oracle.write(index, data.clone());
                self.store()
                    .write_block(BlockIndex(index), Bytes::from(data))
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
            BlockStoreAction::SnapshotCreate { name } => {
                self.stats.snapshots_created += 1;
                self.oracle.snapshot_create(&name);
                self.store()
                    .snapshot_create(&name)
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
            BlockStoreAction::SnapshotDelete { name } => {
                let oracle_ok = self.oracle.snapshot_delete(&name);
                let store_result = self.store().snapshot_delete(&name).await;
                match (oracle_ok, store_result) {
                    (true, Ok(())) | (false, Err(_)) => {}
                    _ => {
                        return Err(SimulationError::Mismatch {
                            context: format!("snapshot_delete({})", name),
                        });
                    }
                }
            }
            BlockStoreAction::SnapshotRestore { name } => {
                let oracle_ok = self.oracle.snapshot_restore(&name);
                let store_result = self.store_mut().snapshot_restore(&name).await;
                match (oracle_ok, store_result) {
                    (true, Ok(())) => {
                        self.validate_all_blocks().await?;
                    }
                    (false, Err(_)) => {}
                    (true, Err(e)) => {
                        return Err(SimulationError::Mismatch {
                            context: format!("snapshot_restore({}) failed: {}", name, e),
                        });
                    }
                    (false, Ok(())) => {
                        return Err(SimulationError::Mismatch {
                            context: format!(
                                "snapshot_restore({}) succeeded but oracle expected failure",
                                name
                            ),
                        });
                    }
                }
            }
            BlockStoreAction::Flush => {
                self.store()
                    .flush()
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> &BlockStoreStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_read_unwritten_returns_zeros() {
        let oracle = BlockStoreOracle::new();
        let data = oracle.read(0);
        assert_eq!(data.len(), BLOCK_SIZE);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn oracle_write_read_roundtrip() {
        let mut oracle = BlockStoreOracle::new();
        let data = vec![0xAB; BLOCK_SIZE];
        oracle.write(5, data.clone());

        assert_eq!(oracle.read(5), data);
        // Other blocks still zero
        assert!(oracle.read(0).iter().all(|&b| b == 0));
        assert!(oracle.read(6).iter().all(|&b| b == 0));
    }

    #[test]
    fn oracle_snapshot_create_and_restore() {
        let mut oracle = BlockStoreOracle::new();

        // Write initial data
        oracle.write(0, vec![1; BLOCK_SIZE]);
        oracle.snapshot_create("snap1");

        // Modify after snapshot
        oracle.write(0, vec![2; BLOCK_SIZE]);
        assert_eq!(oracle.read(0), vec![2; BLOCK_SIZE]);

        // Restore should revert
        assert!(oracle.snapshot_restore("snap1"));
        assert_eq!(oracle.read(0), vec![1; BLOCK_SIZE]);
    }

    #[test]
    fn oracle_snapshot_restore_deletes_later_snapshots() {
        let mut oracle = BlockStoreOracle::new();

        oracle.write(0, vec![1; BLOCK_SIZE]);
        oracle.snapshot_create("snap1");

        oracle.write(0, vec![2; BLOCK_SIZE]);
        oracle.snapshot_create("snap2");

        oracle.write(0, vec![3; BLOCK_SIZE]);
        oracle.snapshot_create("snap3");

        assert_eq!(oracle.snapshot_names().len(), 3);

        // Restore to snap1 should delete snap2 and snap3
        assert!(oracle.snapshot_restore("snap1"));
        let names = oracle.snapshot_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"snap1".to_string()));
    }

    #[test]
    fn oracle_snapshot_delete() {
        let mut oracle = BlockStoreOracle::new();

        oracle.snapshot_create("snap1");
        oracle.snapshot_create("snap2");
        assert_eq!(oracle.snapshot_names().len(), 2);

        assert!(oracle.snapshot_delete("snap1"));
        assert_eq!(oracle.snapshot_names().len(), 1);

        // Deleting non-existent returns false
        assert!(!oracle.snapshot_delete("snap1"));
        assert!(!oracle.snapshot_delete("nonexistent"));
    }

    #[test]
    fn oracle_snapshot_restore_nonexistent_returns_false() {
        let mut oracle = BlockStoreOracle::new();
        assert!(!oracle.snapshot_restore("nonexistent"));
    }
}
