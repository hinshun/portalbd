//! NBD protocol-level simulation harness.
//!
//! Tests byte-level operations through the NBD protocol.

use std::collections::HashMap;

use portalbd::config::{Config, DeviceConfig};
use portalbd::daemon::Daemon;
use portalbd::nbd::StreamListener;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::io::{DuplexStream, duplex};

use nbd::NbdClient;

use super::SimulationError;

/// Actions that can be performed in NBD simulation.
#[derive(Debug, Clone)]
pub enum NbdAction {
    Read { offset: u64, length: u32 },
    Write { offset: u64, data: Vec<u8> },
    Trim { offset: u64, length: u32 },
    WriteZeroes { offset: u64, length: u32 },
    Flush,
}

/// Byte-level oracle for NBD verification.
pub struct NbdOracle {
    data: HashMap<u64, u8>,
}

impl NbdOracle {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn read(&self, offset: u64, length: u32) -> Vec<u8> {
        let mut result = vec![0u8; length as usize];
        for (i, byte) in result.iter_mut().enumerate() {
            if let Some(&b) = self.data.get(&(offset + i as u64)) {
                *byte = b;
            }
        }
        result
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) {
        for (i, &byte) in data.iter().enumerate() {
            let pos = offset + i as u64;
            if byte == 0 {
                self.data.remove(&pos);
            } else {
                self.data.insert(pos, byte);
            }
        }
    }

    pub fn trim(&mut self, offset: u64, length: u32) {
        for pos in offset..offset + length as u64 {
            self.data.remove(&pos);
        }
    }

    pub fn write_zeroes(&mut self, offset: u64, length: u32) {
        self.trim(offset, length);
    }
}

impl Default for NbdOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulation statistics.
#[derive(Debug, Default, Clone)]
pub struct NbdStats {
    pub operations: u64,
    pub reads: u64,
    pub writes: u64,
    pub trims: u64,
    pub write_zeroes: u64,
    pub flushes: u64,
}

/// NBD protocol-level simulation harness.
pub struct NbdHarness {
    rng: SmallRng,
    client: Option<NbdClient<DuplexStream>>,
    oracle: NbdOracle,
    size_bytes: u64,
    stats: NbdStats,
    // Keep listen task handle to ensure it stays alive
    _listen_handle: Option<tokio::task::JoinHandle<()>>,
}

impl NbdHarness {
    pub fn new(seed: u64, size_bytes: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            client: None,
            oracle: NbdOracle::new(),
            size_bytes,
            stats: NbdStats::default(),
            _listen_handle: None,
        }
    }

    pub async fn init(&mut self) -> Result<(), portalbd::StoreError> {
        // Create daemon with in-memory storage
        let config = Config {
            device: DeviceConfig {
                disk_size_gb: (self.size_bytes + 1024 * 1024 * 1024 - 1) / (1024 * 1024 * 1024),
            },
            ..Default::default()
        };
        let daemon = Daemon::from_config(config)
            .await
            .map_err(|e| portalbd::StoreError::backend(e))?;

        // Create StreamListener for in-memory connection
        let (stream_tx, listener) = StreamListener::new(1);

        // Spawn daemon listening task
        let handle = tokio::spawn({
            let daemon = daemon.clone();
            async move {
                let _ = daemon.listen(listener).await;
            }
        });
        self._listen_handle = Some(handle);

        // Create in-memory duplex channel
        let (client_stream, server_stream) = duplex(1024 * 1024);

        // Send server stream to listener
        stream_tx
            .send(server_stream)
            .await
            .map_err(|_| portalbd::StoreError::backend("channel closed"))?;
        drop(stream_tx);

        // Connect client
        let client = NbdClient::connect(client_stream, "portalbd")
            .await
            .map_err(|e| portalbd::StoreError::backend(e))?;

        self.client = Some(client);
        Ok(())
    }

    fn client(&mut self) -> &mut NbdClient<DuplexStream> {
        self.client.as_mut().expect("not initialized")
    }

    /// Run the simulation for a given number of operations.
    pub async fn run(&mut self, operations: u64) -> Result<(), SimulationError> {
        for _ in 0..operations {
            let action = self.sample_action();
            self.execute(action).await?;
            self.stats.operations += 1;
        }
        Ok(())
    }

    fn sample_action(&mut self) -> NbdAction {
        let choice = self.rng.random_range(0..100);
        match choice {
            0..40 => {
                let max_len = (32 * 1024).min(self.size_bytes as u32);
                let length = self.rng.random_range(1..=max_len);
                let max_offset = self.size_bytes.saturating_sub(length as u64);
                let offset = if max_offset > 0 {
                    self.rng.random_range(0..max_offset)
                } else {
                    0
                };
                NbdAction::Read { offset, length }
            }
            40..75 => {
                let max_len = (32 * 1024).min(self.size_bytes as u32);
                let length = self.rng.random_range(1..=max_len);
                let max_offset = self.size_bytes.saturating_sub(length as u64);
                let offset = if max_offset > 0 {
                    self.rng.random_range(0..max_offset)
                } else {
                    0
                };
                let data: Vec<u8> = (0..length).map(|_| self.rng.random()).collect();
                NbdAction::Write { offset, data }
            }
            75..80 => {
                let max_len = (64 * 1024).min(self.size_bytes);
                let length = self.rng.random_range(1..=max_len) as u32;
                let max_offset = self.size_bytes.saturating_sub(length as u64);
                let offset = if max_offset > 0 {
                    self.rng.random_range(0..max_offset)
                } else {
                    0
                };
                NbdAction::Trim { offset, length }
            }
            80..85 => {
                let max_len = (64 * 1024).min(self.size_bytes);
                let length = self.rng.random_range(1..=max_len) as u32;
                let max_offset = self.size_bytes.saturating_sub(length as u64);
                let offset = if max_offset > 0 {
                    self.rng.random_range(0..max_offset)
                } else {
                    0
                };
                NbdAction::WriteZeroes { offset, length }
            }
            _ => NbdAction::Flush,
        }
    }

    /// Execute a single action.
    pub async fn execute(&mut self, action: NbdAction) -> Result<(), SimulationError> {
        match action {
            NbdAction::Read { offset, length } => {
                self.stats.reads += 1;
                let expected = self.oracle.read(offset, length);
                let actual = self
                    .client()
                    .read(offset, length)
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;

                if actual.as_ref() != expected.as_slice() {
                    return Err(SimulationError::Mismatch {
                        context: format!(
                            "read(offset={}, len={}): first diff at byte {}",
                            offset,
                            length,
                            find_diff(&expected, &actual)
                        ),
                    });
                }
            }
            NbdAction::Write { offset, data } => {
                self.stats.writes += 1;
                self.oracle.write(offset, &data);
                self.client()
                    .write(offset, &data)
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
            NbdAction::Trim { offset, length } => {
                self.stats.trims += 1;
                self.oracle.trim(offset, length);
                // NBD trim is a hint; we use write_zeroes for actual zeroing
                self.client()
                    .write_zeroes(offset, length)
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
            NbdAction::WriteZeroes { offset, length } => {
                self.stats.write_zeroes += 1;
                self.oracle.write_zeroes(offset, length);
                self.client()
                    .write_zeroes(offset, length)
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
            NbdAction::Flush => {
                self.stats.flushes += 1;
                self.client()
                    .flush()
                    .await
                    .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> &NbdStats {
        &self.stats
    }
}

fn find_diff(expected: &[u8], actual: &[u8]) -> usize {
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        if e != a {
            return i;
        }
    }
    expected.len().min(actual.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_read_unwritten_returns_zeros() {
        let oracle = NbdOracle::new();
        let data = oracle.read(0, 100);
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn oracle_write_read_roundtrip() {
        let mut oracle = NbdOracle::new();
        let data = vec![0xAB, 0xCD, 0xEF, 0x12];
        oracle.write(10, &data);

        assert_eq!(oracle.read(10, 4), data);
        // Adjacent bytes still zero
        assert_eq!(oracle.read(9, 1), vec![0]);
        assert_eq!(oracle.read(14, 1), vec![0]);
    }

    #[test]
    fn oracle_write_zeros_removes_from_map() {
        let mut oracle = NbdOracle::new();

        // Write non-zero data
        oracle.write(0, &[1, 2, 3, 4]);
        assert_eq!(oracle.read(0, 4), vec![1, 2, 3, 4]);

        // Write zeros should remove entries
        oracle.write(1, &[0, 0]);
        assert_eq!(oracle.read(0, 4), vec![1, 0, 0, 4]);
    }

    #[test]
    fn oracle_partial_read_spanning_written_and_unwritten() {
        let mut oracle = NbdOracle::new();

        // Write at offset 5
        oracle.write(5, &[0xAA, 0xBB, 0xCC]);

        // Read spanning before, during, and after
        let data = oracle.read(3, 8);
        assert_eq!(data, vec![0, 0, 0xAA, 0xBB, 0xCC, 0, 0, 0]);
    }

    #[test]
    fn oracle_trim_removes_bytes() {
        let mut oracle = NbdOracle::new();

        oracle.write(0, &[1, 2, 3, 4, 5]);
        oracle.trim(1, 3);

        assert_eq!(oracle.read(0, 5), vec![1, 0, 0, 0, 5]);
    }

    #[test]
    fn oracle_write_zeroes_same_as_trim() {
        let mut oracle = NbdOracle::new();

        oracle.write(0, &[1, 2, 3, 4, 5]);
        oracle.write_zeroes(1, 3);

        assert_eq!(oracle.read(0, 5), vec![1, 0, 0, 0, 5]);
    }
}
