//! Simulation runners and utilities.
//!
//! This module provides helper functions to run deterministic simulations.
//!
//! ## Usage
//!
//! Requires tokio_unstable for deterministic RNG seeding:
//! ```bash
//! RUSTFLAGS="--cfg tokio_unstable" cargo test -p portalbd-dst
//! ```

use tokio::runtime::RngSeed;

use crate::harness::{BlockStoreHarness, NbdHarness, SimulationError};

/// Build a single-threaded tokio runtime for simulation.
///
/// Uses tokio's unstable `rng_seed` to make async scheduling deterministic,
/// enabling reproduction of failures with the same seed.
pub fn build_runtime(seed: u64) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build()
        .expect("failed to build runtime")
}

/// Run a BlockStore simulation with the given seed and number of operations.
pub async fn run_block_store_simulation(
    seed: u64,
    block_count: u64,
    operations: u64,
) -> Result<(), SimulationError> {
    eprintln!(
        "BlockStore simulation: seed={}, blocks={}, ops={}",
        seed, block_count, operations
    );

    let mut harness = BlockStoreHarness::new(seed, block_count);
    harness
        .init()
        .await
        .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
    harness.run(operations).await?;

    let stats = harness.stats();
    eprintln!("BlockStore simulation complete: {:?}", stats);
    Ok(())
}

/// Run an NBD simulation with the given seed and number of operations.
pub async fn run_nbd_simulation(
    seed: u64,
    size_bytes: u64,
    operations: u64,
) -> Result<(), SimulationError> {
    eprintln!(
        "NBD simulation: seed={}, size={}, ops={}",
        seed, size_bytes, operations
    );

    let mut harness = NbdHarness::new(seed, size_bytes);
    harness
        .init()
        .await
        .map_err(|e| SimulationError::Unexpected(e.to_string()))?;
    harness.run(operations).await?;

    let stats = harness.stats();
    eprintln!("NBD simulation complete: {:?}", stats);
    Ok(())
}

/// Get the seed from environment or generate a random one.
pub fn get_seed() -> u64 {
    std::env::var("DST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random)
}

#[cfg(test)]
mod tests {
    use super::*;
    use portalbd::BLOCK_SIZE;

    /// Quick BlockStore simulation (PR-level).
    #[test]
    fn block_store_simulation() {
        let seed = get_seed();
        let runtime = build_runtime(seed);
        runtime.block_on(async {
            run_block_store_simulation(seed, 10, 100).await.unwrap();
        });
    }

    /// Quick NBD simulation (PR-level).
    #[test]
    fn nbd_simulation() {
        let seed = get_seed();
        let runtime = build_runtime(seed);
        runtime.block_on(async {
            run_nbd_simulation(seed, BLOCK_SIZE as u64 * 10, 100)
                .await
                .unwrap();
        });
    }

    /// Long-running BlockStore simulation (nightly).
    #[test]
    #[ignore]
    fn block_store_simulation_long() {
        let seed = get_seed();
        eprintln!("DST_SEED={}", seed);
        let runtime = build_runtime(seed);
        runtime.block_on(async {
            run_block_store_simulation(seed, 100, 10_000).await.unwrap();
        });
    }

    /// Long-running NBD simulation (nightly).
    #[test]
    #[ignore]
    fn nbd_simulation_long() {
        let seed = get_seed();
        eprintln!("DST_SEED={}", seed);
        let runtime = build_runtime(seed);
        runtime.block_on(async {
            run_nbd_simulation(seed, BLOCK_SIZE as u64 * 100, 10_000)
                .await
                .unwrap();
        });
    }
}
