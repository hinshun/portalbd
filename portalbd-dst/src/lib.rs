//! Deterministic Simulation Testing (DST) for portalbd.
//!
//! This crate provides testing utilities that are intentionally separate from
//! the main portalbd crate to ensure mock implementations are never compiled
//! into production binaries.
//!
//! ## Crate Structure
//!
//! - `harness` - Simulation harnesses and oracles for correctness verification
//! - `simulation` - Deterministic runtime and simulation runners
//!
//! ## Running DST Tests
//!
//! Run from the `portalbd-dst` directory to pick up the `tokio_unstable` config:
//!
//! ```bash
//! cd portalbd-dst
//!
//! # PR-level quick tests (100 iterations)
//! cargo test simulation
//!
//! # Nightly long-running tests
//! cargo test simulation -- --ignored
//! ```

pub mod harness;
pub mod simulation;

// Re-export NBD client from nbd crate
pub use nbd::{NbdClient, NbdError};

/// Backward compatibility alias for NbdClientError.
pub type NbdClientError = NbdError;

pub use harness::{
    BlockStoreAction, BlockStoreHarness, BlockStoreOracle, BlockStoreStats, NbdAction, NbdHarness,
    NbdOracle, NbdStats, SimulationError,
};
pub use simulation::{build_runtime, get_seed, run_block_store_simulation, run_nbd_simulation};
