//! Simulation harnesses for portalbd.
//!
//! Harnesses provide the infrastructure for running deterministic simulations:
//! - Oracle implementations for correctness verification
//! - Action generators for random operation sequences
//! - Statistics tracking

pub mod block_store;
pub mod nbd;

pub use block_store::{BlockStoreAction, BlockStoreHarness, BlockStoreOracle, BlockStoreStats};
pub use nbd::{NbdAction, NbdHarness, NbdOracle, NbdStats};

/// Error type for simulation failures.
#[derive(Debug)]
pub enum SimulationError {
    Mismatch { context: String },
    Unexpected(String),
}

impl std::fmt::Display for SimulationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mismatch { context } => write!(f, "oracle mismatch: {}", context),
            Self::Unexpected(msg) => write!(f, "unexpected error: {}", msg),
        }
    }
}

impl std::error::Error for SimulationError {}
