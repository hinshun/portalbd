//! portalbd: NBD block device with SlateDB checkpoint-based snapshots.
//!
//! One portalbd instance per block device. Snapshots use SlateDB checkpoints
//! for O(1) create and restore operations with copy-on-write semantics.
//!
//! # Library Usage
//!
//! The primary API for running portalbd is the [`Daemon`] struct:
//!
//! ```ignore
//! use portalbd::{Config, Daemon};
//!
//! let config = Config::default();
//! let daemon = Daemon::from_config(config).await?;
//! daemon.start_nbd_server();
//! ```

pub mod config;
pub mod control;
pub mod daemon;
pub mod error;
pub mod nbd;
pub mod record;
pub mod store;
pub mod types;

pub use config::{Config, DeviceConfig, LsmConfig, NbdConfig, RecordConfig, StorageConfig};
pub use daemon::Daemon;
pub use error::{ConfigError, Error, NbdError, Result, StoreError};
pub use nbd::{BlockStoreHandler, HandlerResult, NbdExport, NbdServer, TransmissionHandler};
pub use record::{
    NbdOp, OpCounts, OpRecorder, OpTrace, RecordingHandler, TraceStats, TracedOp, load_trace,
    save_trace,
};
pub use store::BlockStore;
pub use types::{BLOCK_SIZE, BlockIndex, IoLength, SnapshotInfo, zero_block};

pub use object_store;
pub use slatedb;
