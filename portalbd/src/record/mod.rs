//! Operation recording for portalbd.
//!
//! This module provides middleware for recording NBD operations to a trace file.
//! When recording is enabled via config, operations are captured during execution
//! and saved to disk on shutdown.

mod middleware;
mod recorder;
mod types;

pub use middleware::RecordingHandler;
pub use recorder::{OpRecorder, load_trace, save_trace};
pub use types::{NbdOp, OpCounts, OpTrace, TraceStats, TracedOp};
