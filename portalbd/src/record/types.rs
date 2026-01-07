//! Core types for operation tracing.
//!
//! These types capture NBD operations at the protocol level for recording,
//! replay, and benchmark derivation.

use serde::{Deserialize, Serialize};
use slatedb::clock::SystemClock;

/// Maximum operations in a single trace to bound memory.
pub const TRACE_OPS_MAX: usize = 10_000_000;

/// Recorded NBD operation with timing metadata.
///
/// Each operation captures the request, when it occurred, and how long it took.
/// This enables both functional replay and performance analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedOp {
    /// Monotonic sequence number (0-indexed).
    pub seq: u64,

    /// Nanoseconds since trace start when operation began.
    pub timestamp_ns: u64,

    /// The NBD operation.
    pub op: NbdOp,

    /// Duration to complete the operation in nanoseconds.
    pub duration_ns: u64,

    /// Whether the operation succeeded.
    pub success: bool,
}

/// NBD operation variants.
///
/// Operations only store offset and length - no actual data content.
/// This keeps traces compact; during replay, deterministic data is generated.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NbdOp {
    /// Read data from device.
    Read {
        /// Byte offset in device.
        offset: u64,
        /// Number of bytes to read.
        length: u32,
    },

    /// Write data to device.
    Write {
        /// Byte offset in device.
        offset: u64,
        /// Number of bytes written.
        length: u32,
    },

    /// Trim (discard) a range.
    Trim {
        /// Byte offset in device.
        offset: u64,
        /// Number of bytes to trim.
        length: u64,
    },

    /// Write zeroes to a range.
    WriteZeroes {
        /// Byte offset in device.
        offset: u64,
        /// Number of bytes to zero.
        length: u64,
    },

    /// Flush pending writes to stable storage.
    Flush,
}

impl NbdOp {
    /// Returns the byte offset for operations that have one.
    pub fn offset(&self) -> Option<u64> {
        match self {
            NbdOp::Read { offset, .. }
            | NbdOp::Write { offset, .. }
            | NbdOp::Trim { offset, .. }
            | NbdOp::WriteZeroes { offset, .. } => Some(*offset),
            NbdOp::Flush => None,
        }
    }

    /// Returns the byte length for operations that have one.
    pub fn length(&self) -> Option<u64> {
        match self {
            NbdOp::Read { length, .. } | NbdOp::Write { length, .. } => Some(*length as u64),
            NbdOp::Trim { length, .. } | NbdOp::WriteZeroes { length, .. } => Some(*length),
            NbdOp::Flush => None,
        }
    }

    /// Returns true if this is a write operation (modifies data).
    pub fn is_write(&self) -> bool {
        matches!(self, NbdOp::Write { .. } | NbdOp::WriteZeroes { .. })
    }

    /// Returns true if this is a read operation.
    pub fn is_read(&self) -> bool {
        matches!(self, NbdOp::Read { .. })
    }
}

/// Complete trace from a workload run.
///
/// Contains metadata about the recording session and all captured operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpTrace {
    /// Human-readable workload name (e.g., "mkfs.ext4", "pgbench").
    pub workload: String,

    /// Device size in bytes.
    pub device_size_bytes: u64,

    /// ISO8601 timestamp when recording started.
    pub start_time: String,

    /// Total recording duration in nanoseconds.
    pub duration_ns: u64,

    /// Recorded operations in order.
    pub ops: Vec<TracedOp>,
}

impl OpTrace {
    /// Create a new empty trace.
    pub fn new(
        workload: impl Into<String>,
        device_size_bytes: u64,
        clock: &dyn SystemClock,
    ) -> Self {
        Self {
            workload: workload.into(),
            device_size_bytes,
            start_time: clock.now().to_rfc3339(),
            duration_ns: 0,
            ops: Vec::new(),
        }
    }

    /// Total bytes read across all operations.
    pub fn total_bytes_read(&self) -> u64 {
        self.ops
            .iter()
            .filter_map(|op| {
                if let NbdOp::Read { length, .. } = &op.op {
                    Some(*length as u64)
                } else {
                    None
                }
            })
            .sum()
    }

    /// Total bytes written across all operations.
    pub fn total_bytes_written(&self) -> u64 {
        self.ops
            .iter()
            .filter_map(|op| match &op.op {
                NbdOp::Write { length, .. } => Some(*length as u64),
                NbdOp::WriteZeroes { length, .. } => Some(*length),
                _ => None,
            })
            .sum()
    }

    /// Count operations by type.
    pub fn op_counts(&self) -> OpCounts {
        let mut counts = OpCounts::default();
        for traced in &self.ops {
            match &traced.op {
                NbdOp::Read { .. } => counts.reads += 1,
                NbdOp::Write { .. } => counts.writes += 1,
                NbdOp::Trim { .. } => counts.trims += 1,
                NbdOp::WriteZeroes { .. } => counts.write_zeroes += 1,
                NbdOp::Flush => counts.flushes += 1,
            }
        }
        counts
    }

    /// Get comprehensive statistics about this trace.
    pub fn stats(&self) -> TraceStats {
        TraceStats {
            op_count: self.ops.len() as u64,
            bytes_read: self.total_bytes_read(),
            bytes_written: self.total_bytes_written(),
            bytes_total: self.total_bytes_read() + self.total_bytes_written(),
            counts: self.op_counts(),
        }
    }
}

/// Comprehensive statistics for a trace.
#[derive(Debug, Clone, Default)]
pub struct TraceStats {
    /// Total number of operations.
    pub op_count: u64,

    /// Total bytes read.
    pub bytes_read: u64,

    /// Total bytes written.
    pub bytes_written: u64,

    /// Total bytes (read + written).
    pub bytes_total: u64,

    /// Operation counts by type.
    pub counts: OpCounts,
}

/// Operation counts by type.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpCounts {
    pub reads: u64,
    pub writes: u64,
    pub trims: u64,
    pub write_zeroes: u64,
    pub flushes: u64,
}

impl OpCounts {
    pub fn total(&self) -> u64 {
        self.reads + self.writes + self.trims + self.write_zeroes + self.flushes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::clock::DefaultSystemClock;

    fn test_clock() -> DefaultSystemClock {
        DefaultSystemClock::new()
    }

    #[test]
    fn nbd_op_accessors() {
        let read = NbdOp::Read {
            offset: 100,
            length: 512,
        };
        assert_eq!(read.offset(), Some(100));
        assert_eq!(read.length(), Some(512));
        assert!(read.is_read());
        assert!(!read.is_write());

        let write = NbdOp::Write {
            offset: 200,
            length: 1024,
        };
        assert_eq!(write.offset(), Some(200));
        assert_eq!(write.length(), Some(1024));
        assert!(!write.is_read());
        assert!(write.is_write());

        let flush = NbdOp::Flush;
        assert_eq!(flush.offset(), None);
        assert_eq!(flush.length(), None);
    }

    #[test]
    fn trace_statistics() {
        let clock = test_clock();
        let mut trace = OpTrace::new("test", 1024 * 1024, &clock);
        trace.ops.push(TracedOp {
            seq: 0,
            timestamp_ns: 0,
            op: NbdOp::Read {
                offset: 0,
                length: 512,
            },
            duration_ns: 100,
            success: true,
        });
        trace.ops.push(TracedOp {
            seq: 1,
            timestamp_ns: 100,
            op: NbdOp::Write {
                offset: 512,
                length: 1024,
            },
            duration_ns: 200,
            success: true,
        });
        trace.ops.push(TracedOp {
            seq: 2,
            timestamp_ns: 300,
            op: NbdOp::Flush,
            duration_ns: 50,
            success: true,
        });

        assert_eq!(trace.total_bytes_read(), 512);
        assert_eq!(trace.total_bytes_written(), 1024);

        let counts = trace.op_counts();
        assert_eq!(counts.reads, 1);
        assert_eq!(counts.writes, 1);
        assert_eq!(counts.flushes, 1);
        assert_eq!(counts.total(), 3);
    }

    #[test]
    fn trace_serialization_roundtrip() {
        let clock = test_clock();
        let mut trace = OpTrace::new("test", 1024, &clock);
        trace.ops.push(TracedOp {
            seq: 0,
            timestamp_ns: 0,
            op: NbdOp::Write {
                offset: 0,
                length: 4,
            },
            duration_ns: 100,
            success: true,
        });

        let json = serde_json::to_string(&trace).unwrap();
        let restored: OpTrace = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.workload, "test");
        assert_eq!(restored.ops.len(), 1);
        if let NbdOp::Write { length, .. } = &restored.ops[0].op {
            assert_eq!(*length, 4);
        } else {
            panic!("Expected Write");
        }
    }
}
