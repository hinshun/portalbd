//! Operation recorder for capturing NBD operations.
//!
//! The recorder captures operations at the NBD protocol level, storing them
//! for later replay or analysis. It's designed to be thread-safe and efficient,
//! minimizing overhead during recording.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use slatedb::clock::SystemClock;
use tracing::{debug, info, warn};

use crate::record::types::{NbdOp, OpTrace, TRACE_OPS_MAX, TracedOp};

/// Operation recorder that captures NBD operations.
///
/// Thread-safe and designed for minimal overhead. Operations are buffered
/// in memory and can be flushed to disk.
pub struct OpRecorder {
    /// Workload name for the trace.
    workload: String,

    /// Device size in bytes.
    device_size_bytes: u64,

    /// System clock for timestamps.
    clock: Arc<dyn SystemClock>,

    /// Recording start time.
    start: DateTime<Utc>,

    /// Next sequence number.
    seq: AtomicU64,

    /// Recorded operations (bounded by TRACE_OPS_MAX).
    ops: Mutex<Vec<TracedOp>>,

    /// Whether recording has been stopped.
    stopped: AtomicBool,
}

impl OpRecorder {
    /// Create a new recorder.
    pub fn new(
        workload: impl Into<String>,
        device_size_bytes: u64,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        let start = clock.now();
        Self {
            workload: workload.into(),
            device_size_bytes,
            clock,
            start,
            seq: AtomicU64::new(0),
            ops: Mutex::new(Vec::with_capacity(10_000)),
            stopped: AtomicBool::new(false),
        }
    }

    /// Check if recording is active.
    pub fn is_active(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    /// Get the clock used by this recorder.
    pub fn clock(&self) -> &Arc<dyn SystemClock> {
        &self.clock
    }

    /// Record an operation.
    pub fn record_op(&self, op: NbdOp, duration_ns: u64, success: bool) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let now = self.clock.now();
        let elapsed = now.signed_duration_since(self.start);
        let timestamp_ns = elapsed.num_nanoseconds().unwrap_or(0) as u64;

        let traced = TracedOp {
            seq,
            timestamp_ns,
            op,
            duration_ns,
            success,
        };

        let mut ops = self.ops.lock();

        // Bound memory usage
        if ops.len() >= TRACE_OPS_MAX {
            if ops.len() == TRACE_OPS_MAX {
                warn!(
                    "Trace operation limit reached ({}), dropping new operations",
                    TRACE_OPS_MAX
                );
            }
            return;
        }

        ops.push(traced);
    }

    /// Stop recording and return the trace.
    pub fn stop(&self) -> OpTrace {
        self.stopped.store(true, Ordering::Relaxed);

        let now = self.clock.now();
        let elapsed = now.signed_duration_since(self.start);
        let duration_ns = elapsed.num_nanoseconds().unwrap_or(0) as u64;
        let ops = std::mem::take(&mut *self.ops.lock());

        info!(
            workload = %self.workload,
            ops = ops.len(),
            duration_ms = duration_ns / 1_000_000,
            "Recording stopped"
        );

        OpTrace {
            workload: self.workload.clone(),
            device_size_bytes: self.device_size_bytes,
            start_time: self.start.to_rfc3339(),
            duration_ns,
            ops,
        }
    }

    /// Get current operation count.
    pub fn op_count(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Save trace to a JSON file.
    pub fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let trace = self.stop();
        save_trace(&trace, path)
    }
}

/// Save a trace to a JSON file.
pub fn save_trace(trace: &OpTrace, path: &Path) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    serde_json::to_writer_pretty(&mut writer, trace)?;
    writer.flush()?;

    debug!(
        path = %path.display(),
        ops = trace.ops.len(),
        "Trace saved"
    );

    Ok(())
}

/// Load a trace from a JSON file.
pub fn load_trace(path: &Path) -> std::io::Result<OpTrace> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let trace: OpTrace = serde_json::from_reader(reader)?;

    debug!(
        path = %path.display(),
        workload = %trace.workload,
        ops = trace.ops.len(),
        "Trace loaded"
    );

    Ok(trace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::clock::DefaultSystemClock;

    fn test_clock() -> Arc<dyn SystemClock> {
        Arc::new(DefaultSystemClock::new())
    }

    #[test]
    fn basic_recording() {
        let recorder = OpRecorder::new("test", 1024 * 1024, test_clock());

        recorder.record_op(
            NbdOp::Read {
                offset: 0,
                length: 512,
            },
            100,
            true,
        );
        recorder.record_op(
            NbdOp::Write {
                offset: 512,
                length: 4,
            },
            200,
            true,
        );
        recorder.record_op(NbdOp::Flush, 50, true);

        let trace = recorder.stop();

        assert_eq!(trace.workload, "test");
        assert_eq!(trace.ops.len(), 3);
        assert!(matches!(
            trace.ops[0].op,
            NbdOp::Read {
                offset: 0,
                length: 512
            }
        ));
        assert!(matches!(trace.ops[2].op, NbdOp::Flush));
    }

    #[test]
    fn sequence_numbers_monotonic() {
        let recorder = OpRecorder::new("test", 1024, test_clock());

        for _ in 0..100 {
            recorder.record_op(
                NbdOp::Read {
                    offset: 0,
                    length: 512,
                },
                100,
                true,
            );
        }

        let trace = recorder.stop();

        for (i, op) in trace.ops.iter().enumerate() {
            assert_eq!(op.seq, i as u64);
        }
    }

    #[test]
    fn timestamps_monotonic() {
        let recorder = OpRecorder::new("test", 1024, test_clock());

        for _ in 0..10 {
            recorder.record_op(
                NbdOp::Read {
                    offset: 0,
                    length: 512,
                },
                100,
                true,
            );
            // Small busy-wait to ensure clock advances
            std::hint::spin_loop();
        }

        let trace = recorder.stop();

        for window in trace.ops.windows(2) {
            assert!(window[1].timestamp_ns >= window[0].timestamp_ns);
        }
    }

    #[test]
    fn stop_prevents_recording() {
        let recorder = OpRecorder::new("test", 1024, test_clock());

        recorder.record_op(
            NbdOp::Read {
                offset: 0,
                length: 512,
            },
            100,
            true,
        );
        let trace = recorder.stop();

        assert_eq!(trace.ops.len(), 1);

        // Recording after stop should be ignored
        recorder.record_op(
            NbdOp::Read {
                offset: 0,
                length: 512,
            },
            100,
            true,
        );
        assert!(!recorder.is_active());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.trace.json");

        let recorder = OpRecorder::new("test", 1024, test_clock());
        recorder.record_op(
            NbdOp::Write {
                offset: 0,
                length: 4,
            },
            100,
            true,
        );
        recorder.record_op(
            NbdOp::Read {
                offset: 100,
                length: 512,
            },
            50,
            true,
        );

        let original = recorder.stop();
        save_trace(&original, &path).unwrap();

        let loaded = load_trace(&path).unwrap();

        assert_eq!(loaded.workload, original.workload);
        assert_eq!(loaded.ops.len(), original.ops.len());
        assert_eq!(loaded.device_size_bytes, original.device_size_bytes);
    }
}
