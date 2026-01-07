//! Recording middleware for NBD operations.
//!
//! This module provides a `RecordingHandler` that wraps any `TransmissionHandler`
//! and records all operations for later analysis.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::nbd::{HandlerResult, TransmissionHandler};
use crate::record::recorder::OpRecorder;
use crate::record::types::NbdOp;

/// Recording middleware that wraps a handler and records all operations.
///
/// # Example
///
/// ```ignore
/// let clock = Arc::new(DefaultSystemClock::new());
/// let core = BlockStoreHandler::new(store);
/// let recorder = OpRecorder::new("mkfs", size_bytes, clock);
/// let handler = RecordingHandler::new(core, recorder);
///
/// let server = NbdServer::new(Arc::new(handler));
/// ```
pub struct RecordingHandler<H> {
    inner: H,
    recorder: Arc<OpRecorder>,
}

impl<H> RecordingHandler<H> {
    /// Create a new recording handler wrapping the inner handler.
    pub fn new(inner: H, recorder: Arc<OpRecorder>) -> Self {
        Self { inner, recorder }
    }

    /// Get a reference to the recorder.
    pub fn recorder(&self) -> &Arc<OpRecorder> {
        &self.recorder
    }
}

#[async_trait]
impl<H: TransmissionHandler> TransmissionHandler for RecordingHandler<H> {
    async fn read(&self, offset: u64, length: usize) -> HandlerResult<Bytes> {
        let start = self.recorder.clock().now();
        let result = self.inner.read(offset, length).await;
        let end = self.recorder.clock().now();
        let duration_ns = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(0) as u64;

        self.recorder.record_op(
            NbdOp::Read {
                offset,
                length: length as u32,
            },
            duration_ns,
            result.is_ok(),
        );

        result
    }

    async fn write(&self, offset: u64, data: Bytes) -> HandlerResult<()> {
        let start = self.recorder.clock().now();
        let result = self.inner.write(offset, data.clone()).await;
        let end = self.recorder.clock().now();
        let duration_ns = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(0) as u64;

        self.recorder.record_op(
            NbdOp::Write {
                offset,
                length: data.len() as u32,
            },
            duration_ns,
            result.is_ok(),
        );

        result
    }

    async fn trim(&self, offset: u64, length: u64) -> HandlerResult<()> {
        let start = self.recorder.clock().now();
        let result = self.inner.trim(offset, length).await;
        let end = self.recorder.clock().now();
        let duration_ns = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(0) as u64;

        self.recorder
            .record_op(NbdOp::Trim { offset, length }, duration_ns, result.is_ok());

        result
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> HandlerResult<()> {
        let start = self.recorder.clock().now();
        let result = self.inner.write_zeroes(offset, length).await;
        let end = self.recorder.clock().now();
        let duration_ns = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(0) as u64;

        self.recorder.record_op(
            NbdOp::WriteZeroes { offset, length },
            duration_ns,
            result.is_ok(),
        );

        result
    }

    async fn flush(&self) -> HandlerResult<()> {
        let start = self.recorder.clock().now();
        let result = self.inner.flush().await;
        let end = self.recorder.clock().now();
        let duration_ns = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(0) as u64;

        self.recorder
            .record_op(NbdOp::Flush, duration_ns, result.is_ok());

        result
    }
}
