//! Trace replay for performance testing using NBD protocol.

use std::time::Instant;

use tokio::io::{DuplexStream, duplex};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::info;

use nbd::NbdClient;
use portalbd::daemon::Daemon;
use portalbd::nbd::StreamListener;
use portalbd::{NbdOp, OpTrace};

/// Statistics from a trace replay.
#[derive(Debug, Clone, Default)]
pub struct ReplayStats {
    pub ops_total: u64,
    pub ops_success: u64,
    pub ops_failed: u64,
    pub duration_ns: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl ReplayStats {
    pub fn ops_per_sec(&self) -> f64 {
        let secs = self.duration_ns as f64 / 1_000_000_000.0;
        if secs > 0.0 {
            self.ops_total as f64 / secs
        } else {
            0.0
        }
    }

    pub fn read_throughput_mbps(&self) -> f64 {
        let secs = self.duration_ns as f64 / 1_000_000_000.0;
        if secs > 0.0 {
            (self.bytes_read as f64 / (1024.0 * 1024.0)) / secs
        } else {
            0.0
        }
    }

    pub fn write_throughput_mbps(&self) -> f64 {
        let secs = self.duration_ns as f64 / 1_000_000_000.0;
        if secs > 0.0 {
            (self.bytes_written as f64 / (1024.0 * 1024.0)) / secs
        } else {
            0.0
        }
    }

    fn record_result(&mut self, ok: bool) {
        if ok {
            self.ops_success += 1;
        } else {
            self.ops_failed += 1;
        }
    }
}

/// Command sent to a connection task.
enum ReplayOp {
    Write {
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<bool>,
    },
    Read {
        offset: u64,
        length: u32,
        reply: oneshot::Sender<bool>,
    },
    Flush {
        reply: oneshot::Sender<bool>,
    },
    Trim {
        offset: u64,
        length: u32,
        reply: oneshot::Sender<bool>,
    },
    WriteZeroes {
        offset: u64,
        length: u32,
        reply: oneshot::Sender<bool>,
    },
}

/// Dedicated task for each NBD connection.
async fn connection_task(
    mut client: NbdClient<DuplexStream>,
    mut rx: mpsc::UnboundedReceiver<ReplayOp>,
) {
    while let Some(op) = rx.recv().await {
        let (ok, reply) = match op {
            ReplayOp::Write {
                offset,
                data,
                reply,
            } => (client.write(offset, &data).await.is_ok(), reply),
            ReplayOp::Read {
                offset,
                length,
                reply,
            } => (client.read(offset, length).await.is_ok(), reply),
            ReplayOp::Flush { reply } => (client.flush().await.is_ok(), reply),
            ReplayOp::Trim {
                offset,
                length,
                reply,
            } => (client.trim(offset, length).await.is_ok(), reply),
            ReplayOp::WriteZeroes {
                offset,
                length,
                reply,
            } => (client.write_zeroes(offset, length).await.is_ok(), reply),
        };
        let _ = reply.send(ok);
    }
}

/// Trace replayer using NBD protocol with channel-based dispatch.
pub struct NbdReplayer {
    trace: OpTrace,
    txs: Vec<UnboundedSender<ReplayOp>>,
    conn_handles: Vec<JoinHandle<()>>,
    _listen_handle: JoinHandle<()>,
}

impl NbdReplayer {
    /// Create a new replayer with the specified number of connections.
    pub async fn new(
        trace: OpTrace,
        daemon: &Daemon,
        num_connections: usize,
    ) -> Result<Self, nbd::NbdError> {
        // Create listener that accepts streams from channel
        let (stream_tx, listener) = StreamListener::new(num_connections);

        // Spawn daemon listening on the StreamListener
        let daemon = daemon.clone();
        let listen_handle = tokio::spawn(async move {
            let _ = daemon.listen(listener).await;
        });

        // Set up client connections
        let mut txs = Vec::with_capacity(num_connections);
        let mut conn_handles = Vec::with_capacity(num_connections);

        for _ in 0..num_connections {
            let (client_stream, server_stream) = duplex(1024 * 1024);

            // Push server stream to listener
            stream_tx
                .send(server_stream)
                .await
                .map_err(|_| nbd::NbdError::Io(std::io::Error::other("channel closed")))?;

            // Connect client
            let client = NbdClient::connect(client_stream, "portalbd").await?;

            let (tx, rx) = mpsc::unbounded_channel();
            txs.push(tx);
            conn_handles.push(tokio::spawn(connection_task(client, rx)));
        }

        // Drop sender so listener knows when all connections are set up
        drop(stream_tx);

        Ok(Self {
            trace,
            txs,
            conn_handles,
            _listen_handle: listen_handle,
        })
    }

    /// Replay all operations, dispatching across connections.
    pub async fn replay(&self) -> ReplayStats {
        let start = Instant::now();
        let mut stats = ReplayStats::default();
        let num_connections = self.txs.len();
        let mut pending: Vec<oneshot::Receiver<bool>> = Vec::new();
        let mut next_conn = 0;

        for traced in &self.trace.ops {
            match &traced.op {
                NbdOp::Read { offset, length } => {
                    drain_pending(&mut pending, &mut stats).await;

                    let (tx, rx) = oneshot::channel();
                    self.txs[0]
                        .send(ReplayOp::Read {
                            offset: *offset,
                            length: *length,
                            reply: tx,
                        })
                        .unwrap();
                    stats.ops_total += 1;
                    stats.bytes_read += *length as u64;
                    stats.record_result(rx.await.unwrap_or(false));
                }
                NbdOp::Write { offset, length } => {
                    let (tx, rx) = oneshot::channel();
                    self.txs[next_conn]
                        .send(ReplayOp::Write {
                            offset: *offset,
                            data: deterministic_data(*offset, *length as usize),
                            reply: tx,
                        })
                        .unwrap();
                    pending.push(rx);
                    stats.ops_total += 1;
                    stats.bytes_written += *length as u64;
                    next_conn = (next_conn + 1) % num_connections;
                }
                NbdOp::Trim { offset, length } => {
                    let (tx, rx) = oneshot::channel();
                    self.txs[next_conn]
                        .send(ReplayOp::Trim {
                            offset: *offset,
                            length: *length as u32,
                            reply: tx,
                        })
                        .unwrap();
                    pending.push(rx);
                    stats.ops_total += 1;
                    next_conn = (next_conn + 1) % num_connections;
                }
                NbdOp::WriteZeroes { offset, length } => {
                    let (tx, rx) = oneshot::channel();
                    self.txs[next_conn]
                        .send(ReplayOp::WriteZeroes {
                            offset: *offset,
                            length: *length as u32,
                            reply: tx,
                        })
                        .unwrap();
                    pending.push(rx);
                    stats.ops_total += 1;
                    stats.bytes_written += *length;
                    next_conn = (next_conn + 1) % num_connections;
                }
                NbdOp::Flush => {
                    drain_pending(&mut pending, &mut stats).await;

                    // Flush all connections
                    let mut flush_rxs = Vec::with_capacity(num_connections);
                    for tx in &self.txs {
                        let (reply_tx, reply_rx) = oneshot::channel();
                        tx.send(ReplayOp::Flush { reply: reply_tx }).unwrap();
                        flush_rxs.push(reply_rx);
                    }
                    let all_ok = futures::future::join_all(flush_rxs)
                        .await
                        .into_iter()
                        .all(|r| r.unwrap_or(false));
                    stats.ops_total += 1;
                    stats.record_result(all_ok);
                    next_conn = 0;
                }
            }
        }

        drain_pending(&mut pending, &mut stats).await;
        stats.duration_ns = start.elapsed().as_nanos() as u64;

        info!(
            workload = %self.trace.workload,
            ops = stats.ops_total,
            connections = num_connections,
            duration_ms = stats.duration_ns / 1_000_000,
            ops_per_sec = format!("{:.0}", stats.ops_per_sec()),
            "Replay complete"
        );

        stats
    }

    /// Disconnect all clients gracefully.
    pub async fn disconnect(self) {
        drop(self.txs);
        for handle in self.conn_handles {
            let _ = handle.await;
        }
    }
}

async fn drain_pending(pending: &mut Vec<oneshot::Receiver<bool>>, stats: &mut ReplayStats) {
    for rx in pending.drain(..) {
        stats.record_result(rx.await.unwrap_or(false));
    }
}

/// Generate deterministic data for replay. Pattern is based on offset for reproducibility.
fn deterministic_data(offset: u64, length: usize) -> Vec<u8> {
    let mut data = vec![0u8; length];
    let seed = offset.to_le_bytes();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = seed[i % 8] ^ (i as u8);
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use portalbd::TracedOp;
    use portalbd::config::{Config, DeviceConfig};
    use slatedb::clock::DefaultSystemClock;

    #[tokio::test]
    async fn replay_executes_ops() {
        let config = Config {
            device: DeviceConfig { disk_size_gb: 1 },
            ..Default::default()
        };
        let daemon = Daemon::from_config(config).await.unwrap();
        let clock = DefaultSystemClock::new();
        let mut trace = OpTrace::new("test", 1024 * 1024, &clock);

        // Add write, read, flush ops
        trace.ops.push(TracedOp {
            seq: 0,
            timestamp_ns: 0,
            duration_ns: 0,
            success: true,
            op: NbdOp::Write {
                offset: 0,
                length: 512,
            },
        });
        trace.ops.push(TracedOp {
            seq: 1,
            timestamp_ns: 0,
            duration_ns: 0,
            success: true,
            op: NbdOp::Read {
                offset: 0,
                length: 512,
            },
        });
        trace.ops.push(TracedOp {
            seq: 2,
            timestamp_ns: 0,
            duration_ns: 0,
            success: true,
            op: NbdOp::Flush,
        });

        let replayer = NbdReplayer::new(trace, &daemon, 2).await.unwrap();
        let stats = replayer.replay().await;

        assert_eq!(stats.ops_total, 3);
        assert_eq!(stats.ops_success, 3);
        assert_eq!(stats.bytes_read, 512);
        assert_eq!(stats.bytes_written, 512);
    }
}
