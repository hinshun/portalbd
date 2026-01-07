//! Listener trait and implementations for accepting NBD connections.
//!
//! Provides a unified abstraction over different connection sources:
//! - TCP sockets (production)
//! - Unix sockets (optional)
//! - Channel-based streams (testing/benchmarks via `StreamListener`)

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

/// Trait for accepting incoming connections.
///
/// Implemented for `TcpListener`, `UnixListener`, and `StreamListener`.
#[async_trait]
pub trait Listener: Send {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Accept the next incoming connection.
    async fn accept(&mut self) -> std::io::Result<Self::Stream>;
}

#[async_trait]
impl Listener for TcpListener {
    type Stream = TcpStream;

    async fn accept(&mut self) -> std::io::Result<Self::Stream> {
        TcpListener::accept(self)
            .await
            .map(|(stream, _addr)| stream)
    }
}

#[cfg(unix)]
#[async_trait]
impl Listener for UnixListener {
    type Stream = UnixStream;

    async fn accept(&mut self) -> std::io::Result<Self::Stream> {
        UnixListener::accept(self)
            .await
            .map(|(stream, _addr)| stream)
    }
}

/// A listener that accepts streams from a channel.
///
/// Useful for testing and benchmarks where connections are established
/// via in-memory duplex streams rather than real sockets.
///
/// # Example
///
/// ```ignore
/// use tokio::io::duplex;
/// use portalbd::nbd::StreamListener;
///
/// let (tx, listener) = StreamListener::new(4);
///
/// // Spawn server accepting from listener
/// tokio::spawn(daemon.listen(listener));
///
/// // Create client connections
/// for _ in 0..4 {
///     let (client_stream, server_stream) = duplex(1024 * 1024);
///     tx.send(server_stream).await.unwrap();
///     let client = NbdClient::connect(client_stream, "test").await.unwrap();
/// }
/// ```
pub struct StreamListener<S> {
    rx: mpsc::Receiver<S>,
}

impl<S> StreamListener<S> {
    /// Create a new stream listener with the given buffer capacity.
    ///
    /// Returns the sender half for pushing streams and the listener.
    pub fn new(buffer: usize) -> (mpsc::Sender<S>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, Self { rx })
    }
}

#[async_trait]
impl<S> Listener for StreamListener<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = S;

    async fn accept(&mut self) -> std::io::Result<Self::Stream> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn stream_listener_accepts_streams() {
        let (tx, mut listener) = StreamListener::new(2);

        let (_, server1) = duplex(1024);
        let (_, server2) = duplex(1024);

        tx.send(server1).await.unwrap();
        tx.send(server2).await.unwrap();

        // Accept should return the streams in order
        listener.accept().await.unwrap();
        listener.accept().await.unwrap();
    }

    #[tokio::test]
    async fn stream_listener_returns_error_when_closed() {
        let (tx, mut listener) = StreamListener::<tokio::io::DuplexStream>::new(1);
        drop(tx);

        let result = listener.accept().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    }
}
